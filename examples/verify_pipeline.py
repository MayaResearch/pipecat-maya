"""Verify Maya through a real Pipecat worker and paced output transport.

The output is a recorded transport sink, not a browser microphone or phone call.
All provider synthesis, Pipecat frames, queue discard, and audio writes are real.

Run after installing this package and setting MAYA_API_KEY in your environment::

    python examples/verify_pipeline.py --output-dir artifacts/pipeline

This performs a small, billable live synthesis check. It writes synthetic audio,
timestamped events, and a JSON report. Exit status is nonzero on failed checks.
"""

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
import wave
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pipecat
from loguru import logger
from pipecat.frames.frames import (
    EndFrame,
    ErrorFrame,
    InterruptionFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    TTSAudioRawFrame,
    TTSSpeakFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    TTSUpdateSettingsFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.transports.base_transport import TransportParams
from pipecat.workers.runner import WorkerRunner

import pipecat_maya
from pipecat_maya import MayaTTSService, MayaTTSSettings

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--output-dir", type=Path, default=Path("artifacts/pipeline"))
args = parser.parse_args()
ROOT = args.output_dir.resolve()
ROOT.mkdir(parents=True, exist_ok=True)
API_KEY = os.environ.get("MAYA_API_KEY", "")
if not API_KEY.strip():
    parser.error("Set MAYA_API_KEY in your environment before running this example")
logger.remove()
logger.add(sys.stderr, level="WARNING")
OUT = {
    "started_at": datetime.now(timezone.utc).isoformat(),
    "pipecat_version": pipecat.__version__,
    "results": [],
}


def save_wav(name, data, rate):
    with wave.open(str(ROOT / name), "wb") as file:
        file.setnchannels(1)
        file.setsampwidth(2)
        file.setframerate(rate)
        file.writeframes(data)
    return name


class Evidence:
    def __init__(self, name, rate):
        self.name, self.rate = name, rate
        self.epoch = time.monotonic()
        self.events = []
        self.label = "startup"
        self.context_labels = {}
        self.service_audio = defaultdict(bytearray)
        self.played_audio = defaultdict(bytearray)
        self.playback_chunks = []
        self.played_stop_counts = Counter()
        self.service_stop_counts = Counter()
        self.service_start_counts = Counter()
        self.errors = []
        self.interrupt_time = None
        self.interrupted_context = None

    def add(self, event, **details):
        entry = {"t": round(time.monotonic() - self.epoch, 5), "event": event, **details}
        self.events.append(entry)
        return entry

    def write(self):
        contexts = []
        for cid, data in self.service_audio.items():
            label = self.context_labels.get(cid, "unknown")
            name = f"{self.name}-{label}-{cid[:8]}.wav"
            played = self.played_audio[cid]
            interrupted = label == "interrupted"
            exact_prefix = data.startswith(played) if interrupted else played.startswith(data)
            padding = b"" if interrupted else played[len(data) :]
            normal_terminals = (
                self.service_start_counts[cid]
                == self.service_stop_counts[cid]
                == self.played_stop_counts[cid]
                == 1
            )
            if not exact_prefix or any(padding) or (not interrupted and not normal_terminals):
                self.errors.append(f"PCM integrity or terminal count failed for {label}")
            contexts.append(
                {
                    "context_id": cid,
                    "label": label,
                    "service_audio_bytes": len(data),
                    "service_duration_seconds": len(data) / (2 * self.rate),
                    "played_audio_bytes": len(played),
                    "played_duration_seconds": len(played) / (2 * self.rate),
                    "service_starts": self.service_start_counts[cid],
                    "service_stops": self.service_stop_counts[cid],
                    "playback_stops": self.played_stop_counts[cid],
                    "played_prefix_matches_service_pcm": exact_prefix,
                    "tail_padding_only_silence": not any(padding),
                    "wav": save_wav(name, data, self.rate),
                    "played_wav": save_wav(name.replace(".wav", "-played.wav"), played, self.rate),
                }
            )
        timeline = bytearray()
        for chunk in self.playback_chunks:
            position = round(chunk["t"] * self.rate) * 2
            data = chunk["audio"]
            required = position + len(data)
            if len(timeline) < required:
                timeline.extend(bytes(required - len(timeline)))
            timeline[position:required] = data
        stale = [
            e
            for e in self.events
            if self.interrupt_time is not None
            and e["t"] > self.interrupt_time
            and e["event"] == "playback_audio"
            and e.get("context_id") == self.interrupted_context
        ]
        if stale:
            self.errors.append("Cancelled context wrote audio after output interruption")
        if not contexts:
            self.errors.append("No audio contexts completed")
        result = {
            "test": self.name,
            "sample_rate": self.rate,
            "errors": self.errors,
            "contexts": contexts,
            "interruption_time": self.interrupt_time,
            "interrupted_context": self.interrupted_context,
            "old_playback_writes_after_interruption": len(stale),
            "timeline_wav": save_wav(f"{self.name}-timeline.wav", timeline, self.rate),
            "events_file": f"{self.name}-events.json",
            "elapsed_seconds": round(time.monotonic() - self.epoch, 4),
        }
        (ROOT / result["events_file"]).write_text(
            json.dumps(self.events, ensure_ascii=False, indent=2)
        )
        return result


class ServiceTap(FrameProcessor):
    def __init__(self, evidence):
        super().__init__()
        self.evidence = evidence

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        ev = self.evidence
        if direction is FrameDirection.DOWNSTREAM:
            cid = getattr(frame, "context_id", None)
            if isinstance(frame, TTSStartedFrame):
                ev.context_labels[cid] = ev.label
                ev.service_start_counts[cid] += 1
                ev.add("service_start", context_id=cid, label=ev.label)
            elif isinstance(frame, TTSAudioRawFrame):
                if frame.sample_rate != ev.rate or frame.num_channels != 1:
                    ev.errors.append("Unexpected PCM rate or channel count")
                ev.service_audio[cid].extend(frame.audio)
                ev.add(
                    "service_audio",
                    context_id=cid,
                    bytes=len(frame.audio),
                    sample_rate=frame.sample_rate,
                )
            elif isinstance(frame, TTSStoppedFrame):
                ev.service_stop_counts[cid] += 1
                ev.add("service_stop", context_id=cid)
            elif isinstance(frame, InterruptionFrame):
                ev.add("service_interruption")
        if isinstance(frame, ErrorFrame):
            error = str(frame.error).replace(API_KEY, "[redacted]")
            if error not in ev.errors:
                ev.errors.append(error)
                ev.add("error", error=error)
        await self.push_frame(frame, direction)


class RecordedOutput(BaseOutputTransport):
    def __init__(self, evidence):
        super().__init__(
            TransportParams(
                audio_out_enabled=True,
                audio_out_sample_rate=evidence.rate,
                audio_out_10ms_chunks=2,
                audio_out_end_silence_secs=0,
                audio_out_auto_silence=False,
            )
        )
        self.evidence = evidence
        self.active_context = None

    async def start(self, frame):
        await super().start(frame)
        await self.set_transport_ready(frame)

    async def process_frame(self, frame, direction):
        if isinstance(frame, InterruptionFrame):
            ev = self.evidence
            ev.interrupted_context = self.active_context
            marker = ev.add("output_interruption", context_id=self.active_context)
            ev.interrupt_time = marker["t"]
            self.active_context = None
        await super().process_frame(frame, direction)

    async def write_transport_frame(self, frame):
        if isinstance(frame, TTSStartedFrame):
            self.active_context = frame.context_id
            self.evidence.add("playback_start", context_id=self.active_context)

    async def write_audio_frame(self, frame):
        ev = self.evidence
        cid = self.active_context
        marker = ev.add(
            "playback_audio", context_id=cid, bytes=len(frame.audio), sample_rate=frame.sample_rate
        )
        ev.played_audio[cid].extend(frame.audio)
        ev.playback_chunks.append({"t": marker["t"], "audio": frame.audio})
        # The transport's write is paced at real time, as a device/network sink is.
        # Pipecat cancels this task and discards its buffered frames on interruption.
        await asyncio.sleep(len(frame.audio) / (frame.sample_rate * frame.num_channels * 2))
        return True


class PlaybackTap(FrameProcessor):
    def __init__(self, evidence):
        super().__init__()
        self.evidence = evidence

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if direction is FrameDirection.DOWNSTREAM and isinstance(frame, TTSStoppedFrame):
            self.evidence.played_stop_counts[frame.context_id] += 1
            self.evidence.add("playback_stop", context_id=frame.context_id)
        await self.push_frame(frame, direction)


async def until(predicate, timeout=35):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.02)


async def run_pipeline(name, rate, extended):
    ev = Evidence(name, rate)
    service = MayaTTSService(
        api_key=API_KEY, sample_rate=rate, settings=MayaTTSSettings(language="en")
    )
    # The first tap captures upstream errors emitted by the service itself.
    pipeline = Pipeline(
        [ServiceTap(ev), service, ServiceTap(ev), RecordedOutput(ev), PlaybackTap(ev)]
    )
    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(audio_out_sample_rate=rate, enable_metrics=True),
        enable_rtvi=False,
        cancel_on_idle_timeout=False,
    )
    started = asyncio.Event()

    @worker.event_handler("on_pipeline_started")
    async def ready(worker, frame):
        ev.add("pipeline_ready", session_id=service.session_id)
        started.set()

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(worker)

    async def feed():
        await asyncio.wait_for(started.wait(), 30)

        async def speak(label, text, tokens=False, wait=True):
            ev.label = label
            ev.add("input_text", label=label, text=text, token_stream=tokens)
            before = sum(ev.played_stop_counts.values())
            if tokens:
                await worker.queue_frame(LLMFullResponseStartFrame())
                # Deliberately split inside words: Pipecat owns sentence assembly.
                for offset in range(0, len(text), 4):
                    await worker.queue_frame(LLMTextFrame(text[offset : offset + 4]))
                    await asyncio.sleep(0.025)
                await worker.queue_frame(LLMFullResponseEndFrame())
            else:
                await worker.queue_frame(TTSSpeakFrame(text))
            if wait:
                await until(lambda: sum(ev.played_stop_counts.values()) > before or ev.errors)
                if ev.errors:
                    raise RuntimeError("Pipeline emitted an error")

        try:
            if not extended:
                await speak("native_8k_full_tail", "Your order will arrive tomorrow morning.")
            else:
                await speak(
                    "token_sentences",
                    "First, your order will arrive tomorrow morning. "
                    "Second, please be ready at ten o'clock.",
                    tokens=True,
                )
                await speak("direct_speak", "Your order will arrive tomorrow morning")
                await speak(
                    "interrupted",
                    "This is a synthetic interruption test. "
                    "We are checking that buffered speech stops promptly when the caller "
                    "interrupts the assistant and the next reply begins cleanly.",
                    wait=False,
                )
                await until(
                    lambda: any(
                        ev.context_labels.get(cid) == "interrupted" and len(data) >= rate
                        for cid, data in ev.played_audio.items()
                    )
                )
                ev.add("input_interruption")
                await worker.queue_frame(InterruptionFrame())
                await until(lambda: ev.interrupt_time is not None)
                await speak("after_interrupt", "The new response starts now.")
                ev.add(
                    "input_settings_update",
                    model="Maya 2 Native",
                    voice="Arjun",
                    language="hi",
                    speed=0.75,
                )
                await worker.queue_frame(
                    TTSUpdateSettingsFrame(
                        delta=MayaTTSSettings(voice="Arjun", language="hi", speed=0.75)
                    )
                )
                await speak("native_updated_hi_speed", "नमस्ते, आपका दिन शुभ हो।")
                ev.add(
                    "settings_verified",
                    session_id=service.session_id,
                    model=service._settings.model,
                    voice=service._settings.voice,
                    language=service._settings.language,
                    speed=service._settings.speed,
                )
                ev.add(
                    "input_settings_update",
                    model="Maya Calyx",
                    voice="Aarav",
                    language=None,
                    speed=None,
                )
                await worker.queue_frame(
                    TTSUpdateSettingsFrame(
                        delta=MayaTTSSettings(
                            model="Maya Calyx", voice="Aarav", language=None, speed=None
                        )
                    )
                )
                await speak("calyx_updated_auto", "नमस्ते, your order is ready.")
                ev.add(
                    "settings_verified",
                    session_id=service.session_id,
                    model=service._settings.model,
                    voice=service._settings.voice,
                    language=service._settings.language,
                    speed=service._settings.speed,
                )
        except Exception as error:
            ev.errors.append(type(error).__name__)
            ev.add("harness_error", exception_type=type(error).__name__)
        finally:
            await worker.queue_frame(EndFrame())

    try:
        async with asyncio.timeout(160):
            await asyncio.gather(runner.run(), feed())
    except Exception as error:
        ev.errors.append(type(error).__name__)
        await worker.cancel(reason="bounded QA teardown")
    result = ev.write()
    OUT["results"].append(result)
    (ROOT / "pipeline-results.json").write_text(json.dumps(OUT, ensure_ascii=False, indent=2))
    print(json.dumps(result, ensure_ascii=False), flush=True)


async def main():
    package_dir = Path(pipecat_maya.__file__).resolve().parent
    OUT["source_sha256"] = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in package_dir.glob("*.py")
    }
    await run_pipeline("service16k", 16000, True)
    await run_pipeline("service8k", 8000, False)
    OUT["completed_at"] = datetime.now(timezone.utc).isoformat()
    (ROOT / "pipeline-results.json").write_text(json.dumps(OUT, ensure_ascii=False, indent=2))
    if any(result["errors"] for result in OUT["results"]):
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
