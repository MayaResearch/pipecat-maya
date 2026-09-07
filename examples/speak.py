"""Synthesize a complete paragraph through a real Pipecat pipeline to a WAV file.

Run: python examples/speak.py --text 'नमस्ते! आपका ऑर्डर कल पहुँच जाएगा।'
Only MAYA_API_KEY is needed. No microphone, STT, or LLM account is required.
"""

import argparse
import asyncio
import os
import wave
from pathlib import Path

from pipecat.frames.frames import EndFrame, ErrorFrame, Frame, TTSAudioRawFrame, TTSSpeakFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.workers.runner import WorkerRunner

from pipecat_maya import MayaHttpTTSService, MayaTTSService


class WaveOutput(FrameProcessor):
    """Write the PCM emitted by Pipecat using its actual frame format."""

    def __init__(self, output: Path):
        super().__init__()
        self.output = output
        self.writer = None
        self.bytes_written = 0

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, TTSAudioRawFrame):
            if self.writer is None:
                self.writer = wave.open(str(self.output), "wb")
                self.writer.setnchannels(frame.num_channels)
                self.writer.setsampwidth(2)
                self.writer.setframerate(frame.sample_rate)
            self.writer.writeframes(frame.audio)
            self.bytes_written += len(frame.audio)
        await self.push_frame(frame, direction)

    async def cleanup(self):
        await super().cleanup()
        if self.writer is not None:
            self.writer.close()
            self.writer = None


async def main(args):
    service = MayaHttpTTSService if args.http else MayaTTSService
    tts = service(
        api_key=os.environ["MAYA_API_KEY"],
        sample_rate=args.sample_rate,
        settings=service.Settings(
            model=args.model, voice=args.voice, language=args.language, speed=args.speed
        ),
    )
    output = WaveOutput(args.output)
    worker = PipelineWorker(
        Pipeline([tts, output]),
        params=PipelineParams(
            audio_out_sample_rate=args.sample_rate, enable_metrics=True, enable_usage_metrics=True
        ),
        enable_rtvi=False,
    )
    errors = []

    @worker.event_handler("on_pipeline_error")
    async def on_error(worker, frame: ErrorFrame):
        errors.append(frame.error)

    @worker.event_handler("on_pipeline_started")
    async def on_started(worker, frame):
        await worker.queue_frames([TTSSpeakFrame(args.text), EndFrame()])

    runner = WorkerRunner()
    await runner.add_workers(worker)
    await runner.run()
    if errors or output.bytes_written == 0:
        raise RuntimeError(f"Synthesis did not succeed: {errors or 'no audio'}")
    print(f"Saved {args.output} ({output.bytes_written} PCM bytes, {args.sample_rate} Hz)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--text", default="नमस्ते! आपका ऑर्डर कल पहुँच जाएगा।")
    parser.add_argument("--model", default="Maya 2 Native")
    parser.add_argument("--voice", default="Ananya")
    parser.add_argument("--language", default=None)
    parser.add_argument("--speed", type=float, default=None)
    parser.add_argument("--sample-rate", type=int, default=24000)
    parser.add_argument("--http", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("output.wav"))
    asyncio.run(main(parser.parse_args()))
