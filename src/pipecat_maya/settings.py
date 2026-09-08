"""Shared settings for Maya's documented public API."""

import copy
import json
import math
from dataclasses import dataclass, field

from pipecat.services.settings import NOT_GIVEN, TTSSettings
from pipecat.transcriptions.language import Language

MODELS = {
    "Maya 2 Native": ("Ananya", "Arjun"),
    "Maya Calyx": (
        "Amit",
        "Seema",
        "Tripti",
        "Gargi",
        "Aarav",
        "Zara",
        "Rahul",
        "Nila",
        "Riley",
        "Riya",
        "Vikram",
        "Christine",
        "Sagar",
        "Rohan",
        "Jackson",
        "Sana",
        "Tarini",
        "Christopher",
        "Vance",
        "Diya",
        "SagarM",
        "Samar",
        "Shailika",
        "Shreeraj",
        "Vikas",
        "Arushi",
        "Kavita",
        "Neeraj",
        "Neha",
        "Rehan",
        "Kabir",
    ),
}
CATALOG_URL = "https://tts.mayaresearch.ai/v1/tts"
"""Synthesis endpoint. The provider documents no catalogue endpoint; an unknown voice returns
400 and the body lists the valid values, which is what :func:`refresh_catalog` reads."""

_PROBE_VOICE = "__pipecat_maya_catalog_probe__"


async def refresh_catalog(api_key, *, session=None, timeout=15.0, models=None):
    """Replace the shipped voice catalog with what the provider currently serves.

    The catalog above is a literal, so a voice added after a release is unreachable: validation
    rejects it before any request is made, and the error names the voice rather than the stale
    list. Calling this once at startup removes that failure mode without making every import
    depend on the network.

    The provider documents no catalogue endpoint. It does document that an unknown ``voice``
    returns 400 with the valid values in the body, so this asks for a name that cannot exist and
    reads the answer. That is a documented contract, not a scrape.

    Fails safe by construction. A network error, a non-400 status, an unparseable body or an
    empty list leaves the shipped catalog untouched and is reported rather than raised, because
    an application that cannot reach the provider should still start with a usable catalog.

    Args:
        api_key: Maya API key. Never logged or included in the returned report.
        session: Optional ``aiohttp.ClientSession`` to reuse. One is created per call otherwise.
        timeout: Total seconds allowed per model probe.
        models: Model names to refresh. Defaults to every model in the catalog.

    Returns:
        ``{model: {"voices": (...), "added": [...], "removed": [...], "error": str | None}}``.
        ``added`` and ``removed`` are empty when a model is already in sync, and ``error`` is
        set on any model left untouched.
    """
    import aiohttp

    report = {}
    owned = session is None
    session = session or aiohttp.ClientSession()
    try:
        for model in list(models or MODELS):
            entry = {"voices": MODELS.get(model, ()), "added": [], "removed": [], "error": None}
            report[model] = entry
            if model not in MODELS:
                entry["error"] = "unknown model"
                continue
            try:
                live = await _probe_voices(session, api_key, model, timeout)
            except Exception as exc:  # noqa: BLE001 - reported, never raised
                entry["error"] = f"{type(exc).__name__}: {str(exc).replace(api_key, '[redacted]')}"
                continue
            if not live:
                entry["error"] = "no voice list in the provider response"
                continue
            shipped = MODELS[model]
            entry["added"] = [v for v in live if v not in shipped]
            entry["removed"] = [v for v in shipped if v not in live]
            entry["voices"] = tuple(live)
            # Mutated in place: validated_settings() reads this module global, so rebinding the
            # name here would leave validation checking the old catalog.
            MODELS[model] = tuple(live)
    finally:
        if owned:
            await session.close()
    return report


async def _probe_voices(session, api_key, model, timeout):
    """Ask for an impossible voice and read the valid values out of the documented 400."""
    import aiohttp

    payload = {"model": model, "voice": _PROBE_VOICE, "language": "en", "text": "."}
    async with session.post(
        CATALOG_URL,
        json=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as response:
        body = await response.read()
        if response.status != 400:
            # A 200 would mean the probe name was accepted, which makes the answer meaningless.
            raise RuntimeError(f"expected 400 listing the valid voices, got {response.status}")
    try:
        parsed = json.loads(body)
    except ValueError as exc:
        raise RuntimeError("provider 400 body was not JSON") from exc
    voices = parsed.get("available_voices")
    if voices is None and isinstance(parsed.get("detail"), dict):
        voices = parsed["detail"].get("available_voices")
    if not isinstance(voices, list) or not all(isinstance(v, str) and v for v in voices):
        return []
    return voices


LANGUAGES = frozenset({"hi", "te", "bn", "gu", "kn", "ml", "mr", "or", "pa", "ta", "en"})


@dataclass
class MayaTTSSettings(TTSSettings):
    """Runtime Maya settings, usable as a complete store or a sparse update.

    Parameters:
        speed: Native-only speaking speed, 0.5 through 1.25. None means
            the provider default (1.0 on Native), and omits it on Calyx.
        language: None enables automatic language detection, including code-switching.
    """

    speed: float | None = field(default_factory=lambda: NOT_GIVEN)


def service_language(language: Language | str | None) -> str | None:
    """Resolve only documented languages; never silently substitute an accent."""
    if language is None:
        return None
    value = language.value if isinstance(language, Language) else language
    if value == "en-IN":
        return "en"
    if value not in LANGUAGES:
        raise ValueError(f"Unsupported Maya language {value!r}; use {sorted(LANGUAGES)} or None")
    return value


def validated_settings(settings: MayaTTSSettings) -> MayaTTSSettings:
    """Return a validated copy without changing the caller's settings delta."""
    result = copy.deepcopy(settings)
    result.validate_complete()
    if result.extra:
        raise ValueError(f"Unsupported Maya settings: {', '.join(result.extra)}")
    if result.model not in MODELS:
        raise ValueError(f"Unknown Maya model {result.model!r}; use {list(MODELS)}")
    if result.voice not in MODELS[result.model]:
        raise ValueError(f"Voice {result.voice!r} does not belong to {result.model}")
    result.language = service_language(result.language)
    if result.speed is not None:
        if result.model != "Maya 2 Native":
            raise ValueError("speed is supported only by Maya 2 Native; use speed=None for Calyx")
        if (
            isinstance(result.speed, bool)
            or not isinstance(result.speed, (float, int))
            or not math.isfinite(result.speed)
            or not 0.5 <= result.speed <= 1.25
        ):
            raise ValueError("speed must be a number between 0.5 and 1.25")
    return result


def default_settings(delta: MayaTTSSettings | None = None) -> MayaTTSSettings:
    """Create complete defaults and apply optional caller overrides."""
    settings = MayaTTSSettings(model="Maya 2 Native", voice="Ananya", language=None, speed=None)
    if delta is not None:
        settings.apply_update(delta)
    return validated_settings(settings)


def request_settings(settings: MayaTTSSettings) -> dict:
    """Build the provider fields, omitting automatic language and Calyx speed."""
    payload = {"model": settings.model, "voice": settings.voice}
    if settings.language is not None:
        payload["language"] = settings.language
    if settings.model == "Maya 2 Native":
        payload["speed"] = settings.speed if settings.speed is not None else 1.0
    return payload
