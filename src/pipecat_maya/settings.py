"""Shared settings for Maya's documented public API."""

import copy
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
