"""HTTP adapter for the open-weight Maya 2 Native server, not the hosted Maya API."""

import copy
import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from pipecat.services.settings import NOT_GIVEN, TTSSettings
from pipecat.transcriptions.language import Language

from .http import MayaHttpTTSService

NATIVE_LANGUAGES = frozenset(
    {"as", "bn", "en", "gu", "hi", "kn", "ml", "mr", "or", "pa", "ta", "te"}
)


def _language(language):
    value = language.value if isinstance(language, Language) else language
    if value == "en-IN":
        value = "en"
    if value is not None and (not isinstance(value, str) or value not in NATIVE_LANGUAGES):
        raise ValueError(f"Unsupported Native language; use {sorted(NATIVE_LANGUAGES)} or None")
    return value


@dataclass
class MayaNativeTTSSettings(TTSSettings):
    """Native server settings. None language asks the server to detect the script."""

    normalize: bool = field(default_factory=lambda: NOT_GIVEN)


class MayaNativeTTSService(MayaHttpTTSService):
    """Stream self-hosted Maya 2 Native HTTP speech into a Pipecat pipeline.

    Reuses the HTTP adapter's cancellation, bounded retries, session lifecycle,
    sample alignment and resampling. It sends the open-weight server contract,
    which differs from Maya's hosted API in path, payload, voices and metadata.
    """

    Settings = MayaNativeTTSSettings
    _settings: MayaNativeTTSSettings
    _requires_api_key = False

    def __init__(
        self,
        *,
        base_url: str = "http://127.0.0.1:8080",
        api_key: str | None = None,
        settings: MayaNativeTTSSettings | None = None,
        **kwargs,
    ):
        """Connect to a running maya3.server, optionally behind an authenticated proxy.

        base_url is the server root (without /v1). The server itself needs no
        API key; api_key is only for a user-configured reverse proxy. The voice
        must exist in GET /v1/voices. Output rate is controlled by sample_rate.
        """
        if not isinstance(base_url, str) or not base_url.strip():
            raise ValueError("base_url must be a nonempty HTTP(S) server root")
        parts = urlsplit(base_url)
        if (
            parts.scheme not in ("http", "https")
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
        ):
            raise ValueError("base_url must be an HTTP(S) server root without credentials or query")
        if any(name in kwargs for name in ("url", "provider_sample_rate", "encoding")):
            raise ValueError(
                "Native uses base_url and fixed 24 kHz PCM; set sample_rate for output"
            )
        super().__init__(
            api_key=api_key,
            url=base_url.rstrip("/") + "/v1/audio/speech",
            settings=settings,
            **kwargs,
        )

    def _initial_settings(self, settings):
        result = self.Settings(model="maya3", voice="Ahana", language=None, normalize=True)
        if settings is not None:
            result.apply_update(settings)
        return self._validated_settings(result)

    def _validated_settings(self, settings):
        result = copy.deepcopy(settings)
        result.validate_complete()
        if result.extra or result.model != "maya3":
            raise ValueError("Native requires model='maya3' and supports no extra settings")
        if not isinstance(result.voice, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}", result.voice
        ):
            raise ValueError("voice must be a registered Native voice ID")
        result.language = _language(result.language)
        if not isinstance(result.normalize, bool):
            raise ValueError("normalize must be a boolean")
        return result

    def language_to_service_language(self, language: Language) -> str | None:
        """Map a Pipecat language to the self-hosted server's twelve-language catalog."""
        return _language(language)

    def _request_body(self, text):
        result = {
            "model": "maya3",
            "input": text,
            "voice": self._settings.voice,
            "response_format": "pcm",
            "normalize": self._settings.normalize,
        }
        if self._settings.language is not None:
            result["language"] = self._settings.language
        return result

    def _request_headers(self):
        headers = {"Content-Type": "application/json", "User-Agent": "pipecat-maya/0.2.0"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    def _response_audio_format(self, response):
        headers = response.headers
        if (
            headers.get("Content-Type", "").split(";")[0].strip().lower() != "audio/pcm"
            or headers.get("X-Sample-Rate") != "24000"
            or headers.get("X-Channels") != "1"
            or headers.get("X-Encoding") != "pcm_s16le"
        ):
            raise ValueError("Native must return audio/pcm, 24000 Hz, mono, pcm_s16le metadata")
        return 24000, "pcm_s16le"
