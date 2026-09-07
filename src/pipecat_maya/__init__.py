"""Maya Research's community-maintained Pipecat TTS integration."""

from .http import MayaHttpTTSService
from .settings import LANGUAGES, MODELS, MayaTTSSettings
from .tts import MayaTTSService

__all__ = ["MayaTTSService", "MayaHttpTTSService", "MayaTTSSettings", "MODELS", "LANGUAGES"]
__version__ = "0.1.0"
