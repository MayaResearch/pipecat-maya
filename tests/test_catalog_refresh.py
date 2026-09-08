"""Catalog refresh against a mocked provider.

These are mocked, not live: they pin the contract this package relies on (an unknown voice
returns 400 with the valid values) and, more importantly, they pin the failure behaviour.
A refresh that raises or empties the catalog when the provider is unreachable would be worse
than the stale list it replaces, so every failure path is asserted to leave MODELS untouched.
"""

import json

import pytest

from pipecat_maya import refresh_catalog
from pipecat_maya.settings import MODELS, MayaTTSSettings, default_settings

SHIPPED = "Amit"
NEW = "Diya"


class FakeResponse:
    def __init__(self, *, status=400, body=b"", raises=None):
        self.status = status
        self._body = body
        self._raises = raises

    async def __aenter__(self):
        if self._raises is not None:
            raise self._raises
        return self

    async def __aexit__(self, *args):
        return False

    async def read(self):
        return self._body


class FakeSession:
    """Records what was sent so the request itself can be asserted, not just the outcome."""

    def __init__(self, response_for):
        self.response_for = response_for
        self.calls = []

    def post(self, url, *, json=None, headers=None, timeout=None):
        self.calls.append({"url": url, "json": json, "headers": headers})
        return self.response_for(json["model"])


@pytest.fixture(autouse=True)
def restore_catalog():
    original = {model: voices for model, voices in MODELS.items()}
    yield
    MODELS.clear()
    MODELS.update(original)


def body(voices):
    return json.dumps({"available_voices": voices}).encode()


async def test_adds_voices_the_shipped_catalog_does_not_list():
    live = list(MODELS["Maya Calyx"]) + ["Zzz_New_Voice"]
    session = FakeSession(
        lambda model: FakeResponse(body=body(live if model == "Maya Calyx" else ["Ananya"]))
    )
    report = await refresh_catalog("k", session=session, models=["Maya Calyx"])
    assert report["Maya Calyx"]["added"] == ["Zzz_New_Voice"]
    assert report["Maya Calyx"]["removed"] == []
    assert report["Maya Calyx"]["error"] is None
    assert "Zzz_New_Voice" in MODELS["Maya Calyx"]


async def test_a_refreshed_voice_passes_validation():
    """The point of the refresh: validation must see the new name, not the shipped tuple."""
    with pytest.raises(ValueError):
        default_settings(MayaTTSSettings(model="Maya Calyx", voice="Zzz_New_Voice"))
    session = FakeSession(lambda model: FakeResponse(body=body(["Amit", "Zzz_New_Voice"])))
    await refresh_catalog("k", session=session, models=["Maya Calyx"])
    settings = default_settings(MayaTTSSettings(model="Maya Calyx", voice="Zzz_New_Voice"))
    assert settings.voice == "Zzz_New_Voice"


async def test_reports_removals_without_inventing_them():
    session = FakeSession(lambda model: FakeResponse(body=body(["Amit"])))
    report = await refresh_catalog("k", session=session, models=["Maya Calyx"])
    assert SHIPPED not in report["Maya Calyx"]["removed"]
    assert "Seema" in report["Maya Calyx"]["removed"]
    assert MODELS["Maya Calyx"] == ("Amit",)


async def test_probe_asks_the_documented_endpoint_with_an_impossible_voice():
    session = FakeSession(lambda model: FakeResponse(body=body(["Amit"])))
    await refresh_catalog("k", session=session, models=["Maya Calyx"])
    call = session.calls[0]
    assert call["url"] == "https://tts.mayaresearch.ai/v1/tts"
    assert call["json"]["model"] == "Maya Calyx"
    assert call["json"]["voice"] not in MODELS["Maya Calyx"]
    assert call["headers"]["Authorization"] == "Bearer k"


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(status=200, body=b""),
        FakeResponse(body=b"not json"),
        FakeResponse(body=json.dumps({"available_voices": []}).encode()),
        FakeResponse(body=json.dumps({"available_voices": [1, 2]}).encode()),
        FakeResponse(body=json.dumps({"message": "bad voice"}).encode()),
        FakeResponse(raises=TimeoutError("timed out")),
        FakeResponse(raises=OSError("connection refused")),
    ],
)
async def test_every_failure_leaves_the_shipped_catalog_intact(response):
    before = MODELS["Maya Calyx"]
    session = FakeSession(lambda model: response)
    report = await refresh_catalog("k", session=session, models=["Maya Calyx"])
    assert MODELS["Maya Calyx"] == before
    assert report["Maya Calyx"]["error"]
    assert report["Maya Calyx"]["added"] == []


async def test_the_api_key_never_reaches_the_report():
    session = FakeSession(lambda model: FakeResponse(raises=OSError("refused for key sk-secret")))
    report = await refresh_catalog("sk-secret", session=session, models=["Maya Calyx"])
    assert "sk-secret" not in report["Maya Calyx"]["error"]
    assert "[redacted]" in report["Maya Calyx"]["error"]


async def test_nested_detail_body_is_accepted():
    payload = json.dumps({"detail": {"available_voices": ["Amit", "Diya"]}}).encode()
    session = FakeSession(lambda model: FakeResponse(body=payload))
    await refresh_catalog("k", session=session, models=["Maya Calyx"])
    assert MODELS["Maya Calyx"] == ("Amit", "Diya")


async def test_unknown_model_is_reported_and_not_created():
    session = FakeSession(lambda model: FakeResponse(body=body(["Amit"])))
    report = await refresh_catalog("k", session=session, models=["Nope"])
    assert report["Nope"]["error"] == "unknown model"
    assert "Nope" not in MODELS
    assert session.calls == []


async def test_defaults_to_every_model():
    session = FakeSession(
        lambda model: FakeResponse(
            body=body(["Ananya", "Arjun"] if model == "Maya 2 Native" else ["Amit"])
        )
    )
    report = await refresh_catalog("k", session=session)
    assert set(report) == set(MODELS)
    assert MODELS["Maya 2 Native"] == ("Ananya", "Arjun")
