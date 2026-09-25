"""Keep install metadata and the runtime compatibility claim in sync."""

from importlib.metadata import requires, version

import pipecat_maya


def test_release_version_matches_installed_distribution():
    assert pipecat_maya.__version__ == version("pipecat-maya") == "0.2.0"


def test_tested_pipecat_is_the_declared_dependency():
    assert version("pipecat-ai") == "1.11.0"
    assert "pipecat-ai==1.11.0" in requires("pipecat-maya")
