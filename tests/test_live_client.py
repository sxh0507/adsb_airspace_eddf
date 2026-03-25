from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_opensky_live_client_module_contains_oauth2_and_states_contract() -> None:
    module_text = (REPO_ROOT / "src" / "ingestion" / "opensky_live_client.py").read_text()

    for expected_text in [
        "class OpenSkyOAuth2TokenManager",
        "class OpenSkyLiveClient",
        "class OpenSkyRateLimitError",
        "def build_config",
        "def normalize_states_payload",
        "https://opensky-network.org/api",
        "client_credentials",
        "/states/all",
        "Authorization",
    ]:
        assert expected_text in module_text
