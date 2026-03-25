from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_live_notebook_contains_live_ingestion_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "01b_ingest_opensky_live.ipynb").read_text()

    for expected_text in [
        "01b Ingest OpenSky Live",
        "adsb_airspace_eddf.brz_adsb.live_states",
        "adsb_airspace_eddf.obs.live_snapshot_manifest",
        "01b_ingest_opensky_live",
        "OpenSkyLiveClient",
        "OpenSkyOAuth2TokenManager",
        "states/all",
        "catch_up",
        "loop",
        "floor_to_interval",
        "pending_epochs",
        "MERGE INTO",
        "duplicate_snapshot_complete",
        "dbutils.secrets.get",
    ]:
        assert expected_text in notebook_text
