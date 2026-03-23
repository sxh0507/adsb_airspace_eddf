from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_history_notebook_contains_partition_contract_and_live_execution_hooks() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "01a_ingest_opensky_history.ipynb").read_text()

    for expected_text in [
        "01a Ingest OpenSky History",
        "adsb_airspace_eddf.brz_adsb.hist_state_vectors",
        "adsb_airspace_eddf.brz_adsb.hist_flights",
        "adsb_airspace_eddf.obs.ingestion_log",
        "adsb_airspace_eddf.obs.ingestion_partition_log",
        "state_vectors_data4",
        "flights_data4",
        "dry_run",
        "replaceWhere",
        "empty_source",
        "hour",
        "day",
        "OpenSkyTrinoClient",
        "dbutils.secrets.get",
        "01a_ingest_opensky_history",
        "Historical ingestion completed with",
    ]:
        assert expected_text in notebook_text

    assert "NotImplementedError" not in notebook_text


def test_opensky_trino_client_module_exists() -> None:
    module_text = (REPO_ROOT / "src" / "ingestion" / "opensky_trino_client.py").read_text()

    for expected_text in [
        "class OpenSkyTrinoClient",
        "def build_config",
        "trino.opensky-network.org",
        "BasicAuthentication",
        "query_pandas",
    ]:
        assert expected_text in module_text
