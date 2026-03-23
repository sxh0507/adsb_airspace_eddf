from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_contains_frankfurt_scope_and_trino_plan() -> None:
    readme_text = (REPO_ROOT / "README.md").read_text()

    for expected_text in [
        "Frankfurt Airspace Complexity Analytics",
        "adsb_airspace_eddf",
        "EDDF",
        "120-180 NM",
        "SFC-FL100",
        "FL100-FL245",
        "OpenSky Trino",
        "5-minute",
        "15-minute",
        "01a_ingest_opensky_history.ipynb",
        "01b_ingest_opensky_live.ipynb",
        "obs.ingestion_partition_log",
        "dry_run=true",
        "aircraft_count",
        "heading_dispersion",
        "altitude_dispersion",
        "speed_dispersion",
    ]:
        assert expected_text in readme_text


def test_configs_capture_initial_region_and_pipeline_defaults() -> None:
    region_text = (REPO_ROOT / "configs" / "region_config.yaml").read_text()
    pipeline_text = (REPO_ROOT / "configs" / "pipeline_config.yaml").read_text()

    for expected_text in [
        "focus_airport: EDDF",
        "ingestion_radius_nm: 150",
        "min_latitude: 49.5",
        "max_longitude: 9.6",
    ]:
        assert expected_text in region_text

    for expected_text in [
        "catalog_name: adsb_airspace_eddf",
        "trino_connection:",
        "secret_scope: opensky",
        "complexity_window_minutes: 5",
        "trend_window_minutes: 15",
        "traffic_load",
        "interaction",
        "flow_structure",
    ]:
        assert expected_text in pipeline_text
