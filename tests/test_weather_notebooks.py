from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_weather_ingestion_notebook_contains_awc_metar_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "01c_ingest_awc_metar.ipynb").read_text()

    for expected_text in [
        "01c Ingest AWC METAR",
        "adsb_airspace_eddf.brz_weather.metar_raw",
        "01c_ingest_awc_metar",
        "AviationWeatherConfig",
        "aviationweather.gov/api/data",
        "/api/data/metar",
        "derive_query_hours",
        "MERGE INTO",
        "obs.ingestion_log",
        "obs.ingestion_partition_log",
        "previous 15 days",
    ]:
        assert expected_text in notebook_text


def test_weather_alignment_notebook_contains_window_alignment_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "02c_align_weather_to_windows.ipynb").read_text()

    for expected_text in [
        "02c Align Weather To Analysis Windows",
        "slv_airspace.analysis_window_weather_context_v1",
        "02c_align_weather_to_windows",
        "latest_not_after_window_start",
        "max_weather_age_minutes",
        "missing_weather",
        "weather_run_id",
        "analysis_window_start",
        "obs.pipeline_run_log",
    ]:
        assert expected_text in notebook_text
