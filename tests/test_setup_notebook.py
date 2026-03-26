from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_setup_notebook_contains_expected_schemas_tables_and_scope_defaults() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "00_platform_setup_catalog_schema.ipynb").read_text()

    for expected_text in [
        "adsb_airspace_eddf",
        "ref",
        "brz_adsb",
        "brz_weather",
        "slv_airspace",
        "gld_airspace",
        "obs",
        "ref.project_scope",
        "ref.altitude_bands",
        "ref.grid_cells",
        "ref.cell_schemes_v2",
        "ref.airspace_cells_v2",
        "brz_adsb.hist_state_vectors",
        "brz_weather.metar_raw",
        "slv_airspace.flight_states_clean",
        "slv_airspace.flight_states_cellized_v2",
        "slv_airspace.analysis_window_weather_context_v1",
        "gld_airspace.grid_complexity_5m",
        "gld_airspace.horizontal_complexity_v2",
        "gld_airspace.horizontal_hotspots_v2",
        "gld_airspace.complexity_trend_v2",
        "obs.ingestion_partition_log",
        "obs.live_snapshot_manifest",
        "PARTITIONED BY (hour)",
        "PARTITIONED BY (day)",
        "EDDF",
        "SFC-FL100",
        "FL100-FL245",
        "traffic_load",
        "interaction",
        "flow_structure",
        "10 x 10 NM",
        "3000 ft",
    ]:
        assert expected_text in notebook_text
