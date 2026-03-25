from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_live_clean_notebook_contains_v2_live_cellization_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "02b_prepare_live_states_v2.ipynb").read_text()

    for expected_text in [
        "02b Prepare Live States V2",
        "01b_ingest_opensky_live",
        "brz_adsb.live_states",
        "slv_airspace.flight_states_cellized_v2",
        "ref.cell_schemes_v2",
        "ref.airspace_cells_v2",
        "obs.pipeline_run_log",
        "02b_prepare_live_states_v2",
        "silver_live_v2",
        "source_snapshot_time",
        "baro_altitude_m",
        "velocity_mps",
        "true_track_deg",
        "vertical_rate_mps",
        "scope_airport",
        "scope_radius_nm",
        "analysis_window_start",
        "horizontal_cell_id",
        "vertical_cell_id",
        "projection_mode",
        "Refusing to overwrite an entire source run when source_snapshot_time is set",
        "DELETE FROM",
        "local_nm",
    ]:
        assert expected_text in notebook_text
