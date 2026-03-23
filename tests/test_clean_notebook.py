from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_clean_notebook_contains_silver_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "02_clean_and_prepare_states.ipynb").read_text()

    for expected_text in [
        "02 Clean And Prepare States",
        "slv_airspace.flight_states_clean",
        "ref.grid_cells",
        "obs.pipeline_run_log",
        "source_hour_epoch",
        "grid_resolution_deg",
        "time_window_5m",
        "altitude_band_id",
        "grid_id",
        "overwrite_source_run",
        "DELETE FROM",
        "MERGE INTO",
    ]:
        assert expected_text in notebook_text
