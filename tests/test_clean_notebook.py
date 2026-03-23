from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_clean_notebook_contains_v2_cellization_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "02_clean_and_prepare_states.ipynb").read_text()

    for expected_text in [
        "02 Clean And Prepare States",
        "slv_airspace.flight_states_cellized_v2",
        "ref.cell_schemes_v2",
        "ref.airspace_cells_v2",
        "obs.pipeline_run_log",
        "cell_scheme_id",
        "horizontal_cell_nm",
        "vertical_cell_ft",
        "analysis_window_start",
        "horizontal_cell_id",
        "vertical_cell_id",
        "projection_mode",
        "DELETE FROM",
        "local_nm",
    ]:
        assert expected_text in notebook_text
