from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_gold_notebook_contains_v2_gold_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "03_build_complexity_metrics.ipynb").read_text()

    for expected_text in [
        "03 Build Complexity Metrics",
        "gld_airspace.horizontal_complexity_v2",
        "gld_airspace.horizontal_hotspots_v2",
        "gld_airspace.complexity_trend_v2",
        "obs.pipeline_run_log",
        "aircraft_count",
        "heading_dispersion",
        "speed_dispersion",
        "active_vertical_cells",
        "minimum_active_windows",
        "cell_scheme_id",
        "percentile_approx",
        "dense_rank",
        "DELETE FROM",
        "03_build_complexity_metrics",
    ]:
        assert expected_text in notebook_text
