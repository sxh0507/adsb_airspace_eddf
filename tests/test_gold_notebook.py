from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_gold_notebook_contains_gold_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "03_build_complexity_metrics.ipynb").read_text()

    for expected_text in [
        "03 Build Complexity Metrics",
        "gld_airspace.grid_complexity_5m",
        "gld_airspace.complexity_hotspots",
        "gld_airspace.complexity_trend_15m",
        "obs.pipeline_run_log",
        "aircraft_count",
        "heading_dispersion",
        "altitude_dispersion",
        "speed_dispersion",
        "percentile_approx",
        "dense_rank",
        "DELETE FROM",
        "03_build_complexity_metrics",
    ]:
        assert expected_text in notebook_text
