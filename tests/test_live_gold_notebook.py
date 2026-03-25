from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_live_gold_notebook_contains_live_gold_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "03b_build_live_complexity_metrics.ipynb").read_text()

    for expected_text in [
        "03b Build Live Complexity Metrics",
        "02b_prepare_live_states_v2",
        "gld_airspace.horizontal_complexity_v2",
        "gld_airspace.horizontal_hotspots_v2",
        "gld_airspace.complexity_trend_v2",
        "obs.pipeline_run_log",
        "minimum_active_windows",
        "high_complexity_percentile",
        "cell_scheme_id",
        "percentile_approx",
        "dense_rank",
        "03b_build_live_complexity_metrics",
        "gold_live_v2",
    ]:
        assert expected_text in notebook_text
