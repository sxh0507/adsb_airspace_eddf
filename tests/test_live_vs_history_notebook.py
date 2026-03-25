from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_live_vs_history_notebook_contains_baseline_compare_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "03c_compare_live_vs_history_baseline.ipynb").read_text()

    for expected_text in [
        "03c Compare Live Vs History Baseline",
        "03b_build_live_complexity_metrics",
        "03_build_complexity_metrics",
        "gld_airspace.horizontal_complexity_v2",
        "gld_airspace.horizontal_hotspots_v2",
        "gld_airspace.complexity_trend_v2",
        "trend_compare_df",
        "horizontal_alerts_df",
        "hotspot_compare_df",
        "min_weekday_baseline_samples",
        "slot_all_days",
        "weekday_slot",
        "complexity_delta_from_p50",
        "new_hotspot_flag",
    ]:
        assert expected_text in notebook_text
