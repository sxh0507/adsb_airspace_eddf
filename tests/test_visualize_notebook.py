from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_visualize_notebook_contains_visual_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "04_visualize_results.ipynb").read_text()

    for expected_text in [
        "04 Visualize Results",
        "gld_airspace.grid_complexity_5m",
        "gld_airspace.complexity_hotspots",
        "gld_airspace.complexity_trend_15m",
        "ref.grid_cells",
        "matplotlib",
        "top_n",
        "Complexity Heatmap",
        "15-minute Complexity Trend",
        "Top ",
    ]:
        assert expected_text in notebook_text
