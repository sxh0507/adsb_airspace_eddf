from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_visualize_notebook_contains_visual_contract() -> None:
    notebook_text = (REPO_ROOT / "notebooks" / "04_visualize_results.ipynb").read_text()

    for expected_text in [
        "04 Visualize Results",
        "horizontal_complexity_v2",
        "horizontal_hotspots_v2",
        "complexity_trend_v2",
        "ref.airspace_cells_v2",
        "cell_scheme_id",
        "top_n",
        "load_basemap_geojson",
        "draw_basemap",
        "show_basemap",
        "basemap_name",
        "build_vertical_profile_frame",
        "show_vertical_profile",
        "vertical_profile_axis",
        "Frankfurt Vertical Profile (V2)",
        "Frankfurt Horizontal Complexity Heatmap (V2)",
        "Frankfurt 15-minute Complexity Trend (V2)",
        "Top ",
    ]:
        assert expected_text in notebook_text
