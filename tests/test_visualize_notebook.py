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
        "eddf_extended_basemap_v1",
        "build_airspace_structure_frame",
        "feature_coordinate_paths",
        "show_3d_airspace",
        "min_3d_aircraft_count",
        "airspace_3d_marker_scale",
        "Poly3DCollection",
        "tma_boundary",
        "ctr_boundary",
        "terminal_corridor",
        "waypoint",
        "build_vertical_profile_frame",
        "show_vertical_profile",
        "vertical_profile_axis",
        "vertical_profile_runway_id",
        "runway_aligned",
        "Frankfurt 3D Airspace Structure (V2)",
        "Frankfurt Vertical Profile (V2)",
        "Frankfurt Horizontal Complexity Heatmap (V2)",
        "Frankfurt 15-minute Complexity Trend (V2)",
        "Top ",
    ]:
        assert expected_text in notebook_text
