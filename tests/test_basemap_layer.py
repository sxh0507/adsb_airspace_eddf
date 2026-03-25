from __future__ import annotations

from src.visualization.basemap import (
    DEFAULT_BASEMAP_NAME,
    collection_bounds,
    default_layer_style,
    list_available_basemaps,
    load_basemap_geojson,
)


def test_default_basemap_asset_is_available() -> None:
    assert DEFAULT_BASEMAP_NAME in list_available_basemaps()


def test_eddf_basemap_contains_expected_layers() -> None:
    basemap_geojson = load_basemap_geojson()
    layer_kinds = {feature["properties"]["layer_kind"] for feature in basemap_geojson["features"]}
    runway_ids = {
        feature["properties"].get("runway_id")
        for feature in basemap_geojson["features"]
        if feature["properties"]["layer_kind"] == "runway_centerline"
    }

    assert basemap_geojson["metadata"]["focus_airport"] == "EDDF"
    assert {"airport_point", "airport_reference_zone", "simplified_terminal_area", "runway_centerline"} <= layer_kinds
    assert runway_ids == {"07L/25R", "07C/25C", "07R/25L", "18"}


def test_eddf_basemap_bounds_cover_airport_area() -> None:
    basemap_geojson = load_basemap_geojson()
    min_lon, min_lat, max_lon, max_lat = collection_bounds(basemap_geojson)

    assert min_lon < 8.57 < max_lon
    assert min_lat < 50.03 < max_lat
    assert max_lon - min_lon > 1.0
    assert max_lat - min_lat > 0.5


def test_airport_point_default_style_requests_annotation() -> None:
    style = default_layer_style("airport_point")
    assert style["annotate"] is True
    assert style["marker"] == "o"
