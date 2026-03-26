"""Reusable basemap assets and plotting helpers for EDDF visualizations."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any, Iterator


REPO_ROOT = Path(__file__).resolve().parents[2]
BASEMAP_ROOT = REPO_ROOT / "data" / "reference" / "basemaps"
DEFAULT_BASEMAP_NAME = "eddf_simplified_basemap_v1"

DEFAULT_STYLE = {
    "edgecolor": "#555555",
    "facecolor": "none",
    "linewidth": 1.0,
    "linestyle": "-",
    "alpha": 0.9,
    "zorder": 2,
    "marker": "o",
    "s": 24,
    "annotate": False,
    "text_offset": (0.01, 0.01),
    "text_color": "#333333",
}

LAYER_STYLES = {
    "airport_point": {
        "edgecolor": "#b2182b",
        "facecolor": "#b2182b",
        "marker": "o",
        "s": 50,
        "annotate": True,
        "text_offset": (0.015, 0.012),
        "text_color": "#6a0f1a",
        "zorder": 5,
    },
    "runway_centerline": {
        "edgecolor": "#222222",
        "linewidth": 2.2,
        "alpha": 0.95,
        "zorder": 4,
    },
    "airport_reference_zone": {
        "edgecolor": "#4f6fa8",
        "facecolor": "#c7d6f2",
        "linewidth": 1.1,
        "alpha": 0.2,
        "zorder": 1,
    },
    "simplified_terminal_area": {
        "edgecolor": "#4d7c56",
        "facecolor": "#d7ead9",
        "linewidth": 1.2,
        "linestyle": "--",
        "alpha": 0.16,
        "zorder": 0,
    },
    "tma_boundary": {
        "edgecolor": "#3f7f93",
        "facecolor": "#d9edf3",
        "linewidth": 1.3,
        "linestyle": "-.",
        "alpha": 0.1,
        "zorder": 0,
    },
    "ctr_boundary": {
        "edgecolor": "#2f6d57",
        "facecolor": "#dcefe4",
        "linewidth": 1.35,
        "linestyle": "--",
        "alpha": 0.12,
        "zorder": 1,
    },
    "terminal_corridor": {
        "edgecolor": "#7a5c1f",
        "linewidth": 1.6,
        "linestyle": ":",
        "alpha": 0.8,
        "zorder": 3,
    },
    "waypoint": {
        "edgecolor": "#5f4b8b",
        "facecolor": "#5f4b8b",
        "marker": "o",
        "s": 24,
        "annotate": True,
        "text_offset": (0.012, 0.012),
        "text_color": "#4c3d6d",
        "zorder": 5,
    },
}


def list_available_basemaps() -> list[str]:
    """Return available basemap asset names without file extensions."""
    return sorted(path.stem for path in BASEMAP_ROOT.glob("*.geojson"))


def get_basemap_path(name: str = DEFAULT_BASEMAP_NAME) -> Path:
    """Resolve a basemap asset path by logical name or filename."""
    stem = name[:-8] if name.endswith(".geojson") else name
    path = BASEMAP_ROOT / f"{stem}.geojson"
    if not path.exists():
        raise FileNotFoundError(f"Basemap asset not found: {path}")
    return path


def load_basemap_geojson(name: str = DEFAULT_BASEMAP_NAME) -> dict[str, Any]:
    """Load a GeoJSON basemap asset and validate the top-level contract."""
    with get_basemap_path(name).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if payload.get("type") != "FeatureCollection":
        raise ValueError("Basemap asset must be a GeoJSON FeatureCollection.")

    features = payload.get("features")
    if not isinstance(features, list):
        raise ValueError("Basemap asset must define a 'features' list.")

    return payload


def iter_features(
    basemap_geojson: dict[str, Any],
    *,
    layer_kind: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield features from a basemap, optionally filtered by layer kind."""
    for feature in basemap_geojson.get("features", []):
        current_kind = feature.get("properties", {}).get("layer_kind")
        if layer_kind is None or current_kind == layer_kind:
            yield feature


def filter_basemap_layers(
    basemap_geojson: dict[str, Any],
    *,
    layer_kinds: set[str] | None = None,
) -> dict[str, Any]:
    """Return a shallow-cloned basemap containing only selected layers."""
    if layer_kinds is None:
        return deepcopy(basemap_geojson)

    filtered = deepcopy(basemap_geojson)
    filtered["features"] = [
        feature
        for feature in basemap_geojson.get("features", [])
        if feature.get("properties", {}).get("layer_kind") in layer_kinds
    ]
    return filtered


def first_feature(
    basemap_geojson: dict[str, Any],
    *,
    layer_kind: str | None = None,
    feature_id: str | None = None,
    property_name: str | None = None,
    property_value: Any | None = None,
) -> dict[str, Any] | None:
    """Return the first feature that matches the requested filter."""
    for feature in basemap_geojson.get("features", []):
        properties = feature.get("properties", {})
        if layer_kind is not None and properties.get("layer_kind") != layer_kind:
            continue
        if feature_id is not None and properties.get("feature_id") != feature_id:
            continue
        if property_name is not None and properties.get(property_name) != property_value:
            continue
        return feature
    return None


def feature_point_coordinates(feature: dict[str, Any]) -> tuple[float, float]:
    """Return longitude, latitude for a point feature."""
    geometry = feature.get("geometry", {})
    if geometry.get("type") != "Point":
        raise ValueError("feature_point_coordinates expects a Point geometry")
    coordinates = geometry.get("coordinates", [])
    if len(coordinates) < 2:
        raise ValueError("Point geometry must include longitude and latitude")
    return float(coordinates[0]), float(coordinates[1])


def feature_line_endpoints(feature: dict[str, Any]) -> tuple[tuple[float, float], tuple[float, float]]:
    """Return the first and last coordinates of a LineString feature."""
    geometry = feature.get("geometry", {})
    if geometry.get("type") != "LineString":
        raise ValueError("feature_line_endpoints expects a LineString geometry")
    coordinates = geometry.get("coordinates", [])
    if len(coordinates) < 2:
        raise ValueError("LineString geometry must include at least two coordinates")
    start = coordinates[0]
    end = coordinates[-1]
    return (float(start[0]), float(start[1])), (float(end[0]), float(end[1]))


def feature_coordinate_paths(feature: dict[str, Any]) -> list[list[tuple[float, float]]]:
    """Return drawable coordinate paths for LineString and Polygon-like features."""
    geometry = feature.get("geometry", {})
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates", [])

    if geometry_type == "Point":
        if len(coordinates) < 2:
            raise ValueError("Point geometry must include longitude and latitude")
        return [[(float(coordinates[0]), float(coordinates[1]))]]

    if geometry_type == "LineString":
        return [[(float(coord[0]), float(coord[1])) for coord in coordinates if len(coord) >= 2]]

    if geometry_type == "MultiLineString":
        return [
            [(float(coord[0]), float(coord[1])) for coord in part if len(coord) >= 2]
            for part in coordinates
        ]

    if geometry_type == "Polygon":
        return [
            [(float(coord[0]), float(coord[1])) for coord in ring if len(coord) >= 2]
            for ring in coordinates
        ]

    if geometry_type == "MultiPolygon":
        paths: list[list[tuple[float, float]]] = []
        for polygon in coordinates:
            for ring in polygon:
                paths.append([(float(coord[0]), float(coord[1])) for coord in ring if len(coord) >= 2])
        return paths

    raise ValueError(f"feature_coordinate_paths does not support geometry type: {geometry_type}")


def default_layer_style(layer_kind: str | None) -> dict[str, Any]:
    """Return a copy of the default drawing style for a given layer."""
    style = deepcopy(DEFAULT_STYLE)
    if layer_kind is not None:
        style.update(deepcopy(LAYER_STYLES.get(layer_kind, {})))
    return style


def feature_bounds(feature: dict[str, Any]) -> tuple[float, float, float, float]:
    """Compute min_lon, min_lat, max_lon, max_lat for a single feature."""
    coordinates = list(_iter_xy(feature.get("geometry", {})))
    if not coordinates:
        raise ValueError("Feature has no drawable coordinates.")

    longitudes = [coord[0] for coord in coordinates]
    latitudes = [coord[1] for coord in coordinates]
    return (min(longitudes), min(latitudes), max(longitudes), max(latitudes))


def collection_bounds(basemap_geojson: dict[str, Any]) -> tuple[float, float, float, float]:
    """Compute overall min_lon, min_lat, max_lon, max_lat for a basemap."""
    feature_extents = [feature_bounds(feature) for feature in basemap_geojson.get("features", [])]
    if not feature_extents:
        raise ValueError("Basemap has no features.")

    min_lon = min(extent[0] for extent in feature_extents)
    min_lat = min(extent[1] for extent in feature_extents)
    max_lon = max(extent[2] for extent in feature_extents)
    max_lat = max(extent[3] for extent in feature_extents)
    return (min_lon, min_lat, max_lon, max_lat)


def draw_basemap(
    ax,
    basemap_geojson: dict[str, Any],
    *,
    layer_kinds: set[str] | None = None,
    style_overrides: dict[str, dict[str, Any]] | None = None,
    annotate: bool = True,
) -> None:
    """Draw basemap features on a matplotlib axis without geo dependencies."""
    selected = filter_basemap_layers(basemap_geojson, layer_kinds=layer_kinds)
    overrides = style_overrides or {}

    for feature in selected.get("features", []):
        layer_kind = feature.get("properties", {}).get("layer_kind")
        style = default_layer_style(layer_kind)
        style.update(overrides.get(layer_kind, {}))
        _draw_feature(ax, feature, style=style, annotate=annotate)


def _iter_xy(geometry: dict[str, Any]) -> Iterator[tuple[float, float]]:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")

    if geometry_type == "Point":
        if len(coordinates) >= 2:
            yield (float(coordinates[0]), float(coordinates[1]))
        return

    if geometry_type in {"LineString", "MultiPoint"}:
        for coordinate in coordinates or []:
            if len(coordinate) >= 2:
                yield (float(coordinate[0]), float(coordinate[1]))
        return

    if geometry_type in {"Polygon", "MultiLineString"}:
        for part in coordinates or []:
            for coordinate in part:
                if len(coordinate) >= 2:
                    yield (float(coordinate[0]), float(coordinate[1]))
        return

    if geometry_type == "MultiPolygon":
        for polygon in coordinates or []:
            for ring in polygon:
                for coordinate in ring:
                    if len(coordinate) >= 2:
                        yield (float(coordinate[0]), float(coordinate[1]))
        return

    raise ValueError(f"Unsupported geometry type: {geometry_type}")


def _draw_feature(ax, feature: dict[str, Any], *, style: dict[str, Any], annotate: bool) -> None:
    geometry = feature.get("geometry", {})
    geometry_type = geometry.get("type")
    properties = feature.get("properties", {})
    label = properties.get("label")

    if geometry_type == "Point":
        longitude, latitude = next(_iter_xy(geometry))
        ax.scatter(
            [longitude],
            [latitude],
            color=style.get("facecolor", style.get("edgecolor")),
            marker=style.get("marker", "o"),
            s=style.get("s", 24),
            alpha=style.get("alpha", 1.0),
            zorder=style.get("zorder", 2),
        )
        if annotate and style.get("annotate") and label:
            offset_lon, offset_lat = style.get("text_offset", (0.01, 0.01))
            ax.text(
                longitude + offset_lon,
                latitude + offset_lat,
                label,
                fontsize=9,
                color=style.get("text_color", "#333333"),
                zorder=style.get("zorder", 2) + 1,
            )
        return

    if geometry_type == "LineString":
        _draw_line(ax, geometry.get("coordinates", []), style=style)
        return

    if geometry_type == "MultiLineString":
        for part in geometry.get("coordinates", []):
            _draw_line(ax, part, style=style)
        return

    if geometry_type == "Polygon":
        _draw_polygon(ax, geometry.get("coordinates", []), style=style)
        return

    if geometry_type == "MultiPolygon":
        for polygon in geometry.get("coordinates", []):
            _draw_polygon(ax, polygon, style=style)
        return

    raise ValueError(f"Unsupported geometry type for drawing: {geometry_type}")


def _draw_line(ax, coordinates: list[list[float]], *, style: dict[str, Any]) -> None:
    longitudes = [coordinate[0] for coordinate in coordinates]
    latitudes = [coordinate[1] for coordinate in coordinates]
    ax.plot(
        longitudes,
        latitudes,
        color=style.get("edgecolor", "#555555"),
        linewidth=style.get("linewidth", 1.0),
        linestyle=style.get("linestyle", "-"),
        alpha=style.get("alpha", 1.0),
        zorder=style.get("zorder", 2),
    )


def _draw_polygon(ax, coordinates: list[list[list[float]]], *, style: dict[str, Any]) -> None:
    if not coordinates:
        return

    outer_ring = coordinates[0]
    if style.get("facecolor", "none") != "none":
        ax.fill(
            [coordinate[0] for coordinate in outer_ring],
            [coordinate[1] for coordinate in outer_ring],
            facecolor=style.get("facecolor"),
            alpha=style.get("alpha", 1.0),
            zorder=style.get("zorder", 2),
            linewidth=0,
        )

    for ring in coordinates:
        ax.plot(
            [coordinate[0] for coordinate in ring],
            [coordinate[1] for coordinate in ring],
            color=style.get("edgecolor", "#555555"),
            linewidth=style.get("linewidth", 1.0),
            linestyle=style.get("linestyle", "-"),
            alpha=style.get("alpha", 1.0),
            zorder=style.get("zorder", 2),
        )
