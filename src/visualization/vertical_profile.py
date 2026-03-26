"""Vertical profile helpers for EDDF airspace cross-section visualizations."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


NM_PER_DEG_LATITUDE = 60.0


def signed_axis_distance_nm(
    *,
    longitude: float,
    latitude: float,
    origin_longitude: float,
    origin_latitude: float,
    axis: str = "east_west",
    axis_start_longitude: float | None = None,
    axis_start_latitude: float | None = None,
    axis_end_longitude: float | None = None,
    axis_end_latitude: float | None = None,
) -> float:
    """Project a point onto a simple axis or an arbitrary runway-aligned axis."""
    axis_name = axis.strip().lower()

    if axis_name == "east_west":
        lon_nm_per_degree = NM_PER_DEG_LATITUDE * math.cos(math.radians(origin_latitude))
        return (longitude - origin_longitude) * lon_nm_per_degree

    if axis_name == "north_south":
        return (latitude - origin_latitude) * NM_PER_DEG_LATITUDE

    if axis_name == "runway_aligned":
        runway_vector = _axis_vector_nm(
            origin_latitude=origin_latitude,
            axis_start_longitude=axis_start_longitude,
            axis_start_latitude=axis_start_latitude,
            axis_end_longitude=axis_end_longitude,
            axis_end_latitude=axis_end_latitude,
        )
        point_dx_nm, point_dy_nm = _point_offset_nm(
            longitude=longitude,
            latitude=latitude,
            origin_longitude=origin_longitude,
            origin_latitude=origin_latitude,
        )
        return (point_dx_nm * runway_vector[0]) + (point_dy_nm * runway_vector[1])

    raise ValueError("axis must be one of east_west, north_south, or runway_aligned")


def build_vertical_profile_frame(
    states_df: pd.DataFrame,
    *,
    origin_longitude: float,
    origin_latitude: float,
    axis: str = "east_west",
    distance_bin_nm: float = 5.0,
    axis_start_longitude: float | None = None,
    axis_start_latitude: float | None = None,
    axis_end_longitude: float | None = None,
    axis_end_latitude: float | None = None,
) -> pd.DataFrame:
    """Aggregate state rows into a vertical cross-section grid."""
    if distance_bin_nm <= 0:
        raise ValueError("distance_bin_nm must be positive")

    required_columns = {
        "longitude",
        "latitude",
        "icao24",
        "cell_id",
        "min_altitude_ft",
        "max_altitude_ft",
        "altitude_ft",
    }
    missing_columns = sorted(required_columns - set(states_df.columns))
    if missing_columns:
        raise ValueError(f"states_df is missing required columns: {missing_columns}")

    working_df = states_df.copy()
    if working_df.empty:
        return pd.DataFrame(
            columns=[
                "distance_bin_center_nm",
                "distance_bin_start_nm",
                "distance_bin_end_nm",
                "altitude_band_mid_ft",
                "min_altitude_ft",
                "max_altitude_ft",
                "aircraft_count",
                "active_cell_count",
                "avg_altitude_ft",
            ]
        )

    working_df["signed_distance_nm"] = working_df.apply(
        lambda row: signed_axis_distance_nm(
            longitude=float(row["longitude"]),
            latitude=float(row["latitude"]),
            origin_longitude=origin_longitude,
            origin_latitude=origin_latitude,
            axis=axis,
            axis_start_longitude=axis_start_longitude,
            axis_start_latitude=axis_start_latitude,
            axis_end_longitude=axis_end_longitude,
            axis_end_latitude=axis_end_latitude,
        ),
        axis=1,
    )
    working_df["distance_bin_index"] = np.floor(working_df["signed_distance_nm"] / distance_bin_nm).astype(int)
    working_df["distance_bin_start_nm"] = working_df["distance_bin_index"] * distance_bin_nm
    working_df["distance_bin_end_nm"] = working_df["distance_bin_start_nm"] + distance_bin_nm
    working_df["distance_bin_center_nm"] = working_df["distance_bin_start_nm"] + (distance_bin_nm / 2.0)
    working_df["altitude_band_mid_ft"] = (
        working_df["min_altitude_ft"].astype(float) + working_df["max_altitude_ft"].astype(float)
    ) / 2.0

    grouped_df = (
        working_df.groupby(
            [
                "distance_bin_center_nm",
                "distance_bin_start_nm",
                "distance_bin_end_nm",
                "altitude_band_mid_ft",
                "min_altitude_ft",
                "max_altitude_ft",
            ],
            dropna=False,
        )
        .agg(
            aircraft_count=("icao24", "nunique"),
            active_cell_count=("cell_id", "nunique"),
            avg_altitude_ft=("altitude_ft", "mean"),
        )
        .reset_index()
        .sort_values(["altitude_band_mid_ft", "distance_bin_center_nm"], ascending=[True, True])
    )

    return grouped_df


def build_vertical_profile_matrix(
    profile_df: pd.DataFrame,
    *,
    value_column: str = "aircraft_count",
) -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """Return a pivoted profile table and its sorted axis coordinates."""
    if value_column not in profile_df.columns:
        raise ValueError(f"value_column '{value_column}' is not present in profile_df")

    if profile_df.empty:
        empty = pd.DataFrame()
        return empty, np.array([]), np.array([])

    pivot_df = (
        profile_df.pivot_table(
            index="altitude_band_mid_ft",
            columns="distance_bin_center_nm",
            values=value_column,
            aggfunc="sum",
            fill_value=0.0,
        )
        .sort_index(axis=0)
        .sort_index(axis=1)
    )

    return pivot_df, pivot_df.columns.to_numpy(dtype=float), pivot_df.index.to_numpy(dtype=float)


def vertical_profile_axis_label(axis: str, *, runway_id: str | None = None) -> str:
    """Human-readable axis label for a selected profile projection."""
    axis_name = axis.strip().lower()
    if axis_name == "east_west":
        return "Signed Distance From EDDF Along East-West Axis (NM)"
    if axis_name == "north_south":
        return "Signed Distance From EDDF Along North-South Axis (NM)"
    if axis_name == "runway_aligned":
        runway_text = runway_id or "Selected Runway"
        return f"Signed Distance Along {runway_text} Axis (NM)"
    raise ValueError("axis must be one of east_west, north_south, or runway_aligned")


def summarize_vertical_profile(profile_df: pd.DataFrame) -> dict[str, Any]:
    """Return lightweight diagnostics for a computed profile frame."""
    if profile_df.empty:
        return {
            "profile_rows": 0,
            "distance_bins": 0,
            "altitude_bands": 0,
            "peak_aircraft_count": 0,
        }

    return {
        "profile_rows": int(len(profile_df)),
        "distance_bins": int(profile_df["distance_bin_center_nm"].nunique()),
        "altitude_bands": int(profile_df["altitude_band_mid_ft"].nunique()),
        "peak_aircraft_count": int(profile_df["aircraft_count"].max()),
    }


def _point_offset_nm(
    *,
    longitude: float,
    latitude: float,
    origin_longitude: float,
    origin_latitude: float,
) -> tuple[float, float]:
    lon_nm_per_degree = NM_PER_DEG_LATITUDE * math.cos(math.radians(origin_latitude))
    dx_nm = (longitude - origin_longitude) * lon_nm_per_degree
    dy_nm = (latitude - origin_latitude) * NM_PER_DEG_LATITUDE
    return dx_nm, dy_nm


def _axis_vector_nm(
    *,
    origin_latitude: float,
    axis_start_longitude: float | None,
    axis_start_latitude: float | None,
    axis_end_longitude: float | None,
    axis_end_latitude: float | None,
) -> tuple[float, float]:
    if None in {axis_start_longitude, axis_start_latitude, axis_end_longitude, axis_end_latitude}:
        raise ValueError("runway_aligned axis requires axis start and end coordinates")

    lon_nm_per_degree = NM_PER_DEG_LATITUDE * math.cos(math.radians(origin_latitude))
    dx_nm = (float(axis_end_longitude) - float(axis_start_longitude)) * lon_nm_per_degree
    dy_nm = (float(axis_end_latitude) - float(axis_start_latitude)) * NM_PER_DEG_LATITUDE
    magnitude = math.hypot(dx_nm, dy_nm)
    if magnitude == 0:
        raise ValueError("runway_aligned axis start and end points must differ")
    return dx_nm / magnitude, dy_nm / magnitude
