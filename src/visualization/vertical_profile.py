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
) -> float:
    """Project a point onto a simple east-west or north-south airport-centered axis."""
    axis_name = axis.strip().lower()

    if axis_name == "east_west":
        lon_nm_per_degree = NM_PER_DEG_LATITUDE * math.cos(math.radians(origin_latitude))
        return (longitude - origin_longitude) * lon_nm_per_degree

    if axis_name == "north_south":
        return (latitude - origin_latitude) * NM_PER_DEG_LATITUDE

    raise ValueError("axis must be one of east_west or north_south")


def build_vertical_profile_frame(
    states_df: pd.DataFrame,
    *,
    origin_longitude: float,
    origin_latitude: float,
    axis: str = "east_west",
    distance_bin_nm: float = 5.0,
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


def vertical_profile_axis_label(axis: str) -> str:
    """Human-readable axis label for a selected profile projection."""
    axis_name = axis.strip().lower()
    if axis_name == "east_west":
        return "Signed Distance From EDDF Along East-West Axis (NM)"
    if axis_name == "north_south":
        return "Signed Distance From EDDF Along North-South Axis (NM)"
    raise ValueError("axis must be one of east_west or north_south")


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
