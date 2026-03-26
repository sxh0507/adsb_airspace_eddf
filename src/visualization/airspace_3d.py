"""Helpers for 3D cell-based EDDF airspace structure views."""

from __future__ import annotations

from typing import Any

import pandas as pd


def build_airspace_structure_frame(states_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate windowed state rows into one row per active 3D cell."""
    required_columns = {
        "cell_id",
        "horizontal_cell_id",
        "vertical_cell_id",
        "icao24",
        "center_longitude",
        "center_latitude",
        "min_altitude_ft",
        "max_altitude_ft",
    }
    missing_columns = sorted(required_columns - set(states_df.columns))
    if missing_columns:
        raise ValueError(f"states_df is missing required columns: {missing_columns}")

    if states_df.empty:
        return pd.DataFrame(
            columns=[
                "cell_id",
                "horizontal_cell_id",
                "vertical_cell_id",
                "center_longitude",
                "center_latitude",
                "min_altitude_ft",
                "max_altitude_ft",
                "altitude_band_mid_ft",
                "aircraft_count",
                "state_count",
                "event_count",
                "avg_altitude_ft",
            ]
        )

    working_df = states_df.copy()
    working_df["altitude_band_mid_ft"] = (
        working_df["min_altitude_ft"].astype(float) + working_df["max_altitude_ft"].astype(float)
    ) / 2.0

    aggregations: dict[str, tuple[str, str]] = {
        "aircraft_count": ("icao24", "nunique"),
        "state_count": ("icao24", "size"),
    }
    if "event_time" in working_df.columns:
        aggregations["event_count"] = ("event_time", "nunique")
    else:
        working_df["event_time"] = "window"
        aggregations["event_count"] = ("event_time", "nunique")

    if "altitude_ft" in working_df.columns:
        aggregations["avg_altitude_ft"] = ("altitude_ft", "mean")

    grouped_df = (
        working_df.groupby(
            [
                "cell_id",
                "horizontal_cell_id",
                "vertical_cell_id",
                "center_longitude",
                "center_latitude",
                "min_altitude_ft",
                "max_altitude_ft",
                "altitude_band_mid_ft",
            ],
            dropna=False,
        )
        .agg(**aggregations)
        .reset_index()
        .sort_values(
            ["altitude_band_mid_ft", "aircraft_count", "center_longitude", "center_latitude"],
            ascending=[True, False, True, True],
        )
    )

    if "avg_altitude_ft" not in grouped_df.columns:
        grouped_df["avg_altitude_ft"] = grouped_df["altitude_band_mid_ft"]

    return grouped_df


def summarize_airspace_structure(structure_df: pd.DataFrame) -> dict[str, Any]:
    """Return lightweight diagnostics for a computed 3D airspace frame."""
    if structure_df.empty:
        return {
            "structure_rows": 0,
            "active_cells": 0,
            "active_horizontal_cells": 0,
            "vertical_layers": 0,
            "peak_aircraft_count": 0,
        }

    return {
        "structure_rows": int(len(structure_df)),
        "active_cells": int(structure_df["cell_id"].nunique()),
        "active_horizontal_cells": int(structure_df["horizontal_cell_id"].nunique()),
        "vertical_layers": int(structure_df["vertical_cell_id"].nunique()),
        "peak_aircraft_count": int(structure_df["aircraft_count"].max()),
    }
