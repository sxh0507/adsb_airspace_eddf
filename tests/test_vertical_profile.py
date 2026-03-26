from __future__ import annotations

import pandas as pd

from src.visualization.vertical_profile import (
    build_vertical_profile_frame,
    build_vertical_profile_matrix,
    signed_axis_distance_nm,
    summarize_vertical_profile,
    vertical_profile_axis_label,
)


def test_signed_axis_distance_nm_supports_both_axes() -> None:
    east_west = signed_axis_distance_nm(
        longitude=8.62,
        latitude=50.03,
        origin_longitude=8.57,
        origin_latitude=50.03,
        axis="east_west",
    )
    north_south = signed_axis_distance_nm(
        longitude=8.57,
        latitude=50.08,
        origin_longitude=8.57,
        origin_latitude=50.03,
        axis="north_south",
    )

    assert east_west > 0
    assert north_south > 0


def test_signed_axis_distance_nm_supports_runway_aligned_axis() -> None:
    runway_forward = signed_axis_distance_nm(
        longitude=8.60,
        latitude=50.03,
        origin_longitude=8.57,
        origin_latitude=50.03,
        axis="runway_aligned",
        axis_start_longitude=8.52,
        axis_start_latitude=50.01,
        axis_end_longitude=8.66,
        axis_end_latitude=50.04,
    )
    runway_backward = signed_axis_distance_nm(
        longitude=8.54,
        latitude=50.02,
        origin_longitude=8.57,
        origin_latitude=50.03,
        axis="runway_aligned",
        axis_start_longitude=8.52,
        axis_start_latitude=50.01,
        axis_end_longitude=8.66,
        axis_end_latitude=50.04,
    )

    assert runway_forward > 0
    assert runway_backward < 0


def test_build_vertical_profile_frame_groups_rows_into_bins() -> None:
    states_df = pd.DataFrame(
        [
            {
                "longitude": 8.55,
                "latitude": 50.03,
                "icao24": "a1",
                "cell_id": "fra_h_001_001_z_001",
                "min_altitude_ft": 0.0,
                "max_altitude_ft": 3000.0,
                "altitude_ft": 2100.0,
            },
            {
                "longitude": 8.59,
                "latitude": 50.03,
                "icao24": "a2",
                "cell_id": "fra_h_001_002_z_001",
                "min_altitude_ft": 0.0,
                "max_altitude_ft": 3000.0,
                "altitude_ft": 2500.0,
            },
            {
                "longitude": 8.62,
                "latitude": 50.03,
                "icao24": "a3",
                "cell_id": "fra_h_001_003_z_002",
                "min_altitude_ft": 3000.0,
                "max_altitude_ft": 6000.0,
                "altitude_ft": 4200.0,
            },
        ]
    )

    profile_df = build_vertical_profile_frame(
        states_df,
        origin_longitude=8.57,
        origin_latitude=50.03,
        axis="east_west",
        distance_bin_nm=5.0,
    )

    assert set(profile_df.columns) >= {
        "distance_bin_center_nm",
        "altitude_band_mid_ft",
        "aircraft_count",
        "active_cell_count",
        "avg_altitude_ft",
    }
    assert profile_df["aircraft_count"].sum() == 3


def test_build_vertical_profile_matrix_and_summary() -> None:
    profile_df = pd.DataFrame(
        [
            {
                "distance_bin_center_nm": -2.5,
                "distance_bin_start_nm": -5.0,
                "distance_bin_end_nm": 0.0,
                "altitude_band_mid_ft": 1500.0,
                "min_altitude_ft": 0.0,
                "max_altitude_ft": 3000.0,
                "aircraft_count": 2,
                "active_cell_count": 2,
                "avg_altitude_ft": 2000.0,
            },
            {
                "distance_bin_center_nm": 2.5,
                "distance_bin_start_nm": 0.0,
                "distance_bin_end_nm": 5.0,
                "altitude_band_mid_ft": 4500.0,
                "min_altitude_ft": 3000.0,
                "max_altitude_ft": 6000.0,
                "aircraft_count": 1,
                "active_cell_count": 1,
                "avg_altitude_ft": 4200.0,
            },
        ]
    )

    matrix_df, x_values, y_values = build_vertical_profile_matrix(profile_df)
    summary = summarize_vertical_profile(profile_df)

    assert matrix_df.shape == (2, 2)
    assert list(x_values) == [-2.5, 2.5]
    assert list(y_values) == [1500.0, 4500.0]
    assert summary["profile_rows"] == 2
    assert summary["peak_aircraft_count"] == 2
    assert vertical_profile_axis_label("east_west").startswith("Signed Distance")
    assert "07C/25C" in vertical_profile_axis_label("runway_aligned", runway_id="07C/25C")
