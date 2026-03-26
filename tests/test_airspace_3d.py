from __future__ import annotations

import pandas as pd

from src.visualization.airspace_3d import build_airspace_structure_frame, summarize_airspace_structure


def test_build_airspace_structure_frame_groups_rows_per_cell() -> None:
    states_df = pd.DataFrame(
        [
            {
                "cell_id": "fra_h_001_001_z_001",
                "horizontal_cell_id": "fra_h_001_001",
                "vertical_cell_id": "z_001",
                "icao24": "a1",
                "event_time": "2026-03-08T10:30:00",
                "center_longitude": 8.55,
                "center_latitude": 50.02,
                "min_altitude_ft": 0.0,
                "max_altitude_ft": 3000.0,
                "altitude_ft": 1500.0,
            },
            {
                "cell_id": "fra_h_001_001_z_001",
                "horizontal_cell_id": "fra_h_001_001",
                "vertical_cell_id": "z_001",
                "icao24": "a2",
                "event_time": "2026-03-08T10:31:00",
                "center_longitude": 8.55,
                "center_latitude": 50.02,
                "min_altitude_ft": 0.0,
                "max_altitude_ft": 3000.0,
                "altitude_ft": 1800.0,
            },
            {
                "cell_id": "fra_h_001_002_z_002",
                "horizontal_cell_id": "fra_h_001_002",
                "vertical_cell_id": "z_002",
                "icao24": "a3",
                "event_time": "2026-03-08T10:31:00",
                "center_longitude": 8.60,
                "center_latitude": 50.04,
                "min_altitude_ft": 3000.0,
                "max_altitude_ft": 6000.0,
                "altitude_ft": 4200.0,
            },
        ]
    )

    structure_df = build_airspace_structure_frame(states_df)

    assert list(structure_df["cell_id"]) == ["fra_h_001_001_z_001", "fra_h_001_002_z_002"]
    assert list(structure_df["aircraft_count"]) == [2, 1]
    assert list(structure_df["state_count"]) == [2, 1]
    assert list(structure_df["event_count"]) == [2, 1]
    assert list(structure_df["altitude_band_mid_ft"]) == [1500.0, 4500.0]


def test_summarize_airspace_structure_reports_expected_counts() -> None:
    structure_df = pd.DataFrame(
        [
            {
                "cell_id": "c1",
                "horizontal_cell_id": "h1",
                "vertical_cell_id": "z1",
                "aircraft_count": 3,
            },
            {
                "cell_id": "c2",
                "horizontal_cell_id": "h1",
                "vertical_cell_id": "z2",
                "aircraft_count": 1,
            },
            {
                "cell_id": "c3",
                "horizontal_cell_id": "h2",
                "vertical_cell_id": "z1",
                "aircraft_count": 2,
            },
        ]
    )

    summary = summarize_airspace_structure(structure_df)

    assert summary == {
        "structure_rows": 3,
        "active_cells": 3,
        "active_horizontal_cells": 2,
        "vertical_layers": 2,
        "peak_aircraft_count": 3,
    }
