from __future__ import annotations

from datetime import datetime, timezone
import json

from src.weather.awc_metar_client import (
    build_metar_url,
    extract_ceiling_ft_agl,
    normalize_metar_records,
    parse_awc_timestamp,
)


UTC = timezone.utc


def test_build_metar_url_contains_station_format_and_hours() -> None:
    url = build_metar_url(
        base_url="https://aviationweather.gov/api/data",
        station_id="eddf",
        hours=12,
    )

    assert url == "https://aviationweather.gov/api/data/metar?ids=EDDF&format=json&hours=12"


def test_extract_ceiling_ft_agl_uses_lowest_broken_overcast_or_vertical_visibility() -> None:
    cloud_layers = [
        {"cover": "FEW", "base": 1400},
        {"cover": "BKN", "base": 3200},
        {"cover": "OVC", "base": 2100},
    ]

    assert extract_ceiling_ft_agl(cloud_layers) == 2100
    assert extract_ceiling_ft_agl([{"cover": "VV", "base": 600}]) == 600


def test_parse_awc_timestamp_supports_iso8601_zulu() -> None:
    parsed = parse_awc_timestamp("2026-03-25T14:20:00Z")

    assert parsed == datetime(2026, 3, 25, 14, 20, tzinfo=UTC)


def test_normalize_metar_records_maps_awc_fields_to_bronze_schema() -> None:
    rows = normalize_metar_records(
        records=[
            {
                "icaoId": "EDDF",
                "obsTime": "2026-03-25T14:20:00Z",
                "rawOb": "EDDF 251420Z 24012KT 9999 FEW020 BKN040 11/05 Q1015",
                "metarType": "METAR",
                "temp": 11.0,
                "dewp": 5.0,
                "wdir": 240,
                "wspd": 12,
                "wgst": 20,
                "visib": 6.0,
                "altim": 29.97,
                "slp": 1015.2,
                "fltCat": "VFR",
                "wxString": "-RA",
                "lat": 50.033,
                "lon": 8.57,
                "elev": 111,
                "clouds": [
                    {"cover": "FEW", "base": 2000},
                    {"cover": "BKN", "base": 4000},
                ],
            }
        ],
        run_id="weather_ingest_test",
        ingested_at=datetime(2026, 3, 25, 14, 30, tzinfo=UTC),
        source_request_start=datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
        source_request_end=datetime(2026, 3, 25, 15, 0, tzinfo=UTC),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["station_id"] == "EDDF"
    assert row["report_date"].isoformat() == "2026-03-25"
    assert row["observation_time"] == datetime(2026, 3, 25, 14, 20)
    assert row["ceiling_ft_agl"] == 4000
    assert row["flight_category"] == "VFR"
    assert json.loads(row["cloud_layers_json"])[1]["cover"] == "BKN"
    assert row["run_id"] == "weather_ingest_test"
