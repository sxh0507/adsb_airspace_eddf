from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from math import ceil
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


UTC = timezone.utc
DEFAULT_AWC_BASE_URL = "https://aviationweather.gov/api/data"
DEFAULT_AWC_USER_AGENT = "adsb-airspace-eddf-weather/1.0"
DEFAULT_MAX_LOOKBACK_DAYS = 15

CEILING_LAYER_CODES = {"BKN", "OVC", "OVX", "VV"}


class AviationWeatherAPIError(RuntimeError):
    """Raised when the AWC METAR endpoint cannot be queried successfully."""


@dataclass(frozen=True)
class AviationWeatherConfig:
    base_url: str = DEFAULT_AWC_BASE_URL
    station_id: str = "EDDF"
    user_agent: str = DEFAULT_AWC_USER_AGENT
    timeout_seconds: int = 30
    max_lookback_days: int = DEFAULT_MAX_LOOKBACK_DAYS


def parse_awc_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)

    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    normalized = text
    if normalized.endswith("+"):
        normalized = normalized[:-1]
    if normalized.startswith("M") and re.fullmatch(r"M\d+(\.\d+)?", normalized):
        normalized = "-" + normalized[1:]

    if re.fullmatch(r"[-+]?\d+(\.\d+)?", normalized):
        return float(normalized)

    match = re.search(r"[-+]?\d+(\.\d+)?", normalized)
    if match is None:
        return None
    return float(match.group(0))


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    parsed = _coerce_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _cloud_layer_base_ft(layer: dict[str, Any]) -> int | None:
    for key in ("base", "baseFtAgl", "altitudeFt", "heightFt", "vertVis"):
        if key not in layer:
            continue
        base = _coerce_int(layer.get(key))
        if base is not None:
            return base
    return None


def extract_ceiling_ft_agl(cloud_layers: list[dict[str, Any]] | None) -> int | None:
    if not cloud_layers:
        return None

    candidates: list[int] = []
    for layer in cloud_layers:
        coverage = str(layer.get("cover", layer.get("coverage", ""))).strip().upper()
        if coverage not in CEILING_LAYER_CODES:
            continue
        base_ft = _cloud_layer_base_ft(layer)
        if base_ft is not None:
            candidates.append(base_ft)

    if not candidates:
        return None
    return min(candidates)


def build_metar_url(*, base_url: str, station_id: str, hours: int) -> str:
    normalized_base = base_url.rstrip("/")
    query = urlencode(
        {
            "ids": station_id.strip().upper(),
            "format": "json",
            "hours": str(int(hours)),
        }
    )
    return f"{normalized_base}/metar?{query}"


def validate_awc_time_range(
    *,
    start_time: datetime,
    end_time: datetime,
    now_utc: datetime | None = None,
    max_lookback_days: int = DEFAULT_MAX_LOOKBACK_DAYS,
) -> None:
    if start_time.tzinfo is None or end_time.tzinfo is None:
        raise ValueError("start_time and end_time must be timezone-aware UTC datetimes")
    if end_time <= start_time:
        raise ValueError("end_time must be later than start_time")

    current = now_utc or datetime.now(UTC)
    oldest_supported = current - timedelta(days=max_lookback_days)
    if start_time < oldest_supported:
        raise ValueError(
            "AWC METAR API only keeps the previous "
            f"{max_lookback_days} days; requested start_time={start_time.isoformat()} "
            f"is older than supported oldest={oldest_supported.isoformat()}."
        )
    if end_time > current + timedelta(minutes=5):
        raise ValueError("end_time cannot be meaningfully later than the current UTC time")


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [record for record in data if isinstance(record, dict)]
    raise AviationWeatherAPIError("Unexpected AWC METAR JSON payload format")


def fetch_metar_records(
    *,
    config: AviationWeatherConfig,
    hours: int,
) -> list[dict[str, Any]]:
    url = build_metar_url(base_url=config.base_url, station_id=config.station_id, hours=hours)
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": config.user_agent,
        },
    )

    try:
        with urlopen(request, timeout=config.timeout_seconds) as response:
            raw_bytes = response.read()
            if not raw_bytes:
                return []
            payload = json.loads(raw_bytes.decode("utf-8"))
    except HTTPError as exc:
        if exc.code == 204:
            return []
        body = exc.read().decode("utf-8", errors="replace")
        raise AviationWeatherAPIError(f"AWC METAR API returned HTTP {exc.code}: {body}") from exc
    except URLError as exc:
        raise AviationWeatherAPIError(f"Failed to reach AWC METAR API: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise AviationWeatherAPIError(f"AWC METAR API returned invalid JSON: {exc}") from exc

    return _extract_records(payload)


def normalize_metar_records(
    *,
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
    source_request_start: datetime,
    source_request_end: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for record in records:
        station_id = str(
            record.get("icaoId")
            or record.get("stationId")
            or record.get("station")
            or ""
        ).strip().upper()
        observation_time = parse_awc_timestamp(
            record.get("obsTime") or record.get("observationTime") or record.get("reportTime")
        )
        if not station_id or observation_time is None:
            continue

        clouds = record.get("clouds")
        if not isinstance(clouds, list):
            clouds = []

        rows.append(
            {
                "report_date": observation_time.date(),
                "station_id": station_id,
                "observation_time": observation_time.replace(tzinfo=None),
                "raw_text": record.get("rawOb") or record.get("raw_text"),
                "report_type": record.get("metarType") or record.get("reportType"),
                "temperature_c": _coerce_float(record.get("temp")),
                "dewpoint_c": _coerce_float(record.get("dewp")),
                "wind_direction_deg": _coerce_int(record.get("wdir")),
                "wind_speed_kt": _coerce_float(record.get("wspd")),
                "wind_gust_kt": _coerce_float(record.get("wgst")),
                "visibility_sm": _coerce_float(record.get("visib")),
                "altimeter_in_hg": _coerce_float(record.get("altim")),
                "sea_level_pressure_mb": _coerce_float(record.get("slp")),
                "flight_category": record.get("fltCat"),
                "weather_string": record.get("wxString"),
                "ceiling_ft_agl": extract_ceiling_ft_agl(clouds),
                "latitude": _coerce_float(record.get("lat")),
                "longitude": _coerce_float(record.get("lon")),
                "elevation_m": _coerce_float(record.get("elev")),
                "cloud_layers_json": json.dumps(clouds, sort_keys=True),
                "raw_json": json.dumps(record, sort_keys=True, default=str),
                "source_request_start": source_request_start.replace(tzinfo=None),
                "source_request_end": source_request_end.replace(tzinfo=None),
                "ingested_at": ingested_at.replace(tzinfo=None),
                "run_id": run_id,
            }
        )

    return rows


def derive_query_hours(
    *,
    start_time: datetime,
    now_utc: datetime | None = None,
) -> int:
    current = now_utc or datetime.now(UTC)
    return max(1, ceil((current - start_time).total_seconds() / 3600))
