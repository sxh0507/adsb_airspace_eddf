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
WEATHER_TOKEN_RE = re.compile(
    r"^(?:VC)?(?:-|\+)?(?:MI|PR|BC|DR|BL|SH|TS|FZ)?"
    r"(?:DZ|RA|SN|SG|IC|PL|GR|GS|UP|BR|FG|FU|VA|DU|SA|HZ|PY|PO|SQ|FC|SS|DS)+$"
)
CLOUD_TOKEN_RE = re.compile(r"^(FEW|SCT|BKN|OVC|VV)(\d{3}|///)(CB|TCU|ACC)?$")


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


def _meters_to_sm(meters: float) -> float:
    return meters / 1609.344


def _parse_sm_token(token: str, previous_token: str | None = None) -> float | None:
    text = token.strip().upper()
    if not text.endswith("SM"):
        return None

    body = text[:-2]
    if body.startswith("P"):
        body = body[1:]
    if body.startswith("M"):
        body = body[1:]

    if previous_token and previous_token.isdigit() and "/" in body:
        numerator, denominator = body.split("/", 1)
        if numerator.isdigit() and denominator.isdigit() and int(denominator) != 0:
            return float(previous_token) + (int(numerator) / int(denominator))

    if "/" in body:
        numerator, denominator = body.split("/", 1)
        if numerator.isdigit() and denominator.isdigit() and int(denominator) != 0:
            return int(numerator) / int(denominator)

    return _coerce_float(body)


def _raw_tokens(raw_text: str | None) -> list[str]:
    if not raw_text:
        return []
    return [token.strip() for token in str(raw_text).split() if token.strip()]


def _extract_visibility_sm(record: dict[str, Any], *, raw_text: str | None) -> float | None:
    for key in ("visib", "visibility", "visibilitySm", "visibilitySM", "visib_sm"):
        parsed = _coerce_float(record.get(key))
        if parsed is not None:
            return parsed

    tokens = _raw_tokens(raw_text)
    for idx, token in enumerate(tokens):
        normalized = token.upper()
        if normalized == "CAVOK":
            return _meters_to_sm(10000.0)
        if normalized.endswith("SM"):
            previous = tokens[idx - 1] if idx > 0 else None
            parsed = _parse_sm_token(normalized, previous)
            if parsed is not None:
                return parsed
        if re.fullmatch(r"\d{4}", normalized):
            return _meters_to_sm(float(normalized))
    return None


def _extract_cloud_layers(record: dict[str, Any], *, raw_text: str | None) -> list[dict[str, Any]]:
    for key in ("clouds", "cloudLayers", "skyConditions"):
        value = record.get(key)
        if isinstance(value, list):
            return [layer for layer in value if isinstance(layer, dict)]

    layers: list[dict[str, Any]] = []
    for token in _raw_tokens(raw_text):
        match = CLOUD_TOKEN_RE.match(token.upper())
        if not match:
            continue
        cover, base_hundreds, suffix = match.groups()
        layer: dict[str, Any] = {"cover": cover}
        if base_hundreds != "///":
            layer["base"] = int(base_hundreds) * 100
        if suffix:
            layer["cloudType"] = suffix
        layers.append(layer)
    return layers


def _extract_weather_string(record: dict[str, Any], *, raw_text: str | None) -> str | None:
    for key in ("wxString", "weatherString", "wx", "presentWeather"):
        value = record.get(key)
        if value not in (None, ""):
            return str(value).strip()

    tokens = _raw_tokens(raw_text)
    if any(token.upper() == "CAVOK" for token in tokens):
        return "CAVOK"

    weather_tokens: list[str] = []
    for token in tokens:
        normalized = token.upper()
        if WEATHER_TOKEN_RE.fullmatch(normalized):
            weather_tokens.append(normalized)
    if weather_tokens:
        return " ".join(weather_tokens)
    return None


def _derive_flight_category(*, visibility_sm: float | None, ceiling_ft_agl: int | None, raw_text: str | None) -> str | None:
    if raw_text and "CAVOK" in raw_text.upper():
        return "VFR"
    if visibility_sm is None and ceiling_ft_agl is None:
        return None

    if visibility_sm is not None and visibility_sm < 1.0:
        return "LIFR"
    if ceiling_ft_agl is not None and ceiling_ft_agl < 500:
        return "LIFR"
    if visibility_sm is not None and visibility_sm < 3.0:
        return "IFR"
    if ceiling_ft_agl is not None and ceiling_ft_agl < 1000:
        return "IFR"
    if visibility_sm is not None and visibility_sm <= 5.0:
        return "MVFR"
    if ceiling_ft_agl is not None and ceiling_ft_agl <= 3000:
        return "MVFR"
    return "VFR"


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

        raw_text = record.get("rawOb") or record.get("raw_text")
        clouds = _extract_cloud_layers(record, raw_text=raw_text)
        visibility_sm = _extract_visibility_sm(record, raw_text=raw_text)
        weather_string = _extract_weather_string(record, raw_text=raw_text)
        ceiling_ft_agl = extract_ceiling_ft_agl(clouds)
        flight_category = (
            str(record.get("fltCat") or record.get("flightCategory") or "").strip().upper() or None
        )
        if flight_category is None:
            flight_category = _derive_flight_category(
                visibility_sm=visibility_sm,
                ceiling_ft_agl=ceiling_ft_agl,
                raw_text=raw_text,
            )

        rows.append(
            {
                "report_date": observation_time.date(),
                "station_id": station_id,
                "observation_time": observation_time.replace(tzinfo=None),
                "raw_text": raw_text,
                "report_type": record.get("metarType") or record.get("reportType"),
                "temperature_c": _coerce_float(record.get("temp")),
                "dewpoint_c": _coerce_float(record.get("dewp")),
                "wind_direction_deg": _coerce_int(record.get("wdir")),
                "wind_speed_kt": _coerce_float(record.get("wspd")),
                "wind_gust_kt": _coerce_float(record.get("wgst")),
                "visibility_sm": visibility_sm,
                "altimeter_in_hg": _coerce_float(record.get("altim")),
                "sea_level_pressure_mb": _coerce_float(record.get("slp")),
                "flight_category": flight_category,
                "weather_string": weather_string,
                "ceiling_ft_agl": ceiling_ft_agl,
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
