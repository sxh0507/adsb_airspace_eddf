from __future__ import annotations

import json
import os
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib import error, parse, request


UTC = timezone.utc
DEFAULT_BASE_URL = "https://opensky-network.org/api"
DEFAULT_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
DEFAULT_USER_AGENT = "adsb-airspace-eddf-live-ingestion/1.0"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_TOKEN_REFRESH_MARGIN_SECONDS = 30


class OpenSkyAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_body: str | None = None,
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body
        self.retry_after_seconds = retry_after_seconds


class OpenSkyUnauthorizedError(OpenSkyAPIError):
    pass


class OpenSkyRateLimitError(OpenSkyAPIError):
    pass


@dataclass(frozen=True)
class OpenSkyLiveConfig:
    base_url: str = DEFAULT_BASE_URL
    token_url: str = DEFAULT_TOKEN_URL
    client_id: str = ""
    client_secret: str = ""
    request_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    token_refresh_margin_seconds: int = DEFAULT_TOKEN_REFRESH_MARGIN_SECONDS
    user_agent: str = DEFAULT_USER_AGENT
    verify: bool = True


def _coalesce(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _parse_bool(value: str | bool | None, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Cannot parse boolean value from {value!r}")


def build_config(
    *,
    base_url: str | None = None,
    token_url: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    request_timeout_seconds: int | str | None = None,
    token_refresh_margin_seconds: int | str | None = None,
    user_agent: str | None = None,
    verify: bool | str | None = None,
) -> OpenSkyLiveConfig:
    resolved_base_url = _coalesce(base_url, os.getenv("OPENSKY_LIVE_BASE_URL"), DEFAULT_BASE_URL)
    resolved_token_url = _coalesce(token_url, os.getenv("OPENSKY_LIVE_TOKEN_URL"), DEFAULT_TOKEN_URL)
    resolved_client_id = _coalesce(client_id, os.getenv("OPENSKY_LIVE_CLIENT_ID"))
    resolved_client_secret = _coalesce(client_secret, os.getenv("OPENSKY_LIVE_CLIENT_SECRET"))
    resolved_timeout = int(
        _coalesce(request_timeout_seconds, os.getenv("OPENSKY_LIVE_TIMEOUT_SECONDS"), str(DEFAULT_TIMEOUT_SECONDS))
    )
    resolved_refresh_margin = int(
        _coalesce(
            token_refresh_margin_seconds,
            os.getenv("OPENSKY_LIVE_TOKEN_REFRESH_MARGIN_SECONDS"),
            str(DEFAULT_TOKEN_REFRESH_MARGIN_SECONDS),
        )
    )
    resolved_user_agent = _coalesce(user_agent, os.getenv("OPENSKY_LIVE_USER_AGENT"), DEFAULT_USER_AGENT)
    resolved_verify = _parse_bool(os.getenv("OPENSKY_LIVE_VERIFY"), default=True)
    if verify is not None:
        resolved_verify = _parse_bool(verify, default=resolved_verify)

    if not resolved_client_id:
        raise ValueError(
            "Missing OpenSky live API client_id. Provide it via notebook secrets, kwargs, or OPENSKY_LIVE_CLIENT_ID."
        )
    if not resolved_client_secret:
        raise ValueError(
            "Missing OpenSky live API client_secret. Provide it via notebook secrets, kwargs, or OPENSKY_LIVE_CLIENT_SECRET."
        )
    if resolved_timeout <= 0:
        raise ValueError("request_timeout_seconds must be positive")
    if resolved_refresh_margin < 0:
        raise ValueError("token_refresh_margin_seconds must be non-negative")

    return OpenSkyLiveConfig(
        base_url=resolved_base_url.rstrip("/"),
        token_url=resolved_token_url,
        client_id=resolved_client_id,
        client_secret=resolved_client_secret,
        request_timeout_seconds=resolved_timeout,
        token_refresh_margin_seconds=resolved_refresh_margin,
        user_agent=resolved_user_agent,
        verify=resolved_verify,
    )


def _build_ssl_context(*, verify: bool) -> ssl.SSLContext | None:
    if verify:
        return None
    return ssl._create_unverified_context()  # noqa: SLF001


class OpenSkyOAuth2TokenManager:
    def __init__(self, config: OpenSkyLiveConfig) -> None:
        self.config = config
        self._access_token: str | None = None
        self._expires_at: datetime | None = None
        self._ssl_context = _build_ssl_context(verify=config.verify)

    def invalidate(self) -> None:
        self._access_token = None
        self._expires_at = None

    def get_token(self) -> str:
        if self._access_token and self._expires_at and datetime.now(UTC) < self._expires_at:
            return self._access_token
        return self._refresh()

    def build_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get_token()}"}

    def _refresh(self) -> str:
        encoded_payload = parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            }
        ).encode("utf-8")
        token_request = request.Request(
            self.config.token_url,
            data=encoded_payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "User-Agent": self.config.user_agent,
            },
            method="POST",
        )
        try:
            with request.urlopen(
                token_request,
                timeout=self.config.request_timeout_seconds,
                context=self._ssl_context,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            raise OpenSkyUnauthorizedError(
                f"OpenSky token request failed with status {exc.code}",
                status_code=exc.code,
                response_body=response_body,
            ) from exc
        except error.URLError as exc:
            raise OpenSkyAPIError(f"OpenSky token request failed: {exc}") from exc

        access_token = str(payload.get("access_token", "")).strip()
        if not access_token:
            raise OpenSkyAPIError("OpenSky token response did not include an access_token.")

        expires_in = int(payload.get("expires_in", 1800))
        safe_expires_in = max(0, expires_in - self.config.token_refresh_margin_seconds)
        self._access_token = access_token
        self._expires_at = datetime.now(UTC) + timedelta(seconds=safe_expires_in)
        return access_token


class OpenSkyLiveClient:
    def __init__(
        self,
        config: OpenSkyLiveConfig,
        *,
        token_manager: OpenSkyOAuth2TokenManager | None = None,
    ) -> None:
        self.config = config
        self.token_manager = token_manager or OpenSkyOAuth2TokenManager(config)
        self._ssl_context = _build_ssl_context(verify=config.verify)

    def fetch_states_all(
        self,
        *,
        time: int | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        icao24: Iterable[str] | None = None,
        extended: bool = False,
        retry_on_unauthorized: bool = True,
    ) -> dict[str, Any]:
        params: list[tuple[str, str]] = []
        if time is not None:
            params.append(("time", str(int(time))))
        if bbox is not None:
            min_latitude, max_latitude, min_longitude, max_longitude = bbox
            params.extend(
                [
                    ("lamin", str(float(min_latitude))),
                    ("lamax", str(float(max_latitude))),
                    ("lomin", str(float(min_longitude))),
                    ("lomax", str(float(max_longitude))),
                ]
            )
        if icao24 is not None:
            params.extend(("icao24", str(value).strip().lower()) for value in icao24 if str(value).strip())
        if extended:
            params.append(("extended", "1"))

        url = f"{self.config.base_url}/states/all"
        if params:
            url = f"{url}?{parse.urlencode(params, doseq=True)}"

        try:
            payload = self._request_json(url, include_auth=True)
        except OpenSkyUnauthorizedError:
            if not retry_on_unauthorized:
                raise
            self.token_manager.invalidate()
            payload = self._request_json(url, include_auth=True)

        if not isinstance(payload, dict):
            raise OpenSkyAPIError("OpenSky states/all response was not a JSON object.")
        if "time" not in payload:
            raise OpenSkyAPIError("OpenSky states/all response did not include the response time.")
        if payload.get("states") is None:
            payload["states"] = []
        return payload

    def _request_json(self, url: str, *, include_auth: bool) -> dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "User-Agent": self.config.user_agent,
        }
        if include_auth:
            headers.update(self.token_manager.build_auth_header())
        http_request = request.Request(url, headers=headers, method="GET")
        try:
            with request.urlopen(
                http_request,
                timeout=self.config.request_timeout_seconds,
                context=self._ssl_context,
            ) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            retry_after_raw = exc.headers.get("X-Rate-Limit-Retry-After-Seconds") or exc.headers.get("Retry-After")
            retry_after_seconds = int(retry_after_raw) if retry_after_raw and str(retry_after_raw).isdigit() else None
            if exc.code == 401:
                raise OpenSkyUnauthorizedError(
                    f"OpenSky request failed with status {exc.code}",
                    status_code=exc.code,
                    response_body=response_body,
                    retry_after_seconds=retry_after_seconds,
                ) from exc
            if exc.code == 429:
                raise OpenSkyRateLimitError(
                    "OpenSky request was rate limited.",
                    status_code=exc.code,
                    response_body=response_body,
                    retry_after_seconds=retry_after_seconds,
                ) from exc
            raise OpenSkyAPIError(
                f"OpenSky request failed with status {exc.code}",
                status_code=exc.code,
                response_body=response_body,
                retry_after_seconds=retry_after_seconds,
            ) from exc
        except error.URLError as exc:
            raise OpenSkyAPIError(f"OpenSky request failed: {exc}") from exc


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_states_payload(
    payload: dict[str, Any],
    *,
    run_id: str,
    ingested_at: datetime | None = None,
) -> list[dict[str, Any]]:
    snapshot_epoch = int(payload["time"])
    snapshot_time = datetime.fromtimestamp(snapshot_epoch, tz=UTC).replace(tzinfo=None)
    normalized_ingested_at = (
        ingested_at.astimezone(UTC).replace(tzinfo=None) if ingested_at and ingested_at.tzinfo else ingested_at
    )
    if normalized_ingested_at is None:
        normalized_ingested_at = datetime.now(UTC).replace(tzinfo=None)

    rows: list[dict[str, Any]] = []
    for state_vector in payload.get("states", []) or []:
        if not state_vector:
            continue
        icao24 = _normalize_optional_text(state_vector[0] if len(state_vector) > 0 else None)
        if not icao24:
            continue
        rows.append(
            {
                "snapshot_time": snapshot_time,
                "icao24": icao24.lower(),
                "callsign": _normalize_optional_text(state_vector[1] if len(state_vector) > 1 else None),
                "origin_country": _normalize_optional_text(state_vector[2] if len(state_vector) > 2 else None),
                "time_position": state_vector[3] if len(state_vector) > 3 else None,
                "last_contact": state_vector[4] if len(state_vector) > 4 else None,
                "longitude": state_vector[5] if len(state_vector) > 5 else None,
                "latitude": state_vector[6] if len(state_vector) > 6 else None,
                "baro_altitude": state_vector[7] if len(state_vector) > 7 else None,
                "on_ground": state_vector[8] if len(state_vector) > 8 else None,
                "velocity": state_vector[9] if len(state_vector) > 9 else None,
                "true_track": state_vector[10] if len(state_vector) > 10 else None,
                "vertical_rate": state_vector[11] if len(state_vector) > 11 else None,
                "geo_altitude": state_vector[13] if len(state_vector) > 13 else None,
                "squawk": _normalize_optional_text(state_vector[14] if len(state_vector) > 14 else None),
                "position_source": state_vector[16] if len(state_vector) > 16 else None,
                "ingested_at": normalized_ingested_at,
                "run_id": run_id,
            }
        )
    return rows
