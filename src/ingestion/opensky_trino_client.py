from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import pandas as pd

try:
    from trino.auth import ConsoleRedirectHandler, OAuth2Authentication
    from trino.dbapi import connect
except ImportError as exc:  # pragma: no cover - exercised in Databricks runtime
    raise ImportError(
        "The 'trino' package is required. Install it with `pip install trino` on the cluster."
    ) from exc


DEFAULT_HOST = "trino.opensky-network.org"
DEFAULT_PORT = 443
DEFAULT_CATALOG = "minio"
DEFAULT_SCHEMA = "osky"
DEFAULT_HTTP_SCHEME = "https"


@dataclass(frozen=True)
class OpenSkyTrinoConfig:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    user: str = ""
    catalog: str = DEFAULT_CATALOG
    schema: str = DEFAULT_SCHEMA
    http_scheme: str = DEFAULT_HTTP_SCHEME
    verify: bool = True
    request_timeout_seconds: int = 180


def _coalesce(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def build_config(
    *,
    user: str | None = None,
    host: str | None = None,
    port: int | str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    http_scheme: str | None = None,
    verify: bool | None = None,
    request_timeout_seconds: int | None = None,
) -> OpenSkyTrinoConfig:
    resolved_user = _coalesce(user, os.getenv("OPENSKY_TRINO_USER"), os.getenv("OPENSKY_USERNAME"))
    resolved_host = _coalesce(host, os.getenv("OPENSKY_TRINO_HOST"), DEFAULT_HOST)
    resolved_catalog = _coalesce(catalog, os.getenv("OPENSKY_TRINO_CATALOG"), DEFAULT_CATALOG)
    resolved_schema = _coalesce(schema, os.getenv("OPENSKY_TRINO_SCHEMA"), DEFAULT_SCHEMA)
    resolved_http_scheme = _coalesce(
        http_scheme,
        os.getenv("OPENSKY_TRINO_HTTP_SCHEME"),
        DEFAULT_HTTP_SCHEME,
    )
    resolved_port = int(_coalesce(port, os.getenv("OPENSKY_TRINO_PORT"), str(DEFAULT_PORT)))
    resolved_verify = verify if verify is not None else True
    resolved_timeout = request_timeout_seconds or int(
        _coalesce(os.getenv("OPENSKY_TRINO_TIMEOUT_SECONDS"), "180")
    )

    if not resolved_user:
        raise ValueError(
            "Missing OpenSky Trino username. Provide it via notebook widgets, secrets, or OPENSKY_TRINO_USER."
        )

    return OpenSkyTrinoConfig(
        host=resolved_host,
        port=resolved_port,
        user=resolved_user,
        catalog=resolved_catalog,
        schema=resolved_schema,
        http_scheme=resolved_http_scheme,
        verify=resolved_verify,
        request_timeout_seconds=resolved_timeout,
    )


class OpenSkyTrinoClient:
    def __init__(self, config: OpenSkyTrinoConfig) -> None:
        self.config = config
        self.auth = OAuth2Authentication(redirect_auth_url_handler=ConsoleRedirectHandler())

    def query_pandas(self, sql: str) -> pd.DataFrame:
        connection = connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            catalog=self.config.catalog,
            schema=self.config.schema,
            http_scheme=self.config.http_scheme,
            auth=self.auth,
            verify=self.config.verify,
            request_timeout=self.config.request_timeout_seconds,
        )
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description or []]
            return pd.DataFrame(rows, columns=columns)
        finally:
            try:
                cursor.close()
            finally:
                connection.close()


def query_to_pandas(sql: str, **config_kwargs: Any) -> pd.DataFrame:
    config = build_config(**config_kwargs)
    client = OpenSkyTrinoClient(config)
    return client.query_pandas(sql)
