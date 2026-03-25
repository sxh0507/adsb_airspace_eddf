# 01b Live Ingestion Technical Design

## Goal

Build `01b_ingest_opensky_live.ipynb` as a production-ready Bronze live ingestion workflow for Frankfurt (`EDDF`) that:

- ingests near-real-time OpenSky REST snapshots into `adsb_airspace_eddf.brz_adsb.live_states`
- uses OAuth2 client credentials instead of deprecated Basic auth
- supports both demo-style live looping and scheduled catch-up ingestion
- avoids silently dropping snapshots when jobs are delayed, retried, or partially fail
- stays operationally independent from the historical Trino backfill path

This document defines the final technical design for `01b`.

## Source Constraints

The design depends on the current OpenSky REST API behavior:

- `GET /states/all` supports bbox filters via `lamin`, `lamax`, `lomin`, `lomax`
- authenticated users can query snapshots up to 1 hour in the past
- authenticated users have 5-second time resolution
- OAuth2 client credentials is the required auth mechanism
- Basic auth with username and password was deprecated on March 18, 2026

Official source:

- <https://openskynetwork.github.io/opensky-api/rest.html>

## Existing Project Interfaces

The workflow plugs into the current repo contracts:

- Bronze target table: [00_platform_setup_catalog_schema.ipynb:219](/Users/shixiaohong/Downloads/github_projects/adsb_airspace_eddf/notebooks/00_platform_setup_catalog_schema.ipynb#L219)
- Existing ingestion logs: [00_platform_setup_catalog_schema.ipynb:369](/Users/shixiaohong/Downloads/github_projects/adsb_airspace_eddf/notebooks/00_platform_setup_catalog_schema.ipynb#L369)
- Region config bbox: [region_config.yaml](/Users/shixiaohong/Downloads/github_projects/adsb_airspace_eddf/configs/region_config.yaml)
- Existing client pattern to mirror: [opensky_trino_client.py](/Users/shixiaohong/Downloads/github_projects/adsb_airspace_eddf/src/ingestion/opensky_trino_client.py)

## Scope

### In scope

- `01b_ingest_opensky_live.ipynb`
- a reusable OAuth2-enabled REST client in `src/ingestion`
- Bronze write path into `brz_adsb.live_states`
- run modes for `once`, `loop`, and `catch_up`
- idempotent writes
- observability and recoverability

### Out of scope

- live Silver transformation
- live Gold metric generation
- prediction
- frontend/dashboard serving

Those belong to follow-up work after `01b`.

## Proposed Components

### 1. Reusable client module

Create:

- `src/ingestion/opensky_live_client.py`

Main objects:

- `OpenSkyLiveConfig`
- `OpenSkyOAuth2TokenManager`
- `OpenSkyLiveClient`

Responsibilities:

- resolve OAuth2 credentials from widgets, Databricks secrets, or environment
- obtain and refresh bearer tokens automatically
- call `/states/all`
- normalize response payload rows into a pandas DataFrame or Python rows
- expose rate-limit and retry metadata when available

This should be a real project component, not notebook-local glue code.

### 2. Notebook entrypoint

Create:

- `notebooks/01b_ingest_opensky_live.ipynb`

Responsibilities:

- parse widgets and configs
- compute the snapshot queue for the chosen mode
- fetch snapshots through the shared client
- upsert snapshot rows into Bronze
- write run logs and snapshot manifest rows

### 3. Snapshot manifest table

To make catch-up safe, add one dedicated observability table:

- `adsb_airspace_eddf.obs.live_snapshot_manifest`

Recommended schema:

```sql
CREATE TABLE IF NOT EXISTS `{catalog}`.obs.live_snapshot_manifest (
    scope_id STRING NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    snapshot_epoch BIGINT NOT NULL,
    source_object STRING NOT NULL,
    target_table STRING NOT NULL,
    status STRING NOT NULL,
    rows_read BIGINT,
    rows_written BIGINT,
    requested_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    run_id STRING NOT NULL,
    error_message STRING
) USING DELTA
COMMENT 'Manifest of live snapshot ingestion attempts and completion status.'
```

Why add this instead of overloading `obs.ingestion_partition_log`:

- we need a structured key by `scope_id + snapshot_epoch`
- we need to query success gaps efficiently
- packing scope and time into a single string field would work, but it is brittle and hard to maintain

## Target Table Strategy

Use the existing Bronze live table:

- `adsb_airspace_eddf.brz_adsb.live_states`

Current schema is already a good fit for the `/states/all` response:

- `snapshot_time`
- `icao24`
- `callsign`
- `origin_country`
- `time_position`
- `last_contact`
- `longitude`
- `latitude`
- `baro_altitude`
- `on_ground`
- `velocity`
- `true_track`
- `vertical_rate`
- `geo_altitude`
- `squawk`
- `position_source`
- `ingested_at`
- `run_id`

### Partitioning

Do not partition by `snapshot_time`.

Reason:

- `snapshot_time` is too high-cardinality
- it would create tiny partitions and poor table layout

Phase-1 recommendation:

- keep `brz_adsb.live_states` non-partitioned
- configure short retention

If the table later grows into a persistent operational store, revisit coarse partitioning by `snapshot_date` or `snapshot_hour`.

## Config Design

Extend [pipeline_config.yaml](/Users/shixiaohong/Downloads/github_projects/adsb_airspace_eddf/configs/pipeline_config.yaml) with a dedicated block:

```yaml
opensky_live_connection:
  base_url: "https://opensky-network.org/api"
  token_url: "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
  secret_scope: "opensky"
  client_id_key: "live_client_id"
  client_secret_key: "live_client_secret"
live_ingestion:
  default_mode: "loop"
  poll_interval_seconds: 60
  run_duration_minutes: 15
  run_budget_seconds: 720
  schedule_interval_seconds: 900
  safety_buffer_seconds: 90
  recoverable_lookback_seconds: 3300
  retention_days: 14
  max_snapshots_per_run: 80
```

Notes:

- `recoverable_lookback_seconds=3300` means 55 minutes, intentionally inside the API's 1-hour bound
- `run_budget_seconds=720` is 12 minutes
- `schedule_interval_seconds=900` is 15 minutes

## Scope ID

Each live ingestion scope needs a stable ID.

Recommended pattern:

```text
eddf_live_bbox_<bbox_hash>_poll60_v1
```

The hash should be derived from:

- `focus_airport`
- bbox bounds
- `poll_interval_seconds`

Why:

- changing bbox or cadence changes the expected snapshot set
- the catch-up logic must not mix manifests from different scopes

## Notebook Widgets

Recommended widgets:

- `catalog`
- `mode` with `once`, `loop`, `catch_up`
- `poll_interval_seconds`
- `run_duration_minutes`
- `run_budget_seconds`
- `max_snapshots_per_run`
- `safety_buffer_seconds`
- `recoverable_lookback_seconds`
- optional bbox overrides
- `dry_run`

## Run Modes

### 1. `once`

Purpose:

- quick connectivity check
- single-snapshot manual validation

Behavior:

- fetch exactly one safe snapshot
- write Bronze rows idempotently

### 2. `loop`

Purpose:

- development
- interactive demo
- warm-cluster live showcase

Behavior:

- fetch the latest safe snapshot every `poll_interval_seconds`
- continue for `run_duration_minutes`
- skip already-complete snapshots

Recommended default:

- `poll_interval_seconds=60`
- `run_duration_minutes=15`

### 3. `catch_up`

Purpose:

- scheduled production ingestion with overlap protection

Behavior:

- determine which expected snapshots are still missing in the recent recoverable window
- ingest oldest missing snapshots first
- stop when `run_budget_seconds` is reached or the queue is empty

Recommended scheduled job:

- run every 15 minutes
- job budget 12 minutes
- `max_concurrent_runs=1`

This leaves headroom for auth refresh, retries, Delta write time, and cluster jitter.

## Idempotency Design

Idempotency must not be based only on `snapshot_time`.

### Row-level idempotency

Use a Delta `MERGE` key:

```text
snapshot_time + icao24
```

Why:

- the same snapshot contains many aircraft rows
- retries may replay the same snapshot
- partial failures must not duplicate aircraft rows

### Snapshot-level completeness

Track completion in `obs.live_snapshot_manifest` using:

```text
scope_id + snapshot_epoch
```

A snapshot is considered complete only after:

1. fetch succeeded
2. Bronze merge succeeded
3. manifest row is written with `status='success'`

If fetch succeeds but the merge fails, the snapshot must remain pending for the next catch-up run.

## Important Correction To The Simple Watermark Idea

The phrase "start from `last_successful_snapshot_time`" is directionally useful, but it is not strong enough by itself.

Why it can fail:

- suppose snapshots `10:00`, `10:01`, `10:02`
- `10:00` succeeds
- `10:01` fails
- `10:02` succeeds on a retry

If the next run uses `MAX(successful_snapshot_time)=10:02` as the watermark, it will start at `10:03` and silently skip the failed `10:01`.

So the final design should not use raw `MAX(success)` as the only resume signal.

Use one of these two concepts instead:

- `last_contiguous_successful_snapshot_time`
- or better, `expected snapshots minus successful snapshots`

This design adopts the second option because it is simpler and more robust.

## Catch-Up Algorithm

### Core idea

On every scheduled run:

1. compute the most recent safe snapshot boundary
2. define the recent recoverable window
3. build the set of expected snapshot times in that window
4. load the set of successful snapshot times for the same `scope_id`
5. ingest the difference set in chronological order

This is what prevents gaps from being forgotten.

### Time alignment

If `poll_interval_seconds=60`, every expected snapshot must align to 60-second boundaries.

Helper:

```python
def floor_to_interval(epoch_seconds: int, interval_seconds: int) -> int:
    return epoch_seconds - (epoch_seconds % interval_seconds)
```

### Safe upper bound

Never chase the absolute latest instant.

Use:

```python
latest_safe_epoch = floor_to_interval(
    int(now_utc.timestamp()) - safety_buffer_seconds,
    poll_interval_seconds,
)
```

Why:

- the newest edge is the least stable
- token refresh and network delay make "now" slippery
- a safety buffer prevents flapping and duplicate near-edge work

### Recoverable lower bound

Stay within the API's 1-hour lookback guarantee.

Use:

```python
oldest_recoverable_epoch = latest_safe_epoch - recoverable_lookback_seconds
```

With `recoverable_lookback_seconds=3300`, the job intentionally works inside the last 55 minutes.

### Expected snapshot series

Build the expected schedule:

```python
first_expected_epoch = floor_to_interval(
    oldest_recoverable_epoch,
    poll_interval_seconds,
)

expected_epochs = list(
    range(first_expected_epoch, latest_safe_epoch + 1, poll_interval_seconds)
)
```

### Successful snapshot set

Query successful snapshots for the same scope and recent window:

```python
success_epochs = load_successful_snapshot_epochs(
    scope_id=scope_id,
    start_epoch=first_expected_epoch,
    end_epoch=latest_safe_epoch,
)
```

### Pending queue

This is the real anti-gap logic:

```python
pending_epochs = [epoch for epoch in expected_epochs if epoch not in success_epochs]
```

That line is the key reason the design does not silently lose data.

Even if a middle snapshot failed hours ago, it stays in `pending_epochs` until it is successfully written.

### Run budget cutoff

Scheduled jobs must exit before the next trigger can pile up.

Use:

```python
deadline = job_started_at + timedelta(seconds=run_budget_seconds)
for epoch in pending_epochs:
    if datetime.now(timezone.utc) >= deadline:
        break
    ingest_snapshot_epoch(epoch)
```

### Detect irrecoverable lag

If backlog becomes older than the REST API's recoverable range, the job must alert loudly.

Example:

```python
oldest_missing_epoch = load_oldest_missing_snapshot_epoch(scope_id=scope_id)
if oldest_missing_epoch is not None and oldest_missing_epoch < first_expected_epoch:
    raise RuntimeError("Live backlog exceeded recoverable REST lookback window.")
```

This check compares the oldest still-missing snapshot for the scope to the current recoverable boundary.

## Reference Pseudocode

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

UTC = timezone.utc


@dataclass(frozen=True)
class CatchUpPlan:
    scope_id: str
    latest_safe_epoch: int
    first_expected_epoch: int
    pending_epochs: list[int]


def floor_to_interval(epoch_seconds: int, interval_seconds: int) -> int:
    return epoch_seconds - (epoch_seconds % interval_seconds)


def build_catch_up_plan(
    *,
    now_utc: datetime,
    scope_id: str,
    poll_interval_seconds: int,
    safety_buffer_seconds: int,
    recoverable_lookback_seconds: int,
    successful_epochs: set[int],
) -> CatchUpPlan:
    latest_safe_epoch = floor_to_interval(
        int(now_utc.timestamp()) - safety_buffer_seconds,
        poll_interval_seconds,
    )
    first_expected_epoch = floor_to_interval(
        latest_safe_epoch - recoverable_lookback_seconds,
        poll_interval_seconds,
    )
    pending_epochs = [
        epoch
        for epoch in range(
            first_expected_epoch,
            latest_safe_epoch + 1,
            poll_interval_seconds,
        )
        if epoch not in successful_epochs
    ]
    return CatchUpPlan(
        scope_id=scope_id,
        latest_safe_epoch=latest_safe_epoch,
        first_expected_epoch=first_expected_epoch,
        pending_epochs=pending_epochs,
    )


def run_catch_up_job(
    *,
    client,
    scope_id: str,
    poll_interval_seconds: int,
    safety_buffer_seconds: int,
    recoverable_lookback_seconds: int,
    run_budget_seconds: int,
    max_snapshots_per_run: int,
) -> dict:
    job_started_at = datetime.now(UTC)
    deadline = job_started_at + timedelta(seconds=run_budget_seconds)

    successful_epochs = load_successful_snapshot_epochs(
        scope_id=scope_id,
        start_epoch=None,
        end_epoch=None,
    )

    plan = build_catch_up_plan(
        now_utc=job_started_at,
        scope_id=scope_id,
        poll_interval_seconds=poll_interval_seconds,
        safety_buffer_seconds=safety_buffer_seconds,
        recoverable_lookback_seconds=recoverable_lookback_seconds,
        successful_epochs=successful_epochs,
    )

    ingested = 0
    skipped = 0
    failed = 0

    for snapshot_epoch in plan.pending_epochs[:max_snapshots_per_run]:
        if datetime.now(UTC) >= deadline:
            break

        try:
            response = client.fetch_states_all(
                time=snapshot_epoch,
                scope_id=scope_id,
            )
            actual_snapshot_epoch = int(response["time"])
            rows = normalize_live_states(response, run_id=current_run_id())
            merge_rows_into_live_bronze(rows)
            write_manifest_success(
                scope_id=scope_id,
                snapshot_epoch=actual_snapshot_epoch,
                rows_written=len(rows),
            )
            ingested += 1
        except DuplicateSnapshotComplete:
            skipped += 1
        except Exception as exc:
            write_manifest_failure(
                scope_id=scope_id,
                snapshot_epoch=snapshot_epoch,
                error_message=str(exc),
            )
            failed += 1

    return {
        "scope_id": scope_id,
        "pending_snapshot_count": len(plan.pending_epochs),
        "ingested_snapshot_count": ingested,
        "skipped_snapshot_count": skipped,
        "failed_snapshot_count": failed,
        "latest_safe_epoch": plan.latest_safe_epoch,
    }
```

## Why This Does Not Silently Drop Data

The safety comes from three properties working together.

### 1. The queue is rebuilt every run

The job does not assume the previous run completed perfectly.

Each time it starts, it recomputes what snapshots should exist in the recent window.

### 2. Success is tracked by snapshot, not inferred from time progression

The job does not say:

- "I once got something newer, so everything older must be fine"

Instead it says:

- "Show me every expected snapshot that does not yet have a success marker"

That is the critical difference.

### 3. Writes are idempotent

If a snapshot is retried, the Delta merge updates existing `(snapshot_time, icao24)` rows instead of duplicating them.

So the system can safely retry missing snapshots without producing duplicates.

## Bronze Write Strategy

Use a temp view plus Delta `MERGE`.

Conceptually:

```sql
MERGE INTO adsb_airspace_eddf.brz_adsb.live_states AS target
USING live_snapshot_stage AS source
ON target.snapshot_time = source.snapshot_time
AND target.icao24 = source.icao24
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Why `MERGE` instead of append-only:

- retries are normal
- catch-up mode intentionally replays missing snapshots
- append-only would duplicate aircraft rows

## Scheduling Recommendations

### Development and demo

- mode: `loop`
- poll interval: `60s`
- run duration: `15min`

### Scheduled production

- mode: `catch_up`
- schedule: every `15min`
- run budget: `12min`
- safety buffer: `90s`
- max concurrent runs: `1`

Alternative slower schedule:

- mode: `catch_up`
- schedule: every `20min`
- run budget: `15min`

### Warm continuous option

If a stable always-on cluster is acceptable, continuous `loop` on warm compute is simpler operationally than scheduled catch-up.

That option reduces orchestration complexity, but it trades away some cost efficiency.

## Failure Handling

Recommended behavior:

- `401`: refresh token once and retry
- `429`: back off and retry within remaining run budget
- empty snapshot: write `success` with `rows_written=0` if fetch and merge completed correctly
- write failure after fetch success: mark manifest `failed` so catch-up retries it
- backlog older than recoverable window: raise alert and mark run `failed`

## Retention

For phase 1:

- retain Bronze live snapshots for 14 days
- vacuum or prune according to the platform policy

Reason:

- the table is for live comparison and recent validation
- long-term history already belongs in the Trino-backed historical path

## Follow-Up Work

After `01b` lands, the next pieces should be:

1. `02b_prepare_live_states_v2`
2. live complexity aggregation using the same cell scheme as historical V2
3. real-time vs historical baseline comparison
4. optional prediction later, after enough live and historical windows accumulate

## Final Recommendation

Implement `01b` as:

- a reusable OAuth2 REST client
- a Bronze-only notebook
- a dedicated snapshot manifest
- catch-up ingestion based on expected-vs-successful snapshot difference

That combination is the smallest design that is still operationally safe.
