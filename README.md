# Frankfurt Airspace Complexity Analytics

**A reproducible OpenSky ADS-B pipeline for spatio-temporal complexity analysis around Frankfurt**

## Overview

This project builds a reproducible data pipeline for OpenSky ADS-B data and derives spatio-temporal airspace complexity indicators around Frankfurt.

The goal is not generic flight tracking. The goal is to transform raw aircraft state vectors into interpretable grid-level complexity metrics for operational analysis and future research.

The project is intentionally scoped as a minimal, research-oriented case study:

- only Frankfurt
- only airspace complexity
- only a closed-loop first version that can be reproduced and shown on GitHub

## Research Question

**How does airspace complexity evolve over time and space around Frankfurt?**

Sub-questions:

- Which areas around Frankfurt show the highest airspace complexity?
- Which time periods show the highest complexity?
- Which factors drive complexity most strongly in the first version:
  - traffic density
  - heading dispersion
  - altitude mixing
  - speed heterogeneity

## Scope

### Spatial scope

- focus airport: `EDDF`
- ingestion scope: `150 NM` around Frankfurt Airport
- exploratory regional range: `120-180 NM`
- visualization and hotspot analysis can be restricted to a smaller Frankfurt-focused box:
  - latitude `49.5` to `50.8`
  - longitude `7.8` to `9.6`

This split keeps ingestion robust while making the case-study visuals easier to interpret.

### Vertical scope

The first version uses two altitude bands:

- `SFC-FL100`
- `FL100-FL245`

### Temporal scope

- raw states: original minute-level or snapshot-level timestamps
- Gold complexity table: `5-minute` windows
- dashboard and trend plots: `15-minute` rollups

### Data scope

- **required historical source:** OpenSky Trino
- **optional near-real-time source:** OpenSky live API
- **not included in v1 score:** weather

### Catalog convention

The default catalog for this project is:

- `adsb_airspace_eddf`

Weather is intentionally excluded from the first complexity score so that the baseline ADS-B methodology can be completed quickly and explained clearly. Weather can be added in a second phase.

At the scaffold level, the first-phase project keeps the broader component labels:

- `traffic_load`
- `interaction`
- `flow_structure`

The GitHub-facing MVP refines that broad framing into four directly measurable first-pass features.

## Data Sources

### 1. Historical backbone

The historical case study should use OpenSky Trino rather than the public REST API.

Recommended starting tables:

- `state_vectors_data4`
- `flights_data4`

Why:

- historical backfill is the core of the project
- Trino is the stable path for bounded historical extraction
- it avoids overloading the live API for case-study data collection

### 2. Near-real-time extension

The OpenSky live API is optional in the first milestone and can be used later for:

- recent snapshot validation
- near-real-time demonstrations
- comparing live patterns to historical baselines

## Complexity Methodology

The first model stays deliberately simple and interpretable.

### Base features

For each `grid_id x 5-minute window`, compute:

1. `aircraft_count`
2. `heading_dispersion`
3. `altitude_dispersion`
4. `speed_dispersion`

### Feature definitions

#### `aircraft_count`

The number of aircraft observed in the grid cell during the time window.

This is the simplest proxy for traffic density.

#### `heading_dispersion`

Higher heading diversity suggests more heterogeneous flow structure and potentially more coordination complexity.

The first version can use a simplified directional dispersion metric. It does not need a full research-grade circular statistics implementation in the first milestone.

#### `altitude_dispersion`

Higher altitude variation suggests stronger vertical mixing and transition complexity.

The first version can use one of:

- altitude standard deviation
- number of occupied altitude bands

#### `speed_dispersion`

Higher speed variation indicates less homogeneous flow behavior.

The first version uses:

- standard deviation of velocity

### Composite score

Each feature is normalized and combined into a simple weighted score:

```text
complexity_score =
0.25 * norm(aircraft_count)
+ 0.25 * norm(heading_dispersion)
+ 0.25 * norm(altitude_dispersion)
+ 0.25 * norm(speed_dispersion)
```

This first version prioritizes interpretability and reproducibility over optimization.

## Minimal Data Architecture

The project follows a Bronze / Silver / Gold pattern.

### Bronze

Raw ADS-B and extraction outputs.

Recommended tables:

- `bronze_opensky_state_vectors`
- `bronze_opensky_flights`

### Silver

Cleaned and analysis-ready flight states.

Recommended tables:

- `silver_fra_flight_states_clean`

Core processing steps:

- deduplication
- invalid coordinate filtering
- region filtering
- timestamp normalization
- abnormal speed/altitude filtering
- grid assignment
- 5-minute window assignment

### Gold

Research-ready complexity products.

Recommended tables:

- `gold_fra_grid_complexity_5m`
- `gold_fra_complexity_hotspots`
- `gold_fra_complexity_trend_15m`

## Databricks Naming

The Databricks implementation uses fully qualified table names in the form:

`catalog.schema.table`

The default naming contract for the first Frankfurt version is:

- `adsb_airspace_eddf.ref.project_scope`
- `adsb_airspace_eddf.ref.altitude_bands`
- `adsb_airspace_eddf.ref.grid_cells`
- `adsb_airspace_eddf.brz_adsb.hist_state_vectors`
- `adsb_airspace_eddf.brz_adsb.hist_flights`
- `adsb_airspace_eddf.brz_adsb.live_states`
- `adsb_airspace_eddf.brz_weather.metar_raw`
- `adsb_airspace_eddf.slv_airspace.flight_states_clean`
- `adsb_airspace_eddf.gld_airspace.grid_complexity_5m`
- `adsb_airspace_eddf.gld_airspace.complexity_hotspots`
- `adsb_airspace_eddf.gld_airspace.complexity_trend_15m`
- `adsb_airspace_eddf.obs.ingestion_log`
- `adsb_airspace_eddf.obs.ingestion_partition_log`
- `adsb_airspace_eddf.obs.pipeline_run_log`

## Observability Design

### Run-level ingestion log

`adsb_airspace_eddf.obs.ingestion_log` stores one row per notebook run.

Recommended fields:

- `run_id`
- `pipeline_name`
- `source_name`
- `source_object`
- `target_table`
- `scope_id`
- `start_date`
- `end_date`
- `partition_key`
- `planned_partition_count`
- `success_partition_count`
- `failed_partition_count`
- `dry_run`
- `status`
- `rows_written`
- `started_at`
- `completed_at`
- `error_message`
- `metadata_json`

Recommended `status` values:

- `planned`
- `running`
- `success`
- `partial_success`
- `failed`
- `dry_run`

### Partition-level ingestion log

`adsb_airspace_eddf.obs.ingestion_partition_log` stores one row per extracted `hour` or `day` partition.

Recommended fields:

- `run_id`
- `pipeline_name`
- `source_object`
- `target_table`
- `partition_key`
- `partition_value`
- `status`
- `rows_read`
- `rows_written`
- `attempt_no`
- `dry_run`
- `started_at`
- `completed_at`
- `error_message`

Recommended `status` values:

- `success`
- `failed`
- `empty_source`
- `skipped`
- `dry_run`

### `dry_run` semantics

`dry_run=true` means:

- load configs and resolve the final run parameters
- build the planned `hour` and `day` partitions
- validate the source SQL templates
- optionally test connectivity with a lightweight query such as `LIMIT 1`
- do **not** write Bronze tables
- do **not** overwrite any existing partition
- do **not** write to `obs` tables unless an explicit future `log_dry_run` option is added

### Partition overwrite safety

Historical ingestion should use a bounded overwrite design:

1. build the SQL for one partition
2. extract that partition from OpenSky Trino
3. validate schema and row counts
4. only then write with a partition-scoped overwrite such as `replaceWhere`
5. write a row into `obs.ingestion_partition_log`

Safety rule:

- if a source partition returns zero rows, mark it as `empty_source`
- do not overwrite an existing target partition by default
- only clear empty partitions when an explicit `overwrite_empty_partitions=true` option is enabled

## Planned Repository Structure

```text
.
├── README.md
├── requirements.txt
├── .gitignore
├── configs/
│   ├── region_config.yaml
│   └── pipeline_config.yaml
├── data/
│   └── sample/
├── docs/
│   └── figures/
├── notebooks/
│   ├── 00_platform_setup_catalog_schema.ipynb
│   ├── 01a_ingest_opensky_history.ipynb
│   ├── 01b_ingest_opensky_live.ipynb
│   ├── 02_clean_and_prepare_states.ipynb
│   ├── 03_build_complexity_metrics.ipynb
│   └── 04_visualize_results.ipynb
├── src/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── features/
│   ├── visualization/
│   └── utils/
└── tests/
```

## Minimal Execution Plan

The fastest useful build sequence is:

1. define the Frankfurt region and grid
2. backfill bounded historical Frankfurt slices from OpenSky Trino
3. clean and grid ADS-B states
4. compute 5-minute complexity metrics
5. roll up 15-minute trend views
6. generate hotspot and temporal figures

## Reproducibility

### Prerequisites

- Python 3.10+
- OpenSky historical access via Trino
- Trino CLI or Python Trino client
- local environment capable of exporting Parquet or CSV
- Databricks cluster or notebook environment with the `trino` package installed

### OpenSky credentials in Databricks

`01a_ingest_opensky_history.ipynb` resolves Trino credentials in this order:

- `trino_user` or `OPENSKY_TRINO_USER` for the lowercase OpenSky username
- optional Databricks secret lookup for the username
- external OAuth2 authentication for the actual OpenSky login flow

OpenSky's Trino endpoint requires external authentication rather than raw basic username/password exchange.
The project Trino client therefore uses `OAuth2Authentication` instead of `BasicAuthentication`.

Recommended default secret names:

- scope: `opensky`
- username key: `username`
- password key: `password`

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Configure the Frankfurt study region

Edit:

- `configs/region_config.yaml`
- `configs/pipeline_config.yaml`

### 3. Extract historical data

Run the history notebook or Python module to extract bounded OpenSky Trino slices into Bronze tables.

Notebook defaults:

- host: `trino.opensky-network.org`
- catalog: `minio`
- schema: `osky`
- auth mode: external OAuth2 authentication
- partition strategy: `hour` for state vectors and `day` for flights
- write strategy: extract first, then partition-scoped `replaceWhere`

Expected raw outputs:

- `adsb_airspace_eddf.brz_adsb.hist_state_vectors`
- `adsb_airspace_eddf.brz_adsb.hist_flights`

### 4. Build Silver states

Run the cleaning and gridding step to create:

- `silver_fra_flight_states_clean`

### 5. Build Gold metrics

Run the feature notebook to create:

- `gold_fra_grid_complexity_5m`
- `gold_fra_complexity_hotspots`
- `gold_fra_complexity_trend_15m`

### 6. Generate final figures

Create at least:

- a Frankfurt complexity heatmap
- a temporal complexity trend plot
- a hotspot ranking chart

## Planned Notebook Flow

### `00_platform_setup_catalog_schema.ipynb`

Purpose:

- create schemas and baseline Delta tables
- seed the Frankfurt project scope
- seed the initial altitude bands

### `01a_ingest_opensky_history.ipynb`

Purpose:

- extract bounded historical slices from OpenSky Trino
- store raw historical ADS-B data
- overwrite `hour` and `day` partitions safely
- record both run-level and partition-level ingestion status
- support bounded UTC-hour validation runs with `start_hour_utc` and `end_hour_utc`

Primary targets:

- `adsb_airspace_eddf.brz_adsb.hist_state_vectors`
- `adsb_airspace_eddf.brz_adsb.hist_flights`
- `adsb_airspace_eddf.obs.ingestion_log`
- `adsb_airspace_eddf.obs.ingestion_partition_log`

### `01b_ingest_opensky_live.ipynb`

Purpose:

- ingest optional live or recent snapshots from the OpenSky live API
- support recent-pattern validation and demonstration views
- remain operationally independent from historical Trino backfill

### `02_clean_and_prepare_states.ipynb`

Purpose:

- deduplicate raw states
- remove invalid coordinates
- remove abnormal speed/altitude values
- create `grid_id`
- create `time_window_5m`

Output:

- `silver_fra_flight_states_clean`

### `03_build_complexity_metrics.ipynb`

Purpose:

- compute the 4 base features
- normalize them
- calculate `complexity_score`
- generate hotspot rankings

Outputs:

- `gold_fra_grid_complexity_5m`
- `gold_fra_complexity_hotspots`
- `gold_fra_complexity_trend_15m`

### `04_visualize_results.ipynb`

Purpose:

- generate the final project figures

Minimum figures:

- complexity heatmap
- 15-minute or hourly trend plot
- top hotspot ranking

## Expected Outputs

The first complete GitHub-ready version should produce at least:

### 1. Complexity heatmap

A grid-based Frankfurt map colored by average complexity score.

### 2. Temporal trend

A 15-minute or hourly trend view showing how complexity evolves during the case-study period.

### 3. Hotspot ranking

A ranking of the most complex grid cells across the study window.

## Why This Project Matters

This is designed as a research-oriented data engineering and analytics project rather than a generic dashboard demo.

It demonstrates:

- historical aviation data extraction from OpenSky
- reproducible data-pipeline design
- spatio-temporal feature engineering
- interpretable complexity modeling
- research-facing visualization and documentation

## Project Summary For CV Or Email

Short version:

> Built a reproducible OpenSky ADS-B analytics pipeline for Frankfurt and derived grid-level airspace complexity indicators from raw aircraft state vectors.

Research-oriented version:

> Developed a Frankfurt case study on airspace complexity using ADS-B trajectories, focusing on how traffic density, heading dispersion, altitude variation, and speed heterogeneity evolve across space and time.

## Future Work

After the first ADS-B-only milestone, the most natural extensions are:

- add weather as a second-phase explanatory layer
- compare Frankfurt with another airport such as Munich
- build short-horizon complexity forecasts
- connect complexity patterns to operational scenarios

## One-Sentence Project Statement

Use OpenSky ADS-B data to build a reproducible Frankfurt airspace complexity analytics pipeline that outputs spatial hotspots, temporal trends, and interpretable grid-level complexity rankings.
