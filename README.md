# Seattle permitting learning pipeline

A local learning project for Python extraction, Apache Spark processing, and optional Apache Airflow orchestration. Start with a manual residential-permit pipeline; add scheduled orchestration after you can inspect each step. Streamlit and DuckDB explore published Parquet outputs.

## Quick start: standalone MVP

Install Docker Desktop and start it. Allocate about 8 GB of memory. Docker was not available in the initial development environment, so container startup must be verified on your machine.

```sh
mkdir -p data
docker compose build
docker compose run --rm pipeline run --config configs/seattle.json --run-id demo --data-dir /data --fixture fixtures/seattle.jsonl
docker compose up -d dashboard
```

Open http://localhost:8501. Fixtures are synthetic and demonstrate behavior; they are not Seattle observations. To fetch the live Seattle API, omit `--fixture` and choose a new run identifier:

```sh
docker compose run --rm pipeline run --config configs/seattle.json --run-id seattle-live-001 --data-dir /data
```

The live extraction takes a full snapshot so revisions to older permits are captured. Do not interrupt a run and assume its partial raw data is a complete snapshot. Only successfully validated and transformed outputs are published.

For a local Python environment, install Java 17 and Python 3.11, create a virtual environment, and install `requirements.txt`:

```sh
python -m pip install -r requirements.txt
python -m pipeline.cli run --config configs/seattle.json --run-id demo --data-dir data --fixture fixtures/seattle.jsonl
DATA_DIR=data streamlit run dashboard/app.py
python -m unittest discover -s tests -v
```

The Spark runtime uses `local[2]`. No separate Spark cluster is needed. Commands exit unsuccessfully when a stage fails; inspect the error before retrying. See CLI help for independently runnable extraction, validation, transformation, and publication commands:

```sh
python -m pipeline.cli --help
python -m pipeline.cli run --help
```

## Optional Airflow phase

The default dashboard startup does not start Airflow. Enable it explicitly:

```sh
docker compose --profile airflow up -d
```

Open http://localhost:8080 and sign in with `airflow` / `airflow`. These credentials and the webserver secret are for local learning. The DAG starts paused: inspect it, trigger a manual run, and then unpause it when ready for the daily 06:00 America/Los_Angeles schedule. Docker and the computer must be running at the scheduled time. Airflow uses LocalExecutor and a separate PostgreSQL metadata database; Spark still runs in local mode.

```sh
docker compose --profile airflow logs airflow-scheduler
docker compose --profile airflow down
```

Stopping services retains volumes and published files. Removing volumes deletes Airflow metadata, so use that option only deliberately.

## Data interpretation

The initial source is [Seattle Building Permits](https://data.seattle.gov/Built-Environment/Building-Permits/76t5-zqzr/about_data), accessed through its Socrata JSON API. Applications and issuances are monthly event measures from source dates. Permit counts, buildings, and housing units are different measures. An issued permit is not evidence that construction finished. Missing unit values are unknown, and potential supply is not a forecast or the city's total housing inventory.

The main source includes `completeddate`, defined as the date all permit inspections were completed. Completion charts use this documented date, never infer it from status. The separate quarterly ArcGIS housing dataset has multiple rows per permit; it is documented for future reconciliation and is not joined into totals. Building and demolition permit types are selected by default, with other types available through filters. For a complete offline demonstration including rates, run:

```sh
docker compose run --rm pipeline run --run-id demo-rates --data-dir /data --fixture fixtures/seattle.jsonl --rates-fixture fixtures/rates.csv
```

For live rates, add `--with-rates` to a new `run` command. The Airflow DAG includes rate enrichment before publication. For an optional quarterly-source cross-check, add `--reconcile` to a new Seattle run; this produces a coverage/difference report without changing primary totals. Offline reconciliation uses `--completion-fixture fixtures/completion.jsonl`. Rate comparisons use FRED's [30-year fixed mortgage-rate series](https://fred.stlouisfed.org/series/MORTGAGE30US) and represent associations, not causation. Source freshness and coverage should be reviewed before interpreting charts.

Raw snapshots and versioned processed outputs live under the configured data directory. The latest-success pointer lets the dashboard continue using the previous publication when a refresh fails. Use a fresh run identifier for each new snapshot. Historical source revisions can change past monthly totals; observation history starts when this project collects snapshots.

## Add another source or building type

Source configuration maps jurisdiction-specific fields into the shared permit model. The synthetic second-city fixture intentionally reuses permit A001 while changing field names:

```sh
docker compose run --rm pipeline run --config configs/second_city.json --run-id second-city-demo --data-dir /data --fixture fixtures/second_city.jsonl
```

This proves the adapter contract; it does not establish a live connection to another municipality. Normalized identifiers include jurisdiction and source. Commercial records are preserved even though the initial dashboard focuses on residential measures. Adapt the configuration and add representative fixtures before onboarding a real source. A publication represents its configured source/run; cross-city union and combined dashboards require an explicit combined dataset.

Read [the architecture and phase guide](docs/architecture.md) for wave dependencies and acceptance gates.

## Troubleshooting

- If Java or Spark fails locally, use the Docker runtime or verify Java 17 and Python 3.11 are selected.
- If the dashboard has no publication, complete the fixture pipeline first and confirm both commands use the same data directory.
- If API extraction fails, check network access, Socrata rate limits, and the recorded error; do not publish incomplete data.
- If Airflow does not run on schedule, confirm it is unpaused and its scheduler is healthy.
- This project folder is under OneDrive CloudStorage. Synchronization and file eviction can interfere with large snapshots and Spark files. Prefer a nonsynced local data directory for sustained use, and update the Compose data bind mount accordingly.

## Integration tests

```sh
RUN_SPARK_TESTS=1 python -m unittest discover -s tests -v
```

Spark integration tests require Java 17. Unit tests run without Spark.

## Verified source contract

See [source verification](docs/source-verification.md) for checked identifiers, classifications, completion semantics, and limitations of permit-level housing-unit totals.
