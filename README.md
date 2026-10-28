
# Financial Market Data Pipeline

**Architecture Documentation:** [View System Architecture](docs/architecture.md)

[![Financial Market Pipeline CI](https://github.com/AlejandraGMejia/financial-market-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/AlejandraGMejia/financial-market-data-pipeline/actions/workflows/ci.yml)

**Author:** Alejandra G. Mejía

A modular financial market ETL pipeline built with Python, PostgreSQL, Docker, and Apache Airflow, featuring data quality validation, idempotent loading, API usage controls, workflow orchestration, and automated continuous integration.

> **Project Status:** Core implementation complete and validated. ETL, PostgreSQL integration, Apache Airflow orchestration, and GitHub Actions CI are implemented. Final architecture documentation and portfolio deliverables are in progress.

## Project Overview

This project implements a modular ETL pipeline for collecting, validating, and storing daily financial market data.

The system integrates with the Alpha Vantage API, preserves original JSON responses, transforms OHLCV observations into structured records, and loads validated data into PostgreSQL.

The pipeline supports both live API ingestion and offline replay of previously saved datasets.

Apache Airflow provides local workflow orchestration, while GitHub Actions automatically validates code quality, database integration, and DAG behavior.

The main objective is to demonstrate reliable, reproducible, and maintainable data engineering practices.

## Business Problem

Financial analysts and data teams require consistent and accessible historical market data.

Manual collection introduces operational challenges:

- Inconsistent financial data formats
- Duplicate observations
- Missing or invalid OHLCV values
- Repetitive manual processing
- Limited execution traceability
- Uncontrolled API consumption
- Difficulties reproducing data ingestion workflows

This project addresses these challenges through automated extraction, validation, transactional loading, API usage controls, workflow orchestration, and continuous integration.

## Architecture

### ETL Data Flow

```text
                Alpha Vantage API
                        |
                        v
                 Python API Client
                        |
                        v
                  Raw JSON Storage
                        |
                        v
                OHLCV Transformation
                & Data Validation
                        |
                        v
                 PostgreSQL Database
                        |
                        v
                    SQL Analytics
```

### Workflow Orchestration

```text
                   Apache Airflow
                         |
                         v
              financial_market_etl
                         |
                         v
                  run_market_etl
                         |
                         v
               Python ETL Entry Point
                         |
                         v
                 Offline Data Replay
                         |
                         v
                  PostgreSQL Load
                         |
                         v
                 Task Status & Logs
```

### Continuous Integration

```text
                    GitHub Push / PR
                           |
                           v
                     GitHub Actions
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
     Code Quality      ETL Unit Tests   Integration
     Ruff + Bash                        PostgreSQL
                                             |
                                             v
                                     Airflow DAG Tests

                   All jobs must pass
```

The ETL application can run independently from Apache Airflow using its command-line interface.

## Pipeline Workflow

1. **Extract:** Retrieve daily market data from Alpha Vantage or read previously saved JSON files.
2. **Preserve:** Store original API responses for traceability.
3. **Transform:** Convert raw market observations into structured OHLCV records.
4. **Validate:** Apply financial data quality checks before loading.
5. **Load:** Insert or update observations through transactional PostgreSQL UPSERT operations.
6. **Track:** Record ingestion results, successful API fetches, and API request usage.
7. **Orchestrate:** Run the pipeline through Apache Airflow and monitor execution status.
8. **Validate Changes:** Execute automated tests and code quality checks through GitHub Actions.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.14 | Core ETL implementation |
| Requests | Financial API integration |
| pandas | Data transformation and validation |
| Decimal | Financial price precision |
| PostgreSQL 18 | Relational data storage |
| SQLAlchemy | Database connectivity and transactional loading |
| Docker Compose | Local database infrastructure |
| Apache Airflow 3.3.2 | Workflow orchestration and execution monitoring |
| Pytest | Unit and PostgreSQL integration tests |
| unittest | Airflow DAG unit tests |
| Ruff | Python linting and formatting |
| GitHub Actions | Automated continuous integration |
| Git & GitHub | Version control and repository hosting |

## Implemented Features

### Financial Data Extraction

- Alpha Vantage daily time-series ingestion
- Multi-asset processing
- Configurable financial symbol selection
- Original JSON response preservation
- Offline ingestion from saved raw datasets
- Per-asset error handling

### Data Transformation and Storage

- OHLCV transformation and validation
- Chronological organization of financial observations
- Relational database schema with integrity constraints
- Composite primary key for duplicate prevention
- Transactional PostgreSQL UPSERT operations
- Idempotent loading of unchanged records
- Database transaction integration tests

### API Usage Management

- Configurable ingestion freshness checks
- Successful-fetch metadata tracking
- Configurable refresh interval
- Local daily API request budget
- Atomic reservation of API requests
- Provider rate-limit detection
- Blocking of subsequent requests after provider rate-limit errors
- Forced refresh option that preserves API budget restrictions

### Apache Airflow Orchestration

- DAG: `financial_market_etl`
- Task: `run_market_etl`
- Manual workflow execution
- Local offline ETL processing
- Execution history and task logs
- Task failure propagation through process exit codes
- Execution timeout configuration
- Restricted concurrency for local development
- Reusable local startup script

### Continuous Integration

- Automated workflow triggered by pushes and pull requests
- Independent code quality validation
- ETL unit test execution
- PostgreSQL integration tests using temporary database infrastructure
- Apache Airflow DAG tests in an isolated Python environment
- Database schema initialization within CI
- Automatic cleanup of temporary PostgreSQL resources
- Manual workflow execution support

### Testing and Quality

- 61 ETL unit and integration tests
- 4 Apache Airflow DAG unit tests
- 65 automated tests in total across the complete validation suites
- Ruff linting and formatting checks
- Bash syntax validation
- Successful GitHub Actions execution with four validation jobs

## Data Model

### `daily_prices`

The primary analytics table stores daily financial market observations.

| Column | Description |
|---|---|
| `symbol` | Financial instrument identifier |
| `date` | Trading date |
| `open` | Opening price |
| `high` | Highest price |
| `low` | Lowest price |
| `close` | Closing price |
| `volume` | Trading volume |
| `updated_at` | Last database modification timestamp |

The composite primary key `(symbol, date)` prevents duplicate observations for the same financial instrument and trading date.

The UPSERT implementation updates records only when their values differ from existing observations.

This supports idempotent processing and avoids unnecessary database modifications.

### Operational Metadata

The pipeline also maintains operational information through additional PostgreSQL tables.

| Table | Purpose |
|---|---|
| `daily_prices` | Validated daily financial market observations |
| `ingestion_state` | Tracks successful API fetches by asset |
| `api_request_usage` | Tracks and reserves requests against the configured daily API budget |

These tables support reliable loading, ingestion freshness checks, and controlled API consumption.

## Verified Results

The following results have been validated in the development environment and automated CI workflow.

| Validation | Result |
|---|---|
| ETL test suite | 61 passed |
| Airflow DAG tests | 4 passed |
| Total automated tests | 65 passed |
| Ruff linting | Passed |
| Ruff formatting | Passed |
| PostgreSQL integration | Passed |
| Airflow DAG execution | Success |
| AAPL observations processed | 100 |
| Modified rows during repeated execution | 0 |
| GitHub Actions jobs | 4 of 4 successful |

The successful Airflow execution used previously saved AAPL data and demonstrated that repeated ingestion does not modify unchanged database records.

A separate live API ingestion test confirmed rate-limit detection and prevention of subsequent requests after the provider limit was reached.

## Local Development

### Requirements

- Python 3.14
- Docker Engine
- Docker Compose
- Git
- Alpha Vantage API key for live extraction
- Apache Airflow 3.3.2 for local orchestration

### 1. Clone the Repository

```bash
git clone https://github.com/AlejandraGMejia/financial-market-data-pipeline.git
cd financial-market-data-pipeline
```

The repository must be accessible to the person cloning it. A private repository requires appropriate GitHub permissions.

### 2. Configure the ETL Environment

Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the project and development dependencies:

```bash
python -m pip install -e ".[dev]"
```

Create the local configuration file:

```bash
cp .env.example .env
```

Configure the required API key and PostgreSQL credentials in `.env`.

Never commit credentials or secrets.

### 3. Start PostgreSQL

On Fedora, start Docker if necessary:

```bash
sudo systemctl start docker
```

Start PostgreSQL:

```bash
sudo docker compose up -d postgres
```

Check its status:

```bash
sudo docker compose ps
```

Wait until the PostgreSQL container reports a healthy status.

The database is exposed locally through:

```text
127.0.0.1:5433
```

Apply the database schema files from the `sql/` directory in numerical order before the first pipeline execution.

The project includes:

```text
sql/
├── 001_create_daily_prices.sql
├── 002_create_ingestion_state.sql
└── 003_create_api_request_usage.sql
```

### 4. Run the ETL Pipeline

#### Offline Ingestion

Replay previously saved AAPL market data:

```bash
python run_pipeline.py --source raw --symbols AAPL
```

This mode does not consume Alpha Vantage API requests.

Previously saved raw JSON files must be available locally.

#### Live API Ingestion

Fetch AAPL market data:

```bash
python run_pipeline.py --source api --symbols AAPL
```

Process multiple assets:

```bash
python run_pipeline.py --source api --symbols AAPL MSFT NVDA
```

Live ingestion is subject to Alpha Vantage API limits and the configured local request budget.

#### Configure the Refresh Interval

```bash
python run_pipeline.py \
  --source api \
  --symbols AAPL \
  --refresh-hours 24
```

#### Configure the Daily API Budget

```bash
python run_pipeline.py \
  --source api \
  --symbols AAPL \
  --daily-api-limit 5
```

#### Force a New API Fetch

```bash
python run_pipeline.py \
  --source api \
  --symbols AAPL \
  --force
```

The `--force` option bypasses freshness checks but does not bypass the daily request budget.

## Apache Airflow Orchestration

Apache Airflow orchestrates the existing ETL entry point rather than duplicating extraction, transformation, or loading logic.

### DAG Configuration

| Property | Configuration |
|---|---|
| DAG ID | `financial_market_etl` |
| Task ID | `run_market_etl` |
| Schedule | `None` |
| Catchup | Disabled |
| Maximum active DAG runs | 1 |
| Retries | 0 |
| Task execution timeout | 5 minutes |
| Subprocess timeout | 240 seconds |
| Ingestion source | Local raw JSON |
| Default asset | AAPL |

The DAG executes the equivalent of:

```bash
python run_pipeline.py --source raw --symbols AAPL
```

The ETL runs through its dedicated Python virtual environment.

### Why Manual Scheduling?

The DAG currently uses `schedule=None` intentionally.

Manual execution allows validation of orchestration, task logging, error propagation, and database idempotency without generating unnecessary API requests.

Scheduled live ingestion can be introduced later with an execution strategy that respects provider quotas and data freshness rules.

### Airflow Environment

Apache Airflow uses a separate virtual environment:

```text
.venv-airflow/
```

This isolates orchestration dependencies from the core ETL application.

The local Airflow environment must be installed and configured before using the startup script.

For local development, Airflow uses a SQLite metadata database separate from the PostgreSQL database used by the ETL.

### Start Airflow

With PostgreSQL running and the Airflow environment configured:

```bash
./scripts/start_airflow.sh
```

The script configures the DAG directory, disables example DAGs, applies local resource limits, and starts Airflow Standalone.

Open the local dashboard:

http://127.0.0.1:8080

Sign in using the credentials configured for the local Airflow environment.

Airflow Standalone is intended for local development and demonstration rather than production deployment.

### Trigger the DAG

1. Open the Airflow dashboard.
2. Select `financial_market_etl`.
3. Enable the DAG if it is paused.
4. Trigger a manual execution.
5. Open the `run_market_etl` task.
6. Inspect its status and execution logs.

The task invokes the existing ETL pipeline and propagates nonzero process exit codes to Airflow.

### Verified Airflow Execution

The DAG was successfully triggered from the Airflow dashboard on October 28, 2026.

The task logs confirmed:

```text
Starting pipeline for AAPL
Completed AAPL: 100 records, 0 changed

AAPL: success | records=100 | changed=0

Processed: 1 | Skipped: 0 | Failed: 0 | Blocked: 0
Completed without failures: 1/1 assets
```

The execution demonstrates successful orchestration and idempotent loading of previously saved AAPL data.

### Airflow Execution Evidence

**Successful DAG Execution**

![Successful Apache Airflow DAG execution](docs/images/airflow_dag_success.png)

The screenshot shows a manually triggered DAG run completed successfully.

**ETL Task Execution Logs**

![Apache Airflow ETL task logs](docs/images/airflow_task_logs.png)

The task logs show 100 AAPL observations processed with zero modified records during repeated ingestion.

Both screenshots were captured during a successful execution of `financial_market_etl`.

## Testing

### ETL Unit Tests

```bash
python -m pytest -q
```

### Full ETL Test Suite

Execute all ETL tests, including PostgreSQL integration tests:

```bash
RUN_DB_TESTS=1 python -m pytest -q
```

Integration tests require a running PostgreSQL instance with the required schema.

### Airflow DAG Tests

Run the DAG unit tests using the separate Airflow environment:

```bash
.venv-airflow/bin/python -m unittest discover \
  -s airflow_tests \
  -p "test_*.py" \
  -v
```

These tests validate:

- DAG configuration
- Offline ETL command construction
- Missing ETL environment handling
- Subprocess failure propagation

They use mocks and do not make live API requests or load database records.

### Code Quality

Check linting:

```bash
python -m ruff check src tests airflow_tests dags run_pipeline.py
```

Check formatting:

```bash
python -m ruff format --check src tests airflow_tests dags run_pipeline.py
```

Validate the Airflow startup script:

```bash
bash -n scripts/start_airflow.sh
```

## Continuous Integration — GitHub Actions

This project uses GitHub Actions to automatically validate code quality, ETL behavior, PostgreSQL integration, and Apache Airflow DAG configuration.

The workflow is defined in:

```text
.github/workflows/ci.yml
```

It runs automatically on pushes and pull requests targeting `main`, and it can also be triggered manually.

### CI Validation Jobs

| Job | Purpose |
|---|---|
| Code Quality | Ruff linting, formatting, and Bash syntax validation |
| ETL Unit Tests | Executes ETL tests without PostgreSQL integration |
| PostgreSQL Integration Tests | Starts temporary PostgreSQL infrastructure, applies SQL scripts, and executes the full ETL test suite |
| Airflow DAG Tests | Installs Apache Airflow in an isolated environment and runs DAG unit tests |

Each job runs independently in GitHub-hosted infrastructure.

The PostgreSQL integration job creates temporary test resources and removes them after execution.

No local database connection, production credentials, or live Alpha Vantage API requests are required for CI.

### Verified CI Execution

The expanded GitHub Actions workflow completed successfully on October 28, 2026.

| Metric | Result |
|---|---|
| Workflow | Financial Market Pipeline CI |
| Commit | `5b4aa96` |
| Branch | `main` |
| Overall status | Success |
| Jobs passed | 4/4 |
| Total duration | 1 minute, 5 seconds |

Individual job durations:

| Job | Duration | Result |
|---|---|---|
| Code Quality | 9 seconds | Passed |
| ETL Unit Tests | 32 seconds | Passed |
| PostgreSQL Integration Tests | 1 minute, 2 seconds | Passed |
| Airflow DAG Tests | 43 seconds | Passed |

The workflow successfully validated the project's ETL functionality, database integration, Airflow DAG behavior, and Python code quality.

### GitHub Actions Evidence

![Successful GitHub Actions CI execution](docs/images/github_actions_success.png)

The screenshot demonstrates that all four CI validation jobs completed successfully.

### CI Status

The workflow status can be viewed in the repository's GitHub Actions section:

[View GitHub Actions](https://github.com/AlejandraGMejia/financial-market-data-pipeline/actions)

The badge at the top of this README reflects the workflow's latest available status for authorized viewers.

## Technical Reports

Detailed project documentation is available in both languages:

- [Technical Report — English](docs/financial_market_technical_report_en.md)
- [Informe técnico — Español](docs/financial_market_technical_report_es.md)

## Reliability and Design Decisions

### Idempotent Loading

The PostgreSQL UPSERT process avoids unnecessary updates when incoming records match existing observations.

This makes repeated execution safer and reduces redundant database modifications.

### Controlled API Consumption

The pipeline uses freshness checks and a local request budget to reduce unnecessary calls to a rate-limited financial API.

When a provider rate-limit response is detected, subsequent requests in the same execution are blocked.

### Error Propagation

The command-line entry point returns a nonzero exit code when an asset fails or is blocked.

Airflow uses `subprocess.run(..., check=True)` so failed executions can be reflected correctly in task status.

### Separation of Responsibilities

- **ETL Application:** extraction, transformation, validation, and loading.
- **Apache Airflow:** workflow orchestration and operational visibility.
- **PostgreSQL:** structured financial data and ingestion metadata.
- **Docker Compose:** local database infrastructure.
- **GitHub Actions:** automated code quality and test validation.
- **Git & GitHub:** version control and project tracking.

## Development Roadmap

- [x] Project structure and Python environment
- [x] Financial API client
- [x] Raw data ingestion
- [x] OHLCV transformation and validation
- [x] PostgreSQL schema and data loading
- [x] Transactional UPSERT operations
- [x] Unit and database integration testing
- [x] Multi-asset ingestion
- [x] Incremental ingestion through freshness checks
- [x] API request budgeting and provider rate-limit handling
- [x] Operational ingestion metadata
- [x] Apache Airflow DAG implementation
- [x] Airflow execution and task log validation
- [x] Airflow DAG unit tests
- [x] Reusable local Airflow startup script
- [x] Airflow execution evidence
- [x] GitHub Actions CI/CD automation
- [x] PostgreSQL integration testing in CI
- [x] Airflow DAG testing in CI
- [x] GitHub Actions execution evidence
- [ ] Final architecture diagram and documentation
- [ ] Portfolio presentation and project report

## Security

API credentials and database passwords are provided through local configuration and environment variables.

The following should remain excluded from version control:

- `.env` and other local secret files
- Python virtual environments
- Downloaded raw market datasets
- Airflow metadata and runtime files

The local Airflow dashboard is configured for localhost access.

GitHub Actions uses temporary test database credentials rather than personal or production credentials.

Real API keys and authentication passwords must never be included in commits, screenshots, or shared logs.

## Limitations

- The current Airflow DAG uses manual triggering rather than scheduled ingestion.
- The DAG relies on local project Python environments.
- Airflow Standalone and SQLite metadata storage are intended for development and demonstration.
- A fresh local installation requires separate Airflow environment setup.
- Offline replay requires previously saved raw data.
- Live API extraction is constrained by Alpha Vantage rate limits and the configured request budget.
- The CI pipeline validates software behavior but does not deploy the ETL to production.
- Production deployment, alerting, and cloud infrastructure are outside the current implementation scope.

---

## Portfolio Presentations

The project is available as an editable PowerPoint presentation in both languages:

- [English Presentation](presentations/financial_market_presentation_en.pptx)
- [Presentación en Español](presentations/financial_market_presentation_es.pptx)

## Author

**Alejandra G. Mejía**

Data Analyst | Aspiring Data Engineer

**GitHub:** [AlejandraGMejia](https://github.com/AlejandraGMejia)

**Portfolio Project 02 — Financial Market Data Pipeline**

*Designed and developed as part of a professional portfolio focused on Data Analytics, Data Engineering, and reliable data-driven systems.*
