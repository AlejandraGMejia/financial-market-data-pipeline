
# Financial Market Data Pipeline

An automated-ready financial market ETL pipeline built with Python,
PostgreSQL, Docker, and automated data quality tests.

> **Project Status:** In active development — ETL core implemented.
> Multi-asset ingestion, scheduling, and monitoring are planned.

## Project Overview

This project implements a modular ETL pipeline for collecting,
validating, and storing daily financial market data.

The system retrieves OHLCV data from Alpha Vantage, preserves
raw JSON responses, validates financial data quality, and loads
structured records into PostgreSQL.

The goal is to build a reliable, reproducible data engineering
solution that can evolve into a fully automated pipeline.

## Business Problem

Financial analysts require consistent, accurate, and accessible
historical market data.

Manual data collection introduces operational risks, including:

- Inconsistent data formats
- Duplicate records
- Missing or invalid financial observations
- Time-consuming repetitive work
- Limited traceability of data processing

This project addresses these challenges through modular extraction,
validation, transactional loading, and automated testing.

## Architecture

```text
Alpha Vantage API
       |
       v
Python HTTP Client
       |
       v
Raw JSON Storage
       |
       v
OHLCV Transformation
& Data Quality Validation
       |
       v
PostgreSQL Database
       |
       v
SQL Analytics
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.14 | ETL development |
| Requests | Financial API integration |
| pandas | Data transformation |
| Decimal | Financial price precision |
| PostgreSQL 18 | Relational data storage |
| SQLAlchemy | Database connectivity and UPSERT |
| Docker Compose | Local database infrastructure |
| Pytest | Unit and integration testing |
| Ruff | Code quality and formatting |
| Git | Version control |

## Implemented Features

- Alpha Vantage daily market data extraction
- Raw JSON persistence
- Financial OHLCV validation
- Chronological transformation into tabular data
- PostgreSQL schema with integrity constraints
- Transactional UPSERT operations
- Idempotent loading for unchanged records
- Per-asset error handling
- Local JSON replay mode
- Automated unit and database integration tests

## Data Model

The `daily_prices` table stores:

| Column | Description |
|---|---|
| symbol | Financial instrument |
| date | Trading date |
| open | Opening price |
| high | Highest price |
| low | Lowest price |
| close | Closing price |
| volume | Trading volume |
| updated_at | Last database modification |

The composite primary key `(symbol, date)` prevents duplicate
observations for the same asset and trading date.

## Verified Results

Current validated development results:

- 41 automated tests passing
- Ruff lint checks passing
- 100 AAPL daily records extracted and validated
- 100 AAPL records stored in PostgreSQL
- Repeated unchanged loads produce zero modified rows
- Database rollback behavior validated through integration tests

## Local Development

### Requirements

- Python 3.14
- Docker Engine and Docker Compose
- Git
- Alpha Vantage API key for live extraction

### Setup

Clone the repository and enter the project directory.

Create a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the project:

```bash
python -m pip install -e ".[dev]"
```

Create a local configuration file:

```bash
cp .env.example .env
```

Configure your own API key and PostgreSQL credentials in `.env`.

Never commit real credentials.

### Start PostgreSQL

```bash
docker compose up -d postgres
```

Apply the database schema:

```bash
docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 \
  -U "$POSTGRES_USER" -d financial_market \
  < sql/001_create_daily_prices.sql
```

The application connects to PostgreSQL through
`127.0.0.1:5433`.

### Run the Pipeline

Extract AAPL from Alpha Vantage:

```bash
python run_pipeline.py --source api --symbols AAPL
```

Replay previously downloaded local data:

```bash
python run_pipeline.py --source raw --symbols AAPL
```

### Run Tests

Unit tests:

```bash
python -m pytest -q
```

Full test suite, including PostgreSQL integration tests:

```bash
RUN_DB_TESTS=1 python -m pytest -q
```

Code quality checks:

```bash
python -m ruff check src tests run_pipeline.py
python -m ruff format --check src tests run_pipeline.py
```

Database integration tests require a running PostgreSQL
container with the schema already created.

## Development Roadmap

- [x] Project structure and environment
- [x] Financial API client
- [x] Raw data ingestion
- [x] OHLCV transformation and validation
- [x] PostgreSQL schema and data loading
- [x] Transactional UPSERT and integration tests
- [x] Initial pipeline orchestration
- [ ] Multi-asset ingestion
- [ ] Incremental processing strategy
- [ ] Operational metadata and monitoring
- [ ] Apache Airflow orchestration
- [ ] CI/CD automation
- [ ] Final architecture documentation

## Security

API keys and database passwords are loaded from local
environment variables.

The `.env` file and downloaded raw datasets are excluded
from version control.

This repository is intended for educational and portfolio
demonstration purposes.

## Author

Data Engineering Portfolio Project — Project 02
