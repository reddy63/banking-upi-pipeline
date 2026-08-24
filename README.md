# 🏦 Banking UPI Pipeline

> **Production-grade Medallion Architecture data pipeline for UPI banking transactions**
> Built with PySpark · Delta Lake · Apache Airflow · dbt · Snowflake

---

## 📐 Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                         │
│   CSV Bank Statements (daily dumps)    UPI Transaction REST API (paginated)  │
└────────────────────────┬───────────────────────────┬─────────────────────────┘
                         │                           │
                         ▼                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                                         │
│  ingestion/csv_reader.py            ingestion/api_client.py                  │
│  · File detection & header norm     · Cursor pagination                      │
│  · In-file deduplication            · 429 rate-limit backoff                 │
│  · Parquet output                   · 5xx retry with exponential backoff     │
└────────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                      RAW LAYER  (Parquet, date-partitioned)                  │
│  raw/source=csv/date=YYYY-MM-DD/data.parquet                                 │
│  raw/source=api/date=YYYY-MM-DD/data.parquet                                 │
│  raw/raw_loader.py  → schema check · row counts · manifest                  │
└────────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                   🥉 BRONZE LAYER  (Delta Lake, append)                      │
│  delta_lake/bronze/upi_transactions/                                         │
│  · Schema enforcement (StructType)                                           │
│  · Metadata: _source_date, _pipeline_run_ts, _source                        │
│  · Partition: _source_date                                                   │
│  · DQ validation: 6 checks (null rates, dup rate, VPA format, status vocab)  │
└────────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                   🥈 SILVER LAYER  (Delta Lake, append)                      │
│  delta_lake/silver/upi_transactions/                                         │
│  · Deduplication (keep latest per txn_id)                                   │
│  · Status normalization → SUCCESS | FAILED | PENDING | REVERSED             │
│  · Derived: txn_date, txn_hour, is_weekend, is_high_value, is_off_hours     │
│  · Bank extraction from VPA (sender_bank, receiver_bank)                    │
│  · Partition: txn_date                                                       │
└────────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                   🥇 GOLD LAYER  (Delta Lake, overwrite-by-partition)        │
│  delta_lake/gold/                                                             │
│  ├── daily_summary/       (per-date × bank × status × device KPIs)          │
│  ├── customer_360/        (lifetime per-VPA aggregates)                      │
│  ├── fraud_signals/       (rule-based fraud signal rows)                     │
│  └── channel_performance/ (per device_type × bank stats)                    │
└────────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        dbt TRANSFORM LAYER                                   │
│  dbt/models/                                                                 │
│  ├── staging/                                                                │
│  │   └── stg_upi_transactions     (view — light cleaning + audit cols)       │
│  ├── intermediate/                                                           │
│  │   └── int_transactions_enriched (ephemeral — size bucket, risk flags)    │
│  └── marts/                                                                  │
│      ├── customer/                                                            │
│      │   ├── dim_customers         (customer tier, home city, stats)         │
│      │   └── fct_customer_activity (daily per-customer KPIs)                │
│      ├── transaction/                                                         │
│      │   ├── fct_transactions      (enriched transaction fact)               │
│      │   └── fct_daily_summary     (time-series dashboard table)            │
│      └── fraud/                                                               │
│          ├── fct_fraud_signals     (5 rule-based signals per txn)            │
│          └── fct_fraud_summary     (daily fraud KPIs per signal type)       │
└────────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                       SNOWFLAKE DATA WAREHOUSE                               │
│  warehouse/snowflake_loader.py                                                │
│  · MERGE upsert (no duplicates in target)                                    │
│  · Clustered by (txn_date, sender_bank)                                     │
└──────────────────────────────────────────────────────────────────────────────┘

Orchestration: Apache Airflow (dags/banking_pipeline.py)
Docker: docker/docker-compose.yml (Airflow + Redis + PostgreSQL + mock API)
```

---

## 🚀 Quick Start (Local Dev)

### 1. Clone & install

```bash
git clone <repo>
cd banking-upi-pipeline
pip install -r requirements-dev.txt
```

### 2. Generate mock data

```bash
make generate-data
# Creates: data/mock_csv/bank_statement_YYYY-MM-DD.csv  (7 days)
#          data/mock_api/api_seed_data.json
```

### 3. Start the mock API server

```bash
make start-api
# FastAPI docs: http://localhost:8000/docs
```

### 4. Run the pipeline (for today)

```bash
make run-pipeline
# Runs: ingest-csv → ingest-api → raw-manifest → bronze → silver → gold → dbt
```

Or step by step:

```bash
make ingest-csv    DATE=2026-08-23
make ingest-api    DATE=2026-08-23
make raw-manifest  DATE=2026-08-23
make bronze        DATE=2026-08-23
make silver        DATE=2026-08-23
make gold          DATE=2026-08-23
make dbt-run
make dbt-test
```

### 5. Run tests

```bash
make test           # full suite
make test-unit      # skip PySpark tests (fast)
make test-cov       # with HTML coverage report
```

---

## 🐳 Docker (Full Stack)

```bash
make docker-up
# Airflow UI:   http://localhost:8080  (admin / admin)
# Mock API:     http://localhost:8000/docs
```

```bash
make docker-down   # stop & remove volumes
```

---

## 📁 Project Structure

```
banking-upi-pipeline/
├── config/
│   └── settings.py              ← All env-driven configuration
├── ingestion/
│   ├── base_reader.py           ← Abstract BaseReader (read + write_raw)
│   ├── csv_reader.py            ← Source 1: CSV batch reader
│   └── api_client.py            ← Source 2: Paginated REST API client
├── raw/
│   └── raw_loader.py            ← Landing zone scanner & manifest
├── spark/
│   ├── bronze/bronze_processor.py    ← Raw → Bronze Delta
│   ├── silver/silver_processor.py    ← Bronze → Silver Delta
│   ├── gold/gold_processor.py        ← Silver → Gold Delta
│   └── validation/data_quality.py   ← 6 DQ checks on Bronze
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml             ← dev (DuckDB) + ci/prod (Snowflake)
│   ├── macros/                  ← generate_schema_name, audit_columns, safe_divide
│   ├── tests/generic/           ← assert_positive custom test
│   └── models/
│       ├── staging/             ← stg_upi_transactions (view)
│       ├── intermediate/        ← int_transactions_enriched (ephemeral)
│       └── marts/
│           ├── customer/        ← dim_customers, fct_customer_activity
│           ├── transaction/     ← fct_transactions, fct_daily_summary
│           └── fraud/           ← fct_fraud_signals, fct_fraud_summary
├── warehouse/
│   └── snowflake_loader.py      ← Silver → Snowflake (MERGE upsert)
├── dags/
│   └── banking_pipeline.py      ← Airflow DAG (full orchestration)
├── docker/
│   ├── docker-compose.yml       ← Airflow + Redis + Postgres + mock API
│   └── Dockerfile.airflow       ← Custom Airflow image with Java + PySpark
├── tests/
│   ├── conftest.py              ← Shared fixtures
│   ├── test_csv_reader.py
│   ├── test_api_client.py
│   ├── test_bronze_processor.py
│   ├── test_silver_processor.py
│   └── test_data_quality.py
├── data/
│   ├── mock_data_generator.py   ← Generate CSV + API seed data
│   └── mock_api_server.py       ← FastAPI mock UPI transaction server
├── requirements.txt
├── requirements-dev.txt
├── setup.py
├── .env.example
├── Makefile
└── README.md
```

---

## ⚙️ Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MOCK_API_BASE_URL` | `http://localhost:8000` | Mock API server URL |
| `MOCK_API_KEY` | `dev-api-key-123` | API Bearer token |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `SNOWFLAKE_ACCOUNT` | `your_account` | Snowflake account identifier |
| `DBT_TARGET` | `dev` | dbt target (dev/ci/prod) |

---

## 🧪 Data Quality Checks

The Bronze validation layer runs **6 checks** before Silver processing:

| Check | Threshold | Severity |
|-------|-----------|----------|
| `txn_id_null_rate` | = 0% | 🔴 CRITICAL |
| `amount_null_rate` | ≤ 1% | 🔴 CRITICAL |
| `txn_id_duplicate_rate` | ≤ 2% | 🟡 WARNING |
| `amount_range` | ≤ 0.5% out of [₹0.01, ₹10L] | 🟡 WARNING |
| `valid_status_values` | ≤ 1% unknown | 🟡 WARNING |
| `sender_vpa_format` | ≤ 2% invalid VPA | 🟡 WARNING |

CRITICAL failures **stop the DAG** immediately.

---

## 🕵️ Fraud Signal Rules

The `fct_fraud_signals` dbt model implements **5 rule-based signals**:

| Signal | Rule | Score |
|--------|------|-------|
| `HIGH_VALUE_OFF_HOURS` | amount > ₹50K AND hour 0–5 AM | 0.85 |
| `RAPID_REPEAT_SENDER` | 3+ txns from same sender in 1 hour | 0.75 |
| `ALWAYS_FAILED_SENDER` | Sender 100% fail rate (≥5 daily txns) | 0.70 |
| `HIGH_VALUE_REVERSAL` | Reversed txn with amount > ₹50K | 0.65 |
| `CROSS_BANK_HIGH_VALUE` | High-value cross-bank transfer | 0.60 |

---

## 🔄 Airflow DAG

```
start
  ├─ ingest_csv ──────────┐
  ├─ ingest_api ──────────┤
                          ▼
                    raw_manifest ── (fails if schema drift)
                          │
                    bronze_processing
                          │
                    data_quality ── (fails DAG on CRITICAL errors)
                          │
                    silver_processing
                          │
                    gold_processing
                          │
                    dbt_run → dbt_test
                          │
                    snowflake_load
                          │
                    pipeline_summary
                          │
                         end
```

Schedule: **Daily at 02:00 UTC**

---

## 📊 dbt Lineage

```
stg_upi_transactions (view)
  └── int_transactions_enriched (ephemeral)
        ├── dim_customers (table)
        ├── fct_customer_activity (table)
        ├── fct_transactions (table)
        ├── fct_daily_summary (table)
        ├── fct_fraud_signals (table)
        └── fct_fraud_summary (table)
              └── stg_upi_transactions (ref)
```

---

## 🛠️ Makefile Reference

| Target | Description |
|--------|-------------|
| `make install` | Install production deps |
| `make install-dev` | Install dev+test deps |
| `make generate-data` | Generate mock CSV + API data |
| `make start-api` | Start mock FastAPI server |
| `make run-pipeline DATE=YYYY-MM-DD` | Full end-to-end pipeline |
| `make bronze DATE=...` | Run Bronze processor |
| `make silver DATE=...` | Run Silver processor |
| `make gold DATE=...` | Run Gold processor |
| `make dbt-run` | Run all dbt models |
| `make dbt-test` | Run dbt tests |
| `make dbt-docs` | Serve dbt docs on :8080 |
| `make test` | Run pytest suite |
| `make test-cov` | Run tests with coverage |
| `make lint` | Ruff linter |
| `make format` | Black formatter |
| `make docker-up` | Start Docker stack |
| `make docker-down` | Stop Docker stack |
| `make clean` | Remove artifacts |

---

## 🏗️ Technology Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python, Pandas, Requests |
| Storage | Apache Parquet, Delta Lake |
| Processing | Apache Spark (PySpark) 3.5 |
| Orchestration | Apache Airflow 2.9 |
| Transformation | dbt-core 1.8 |
| Dev DB | DuckDB |
| Production DW | Snowflake |
| API Mock | FastAPI + Uvicorn |
| Containerization | Docker + Docker Compose |
| Testing | pytest + pytest-mock |
| Linting | ruff + black |
