# 🏦 Banking UPI Pipeline (Snowflake-Native ELT)

> **Production-grade ELT data pipeline for UPI banking transactions**
> Built with Python · Apache Airflow · Snowflake · dbt

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
│                      INGESTION LAYER (Python)                                │
│  ingestion/csv_reader.py            ingestion/api_client.py                  │
│  · File detection & header norm     · Cursor pagination                      │
│  · In-file deduplication            · 429 rate-limit backoff                 │
│  · Parquet output (ISO-8601 strings)· 5xx retry with exponential backoff     │
└────────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                      LOCAL LANDING ZONE (Parquet)                            │
│  raw/source=csv/date=YYYY-MM-DD/data.parquet                                 │
│  raw/source=api/date=YYYY-MM-DD/data.parquet                                 │
│  raw/raw_loader.py  → generates raw manifest (schema & row counts)           │
└────────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                      SNOWFLAKE LOAD LAYER                                    │
│  warehouse/snowflake_ingest.py                                               │
│  · Idempotent DELETE of existing _source_date                                │
│  · PUT Parquet files to Snowflake Internal Stage                             │
│  · COPY INTO RAW_LAYER.RAW_UPI_TRANSACTIONS                                  │
└────────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                      dbt TRANSFORM LAYER (Snowflake)                         │
│  dbt/models/                                                                 │
│  ├── staging/                                                                │
│  │   └── stg_upi_transactions     (incremental — light cleaning)             │
│  ├── intermediate/                                                           │
│  │   └── int_transactions_enriched (ephemeral — risk flags, size buckets)    │
│  ├── snapshots/                                                              │
│  │   └── snp_customers             (SCD Type 2 — tracks customer profiles)   │
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
bash scripts/setup.sh
```

### 2. Configure Environment

Copy `.env.example` to `.env` and fill in your Snowflake credentials:

```bash
cp .env.example .env
```

Required variables:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE=DATA_ENGINEER`
- `SNOWFLAKE_DATABASE=BANKING_DW`

### 3. Generate mock data & Start API

```bash
python data/mock_data_generator.py
make start-api
```

### 4. Run the complete pipeline

```bash
# Runs Python ingest → Snowflake COPY INTO → dbt run → dbt snapshot → dbt test
bash scripts/run_pipeline.sh 2026-08-23
```

---

## 🐳 Docker Airflow (Full Stack)

```bash
make docker-up
# Airflow UI:   http://localhost:8080  (admin / admin)
# Mock API:     http://localhost:8000/docs
```

To shut down:
```bash
make docker-down
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
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml             ← Snowflake configuration
│   ├── macros/                  ← Custom generic tests & utilities
│   ├── snapshots/               ← SCD Type 2 definitions
│   └── models/                  ← SQL transform models
├── warehouse/
│   └── snowflake_ingest.py      ← Raw → Snowflake (PUT + COPY INTO)
├── dags/
│   └── banking_pipeline.py      ← Airflow DAG (full orchestration)
├── docker/
│   ├── docker-compose.yml       ← Airflow + Redis + Postgres + mock API
│   └── Dockerfile.airflow       ← Custom Airflow image
├── data/
│   ├── mock_data_generator.py   ← Generate CSV + API seed data
│   └── mock_api_server.py       ← FastAPI mock UPI transaction server
├── scripts/
│   ├── setup.sh                 ← Python venv setup
│   └── run_pipeline.sh          ← End-to-end execution script
├── .env.example
├── Makefile
└── README.md
```

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
                  snowflake_ingest ── (idempotent COPY INTO)
                          │
                    dbt_run_prep ── (staging & intermediate)
                          │
                    dbt_snapshot ── (SCD Type 2 customers)
                          │
                    dbt_run_marts ── (fact & dimension tables)
                          │
                       dbt_test ── (45+ data quality checks)
                          │
                   pipeline_summary
                          │
                         end
```

Schedule: **Daily at 02:00 UTC**

---

## 🏗️ Technology Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python, Pandas, Requests |
| Storage | Apache Parquet |
| Data Warehouse | Snowflake |
| Orchestration | Apache Airflow 2.9 |
| Transformation | dbt-core 1.8 |
| API Mock | FastAPI + Uvicorn |
| Containerization | Docker + Docker Compose |
