# 🏦 Banking UPI Pipeline

> **Production-grade ELT data pipeline for UPI banking transactions**  
> Built with Python · Apache Airflow · Snowflake · dbt-core

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
│                                                                     │
│  [CSV] Bank Statement Dumps      [API] Mock UPI Transaction REST    │
│  · 500 rows/day + 5 duplicates   · 300 rows/day, paginated          │
│  · bank_statement_YYYY-MM-DD.csv · Bearer token auth                │
└────────────┬────────────────────────────────┬───────────────────────┘
             │                                │
             ▼                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER (Python)                        │
│  ingestion/csv_reader.py          ingestion/api_client.py           │
│  · Header normalization           · Cursor-based pagination         │
│  · In-file deduplication          · 429 rate-limit backoff          │
│  · Writes Parquet to raw/         · 5xx exponential retry (x5)      │
└─────────────────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│              LOCAL LANDING ZONE (Apache Parquet)                    │
│  raw/source=csv/date=YYYY-MM-DD/data.parquet                        │
│  raw/source=api/date=YYYY-MM-DD/data.parquet                        │
│                                                                     │
│  raw/raw_loader.py  -- manifest: row counts + schema validation     │
└─────────────────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  SNOWFLAKE RAW LAYER (COPY INTO)                    │
│  warehouse/snowflake_ingest.py                                      │
│  · DELETE existing _source_date rows (idempotent re-runs)           │
│  · PUT Parquet to Snowflake Internal Stage                          │
│  · COPY INTO RAW_LAYER.RAW_UPI_TRANSACTIONS                         │
│  · Table CLUSTERED BY (to_date(txn_timestamp))                      │
└─────────────────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  dbt TRANSFORM LAYER (Snowflake)                    │
│                                                                     │
│  STAGING (incremental view)                                         │
│  └── stg_upi_transactions  -- dedup + normalise + boolean flags     │
│                                                                     │
│  INTERMEDIATE (ephemeral CTE)                                       │
│  ├── int_transactions_enriched -- risk features + velocity ranks    │
│  └── int_customers_current    -- latest customer profile            │
│                                                                     │
│  SNAPSHOTS (SCD Type 2)                                             │
│  └── snp_customers -- tracks tier/city/device changes over time     │
│                                                                     │
│  MARTS (daily table refresh)                                        │
│  ├── mart_transaction.fct_transactions  -- enriched fact (1/txn)    │
│  ├── mart_transaction.fct_daily_summary -- time-series KPI table    │
│  ├── mart_customer.fct_customer_activity -- per-customer 360 view   │
│  ├── mart_customer.dim_customers        -- customer dimension        │
│  ├── mart_fraud.fct_fraud_signals       -- 1 row per (txn, signal)  │
│  ├── mart_fraud.fct_fraud_scores        -- composite risk score/tier │
│  └── mart_fraud.fct_fraud_summary       -- daily fraud KPIs         │
└─────────────────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION (Apache Airflow)                     │
│  dags/banking_pipeline.py  DAG: banking_upi_pipeline                │
│  Schedule: 0 2 * * * (02:00 UTC daily)                              │
│  Executor: LocalExecutor   Retries: 2-5 per task                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Airflow DAG

```
start
  ├─ ingest_csv ─────────────────┐   PythonOperator -- CSV batch ingestion
  ├─ ingest_api ─────────────────┘   PythonOperator -- REST API polling
                               ↓
                        raw_manifest      PythonOperator -- schema + row count validation
                               ↓
                      snowflake_ingest    PythonOperator -- DELETE + PUT + COPY INTO
                               ↓
                        dbt_run_prep      BashOperator   -- staging & intermediate models
                               ↓
                        dbt_snapshot      BashOperator   -- SCD Type 2 customer snapshot
                               ↓
                        dbt_run_marts     BashOperator   -- all mart tables
                               ↓
                          dbt_test        BashOperator   -- 45+ data quality tests
                               ↓
                      pipeline_summary    PythonOperator -- XCom row count summary
                               ↓
                             end
```

**Key DAG settings:**

| Setting | Value |
|---|---|
| `schedule_interval` | `0 2 * * *` (02:00 UTC daily) |
| `start_date` | `2026-08-01` |
| `catchup` | `True` (backfills historical dates automatically) |
| `max_active_runs` | `1` |
| `retries` | `2` default, up to `5` for API tasks |
| `execution_timeout` | `2 hours` |

---

## 📊 Real Data Snapshot (Snowflake)

> Captured from a live pipeline run on **2026-08-24**.

### Raw Layer — `RAW_LAYER.RAW_UPI_TRANSACTIONS`

| _SOURCE_DATE | Row Count | Loaded Via |
|---|---|---|
| 2026-08-01 | 500 | CSV + API Parquet, COPY INTO |
| 2026-08-02 | 500 | CSV + API Parquet, COPY INTO |
| 2026-08-03 | 500 | CSV + API Parquet, COPY INTO |
| 2026-08-23 | 500 | CSV + API Parquet, COPY INTO |
| **Total** | **2,000** | **4 daily DAG runs** |

**Sample raw row:**
```
TXN_ID       : 1b5d7ea5-12bd-45ea-961d-bc4d752cf41e
UPI_REF      : UPI908296038612
SENDER_VPA   : rahul317@oksbi
RECEIVER_VPA : vikas820@oksbi
AMOUNT       : 28173.80 INR
STATUS       : SUCCESS
CITY         : Kolkata
DEVICE_TYPE  : Web
INGESTED_AT  : 2026-08-24 08:36:07
_SOURCE      : api
_SOURCE_DATE : 2026-08-23
```

---

### dbt Tables in Snowflake — `BANKING_DW`

| Schema | Table | Rows | Description |
|---|---|---|---|
| `STAGING` | `STG_UPI_TRANSACTIONS` | 500 | Cleaned, deduplicated staging (incremental) |
| `INTERMEDIATE` | `INT_TRANSACTIONS_ENRICHED` | 500 | Risk flags, velocity ranks, and time attributes added to staging |
| `INTERMEDIATE` | `INT_CUSTOMERS_CURRENT` | 500 | Latest customer profile + transaction stats |
| `SNAPSHOTS` | `SNP_CUSTOMERS` | 500 | SCD Type 2 — tracks tier/city/device changes |
| `MART_TRANSACTION` | `FCT_TRANSACTIONS` | 500 | Enriched fact — 1 row per transaction |
| `MART_TRANSACTION` | `FCT_DAILY_SUMMARY` | 369 | Aggregated daily KPIs by bank and date |
| `MART_CUSTOMER` | `FCT_CUSTOMER_ACTIVITY` | 500 | Per-customer 360 view with lifetime spend |
| `MART_CUSTOMER` | `DIM_CUSTOMERS` | 500 | Customer dimension (tier, home city, bank) |
| `MART_FRAUD` | `FCT_FRAUD_SIGNALS` | 16 | 1 row per (transaction, triggered rule) |
| `MART_FRAUD` | `FCT_FRAUD_SCORES` | 16 | Composite risk score + FRAUD_RISK_TIER |
| `MART_FRAUD` | `FCT_FRAUD_SUMMARY` | 1 | Executive fraud KPI (flagged vs. clean) |

**Sample `FCT_TRANSACTIONS` row:**
```
TXN_ID         : ea68d682-b657-4c62-b1ae-2b2bb9a14fc4
SENDER_VPA     : kavya546@okaxis
RECEIVER_VPA   : rahul239@oksbi
AMOUNT_INR     : 2347.79
STATUS         : FAILED
TXN_DATE       : 2026-08-23
DAY_PART       : EVENING
IS_CROSS_BANK  : TRUE
IS_HIGH_VALUE  : FALSE
IS_WEEKEND     : FALSE
CUSTOMER_TIER  : BRONZE
DEVICE_TYPE    : iOS
CITY           : Hyderabad
```

**Sample `FCT_FRAUD_SCORES` row:**
```
TXN_ID            : d1a36661-7c89-4961-921a-100a650e4dc9
SENDER_VPA        : anita841@upi
FRAUD_SCORE       : 0.60
SIGNALS_TRIGGERED : 1
TRIGGERED_RULES   : ["CROSS_BANK_HIGH_VALUE"]
FRAUD_RISK_TIER   : HIGH
```

---

## 🕵️ Fraud Detection Rules

The `fct_fraud_signals` model implements **5 rule-based signals**. A single transaction can trigger multiple signals, each scored independently:

| Signal | Rule | Risk Score |
|---|---|---|
| `HIGH_VALUE_OFF_HOURS` | Amount > Rs.50,000 AND hour between 00:00-05:59 | **0.85** |
| `RAPID_REPEAT_SENDER` | 3+ transactions from same sender within same hour | **0.75** |
| `ALWAYS_FAILED_SENDER` | Sender 100% failure rate with 5+ transactions that day | **0.70** |
| `HIGH_VALUE_REVERSAL` | REVERSED status AND amount > Rs.50,000 | **0.65** |
| `CROSS_BANK_HIGH_VALUE` | Cross-bank SUCCESS transfer AND amount > Rs.50,000 | **0.60** |

Scores are aggregated in `fct_fraud_scores` into a composite `FRAUD_RISK_TIER` (HIGH / MEDIUM / LOW).

---

## 📦 Data Volume Per Pipeline Run

| Source | Rows/Day | Notes |
|---|---|---|
| CSV Bank Statements | 500 + 5 duplicates | Duplicates removed at ingestion by `CSVReader` |
| UPI Transaction API | 300 | Cursor-paginated, 200 records per page max |
| **Total into RAW_LAYER** | **~800** | After dedup at source level |

**Mock data distribution (from `mock_data_generator.py`):**

| Dimension | Distribution |
|---|---|
| Transaction status | 75% SUCCESS · 15% FAILED · 8% PENDING · 2% REVERSED |
| Amount micro (<Rs.500) | 50% |
| Amount medium (Rs.500-5K) | 30% |
| Amount large (Rs.5K-50K) | 15% |
| Amount high-value (>Rs.50K) | 5% |
| UPI banks | okaxis, oksbi, okhdfcbank, okicici, paytm, ybl, upi |
| Cities | Mumbai, Delhi, Bangalore, Hyderabad, Chennai, Kolkata, Pune, Ahmedabad, Jaipur, Lucknow |
| Devices | Android, iOS, Web |

---

## 🚀 Quick Start

### 1. Clone & Install

```bash
git clone <repo>
cd banking-upi-pipeline

python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Create required local directories
mkdir -p raw/source=csv raw/source=api logs/manifest data/mock_csv data/mock_api local_db
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

Required variables:

```env
SNOWFLAKE_ACCOUNT=your_account_identifier    # e.g. xy12345.ap-southeast-1
SNOWFLAKE_USER=pipeline_user
SNOWFLAKE_PASSWORD=your_secure_password
SNOWFLAKE_ROLE=DATA_ENGINEER
SNOWFLAKE_DATABASE=BANKING_DW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

### 3. Generate Mock Data

```bash
python data/mock_data_generator.py
```

Output:
```
Generating mock data...
  CSV: bank_statement_2026-08-01.csv  (505 rows)
  ...
  API seed: api_seed_data.json  (7200 records)
Done. Mock data ready in data/mock_csv/ and data/mock_api/
```

### 4. Start Full Stack with Docker

```bash
make docker-up
```

Services started:

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Mock UPI API | http://localhost:8000/docs | Bearer dev-api-key-123 |

```bash
make docker-down    # stop + remove containers (keeps volumes)
```

### 5. Trigger a Pipeline Run

**From Airflow UI:**
1. Open http://localhost:8080
2. Find `banking_upi_pipeline` DAG → toggle **ON**
3. Click **Trigger DAG w/ config** → set execution date

**From CLI:**
```bash
docker exec banking-airflow-scheduler \
  airflow dags trigger banking_upi_pipeline --exec-date 2026-08-23
```

---

## 🛠 Makefile Commands

```bash
make install           # pip install -r requirements.txt
make generate-data     # python data/mock_data_generator.py (24 days)
make start-api         # uvicorn data.mock_api_server:app --reload --port 8000

make ingest-csv        # Run CSV ingestion for DATE=YYYY-MM-DD
make ingest-api        # Run API ingestion for DATE=YYYY-MM-DD
make raw-manifest      # Validate raw landing zone for DATE=YYYY-MM-DD

make dbt-run           # cd dbt && dbt run
make dbt-test          # cd dbt && dbt test
make dbt-docs          # Generate & serve dbt docs on port 8080

make docker-up         # Start full Docker stack
make docker-down       # Stop + remove containers + volumes
make clean             # Remove __pycache__, dbt/target, build artifacts
```

---

## 📁 Project Structure

```
banking-upi-pipeline/
├── config/
│   └── settings.py                   All env-driven config (Snowflake, API, paths)
├── ingestion/
│   ├── base_reader.py                Abstract BaseReader (read + write_raw Parquet)
│   ├── csv_reader.py                 Source 1: CSV batch ingestion + dedup
│   └── api_client.py                 Source 2: REST API, cursor pagination + retry
├── raw/
│   └── raw_loader.py                 Landing zone scanner + manifest generator
├── warehouse/
│   └── snowflake_ingest.py           PUT + COPY INTO Snowflake (idempotent)
├── dbt/
│   ├── dbt_project.yml               Project config (schemas, materializations, vars)
│   ├── profiles.yml                  Connections: dev=DuckDB, prod=Snowflake
│   ├── macros/                       audit_columns() + custom generic tests
│   ├── snapshots/
│   │   └── snp_customers.sql         SCD Type 2 snapshot
│   └── models/
│       ├── staging/
│       │   └── stg_upi_transactions.sql         Incremental, deduplicated
│       ├── intermediate/
│       │   ├── int_transactions_enriched.sql    Risk flags, velocity ranks (ephemeral)
│       │   └── int_customers_current.sql        Latest customer profile
│       └── marts/
│           ├── transaction/
│           │   ├── fct_transactions.sql         Core transaction fact table
│           │   └── fct_daily_summary.sql        Daily aggregated KPIs
│           ├── customer/
│           │   ├── dim_customers.sql            Customer dimension
│           │   └── fct_customer_activity.sql    Per-customer 360 stats
│           └── fraud/
│               ├── fct_fraud_signals.sql        1 row per (txn, triggered rule)
│               ├── fct_fraud_scores.sql         Composite risk score + tier
│               └── fct_fraud_summary.sql        Executive KPI summary
├── dags/
│   └── banking_pipeline.py           Airflow DAG (full orchestration)
├── docker/
│   ├── docker-compose.yml            Airflow + PostgreSQL + Mock API
│   └── Dockerfile.airflow            Custom image: apache/airflow:2.9.3-python3.11
├── data/
│   ├── mock_data_generator.py        Generates 24 days of CSV + API seed data
│   └── mock_api_server.py            FastAPI mock UPI server (paginated, Bearer auth)
├── requirements.txt
├── requirements-dev.txt
├── Makefile
├── .env.example
└── README.md
```

---

## 🏗️ Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow | 2.9.3 |
| Data Warehouse | Snowflake | — |
| Transformation | dbt-core + dbt-snowflake | 1.8.x |
| Ingestion | Python, Pandas, Requests | 3.11 / 2.1+ |
| Local Storage | Apache Parquet (PyArrow) | 14+ |
| Mock API | FastAPI + Uvicorn | 0.110+ |
| Containerisation | Docker + Docker Compose | — |
| Dev Warehouse | DuckDB (local `dev` dbt target) | — |

---

## ⚙️ Key Design Decisions

### Idempotent Snowflake Load
Every `snowflake_ingest` run executes:
```sql
DELETE FROM RAW_LAYER.raw_upi_transactions WHERE _source_date = '2026-08-23'
```
before `COPY INTO`. This makes every task safely re-runnable — Airflow can retry a failed task with zero risk of duplicating data in the warehouse.

### Incremental dbt Staging
`stg_upi_transactions` is materialised as an `incremental` model. On each run it only processes rows where `_source_date >= max(txn_date)` in the existing table, so compute cost stays flat as the dataset grows over months.

### SCD Type 2 Snapshot
`snp_customers` uses dbt's `strategy='check'` snapshot on `customer_tier`, `home_city`, and `primary_device_type`. This automatically creates a full history of every customer profile change, enabling true point-in-time analysis.

### Dual-Source Ingestion
Both CSV and API sources are ingested independently each day and written to separate Parquet partitions (`source=csv` / `source=api`). The `_source` column in Snowflake always preserves where each row came from.

---

## 🔒 Security Notes

- **`.env` is gitignored** — credentials are never committed to this repository
- **`.env.example`** contains only placeholder values — safe to commit, never contains real secrets
- Snowflake credentials flow into Docker containers via `env_file: ../.env` in `docker-compose.yml`
- The mock API uses a static dev-only Bearer token (`dev-api-key-123`) — replace with real auth in production
