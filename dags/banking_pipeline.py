"""
Banking UPI Pipeline — Airflow DAG
====================================
ELT pipeline: Python ingestion → Snowflake RAW_LAYER → dbt transforms.
Architecture: Snowflake-native ELT.

DAG Flow:
  start
    ├─ ingest_csv         [PythonOperator]  CSV batch reader → raw/ Parquet
    ├─ ingest_api         [PythonOperator]  REST API poller  → raw/ Parquet
        └─ raw_manifest   [PythonOperator]  Schema check + row count manifest
              └─ snowflake_ingest  [PythonOperator]  DELETE + PUT + COPY INTO (idempotent load)
                    └─ dbt_run   [BashOperator]  Incremental staging, SCD2, fraud scoring, marts
                          └─ dbt_test  [BashOperator]  Schema tests + singular DQ assertions
                                └─ pipeline_summary  [PythonOperator]  XCom summary report
                                      └─ end

Key design decisions:
  - Idempotent: DELETE for _source_date before every COPY INTO — safe to re-run
  - Incremental: dbt staging only processes new _source_date rows
  - SCD2: dbt snapshot tracks customer risk tier & device changes over time
  - Fraud: rule-based scoring (velocity, off-hours, high-value) in fct_fraud_scores

Schedule : Daily at 02:00 UTC (after midnight transaction cutoff)
Owner    : data-engineering
Tags     : banking, upi, elt, snowflake, dbt
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# ── Project paths (injected via env var or resolved relative to dags/) ─────────
PROJECT_ROOT = Path(os.environ.get("PIPELINE_ROOT", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(PROJECT_ROOT))

PYTHON_BIN   = os.environ.get("PIPELINE_PYTHON", sys.executable)
DBT_DIR      = str(PROJECT_ROOT / "dbt")
DBT_TARGET   = os.environ.get("DBT_TARGET", "prod")

# ── Default DAG args ────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            [os.environ.get("ALERT_EMAIL", "de-alerts@banking.internal")],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout":timedelta(hours=2),
}


# ─── Task callables ─────────────────────────────────────────────────────────────

def _ingest_csv(**context):
    """Run CSV batch ingestion for the execution date."""
    from ingestion.csv_reader import CSVReader
    date = context["ds"]  # YYYY-MM-DD from Airflow
    reader = CSVReader(
        csv_dir       = str(PROJECT_ROOT / "data" / "mock_csv"),
        raw_base_path = str(PROJECT_ROOT / "raw"),
    )
    path = reader.run(date=date)
    context["task_instance"].xcom_push(key="csv_raw_path", value=str(path))
    print(f"[ingest_csv] Done for {date}: {path}")


def _ingest_api(**context):
    """Poll the UPI Transaction API for the execution date."""
    from config.settings import MOCK_API_BASE_URL, MOCK_API_KEY
    from ingestion.api_client import APIClient

    date = context["ds"]
    client = APIClient(
        base_url      = MOCK_API_BASE_URL,
        api_key       = MOCK_API_KEY,
        raw_base_path = str(PROJECT_ROOT / "raw"),
    )
    path = client.run(from_date=date, to_date=date, date=date)
    context["task_instance"].xcom_push(key="api_raw_path", value=str(path))
    print(f"[ingest_api] Done for {date}: {path}")


def _raw_manifest(**context):
    """Generate raw landing zone manifest for the execution date."""
    from raw.raw_loader import RawLoader

    date   = context["ds"]
    loader = RawLoader(str(PROJECT_ROOT / "raw"))
    manifest = loader.manifest(date)
    manifest_path = loader.save_manifest(date)

    context["task_instance"].xcom_push(key="raw_manifest", value=manifest)
    context["task_instance"].xcom_push(key="manifest_path", value=str(manifest_path))

    if not manifest["ready"]:
        raise ValueError(
            f"Raw landing zone NOT ready for {date}. "
            f"Missing data or schema drift detected."
        )
    print(f"[raw_manifest] Ready for {date}: {manifest['total_rows']} total rows")


def _snowflake_ingest(**context):
    """Load Raw Parquet data directly into Snowflake via COPY INTO."""
    from warehouse.snowflake_ingest import load

    date   = context["ds"]
    raw_path = str(PROJECT_ROOT / "raw")
    rows = load(date, raw_path)
    context["task_instance"].xcom_push(key="snowflake_rows", value=rows)
    print(f"[snowflake_ingest] Loaded {rows} rows for {date}")


def _pipeline_summary(**context):
    """Emit a final summary log/metric for the pipeline run."""
    ti   = context["task_instance"]
    date = context["ds"]

    csv_path       = ti.xcom_pull(task_ids="ingest_csv",   key="csv_raw_path")
    api_path       = ti.xcom_pull(task_ids="ingest_api",   key="api_raw_path")
    manifest       = ti.xcom_pull(task_ids="raw_manifest", key="raw_manifest") or {}
    snowflake_rows = ti.xcom_pull(task_ids="snowflake_ingest", key="snowflake_rows")

    summary = f"""
╔══════════════════════════════════════════════════════╗
║         Banking UPI Pipeline Summary — {date}         ║
╚══════════════════════════════════════════════════════╝
  Raw total rows  : {manifest.get('total_rows', 'N/A'):,}
  Snowflake rows  : {snowflake_rows or 'N/A'}
  CSV landing     : {csv_path}
  API landing     : {api_path}
"""
    print(summary)


# ─── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id          = "banking_upi_pipeline",
    default_args    = default_args,
    description     = "End-to-end UPI banking data pipeline (Snowflake-native ELT)",
    schedule_interval = "0 2 * * *",   # 02:00 UTC daily
    start_date      = datetime(2026, 8, 1),
    catchup         = True,
    max_active_runs = 1,
    tags            = ["banking", "upi", "pipeline", "snowflake"],
    doc_md          = __doc__,
) as dag:

    # ── Start sentinel ────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Ingestion ─────────────────────────────────────────────────────────────
    ingest_csv = PythonOperator(
        task_id         = "ingest_csv",
        python_callable = _ingest_csv,
        retries         = 3,
        doc_md          = "Read daily bank statement CSV dumps from data/mock_csv/",
    )

    ingest_api = PythonOperator(
        task_id         = "ingest_api",
        python_callable = _ingest_api,
        retries         = 5,
        retry_delay     = timedelta(minutes=2),
        doc_md          = "Poll UPI Transaction API with cursor pagination",
    )

    # ── Raw manifest ──────────────────────────────────────────────────────────
    raw_manifest = PythonOperator(
        task_id         = "raw_manifest",
        python_callable = _raw_manifest,
        doc_md          = "Validate raw landing zone schema and row counts",
    )

    # ── Snowflake Ingest ──────────────────────────────────────────────────────
    snowflake_ingest = PythonOperator(
        task_id         = "snowflake_ingest",
        python_callable = _snowflake_ingest,
        doc_md          = "Load Raw Parquet directly into Snowflake via COPY INTO",
    )

    # ── dbt run (marts) ───────────────────────────────────────────────────────
    dbt_run_marts = BashOperator(
        task_id      = "dbt_run_marts",
        bash_command = (
            f"cd {DBT_DIR} && "
            f"dbt run --select models/marts --target {DBT_TARGET} --vars '{{\"run_date\": \"{{{{ ds }}}}\"}}' "
            f"--profiles-dir {DBT_DIR}"
        ),
        doc_md       = "Run final marts that depend on the snapshot",
    )

    # ── dbt run (staging & intermediate) ──────────────────────────────────────
    dbt_run_prep = BashOperator(
        task_id      = "dbt_run_prep",
        bash_command = (
            f"cd {DBT_DIR} && "
            f"dbt run --exclude models/marts --target {DBT_TARGET} --vars '{{\"run_date\": \"{{{{ ds }}}}\"}}' "
            f"--profiles-dir {DBT_DIR}"
        ),
        doc_md       = "Run staging and intermediate models before snapshot",
    )

    # ── dbt snapshot ──────────────────────────────────────────────────────────
    dbt_snapshot = BashOperator(
        task_id      = "dbt_snapshot",
        bash_command = (
            f"cd {DBT_DIR} && "
            f"dbt snapshot --target {DBT_TARGET} --profiles-dir {DBT_DIR}"
        ),
        doc_md       = "Run SCD Type 2 snapshots for customers",
    )

    # ── dbt test ──────────────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id      = "dbt_test",
        bash_command = (
            f"cd {DBT_DIR} && "
            f"dbt test --target {DBT_TARGET} --profiles-dir {DBT_DIR}"
        ),
        doc_md       = "Run all dbt generic and schema tests",
    )


    # ── Summary ───────────────────────────────────────────────────────────────
    pipeline_summary = PythonOperator(
        task_id         = "pipeline_summary",
        python_callable = _pipeline_summary,
        trigger_rule    = "all_done",   # run even if Snowflake had issues
        doc_md          = "Print pipeline run summary with row counts and DQ status",
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_success")

    # ── Task dependencies ─────────────────────────────────────────────────────
    start >> [ingest_csv, ingest_api] >> raw_manifest
    raw_manifest >> snowflake_ingest >> dbt_run_prep >> dbt_snapshot >> dbt_run_marts >> dbt_test
    dbt_test >> pipeline_summary >> end
