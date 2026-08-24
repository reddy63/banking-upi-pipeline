#!/usr/bin/env python3
"""
Full pipeline health check.
Runs every layer and reports pass/fail with details.
"""
import sys
import os
import json
import traceback
from pathlib import Path
from datetime import datetime, timedelta

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

RED   = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW= '\033[1;33m'
CYAN  = '\033[0;36m'
NC    = '\033[0m'

passed = []
failed = []
warnings = []

def ok(label, detail=''):
    msg = f"{GREEN}  PASS{NC}  {label}"
    if detail: msg += f"  ({detail})"
    print(msg)
    passed.append(label)

def fail(label, err):
    print(f"{RED}  FAIL{NC}  {label}")
    print(f"         {err}")
    failed.append((label, str(err)))

def warn(label, detail):
    print(f"{YELLOW}  WARN{NC}  {label}  ({detail})")
    warnings.append((label, detail))

def section(title):
    print(f"\n{CYAN}{'='*55}{NC}")
    print(f"{CYAN} {title}{NC}")
    print(f"{CYAN}{'='*55}{NC}")


# ── CHECK 1: Imports ─────────────────────────────────────────
section("CHECK 1: Module imports")
for module in [
    'config.settings',
    'ingestion.base_reader',
    'ingestion.csv_reader',
    'ingestion.api_client',
    'raw.raw_loader',
]:
    try:
        __import__(module)
        ok(module)
    except Exception as e:
        fail(module, e)


# ── CHECK 2: Config values ────────────────────────────────────
section("CHECK 2: Config / settings")
try:
    from config.settings import (
    MOCK_API_BASE_URL, MOCK_API_KEY, API_PAGE_SIZE,
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE, POSTGRES_CONN,
)
    ok("MOCK_API_BASE_URL", MOCK_API_BASE_URL)
    ok("MOCK_API_KEY",      MOCK_API_KEY[:8] + '...')
    ok("API_PAGE_SIZE",     API_PAGE_SIZE)
    ok("SNOWFLAKE_ACCOUNT", SNOWFLAKE_ACCOUNT)
    ok("SNOWFLAKE_DATABASE",SNOWFLAKE_DATABASE)
    ok("POSTGRES_CONN",     POSTGRES_CONN[:35]+"...")

    # Check no Spark/Delta references
    import config.settings as cfg
    src = Path(cfg.__file__).read_text()
    if 'pyspark' in src.lower() or 'delta_lake' in src.lower():
        warn("settings.py", "still contains pyspark/delta_lake references")
    else:
        ok("No Spark/Delta refs in settings.py")
except Exception as e:
    fail("config.settings", e)


# ── CHECK 3: Mock data ────────────────────────────────────────
section("CHECK 3: Mock data files")
csv_dir = PROJECT / 'data' / 'mock_csv'
api_file = PROJECT / 'data' / 'mock_api' / 'api_seed_data.json'

csv_files = list(csv_dir.glob('*.csv'))
if csv_files:
    ok(f"CSV files found", f"{len(csv_files)} files")
    # Check last file row count
    import pandas as pd
    last_csv = sorted(csv_files)[-1]
    df = pd.read_csv(last_csv)
    ok(f"Last CSV readable", f"{last_csv.name}: {len(df)} rows, {len(df.columns)} cols")
    # Check for expected columns
    expected = {'txn_id','sender_vpa','receiver_vpa','amount','status','txn_timestamp'}
    missing = expected - set(df.columns)
    if missing:
        fail(f"CSV schema", f"Missing columns: {missing}")
    else:
        ok("CSV schema", f"All required columns present")
else:
    fail("CSV files", "No CSV files found in data/mock_csv/")

if api_file.exists():
    data = json.loads(api_file.read_text())
    ok("API seed file", f"{len(data)} records")
    # Spot check first record
    rec = data[0]
    expected_keys = {'txn_id','sender_vpa','receiver_vpa','amount','status'}
    missing = expected_keys - set(rec.keys())
    if missing:
        fail("API seed schema", f"Missing keys: {missing}")
    else:
        ok("API seed schema", "All required keys present")
else:
    fail("API seed file", "data/mock_api/api_seed_data.json not found")


# ── CHECK 4: CSV Reader ───────────────────────────────────────
section("CHECK 4: CSV Reader (ingestion)")
try:
    from ingestion.csv_reader import CSVReader
    TEST_DATE = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    # Find what dates actually exist
    csv_dates = sorted([
        f.name.replace('bank_statement_','').replace('.csv','')
        for f in csv_dir.glob('bank_statement_*.csv')
    ])
    if csv_dates:
        TEST_DATE = csv_dates[-1]

    reader = CSVReader(
        csv_dir=str(csv_dir),
        raw_base_path=str(PROJECT / 'raw'),
    )
    # Test file detection
    files = reader.detect_files(TEST_DATE)
    if files:
        ok(f"detect_files({TEST_DATE})", f"{len(files)} file(s) found")
    else:
        fail(f"detect_files({TEST_DATE})", "No CSV files detected")

    # Test full read
    df = reader.read(date=TEST_DATE)
    if len(df) > 0:
        ok("CSVReader.read()", f"{len(df)} rows loaded after dedup")
        ok("CSVReader schema", f"Columns: {list(df.columns[:5])}...")
    else:
        fail("CSVReader.read()", "Returned empty DataFrame")
except Exception as e:
    fail("CSVReader", traceback.format_exc(limit=3))


# ── CHECK 5: Raw Parquet files ────────────────────────────────
section("CHECK 5: Raw landing zone (Parquet)")
import pandas as pd
raw_dir = PROJECT / 'raw'
for source in ['csv', 'api']:
    source_dirs = list((raw_dir / f'source={source}').glob('date=*/data.parquet'))
    if source_dirs:
        # Read latest
        latest = sorted(source_dirs)[-1]
        try:
            df = pd.read_parquet(latest)
            ok(f"raw/source={source}", f"{latest.parent.name}: {len(df)} rows, {len(df.columns)} cols")
            # Null check on txn_id
            null_rate = df['txn_id'].isna().mean()
            if null_rate > 0:
                warn(f"txn_id nulls in {source}", f"{null_rate:.1%} null")
            else:
                ok(f"txn_id null rate ({source})", "0% nulls")
        except Exception as e:
            fail(f"raw/source={source} read", e)
    else:
        warn(f"raw/source={source}", "No parquet files found yet")


# ── CHECK 6: Raw Loader ───────────────────────────────────────
section("CHECK 6: Raw loader manifest")
try:
    from raw.raw_loader import RawLoader
    loader = RawLoader(str(PROJECT / 'raw'))
    # Use the date from CSV parquet files
    parquet_dates = sorted([
        p.parent.name.replace('date=', '')
        for p in (PROJECT / 'raw').rglob('data.parquet')
    ])
    if parquet_dates:
        # Find latest date that has actual data (not empty parquet)
        import pandas as pd
        best_date = None
        for d in sorted(set(parquet_dates), reverse=True):
            try:
                files = list((PROJECT / 'raw').rglob(f'date={d}/data.parquet'))
                total = sum(len(pd.read_parquet(fp)) for fp in files)
                if total > 0:
                    best_date = d
                    break
            except Exception:
                continue
        check_date = best_date or parquet_dates[-1]
        manifest = loader.manifest(check_date)
        ok("RawLoader.manifest()", f"date={check_date}, total_rows={manifest.get('total_rows',0):,}")
        if manifest.get('ready'):
            ok("Manifest ready", "Both sources validated")
        else:
            warn("Manifest", f"Not ready: {manifest.get('sources',{})}")
    else:
        warn("RawLoader", "No parquet dates found to check")
except Exception as e:
    fail("RawLoader", traceback.format_exc(limit=3))


# ── CHECK 7: API Client (structure, no live call) ────────────
section("CHECK 7: API client (structure check)")
try:
    from ingestion.api_client import APIClient
    import inspect
    # Check key methods exist
    for method in ['read', '_fetch_page', '_build_session']:
        if hasattr(APIClient, method):
            ok(f"APIClient.{method}()", "method exists")
        else:
            fail(f"APIClient.{method}()", "method missing")
    # Check retry + rate limit constants
    client_src = inspect.getsource(APIClient)
    for attr in ['MAX_RETRIES', 'BACKOFF_FACTOR', 'RATE_LIMIT_429_SLEEP']:
        if attr in client_src:
            ok(f"APIClient.{attr}", "defined")
        else:
            warn(f"APIClient.{attr}", "not found")
except Exception as e:
    fail("APIClient", e)


# ── CHECK 8: dbt project ──────────────────────────────────────
section("CHECK 8: dbt project files")
dbt_dir = PROJECT / 'dbt'
dbt_files = {
    'dbt_project.yml':                   dbt_dir / 'dbt_project.yml',
    'profiles.yml':                      dbt_dir / 'profiles.yml',
    'stg_upi_transactions.sql':          dbt_dir / 'models/staging/stg_upi_transactions.sql',
    'int_transactions_enriched.sql':     dbt_dir / 'models/intermediate/int_transactions_enriched.sql',
    'int_customers_current.sql':         dbt_dir / 'models/intermediate/int_customers_current.sql',
    'fct_transactions.sql':              dbt_dir / 'models/marts/transaction/fct_transactions.sql',
    'fct_daily_summary.sql':             dbt_dir / 'models/marts/transaction/fct_daily_summary.sql',
    'dim_customers.sql':                 dbt_dir / 'models/marts/customer/dim_customers.sql',
    'fct_customer_activity.sql':         dbt_dir / 'models/marts/customer/fct_customer_activity.sql',
    'fct_fraud_signals.sql':             dbt_dir / 'models/marts/fraud/fct_fraud_signals.sql',
    'fct_fraud_scores.sql':              dbt_dir / 'models/marts/fraud/fct_fraud_scores.sql',
    'fct_fraud_summary.sql':             dbt_dir / 'models/marts/fraud/fct_fraud_summary.sql',
    'snp_customers.sql (SCD2)':          dbt_dir / 'snapshots/snp_customers.sql',
    'assert_amount_positive.sql':        dbt_dir / 'tests/assert_amount_positive.sql',
    'assert_no_future_transactions.sql': dbt_dir / 'tests/assert_no_future_transactions.sql',
    'audit_columns.sql (macro)':         dbt_dir / 'macros/audit_columns.sql',
}
for label, path in dbt_files.items():
    if path.exists():
        size = path.stat().st_size
        ok(label, f"{size} bytes")
    else:
        fail(label, f"File not found: {path}")

# Check stg is incremental
stg = (dbt_dir / 'models/staging/stg_upi_transactions.sql').read_text()
if 'incremental' in stg and 'is_incremental' in stg:
    ok("stg_upi_transactions incremental", "materialized=incremental + is_incremental() filter")
else:
    fail("stg_upi_transactions incremental", "Missing incremental config or filter")

# Check SCD2 snapshot config
snp = (dbt_dir / 'snapshots/snp_customers.sql').read_text()
if 'strategy' in snp and 'check' in snp and 'check_cols' in snp:
    ok("snp_customers SCD2", "strategy=check with check_cols defined")
else:
    fail("snp_customers SCD2", "Missing check strategy or check_cols")

# Check fraud signals has all 5 signals
for signal in ['HIGH_VALUE_OFF_HOURS','RAPID_REPEAT_SENDER','ALWAYS_FAILED_SENDER',
               'HIGH_VALUE_REVERSAL','CROSS_BANK_HIGH_VALUE']:
    signals_sql = (dbt_dir / 'models/marts/fraud/fct_fraud_signals.sql').read_text()
    if signal in signals_sql:
        ok(f"Fraud signal: {signal}")
    else:
        fail(f"Fraud signal: {signal}", "Not found in fct_fraud_signals.sql")

# Check fraud score risk tiers
scores_sql = (dbt_dir / 'models/marts/fraud/fct_fraud_scores.sql').read_text()
for tier in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
    if tier in scores_sql:
        ok(f"Risk tier: {tier}")
    else:
        fail(f"Risk tier: {tier}", "Not found in fct_fraud_scores.sql")


# ── CHECK 9: DAG file ─────────────────────────────────────────
section("CHECK 9: Airflow DAG")
dag_file = PROJECT / 'dags' / 'banking_pipeline.py'
import re as _re
dag_src = dag_file.read_text()

# Check no Spark tasks
for spark_ref in ['bronze_processing','silver_processing','PySpark','spark-submit']:
    if spark_ref in dag_src:
        fail(f"DAG Spark reference", f"'{spark_ref}' still in dag file")
    else:
        ok(f"No '{spark_ref}' in DAG")

# Check correct ELT tasks
for task in ['ingest_csv','ingest_api','raw_manifest','snowflake_ingest','dbt_run_prep','dbt_snapshot','dbt_run_marts','dbt_test','pipeline_summary']:
    if (('task_id' in dag_src and task in dag_src):
        ok(f"DAG task: {task}")
    else:
        fail(f"DAG task: {task}", "task_id not found")

# Check correct dependency chain
if '[ingest_csv, ingest_api] >> raw_manifest' in dag_src:
    ok("DAG dependency: parallel ingest >> manifest")
else:
    fail("DAG dependency", "parallel ingest >> manifest pattern not found")

if 'snowflake_ingest >> dbt_run_prep >> dbt_snapshot >> dbt_run_marts >> dbt_test' in dag_src:
    ok("DAG dependency: snowflake >> prep >> snapshot >> marts >> test")
else:
    fail("DAG dependency", "chain pattern not found")


# ── CHECK 10: Snowflake ingest (structure) ────────────────────
section("CHECK 10: Snowflake ingest (structure)")
sf_src = (PROJECT / 'warehouse' / 'snowflake_ingest.py').read_text()

for check, pattern in [
    ("DELETE before COPY (idempotency)",   "DELETE FROM"),
    ("PUT command",                         "PUT file://"),
    ("COPY INTO command",                   "COPY INTO"),
    ("Internal stage creation",             "CREATE STAGE"),
    ("ON_ERROR CONTINUE",                   "ON_ERROR"),
    ("PURGE after load",                    "PURGE"),
    ("Source detection from filename",      "METADATA$FILENAME"),
]:
    if pattern in sf_src:
        ok(check)
    else:
        fail(check, f"Pattern '{pattern}' not found in snowflake_ingest.py")


# ── CHECK 11: Docker files ────────────────────────────────────
section("CHECK 11: Docker")
for f in ['docker/docker-compose.yml', 'docker/Dockerfile.airflow']:
    path = PROJECT / f
    if path.exists():
        ok(f, f"{path.stat().st_size} bytes")
    else:
        fail(f, "File not found")

docker_src = (PROJECT / 'docker' / 'docker-compose.yml').read_text()
for svc in ['airflow', 'postgres', 'mock-api']:
    if svc in docker_src:
        ok(f"Docker service: {svc}")
    else:
        warn(f"Docker service: {svc}", "Not found in compose file")

# Confirm no Spark service in Docker
for spark_svc in ['spark-master', 'spark-worker', 'bitnami/spark']:
    if spark_svc in docker_src:
        fail(f"Docker has Spark service", f"'{spark_svc}' found — remove it")
    else:
        ok(f"No Spark in Docker ({spark_svc})")


# ── CHECK 12: Unit tests ──────────────────────────────────────
section("CHECK 12: Unit tests")
import subprocess
result = subprocess.run(
    ['python3', '-m', 'pytest', 'tests/', '-v', '--tb=short', '-q'],
    capture_output=True, text=True,
    cwd=str(PROJECT)
)
print(result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout)
if result.returncode == 0:
    ok("pytest tests/", "All tests passed")
else:
    fail("pytest tests/", "Some tests failed")
    if result.stderr:
        print(result.stderr[-1000:])


# ── CHECK 13: Scripts ─────────────────────────────────────────
section("CHECK 13: Scripts")
for script in ['scripts/setup.sh', 'scripts/run_pipeline.sh']:
    path = PROJECT / script
    if path.exists():
        mode = oct(path.stat().st_mode)[-3:]
        executable = path.stat().st_mode & 0o111
        if executable:
            ok(script, f"exists + executable (mode {mode})")
        else:
            warn(script, "exists but not executable")
    else:
        fail(script, "Not found")


# ── SUMMARY ───────────────────────────────────────────────────
section("FULL PIPELINE HEALTH SUMMARY")
print(f"\n  {GREEN}PASSED :{NC} {len(passed)}")
print(f"  {YELLOW}WARNINGS:{NC} {len(warnings)}")
print(f"  {RED}FAILED :{NC} {len(failed)}")

if warnings:
    print(f"\n{YELLOW}Warnings:{NC}")
    for label, detail in warnings:
        print(f"  {YELLOW}!{NC} {label}: {detail}")

if failed:
    print(f"\n{RED}Failures:{NC}")
    for label, err in failed:
        print(f"  {RED}x{NC} {label}: {err}")
    sys.exit(1)
else:
    print(f"\n{GREEN}All checks passed. Pipeline is healthy.{NC}")