import re
import subprocess
import pandas as pd
from pathlib import Path

P = Path('.')
passed = []
failed = []

G = '\033[0;32m'
R = '\033[0;31m'
C = '\033[0;36m'
N = '\033[0m'

def ok(msg):
    print(f'{G}  PASS{N}  {msg}')
    passed.append(msg)

def fail(msg, why=''):
    print(f'{R}  FAIL{N}  {msg}' + (f'  -- {why}' if why else ''))
    failed.append(msg)

def section(t):
    print(f'\n{C}=== {t} ==={N}')


# ------------------------------------------------------------------ DAG
section('DAG TASKS & DEPENDENCIES')
dag = (P / 'dags/banking_pipeline.py').read_text()

for task in ['ingest_csv', 'ingest_api', 'raw_manifest', 'snowflake_ingest',
             'dbt_run_prep', 'dbt_snapshot', 'dbt_run_marts', 'dbt_test',
             'pipeline_summary']:
    pat = r'task_id\s*=\s*["\']' + re.escape(task) + r'["\']'
    if re.search(pat, dag):
        ok(f'task: {task}')
    else:
        fail(f'task: {task}', 'not found in DAG')

chain = 'snowflake_ingest >> dbt_run_prep >> dbt_snapshot >> dbt_run_marts >> dbt_test'
if chain in dag:
    ok('dependency chain correct')
else:
    fail('dependency chain', 'expected: snowflake_ingest >> dbt_run_prep >> dbt_snapshot >> dbt_run_marts >> dbt_test')

for spark in ['bronze_processing', 'silver_processing', 'PySpark', 'spark-submit']:
    if spark not in dag:
        ok(f'No {spark!r} in DAG')
    else:
        fail(f'{spark!r} found in DAG', 'must be removed')


# ------------------------------------------------------------------ SETTINGS
section('CONFIG / SETTINGS')
st = (P / 'config/settings.py').read_text()

for var in ['MOCK_API_BASE_URL', 'MOCK_API_KEY', 'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_DATABASE', 'SNOWFLAKE_RAW_TABLE', 'SNOWFLAKE_STAGE_PATH',
            'POSTGRES_CONN']:
    if var in st:
        ok(f'settings.{var}')
    else:
        fail(f'settings.{var}', 'missing')

for bad in ['SPARK_MASTER', 'DELTA_BASE', 'BRONZE_PATH', 'SILVER_PATH']:
    if bad not in st:
        ok(f'No {bad!r} (Spark/Delta removed)')
    else:
        fail(f'{bad!r} still in settings', 'old architecture remnant')


# ------------------------------------------------------------------ SNOWFLAKE INGEST
section('SNOWFLAKE INGEST (warehouse/snowflake_ingest.py)')
sf = (P / 'warehouse/snowflake_ingest.py').read_text()

for label, pat in [
    ('Idempotent DELETE before COPY',     'DELETE FROM'),
    ('PUT files to stage',                'PUT file://'),
    ('COPY INTO command',                 'COPY INTO'),
    ('Internal stage creation',           'CREATE STAGE'),
    ('ON_ERROR CONTINUE',                 'ON_ERROR'),
    ('PURGE staged files after load',     'PURGE'),
    ('Source detection via filename',     'METADATA$FILENAME'),
]:
    if pat in sf:
        ok(label)
    else:
        fail(label, f'{pat!r} not found')


# ------------------------------------------------------------------ DBT
section('DBT MODELS')
stg = (P / 'dbt/models/staging/stg_upi_transactions.sql').read_text()

for label, pat in [
    ('Staging: incremental materialised', "materialized = 'incremental'"),
    ('Staging: is_incremental() filter',  'is_incremental()'),
    ('Staging: row_number dedup',         'row_number() over'),
    ('Staging: status CASE normalisation','case upper(trim(status))'),
    ('Staging: sender_bank extraction',   'regexp_substr'),
]:
    if pat in stg:
        ok(label)
    else:
        fail(label, f'{pat!r} not found')

snp = (P / 'dbt/snapshots/snp_customers.sql').read_text()
for label, pat in [
    ("Snapshot: SCD2 check strategy",    "strategy='check'"),
    ('Snapshot: check_cols defined',      'check_cols'),
    ('Snapshot: unique_key=sender_vpa',   'sender_vpa'),
]:
    if pat in snp:
        ok(label)
    else:
        fail(label, f'{pat!r} not found')

sig = (P / 'dbt/models/marts/fraud/fct_fraud_signals.sql').read_text()
for signal in ['HIGH_VALUE_OFF_HOURS', 'RAPID_REPEAT_SENDER',
               'ALWAYS_FAILED_SENDER', 'HIGH_VALUE_REVERSAL',
               'CROSS_BANK_HIGH_VALUE']:
    if signal in sig:
        ok(f'Fraud signal: {signal}')
    else:
        fail(f'Fraud signal: {signal}', 'missing from fct_fraud_signals.sql')

scr = (P / 'dbt/models/marts/fraud/fct_fraud_scores.sql').read_text()
for tier in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
    if tier in scr:
        ok(f'Risk tier: {tier}')
    else:
        fail(f'Risk tier: {tier}', 'missing from fct_fraud_scores.sql')

macro = (P / 'dbt/macros/audit_columns.sql').read_text()
for label, pat in [
    ('macro: audit_columns()',  'macro audit_columns'),
    ('macro: safe_divide()',    'macro safe_divide'),
    ('macro: is_valid_vpa()',   'macro is_valid_vpa'),
]:
    if pat in macro:
        ok(label)
    else:
        fail(label, 'not found')


# ------------------------------------------------------------------ DOCKER
section('DOCKER')
dc = (P / 'docker/docker-compose.yml').read_text()

for svc in ['airflow', 'postgres', 'mock-api']:
    if svc in dc:
        ok(f'Service: {svc}')
    else:
        fail(f'Service: {svc}', 'missing from docker-compose.yml')

for bad in ['spark-master', 'spark-worker', 'bitnami/spark']:
    if bad not in dc:
        ok(f'No Spark in Docker ({bad})')
    else:
        fail(f'Spark found in Docker', f'{bad!r} present -- remove it')


# ------------------------------------------------------------------ UNIT TESTS
section('UNIT TESTS (pytest)')
r = subprocess.run(
    ['python3', '-m', 'pytest', 'tests/', '-v', '--tb=short'],
    capture_output=True, text=True, cwd=str(P)
)
print(r.stdout[-1500:])
if r.returncode == 0:
    ok('All unit tests passed')
else:
    fail('Unit tests', 'one or more tests failed')


# ------------------------------------------------------------------ RAW PARQUET
section('RAW PARQUET LANDING ZONE')
for source in ['csv', 'api']:
    files = sorted((P / 'raw').rglob(f'source={source}/*/data.parquet'))
    if files:
        df = pd.read_parquet(files[-1])
        null_count = int(df['txn_id'].isna().sum())
        ok(f'raw/source={source}: {files[-1].parent.name}, {len(df)} rows, '
           f'{len(df.columns)} cols, txn_id nulls={null_count}')
    else:
        fail(f'raw/source={source}', 'no parquet files found')


# ------------------------------------------------------------------ MOCK DATA
section('MOCK DATA')
csv_files = list((P / 'data/mock_csv').glob('bank_statement_*.csv'))
if csv_files:
    ok(f'CSV mock files: {len(csv_files)} files covering {len(csv_files)} days')
    df = pd.read_csv(sorted(csv_files)[-1])
    ok(f'Latest CSV: {len(df)} rows, {len(df.columns)} columns')
    dupes = len(df) - df['txn_id'].nunique()
    ok(f'Injected duplicates in latest CSV: {dupes} (expected ~5)')
else:
    fail('CSV mock files', 'no files found in data/mock_csv/')

import json
api_seed = P / 'data/mock_api/api_seed_data.json'
if api_seed.exists():
    records = json.loads(api_seed.read_text())
    ok(f'API seed: {len(records)} records')
    statuses = {}
    for r in records:
        statuses[r.get('status', '?')] = statuses.get(r.get('status', '?'), 0) + 1
    ok(f'Status distribution: {statuses}')
else:
    fail('API seed', 'data/mock_api/api_seed_data.json not found')


# ------------------------------------------------------------------ SCRIPTS
section('SCRIPTS')
for script in ['scripts/setup.sh', 'scripts/run_pipeline.sh']:
    path = P / script
    if path.exists():
        is_exec = path.stat().st_mode & 0o111
        if is_exec:
            ok(f'{script} (executable)')
        else:
            fail(f'{script}', 'not executable -- run: chmod +x ' + script)
    else:
        fail(f'{script}', 'file not found')


# ------------------------------------------------------------------ SUMMARY
print(f'\n{C}================================================={N}')
print(f'{G}  PASSED : {len(passed)}{N}')
print(f'{R}  FAILED : {len(failed)}{N}')
print(f'{C}================================================={N}')

if failed:
    print(f'\n{R}Failures to fix:{N}')
    for item in failed:
        print(f'  x  {item}')
    raise SystemExit(1)
else:
    print(f'\n{G}Pipeline is fully healthy. Ready to push to GitHub.{N}')
