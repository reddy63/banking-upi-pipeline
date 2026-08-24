"""
Snowflake-native ingestor.
Reads Raw Parquet for a date and loads directly into Snowflake RAW_LAYER
using COPY INTO (fastest bulk load — no pandas, no PySpark needed).
"""
import sys
from datetime import datetime
from pathlib import Path

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,
    SNOWFLAKE_ROLE, SNOWFLAKE_RAW_TABLE, SNOWFLAKE_STAGE_PATH,
)

CREATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_RAW_TABLE} (
    txn_id        VARCHAR(36)    NOT NULL,
    upi_ref       VARCHAR(50),
    sender_vpa    VARCHAR(100),
    receiver_vpa  VARCHAR(100),
    amount        NUMBER(15, 2),
    currency      VARCHAR(3)    DEFAULT 'INR',
    status        VARCHAR(20),
    txn_timestamp TIMESTAMP_NTZ,
    device_type   VARCHAR(20),
    device_id     VARCHAR(50),
    ip_address    VARCHAR(45),
    city          VARCHAR(50),
    remarks       VARCHAR(200),
    ingested_at   TIMESTAMP_NTZ,
    _source_date  DATE,
    _source       VARCHAR(10)
)
CLUSTER BY (to_date(txn_timestamp));
"""

def get_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

def load(date: str, raw_parquet_base: str) -> int:
    """
    Creates stage and table, uploads Parquet to stage, and executes COPY INTO.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Create table
    print(f"[snowflake_ingest] Creating table {SNOWFLAKE_RAW_TABLE} if not exists...")
    cur.execute(CREATE_DDL)

    # 2. Create internal stage
    stage_name = SNOWFLAKE_STAGE_PATH.lstrip('@')
    cur.execute(f"CREATE STAGE IF NOT EXISTS {stage_name} FILE_FORMAT = (TYPE='PARQUET')")
    
    # 3. Upload local parquet files to Snowflake internal stage
    # Path is: raw/source=csv/date=YYYY-MM-DD/data.parquet
    for source in ["csv", "api"]:
        parquet_file = Path(raw_parquet_base) / f"source={source}" / f"date={date}" / "data.parquet"
        if parquet_file.exists():
            cur.execute(f"PUT file://{parquet_file} {SNOWFLAKE_STAGE_PATH}/{date}/")
            print(f"[snowflake_ingest] Uploaded {parquet_file.name} to {SNOWFLAKE_STAGE_PATH}/{date}/")

    # 4. COPY INTO — Snowflake reads from internal stage
    copy_sql = f"""
    COPY INTO {SNOWFLAKE_RAW_TABLE}
    FROM (
        SELECT
            $1:txn_id::VARCHAR,
            $1:upi_ref::VARCHAR,
            $1:sender_vpa::VARCHAR,
            $1:receiver_vpa::VARCHAR,
            $1:amount::NUMBER(15,2),
            $1:currency::VARCHAR,
            $1:status::VARCHAR,
            $1:txn_timestamp::TIMESTAMP_NTZ,
            $1:device_type::VARCHAR,
            $1:device_id::VARCHAR,
            $1:ip_address::VARCHAR,
            $1:city::VARCHAR,
            $1:remarks::VARCHAR,
            $1:ingested_at::TIMESTAMP_NTZ,
            '{date}'::DATE,
            CASE WHEN METADATA$FILENAME ILIKE '%source=csv%' THEN 'csv' ELSE 'api' END
        FROM {SNOWFLAKE_STAGE_PATH}/{date}/
    )
    FILE_FORMAT = (TYPE='PARQUET')
    ON_ERROR = 'CONTINUE'
    PURGE = TRUE;
    """
    
    print("[snowflake_ingest] Executing COPY INTO...")
    
    # Ensure idempotency by deleting any existing records for this date
    delete_sql = f"DELETE FROM {SNOWFLAKE_RAW_TABLE} WHERE _source_date = '{date}'"
    print(f"[snowflake_ingest] Cleaning up existing data for {date}...")
    cur.execute(delete_sql)

    cur.execute(copy_sql)
    
    # fetchone returns a tuple with status, typically:
    # (file, status, rows_parsed, rows_loaded, error_limit, errors_seen, ...)
    # However, because we are using a SELECT transform in COPY INTO, it returns rows loaded directly or a single summary string.
    # Safe fallback if it's not returning row count cleanly:
    result = cur.fetchone()
    
    if result:
        # Check if it returned a number or summary
        if len(result) > 2 and isinstance(result[3], int):
             rows = result[3] # rows_loaded
        else:
             rows = result[0] # Try to get whatever was returned
    else:
        rows = 0

    print(f"[snowflake_ingest] COPY INTO loaded {rows} rows for {date}")
    cur.close()
    conn.close()
    return rows

if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.utcnow().strftime("%Y-%m-%d")
    raw_path = str(Path(__file__).resolve().parents[1] / "raw")
    load(date, raw_path)
