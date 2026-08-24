import os
from pathlib import Path

# ── Project root ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR  = BASE_DIR / "raw"

# ── Raw layer paths ───────────────────────────────────────────────────────────
RAW_CSV_PATH = str(RAW_DIR / "source=csv")
RAW_API_PATH = str(RAW_DIR / "source=api")

# ── Snowflake Raw Paths ────────────────────────────────────────────────────────
SNOWFLAKE_RAW_TABLE  = "RAW_LAYER.raw_upi_transactions"
SNOWFLAKE_STAGE_PATH = os.getenv("SNOWFLAKE_STAGE_PATH", "@upi_raw_stage")

# ── Mock API server ───────────────────────────────────────────────────────────
MOCK_API_BASE_URL    = os.getenv("MOCK_API_BASE_URL", "http://localhost:8000")
MOCK_API_KEY         = os.getenv("MOCK_API_KEY", "dev-api-key-123")
API_PAGE_SIZE        = int(os.getenv("API_PAGE_SIZE", "200"))
API_RATE_LIMIT_DELAY = float(os.getenv("API_RATE_LIMIT_DELAY", "0.3"))

# ── Snowflake ─────────────────────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT",   "your_account")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER",      "pipeline_user")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD",  "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE",  "BANKING_DW")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA",    "RAW_LAYER")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE",      "DATA_ENGINEER")




# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# -- PostgreSQL / BI ---------------------------------------------------------
POSTGRES_CONN = os.getenv(
    'POSTGRES_CONN',
    'postgresql://airflow:airflow@localhost:5432/banking_bi'
)
