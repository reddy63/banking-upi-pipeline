#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — Run the full ELT pipeline locally for a given date
#
# Usage:
#   bash scripts/run_pipeline.sh                  # runs for today
#   bash scripts/run_pipeline.sh 2026-08-23       # runs for a specific date
#   bash scripts/run_pipeline.sh --skip-ingest    # skip ingestion, re-run from Snowflake load
#   bash scripts/run_pipeline.sh --dbt-only       # only run dbt (staging already loaded)
#
#   1. csv_reader     → raw/source=csv/date=DATE/data.parquet
#   2. api_client     → raw/source=api/date=DATE/data.parquet
#   3. raw_loader     → validates manifest (schema + row counts)
#   4. snowflake_ingest → DELETE + PUT + COPY INTO (idempotent)
#   5. dbt snapshot   → SCD2 snapshots
#   6. dbt run        → incremental staging, fraud scoring, marts
#   7. dbt test       → schema + singular DQ assertions
# =============================================================================
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

# Colours
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${GREEN}[pipeline]${NC} $*"; }
step()    { echo -e "${CYAN}\n─── $* ───${NC}"; }
warn()    { echo -e "${YELLOW}[warn]${NC}    $*"; }
fail()    { echo -e "${RED}[FAILED]${NC}  $*"; exit 1; }
success() { echo -e "${GREEN}[OK]${NC}      $*"; }

# ── Args ────────────────────────────────────────────────────────────────────────────────
DATE=""
SKIP_INGEST=false
DBT_ONLY=false
SKIP_SNOWFLAKE=false

for arg in "$@"; do
    case $arg in
        --skip-ingest)   SKIP_INGEST=true ;;
        --dbt-only)      DBT_ONLY=true; SKIP_INGEST=true; SKIP_SNOWFLAKE=true ;;
        --skip-snowflake) SKIP_SNOWFLAKE=true ;;
        --help|-h)
            sed -n '2,20p' "$0" | sed 's/# //'
            exit 0 ;;
        *) DATE="$arg" ;;
    esac
done

DATE="${DATE:-$(date -u +%Y-%m-%d)}"

# ── Python binary ──────────────────────────────────────────────────────────────────────────
if [ -f ".venv/bin/python3" ]; then
    PYTHON="$PROJECT_ROOT/.venv/bin/python3"
elif [ -f ".venv/bin/python" ]; then
    PYTHON="$PROJECT_ROOT/.venv/bin/python"
else
    PYTHON="python3"
    warn "No .venv found, using system python3. Run scripts/setup.sh first."
fi

# ── Load .env ──────────────────────────────────────────────────────────────────────────────
if [ -f .env ]; then
    set -o allexport
    source .env
    set +o allexport
    info ".env loaded"
else
    warn ".env not found. Snowflake load will use defaults / fail. Run setup.sh first."
fi

START_TS=$(date +%s)

echo ""
info "================================================"
info " Banking UPI Pipeline — ELT Run"
info " Date     : $DATE"
info " Mode     : skip_ingest=$SKIP_INGEST  dbt_only=$DBT_ONLY"
info "================================================"

# ── Step 1: CSV ingestion ────────────────────────────────────────────────────────────────
if [ "$SKIP_INGEST" = false ]; then
    step "Step 1/7: CSV ingestion"
    $PYTHON -m ingestion.csv_reader "$DATE" \
        && success "CSV ingestion done" \
        || fail "CSV ingestion failed"
else
    warn "Step 1/7: CSV ingestion SKIPPED"
fi

# ── Step 2: API ingestion ─────────────────────────────────────────────────────────────────
if [ "$SKIP_INGEST" = false ]; then
    step "Step 2/7: API ingestion"
    $PYTHON -m ingestion.api_client "$DATE" \
        && success "API ingestion done" \
        || fail "API ingestion failed"
else
    warn "Step 2/7: API ingestion SKIPPED"
fi

# ── Step 3: Raw manifest validation ──────────────────────────────────────────────────────
step "Step 3/7: Raw landing zone manifest"
$PYTHON -c "
import sys
sys.path.insert(0, '.')
from raw.raw_loader import RawLoader
loader = RawLoader('raw')
manifest = loader.manifest('$DATE')
print(f\"  Total rows  : {manifest['total_rows']:,}\")
print(f\"  Ready       : {manifest['ready']}\")
for src, info in manifest['sources'].items():
    print(f\"  {src:<8}: {info['row_count']:>6,} rows  schema={info['schema'].get('status', 'N/A')}\")
if not manifest['ready']:
    print('ERROR: Raw landing zone not ready', file=sys.stderr)
    sys.exit(1)
loader.save_manifest('$DATE')
" && success "Manifest validated" || fail "Manifest validation failed"

# ── Step 4: Snowflake idempotent load ──────────────────────────────────────────────────────
if [ "$SKIP_SNOWFLAKE" = false ]; then
    step "Step 4/7: Snowflake load (DELETE + PUT + COPY INTO)"
    $PYTHON warehouse/snowflake_ingest.py "$DATE" \
        && success "Snowflake load done" \
        || fail "Snowflake load failed"
else
    warn "Step 4/7: Snowflake load SKIPPED"
fi

# ── Step 5: dbt run (staging & intermediate) ───────────────────────────────────────
DBT_TARGET="${DBT_TARGET:-prod}"
step "Step 5/8: dbt run (staging/intermediate) (target=$DBT_TARGET)"
cd dbt
dbt run --exclude models/marts --target "$DBT_TARGET" --vars "{run_date: '$DATE'}" \
    && success "dbt run (staging/intermediate) complete" \
    || fail "dbt run (staging/intermediate) failed"

# ── Step 6: dbt snapshot ─────────────────────────────────────────────────────────────────────────
step "Step 6/8: dbt snapshot (target=$DBT_TARGET)"
dbt snapshot --target "$DBT_TARGET" \
    && success "dbt snapshot complete" \
    || fail "dbt snapshot failed"

# ── Step 7: dbt run (marts) ──────────────────────────────────────────────────────────────────────
step "Step 7/8: dbt run (marts) (target=$DBT_TARGET)"
dbt run --select models/marts --target "$DBT_TARGET" --vars "{run_date: '$DATE'}" \
    && success "dbt run (marts) complete" \
    || fail "dbt run (marts) failed"

# ── Step 8: dbt test ─────────────────────────────────────────────────────────────────────────────
step "Step 8/8: dbt test"
dbt test --target "$DBT_TARGET" \
    && success "dbt tests passed" \
    || fail "dbt tests failed"
cd "$PROJECT_ROOT"

# ── Summary ───────────────────────────────────────────────────────────────────────────────
ELAPSED=$(( $(date +%s) - START_TS ))
echo ""
info "================================================"
info " Pipeline complete! Date: $DATE"
info " Elapsed: ${ELAPSED}s"
info " Manifest: logs/manifest/raw_manifest_${DATE}.json"
info "================================================"
