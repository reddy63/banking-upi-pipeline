#!/usr/bin/env bash
# =============================================================================
# setup.sh — One-command environment bootstrap for banking-upi-pipeline
# Run once after cloning: bash scripts/setup.sh
# =============================================================================
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[setup]${NC} $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC}  $*"; }
error() { echo -e "${RED}[error]${NC} $*"; exit 1; }

info "Project root: $PROJECT_ROOT"

# ── 1. Python version check ────────────────────────────────────────────────────────────
PY_VER=$(python3 --version 2>&1 | grep -oP '\d+\.\d+' | head -1)
REQ_MAJOR=3; REQ_MINOR=10
ACTUAL_MAJOR=$(echo $PY_VER | cut -d. -f1)
ACTUAL_MINOR=$(echo $PY_VER | cut -d. -f2)
if [[ $ACTUAL_MAJOR -lt $REQ_MAJOR ]] || \
   [[ $ACTUAL_MAJOR -eq $REQ_MAJOR && $ACTUAL_MINOR -lt $REQ_MINOR ]]; then
    error "Python 3.10+ required. Found: $PY_VER"
fi
info "Python $PY_VER ✔"

# ── 2. Virtual environment ──────────────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
    info "Creating virtual environment .venv ..."
    python3 -m venv .venv
else
    info ".venv already exists, skipping creation"
fi

source .venv/bin/activate
info "Virtualenv activated"

# ── 3. Install Python dependencies ────────────────────────────────────────────────────
info "Installing core dependencies (requirements.txt) ..."
pip install --quiet --upgrade pip
pip install --quiet \
    pandas pyarrow requests urllib3 \
    fastapi uvicorn httpx \
    python-dotenv
info "Core dependencies installed ✔"

# Dev dependencies (pytest, etc.)
if [ -f requirements-dev.txt ]; then
    info "Installing dev dependencies ..."
    pip install --quiet -r requirements-dev.txt
    info "Dev dependencies installed ✔"
fi

# ── 4. Copy .env if it doesn’t exist ────────────────────────────────────────────────────
if [ ! -f .env ]; then
    cp .env.example .env
    warn ".env created from .env.example. Fill in your Snowflake credentials."
else
    info ".env already exists"
fi

# ── 5. Create required local directories ─────────────────────────────────────────────────
mkdir -p raw/source=csv raw/source=api \
         data/mock_csv data/mock_api \
         logs/manifest local_db
info "Local directories ready ✔"

# ── 6. Generate mock data ─────────────────────────────────────────────────────────────────
if [ ! "$(ls -A data/mock_csv 2>/dev/null)" ]; then
    info "Generating mock UPI data ..."
    python3 data/mock_data_generator.py
    info "Mock data generated ✔"
else
    info "Mock CSV data already present, skipping generation"
fi

# ── 7. Run tests ─────────────────────────────────────────────────────────────────────────────
info "Running test suite ..."
python3 -m pytest tests/ -q --tb=short 2>&1 && info "All tests passed ✔" || warn "Some tests failed — check output above"

# ── Done ──────────────────────────────────────────────────────────────────────────────────
echo ""
info "========================================="
info " Setup complete! Next steps:"
info "========================================="
echo -e "  1. Fill in .env with your Snowflake credentials"
echo -e "  2. Start the mock API:  uvicorn data.mock_api_server:app --reload --port 8000"
echo -e "  3. Run the pipeline:    bash scripts/run_pipeline.sh"
echo -e "  4. Start Airflow UI:    docker compose -f docker/docker-compose.yml up -d"
echo ""
