# Banking UPI Pipeline — Makefile
# Convenience commands for local development

PYTHON     := python3
PIP        := pip3
DATE       ?= $(shell date -u +%Y-%m-%d)
PYTEST     := pytest
DBT_TARGET ?= dev

.PHONY: help install install-dev generate-data start-api \
        ingest-csv ingest-api raw-manifest \
        dbt-run dbt-test dbt-docs \
        test test-unit test-cov lint format \
        docker-up docker-down clean

## ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Banking UPI Pipeline — Available targets:"
	@echo ""
	@echo "  Setup"
	@echo "    install          Install production dependencies"
	@echo "    install-dev      Install dev+test dependencies"
	@echo ""
	@echo "  Data & Servers"
	@echo "    generate-data    Generate mock CSV + API seed data"
	@echo "    start-api        Start mock FastAPI server (port 8000)"
	@echo ""
	@echo "  Pipeline steps (DATE=YYYY-MM-DD)"
	@echo "    ingest-csv       Run CSV batch ingestion"
	@echo "    ingest-api       Run API polling ingestion"
	@echo "    raw-manifest     Scan & validate raw landing zone"
	@echo ""
	@echo "  dbt"
	@echo "    dbt-run          Run all dbt models"
	@echo "    dbt-test         Run all dbt tests"
	@echo "    dbt-docs         Generate & serve dbt docs"
	@echo ""
	@echo "  Testing"
	@echo "    test             Run full pytest suite"
	@echo "    test-cov         Run tests with coverage report"
	@echo ""
	@echo "  Code Quality"
	@echo "    lint             Run ruff linter"
	@echo "    format           Format with black"
	@echo ""
	@echo "  Docker"
	@echo "    docker-up        Start all services (Airflow + mock API)"
	@echo "    docker-down      Stop and remove containers"
	@echo ""
	@echo "  Utility"
	@echo "    clean            Remove build artifacts"
	@echo ""

## ── Setup ─────────────────────────────────────────────────────────────────────
install:
	$(PIP) install -r requirements.txt

install-dev:
	$(PIP) install -r requirements-dev.txt

## ── Data & servers ────────────────────────────────────────────────────────────
generate-data:
	$(PYTHON) data/mock_data_generator.py

start-api:
	uvicorn data.mock_api_server:app --reload --port 8000

## ── Pipeline steps ────────────────────────────────────────────────────────────
ingest-csv:
	$(PYTHON) -m ingestion.csv_reader $(DATE)

ingest-api:
	$(PYTHON) -m ingestion.api_client $(DATE)

raw-manifest:
	$(PYTHON) -m raw.raw_loader $(DATE)

## Full pipeline run for a date
run-pipeline: ingest-csv ingest-api raw-manifest dbt-run dbt-test
	@echo "Pipeline complete for $(DATE)"

## ── dbt ───────────────────────────────────────────────────────────────────────
dbt-run:
	cd dbt && dbt run --target $(DBT_TARGET) --profiles-dir .

dbt-test:
	cd dbt && dbt test --target $(DBT_TARGET) --profiles-dir .

dbt-docs:
	cd dbt && dbt docs generate --target $(DBT_TARGET) --profiles-dir . && \
	          dbt docs serve --profiles-dir . --port 8080

## ── Testing ──────────────────────────────────────────────────────────────────
test:
	$(PYTEST) tests/ -v --tb=short



test-cov:
	$(PYTEST) tests/ --cov=. --cov-report=term-missing --cov-report=html \
	          --cov-omit="data/*,docker/*,dbt/*,dags/*"

lint:
	ruff check ingestion/ raw/ warehouse/ config/ tests/

format:
	black ingestion/ raw/ warehouse/ config/ tests/ dags/

## ── Docker ────────────────────────────────────────────────────────────────────
docker-up:
	cd docker && docker compose up --build -d
	@echo "Airflow UI: http://localhost:8080  (admin/admin)"
	@echo "Mock API:   http://localhost:8000/docs"

docker-down:
	cd docker && docker compose down -v

## ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf dbt/target dbt/dbt_packages htmlcov .coverage
	@echo "Clean complete"
