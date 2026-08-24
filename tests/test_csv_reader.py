"""
Unit tests for ingestion/csv_reader.py
Tests: file detection, column normalization, deduplication, write_raw integration.
"""
import csv
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from ingestion.csv_reader import CSVReader, COLUMN_MAP
from tests.conftest import TEST_DATE, make_txn


# ── Helpers ────────────────────────────────────────────────────────────────────

def write_csv(path: Path, rows: list[dict]) -> Path:
    """Helper to write rows to a CSV file."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return path


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestCSVReaderDetectFiles:
    """Tests for CSVReader.detect_files()"""

    def test_detect_matching_file(self, tmp_path, tmp_raw_dir):
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        rows = [make_txn()]
        write_csv(csv_dir / f"bank_statement_{TEST_DATE}.csv", rows)

        reader = CSVReader(str(csv_dir), str(tmp_raw_dir))
        files = reader.detect_files(TEST_DATE)
        assert len(files) == 1
        assert files[0].name == f"bank_statement_{TEST_DATE}.csv"

    def test_no_match_returns_empty(self, tmp_path, tmp_raw_dir):
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        rows = [make_txn()]
        write_csv(csv_dir / f"bank_statement_2026-01-01.csv", rows)

        reader = CSVReader(str(csv_dir), str(tmp_raw_dir))
        files = reader.detect_files(TEST_DATE)
        assert files == []

    def test_multiple_files_detected(self, tmp_path, tmp_raw_dir):
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        rows = [make_txn()]
        write_csv(csv_dir / f"bank_statement_{TEST_DATE}_batch1.csv", rows)
        write_csv(csv_dir / f"bank_statement_{TEST_DATE}_batch2.csv", rows)

        reader = CSVReader(str(csv_dir), str(tmp_raw_dir))
        files = reader.detect_files(TEST_DATE)
        assert len(files) == 2


class TestCSVReaderRead:
    """Tests for CSVReader.read()"""

    def test_read_basic(self, tmp_csv_dir, tmp_raw_dir):
        reader = CSVReader(str(tmp_csv_dir), str(tmp_raw_dir))
        df = reader.read(date=TEST_DATE)

        assert not df.empty
        assert "txn_id" in df.columns
        assert "sender_vpa" in df.columns
        assert "amount" in df.columns

    def test_read_deduplicates(self, tmp_csv_dir, tmp_raw_dir):
        """CSV fixture has 12 rows (10 unique + 2 duplicates) — read should return 10."""
        reader = CSVReader(str(tmp_csv_dir), str(tmp_raw_dir))
        df = reader.read(date=TEST_DATE)
        assert len(df) == 10

    def test_read_no_files_returns_empty(self, tmp_path, tmp_raw_dir):
        empty_csv_dir = tmp_path / "empty"
        empty_csv_dir.mkdir()
        reader = CSVReader(str(empty_csv_dir), str(tmp_raw_dir))
        df = reader.read(date=TEST_DATE)
        assert df.empty

    def test_read_sets_currency_default(self, tmp_csv_dir, tmp_raw_dir):
        reader = CSVReader(str(tmp_csv_dir), str(tmp_raw_dir))
        df = reader.read(date=TEST_DATE)
        assert (df["currency"] == "INR").all()

    def test_read_adds_ingested_at(self, tmp_csv_dir, tmp_raw_dir):
        reader = CSVReader(str(tmp_csv_dir), str(tmp_raw_dir))
        df = reader.read(date=TEST_DATE)
        assert "ingested_at" in df.columns
        assert df["ingested_at"].notna().all()

    def test_column_map_normalization(self, tmp_path, tmp_raw_dir):
        """Test that aliased column names are correctly renamed."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        rows = [
            {
                "transaction_id": str(uuid.uuid4()),
                "upi_reference":  "UPI123456",
                "from_vpa":       "sender@okaxis",
                "to_vpa":         "receiver@oksbi",
                "txn_amount":     "500.00",
                "currency":       "INR",
                "txn_status":     "SUCCESS",
                "timestamp":      f"{TEST_DATE} 10:00:00",
                "device":         "Android",
                "device_id":      "abc123",
                "ip":             "1.2.3.4",
                "city":           "Mumbai",
                "remarks":        "food",
                "ingested_at":    datetime.utcnow().isoformat(),
            }
        ]
        write_csv(csv_dir / f"bank_statement_{TEST_DATE}.csv", rows)

        reader = CSVReader(str(csv_dir), str(tmp_raw_dir))
        df = reader.read(date=TEST_DATE)

        assert "txn_id" in df.columns         # transaction_id → txn_id
        assert "upi_ref" in df.columns        # upi_reference → upi_ref
        assert "sender_vpa" in df.columns     # from_vpa → sender_vpa
        assert "receiver_vpa" in df.columns   # to_vpa → receiver_vpa
        assert "amount" in df.columns         # txn_amount → amount


class TestCSVReaderRun:
    """Integration test: read → write_raw"""

    def test_run_writes_parquet(self, tmp_csv_dir, tmp_raw_dir):
        reader = CSVReader(str(tmp_csv_dir), str(tmp_raw_dir))
        out_path = reader.run(date=TEST_DATE)

        assert out_path.exists()
        assert out_path.suffix == ".parquet"
        df = pd.read_parquet(out_path)
        assert len(df) == 10    # 12 rows - 2 duplicates
