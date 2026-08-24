"""
Shared pytest fixtures for the banking-upi-pipeline test suite.
"""
import csv
import json
import os
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

# ── Ensure project root is on sys.path ─────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


# ── Dates ──────────────────────────────────────────────────────────────────────
TEST_DATE = "2026-08-16"
TEST_DATE2 = "2026-08-17"


# ── Sample transaction factory ─────────────────────────────────────────────────
def make_txn(
    txn_id: str | None = None,
    date: str = TEST_DATE,
    amount: float = 1500.00,
    status: str = "SUCCESS",
    sender: str = "rahul123@okaxis",
    receiver: str = "priya456@oksbi",
    hour: int = 10,
    city: str = "Mumbai",
    device: str = "Android",
) -> dict:
    """Create a single mock transaction record."""
    return {
        "txn_id":        txn_id or str(uuid.uuid4()),
        "upi_ref":       f"UPI{uuid.uuid4().int % 10**12:012d}",
        "sender_vpa":    sender,
        "receiver_vpa":  receiver,
        "amount":        str(amount),
        "currency":      "INR",
        "status":        status,
        "txn_timestamp": f"{date} {hour:02d}:30:00",
        "device_type":   device,
        "device_id":     str(uuid.uuid4())[:16],
        "ip_address":    "192.168.1.100",
        "city":          city,
        "remarks":       "Test transfer",
        "ingested_at":   datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ── Temp directories ──────────────────────────────────────────────────────────

@pytest.fixture(scope="function")
def tmp_csv_dir(tmp_path):
    """Temporary directory containing a sample CSV for TEST_DATE."""
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()

    rows = [make_txn(date=TEST_DATE) for _ in range(10)]
    # Add 2 duplicates
    rows += [rows[0].copy(), rows[1].copy()]

    filepath = csv_dir / f"bank_statement_{TEST_DATE}.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return csv_dir


@pytest.fixture(scope="function")
def tmp_raw_dir(tmp_path):
    """Temporary raw/ directory."""
    return tmp_path / "raw"


@pytest.fixture(scope="function")
def sample_raw_parquet(tmp_path):
    """
    Write a raw Parquet file to tmp_path/raw/source=csv/date=TEST_DATE/data.parquet.
    Returns the path to the parquet file.
    """
    rows = [make_txn(date=TEST_DATE) for _ in range(20)]
    df = pd.DataFrame(rows)
    df["amount"] = df["amount"].astype(float)
    df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"])
    df["ingested_at"]   = pd.to_datetime(df["ingested_at"])

    out_dir = tmp_path / "raw" / "source=csv" / f"date={TEST_DATE}"
    out_dir.mkdir(parents=True)
    parquet_path = out_dir / "data.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    return parquet_path


@pytest.fixture(scope="function")
def sample_df():
    """A minimal sample DataFrame with canonical columns."""
    rows = [make_txn(date=TEST_DATE) for _ in range(15)]
    df = pd.DataFrame(rows)
    df["amount"] = df["amount"].astype(float)
    return df


# ── Mock API response ─────────────────────────────────────────────────────────

@pytest.fixture
def mock_api_response():
    """Build a mock paginated API response dict."""
    records = [make_txn(date=TEST_DATE) for _ in range(5)]
    return {
        "records":     records,
        "total":       5,
        "count":       5,
        "next_cursor": None,
    }
