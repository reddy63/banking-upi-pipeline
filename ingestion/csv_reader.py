"""
Source 1: CSV batch ingestion
Pattern  : schedule-triggered, file detection, bulk load

Reads daily bank statement CSV dumps from data/mock_csv/,
normalizes column names, drops duplicates at source level,
and writes to raw/source=csv/date=YYYY-MM-DD/data.parquet.
"""
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from ingestion.base_reader import BaseReader


# Raw CSV column → canonical column name
COLUMN_MAP = {
    "transaction_id":   "txn_id",
    "txn_id":           "txn_id",
    "upi_reference":    "upi_ref",
    "upi_ref":          "upi_ref",
    "from_vpa":         "sender_vpa",
    "sender_vpa":       "sender_vpa",
    "to_vpa":           "receiver_vpa",
    "receiver_vpa":     "receiver_vpa",
    "txn_amount":       "amount",
    "amount":           "amount",
    "currency":         "currency",
    "txn_status":       "status",
    "status":           "status",
    "timestamp":        "txn_timestamp",
    "txn_timestamp":    "txn_timestamp",
    "device":           "device_type",
    "device_type":      "device_type",
    "device_id":        "device_id",
    "ip":               "ip_address",
    "ip_address":       "ip_address",
    "city":             "city",
    "remarks":          "remarks",
    "ingested_at":      "ingested_at",
}


class CSVReader(BaseReader):
    """
    Reads one or more bank statement CSV files for a given date,
    normalizes headers, drops in-file duplicates, adds metadata.
    """

    def __init__(self, csv_dir: str, raw_base_path: str):
        super().__init__(raw_base_path, source_name="csv")
        self.csv_dir = Path(csv_dir)

    # ── public ───────────────────────────────────────────────────────────────────────

    def detect_files(self, date: str) -> list[Path]:
        """
        Detect all CSV files for a given date string (YYYY-MM-DD).
        Matches: bank_statement_2026-08-23.csv  or  2026-08-23_*.csv
        """
        pattern = re.compile(rf".*{re.escape(date)}.*\.csv$", re.IGNORECASE)
        files = [f for f in self.csv_dir.glob("*.csv") if pattern.match(f.name)]
        if not files:
            self.logger.warning("No CSV files found for date %s in %s", date, self.csv_dir)
        return sorted(files)

    def read(self, date: str | None = None, **kwargs) -> pd.DataFrame:
        """
        Read all CSV files for `date` (defaults to today).
        Returns a cleaned, deduplicated DataFrame.
        """
        date = date or datetime.utcnow().strftime("%Y-%m-%d")
        files = self.detect_files(date)

        if not files:
            self.logger.warning("Returning empty DataFrame — no files for %s", date)
            return pd.DataFrame()

        frames = []
        for filepath in files:
            self.logger.info("Reading %s", filepath.name)
            try:
                df = pd.read_csv(
                    filepath,
                    dtype=str,           # read everything as str first
                    on_bad_lines="warn",
                    low_memory=False,
                )
                df = self._normalize(df)
                frames.append(df)
                self.logger.info("  %d rows loaded", len(df))
            except Exception as exc:
                self.logger.error("Failed to read %s: %s", filepath.name, exc)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        before   = len(combined)
        combined = self._deduplicate(combined)
        self.logger.info(
            "Loaded %d raw rows, %d after dedup (removed %d duplicates)",
            before, len(combined), before - len(combined),
        )
        return combined

    # ── private ───────────────────────────────────────────────────────────────────────

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and add metadata."""
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns=COLUMN_MAP)
        df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        df["currency"]    = df.get("currency", pd.Series(dtype=str)).fillna("INR")
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep the first occurrence of each txn_id within this batch."""
        if "txn_id" in df.columns:
            return df.drop_duplicates(subset=["txn_id"], keep="first")
        return df


# ── CLI entry point ───────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    date = sys.argv[1] if len(sys.argv) > 1 else datetime.utcnow().strftime("%Y-%m-%d")
    reader = CSVReader(
        csv_dir       = str(Path(__file__).parent.parent / "data" / "mock_csv"),
        raw_base_path = str(Path(__file__).parent.parent / "raw"),
    )
    reader.run(date=date)
