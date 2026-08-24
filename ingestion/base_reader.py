"""
Abstract base class for all ingestion readers.
Every reader must implement read() and write_raw().
The contract: both methods write date-partitioned Parquet
to the raw/ folder, partitioned by source and date.
"""
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


class BaseReader(ABC):
    """Common interface for CSV and API ingestion readers."""

    # Canonical column names every reader must produce
    REQUIRED_COLUMNS = [
        "txn_id", "upi_ref", "sender_vpa", "receiver_vpa",
        "amount", "currency", "status", "txn_timestamp",
        "device_type", "device_id", "ip_address", "city",
        "remarks", "ingested_at",
    ]

    def __init__(self, raw_base_path: str, source_name: str):
        self.raw_base_path = Path(raw_base_path)
        self.source_name   = source_name          # "csv" or "api"
        self.logger        = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def read(self, **kwargs) -> pd.DataFrame:
        """Pull data from the source and return a normalized DataFrame."""
        ...

    def validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all required columns are present; add nulls for missing ones."""
        for col in self.REQUIRED_COLUMNS:
            if col not in df.columns:
                self.logger.warning("Column '%s' missing — filling with None", col)
                df[col] = None
        return df[self.REQUIRED_COLUMNS]

    def write_raw(self, df: pd.DataFrame, date: str | None = None) -> Path:
        """
        Persist DataFrame to:  raw/source={source}/date={YYYY-MM-DD}/data.parquet
        Returns the output path.
        """
        date = date or datetime.utcnow().strftime("%Y-%m-%d")
        df   = self.validate_schema(df)

        # Type coercions for safe Parquet serialization
        df["amount"]       = pd.to_numeric(df["amount"], errors="coerce")
        df["txn_timestamp"] = pd.to_datetime(df["txn_timestamp"], errors="coerce")
        df["ingested_at"]  = pd.to_datetime(df["ingested_at"],  errors="coerce")

        out_dir  = self.raw_base_path / f"source={self.source_name}" / f"date={date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "data.parquet"

        df.to_parquet(out_file, index=False, engine="pyarrow")
        self.logger.info(
            "[%s] Wrote %d rows → %s", self.source_name, len(df), out_file
        )
        return out_file

    def run(self, **kwargs) -> Path:
        """Convenience method: read then write raw in one call."""
        self.logger.info("[%s] Starting ingestion", self.source_name)
        df   = self.read(**kwargs)
        path = self.write_raw(df, kwargs.get("date"))
        self.logger.info(
            "[%s] Ingestion complete. %d rows written.", self.source_name, len(df)
        )
        return path
