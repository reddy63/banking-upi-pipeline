"""
Source 2: REST API polling client
Pattern  : cursor-based pagination, token refresh, rate-limit backoff

Polls the mock UPI Transaction API (or real Razorpay/NPCI endpoint),
handling pagination via next_cursor, 429 backoff, and auth headers.
Writes raw JSON batches as Parquet to raw/source=api/date=YYYY-MM-DD/.
"""
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.base_reader import BaseReader


class APIClient(BaseReader):
    """
    Polls a paginated UPI transaction REST API.
    Handles: auth headers, cursor pagination, 429 rate-limit backoff,
             transient 5xx retries, and partial-batch recovery.
    """

    MAX_RETRIES          = 5
    BACKOFF_FACTOR       = 1.5    # exponential: 1.5s, 2.25s, 3.4s ...
    RATE_LIMIT_DELAY     = 0.3    # seconds between normal requests
    RATE_LIMIT_429_SLEEP = 60     # seconds to sleep on HTTP 429

    def __init__(
        self,
        base_url:      str,
        api_key:       str,
        raw_base_path: str,
        page_size:     int = 200,
    ):
        super().__init__(raw_base_path, source_name="api")
        self.base_url  = base_url.rstrip("/")
        self.api_key   = api_key
        self.page_size = page_size
        self.session   = self._build_session()

    # ── Session with retry ──────────────────────────────────────────────────────────

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=self.BACKOFF_FACTOR,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        session.mount("http://",  HTTPAdapter(max_retries=retry))
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Accept":        "application/json",
            "User-Agent":    "banking-upi-pipeline/1.0",
        })
        return session

    # ── Public ──────────────────────────────────────────────────────────────────────────

    def read(
        self,
        from_date: Optional[str] = None,
        to_date:   Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Fetch ALL pages for the given date range using cursor pagination.
        Returns a single DataFrame of all records.
        """
        today     = datetime.utcnow().strftime("%Y-%m-%d")
        from_date = from_date or today
        to_date   = to_date   or today

        self.logger.info(
            "Polling API %s/api/v1/transactions (%s → %s)",
            self.base_url, from_date, to_date,
        )

        all_records: list[dict] = []
        cursor:      Optional[str] = None
        page_num  = 0

        while True:
            page_num += 1
            records, cursor = self._fetch_page(from_date, to_date, cursor)
            all_records.extend(records)
            self.logger.info(
                "  Page %d: %d records fetched (total so far: %d)",
                page_num, len(records), len(all_records),
            )
            if not cursor:
                break
            time.sleep(self.RATE_LIMIT_DELAY)

        self.logger.info("API poll complete: %d total records", len(all_records))
        return pd.DataFrame(all_records) if all_records else pd.DataFrame()

    # ── Private ─────────────────────────────────────────────────────────────────────────

    def _fetch_page(
        self,
        from_date: str,
        to_date:   str,
        cursor:    Optional[str],
    ) -> tuple[list[dict], Optional[str]]:
        """Fetch a single page; handle 429 with sleep-and-retry."""
        params: dict = {
            "from_date": from_date,
            "to_date":   to_date,
            "limit":     self.page_size,
        }
        if cursor:
            params["cursor"] = cursor

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self.session.get(
                    f"{self.base_url}/api/v1/transactions",
                    params=params,
                    timeout=30,
                )

                if resp.status_code == 429:
                    sleep_for = int(
                        resp.headers.get("Retry-After", self.RATE_LIMIT_429_SLEEP)
                    )
                    self.logger.warning(
                        "Rate limited (429). Sleeping %ds (attempt %d/%d)",
                        sleep_for, attempt, self.MAX_RETRIES,
                    )
                    time.sleep(sleep_for)
                    continue

                if resp.status_code == 401:
                    raise RuntimeError("API authentication failed. Check MOCK_API_KEY.")

                resp.raise_for_status()
                data = resp.json()
                return data.get("records", []), data.get("next_cursor")

            except requests.exceptions.ConnectionError as exc:
                self.logger.error("Connection error (attempt %d): %s", attempt, exc)
                if attempt == self.MAX_RETRIES:
                    raise
                time.sleep(self.BACKOFF_FACTOR ** attempt)

        return [], None


# ── CLI entry point ───────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from config.settings import MOCK_API_BASE_URL, MOCK_API_KEY

    date = sys.argv[1] if len(sys.argv) > 1 else datetime.utcnow().strftime("%Y-%m-%d")
    client = APIClient(
        base_url      = MOCK_API_BASE_URL,
        api_key       = MOCK_API_KEY,
        raw_base_path = str(Path(__file__).parent.parent / "raw"),
    )
    client.run(from_date=date, to_date=date, date=date)
