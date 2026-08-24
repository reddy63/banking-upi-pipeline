"""
Unit tests for ingestion/api_client.py
Tests: session building, pagination loop, 429 handling, error recovery.
All HTTP calls are mocked — no live server needed.
"""
import json
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest
import requests

from ingestion.api_client import APIClient
from tests.conftest import TEST_DATE, make_txn


BASE_URL = "http://test-api.local"
API_KEY  = "test-key-abc"


@pytest.fixture
def client(tmp_path):
    return APIClient(
        base_url      = BASE_URL,
        api_key       = API_KEY,
        raw_base_path = str(tmp_path / "raw"),
        page_size     = 3,
    )


def _make_response(records: list, next_cursor=None, status_code=200):
    """Build a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = {
        "records":    records,
        "total":      len(records),
        "count":      len(records),
        "next_cursor": next_cursor,
    }
    resp.headers = {"Retry-After": "1"}
    resp.raise_for_status = MagicMock()
    return resp


class TestAPIClientSession:
    def test_session_has_auth_header(self, client):
        assert "Authorization" in client.session.headers
        assert client.session.headers["Authorization"] == f"Bearer {API_KEY}"

    def test_session_has_user_agent(self, client):
        assert "banking-upi-pipeline" in client.session.headers["User-Agent"]


class TestAPIClientRead:

    @patch("ingestion.api_client.time.sleep")
    def test_single_page(self, mock_sleep, client):
        records = [make_txn() for _ in range(3)]
        response = _make_response(records, next_cursor=None)

        with patch.object(client.session, "get", return_value=response):
            df = client.read(from_date=TEST_DATE, to_date=TEST_DATE)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    @patch("ingestion.api_client.time.sleep")
    def test_multi_page_pagination(self, mock_sleep, client):
        """Two pages: first returns cursor='3', second returns no cursor."""
        page1 = [make_txn() for _ in range(3)]
        page2 = [make_txn() for _ in range(2)]

        resp1 = _make_response(page1, next_cursor="3")
        resp2 = _make_response(page2, next_cursor=None)

        with patch.object(client.session, "get", side_effect=[resp1, resp2]):
            df = client.read(from_date=TEST_DATE, to_date=TEST_DATE)

        assert len(df) == 5

    @patch("ingestion.api_client.time.sleep")
    def test_429_triggers_sleep_and_retry(self, mock_sleep, client):
        """On 429, client should sleep Retry-After seconds and retry."""
        rate_limit_resp = MagicMock(spec=requests.Response)
        rate_limit_resp.status_code = 429
        rate_limit_resp.headers = {"Retry-After": "2"}

        ok_resp = _make_response([make_txn()], next_cursor=None)

        with patch.object(client.session, "get", side_effect=[rate_limit_resp, ok_resp]):
            df = client.read(from_date=TEST_DATE, to_date=TEST_DATE)

        mock_sleep.assert_called_with(2)
        assert len(df) == 1

    def test_401_raises_runtime_error(self, client):
        resp_401 = MagicMock(spec=requests.Response)
        resp_401.status_code = 401
        resp_401.headers = {}

        with patch.object(client.session, "get", return_value=resp_401):
            with pytest.raises(RuntimeError, match="authentication failed"):
                client.read(from_date=TEST_DATE, to_date=TEST_DATE)

    @patch("ingestion.api_client.time.sleep")
    def test_empty_response_returns_empty_df(self, mock_sleep, client):
        response = _make_response([], next_cursor=None)

        with patch.object(client.session, "get", return_value=response):
            df = client.read(from_date=TEST_DATE, to_date=TEST_DATE)

        assert df.empty

    @patch("ingestion.api_client.time.sleep")
    def test_connection_error_retries(self, mock_sleep, client):
        """ConnectionError should retry up to MAX_RETRIES times then raise."""
        from unittest.mock import patch as _patch

        with _patch.object(
            client.session, "get",
            side_effect=requests.exceptions.ConnectionError("Connection refused")
        ) as mock_get:
            with pytest.raises(requests.exceptions.ConnectionError):
                client.read(from_date=TEST_DATE, to_date=TEST_DATE)

            # Should have been called MAX_RETRIES times
            assert mock_get.call_count == client.MAX_RETRIES


class TestAPIClientRun:

    @patch("ingestion.api_client.time.sleep")
    def test_run_writes_parquet(self, mock_sleep, client, tmp_path):
        records = [make_txn() for _ in range(5)]
        response = _make_response(records, next_cursor=None)

        with patch.object(client.session, "get", return_value=response):
            out_path = client.run(from_date=TEST_DATE, to_date=TEST_DATE, date=TEST_DATE)

        assert out_path.exists()
        df = pd.read_parquet(out_path)
        assert len(df) == 5
