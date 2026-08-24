"""
Mock UPI Transaction REST API  (FastAPI)
Mimics a paginated banking API with cursor-based pagination,
Bearer token auth, and realistic rate-limit headers.

Start: uvicorn data.mock_api_server:app --reload --port 8000
"""
import json
import math
from pathlib import Path
from typing import Optional

from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

VALID_API_KEY = "dev-api-key-123"
SEED_FILE = Path(__file__).parent / "mock_api" / "api_seed_data.json"

_RECORDS: list = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _RECORDS
    if SEED_FILE.exists():
        _RECORDS = json.loads(SEED_FILE.read_text())
        print(f"Loaded {len(_RECORDS)} seed records")
    else:
        print("WARNING: seed file not found. Run data/mock_data_generator.py first.")
    yield

app = FastAPI(title="Mock UPI Transaction API", version="1.0.0", lifespan=lifespan)


def _authenticate(authorization: Optional[str]):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.split(" ", 1)[1]
    if token != VALID_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")


@app.get("/health")
def health():
    return {"status": "ok", "records_loaded": len(_RECORDS)}


@app.get("/api/v1/transactions")
def get_transactions(
    from_date:  str            = Query(...,  description="YYYY-MM-DD"),
    to_date:    str            = Query(...,  description="YYYY-MM-DD"),
    limit:      int            = Query(200,  ge=1, le=500),
    cursor:     Optional[str]  = Query(None, description="Pagination cursor (offset)"),
    authorization: Optional[str] = Header(None),
):
    _authenticate(authorization)

    # Filter by date range
    filtered = [
        r for r in _RECORDS
        if from_date <= r["txn_timestamp"][:10] <= to_date
    ]

    # Cursor is just a string-encoded offset
    offset = int(cursor) if cursor and cursor.isdigit() else 0
    page   = filtered[offset: offset + limit]
    next_cursor = str(offset + limit) if (offset + limit) < len(filtered) else None

    return JSONResponse(
        content={
            "records":      page,
            "total":        len(filtered),
            "count":        len(page),
            "next_cursor":  next_cursor,
            "page":         math.ceil(offset / limit) + 1,
        },
        headers={
            "X-RateLimit-Limit":     "100",
            "X-RateLimit-Remaining": "99",
            "X-RateLimit-Reset":     "60",
        },
    )


@app.get("/api/v1/transactions/{txn_id}")
def get_transaction(txn_id: str, authorization: Optional[str] = Header(None)):
    _authenticate(authorization)
    for r in _RECORDS:
        if r["txn_id"] == txn_id:
            return r
    raise HTTPException(status_code=404, detail="Transaction not found")
