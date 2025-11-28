import os
import sys
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List

from fastapi import FastAPI

from data_fetcher.tools.get_fixture import (
    APIFootballClient,
    _pg_config,
    _pg_connect,
    _pg_ensure_table,
    _pg_upsert,
)


app = FastAPI()


def _dates_utc_window() -> List[str]:
    today = datetime.now(timezone.utc).date()
    y = (today - timedelta(days=1)).isoformat()
    t = today.isoformat()
    tm = (today + timedelta(days=1)).isoformat()
    return [y, t, tm]


def _run_once_for_dates(dates: List[str]) -> int:
    client = APIFootballClient()
    cfg = _pg_config()
    if not cfg:
        return 0
    conn = _pg_connect(cfg)
    _pg_ensure_table(conn)
    total = 0
    for d in dates:
        data = client.get_fixtures_by_date(d, timezone="UTC")
        if not data:
            continue
        rows = client.normalize_response(data)
        total += _pg_upsert(conn, rows)
    conn.close()
    return total


async def _midnight_loop():
    while True:
        now = datetime.now(timezone.utc)
        midnight = datetime(year=now.year, month=now.month, day=now.day, tzinfo=timezone.utc)
        if now >= midnight:
            midnight = midnight + timedelta(days=1)
        await asyncio.sleep((midnight - now).total_seconds())
        _run_once_for_dates(_dates_utc_window())


async def _twoam_loop():
    while True:
        now = datetime.now(timezone.utc)
        target = datetime(year=now.year, month=now.month, day=now.day, hour=2, tzinfo=timezone.utc)
        if now >= target:
            target = target + timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        p = await asyncio.create_subprocess_exec(sys.executable, "-m", "data_fetcher.tools.get_odds_match")
        await p.wait()


@app.on_event("startup")
async def _startup():
    asyncio.create_task(_midnight_loop())
    asyncio.create_task(_twoam_loop())


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/run")
async def run_now():
    n = await asyncio.to_thread(_run_once_for_dates, _dates_utc_window())
    return {"written": n, "dates": _dates_utc_window(), "timezone": "UTC"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")))
