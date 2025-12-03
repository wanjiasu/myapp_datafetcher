import os
import sys
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List
from zoneinfo import ZoneInfo

from fastapi import FastAPI

from data_fetcher.tools.get_fixture import (
    APIFootballClient,
    _pg_config,
    _pg_connect,
    _pg_ensure_table,
    _pg_upsert,
)


app = FastAPI()


def _tz_name() -> str:
    return os.getenv("FETCH_TIMEZONE", "UTC")


def _tz() -> ZoneInfo:
    try:
        return ZoneInfo(_tz_name())
    except Exception:
        return ZoneInfo("UTC")


def _dates_utc_window() -> List[str]:
    tz = _tz()
    today = datetime.now(tz).date()
    past = int(os.getenv("FETCH_DAYS_PAST", "1"))
    future = int(os.getenv("FETCH_DAYS_FUTURE", "1"))
    out: List[str] = []
    for d in range(-past, future + 1):
        out.append((today + timedelta(days=d)).isoformat())
    return out


def _run_once_for_dates(dates: List[str]) -> int:
    client = APIFootballClient()
    cfg = _pg_config()
    if not cfg:
        return 0
    conn = _pg_connect(cfg)
    _pg_ensure_table(conn)
    total = 0
    for d in dates:
        data = client.get_fixtures_by_date(d, timezone=_tz_name())
        if not data:
            continue
        rows = client.normalize_response(data)
        total += _pg_upsert(conn, rows)
    conn.close()
    try:
        print({"written": total, "dates": dates, "timezone": _tz_name()})
    except Exception:
        pass
    return total


async def _midnight_loop():
    while True:
        tz = _tz()
        now = datetime.now(tz)
        midnight = datetime(year=now.year, month=now.month, day=now.day, tzinfo=tz)
        if now >= midnight:
            midnight = midnight + timedelta(days=1)
        sleep_s = max(0.0, (midnight - now).total_seconds())
        await asyncio.sleep(sleep_s)
        _run_once_for_dates(_dates_utc_window())


async def _twoam_loop():
    while True:
        tz = _tz()
        now = datetime.now(tz)
        target = datetime(year=now.year, month=now.month, day=now.day, hour=2, tzinfo=tz)
        if now >= target:
            target = target + timedelta(days=1)
        sleep_s = max(0.0, (target - now).total_seconds())
        await asyncio.sleep(sleep_s)
        p = await asyncio.create_subprocess_exec(sys.executable, "-m", "data_fetcher.tools.get_odds_match")
        await p.wait()


def _interval_hours() -> int:
    try:
        v = int(os.getenv("FETCH_INTERVAL_HOURS", "3"))
        return 1 if v < 1 else (24 if v > 24 else v)
    except Exception:
        return 3


async def _interval_loop():
    while True:
        tz = _tz()
        now = datetime.now(tz)
        ih = _interval_hours()
        base = now.replace(minute=0, second=0, microsecond=0)
        q = (base.hour // ih) + 1
        nh = (q * ih) % 24
        day_add = 1 if nh <= base.hour else 0
        target = base.replace(hour=nh) + timedelta(days=day_add)
        sleep_s = max(0.0, (target - now).total_seconds())
        await asyncio.sleep(sleep_s)
        _run_once_for_dates(_dates_utc_window())


@app.on_event("startup")
async def _startup():
    asyncio.create_task(_interval_loop())
    asyncio.create_task(_twoam_loop())


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/run")
async def run_now():
    n = await asyncio.to_thread(_run_once_for_dates, _dates_utc_window())
    return {"written": n, "dates": _dates_utc_window(), "timezone": _tz_name()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")))
