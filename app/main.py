from __future__ import annotations

import contextlib
import json
import logging
import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.analytics import compute_monday_gaps
from app.data_fetcher import fetch_all_series

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
CACHE_PATH = DATA_DIR / "monday_gap_cache.json"
REFRESH_ON_STARTUP = os.getenv("REFRESH_ON_STARTUP", "1") == "1"
logger = logging.getLogger("comex-gap-panel")

app = FastAPI(title="COMEX Gold Monday Gap Panel")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def refresh_cache() -> dict:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw = fetch_all_series()
    analytics = compute_monday_gaps(raw["daily_rows"], intraday_rows=raw["intraday_rows"], years=5)
    payload = {
        "symbol": raw["symbol"],
        "source": raw["source"],
        "updated_at": raw["updated_at"],
        "range": analytics["range"],
        "summary": analytics["summary"],
        "intraday_summary": analytics["intraday_summary"],
        "items": analytics["items"],
    }
    CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def load_cache() -> dict:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return refresh_cache()


@app.on_event("startup")
async def startup_event() -> None:
    with contextlib.suppress(Exception):
        if REFRESH_ON_STARTUP:
            app.state.payload = refresh_cache()
            return
    with contextlib.suppress(Exception):
        app.state.payload = load_cache()


@app.get("/api/data")
def api_data():
    try:
        payload = getattr(app.state, "payload", None) or load_cache()
        app.state.payload = payload
        return payload
    except Exception as e:
        logger.exception("load data failed")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/refresh")
def api_refresh():
    try:
        payload = refresh_cache()
        app.state.payload = payload
        return payload
    except Exception as e:
        logger.exception("refresh failed")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
