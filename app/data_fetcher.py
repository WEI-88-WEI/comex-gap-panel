from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/GC=F?range=10y&interval=1d&includePrePost=false"


def fetch_daily_series() -> dict[str, Any]:
    headers = {"User-Agent": "Mozilla/5.0"}
    with httpx.Client(timeout=30.0, headers=headers, follow_redirects=True) as client:
        resp = client.get(YAHOO_URL)
        resp.raise_for_status()
        payload = resp.json()

    result = payload["chart"]["result"][0]
    timestamps = result.get("timestamp", [])
    quote = result["indicators"]["quote"][0]
    adjclose = result.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])

    rows: list[dict[str, Any]] = []
    for idx, ts in enumerate(timestamps):
        row = {
            "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
            "open": _safe_pick(quote.get("open"), idx),
            "high": _safe_pick(quote.get("high"), idx),
            "low": _safe_pick(quote.get("low"), idx),
            "close": _safe_pick(quote.get("close"), idx),
            "volume": _safe_pick(quote.get("volume"), idx),
            "adjclose": _safe_pick(adjclose, idx),
        }
        if row["open"] is None and row["close"] is None:
            continue
        rows.append(row)

    return {
        "symbol": "GC=F",
        "source": "Yahoo Finance chart API",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
    }


def _safe_pick(values: Any, idx: int):
    if not isinstance(values, list):
        return None
    if idx >= len(values):
        return None
    return values[idx]
