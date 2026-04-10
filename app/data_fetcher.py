from __future__ import annotations

import datetime as dt
import json
import random
import re
import string
import time
from typing import Any

from websocket import create_connection

TV_WS_URL = "wss://data.tradingview.com/socket.io/websocket"
TV_ORIGIN = "https://data.tradingview.com"
SYMBOL = "COMEX:GC1!"
N_BARS = 1300
MAX_RETRIES = 3


def fetch_daily_series() -> dict[str, Any]:
    rows = _fetch_tradingview_daily_bars(symbol=SYMBOL, n_bars=N_BARS)
    return {
        "symbol": SYMBOL,
        "source": "TradingView websocket (COMEX:GC1!)",
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "rows": rows,
    }


def _fetch_tradingview_daily_bars(symbol: str, n_bars: int) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return _fetch_once(symbol, n_bars)
        except Exception as e:
            last_error = e
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"TradingView fetch failed after {MAX_RETRIES} retries: {last_error}")


def _fetch_once(symbol: str, n_bars: int) -> list[dict[str, Any]]:
    ws = create_connection(
        TV_WS_URL,
        header=[f"Origin: {TV_ORIGIN}", "User-Agent: Mozilla/5.0"],
        timeout=20,
    )
    chart_session = _gen_session("cs_")
    quote_session = _gen_session("qs_")

    messages = [
        ("set_auth_token", ["unauthorized_user_token"]),
        ("chart_create_session", [chart_session, ""]),
        ("quote_create_session", [quote_session]),
        (
            "quote_set_fields",
            [
                quote_session,
                "ch",
                "chp",
                "current_session",
                "description",
                "local_description",
                "language",
                "exchange",
                "fractional",
                "is_tradable",
                "lp",
                "lp_time",
                "minmov",
                "minmove2",
                "original_name",
                "pricescale",
                "pro_name",
                "short_name",
                "type",
                "update_mode",
                "volume",
                "currency_code",
                "rchp",
                "rtc",
            ],
        ),
        ("quote_add_symbols", [quote_session, symbol, {"flags": ["force_permission"]}]),
        ("quote_fast_symbols", [quote_session, symbol]),
        (
            "resolve_symbol",
            [chart_session, "symbol_1", '={"symbol":"COMEX:GC1!","adjustment":"splits","session":"regular"}'],
        ),
        ("create_series", [chart_session, "s1", "s1", "symbol_1", "1D", n_bars]),
        ("switch_timezone", [chart_session, "exchange"]),
    ]

    for func, args in messages:
        ws.send(_tv_msg(func, args))

    chunks: list[str] = []
    try:
        start = time.time()
        while True:
            payload = ws.recv()
            chunks.append(payload)
            if "series_completed" in payload:
                break
            if time.time() - start > 25:
                raise RuntimeError("TradingView websocket timed out before series_completed")
    finally:
        ws.close()

    raw = "\n".join(chunks)
    rows = _parse_rows(raw)
    if len(rows) < 1000:
        raise RuntimeError(f"Too few rows parsed from TradingView: {len(rows)}")
    return rows


def _parse_rows(raw: str) -> list[dict[str, Any]]:
    marker = '"s":['
    start = raw.find(marker)
    if start == -1:
        raise RuntimeError("TradingView response missing series payload")
    i = start + len(marker)
    depth = 1
    buf = []
    while i < len(raw) and depth > 0:
        ch = raw[i]
        if ch == '[':
            depth += 1
        elif ch == ']':
            depth -= 1
            if depth == 0:
                break
        buf.append(ch)
        i += 1
    content = ''.join(buf)

    pattern = re.compile(r'"i":(\d+),"v":\[(\d+(?:\.\d+)?),([\-\d.]+),([\-\d.]+),([\-\d.]+),([\-\d.]+),([\-\d.]+)\]')
    rows: list[dict[str, Any]] = []
    for m in pattern.finditer(content):
        ts = float(m.group(2))
        rows.append(
            {
                "date": dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%Y-%m-%d"),
                "open": float(m.group(3)),
                "high": float(m.group(4)),
                "low": float(m.group(5)),
                "close": float(m.group(6)),
                "volume": float(m.group(7)),
                "adjclose": None,
            }
        )
    if not rows:
        raise RuntimeError("No daily bars parsed from TradingView response")
    return rows


def _gen_session(prefix: str) -> str:
    return prefix + ''.join(random.choice(string.ascii_lowercase) for _ in range(12))


def _tv_msg(func: str, args: list[Any]) -> str:
    payload = json.dumps({"m": func, "p": args}, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"
