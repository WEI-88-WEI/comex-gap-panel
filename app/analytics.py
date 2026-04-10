from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any


MARKET_OPEN_HOUR_UTC = 22
MARKET_OPEN_MINUTE_UTC = 0


def compute_monday_gaps(rows: list[dict[str, Any]], intraday_rows: list[dict[str, Any]] | None = None, years: int = 5) -> dict[str, Any]:
    cleaned = []
    for row in rows:
        try:
            dt = datetime.strptime(row["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            continue
        item = dict(row)
        item["dt"] = dt
        cleaned.append(item)

    cleaned.sort(key=lambda x: x["dt"])
    if not cleaned:
        return {"items": [], "summary": {}, "intraday_summary": {}}

    intraday_map = build_intraday_monday_metrics(intraday_rows or [])

    end_dt = cleaned[-1]["dt"]
    start_dt = end_dt - timedelta(days=365 * years + 7)
    window = [r for r in cleaned if r["dt"] >= start_dt]

    items: list[dict[str, Any]] = []
    prev = None
    for row in window:
        if row["dt"].weekday() == 0 and row.get("open") is not None and prev and prev.get("close") is not None:
            gap = row["open"] - prev["close"]
            gap_pct = (gap / prev["close"] * 100.0) if prev["close"] else None
            intraday_range_pct = None
            if row.get("high") is not None and row.get("low") is not None and row["open"]:
                intraday_range_pct = (row["high"] - row["low"]) / row["open"] * 100.0
            monday_key = row["date"]
            extra = intraday_map.get(monday_key, {})
            items.append(
                {
                    "monday_date": row["date"],
                    "previous_trading_date": prev["date"],
                    "previous_close": prev["close"],
                    "monday_open": row["open"],
                    "gap": gap,
                    "gap_pct": gap_pct,
                    "monday_high": row.get("high"),
                    "monday_low": row.get("low"),
                    "monday_close": row.get("close"),
                    "monday_intraday_range_pct": intraday_range_pct,
                    **extra,
                }
            )
        prev = row

    gap_pcts = [x["gap_pct"] for x in items if x.get("gap_pct") is not None]
    abs_gap_pcts = [abs(x) for x in gap_pcts]
    pos = [x for x in gap_pcts if x > 0]
    neg = [x for x in gap_pcts if x < 0]

    vol5 = [x["open_5m_move_pct"] for x in items if x.get("open_5m_move_pct") is not None]
    vol10 = [x["open_10m_move_pct"] for x in items if x.get("open_10m_move_pct") is not None]
    weekend5 = [x["weekend_last_to_5m_pct"] for x in items if x.get("weekend_last_to_5m_pct") is not None]
    weekend10 = [x["weekend_last_to_10m_pct"] for x in items if x.get("weekend_last_to_10m_pct") is not None]

    summary = {
        "count": len(items),
        "avg_gap_pct": mean(gap_pcts) if gap_pcts else None,
        "avg_abs_gap_pct": mean(abs_gap_pcts) if abs_gap_pcts else None,
        "max_up_gap_pct": max(gap_pcts) if gap_pcts else None,
        "max_down_gap_pct": min(gap_pcts) if gap_pcts else None,
        "up_weeks": len(pos),
        "down_weeks": len(neg),
        "up_ratio_pct": (len(pos) / len(items) * 100.0) if items else None,
        "down_ratio_pct": (len(neg) / len(items) * 100.0) if items else None,
    }

    intraday_summary = {
        "count_5m": len(vol5),
        "count_10m": len(vol10),
        "avg_open_5m_move_pct": mean(vol5) if vol5 else None,
        "avg_abs_open_5m_move_pct": mean([abs(x) for x in vol5]) if vol5 else None,
        "avg_open_10m_move_pct": mean(vol10) if vol10 else None,
        "avg_abs_open_10m_move_pct": mean([abs(x) for x in vol10]) if vol10 else None,
        "avg_weekend_last_to_5m_pct": mean(weekend5) if weekend5 else None,
        "avg_abs_weekend_last_to_5m_pct": mean([abs(x) for x in weekend5]) if weekend5 else None,
        "avg_weekend_last_to_10m_pct": mean(weekend10) if weekend10 else None,
        "avg_abs_weekend_last_to_10m_pct": mean([abs(x) for x in weekend10]) if weekend10 else None,
        "intraday_range": {
            "start": intraday_rows[0]["timestamp"] if intraday_rows else None,
            "end": intraday_rows[-1]["timestamp"] if intraday_rows else None,
        },
    }

    return {
        "items": items,
        "summary": summary,
        "intraday_summary": intraday_summary,
        "range": {
            "start": window[0]["date"] if window else None,
            "end": window[-1]["date"] if window else None,
        },
    }


def build_intraday_monday_metrics(intraday_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    parsed = []
    for row in intraday_rows:
        ts = row.get("timestamp")
        if not ts:
            continue
        try:
            dt_obj = datetime.fromisoformat(ts)
        except Exception:
            continue
        item = dict(row)
        item["dt"] = dt_obj.astimezone(timezone.utc)
        parsed.append(item)
    parsed.sort(key=lambda x: x["dt"])

    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in parsed:
        monday_date = _infer_monday_market_date(row["dt"])
        if monday_date is None:
            continue
        by_date.setdefault(monday_date, []).append(row)

    out: dict[str, dict[str, Any]] = {}
    for monday_date, arr in by_date.items():
        arr.sort(key=lambda x: x["dt"])
        open_bar = next((x for x in arr if x["dt"].hour == MARKET_OPEN_HOUR_UTC and x["dt"].minute == 0), None)
        bar5 = next((x for x in arr if x["dt"].hour == MARKET_OPEN_HOUR_UTC and x["dt"].minute == 5), None)
        bar10 = next((x for x in arr if x["dt"].hour == MARKET_OPEN_HOUR_UTC and x["dt"].minute == 10), None)
        weekend_last = _find_weekend_last_bar(arr)
        rec: dict[str, Any] = {}
        if weekend_last:
            rec["weekend_last_timestamp"] = weekend_last["dt"].isoformat()
            rec["weekend_last_close"] = weekend_last.get("close")
        if open_bar:
            open_px = open_bar.get("open")
            rec["open_bar_timestamp"] = open_bar["dt"].isoformat()
            rec["open_bar_open"] = open_px
            if open_px:
                if bar5 and bar5.get("close") is not None:
                    rec["open_5m_close"] = bar5["close"]
                    rec["open_5m_move_pct"] = (bar5["close"] - open_px) / open_px * 100.0
                if bar10 and bar10.get("close") is not None:
                    rec["open_10m_close"] = bar10["close"]
                    rec["open_10m_move_pct"] = (bar10["close"] - open_px) / open_px * 100.0
        if weekend_last and weekend_last.get("close"):
            base = weekend_last["close"]
            if bar5 and bar5.get("close") is not None:
                rec["weekend_last_to_5m_pct"] = (bar5["close"] - base) / base * 100.0
            if bar10 and bar10.get("close") is not None:
                rec["weekend_last_to_10m_pct"] = (bar10["close"] - base) / base * 100.0
        if rec:
            out[monday_date] = rec
    return out


def _infer_monday_market_date(ts: datetime) -> str | None:
    ts = ts.astimezone(timezone.utc)
    if ts.weekday() == 6 and ts.hour >= MARKET_OPEN_HOUR_UTC:
        return (ts + timedelta(hours=2)).date().isoformat()
    if ts.weekday() == 0:
        return ts.date().isoformat()
    return None


def _find_weekend_last_bar(arr: list[dict[str, Any]]) -> dict[str, Any] | None:
    weekend = [x for x in arr if x["dt"].weekday() == 6 and (x["dt"].hour < MARKET_OPEN_HOUR_UTC or (x["dt"].hour == MARKET_OPEN_HOUR_UTC and x["dt"].minute == 0))]
    if weekend:
        return weekend[-1]
    monday_pre = [x for x in arr if x["dt"].weekday() == 0 and (x["dt"].hour < MARKET_OPEN_HOUR_UTC or (x["dt"].hour == MARKET_OPEN_HOUR_UTC and x["dt"].minute == 0))]
    if monday_pre:
        return monday_pre[0]
    return None
