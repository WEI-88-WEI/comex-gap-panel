from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any


def compute_monday_gaps(rows: list[dict[str, Any]], years: int = 5) -> dict[str, Any]:
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
        return {"items": [], "summary": {}}

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
                }
            )
        prev = row

    gap_pcts = [x["gap_pct"] for x in items if x.get("gap_pct") is not None]
    abs_gap_pcts = [abs(x) for x in gap_pcts]
    pos = [x for x in gap_pcts if x > 0]
    neg = [x for x in gap_pcts if x < 0]

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

    return {
        "items": items,
        "summary": summary,
        "range": {
            "start": window[0]["date"] if window else None,
            "end": window[-1]["date"] if window else None,
        },
    }
