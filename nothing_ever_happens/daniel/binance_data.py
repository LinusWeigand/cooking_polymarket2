"""
Binance data utilities for fetching BTC/USDT market data.

Public-only endpoints are used; no API key required.

Functions:
- get_intraday_close_for_date: intraday close series for a specific calendar date in a given timezone.
- get_daily_close_series: daily close series over a date range.
- get_last_price: latest spot price.
- get_price_at_time: approximate price at a specific UTC timestamp from klines.
 - get_hour_open: open price of the 1h candle starting at a given UTC hour.
"""
from __future__ import annotations

from datetime import datetime, timedelta, date, time as dt_time
from typing import Tuple, Optional

import pandas as pd
import pytz

try:
    from binance.spot import Spot as BinanceSpot
except Exception as e:  # pragma: no cover
    raise ImportError(
        "binance-connector is required. Install with `pip install binance-connector`.\n"
        f"Original import error: {e}"
    )

# ---------- Helpers ----------

UTC = pytz.utc


def _to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return int(dt.timestamp() * 1000)


def _client() -> BinanceSpot:
    # Public client; no API key needed for market data
    return BinanceSpot()


# ---------- Core fetchers ----------

KLINE_COLS = [
    "open_time",    # 0
    "open",         # 1
    "high",         # 2
    "low",          # 3
    "close",        # 4
    "volume",       # 5
    "close_time",   # 6
    "qav",          # 7 quote asset volume
    "trades",       # 8
    "taker_base",   # 9
    "taker_quote",  # 10
    "ignore",       # 11
]


def _fetch_klines_df(symbol: str, interval: str, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
    """Fetch klines between [start_utc, end_utc] with pagination and return as DataFrame.
    Index is the close_time in UTC.
    """
    if start_utc.tzinfo is None or end_utc.tzinfo is None:
        raise ValueError("start_utc and end_utc must be timezone-aware")
    if end_utc <= start_utc:
        return pd.DataFrame(columns=KLINE_COLS)

    cli = _client()
    start_ms = _to_ms(start_utc)
    end_ms = _to_ms(end_utc)

    out = []
    while True:
        batch = cli.klines(symbol=symbol, interval=interval, startTime=start_ms, endTime=end_ms, limit=1000)
        if not batch:
            break
        out.extend(batch)
        # Advance start_ms to past the last close_time to avoid duplicates
        last_close_ms = batch[-1][6]
        # If we received fewer than 1000 rows or we've reached/passed end, stop
        if len(batch) < 1000 or last_close_ms >= end_ms:
            break
        start_ms = last_close_ms + 1

    if not out:
        return pd.DataFrame(columns=KLINE_COLS)

    df = pd.DataFrame(out, columns=KLINE_COLS)
    # Convert numeric types
    for col in ("open", "high", "low", "close", "volume", "qav", "taker_base", "taker_quote"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df = df.set_index("close_time").sort_index()
    return df


def get_intraday_close_for_date(
    symbol: str,
    target_date: date,
    target_tz: pytz.BaseTzInfo,
    intervals: tuple[str, ...] = ("1m", "5m", "15m"),
) -> Tuple[pd.Series, Optional[str]]:
    """Return (close_series_utc, used_interval) for the given calendar date in target_tz.
    If no data found, returns (empty_series, None).
    """
    start_local = target_tz.localize(datetime.combine(target_date, dt_time(0, 0)))
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(UTC)
    end_utc = end_local.astimezone(UTC)

    for interval in intervals:
        df = _fetch_klines_df(symbol, interval, start_utc, end_utc)
        if not df.empty:
            closes = df["close"].dropna()
            closes.index = closes.index.tz_convert(UTC)
            return closes, interval
    return pd.Series(dtype=float), None


def get_daily_close_series(symbol: str, start_date: date, end_date_exclusive: date) -> pd.Series:
    """Fetch daily close series for [start_date, end_date_exclusive).
    Returns a UTC tz-aware Series indexed by close_time.
    """
    start_local = UTC.localize(datetime.combine(start_date, dt_time(0, 0)))
    end_local = UTC.localize(datetime.combine(end_date_exclusive, dt_time(0, 0)))
    df = _fetch_klines_df(symbol, "1d", start_local, end_local)
    if df.empty:
        return pd.Series(dtype=float)
    closes = df["close"].dropna()
    closes.index = closes.index.tz_convert(UTC)
    return closes


def get_last_price(symbol: str) -> float:
    cli = _client()
    data = cli.ticker_price(symbol=symbol)
    return float(data["price"])


def get_price_at_time(symbol: str, target_utc: datetime, interval: str = "1m") -> float:
    """Return an approximate price at the given UTC timestamp using klines.

    Preference order:
    1) Close price of the kline whose close_time == target_utc.
    2) Close price of the last kline with close_time <= target_utc.
    3) Close price of the nearest kline by absolute time difference within a small window.

    Raises ValueError if no kline is found within the search window.
    """
    if target_utc.tzinfo is None:
        raise ValueError("target_utc must be timezone-aware (UTC)")

    # Define a small search window around the target time
    # For 1m interval, +/- 5 minutes is usually sufficient
    window = pd.Timedelta(minutes=5)
    start_utc = (target_utc - window).astimezone(UTC)
    end_utc = (target_utc + window).astimezone(UTC)

    df = _fetch_klines_df(symbol, interval, start_utc, end_utc)
    if df.empty:
        raise ValueError("No klines available around the requested timestamp")

    # Exact match on close_time
    if target_utc in df.index:
        return float(df.loc[target_utc, "close"])  # exact candle close

    # Last close_time <= target
    df_before = df[df.index <= target_utc]
    if not df_before.empty:
        return float(df_before.iloc[-1]["close"])  # previous close

    # Otherwise choose the nearest by absolute time difference
    nearest_idx = (df.index - target_utc).abs().argmin()
    return float(df.iloc[nearest_idx]["close"]) 


def get_hour_open(symbol: str, hour_start_utc: datetime) -> float:
    """Return the open price of the 1h candle that starts at `hour_start_utc`.

    Searches for the kline whose open_time == hour_start_utc. If not directly found,
    looks for the kline whose close_time == hour_start_utc + 1h. If still not found,
    returns the first kline's open within a small window around the hour.
    """
    if hour_start_utc.tzinfo is None:
        raise ValueError("hour_start_utc must be timezone-aware (UTC)")

    hour_end_utc = hour_start_utc + pd.Timedelta(hours=1)
    # Small window to ensure coverage
    start_utc = hour_start_utc - pd.Timedelta(minutes=2)
    end_utc = hour_end_utc + pd.Timedelta(minutes=2)

    df = _fetch_klines_df(symbol, "1h", start_utc, end_utc)
    if df.empty:
        raise ValueError("No 1h klines found around the requested hour.")

    # Prefer exact open_time match
    if "open_time" in df.columns:
        mask = df["open_time"] == hour_start_utc
        if mask.any():
            return float(df.loc[mask, "open"].iloc[0])

    # Next, check for close_time match (index)
    if hour_end_utc in df.index:
        return float(df.loc[hour_end_utc, "open"])

    # Fallback: first row's open within window
    return float(df.iloc[0]["open"]) 
