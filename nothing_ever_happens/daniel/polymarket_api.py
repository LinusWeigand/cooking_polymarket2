import datetime
from itertools import filterfalse
import pytz
import time as pytime
import pandas as pd
from IPython.display import display, clear_output
from typing import List, Dict, Optional, Union
import requests
import json
from urllib.parse import quote_plus
from . import binance_data as bdata

from crypto import get_probs_option_iv as crypto

def live_update_option_price(
    refresh_seconds: int = 15,
):
    """Continuously display the live option fair price every `refresh_seconds` seconds.

    Uses pricer.get_polymarket_fair_price (Monte Carlo with GARCH vol).
    In notebooks, uses rich display; otherwise prints to console.
    """


    count = 0
    try:
        while True:

            SYMBOL = "BTCUSDT"
            now = datetime.datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')
            last_full_hour = datetime.datetime.now(pytz.utc).replace(minute=0, second=0, microsecond=0)
            next_full_hour = last_full_hour + datetime.timedelta(hours=1)
            minutes_until_expiration = (next_full_hour - datetime.datetime.now(pytz.utc)).total_seconds() / 60
            err_text = None
            
            strike_price = float(bdata.get_price_at_time(SYMBOL, last_full_hour, interval="1m"))
            current_price = float(bdata.get_price_at_time(SYMBOL, datetime.datetime.now(pytz.utc), interval="1m"))

            clear_output(wait=True)

            polymarket_event = get_this_hour_btc_event()
            polymarket_event_price = polymarket_event['markets'][0]['outcomePrices']
        
            polymarket_event_price = float(json.loads(polymarket_event_price)[0])
            if strike_price and current_price:
                fair_prob = crypto.get_prob_above(strike_price, minutes_until_expiration)
                row = {
                    "Timestamp (UTC)": now,
                    "Fair Price ": fair_prob,
                    "Polymarket Price ": polymarket_event_price,
                    "Delta ": fair_prob - polymarket_event_price,
                    "S0 (BTC-USD)": current_price,
                    "Strike (K)": strike_price,
                    "Strike (UTC)": last_full_hour,
                    "Expiration (UTC)": next_full_hour,
                    "T (minutes)": minutes_until_expiration,
                }
                df = pd.DataFrame([row])
                try:
                    display(
                        df.style.format({
                            "Fair Price ": "{:.4f}",
                            "Polymarket Price ": "{:.4f}",
                            "Delta ": "+{:.4f}",
                            "S0 (BTC-USD)": "${:,.2f}",
                            "Strike (K)": "${:,.2f}",
                            "T (minutes)": "{:.0f}",
                        })
                    )
                except Exception:
                    print(df.to_string(index=False))
            else:
                print(f"[{now}] Pricing failed:\n{err_text or '(no details)'}")

            print(f"(Updating every {refresh_seconds} seconds; Ctrl-C to stop)")

            pytime.sleep(refresh_seconds)
    except KeyboardInterrupt:
        print("Stopped live updates.")



GAMMA_API_BASE = "https://gamma-api.polymarket.com"


def get_all_markets(
    search: str,
    status: str = "active",  # or "closed"
    limit: int = 50,
    start_page: int = 1,
    ascending: str = "false",
    session: Optional[requests.Session] = None,
    timeout: float = 15.0,
) -> List[Dict]:
    """
    Fetch all markets from the Polymarket Gamma API public search, iterating through pages
    until a page returns fewer than `limit` events.

    This closely adapts the provided snippet by calling:
      GET {GAMMA_API_BASE}/public-search?events_status={status}&q={search}&limit_per_type={limit}&page={page}

    Args:
        search: The search query, e.g. "bitcoin up or down".
        status: Event status filter, e.g. "active" or "closed".
        limit: Page size per type (Gamma uses `limit_per_type`), default 50.
        start_page: Page number to start from, default 1.
        session: Optional `requests.Session` to reuse connections.
        timeout: Request timeout in seconds.

    Returns:
        A list of market objects (dicts) collated across all pages.
    """
    s = session or requests.Session()

    page = start_page
    events_list = []

    while True:
        # Adaptation of the provided snippet using inline URL parameters
        url = (
            f"{GAMMA_API_BASE}/public-search?"
            f"events_status={status}&q={search}&limit_per_type={limit}&page={page}&ascending={ascending}"
        )
        resp = s.get(url, timeout=timeout)
        resp.raise_for_status()
        chunk = resp.json()


        # Determine when to stop based on the number of events returned on this page
        events = chunk["events"]
        events_list.extend(events)
        # Stop if the number of events is less than the requested limit
        if len(events) < limit:
            break

        page += 1

    return events_list

def _to_utc_hour(s: str) -> datetime.datetime:
        """Parse ISO-8601 string robustly and normalize to the top of the hour in UTC.

        Works across Python versions by trying:
        - datetime.fromisoformat (with 'Z' fix)
        - python-dateutil (if installed)
        - strptime fallbacks
        """
        v = s.strip()
        # Make a variant that stdlib fromisoformat accepts if input ends with 'Z'
        v_std = v[:-1] + "+00:00" if v.endswith("Z") else v

        dt = None
        # 1) Try stdlib fromisoformat (Py 3.7+)
        try:
            dt = datetime.datetime.fromisoformat(v_std)  # type: ignore[attr-defined]
        except Exception:
            dt = None

        # 2) Fall back to dateutil if available
        if dt is None:
            try:
                from dateutil import parser as _parser  # type: ignore
                dt = _parser.isoparse(v)
            except Exception:
                dt = None

        # 3) Last resort: strptime with common patterns
        if dt is None:
            v_z = (
                v.replace("Z", "+0000")
                 .replace("+00:00", "+0000")
                 .replace("-00:00", "-0000")
            )
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
                try:
                    dt = datetime.datetime.strptime(v_z, fmt)
                    break
                except Exception:
                    continue

        if dt is None:
            raise ValueError(f"Unrecognized ISO-8601 datetime: {s!r}")

        # Normalize to aware UTC and top-of-hour
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.utc)
        else:
            dt = dt.astimezone(pytz.utc)
        return dt.replace(minute=0, second=0, microsecond=0)


def get_this_hour_btc_event():
    now = datetime.datetime.now(pytz.utc)
    markets = get_all_markets(
        search="bitcoin up or down",
        status="active",  # or "closed"
    )
    markets = [m for m in markets if m["seriesSlug"] == "btc-up-or-down-hourly"]
    
    
    last_full_hour = datetime.datetime.now(pytz.utc).replace(minute=0, second=0, microsecond=0)
    markets = [m for m in markets if _to_utc_hour(m["markets"][0]["eventStartTime"]) == last_full_hour]
    return markets[0]


def filter_series_slugs(events: List[Dict], series_slug: str) -> List[Dict]:
    return [ev for ev in events if ev["seriesSlug"] == series_slug]
    
