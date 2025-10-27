import httpx
import requests
import pandas as pd
import numpy as np
import os
import time

# --- DATA FETCHING CONFIG ---
API_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 1000
FILENAME = "../../data/btc_1m_log_returns.csv"
WINDOW_SIZE = 10000


def get_bitcoin_1h_open_price():
    params = {
        "symbol": SYMBOL,
        "interval": "1h",
        "limit": 1
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    data = response.json()
    current_candle = data[0]
    open_price = current_candle[1]

    return float(open_price)

def get_latest_bitcoin_price():
    url = "https://api.binance.com/api/v3/ticker/price"
    params = {"symbol": "BTCUSDT"}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return float(data['price'])


# 20 requests per second at most
# We only do 4 requests per second right now
async def get_latest_bitcoin_price_async(http_client: httpx.AsyncClient):
    # start = time.time_ns()
    url = "https://api.binance.com/api/v3/ticker/price"
    params = {"symbol": "BTCUSDT"}
    try:
        response = await http_client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        # elapsed_ns = time.time_ns() - start
        # print(f"Binance call: {elapsed_ns // 1_000_000} ms")
        return float(data['price'])
    except httpx.HTTPError as e:
        print(f"Error fetching BTC price: {e}")
        return None


def fetch_candles(limit=LIMIT, startTime=None, endTime=None):
    """Fetches k-line candle data from Binance."""
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": limit
    }
    if startTime:
        params["startTime"] = startTime
    if endTime:
        params["endTime"] = endTime

    resp = requests.get(API_URL, params=params)
    resp.raise_for_status()
    return resp.json()


def klines_to_df(klines):
    """Converts raw kline data to a pandas DataFrame."""
    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "num_trades", "taker_base_vol",
        "taker_quote_vol", "ignore"
    ])
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close"] = df["close"].astype(float)
    return df[["open_time", "close"]]


def compute_log_returns(df):
    """Computes log returns WITHOUT the *100 scaling - we'll handle scaling in GARCH fitting."""
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    return df.dropna()[["open_time", "close", "log_return"]]


def backfill_initial():
    """Performs initial backfill of historical data."""
    print(f"Performing initial backfill for {WINDOW_SIZE} candles...")
    os.makedirs(os.path.dirname(FILENAME), exist_ok=True)
    all_klines = []
    end_time = int(time.time() * 1000)

    while len(all_klines) < WINDOW_SIZE:
        klines = fetch_candles(limit=LIMIT, endTime=end_time)
        if not klines:
            break
        all_klines = klines + all_klines
        end_time = klines[0][0] - 1
        time.sleep(0.2)

    df = klines_to_df(all_klines[-WINDOW_SIZE:])
    returns = compute_log_returns(df)
    returns.to_csv(FILENAME, index=False)
    print(f"Backfilled {len(returns)} candles into {FILENAME}")


def update_file():
    """Updates the data file with the latest candles."""
    if not os.path.exists(FILENAME):
        backfill_initial()
        return

    existing = pd.read_csv(FILENAME, parse_dates=["open_time"])
    last_time = int(existing["open_time"].max().timestamp() * 1000)

    new_klines = fetch_candles(startTime=last_time + 1)
    if not new_klines or len(new_klines) <= 1:
        print("No new candles to update.")
        return

    last_row = existing.tail(1)
    new_df_raw = klines_to_df(new_klines)
    temp_df = pd.concat([last_row[['open_time', 'close']], new_df_raw], ignore_index=True)
    temp_df['open_time'] = pd.to_datetime(temp_df['open_time'])
    new_returns = compute_log_returns(temp_df)
    updated = pd.concat([existing, new_returns]).drop_duplicates(subset="open_time", keep="last")
    updated = updated.sort_values("open_time").tail(WINDOW_SIZE)
    updated.to_csv(FILENAME, index=False)
    print(f"Updated {FILENAME} with {len(new_returns)} new candles. Total stored: {len(updated)}")


if __name__ == '__main__':
    update_file()
