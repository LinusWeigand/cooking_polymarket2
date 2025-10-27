import asyncio
import time
import pandas as pd
import sys

from crypto.api.binance import get_latest_bitcoin_price, get_bitcoin_1h_open_price
from crypto.api.polymarket.get_event import get_current_event
from crypto.utils import get_next_hour_timestamp, Asset

try:
    import garch_monte_carlo
except ImportError:
    print("Error: Could not import the 'garch_monte_carlo' module.", file=sys.stderr)
    sys.exit(1)

NUM_SIMULATIONS = 1_000_000
FILENAME = "../data/btc_1m_log_returns.csv"

if __name__ == "__main__":
    returns = pd.read_csv(FILENAME)["log_return"].dropna().tolist()
    event = get_current_event(Asset.Bitcoin)
    open_price = get_bitcoin_1h_open_price()
    close_timestamp = get_next_hour_timestamp()

    while True:
        secs_left = close_timestamp - time.time()
        mins = secs_left // 60
        secs = secs_left % 60
        current_btc_price = get_latest_bitcoin_price()

        p_fair = garch_monte_carlo.calculate_probability_plain(
            returns=returns,
            current_price=current_btc_price,
            target_price=open_price,
            horizon_minutes=max(1, round(mins + secs / 60)),
            num_simulations=NUM_SIMULATIONS
        )
        print(p_fair)
