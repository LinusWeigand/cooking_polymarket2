import asyncio
import time
import threading
import pandas as pd
import logging
import requests
import sys

from crypto.api.binance import get_latest_bitcoin_price

try:
    import garch_monte_carlo
except ImportError:
    print("Error: Could not import the 'garch_monte_carlo' module.", file=sys.stderr)
    sys.exit(1)

from api.binance import update_file, backfill_initial, FILENAME

# --- Configuration ---
TARGET_MULTIPLIER = 1.0005   # Target price: 0.5% above current
HORIZON_SECONDS = 60 * 60
NUM_SIMULATIONS = 1_000_000
POLL_INTERVAL_SECONDS = 0.25

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BTCPriceMonitor:
    """Fetch BTC prices and run plain Monte Carlo simulations."""

    def __init__(self, file_lock):
        self.file_lock = file_lock
        self.last_price = None
        self.price_changes = 0
        self.update_count = 0
        self.simulation_count = 0
        self.start_time = time.time()

    async def handle_price_update(self, price, timestamp_ms, fetch_time_ms):
        self.update_count += 1
        if self.last_price is not None and price != self.last_price:
            self.price_changes += 1
        self.last_price = price

        target_price = price * TARGET_MULTIPLIER
        sim_start_time = time.time_ns()
        prob = self.calculate_probability(price, target_price)
        sim_end_time = time.time_ns()
        sim_time_ms = (sim_end_time - sim_start_time) / 1_000_000

        if prob is not None:
            elapsed = time.time() - self.start_time
            updates_per_sec = self.update_count / elapsed if elapsed > 0 else 0
            logger.info(
                f"💰 ${price:,.2f} | P(>{target_price:,.2f})={prob:.2%} | "
                f"Fetch:{fetch_time_ms:.0f}ms | Sim:{sim_time_ms:.0f}ms | "
                f"Upd/s:{updates_per_sec:.1f} | PriceΔ:{self.price_changes}"
            )

    def calculate_probability(self, current_price, target_price):
        try:
            with self.file_lock:
                returns = pd.read_csv(FILENAME)["log_return"].dropna().tolist()

            horizon_minutes = max(1, int(HORIZON_SECONDS / 60))

            prob = garch_monte_carlo.calculate_probability_plain(
                returns=returns,
                current_price=current_price,
                target_price=target_price,
                horizon_minutes=horizon_minutes,
                num_simulations=NUM_SIMULATIONS,
            )
            self.simulation_count += 1
            return prob
        except Exception as e:
            logger.error(f"❌ Monte Carlo simulation error: {e}", exc_info=True)
            return None

    async def run(self):
        self.candle_manager.start()
        logger.info("🚀 Starting BTC Price Monitor...")
        logger.info(f"📊 Target: {(TARGET_MULTIPLIER - 1):.2%} in {HORIZON_SECONDS}s")
        logger.info(f"🔢 Simulations per calculation: {NUM_SIMULATIONS:,}")
        logger.info(f"📡 Polling Binance API every {POLL_INTERVAL_SECONDS}s")

        while True:
            try:
                fetch_start = time.time()
                price = await asyncio.to_thread(get_latest_bitcoin_price)
                fetch_time_ms = (time.time() - fetch_start) * 1000
                timestamp_ms = int(time.time() * 1000)
                await self.handle_price_update(price, timestamp_ms, fetch_time_ms)
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Could not fetch price: {e}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"❌ Unexpected error: {e}", exc_info=True)
                await asyncio.sleep(1)


async def main():
    monitor = BTCPriceMonitor()
    await monitor.run()

def calculate_probability(self, returns, current_price, target_price, min_remaining):
    try:
        prob = garch_monte_carlo.calculate_probability_plain(
            returns=returns,
            current_price=current_price,
            target_price=target_price,
            horizon_minutes=min_remaining,
            num_simulations=NUM_SIMULATIONS,
        )
        return prob
    except Exception as e:
        logger.error(f"❌ Monte Carlo simulation error: {e}", exc_info=True)
        return None


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Shutting down gracefully...")
