import pandas as pd
import time
import math
import pickle
from pathlib import Path
import hashlib
from arch import arch_model

try:
    import garch_monte_carlo
except ImportError:
    print("=" * 80)
    print("!!! ERROR: Could not import 'garch_monte_carlo'")
    print("Run 'maturin develop' in the Rust project directory.")
    print("=" * 80)
    exit()

FILENAME = "../data/btc_1m_log_returns.csv"
NUM_SIMULATIONS = 800000


class GARCHCache:
    """Cache GARCH model to avoid refitting"""

    def __init__(self, cache_dir="../cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

    def get_cache_key(self, log_returns):
        """Hash based on data length + last value"""
        key_data = f"{len(log_returns)}_{log_returns.iloc[-1]:.10f}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def load(self, log_returns):
        cache_key = self.get_cache_key(log_returns)
        cache_file = self.cache_dir / f"garch_{cache_key}.pkl"

        if cache_file.exists():
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        return None

    def save(self, log_returns, garch_res, std_residuals):
        cache_key = self.get_cache_key(log_returns)
        cache_file = self.cache_dir / f"garch_{cache_key}.pkl"

        with open(cache_file, 'wb') as f:
            pickle.dump({
                'garch_res': garch_res,
                'std_residuals': std_residuals
            }, f)


def fit_garch_cached(log_returns, cache):
    """Fit GARCH with caching"""
    cached = cache.load(log_returns)
    if cached:
        print("✅ Using cached GARCH model")
        return cached['garch_res'], cached['std_residuals']

    print("🔄 Fitting new GARCH model...")
    scaled_returns = log_returns * 100
    am = arch_model(scaled_returns, p=1, q=1, vol='Garch', dist='t', rescale=True)
    res = am.fit(disp='off')

    if not res.convergence_flag:
        print("⚠️  WARNING: GARCH model did not converge properly!")

    print(res.summary())

    std_resid = res.resid / res.conditional_volatility
    cache.save(log_returns, res, std_resid.dropna())

    return res, std_resid.dropna()


class FastGARCHSimulator:
    """Optimized simulator - fit once, query many times"""

    def __init__(self, filename=FILENAME):
        self.filename = filename
        self.cache = GARCHCache()

        # Load data
        self.log_returns = pd.read_csv(filename)['log_return'].dropna()

        # Fit GARCH (cached)
        self.garch_res, self.std_residuals = fit_garch_cached(
            self.log_returns, self.cache
        )

        # Extract parameters once
        params = self.garch_res.params
        self.omega = params['omega']
        self.alpha = params['alpha[1]']
        self.beta = params['beta[1]']
        self.residuals_list = self.std_residuals.tolist()

        # Cache state
        self.last_resid = self.garch_res.resid.iloc[-1]
        self.last_sigma_sq = self.garch_res.conditional_volatility.iloc[-1] ** 2

        # Diagnostics
        alpha_beta_sum = self.alpha + self.beta
        print(f"\n📊 Model: α+β = {alpha_beta_sum:.4f}")
        if alpha_beta_sum > 0.999:
            print("   ⚠️  WARNING: Close to non-stationarity")
        print()

    def get_probability(self, start_price, target_price, horizon_seconds,
                        num_simulations=NUM_SIMULATIONS):
        """Calculate probability using optimized Rust function"""
        horizon_minutes = max(1, math.ceil(horizon_seconds / 60))

        return garch_monte_carlo.calculate_probability_only(
            omega=self.omega,
            alpha=self.alpha,
            beta=self.beta,
            last_resid=self.last_resid,
            last_sigma_sq=self.last_sigma_sq,
            residuals=self.residuals_list,
            current_price=start_price,
            target_price=target_price,
            horizon_minutes=horizon_minutes,
            num_simulations=num_simulations
        )


if __name__ == "__main__":
    # Initialize simulator once
    print("Initializing simulator...")
    sim = FastGARCHSimulator()

    # Test price
    start_price = 114577.85
    print(f"💰 Test BTC Price: ${start_price:,.2f}\n")

    # Single query test
    TARGET_PRICE = start_price * 1.005
    HORIZON_SECONDS = 60 * 15

    print("🚀 Running single probability calculation...")
    start_time = time.time()

    prob = sim.get_probability(
        start_price,
        TARGET_PRICE,
        HORIZON_SECONDS,
        NUM_SIMULATIONS
    )

    elapsed = time.time() - start_time

    print(f"\n>>> P(BTC > ${TARGET_PRICE:,.2f} in {HORIZON_SECONDS}s) = {prob:.2%}")
    print(f"⏱️  Simulation time: {elapsed:.3f}s\n")
    print(f"{elapsed:.3f}s - {prob:.2%}")

    # Multiple rapid queries
    # print("=" * 60)
    # print("Running multiple rapid queries...")
    # print("=" * 60)
    #
    # queries = [
    #     (start_price * 1.005, 900, "+0.5% in 15min"),
    #     (start_price * 1.01, 1800, "+1.0% in 30min"),
    #     (start_price * 0.995, 300, "-0.5% in 5min"),
    #     (start_price * 1.002, 600, "+0.2% in 10min"),
    #     (start_price * 1.008, 1200, "+0.8% in 20min"),
    # ]
    #
    # total_start = time.time()
    #
    # for target, horizon_sec, description in queries:
    #     query_start = time.time()
    #     prob = sim.get_probability(start_price, target, horizon_sec, NUM_SIMULATIONS)
    #     query_time = time.time() - query_start
    #     print(f"{description:20} = {prob:6.2%}  [{query_time:.3f}s]")
    #
    # total_elapsed = time.time() - total_start
    #
    # print(f"\n{'=' * 60}")
    # print(f"Total time: {total_elapsed:.3f}s")
    # print(f"Average per query: {total_elapsed / len(queries):.3f}s")
    # print(f"Queries per second: {len(queries) / total_elapsed:.2f}")