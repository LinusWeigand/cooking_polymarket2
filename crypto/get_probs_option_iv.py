import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from scipy.interpolate import PchipInterpolator
import numpy as np
import time

from crypto.api.binance import get_latest_bitcoin_price
from crypto.api.deribit import fetch_ticker_data, get_bitcoin_0dte_option_chain


class OptionDataCache:
    def __init__(self):
        self.last_update = 0
        self.options = []
        self.iv_interp = None

    def update_if_needed(self, update_interval_sec=60):
        if time.time() - self.last_update > update_interval_sec:
            options, expiration_timestamp_ms = get_bitcoin_0dte_option_chain()
            S = get_latest_bitcoin_price()
            vol_smile = get_vol_smile(options)
            self.iv_interp = smooth_vol_smile(vol_smile)
            self.last_update = time.time()
            self.options = options



def get_vol_smile(options):
    volatility_data_api = []
    r = 0.0

    for option in options:
        name = option['instrument_name']
        option_type = option['option_type']

        ticker_data = fetch_ticker_data(name)

        iv_api = ticker_data.get('mark_iv')
        K = option['strike']

        if iv_api is not None:
            volatility_data_api.append({
                'strike': float(K),
                'iv': iv_api,
                'type': option_type
            })

    df_api = pd.DataFrame(volatility_data_api)
    return df_api

def smooth_vol_smile(vol_smile):
    df_unique = vol_smile.groupby('strike')['iv'].mean().reset_index()
    strikes = df_unique['strike'].values
    ivs = df_unique['iv'].values / 100

    iv_interp = PchipInterpolator(strikes, ivs)

    strike_dense = np.linspace(strikes.min(), strikes.max(), 500)
    # iv_dense = iv_interp(strike_dense) * 100

    return iv_interp

def scale_vol_smile(original_iv_interp, T_original_sec, T_target_sec):
    scaling_factor = np.sqrt(T_target_sec / T_original_sec)
    return lambda K: original_iv_interp(K) * scaling_factor

def plot_vol_smile(iv_interp, strike_dense, iv_dense, df_api, min_expiry, S):
    plt.figure(figsize=(12, 7))
    calls_api = df_api[df_api['type'] == 'call']
    puts_api = df_api[df_api['type'] == 'put']

    plt.plot(calls_api['strike'], calls_api['iv'], 'o', color='blue', label='API Calls (mark_iv)')
    plt.plot(puts_api['strike'], puts_api['iv'], 'o', color='orange', label='API Puts (mark_iv)')
    plt.plot(strike_dense, iv_dense, '-', color='green', label='Interpolated Smile')

    plt.axvline(x=S, color='gray', linestyle='--', label=f'Current BTC Price: ${S:,.0f}')
    plt.title(f'BTC Volatility Smile - Expiration: {datetime.fromtimestamp(min_expiry/1000).strftime("%d%b%y")}')
    plt.xlabel('Strike Price ($)')
    plt.ylabel('Implied Volatility (%)')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.show()

def plot_pdf(S_grid, pdf):
    plt.figure(figsize=(10,6))
    plt.plot(S_grid, pdf, color='green')
    plt.title('Risk-Neutral PDF for BTC in 1 Hour')
    plt.xlabel('BTC Price ($)')
    plt.ylabel('Probability Density')
    plt.grid(True)
    plt.show()

def calc_pdf_fast(S, r, iv_interp, ms_left):
    seconds_left = ms_left / 1000
    year_seconds = 365 * 24 * 60 * 60
    T = seconds_left / year_seconds

    S_grid = np.linspace(S * 0.98, S * 1.02, 1000)
    sigma = iv_interp(S_grid)
    pdf = (1 / (S_grid * sigma * np.sqrt(2 * np.pi * T))) * \
          np.exp(-(np.log(S_grid / S) - (r - 0.5 * sigma ** 2) * T) ** 2 / (2 * sigma ** 2 * T))
    pdf /= pdf.sum() * (S_grid[1] - S_grid[0])
    return S_grid, pdf

def get_prob_above_below(price, S, iv_interp, ms_left):
    S_grid, pdf = calc_pdf_fast(S, r=0.0, iv_interp=iv_interp, ms_left=ms_left)
    prob_above = np.trapz(pdf[S_grid > price], S_grid[S_grid > price])
    # prob_below = np.trapz(pdf[S_grid <= price], S_grid[S_grid <= price])
    prob_below = 1 - prob_above # Faster
    return prob_above, prob_below


if __name__ == "__main__":
    cache = OptionDataCache()

    mins=33
    secs=0
    total_seconds = mins * 60 + secs

    while True:
        cache.update_if_needed()
        S = get_latest_bitcoin_price()
        target_price = 113699.24
        prob_above, prob_below = get_prob_above_below(
            target_price,
            S,
            cache.iv_interp,
            ms_left=total_seconds * 1000
        )
        print(prob_above)
        # print(prob_below)

        time.sleep(0.1)
        total_seconds -= 0.1


