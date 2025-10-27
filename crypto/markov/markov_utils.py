import time
import threading
import math
import numpy as np
import pandas as pd
from typing import Tuple, Any, Optional

# UI
import tkinter as tk
from tkinter import ttk

# SciPy for the fitted distribution
from scipy import stats as sp_stats

# Reuse your Binance helpers
from crypto.api.binance import fetch_candles, klines_to_df

# -----------------------------
# Probability/model components
# -----------------------------

def _freeze_best_model_from_fit(
    fit_result: dict,
    ranking_metric: str = "bic",  # one of {"bic","chi2","kl"}
) -> Tuple[str, Any]:
    """
    Extract the best fitted model (SciPy frozen distribution) from analyze_distribution_fit(...) result.
    """
    if "fits" not in fit_result:
        raise ValueError("fit_result must contain key 'fits' (a DataFrame)")

    fits_df = fit_result["fits"]
    metric = ranking_metric.lower()
    if metric not in {"bic", "chi2", "kl"}:
        raise ValueError("ranking_metric must be one of {'bic','chi2','kl'}")

    key_map = {"bic": "best_by_bic", "chi2": "best_by_chi2", "kl": "best_by_kl"}
    preferred = fit_result.get(key_map[metric], None)
    if preferred is None or preferred not in fits_df.index:
        valid = fits_df.replace([np.inf, -np.inf], np.nan).dropna(subset=[metric])
        if valid.empty:
            raise ValueError(f"No valid models found to rank by {metric}")
        preferred = valid.sort_values(metric).index[0]

    dist_map = {
        "norm": sp_stats.norm,
        "t": sp_stats.t,
        "laplace": sp_stats.laplace,
        "cauchy": sp_stats.cauchy,
        "skewnorm": sp_stats.skewnorm,
    }
    if preferred not in dist_map:
        raise ValueError(f"Unsupported best model '{preferred}'")

    params = fits_df.loc[preferred, "params"]
    if params is None:
        raise ValueError(f"Best model '{preferred}' has no fitted params")

    frozen = dist_map[preferred](*params)
    return preferred, frozen


def _std_norm_sf(z):
    # upper-tail of standard normal; sp_stats handles vector/scalar
    return sp_stats.norm.sf(z)


def probability_hour_finishes_positive_from_fit(
    fit_result: dict,
    current_minute_index: int,     # 0..59 within hour, last completed minute
    cum_delta_so_far: float,       # sum of deltas up to and including current minute
    include_current_minute: bool = True,
    minutes_per_hour: int = 60,
    ranking_metric: str = "bic",
) -> float:
    """
    Compute P(hour finishes positive) using Gaussian (CLT) approximation for remaining minutes.
    Moments (mu, sigma) are taken from the best fitted minute-distribution model.

    - remaining sum S ~ Normal(remaining*mu, remaining*sigma^2)
    - We want P(cum_delta_so_far + S > 0).
    """
    _, frozen = _freeze_best_model_from_fit(fit_result, ranking_metric=ranking_metric)
    mu = float(frozen.mean())
    sigma = float(frozen.std())

    k = current_minute_index + (1 if include_current_minute else 0)
    k = max(0, min(k, minutes_per_hour))
    remaining = minutes_per_hour - k

    if remaining <= 0 or sigma <= 0:
        final_value = cum_delta_so_far + remaining * mu
        if final_value > 0: return 1.0
        if final_value < 0: return 0.0
        return 0.5

    m = remaining * mu
    s = math.sqrt(remaining) * sigma
    z = (-cum_delta_so_far - m) / s
    p_pos = float(_std_norm_sf(z))
    return p_pos


# -----------------------------
# Live data + UI
# -----------------------------

def _lerp(a, b, t): return a + (b - a) * t
def _lerp_color(c1, c2, t):
    # colors as (r,g,b) 0..255
    return tuple(int(round(_lerp(c1[i], c2[i], t))) for i in range(3))
def _rgb_hex(rgb): return f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"

class LiveProbabilityUI:
    """
    Tkinter UI that:
     - polls Binance 1m klines,
     - computes cum delta within the current hour,
     - shows P(hour finishes positive) from your fitted model,
     - pretty, minimalistic gauge + progress + stats.
    """
    def __init__(
        self,
        fit_result: dict,
        ranking_metric: str = "bic",
        poll_seconds: float = 5.0,
        history_limit_minutes: int = 120,   # how many minutes to fetch each poll
        window_title: str = "BTC Hour-Finish Probability",
        width: int = 460,
        height: int = 360,
    ):
        self.fit_result = fit_result
        self.ranking_metric = ranking_metric
        self.poll_seconds = poll_seconds
        self.history_limit_minutes = max(60, history_limit_minutes)
        self.width = width
        self.height = height

        self._stop_event = threading.Event()
        self._last_open_time = None
        self._prob_history = []   # for sparkline

        # Prepare UI
        self.root = tk.Tk()
        self.root.title(window_title)
        self.root.configure(bg="#0f172a")  # slate-900
        try:
            self.root.attributes("-topmost", False)
        except Exception:
            pass

        # Canvas
        self.canvas = tk.Canvas(self.root, width=self.width, height=self.height, bg="#0f172a", highlightthickness=0)
        self.canvas.pack(fill="both", expand=True)

        # Fonts
        self.font_large = ("Helvetica", 28, "bold")
        self.font_medium = ("Helvetica", 14, "bold")
        self.font_small = ("Helvetica", 11)

        # Static labels
        self.title_text = self.canvas.create_text(
            self.width // 2, 30, fill="#e2e8f0", font=("Helvetica", 16, "bold"),
            text="BTC Hour-Finish Probability"
        )
        self.status_text = self.canvas.create_text(
            self.width // 2, self.height - 20, fill="#94a3b8", font=self.font_small,
            text="Connecting to Binance..."
        )

        # Placeholders for dynamic items
        self.arc_bg = None
        self.arc_fg = None
        self.minute_ring = None
        self.prob_text = None
        self.cum_text = None
        self.model_text = None
        self.countdown_text = None

        # Draw static gauge background
        self._draw_static()

        # Start periodic update loop
        self.root.after(200, self._update_loop)
        # Start countdown updates (independent of data polling)
        self._update_countdown()

    def _draw_static(self):
        cx, cy = self.width // 2, self.height // 2 + 30
        r_outer = min(self.width, self.height) // 2 - 30
        r_inner = r_outer - 24

        # Background arc (full semicircle)
        self.arc_bg = self.canvas.create_arc(
            cx - r_outer, cy - r_outer, cx + r_outer, cy + r_outer,
            start=180, extent=180, style="arc", outline="#334155", width=20
        )

        # Foreground arc (probability)
        self.arc_fg = self.canvas.create_arc(
            cx - r_outer, cy - r_outer, cx + r_outer, cy + r_outer,
            start=180, extent=0, style="arc", outline="#22c55e", width=20
        )

        # Minute progress ring (inner thin arc)
        self.minute_ring = self.canvas.create_arc(
            cx - r_inner, cy - r_inner, cx + r_inner, cy + r_inner,
            start=180, extent=0, style="arc", outline="#64748b", width=4
        )

        # Numeric readouts
        self.prob_text = self.canvas.create_text(
            cx, cy - 20, fill="#e2e8f0", font=self.font_large, text="--%"
        )
        # Countdown to next full hour under the probability
        self.countdown_text = self.canvas.create_text(
            cx, cy + 0, fill="#a7f3d0", font=self.font_small, text="T−--:-- to next hour"
        )
        self.cum_text = self.canvas.create_text(
            cx, cy + 28, fill="#cbd5e1", font=self.font_medium, text="Δhour: --"
        )
        self.model_text = self.canvas.create_text(
            cx, cy + 54, fill="#94a3b8", font=self.font_small, text=""
        )

    def _set_status(self, msg: str):
        self.canvas.itemconfigure(self.status_text, text=msg)

    def _fetch_latest_df(self) -> pd.DataFrame:
        # Fetch recent 1m candles
        klines = fetch_candles(limit=min(1000, self.history_limit_minutes))
        df = klines_to_df(klines)
        # Ensure numeric close
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        # Compute minute deltas
        df["delta"] = df["close"].diff()
        # Drop the first NaN delta
        df = df.dropna(subset=["delta"]).reset_index(drop=True)
        return df

    def _compute_current_hour_state(self, df: pd.DataFrame) -> Tuple[Optional[pd.Timestamp], int, float]:
        """
        Return (hour_start, current_minute_index, cum_delta_so_far).
        Uses the last completed 1m candle timestamp in df as the current point.
        """
        if df.empty:
            return None, 0, 0.0

        # Last completed minute
        last_ts = pd.to_datetime(df["open_time"].iloc[-1])
        hour_start = last_ts.floor("h")
        minute_idx = int(last_ts.minute)

        # Sum deltas within this hour up to current minute
        within_hour = df["open_time"].dt.floor("h") == hour_start
        cum_delta = float(df.loc[within_hour, "delta"].sum())

        return hour_start, minute_idx, cum_delta

    def _probability_color(self, p: float) -> str:
        # Red -> Yellow -> Green gradient
        red = (231, 76, 60)
        yellow = (246, 190, 0)
        green = (46, 204, 113)
        t = max(0.0, min(1.0, p))
        if t < 0.5:
            c = _lerp_color(red, yellow, t / 0.5)
        else:
            c = _lerp_color(yellow, green, (t - 0.5) / 0.5)
        return _rgb_hex(c)

    def _update_gauge(self, p: float, minute_idx: int, cum_delta: float, model_name: str):
        cx, cy = self.width // 2, self.height // 2 + 30
        r_outer = min(self.width, self.height) // 2 - 30
        r_inner = r_outer - 24

        # Update foreground arc to reflect probability on 180 degrees
        extent = max(0.0, min(180.0, 180.0 * p))
        color = self._probability_color(p)
        self.canvas.itemconfigure(self.arc_fg, extent=extent, outline=color)

        # Minute progress ring (0..59 mapped to 180 degrees)
        minute_extent = 180.0 * max(0, min(59, minute_idx)) / 59.0 if minute_idx > 0 else 0.0
        self.canvas.itemconfigure(self.minute_ring, extent=minute_extent)

        # Texts
        self.canvas.itemconfigure(self.prob_text, text=f"{p*100:,.2f}%")
        self.canvas.itemconfigure(self.cum_text, text=f"Δhour: {cum_delta:,.2f}")
        self.canvas.itemconfigure(self.model_text, text=f"Model: {model_name.upper()}")

    def _update_loop(self):
        if self._stop_event.is_set():
            return

        try:
            df = self._fetch_latest_df()
            hour_start, minute_idx, cum_delta = self._compute_current_hour_state(df)

            if hour_start is None:
                self._set_status("No data yet...")
            else:
                # Compute probability
                model_name, frozen = _freeze_best_model_from_fit(self.fit_result, self.ranking_metric)
                mu = float(frozen.mean())
                sigma = float(frozen.std())
                p = probability_hour_finishes_positive_from_fit(
                    fit_result=self.fit_result,
                    current_minute_index=minute_idx,
                    cum_delta_so_far=cum_delta,
                    include_current_minute=True,
                    ranking_metric=self.ranking_metric,
                )

                # Update history and UI
                self._prob_history.append(p)
                self._prob_history = self._prob_history[-240:]  # keep last ~4 hours of mins
                self._update_gauge(p, minute_idx, cum_delta, model_name)
                self._set_status(
                    f"{hour_start.strftime('%Y-%m-%d %H:%M')} | minute {minute_idx:02d}/59 | μ={mu:.5f}, σ={sigma:.5f}"
                )

        except Exception as e:
            self._set_status(f"Error: {e}")

        # Schedule next update
        self.root.after(int(self.poll_seconds * 1000), self._update_loop)

    def _update_countdown(self):
        """Update 'T−MM:SS to next hour' once per second."""
        if self._stop_event.is_set():
            return
        now = pd.Timestamp.now()
        next_hour = (now.floor("h") + pd.Timedelta(hours=1))
        remaining = max(0, int((next_hour - now).total_seconds()))
        mm, ss = divmod(remaining, 60)
        self.canvas.itemconfigure(self.countdown_text, text=f"T−{mm:02d}:{ss:02d} to next hour")
        # Schedule next tick
        self.root.after(1000, self._update_countdown)

    def run(self):
        try:
            self.root.mainloop()
        finally:
            self._stop_event.set()


def run_live_probability_ui(
    fit_result: dict,
    ranking_metric: str = "bic",
    poll_seconds: float = 5.0,
    history_limit_minutes: int = 180,
    window_title: str = "BTC Hour-Finish Probability",
):
    """
    Convenience function to start the live UI. Blocks until window is closed.
    """
    ui = LiveProbabilityUI(
        fit_result=fit_result,
        ranking_metric=ranking_metric,
        poll_seconds=poll_seconds,
        history_limit_minutes=history_limit_minutes,
        window_title=window_title,
    )
    ui.run()