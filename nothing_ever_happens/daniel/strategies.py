"""
Strategy helpers for constructing equalized payout portfolios across multi-outcome markets.

This module extends the N-way "No" equalization to include an extra parlay-style event
E := not(A) AND not(B) AND ... ("none-of-these"). Given the Yes price for E and the
Yes prices for A, B, C, ... it computes a portfolio that equalizes payout across all
N+1 mutually exclusive states (each candidate i wins, or "none-of-these").

IMPORTANT:
- To truly equalize payouts across all states using only No(A_i) and Yes(E), you must
  be able to SHORT the Yes(E) token. The solution requires buying s shares of each No(A_i)
  and shorting s shares of Yes(E).
- If you cannot short, you can still use the outputs for analysis, arbitrage detection,
  and sizing relative to a target payout, but the equalization across the 'none' state
  won't hold with strictly long-only positions.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import math


def _validate_probs(xs: List[float]) -> List[float]:
    out = []
    for x in xs:
        try:
            fx = float(x)
        except Exception:
            fx = float('nan')
        # clip into [0,1] if slightly off due to rounding
        if math.isnan(fx):
            out.append(float('nan'))
        else:
            out.append(max(0.0, min(1.0, fx)))
    return out


def equalize_no_with_parlay(
    yes_prices: List[float],
    parlay_yes_price: float,
    *,
    budget: Optional[float] = None,
    target_payout: Optional[float] = None,
    outcome_names: Optional[List[str]] = None,
    use_parlay_no: bool = True,
) -> Dict[str, Any]:
    """Equalize payout across N candidates plus a parlay 'none-of-these' event.

    Instruments used (two equivalent constructions):
      A) Long-only (default):
         - Buy s shares of No(A_i) for each candidate i
         - Buy s shares of No(E), where E = not(A1) AND ... AND not(An)
         This requires a market where No(E) is available to buy at price (1 - e).
      B) With shorting (set use_parlay_no=False):
         - Buy s shares of No(A_i) for each candidate i
         - Short s shares of Yes(E)

    State payouts with this portfolio:
      - With long-only construction (A):
          If candidate i wins: (N-1)*s from No(A_j) + s from No(E) = N*s
          If none-of-these occurs: N*s from No(A_i) + 0 from No(E) = N*s
        So all states pay N*s.
      - With shorting construction (B):
          If candidate i wins: (N-1)*s + (-0) = (N-1)*s
          If none-of-these occurs:  N*s + (-s)  = (N-1)*s

    Costs (ignoring fees):
      - Long-only (A):  total spend = s * (sum_no + (1 - e))
      - Shorting  (B):  total spend = s * (sum_no - e)

    Arbitrage per-share margin:
      margin_per_share = (N-1) - (sum_no - e) = (N-1) + e - sum_no
      If margin_per_share > 0, the equalized portfolio has positive profit across all states.

    Sizing rules:
      - Long-only (A):
          If target_payout is provided: s = target_payout / N
          Else if budget is provided: s = budget / (sum_no + (1 - e))
      - Shorting (B):
          If target_payout is provided: s = target_payout / (N-1)
          Else if budget is provided: s = budget / (sum_no - e)
      - Else: s = 1.0 (unit sizing)

    Returns a dict matching the style of equalize_n_way_no, with additional keys for the parlay leg.
    """
    if outcome_names is not None and len(outcome_names) != len(yes_prices):
        raise ValueError("outcome_names length must match yes_prices length")

    p_yes = _validate_probs(list(yes_prices))
    e = float(parlay_yes_price)
    e = max(0.0, min(1.0, e))
    N = len(p_yes)
    if N < 2:
        raise ValueError("Need at least 2 outcomes to perform N-way equalization with parlay.")

    # Convert to No prices
    q_no = [1.0 - p for p in p_yes]
    sum_no = sum(q_no)

    margin_per_share = (N - 1) + e - sum_no
    arbitrage = margin_per_share > 0

    # Determine share size s
    s = None
    if target_payout is not None:
        if N - 1 == 0:
            raise ValueError("N-1 is zero; cannot target payout with a single outcome.")
        s = float(target_payout) / (N - 1)
    elif budget is not None:
        denom = (sum_no - e)
        if denom == 0:
            raise ValueError("Budget-based sizing undefined because (sum_no - parlay_price) == 0")
        s = float(budget) / denom
    else:
        s = 1.0

    # Build portfolio and economics depending on construction
    shares_no = [0.0 for _ in range(N)]
    share_parlay_yes = 0.0
    share_parlay_no = 0.0

    if use_parlay_no:
        # Long-only construction (buy No on each candidate AND No on the parlay)
        if target_payout is not None:
            s = float(target_payout) / N
        elif budget is not None:
            denom = (sum_no + (1.0 - e))
            if denom == 0:
                raise ValueError("Budget-based sizing undefined because (sum_no + (1 - e)) == 0")
            s = float(budget) / denom
        else:
            s = 1.0

        shares_no = [s for _ in range(N)]
        share_parlay_no = s

        spend_no = [s * q for q in q_no]
        spend_parlay_no = s * (1.0 - e)
        spend_parlay_yes = 0.0

        total_spend = sum(spend_no) + spend_parlay_no
        payout_equalized = N * s
        profit_equalized = payout_equalized - total_spend
        roi = (profit_equalized / total_spend) if total_spend != 0 else float('inf')
    else:
        # Shorting construction (buy No on candidates, SHORT Yes on parlay)
        if target_payout is not None:
            s = float(target_payout) / (N - 1)
        elif budget is not None:
            denom = (sum_no - e)
            if denom == 0:
                raise ValueError("Budget-based sizing undefined because (sum_no - parlay_price) == 0")
            s = float(budget) / denom
        else:
            s = 1.0

        shares_no = [s for _ in range(N)]
        share_parlay_yes = -s

        spend_no = [s * q for q in q_no]
        spend_parlay_yes = share_parlay_yes * e  # negative if s>0 (short proceeds)
        spend_parlay_no = 0.0

        total_spend = sum(spend_no) + spend_parlay_yes
        payout_equalized = (N - 1) * s
        profit_equalized = payout_equalized - total_spend
        roi = (profit_equalized / total_spend) if total_spend != 0 else float('inf')

    # Build state payouts (all equal to payout_equalized)
    names = outcome_names or [f"O{i+1}" for i in range(N)]
    state_payouts = {name: payout_equalized for name in names}
    state_payouts["none_of_these"] = payout_equalized

    # Compose result dict similar to equalize_n_way_no
    result: Dict[str, Any] = {
        "n": N,
        "outcome_names": names,
        "yes_prices": p_yes,
        "no_prices": q_no,
        "parlay_yes_price": e,
        "shares_no_each": s,
        "shares_no_by_outcome": {name: s for name in names},
        "share_parlay_yes": share_parlay_yes,
        "share_parlay_no": share_parlay_no,
        "spend_no_by_outcome": {name: cost for name, cost in zip(names, spend_no)},
        "spend_parlay_yes": (spend_parlay_yes if not use_parlay_no else 0.0),
        "spend_parlay_no": (spend_parlay_no if use_parlay_no else 0.0),
        "sum_no_prices": sum_no,
        "arbitrage_margin_per_share": margin_per_share,
        "arbitrage": arbitrage,
        "total_spend": total_spend,
        "payout": payout_equalized,
        "guaranteed_payout": payout_equalized,
        "guaranteed_profit": profit_equalized,
        "roi": roi,
        "state_payouts": state_payouts,
        "construction": ("long_no_parlay" if use_parlay_no else "short_parlay_yes"),
        "notes": (
            "Long-only construction buys No on each candidate and No on the parlay at price (1 - e), giving equal payout N*s. "
            "Alternatively, shorting Yes(E) by s (if available) yields equal payout (N-1)*s."
        ),
    }

    return result


def equalize_n_way_no_with_parlay(
    yes_prices: List[float],
    parlay_yes_price: float,
    *,
    budget: Optional[float] = None,
    target_payout: Optional[float] = None,
    names: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Return the SAME dict structure as `equalize_n_way_no`, but also accept a parlay 'none-of-these' YES price.

    This function does NOT short the parlay leg. It uses the parlay price to compute an enhanced
    arbitrage indicator, while the portfolio is the standard equal-each 'No' across the N candidates.

    Inputs:
      - yes_prices: list of N YES prices for candidates A,B,C,... (0..1)
      - parlay_yes_price: price of E = not(A) AND not(B) AND ... (0..1)
      - Provide exactly one of budget OR target_payout (same semantics as equalize_n_way_no)
      - names: optional list of candidate names (length N)

    Returns: dict with keys identical to `equalize_n_way_no`:
      { 'N', 'shares_each', 'no_price_sum', 'spend_per_contract', 'total_spend',
        'payout_if_any_wins', 'guaranteed_pnl', 'roi', 'is_arbitrage' }
    and includes extra informational keys:
      - 'parlay_yes_price'
      - 'is_arbitrage_with_parlay'  (True if (N-1)+e - sum_no > 0)
      - 'parlay_arbitrage_margin_per_share' = (N-1)+e - sum_no
    """
    if (budget is None) == (target_payout is None):
        raise ValueError("Specify exactly one of: budget OR target_payout.")

    p_yes = _validate_probs(list(yes_prices))
    N = len(p_yes)
    if N < 2:
        raise ValueError("Provide at least 2 outcomes.")

    e = float(parlay_yes_price)
    e = max(0.0, min(1.0, e))

    # Convert to No prices
    q_no = [1.0 - p for p in p_yes]
    S = sum(q_no)  # sum of No prices
    if S <= 0:
        raise ValueError("Sum of implied 'No' prices must be > 0.")

    # Equal-each No shares to equalize winner states
    if target_payout is None:
        shares_each = float(budget) / S
        total_spend = float(budget)
        payout_if_winner = (N - 1) * shares_each
    else:
        shares_each = float(target_payout) / (N - 1)
        total_spend = shares_each * S
        payout_if_winner = float(target_payout)

    # Per-contract spends (candidates only)
    if names is None:
        names = [f"Candidate {i+1}" for i in range(N)]
    if len(names) != N:
        raise ValueError("Length of names must match number of prices.")

    spend_rows = []
    for name, q in zip(names, q_no):
        spend_rows.append({"candidate": name, "no_price": q, "spend": shares_each * q})

    pnl_when_any_wins = payout_if_winner - total_spend
    roi = pnl_when_any_wins / total_spend if total_spend != 0 else float("nan")

    # Base arbitrage (no-only): S < N-1
    is_arb_no_only = S < (N - 1) - 1e-12

    # Enhanced arbitrage if you could also short the parlay: margin = (N-1) + e - S
    parlay_margin = (N - 1) + e - S
    is_arb_with_parlay = parlay_margin > 0

    result: Dict[str, Any] = {
        "N": N,
        "shares_each": shares_each,
        "no_price_sum": S,
        "spend_per_contract": spend_rows,
        "total_spend": total_spend,
        "payout_if_any_wins": payout_if_winner,
        "guaranteed_pnl": pnl_when_any_wins,
        "roi": roi,
        "is_arbitrage": is_arb_no_only,
        # extras:
        "parlay_yes_price": e,
        "is_arbitrage_with_parlay": is_arb_with_parlay,
        "parlay_arbitrage_margin_per_share": parlay_margin,
    }

    return result
