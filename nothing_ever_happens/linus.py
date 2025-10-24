import numpy as np
from scipy.optimize import minimize

BUDGET = 100.0
T = 96

PROBS_ABCD = np.array([0.55, 0.08, 0.14, 0.30])
P_E_MARKET = 0.34


def get_p_e(probs_abcd):
    pA, pB, pC, pD = probs_abcd
    return (1-pA)*(1-pB)*(1-pC)*(1-pD)

P_E = get_p_e(PROBS_ABCD)

# Payout if event index happens
def get_payout_i_happens(index, stakes, t):
    if index < 0 or index >= len(PROBS_ABCD):
        print("get_payout: Index out of bounds")
        return

    # Nothing happens NO bet wins
    # Other events get cashed out at time decay

    result = 0
    result += 1 / (1 - P_E_MARKET) * stakes[-1]

    for i in range(0, len(PROBS_ABCD)):
        if i == index:
            continue
        result += t/T * stakes[i]


    return result

# Payout if nothing happens
def get_payout_nothing_happens(stakes):
    result = 0
    for i in range(0, len(PROBS_ABCD)):
        result += 1 / (1 - PROBS_ABCD[i]) * stakes[i]
    return result

def expected_payoff(stakes):
    result = 0
    result += P_E * get_payout_nothing_happens(stakes)
    for t in range(1, T+1):
        for i in range(0, len(PROBS_ABCD)):
             result += 1 / T * PROBS_ABCD[i] * get_payout_i_happens(i, stakes, t)

    return result 


# --- Optimization Objective ---
def objective_function(x):
    return -expected_payoff(x)

def calculate_all_payoffs(x):
    payoffs = []

    for i in range(len(PROBS_ABCD)):
        # for t in range(1, T+1):
        payoffs.append(get_payout_i_happens(i, x, T))


    payoffs.append(get_payout_nothing_happens(x))

    return np.array(payoffs)


# --- Optimization Setup ---
bounds = [(0, None) for _ in range(5)]
initial_stakes = np.full(5, BUDGET / 5)

constraints = [
    {'type': 'eq', 'fun': lambda x: np.sum(x) - BUDGET},
    {'type': 'ineq', 'fun': lambda x: calculate_all_payoffs(x) - BUDGET}
]

# --- Run Optimization ---
result = minimize(
    objective_function,
    initial_stakes,
    method='SLSQP',
    bounds=bounds,
    constraints=constraints
)

# --- Display Results ---
if result.success:
    optimal_stakes = result.x
    max_ev = -result.fun
    final_payoffs = calculate_all_payoffs(optimal_stakes)
    min_payoff = np.min(final_payoffs)
    guaranteed_profit = min_payoff - BUDGET

    print("✅ Arbitrage Optimization Successful!")
    print("-" * 45)
    print(f"Maximized Expected Payoff: {max_ev:,.2f}")
    print(f"Guaranteed Minimum Payoff: {min_payoff:,.2f}")
    print(f"Guaranteed Minimum Profit: {guaranteed_profit:,.2f} ({(guaranteed_profit/BUDGET):.2%})")
    print(f"Total Investment:          {np.sum(optimal_stakes):,.2f}")
    print("-" * 45)
    print("Optimal Stakes to Guarantee Profit:")
    for i, stake in enumerate(optimal_stakes):
        print(f"  - Stake {chr(97+i).upper()}: {stake:22,.2f}")
    print("-" * 45)
    print("Payoff for Each Scenario:")
    print(f"  - Scenario A only:     {final_payoffs[0]:15,.2f}")
    print(f"  - Scenario B only:     {final_payoffs[1]:15,.2f}")
    print(f"  - Scenario C only:     {final_payoffs[2]:15,.2f}")
    print(f"  - Scenario D only:     {final_payoffs[3]:15,.2f}")
    print(f"  - Nothing Happens (E): {final_payoffs[4]:15,.2f}")
else:
    print("❌ Optimization Failed.")
    print("   Reason:", result.message)
