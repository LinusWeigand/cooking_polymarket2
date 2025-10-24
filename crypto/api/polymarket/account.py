import requests
from dotenv import load_dotenv
import os
import asyncio
import time
import json

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, BalanceAllowanceParams, AssetType, OpenOrderParams, TradeParams

from crypto.api.polymarket.get_event import get_current_event
from crypto.api.polymarket.mod import GAMMA_API_BASE, DATA_API_BASE
from crypto.utils import Asset

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137

# 2 updated per second allowed
# 12 GET per second allowed
def update_allowances(client):
    """Update USDC allowances for the Exchange contract"""
    try:
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=0 # EOA signature type
        )
        result = client.update_balance_allowance(params)
        print(f"Allowances updated successfully: {result}")
        return True
    except Exception as e:
        print(f"Error updating allowances: {e}")
        return False

# 20 requests per second allowed
def cancel_all_orders(client):
    """Cancel all open orders"""
    try:
        resp = client.cancel_all()
        print(f"Canceled {len(resp.get('canceled', []))} orders")
        return resp
    except Exception as e:
        print(f"Error canceling all orders: {e}")
        return None

# Place & Cancel: 240 requests per second allowed
def cancel_order(client, order_id):
    """Cancel a single order by order ID"""
    try:
        resp = client.cancel(order_id)

        if resp.get('canceled'):
            print(f"Successfully canceled order: {order_id}")
            return True
        elif resp.get('not_canceled'):
            reason = resp['not_canceled'].get(order_id, 'Unknown reason')
            print(f"Failed to cancel order {order_id}: {reason}")
            return False

        return False
    except Exception as e:
        print(f"Error canceling order {order_id}: {e}")
        return False

async def get_my_open_orders_async(clob_client, condition_id=None):
    """Async Wrapper: Runs the synchronous get_my_open_orders in a thread."""
    return await asyncio.to_thread(get_my_open_orders, clob_client, condition_id)
# 15 requests per second allowed
def get_my_open_orders(client, condition_id=None, token_id=None):
    """Get all open orders for the user"""
    # start = time.time_ns()
    try:
        params = OpenOrderParams()
        if condition_id:
            params.market = condition_id
        if token_id:
            params.asset_id = token_id

        orders = client.get_orders(params)
        # print(f"Found {len(orders)} open orders")
        # elapsed_ns = time.time_ns() - start
        # print(f"Get Open Orders : {elapsed_ns // 1_000_000} ms")
        return orders
    except Exception as e:
        print(f"Error getting open orders: {e}")
        return []

# Place & Cancel: 240 requests per second allowed
def place_order(client, token_id: str, price: float, size: float, side: str):
    try:
        order_args = OrderArgs(
            price=price,
            size=size,
            side=side.upper(),
            token_id=token_id
        )

        signed_order = client.create_order(order_args)

        resp = client.post_order(signed_order)
        # print(json.dumps(resp, indent=2))
        order_id = resp['orderID']
        return order_id

    except Exception as e:
        print(f"Error placing order: {e}")
        return None

async def get_my_trade_history_async(clob_client, condition_id=None):
    """Async Wrapper: Runs the synchronous get_my_trade_history in a thread."""
    return await asyncio.to_thread(get_my_trade_history, clob_client, condition_id)

def get_my_trade_history(client, condition_id=None):
    """Get trade history for the user, optionally filtered by market"""
    # start = time.time_ns()

    try:
        params = TradeParams()

        if condition_id:
            params.market = condition_id

        trades = client.get_trades(params)
        # elapsed_ns = time.time_ns() - start
        # print(f"Get Trade History : {elapsed_ns // 1_000_000} ms")
        return trades
    except Exception as e:
        print(f"Error getting trade history: {e}")
        return []


def get_client():
    load_dotenv()
    private_key = os.getenv("PRIVATE_KEY")
    polymarket_proxy_address = os.getenv("POLYMARKET_PROXY_ADDRESS")

    if not private_key:
        raise ValueError("PRIVATE_KEY not found in .env file. Please check your setup.")

    if not polymarket_proxy_address:
        raise ValueError("POLYMARKET_PROXY_ADDRESS not found in .env file. Please check your setup.")

    if private_key.startswith("0x"):
        # print("Detected '0x' prefix. Removing it for the signing library.")
        private_key = private_key[2:]

    if len(private_key) != 64:
        raise ValueError(
            f"Invalid private key length. Expected 64 characters, but got {len(private_key)}. "
            "Please ensure you have the correct, full private key."
        )

    client = ClobClient(HOST, key=private_key, chain_id=CHAIN_ID,
                            signature_type=2, funder=polymarket_proxy_address)
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


if __name__ == '__main__':
    client = get_client()
    event = get_current_event(Asset.Bitcoin)
    market = event['markets'][0]
    token_ids = json.loads(market['clobTokenIds'])
    outcomes = json.loads(market['outcomes'])
    token_to_outcome = dict(zip(token_ids, outcomes))
    yes_token_id = next(
        (tid for tid, outcome in token_to_outcome.items() if outcome.lower() == "up" or outcome.lower() == "yes"), None)

    no_token_id = next(
        (tid for tid, outcome in token_to_outcome.items() if outcome.lower() == "down" or outcome.lower() == "no"), None)

    # if not update_allowances(client):
        # print("Failed to update allowances. Exiting.")
        # exit(1)

    # place_order(client, yes_token_id, 0.73, 5, 'BUY')
    # place_order(client, no_token_id, 0.01, 5, 'BUY')

    # print(get_my_open_orders(client, token_id=yes_token_id))
    # print(get_my_open_orders(client, token_id=no_token_id))

    # cancel_all_orders(client)
    # update_allowances(client)

    # order_id = "0x3c0bdea0a0636168e4225975bf94311fef59f9c75cd56d890801ae4b44305978"

    condition_id = market['conditionId']
    # print(get_my_open_positions(client, condition_id=condition_id))
    # print(get_my_trade_history(client, condition_id=condition_id))
    print(get_my_open_orders(client, condition_id=condition_id))