import httpx
import requests
import json
import time

from crypto.api.polymarket.get_event import get_up_or_down_event, get_current_event
from crypto.api.polymarket.mod import CLOB_API_BASE
from crypto.utils import Asset

def get_order_book_with_token_ids(event):
    market = event["markets"][0]
    token_ids = json.loads(market["clobTokenIds"])
    outcomes = json.loads(market["outcomes"])
    token_to_outcome = dict(zip(token_ids, outcomes))
    yes_token_id = next(
        (tid for tid, outcome in token_to_outcome.items() if outcome.lower() == "up" or outcome.lower() == "yes"), None)

    no_token_id = next(
        (tid for tid, outcome in token_to_outcome.items() if outcome.lower() == "down" or outcome.lower() == "no"), None)

    url = f"{CLOB_API_BASE}/books"
    payload = {"token_id": yes_token_id},
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Referer": "https://polymarket.com/"
    }
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    payload = resp.json()
    yes_order_book = payload[0]

    yes_order_book['bids'].sort(key=lambda x: float(x['price']), reverse=True)
    yes_order_book['asks'].sort(key=lambda x: float(x['price']))
    return yes_order_book, yes_token_id, no_token_id

async def get_order_book_with_token_ids_async(http_client: httpx.AsyncClient, event):
    # start = time.time_ns()
    market = event["markets"][0]
    token_ids = json.loads(market["clobTokenIds"])
    outcomes = json.loads(market["outcomes"])
    token_to_outcome = dict(zip(token_ids, outcomes))
    yes_token_id = next(
    (tid for tid, outcome in token_to_outcome.items() if outcome.lower() == "up" or outcome.lower() == "yes"), None)

    no_token_id = next(
        (tid for tid, outcome in token_to_outcome.items() if outcome.lower() == "down" or outcome.lower() == "no"), None)

    url = f"{CLOB_API_BASE}/books"
    payload = {"token_id": yes_token_id},

    headers = {"Content-Type": "application/json"}
    try:
        resp = await http_client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        yes_order_book = resp.json()[0]
        yes_order_book['bids'].sort(key=lambda x: float(x['price']), reverse=True)
        yes_order_book['asks'].sort(key=lambda x: float(x['price']))
        # elapsed_ns = time.time_ns() - start
        # print(f"Get Orderbook : {elapsed_ns // 1_000_000} ms")
        return yes_order_book, yes_token_id, no_token_id
    except httpx.HTTPError as e:
        print(f"Error fetching order book: {e}")
        return None, None, None

if __name__ == "__main__":
    event = get_current_event(Asset.Bitcoin)
    yes_order_book, _, _ = get_order_book_with_token_ids(event)

    # print("Asks:", yes_order_book["asks"])
    # print("Bids:", yes_order_book["bids"])

