import asyncio
from time import sleep
import httpx
from crypto.api.binance import get_latest_bitcoin_price, get_bitcoin_1h_open_price, get_latest_bitcoin_price_async
from crypto.api.polymarket.account import cancel_order, place_order, get_client, \
    get_my_trade_history_async, get_my_open_orders_async, update_allowances
from crypto.api.polymarket.get_event import get_current_event
from crypto.api.polymarket.get_orderbook import get_order_book_with_token_ids_async
from crypto.candle_manager import CandleManager
import time
import math
import threading
import pandas as pd
import os
import sys
import datetime

from crypto.tests.simulate_event import get_mock_data, get_mock_p_fair
from crypto.utils import Asset, get_next_hour_timestamp
try:
    import garch_monte_carlo
except ImportError:
    print("Error: Could not import the 'garch_monte_carlo' module.", file=sys.stderr)
    sys.exit(1)

FILENAME = "../data/btc_1m_log_returns.csv"

def to_size(x):
    return max(round(float(x), 2), 0.)


def is_matching(a, b):
    return a['type'] == b['type'] and a['side'] == b['side'] and math.isclose(a['price'], b['price'])


def less_than(a, b):
    return a <= b + 1e-6


# If previous Limit Order (Pending Order) was executed
# We assume Limit Orders are executed if better than the best offer
def was_executed(order_book, limit_order):
    bids = order_book['bids']
    asks = order_book['asks']
    best_bid_price = float(bids[0]['price']) if len(bids) > 0 else 0.
    best_ask_price = float(asks[0]['price']) if len(asks) > 0 else 1.
    o = limit_order
    if o['type'] == 'YES':
        if o['side'] == 'BUY':
            price_matched = math.isclose(best_bid_price, o['price'])
            return not price_matched and best_bid_price < o['price']
        elif o['side'] == 'SELL':
            price_matched = math.isclose(best_ask_price, o['price'])
            return not price_matched and best_ask_price > o['price']
    elif o['type'] == 'NO':
        if o['side'] == 'BUY':
            best_bid_price_no = 1. - best_ask_price
            price_matched = math.isclose(best_bid_price_no, o['price'])
            return not price_matched and best_bid_price_no < o['price']
        elif o['side'] == 'SELL':
            best_ask_price_no = 1. - best_bid_price
            price_matched = math.isclose(best_ask_price_no, o['price'])
            return not price_matched and best_ask_price_no > o['price']
    return False


# If order matches an opposing order in the order_book and will be executed immediately
def order_matches_order_book(order, order_book):
    bids = order_book['bids']
    asks = order_book['asks']
    best_bid_price = float(bids[0]['price']) if len(bids) > 0 else 0.
    best_ask_price = float(asks[0]['price']) if len(asks) > 0 else 1.
    o = order
    if o['type'] == 'YES':
        if o['side'] == 'BUY':
            is_matching = math.isclose(best_ask_price, o['price'])
            return is_matching or o['price'] > best_ask_price
        elif o['side'] == 'SELL':
            is_matching = math.isclose(best_bid_price, o['price'])
            return is_matching or o['price'] < best_bid_price
    elif o['type'] == 'NO':
        if o['side'] == 'BUY':
            best_ask_price_no = 1. - best_bid_price
            is_matching = math.isclose(best_ask_price_no, o['price'])
            return is_matching or o['price'] > best_ask_price_no
        elif o['side'] == 'SELL':
            best_bid_price_no = 1. - best_ask_price
            is_matching = math.isclose(best_bid_price_no, o['price'])
            return is_matching or o['price'] < best_bid_price_no

    return False

def get_market_sell_value(position, order_book):
    bids = order_book['bids']
    asks = order_book['asks']
    p = position
    size = p['size']
    value = 0.
    if p['type'] == 'YES':
        for bid in bids:
            price = float(bid['price'])
            size_matched = min(size, float(bid['size']))
            value += size_matched * price
            size -= size_matched
            if size < 0.01:
                return value
    elif p['type'] == 'NO':
        for ask in asks:
            price = 1. - float(ask['price'])
            size_matched = min(size, float(ask['size']))
            value += size_matched * price
            size -= size_matched
            if size < 0.01:
                return value
    return value

class MarketMakerBot:
    def __init__(self, config, asset: Asset, get_open_price, get_latest_price, get_current_event, get_close_timestamp):
        self.yes_shares = 0.
        self.no_shares = 0.
        self.longs = 0.
        self.shorts = 0.
        self.pending_longs = 0.
        self.pending_shorts = 0.
        self.pending_orders = []
        self.pending_trades = []
        self.config = config

        self.get_latest_price = get_latest_price
        self.get_open_price = get_open_price
        self.get_current_event = get_current_event
        self.get_close_timestamp = get_close_timestamp

        self.close_timestamp = self.get_close_timestamp()
        self.open_price = self.get_open_price()
        self.event = self.get_current_event(asset)
        self.asset = asset
        self.tick_size = 0.01

        self.file_lock = threading.Lock()
        self.candle_manager = CandleManager(self.file_lock, self.read_returns())
        self.returns = None

        self.client = get_client()
        self.address = os.getenv("POLYMARKET_PROXY_ADDRESS")

        self.cash = config['PORTFOLIO_SIZE']
        self.min_order_size = None
        self.p_fair = None

        self.logs = []


    def run(self):
        self.read_returns()

        if not update_allowances(self.client):
           print("Failed to update allowances. Exiting.")
           exit(1)

        # self.candle_manager.start()
        # print(self.event)

        while True:
            print("_" * 60)

            secs_left = self.close_timestamp - time.time()
            if secs_left <= 0:
                self.switch_events()
                continue
            mins = secs_left // 60
            secs = secs_left % 60
            # print(f"{mins:.0f} Minuten und {secs:.0f} Sekunden verbleibend")

            # 440ms
            # fetched_data = get_mock_data()
            fetched_data = asyncio.run(self.fetch_market_data())
            current_btc_price = fetched_data[0]
            order_book, yes_token_id, no_token_id = fetched_data[1]
            my_trade_history = fetched_data[2]
            my_open_orders = fetched_data[3]

            # 70ms
            self.p_fair = garch_monte_carlo.calculate_probability_plain(
                returns=self.returns,
                current_price=current_btc_price,
                target_price=self.open_price,
                horizon_minutes=max(1, round(mins + secs / 60)),
                num_simulations=self.config['NUM_SIMULATIONS'],
            )
            # self.p_fair = get_mock_p_fair()
            print(f"p_fair: {self.p_fair}")

            self.min_order_size = float(order_book['min_order_size'])
            self.tick_size = float(order_book['tick_size'])

            # Inventory
            # my_trades = self.get_my_trades(my_trade_history)
            # self.update_inventory_trades(my_trades)

            # self.add_new_orders_to_pending_trades(my_open_orders)

            self.update_pending_orders(order_book)

            order_plan = self.get_order_plan(order_book)

            order_plan = self.reduce_order_plan_size_based_on_pending_orders(order_plan)

            # self.remove_pending_orders_from_orders(my_open_orders)

            # Keep matching orders, reduce current orders, cancel all other
            # orders_to_cancel = self.remove_order_plan_from_open_orders(order_plan, my_open_orders)

            # Cancel orders
            # for o in orders_to_cancel:
            # cancel_order(self.client, o['id'])

            # Filter out orders
            # order_plan = [o for o in order_plan if o['size'] >= self.min_order_size]

            # Execute orders
            # self.execute_orders(order_plan, yes_token_id, no_token_id)
            self.simulate_execute_orders(order_plan, order_book)

            print(f"Pending: {self.pending_orders}")
            self.print_positions(order_book)


            position_value = self.get_position_value(order_book)
            print(f"PnL: ${(self.cash + position_value - self.config['PORTFOLIO_SIZE']):.2f}")
            time.sleep(self.config['LOOP_DELAY_SECS'])

    def print_positions(self, order_book):
        yes_value = 0.
        no_value = 0.
        if self.yes_shares >= self.min_order_size:
            order = {'type': 'YES', 'side': 'SELL', 'size': self.yes_shares}
            yes_value += get_market_sell_value(order, order_book)
        if self.no_shares >= self.min_order_size:
            order = {'type': 'NO', 'side': 'SELL', 'size': self.no_shares}
            no_value += get_market_sell_value(order, order_book)
        print(f"YES: ${yes_value:.2f}, NO: ${no_value:.2f}")


    def get_position_value(self, order_book):
        value = 0.
        if self.yes_shares >= self.min_order_size:
            position = {'type': 'YES', 'size': self.yes_shares}
            value += get_market_sell_value(position, order_book)
        if self.no_shares >= self.min_order_size:
            position = {'type': 'NO', 'size': self.no_shares}
            value += get_market_sell_value(position, order_book)
        return value


    def update_inventory(self, executed_order):
        o = executed_order
        value = o['size'] * o['price']
        print(f"Executed: BUY {o['size']} NO for ${o['price']:.2} (${value:.2})")
        if o['type'] == 'YES':
            if o['side'] == 'BUY':
                self.yes_shares += o['size']
                self.longs += value
                self.cash -= value
            elif o['side'] == 'SELL':
                self.yes_shares -= o['size']
                self.longs -= value
                self.cash += value
        elif o['type'] == 'NO':
            if o['side'] == 'BUY':
                self.no_shares += o['size']
                self.shorts += value
                self.cash -= value
            elif o['side'] == 'SELL':
                self.no_shares -= o['size']
                self.shorts -= value
                self.cash += value

    def update_pending_inventory(self, pending_order):
        o = pending_order
        value = o['size'] * o['price']
        if o['type'] == 'YES':
            if o['side'] == 'BUY':
                self.pending_longs += value
            elif o['side'] == 'SELL':
                self.pending_longs -= value
        elif o['type'] == 'NO':
            if o['side'] == 'BUY':
                self.pending_shorts += value
            elif o['side'] == 'SELL':
                self.pending_shorts -= value

    def print_inventory(self):
        print("-" * 20)
        print(f"Longs: ${self.longs:.2}, Shorts: ${self.shorts:.2}")
        print(f"PnL: ${(self.cash + self.longs + self.shorts - self.config['PORTFOLIO_SIZE']):.2}")

    def add_order_to_logs(self, order, order_book):
        o = order
        bids = order_book['bids']
        asks = order_book['asks']
        best_bid_price = float(bids[0]['price']) if len(bids) > 0 else 0.
        best_ask_price = float(asks[0]['price']) if len(asks) > 0 else 1.
        self.logs.append({
            'time': time.time(),
            'type': o['type'],
            'side': o['side'],
            'size': o['size'],
            'price': o['price'],
            'best_bid': best_bid_price,
            'best_ask': best_ask_price,
            'p_fair': self.p_fair
        })

    def print_logs(self):
        print("#" * 100)

        for o in self.logs:
            dt_object = datetime.datetime.fromtimestamp(o['time'])
            time_string = dt_object.strftime("%H:%M:%S")
            print(f"{time_string} {o['side']} {o['type']} @ ${o['price']:.2f} | Best Bid: ${o['best_bid']:.2f}, Best Ask: ${o['best_ask']:.2f}, P_fair: {o['p_fair']}")

        print("#" * 100)

    def update_pending_orders(self, order_book):
        new_pending_orders = []
        self.pending_longs = 0.
        self.pending_shorts = 0.

        for o in self.pending_orders:
            if was_executed(order_book, o):
                print("LIMIT")
                self.update_inventory(o)
                self.add_order_to_logs(o, order_book)
                self.print_logs()
            else:
                new_pending_orders.append(o)
                self.update_pending_inventory(o)

        self.pending_orders = new_pending_orders


    async def fetch_market_data(self, max_retries=5, initial_delay=1.0, backoff_factor=2.0):
        market_condition_id = self.event['markets'][0]['conditionId']
        async with httpx.AsyncClient() as http_client:
            for attempt in range(max_retries):
                try:
                    tasks = [
                        get_latest_bitcoin_price_async(http_client),
                        get_order_book_with_token_ids_async(http_client, self.event),
                        get_my_trade_history_async(self.client, condition_id=market_condition_id),
                        get_my_open_orders_async(self.client, condition_id=market_condition_id),
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    errors = [res for res in results if isinstance(res, Exception)]
                    if not errors:
                        return results
                    if attempt >= max_retries - 1:
                        raise errors[0]
                except Exception as e:
                    if attempt >= max_retries - 1:
                        raise e

                delay = (initial_delay * (backoff_factor ** attempt))
                await asyncio.sleep(delay)

        return results

    def switch_events(self):
        print("switching events")
        sleep(60)
        self.close_timestamp = self.get_close_timestamp()
        self.open_price = self.get_open_price()
        self.event = self.get_current_event(self.asset)
        self.yes_shares = 0.
        self.no_shares = 0.
        print("event gewechselt")

    def read_returns(self):
        with self.file_lock:
            self.returns = pd.read_csv(FILENAME)["log_return"].dropna().tolist()

    def get_my_best_bid_ask(self, best_bid_price, best_ask_price):
        a = 1 / self.tick_size

        my_best_bid_price = self.p_fair - self.config['RISK_THRESHOLD']
        no_bid = False
        if my_best_bid_price < 0:
            my_best_bid_price = 0
            no_bid = True
            print("no bid")
        else:
            my_best_bid_price = self.clamp_price(math.floor(my_best_bid_price * a) / a)
            my_best_bid_price = self.to_price(min(my_best_bid_price, best_bid_price + self.tick_size))

        my_best_ask_price = self.p_fair + self.config['RISK_THRESHOLD']
        no_ask = False
        if my_best_ask_price > 1:
            my_best_ask_price = 1
            no_ask = True
            print("no ask")
        else:
            my_best_ask_price = self.clamp_price(math.ceil(my_best_ask_price * a) / a)
            my_best_ask_price = self.to_price(max(my_best_ask_price, best_ask_price - self.tick_size))

        return my_best_bid_price, my_best_ask_price, no_bid, no_ask

    def get_snipe_mode(self, best_bid_price, best_ask_price):
        sniping_bid = False
        sniping_ask = False
        if best_bid_price - self.config['RISK_THRESHOLD'] > self.p_fair:
            sniping_bid = True

        if best_ask_price + self.config['RISK_THRESHOLD'] < self.p_fair:
            sniping_ask = True

        if sniping_bid and sniping_ask:
            print("That should not happen: sniping_bids == sniping_asks == True")

        return sniping_bid, sniping_ask

    def get_my_trades(self, my_trade_history):
        my_trades = []
        for taker_trade in my_trade_history:
            if taker_trade['maker_address'] == self.address:
                outcome = taker_trade['outcome']
                my_trades.append({
                    'order_id': taker_trade['taker_order_id'],
                    'size': to_size(taker_trade['size']),
                    'price': self.to_price(taker_trade['price']),
                    'side': taker_trade['side'],
                    'type': 'YES' if outcome == 'Up' else 'NO'
                })
            else:
                for maker_trade in taker_trade['maker_orders']:
                    if maker_trade['maker_address'] == self.address:
                        outcome = maker_trade['outcome']
                        my_trades.append({
                            'order_id': maker_trade['order_id'],
                            'size': to_size(maker_trade['matched_amount']),
                            'price': self.to_price(maker_trade['price']),
                            'side': maker_trade['side'],
                            'type': 'YES' if outcome == 'Up' else 'NO'
                        })
        return my_trades

    def update_inventory_trades(self, my_trades):
        for trade in my_trades:
            side = trade['side']
            type = trade['type']
            size = trade['size']
            trade_value = size * trade['price']
            print(f"Trades: {trade}")
            if type == 'YES':
                if side == 'BUY':
                    self.yes_shares += size
                    self.longs += trade_value
                else:
                    self.yes_shares -= size
                    self.longs -= trade_value
            else:
                if side == 'BUY':
                    self.no_shares += size
                    self.shorts += trade_value
                else:
                    self.no_shares -= size
                    self.shorts -= trade_value

    def add_new_orders_to_pending_trades(self, my_open_orders):
        pending_trade_ids = [t['order_id'] for t in self.pending_trades]
        new_orders = [o for o in my_open_orders if o['id'] not in pending_trade_ids]
        for order in new_orders:
            outcome = order['outcome']
            self.pending_trades.append({
                'order_id': order['id'],
                'size': to_size(order['original_size']),
                'price': self.to_price(order['price']),
                'side': order['side'],
                'type': 'YES' if outcome == 'Up' else 'NO'
            })

    def remove_pending_trades_from_trades(self, my_trades):
        pending_trade_ids_to_remove = []
        for trade in self.pending_trades:
            matching_trades = [t for t in my_trades if t['order_id'] == trade['order_id']]

            size = trade['size']
            for t in matching_trades:
                trade['size'] = to_size(size - t['size'])

            if size < 0.1:
                pending_trade_ids_to_remove.append(trade['order_id'])
                continue

        self.pending_trades = [t for t in self.pending_trades if t['order_id'] not in pending_trade_ids_to_remove]

    def remove_pending_orders_from_orders(self, my_open_orders):
        open_order_ids = [o['id'] for o in my_open_orders]
        self.pending_orders = [o for o in self.pending_orders if o['id'] not in open_order_ids]

    def remove_order_plan_from_open_orders(self, order_plan, my_open_orders):
        matching_open_order_ids = []
        for order in order_plan:
            value = order['price'] * order['size']
            print(
                f"price: {order['price']}, size: {order['size']}, side: {order['side']}, type: {order['type']}, value: {value:.2f}")

            # Reduce size from orders which already are in open_orders
            matching_open_orders = [o for o in my_open_orders
                                    if math.isclose(self.to_price(o['price']), order['price'])
                                    and o['side'] == order['side']
                                    and order['type'] == ('YES' if o['outcome'] == 'Up' else 'NO')]

            for o in matching_open_orders:
                matching_open_order_ids.append(o['id'])
                size_reduction = float(o['original_size']) - float(o['size_matched'])
                print(f"Reducing Size (Open Order): {size_reduction}")
                order['size'] = to_size(order['size'] - size_reduction)

            # Reduce size from orders which already are in pending orders
            matching_pending_orders = [o for o in self.pending_orders
                                       if math.isclose(self.to_price(o['price']), order['price'])
                                       and o['side'] == order['side']
                                       and order['type'] == o['type']]

            for o in matching_pending_orders:
                print(f"Reducing Size (Pending Order): {o['size']}")
                order['size'] = to_size(order['size'] - o['size'])

        orders_to_cancel = [o for o in my_open_orders if o['id'] not in matching_open_order_ids]
        return orders_to_cancel

    def get_order_plan(self, order_book):
        bids = order_book['bids']
        asks = order_book['asks']
        best_bid = bids[0] if len(bids) > 0 else None
        best_ask = asks[0] if len(asks) > 0 else None
        best_bid_price = self.to_price(best_bid['price']) if not best_bid is None else 0.
        best_ask_price = self.to_price(best_ask['price']) if not best_ask is None else 1.
        best_bid_size = to_size(best_bid['size']) if not best_bid is None else 0.
        best_ask_size = to_size(best_ask['size']) if not best_ask is None else 0.

        print(f"Best Bid: {best_bid_price}, Best Ask: {best_ask_price}")

        (my_best_bid_price,
         my_best_ask_price,
         no_bid, no_ask) = self.get_my_best_bid_ask(best_bid_price, best_ask_price)

        if no_bid:
            print("No bid")
        if no_ask:
            print("No ask")
        print(f"My Best Bid: {my_best_bid_price}, My Best Ask: {my_best_ask_price}")

        sniping_bid, sniping_ask = self.get_snipe_mode(best_bid_price, best_ask_price)

        # if sniping_bid:
            # print("Sniping Bid")
        # if sniping_ask:
            # print("Sniping Ask")

        max_inventory = self.config['MAX_INVENTORY']
        long_inventory_left = max_inventory - self.longs
        short_inventory_left = max_inventory - self.shorts
        order_plan = []

        # Sell old positions
        if self.yes_shares >= self.min_order_size:
            price = my_best_ask_price
            size = to_size(self.yes_shares)
            if sniping_bid:
                price = best_bid_price
                size = min(best_bid_size, size)
            order_plan.append({
                'price': price,
                'size': size,
                'side': 'SELL',
                'type': 'YES'
            })
        if self.no_shares >= self.min_order_size:
            price = self.to_price(1. - my_best_bid_price)
            size = to_size(self.no_shares)
            if sniping_ask:
                price = self.to_price(1. - best_ask_price)
                size = min(best_ask_size, size)
            order_plan.append({
                'price': price,
                'size': size,
                'side': 'SELL',
                'type': 'NO'
            })

        # Open new positions
        if long_inventory_left >= 1. and not no_bid:
            size = to_size(long_inventory_left / my_best_bid_price)
            price = my_best_bid_price
            if sniping_ask:
                size = to_size(long_inventory_left / best_ask_price)
                size = min(size, best_ask_size)
                price = best_ask_price
            order_plan.append({
                'price': price,
                'size': size,
                'side': 'BUY',
                'type': 'YES'
            })
        if short_inventory_left >= 1. and not no_ask:
            price = self.to_price(1. - my_best_ask_price)
            size = to_size(short_inventory_left / price)
            if sniping_bid:
                price = self.to_price(1. - best_bid_price)
                size = to_size(short_inventory_left / price)
                size = min(size, best_bid_size)
            order_plan.append({
                'price': price,
                'size': size,
                'side': 'BUY',
                'type': 'NO'
            })

        return order_plan

    def execute_orders(self, order_plan, yes_token_id, no_token_id):
        for o in order_plan:
            order_id = place_order(
                self.client,
                yes_token_id if o['type'] == 'YES' else no_token_id,
                o['price'],
                o['size'],
                o['side']
            )
            if not order_id:
                continue

            self.pending_orders.append({
                'id': order_id,
                'size': o['size'],
                'price': o['price'],
                'side': o['side'],
                'type': o['type']
            })
            print(
                f"Placed: {o['side']} {o['size']} {o['type']} shares for ${o['price']} (${(o['price'] * o['size']):.2f})")
            order_value = to_size(o['size']) * self.to_price(o['price'])

    def simulate_execute_orders(self, order_plan, order_book):
        order_plan.extend(self.pending_orders)
        new_pending_orders = []
        self.pending_longs = 0.
        self.pending_shorts = 0.

        for o in order_plan:
            if order_matches_order_book(o, order_book):
                print("Market")
                self.update_inventory(o)
                self.add_order_to_logs(o, order_book)
                self.print_logs()
            else:
                new_pending_orders.append(o)
                self.update_pending_inventory(o)

        self.pending_orders = new_pending_orders

    # Reduce current orders size by matching open orders size
    # if is matching and p_size > o_size: cancel pending order
    def reduce_order_plan_size_based_on_pending_orders(self, order_plan):
        self.pending_longs = 0.
        self.pending_shorts = 0.
        new_pending_orders = []
        for p in self.pending_orders:
            for o in order_plan:
                if is_matching(o, p) and less_than(p['size'], o['size']):
                    o['size'] -= p['size']
                    new_pending_orders.append(p)
                    self.update_pending_inventory(o)

        order_plan = [o for o in order_plan if o['size'] >= self.min_order_size]

        self.pending_orders = new_pending_orders
        return order_plan


    def clamp_price(self, x):
        low = self.tick_size
        high = 1 - self.tick_size
        return max(low, min(x, high))

    def to_price(self, x):
        a = 1 / self.tick_size
        return self.clamp_price(round(float(x) * a) / a)


if __name__ == "__main__":
    BOT_CONFIG = {
        "PORTFOLIO_SIZE": 10.,
        "MAX_POSITION_PERCENT": 0.5, # Dispute window is 1-2 hours
        "RISK_THRESHOLD": 0.005, # 0.5 %
        "LIMIT_ORDER_SIZE": 10,
        "LOOP_DELAY_SECS": 0,
        "NUM_SIMULATIONS": 1_000_000
    }

    BOT_CONFIG["MAX_INVENTORY"] = BOT_CONFIG["PORTFOLIO_SIZE"] * BOT_CONFIG["MAX_POSITION_PERCENT"]

    bot = MarketMakerBot(
        config = BOT_CONFIG,
        asset = Asset.Bitcoin,
        get_latest_price=get_latest_bitcoin_price,
        get_open_price=get_bitcoin_1h_open_price,
        get_current_event=get_current_event,
        get_close_timestamp=get_next_hour_timestamp
    )

    bot.run()


