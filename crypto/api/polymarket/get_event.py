from zoneinfo import ZoneInfo

from crypto.api.polymarket.mod import GAMMA_API_BASE
from crypto.utils import Asset, month_to_str, local_hour_to_eastern_time, hour_to_string
import requests
from datetime import datetime

def get_up_or_down_event(asset: Asset, day: int, month: int, hour: int):
    month_str = month_to_str(month)
    hour = hour_to_string(hour)
    ticker = f"{asset.value}-up-or-down-{month_str}-{day}-{hour}"
    print(ticker)
    url = f"{GAMMA_API_BASE}/events/slug/{ticker}"

    resp = requests.get(url)
    resp.raise_for_status()
    current_event = resp.json()

    return current_event

def get_current_event(asset: Asset):
    eastern_tz = ZoneInfo("America/New_York")
    now_et = datetime.now(eastern_tz)
    return get_up_or_down_event(asset, now_et.day, now_et.month, now_et.hour)

if __name__ == "__main__":
    event = get_current_event(Asset.Bitcoin)
    print(event)
