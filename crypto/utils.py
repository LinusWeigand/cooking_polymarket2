from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import calendar
from tzlocal import get_localzone
from enum import Enum
import math

class Asset(Enum):
    Bitcoin = "bitcoin"
    Ethereum = "ethereum"
    Solana = "solana"
    XRP = "xrp"

def get_next_hour_timestamp():
    now = datetime.now()
    next_hour = (now.replace(minute=0, second=0, microsecond=0)
                 + timedelta(hours=1))
    return int(next_hour.timestamp())

def hour_to_string(hour: int):
    suffix = "am" if hour < 12 else "pm"
    hour_12 = hour % 12 or 12

    return f"{hour_12}{suffix}-et"

def local_hour_to_eastern_time(hour: int):
    local_timezone = get_localzone()
    now_local = datetime.now(local_timezone).replace(
        hour=hour, minute=0, second=0, microsecond=0
    )
    now_et = now_local.astimezone(ZoneInfo("America/New_York"))
    et_hour = now_et.hour

    suffix = "am" if et_hour < 12 else "pm"
    hour_12 = et_hour % 12 or 12

    return f"{hour_12}{suffix}-et"

def month_to_str(month: int) -> str:
    return calendar.month_name[month].lower()

def calculate_time_to_expiration(exp_timestamp_ms):
    """Time to expiration in years."""
    expiration_dt = datetime.fromtimestamp(exp_timestamp_ms / 1000, tz=timezone.utc)
    now_dt = datetime.now(timezone.utc)
    time_delta = (expiration_dt - now_dt).total_seconds()
    if time_delta < 0:
        return 0
    return time_delta / (365 * 24 * 60 * 60)


if __name__ == '__main__':
    print(next_full_hour_timestamp())
