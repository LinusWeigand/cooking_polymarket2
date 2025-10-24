"""
Compatibility helpers for parsing ISO-8601 datetimes (e.g. "2024-05-21T13:00:00Z")
across Python versions and normalizing to UTC.

Usage (from a notebook in cooking_polymarket/):

    from utils.datetime_compat import parse_iso8601, normalize_to_full_hour_utc

    target = normalize_to_full_hour_utc(last_full_hour)
    markets = [
        m for m in markets
        if normalize_to_full_hour_utc(parse_iso8601(m["markets"][0]["eventStartTime"])) == target
    ]

This will work on Python 3.6+ and handles trailing "Z" and timezone offsets.
"""
from __future__ import annotations

import datetime as _dt

try:
    from dateutil import parser as _parser  # type: ignore
    _HAS_DATEUTIL = True
except Exception:
    _HAS_DATEUTIL = False


def parse_iso8601(
    dt_str: str,
    *,
    assume_naive_tz: _dt.tzinfo | None = _dt.timezone.utc,
    to_utc: bool = True,
) -> _dt.datetime:
    """
    Parse an ISO-8601 datetime string robustly across Python versions.

    - Accepts trailing 'Z' and timezone offsets.
    - Uses stdlib when possible, falls back to python-dateutil if installed,
      then to strptime patterns.
    - By default, returns a timezone-aware UTC datetime.

    Parameters
    ----------
    dt_str : str
        ISO-8601 datetime string, e.g. "2024-05-21T13:00:00Z" or with offset.
    assume_naive_tz : tzinfo | None, default UTC
        If the parsed datetime is naive (no tzinfo), attach this tz. Set to None
        to keep naive results.
    to_utc : bool, default True
        If True and tz-aware, convert to UTC.
    """
    s = dt_str.strip()

    # Make a variant friendlier to datetime.fromisoformat for 'Z'
    s_std = s[:-1] + "+00:00" if s.endswith("Z") else s

    dt: _dt.datetime | None = None

    # 1) Try stdlib fromisoformat (Py 3.7+) with 'Z' fix
    try:
        dt = _dt.datetime.fromisoformat(s_std)  # type: ignore[attr-defined]
    except Exception:
        dt = None

    # 2) Fall back to dateutil if available
    if dt is None and _HAS_DATEUTIL:
        try:
            dt = _parser.isoparse(s)
        except Exception:
            dt = None

    # 3) Last resort: strptime with common patterns
    if dt is None:
        # %z expects +HHMM; normalize a few common forms
        s_z = (
            s.replace("Z", "+0000")
             .replace("+00:00", "+0000")
             .replace("-00:00", "-0000")
        )
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
            try:
                dt = _dt.datetime.strptime(s_z, fmt)
                break
            except Exception:
                continue

    if dt is None:
        raise ValueError(f"Unrecognized ISO-8601 datetime: {dt_str!r}")

    # Normalize tz handling
    if dt.tzinfo is None and assume_naive_tz is not None:
        dt = dt.replace(tzinfo=assume_naive_tz)
    if to_utc and dt.tzinfo is not None:
        dt = dt.astimezone(_dt.timezone.utc)

    return dt


essential_utc = _dt.timezone.utc  # convenience alias


def normalize_to_full_hour_utc(dt: _dt.datetime, *, make_naive: bool = False) -> _dt.datetime:
    """
    Normalize a datetime to the top of the hour in UTC.

    - Makes input tz-aware UTC (assumes naive is UTC),
      then zeroes minute/second/microsecond.
    - If make_naive=True, strips tzinfo at the end.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    else:
        dt = dt.astimezone(_dt.timezone.utc)

    dt = dt.replace(minute=0, second=0, microsecond=0)
    if make_naive:
        dt = dt.replace(tzinfo=None)
    return dt
