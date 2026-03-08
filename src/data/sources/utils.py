from datetime import date, datetime, time
from zoneinfo import ZoneInfo


def to_datetime(d: date, timezone: str = "Europe/Istanbul") -> datetime:
    """
    Convert a date to a timezone-aware datetime at 00:00:00 local time.
    """
    return datetime.combine(d, time.min, tzinfo=ZoneInfo(timezone))