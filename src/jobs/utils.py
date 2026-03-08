from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def get_today(zone_info: str = "Europe/Istanbul") -> datetime.date:
    """
    Returns today's date in the specified timezone.

    Parameters
    ----------
    zone_info : str
        IANA timezone string (default "Europe/Istanbul").

    Returns:
        datetime.date: Today's date in that timezone.
    """
    now = datetime.now(ZoneInfo(zone_info))
    return now.date()


def get_yesterday(zone_info: str = "Europe/Istanbul") -> datetime.date:
    """
    Returns yesterday's date in the specified timezone.

    Parameters
    ----------
    zone_info : str
        IANA timezone string (default "Europe/Istanbul").

    Returns:
        datetime.date: Yesterday's date in that timezone.
    """
    today = get_today(zone_info)
    return today - timedelta(days=1)