from typing import Union, Optional
from datetime import datetime, date, timedelta


class InvalidDateError(ValueError):
    """Raised when a single date value is invalid or unsupported."""


class InvalidDateIntervalError(ValueError):
    """Raised when a date interval (start/end) is invalid."""


def validate_date(value: Union[str, date]) -> date:
    """
    Validate and normalize a date value.

    Parameters
    ----------
    value : str or datetime.date
        Date value to validate. Strings must be in 'YYYY-MM-DD' format.

    Returns
    -------
    datetime.date
        A validated and normalized date object.
    """
    match value:
        case datetime():
            return value.date()
        
        case date():
            return value
        
        case str():
            try:
                return datetime.strptime(value, "%Y-%m-%d").date()
            except ValueError as exc:
                raise InvalidDateError(f"invalid date format {value!r}, expected 'YYYY-MM-DD'") from exc
            
        case _:
            raise TypeError(f"unsupported date type {type(value).__name__}; expected str or datetime.date")
        

def validate_date_interval(
    start_date: Union[str, date],
    end_date: Union[str, date],
    *,
    max_range: Optional[timedelta] = None,
) -> tuple[date, date]:
    """
    Validate and normalize a date interval.

    Parameters
    ----------
    start_date : str or datetime.date
        Interval start date.
    end_date : str or datetime.date
        Interval end date.
    max_range : datetime.timedelta, optional
        Maximum allowed length of the interval.

    Returns
    -------
    tuple[datetime.date, datetime.date]
        A validated and normalized date range.
    """
    start_date = validate_date(start_date)
    end_date = validate_date(end_date)

    if start_date > end_date:
        raise InvalidDateIntervalError("start_date must be less than or equal to end date")

    if max_range is not None and (end_date - start_date) > max_range:
        raise InvalidDateIntervalError(
            f"date range exceeds maximum of {max_range.days} days"
        )

    return start_date, end_date