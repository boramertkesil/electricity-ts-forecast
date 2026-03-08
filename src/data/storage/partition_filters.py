from typing import Callable, Mapping, Union
from datetime import date

from src.validation.date import validate_date_interval

PartitionFilter = Callable[[Mapping[str, str]], bool]


def dates_between(
    partition_column: str,
    start_date: Union[str, date],
    end_date: Union[str, date],
) -> PartitionFilter:
    """
    Return an awswrangler `partition_filter` that keeps partitions whose
    `partition_column` value is between `start_date` and `end_date`.

    Assumes partition values are ISO dates (YYYY-MM-DD), otherwise
    lexicographic comparison does not work.
    """
    start_date, end_date = validate_date_interval(start_date, end_date)

    start_date_iso = start_date.isoformat()
    end_date_iso = end_date.isoformat()

    def filter(partition: Mapping[str, str]) -> bool:
        date = partition[partition_column]
        return start_date_iso <= date <= end_date_iso

    return filter