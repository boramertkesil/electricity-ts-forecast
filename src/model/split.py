from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Interval:
    start: pd.Timestamp
    end: pd.Timestamp  # inclusive

    def slice(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.loc[self.start:self.end].copy()

    @property
    def length(self) -> int:
        """Expected number of hourly timestamps in [start, end], assuming 1h frequency and no gaps."""
        hours = int((self.end - self.start) / pd.Timedelta(hours=1))
        return hours + 1
    
    def __add__(self, other: "Interval") -> "Interval":
        if not isinstance(other, Interval):
            return NotImplemented

        # allow merge if overlapping or touching (adjacent by 1 hour)
        if other.start > self.end + pd.Timedelta(hours=1) or self.start > other.end + pd.Timedelta(hours=1):
            raise ValueError("Intervals do not overlap or touch; cannot merge with +.")

        return Interval(start=min(self.start, other.start), end=max(self.end, other.end))
    
    def __repr__(self) -> str:
        return f"Interval(start={self.start!s}, end={self.end!s}, length={self.length})"
    

def split(df: pd.DataFrame, validation_days: int, test_days: int):
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("df must have an index of type pandas.DatetimeIndex")
    
    start = df.index.min()
    end = df.index.max()
    
    expected_date_range = pd.date_range(
        start=start,
        end=end,
        freq="h"
    )

    # check for missing/extra timestamps in the index
    dates_complete = df.index.equals(expected_date_range)

    if not dates_complete:
        raise ValueError("index must be complete hourly timestamps (no gaps or duplicates).")

    end_normalized = end.normalize()  # 00:00 of last day in data

    test = Interval(
        start = end_normalized - pd.Timedelta(days=test_days - 1),    # 00:00 of first test day
        end = end_normalized + pd.Timedelta(hours=23),                # 23:00 of last test day
    )

    validation = Interval(
        start = test.start - pd.Timedelta(days=validation_days),      # 00:00 of first val day
        end = test.start - pd.Timedelta(hours=1),                     # 23:00 of last val day
    )

    train = Interval(
        start = start,                                                # 00:00 of first train day
        end = validation.start - pd.Timedelta(hours=1),               # 23:00 of last train day
    )

    return train, validation, test