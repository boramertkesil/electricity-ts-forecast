from typing import Optional, Union

import pandas as pd


def set_datetime_index(
    df: pd.DataFrame,
    column: str,
    index_name: Optional[str] = None,
    drop_tz_info: bool = True,
) -> pd.DataFrame:
    """
    Parse `column` as datetime and set it as the DataFrame index.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    column : str
        Name of the source column.
    drop_tz_info : bool, default True
        Whether to drop timezone information if there is one.
    index_name : str | None, default None
        If provided, rename the resulting DatetimeIndex to this value.
        If None, keeps the original column name.

    Returns
    -------
    pd.DataFrame
        DataFrame with a parsed datetime index.
    """
    df = df.copy()

    df[column] = pd.to_datetime(df[column], errors="raise")
    df = df.set_index(column)

    if drop_tz_info and df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    if index_name is not None:
        df.index = df.index.rename(index_name)

    return df


def extract_date_column(
    df: Union[pd.DataFrame, pd.Series],
    column_name: str = "date"
) -> pd.DataFrame:
    """
    Extracts a date column from the input's `DatetimeIndex`.

    Parameters
    ----------
    df : pd.DataFrame or pd.Series
        Input data with a DatetimeIndex.
        If a Series is provided, it will be converted to a DataFrame.
    column_name : str, default "date"
        Name of the output date column.

    Returns
    -------
    pd.DataFrame 
        DataFrame with added date column.
    """
    if isinstance(df, pd.Series):
        df = df.to_frame()

    df = df.copy()
    df[column_name] = df.index.date
    return df