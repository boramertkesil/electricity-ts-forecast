from datetime import date, timedelta
from typing import Optional
import logging

from skforecast.recursive import ForecasterRecursive
import pandas as pd

from src.data.sources.openmeteo.client import OpenMeteoClient
from src.data.storage.stores.forecaster import read_forecaster
from src.data.storage.partition_filters import dates_between
from src.data.preprocessing.datetime import extract_date_column, set_datetime_index
from src.jobs.utils import get_today
from src.configs.config import (
    load_config,
    get_forecaster_id,
    get_exog_pipeline,
    get_locations,
    get_s3_bucket,
    get_s3_dataset,
)

from src.jobs.constants import EXOG_CFG, STORAGE_CFG, LOCATIONS_CFG


def run(
    n_days: int,
    window_size: int,
) -> None:
    """
    Run the prediction job.

    This function generates electricity consumption forecasts for the
    next `n_days`.

    Parameters
    ----------
    n_days : int
        Number of days to forecast ahead.
    window_size : int or None
        Size of the historical window in hours. If None,
        the forecaster's configured ``window_size`` is used.

    IMPORTANT:
        If the forecasting pipeline uses exogenous features that
        require a larger historical context (e.g., lagged features,
        rolling statistics, or transformations with lookback windows),
        you must explicitly provide a sufficiently large ``window_size``.
        Otherwise, the feature matrix may contain NaN values or
        produce incorrect predictions.
    """
    # logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    logging.info(f"Running forecast job for {n_days} day(s)")
    
    # load configuration
    exog_cfg = load_config(EXOG_CFG)
    storage_cfg = load_config(STORAGE_CFG)
    locations_cfg = load_config(LOCATIONS_CFG)

    # load forecaster
    logging.info("Loading trained forecaster...")
    forecaster = _load_forecaster(storage_cfg)
    logging.info(f"Forecaster '{forecaster.forecaster_id}' loaded successfully")

    # determine last window range
    start, end = _get_last_window(window_size, forecaster)

    # extract data
    logging.info(f"Loading historical data for the last window: {start} -> {end}...")
    data = _get_historical_data(start, end, storage_cfg)

    # features and target
    X = data.drop(columns=["consumption"])
    y = data["consumption"].asfreq("h")

    # extract temperature forecasts
    logging.info(f"Extracting temperature forecast values from OpenMeteo...")
    X_hat = _get_temperature_forecasts(n_days, locations_cfg)

    # fit and transform exogenous features
    logging.info("Transforming exogenous features...")
    X_transformed = _fit_transform_exog(X, X_hat, exog_cfg)

    # keep only future features starting from today
    today = get_today()
    X_future = X_transformed.loc[today:]

    # predict
    logging.info("Forecasting...")
    steps = n_days * 24
    preds = _predict(y, X_future, forecaster, steps)

    # load to S3
    logging.info("Loading forecast data to s3...")
    _load(preds, storage_cfg)

    logging.info(f"Forecast job completed successfully")


def _predict(
    y: pd.Series,
    X: pd.DataFrame,
    forecaster: ForecasterRecursive,
    steps: int,
) -> pd.Series:
    """Generate recursive forecasts."""
    return forecaster.predict(
        steps=steps,
        last_window=y,
        exog=X,
    )


def _load_forecaster(storage_cfg: dict) -> ForecasterRecursive:
    """Load trained forecaster from S3."""
    forecaster_id = get_forecaster_id(storage_cfg)
    bucket = get_s3_bucket(storage_cfg)

    return read_forecaster(
        forecaster_id=forecaster_id,
        s3_bucket=bucket,
    )


def _get_last_window(
    window_size: Optional[int],
    forecaster: ForecasterRecursive,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Compute the start and end timestamps for the last window."""
    if window_size is None:
        window_size = forecaster.window_size

    today = get_today()

    start = today - timedelta(hours=window_size)
    end = today - timedelta(days=1)

    return start, end


def _get_historical_data(
    start_date: date,
    end_date: date,
    storage_cfg: dict,
) -> pd.DataFrame:
    """Read partitioned historical dataset between two dates."""
    bucket = get_s3_bucket(storage_cfg)
    dataset = get_s3_dataset(storage_cfg, "features")

    if "date" not in dataset.partition_columns:
        raise ValueError("S3 dataset must have 'date' as a partition column")

    date_filter = dates_between(
        partition_column="date",
        start_date=start_date,
        end_date=end_date,
    )

    data = dataset.read_partitioned(
        s3_bucket=bucket,
        partition_filter=date_filter,
        drop_partitions=True,
    )

    return data


def _fit_transform_exog(
    X: pd.DataFrame,
    X_hat: pd.DataFrame,
    exog_cfg: dict,
) -> pd.DataFrame:
    """Fit and transform exogenous variables using configured pipeline."""
    exog_transformer = get_exog_pipeline(exog_cfg)

    X_combined = pd.concat([X, X_hat], axis=0)
    X_transformed = exog_transformer.fit_transform(X_combined)

    return X_transformed


def _get_temperature_forecasts(
    n_days: int,
    locations_cfg: dict,
) -> pd.DataFrame:
    """Retrieve temperature forecasts for multiple locations."""
    client = OpenMeteoClient()
    locations = get_locations(locations_cfg)

    responses = client.get_forecast_temperature(
        forecast_days=n_days,
        locations=locations,
    )

    frames: list[pd.DataFrame] = []

    for resp, loc in zip(responses, locations):
        df = resp.df()
        df = df.rename(columns={"temperature_2m": f"{loc.name}_temp"})
        df = set_datetime_index(
            df,
            column="time",
            index_name="datetime",
        )
        frames.append(df)

    temperatures_wide = pd.concat(frames, join="inner", axis=1)
    return temperatures_wide


def _load(df: pd.DataFrame, storage_cfg: dict) -> None:
    """Extract date column and load dataframe into S3 as a partitioned dataset."""
    bucket = get_s3_bucket(storage_cfg)
    dataset = get_s3_dataset(storage_cfg, "forecasts")

    df = extract_date_column(df, column_name="date")

    dataset.write_partitioned(
        df=df,
        s3_bucket=bucket,
        index=True,
    )

if __name__ == "__main__":
    run(n_days=1, window_size=168)