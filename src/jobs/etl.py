from datetime import date
from typing import Union
import logging

import pandas as pd

from src.data.sources.epias.client import EpiasClient
from src.data.sources.openmeteo.client import OpenMeteoClient
from src.data.preprocessing.datetime import set_datetime_index, extract_date_column
from src.validation.date import validate_date_interval
from src.jobs.utils import get_yesterday
from src.configs.config import (
    load_config,
    get_locations,
    get_s3_bucket,
    get_s3_dataset,
)

from src.jobs.constants import STORAGE_CFG, LOCATIONS_CFG


def run(
    start_date: Union[str, date],
    end_date: Union[str, date],
) -> None:
    """
    Run the ETL job. 
    
    This function fetches EPİAŞ consumption and Open-Meteo
    temperatures for the given date range, joins them on the 
    datetime index, and writes the result to S3 partitioned by date.

    Parameters
    ----------
    start_date : str, datetime.date, or None
        Start date of the interval. May be provided either as a
        ``datetime.date`` object or as a string in ISO format
        (``YYYY-MM-DD``).

    end_date : str or datetime.date, or None
        End date of the interval. May be provided either as a
        ``datetime.date`` object or as a string in ISO format
        (``YYYY-MM-DD``).
    """
    # validate
    start_date, end_date = validate_date_interval(start_date, end_date)

    # logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    logging.info(f"Running ETL job for dates: {start_date} -> {end_date}")

    # load configuration
    storage_cfg = load_config(STORAGE_CFG)
    locations_cfg = load_config(LOCATIONS_CFG)
    
    # extract data
    logging.info(f"Extracting consumption values from EPİAŞ...")
    consumption_df = _get_consumption(start_date, end_date)

    logging.info(f"Extracting temperature values from OpenMeteo...")
    temperature_df = _get_temperature(start_date, end_date, locations_cfg)

    # transform
    logging.info("Transforming data...")
    transformed_df = _transform(consumption_df, temperature_df)

    # load to S3
    logging.info("Loading transformed data to S3...")
    _load(transformed_df, storage_cfg)

    logging.info(f"ETL job completed successfully")


def _get_consumption(start_date: date, end_date: date) -> pd.DataFrame:
    """Extract electricity consumption data from EPIAS."""
    client = EpiasClient.from_env(
        username_var="EPIAS_USERNAME",
        password_var="EPIAS_PASSWORD",
    )

    response = client.get_realtime_consumption(start_date, end_date)
    df = response.df()

    # drop unnecessary column
    df = df.drop(columns=["time"], errors="ignore")

    # set datetime index
    df = set_datetime_index(
        df,
        column="date",
        drop_tz_info=True,
        index_name="datetime",
    )

    return df


def _get_temperature(start_date: date, end_date: date, locations_cfg: dict) -> pd.DataFrame:
    """Extract temperature data for multiple locations from OpenMeteo."""
    client = OpenMeteoClient()
    locations = get_locations(locations_cfg)
    responses = client.get_temperature(start_date, end_date, locations)

    dfs = []

    for resp, loc in zip(responses, locations):
        df = resp.df()

        # Rename temperature column for location
        df = df.rename(columns={"temperature_2m": f"{loc.name}_temp"})

        # Set datetime index
        df = set_datetime_index(
            df,
            column="time",
            index_name="datetime"
        )

        dfs.append(df)

    # Concatenate all location DataFrames into a wide table
    temperatures_wide = pd.concat(dfs, join="inner", axis=1)
    return temperatures_wide


def _transform(consumption_df: pd.DataFrame, temperature_df: pd.DataFrame) -> pd.DataFrame:
    """Combine consumption and temperature data."""
    combined_df = consumption_df.join(
        temperature_df,
        how="left",
        validate="1:1"
    )

    return combined_df


def _load(df: pd.DataFrame, storage_cfg: dict) -> None:
    """Extract date column and load dataframe into S3 as a partitioned dataset."""
    bucket = get_s3_bucket(storage_cfg)
    dataset = get_s3_dataset(storage_cfg, "features")

    df = extract_date_column(df, column_name="date")

    dataset.write_partitioned(
        df=df,
        s3_bucket=bucket,
        index=True,
    )

if __name__ == "__main__":
    yesterday = get_yesterday()
    run(start_date=yesterday, end_date=yesterday)