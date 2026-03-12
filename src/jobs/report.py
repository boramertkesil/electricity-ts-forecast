from datetime import date, timedelta
from typing import Optional
import logging

from matplotlib import pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

from src.data.storage.partition_filters import dates_between
from src.data.storage.stores.figure import write_figure
from src.jobs.utils import get_today
from skforecast.plot import set_dark_theme
from src.configs.config import (
    load_config,
    get_s3_bucket,
    get_s3_dataset,
)

from src.jobs.constants import STORAGE_CFG


def run(
    n_days: int,
    last_n_days: int,
) -> None:
    # logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    logging.info(f"Running report: next {n_days} day(s) and last {last_n_days} day(s)")

    # load configuration
    storage_cfg = load_config(STORAGE_CFG)

    # extract prediction values from S3
    logging.info("Extracting predictions from S3...")

    today = get_today()
    start_date = today - timedelta(days=last_n_days)
    end_date = today + timedelta(days=(n_days - 1))

    pred = _get_predictions(start_date, end_date, storage_cfg)

    pred_hist = pred[:today]   # previous forecasts
    pred_future = pred[today:] # future forecast

    # extract actual consumption values from S3
    logging.info("Extracting actual values from S3...")
    actual_hist = _get_historical_data(start_date, end_date, storage_cfg)

    # keep only the consumption column
    actual_hist = actual_hist["consumption"]

    # plot and save results
    logging.info("Writing results to S3...")
    fig_forecast = _plot_forecast(pred_future, today)
    fig_hist = _plot_hist(pred_hist, actual_hist, today)

    s3_bucket = get_s3_bucket(storage_cfg)

    write_figure(fig_forecast, "forecast_day_ahead.png", s3_bucket)
    write_figure(fig_hist, "actual_vs_forecast_last_7_days.png", s3_bucket)

    logging.info(f"Report job completed successfully")


def _plot_forecast(pred_future: pd.DataFrame, last_update: date):
    """Return a matplotlib figure for forecasted values"""
    set_dark_theme()

    fig, ax = plt.subplots(figsize=(14, 6), constrained_layout=True)

    # plot forecasts
    ax.plot(pred_future.index, pred_future, marker="o", color="#f7931a", label='lgbm_recursive_v1')

    # axes labels
    ax.set_ylabel('MW')

    # major ticks: dates
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))

    # minor ticks: hours
    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=3))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter("%H"))

    ax.tick_params(axis="x", which="major", pad=15, labelsize=10)
    ax.tick_params(axis="x", which="minor", labelsize=8)

    # Grid and legend
    ax.grid(True, alpha=0.4)
    ax.legend()

    # Title + subtitle
    ax.set_title(f"Electricity Load: Forecast", fontsize=16, pad=24)
    ax.text(
        0.5, 1.02,
        f"Last update: {last_update.strftime('%d %B %Y')}",
        transform=ax.transAxes,
        ha="center",
        va="bottom",
        fontsize=10,
        color="0.75"
    )

    return fig

def _plot_hist(pred_hist: pd.DataFrame, actual_hist: pd.DataFrame, last_update: date):
    """Return a matplotlib figure for actual vs predicted values"""
    set_dark_theme()

    fig, ax = plt.subplots(figsize=(14, 6), constrained_layout=True)

    # plot actual and forecasts
    ax.plot(actual_hist.index, actual_hist, label='Actual')
    ax.plot(pred_hist.index, pred_hist, color="#f7931a", label='Forecasted')

    # axes labels
    ax.set_ylabel('MW')

    # major ticks: dates
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))

    ax.tick_params(axis="x", which="major", pad=15, labelsize=10)

    # Grid and legend
    ax.grid(True, alpha=0.4)
    ax.legend()

    # Title + subtitle
    ax.set_title(f"Electricity Load: Actual vs Forecast", fontsize=16, pad=24)
    ax.text(
        0.5, 1.02,
        f"Last update: {last_update.strftime('%d %B %Y')}",
        transform=ax.transAxes,
        ha="center",
        va="bottom",
        fontsize=10,
        color="0.75"
    )

    return fig


def _get_predictions(start_date: date, end_date: date, storage_cfg: dict) -> pd.DataFrame:
    """Read forecast data for number of days."""
    bucket = get_s3_bucket(storage_cfg)
    dataset = get_s3_dataset(storage_cfg, "forecasts")

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

"""
Not used currently may implement later to compare with offical 
forecasts from EPİAŞ.

def _get_lep(start_date: date, end_date: date) -> pd.DataFrame:
    client = EpiasClient.from_env(
        username_var="EPIAS_USERNAME",
        password_var="EPIAS_PASSWORD",
    )

    response = client.get_load_estimation_plan(start_date, end_date)
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
"""

if __name__ == "__main__":
    run(n_days=1, last_n_days=7)