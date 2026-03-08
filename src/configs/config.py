from pathlib import Path
import yaml

from src.data.sources.openmeteo.location import Location
from src.data.storage.bucket import S3Bucket
from src.data.storage.stores.dataset import S3DatasetStore
from src.data.storage.catalog import GlueCatalog

from sklearn.pipeline import Pipeline
from feature_engine.datetime import DatetimeFeatures
from feature_engine.timeseries.forecasting import WindowFeatures
from src.features.transformers.weighted_average import WeightedAverage

CONFIG_DIR = Path(__file__).resolve().parent

def load_config(name: str) -> dict:
    """
    Load a YAML configuration file from the local config directory.

    Parameters
    ----------
    name : str
        Filename of the YAML configuration to load (e.g. "locations.yaml").

    Returns
    -------
    dict
        Parsed configuration data as a dictionary.
    """
    path = CONFIG_DIR / name

    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")

    with path.open("r") as f:
        data = yaml.safe_load(f)

    if not data:
        raise RuntimeError(f"{name} is empty or invalid")

    return data


def get_s3_bucket(cfg: dict) -> S3Bucket:
    try:
        bucket = cfg["s3"]["bucket"]
    except KeyError as e:
        raise ValueError("Missing required config key: s3.bucket") from e

    if not isinstance(bucket, str) or not bucket:
        raise ValueError("s3.bucket must be a non-empty string")

    return S3Bucket(bucket=bucket)
    

def get_s3_dataset(cfg: dict, name: str) -> S3DatasetStore:
    try:
        datasets  = cfg["s3"]["datasets"]
    except KeyError as e:
        raise ValueError("Missing required config key: s3.datasets") from e
    
    if name not in datasets:
        raise ValueError(f"Dataset '{name}' not found in s3.datasets")

    d = datasets[name]

    return S3DatasetStore(
        prefix=d["prefix"],
        mode=d["mode"],
        partition_columns=d["partition_columns"],
        glue_catalog=GlueCatalog(
            database=d["glue_catalog"]["database"],
            table=d["glue_catalog"]["table"],
        ),
    )


def get_locations(cfg: dict) -> list[Location]:
    try:
        locations_cfg = cfg["locations"]
    except KeyError:
        raise ValueError("Missing required config key: locations")
    
    if not isinstance(locations_cfg, list):
        raise ValueError("locations must be a list")
    
    locations: list[Location] = []

    for location_cfg in locations_cfg:
        locations.append(
        Location(
            name=location_cfg["name"],
            lat=float(location_cfg["latitude"]),
            lon=float(location_cfg["longitude"]),
            )
        )

    return locations


TRANSFORMER_REGISTRY = {
    "DatetimeFeatures": DatetimeFeatures,
    "WeightedAverage": WeightedAverage,
    "WindowFeatures": WindowFeatures,
}


def get_exog_pipeline(cfg: dict) -> Pipeline:
    try:
        steps_cfg = cfg["pipeline"]
    except KeyError:
        raise ValueError("Missing required config key: pipeline")
    steps = []
    for step in steps_cfg:
        cls = TRANSFORMER_REGISTRY[step["type"]]
        steps.append((step["name"], cls(**step.get("params", {}))))
    return Pipeline(steps)


def get_forecaster_id(cfg: dict) -> str:
    try:
        forecaster_cfg = cfg["s3"]["forecaster"]
    except KeyError as e:
        raise ValueError("Missing required config key: s3.forecaster") from e

    forecaster_id = forecaster_cfg.get("id")
    if not isinstance(forecaster_id, str) or not forecaster_id:
        raise ValueError("s3.forecaster.id must be a non-empty string")

    return forecaster_id