import io
import pickle
from typing import Union

import awswrangler as wr
from skforecast.recursive import ForecasterRecursive
from skforecast.direct import ForecasterDirect

from src.data.storage.bucket import S3Bucket


def write_forecaster(
    forecaster: Union[ForecasterRecursive, ForecasterDirect],
    s3_bucket: S3Bucket,
) -> None:
    """
    Write a fitted forecaster to S3 using awswrangler.

    Parameters
    ----------
    forecaster : ForecasterRecursive or ForecasterDirect
        The forecaster object to upload. Must be either
        a ForecasterRecursive or ForecasterDirect and already fitted.

    s3_bucket : S3Bucket
        Target S3 bucket configuration.  
    """
    if not forecaster.is_fitted:
        raise ValueError("Forecaster must be fitted before uploading to S3.")
    
    if not isinstance(forecaster, (ForecasterRecursive, ForecasterDirect)):
        raise TypeError(
            f"Unsupported forecaster type: {type(forecaster).__name__}. "
            "Expected ForecasterRecursive or ForecasterDirect."
        )

    key = f"{forecaster.forecaster_id}.pkl"
    
    with io.BytesIO() as buffer:
        pickle.dump(forecaster, buffer)
        buffer.seek(0)

        wr.s3.upload(
            local_file=buffer,
            path=s3_bucket.forecaster_uri(key),
            boto3_session=s3_bucket.session,
        )


def read_forecaster(
    forecaster_id: str,
    s3_bucket: S3Bucket
) -> Union[ForecasterRecursive, ForecasterDirect]:
    """
    Download a forecaster pickle from S3 into memory.

    Parameters
    ----------
    forecaster_id : str
        The unique identifier of the forecaster to load.

    s3_bucket : S3Bucket
        Target S3 bucket configuration.

    Returns
    -------
    ForecasterRecursive or ForecasterDirect
        The forecaster object loaded from S3.
    """
    path = s3_bucket.forecaster_uri(f"{forecaster_id}.pkl")

    with io.BytesIO() as buffer:
        wr.s3.download(path=path, local_file=buffer)
        buffer.seek(0)

        forecaster = pickle.load(buffer)

    return forecaster