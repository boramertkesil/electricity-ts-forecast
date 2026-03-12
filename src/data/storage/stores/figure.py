import io

import awswrangler as wr
from matplotlib import pyplot as plt
from src.data.storage.bucket import S3Bucket


def write_figure(
    fig: plt.Figure,
    fig_name: str,
    s3_bucket: S3Bucket,
) -> None:
    """
    Write a matplotlib figure to S3 using awswrangler.

    Parameters
    ----------
    fig : matplotlib.figure.Figure
        The figure to upload.

    report_name : str
        Name of the file in S3.

    s3_bucket : S3Bucket
        Target S3 bucket configuration.  
    """
    if not isinstance(fig, plt.Figure):
        raise TypeError(f"Expected a matplotlib.figure.Figure, got {type(fig).__name__}.")

    with io.BytesIO() as buffer:
        fig.savefig(buffer, format="png", bbox_inches="tight")
        buffer.seek(0)

        wr.s3.upload(
            local_file=buffer,
            path=s3_bucket.figure_uri(fig_name),
            boto3_session=s3_bucket.session,
            s3_additional_kwargs={
                "ContentType": "image/png",
                "CacheControl": "no-cache"
            }
        )