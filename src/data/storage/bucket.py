from typing import Optional

import boto3


class S3Bucket:
    """S3 bucket with URI builders."""

    def __init__(self, bucket: str):
        """
        Parameters
        ----------
        bucket : str
            S3 bucket name
        """
        self.bucket = bucket
        self._session: Optional[boto3.Session] = None

    @property
    def session(self) -> boto3.Session:
        # Not quite sure this belongs here, since 
        # it's not really a property of the bucket.
        """Return a reusable boto3 session."""
        if self._session is None:
            self._session = boto3.Session()
        return self._session

    @property
    def base_uri(self) -> str:
        """Base URI for the bucket."""
        return f"s3://{self.bucket}"

    def dataset_uri(self, dataset: str) -> str:
        """Datasets root URI"""
        return f"{self.base_uri}/datasets/{dataset}/"
    
    def forecaster_uri(self, forecaster: str) -> str:
        """Forecaster root URI"""
        return f"{self.base_uri}/forecasters/{forecaster}"
