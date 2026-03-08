from typing import Literal, Optional
from dataclasses import dataclass

import awswrangler as wr
import pandas as pd

from src.data.storage.bucket import S3Bucket
from src.data.storage.catalog import GlueCatalog
from src.data.storage.partition_filters import PartitionFilter


@dataclass(frozen=True)
class S3DatasetStore:
    """Declarative definition of an S3 dataset."""

    prefix: str
    """S3 prefix under which dataset files are stored."""

    mode: Literal["append", "overwrite", "overwrite_partitions"] = "overwrite_partitions"
    """
    Write mode for awswrangler Parquet datasets. See: 
    https://aws-sdk-pandas.readthedocs.io/en/3.15.0/tutorials/004%20-%20Parquet%20Datasets.html
    """

    partition_columns: Optional[list[str]] = None
    """Columns used for partitioning."""

    glue_catalog: Optional[GlueCatalog] = None
    """
    Optional Glue catalog configuration.

    If provided, the dataset will be registered/updated in the Glue Data Catalog.
    If None, data is written to S3 without Glue catalog.
    """

    def write_partitioned(
        self,           
        df: pd.DataFrame, 
        s3_bucket: S3Bucket, 
        *,
        index: bool = False
    ) -> None:
        """
        Write a partitioned Parquet dataset to S3 using awswrangler.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame containing the data to write. Must include all configured
            partition columns.
        s3_bucket : S3Bucket
            Target S3 bucket configuration.
        index : bool, optional
            Whether to store the DataFrame index in the output Parquet files.
        """
        if not self.partition_columns:
            raise ValueError("partition_columns is not configured for this dataset.")

        missing = set(self.partition_columns) - set(df.columns)
        if missing:
            raise ValueError(
                f"Partition column(s) not found in DataFrame: {sorted(missing)}"
            )

        database = table = None
        if self.glue_catalog:
            database, table = self.glue_catalog.database, self.glue_catalog.table

        wr.s3.to_parquet(
            df=df,
            index=index,
            path=s3_bucket.dataset_uri(self.prefix),
            dataset=True,
            partition_cols=self.partition_columns,
            mode=self.mode,
            database=database,
            table=table,
            boto3_session=s3_bucket.session,
            use_threads=True
        )

    def read_partitioned(
        self,
        s3_bucket: S3Bucket,
        partition_filter: Optional[PartitionFilter] = None,
        *,
        drop_partitions: bool = True,
    ) -> pd.DataFrame:
        """
        Read a partitioned Parquet dataset from S3 using awswrangler.

        Parameters
        ----------
        s3_bucket : S3Bucket
            Source S3 bucket configuration, including base URI and boto3 session.
        partition_filter : PartitionFilter, optional
            Callable used by awswrangler to filter partitions before reading.
        drop_partitions : bool, optional
            If True, drop the partition columns (``self.partition_columns``) from the returned
            DataFrame.

        Returns
        -------
        pandas.DataFrame
            Dataset contents as a DataFrame.
        """
        if not self.partition_columns:
            raise ValueError("partition_columns is not configured for this dataset.")

        df = wr.s3.read_parquet(
            path=s3_bucket.dataset_uri(self.prefix),
            dataset=True,
            partition_filter=partition_filter,
            boto3_session=s3_bucket.session,
            use_threads=True,
        )

        if drop_partitions:
            df = df.drop(columns=self.partition_columns)

        return df

