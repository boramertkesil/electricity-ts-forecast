from dataclasses import dataclass

import awswrangler as wr


@dataclass(frozen=True)
class GlueCatalog:
    database: str
    table: str

    @property
    def partitions(self) -> list[dict]:
        """
        Return partitions of the Glue table.

        """
        return wr.catalog.get_partitions(database=self.database, table=self.table).values()
    
    @property
    def latest_partition(self):
        """
        Returns the latest partition based on partition values.
        """
        if not self.partitions:
            return None

        # Assumes partitions are dictionaries with keys like 'Values' or column names
        # Sort by the first partition column
        return max(self.partitions)