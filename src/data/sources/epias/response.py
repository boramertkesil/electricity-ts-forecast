import pandas as pd

from src.data.sources.common.response import BaseResponse


class EpiasResponse(BaseResponse):
    """A response class for EPİAŞ API requests."""

    def df(self) -> pd.DataFrame:
        """Returns the response data as a pandas DataFrame."""
        data = self.data

        if isinstance(data, dict):
            if "items" in data:
                return pd.DataFrame(data["items"])

        raise TypeError(
            f"EPIAS response at {self.url} has unsupported type "
            f"{type(data).__name__}"
        )