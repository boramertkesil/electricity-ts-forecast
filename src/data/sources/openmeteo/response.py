import pandas as pd

from src.data.sources.common.response import BaseResponse


class OpenMeteoResponse(BaseResponse):
    """A response class for OpenMeteo API requests."""

    @property
    def latitude(self) -> float:
        """Latitude of the requested location."""
        return self.data["latitude"]

    @property
    def longitude(self) -> float:
        """Longitude of the requested location."""
        return self.data["longitude"]

    @property
    def timezone(self) -> str:
        """Timezone of the requested location."""
        return self.data["timezone"]

    def df(self) -> pd.DataFrame:
        """Returns the response data as a pandas DataFrame."""
        data = self.data

        if "hourly" in data:
            return pd.DataFrame(data["hourly"])
        
        raise ValueError(
            f"No parseable hourly data found in response "
            f"(available keys: {list(data.keys())})"
        )