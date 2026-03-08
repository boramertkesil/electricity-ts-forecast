from typing import Union
from datetime import date

from src.data.sources.common.client import BaseClient
from src.data.sources.openmeteo.response import OpenMeteoResponse
from src.data.sources.openmeteo.location import Location
from src.validation.date import validate_date_interval
from src.validation.location import validate_locations


class OpenMeteoClient(BaseClient):
    """Client for interacting with the Open-Meteo API."""

    response_type = OpenMeteoResponse
    archive_base_url  = "https://archive-api.open-meteo.com/v1"
    forecast_base_url = "https://api.open-meteo.com/v1"

    def get_temperature(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        locations: Union[Location, list[Location]],
        *,
        timezone: str = "auto",
    ) -> list[OpenMeteoResponse]:
        """
        Fetch historical hourly temperature data for one or more locations.

        Retrieves archived hourly 2-meter air temperature data from the
        Open-Meteo Archive API for the specified date range and locations.

        Parameters
        ----------
        start_date : str or datetime.date
            Start date of the interval. May be provided either as a
            ``datetime.date`` object or as a string in ISO format
            (``YYYY-MM-DD``).

        end_date : str or datetime.date
            End date of the interval. May be provided either as a
            ``datetime.date`` object or as a string in ISO format
            (``YYYY-MM-DD``).

        locations : Location or list[Location]
            One or more location objects.

        timezone : str, optional
            Timezone for returned hourly timestamps. Use ``"auto"`` to let Open-Meteo infer
            the timezone from each locations latitude/longitude, or pass an explicit timezone
            (e.g., ``"Europe/Istanbul"``).

        Returns
        -------
        list[OpenMeteoResponse]
            Response objects for the Open-Meteo API requests.
        """
        start_date, end_date = validate_date_interval(start_date, end_date)
        locations = validate_locations(locations)

        responses: list[OpenMeteoResponse] = []

        for location in locations:
            resp = self._make_request(
                method="GET",
                url=f"{self.archive_base_url}/archive",
                params={
                    "latitude": location.lat,
                    "longitude": location.lon,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "hourly": "temperature_2m",
                    "timezone": timezone,
                },
            )
            responses.append(resp)
            
        return responses

    def get_forecast_temperature(
        self,
        locations: Union[Location, list[Location]],
        forecast_days: int = 1,
        *,
        timezone: str = "auto",
    ) -> list[OpenMeteoResponse]:
        """
        Fetch forecasted hourly temperature data for one or more locations.

        Retrieves hourly 2-meter air temperature forecasts from the
        Open-Meteo Forecast API.

        Parameters
        ----------
        locations : Location or list[Location]
            One or more location objects.

        forecast_days : int, optional
            Number of days to forecast.

        timezone : str, optional
            Timezone for returned hourly timestamps. Use ``"auto"`` to let Open-Meteo infer
            the timezone from each locations latitude/longitude, or pass an explicit timezone
            (e.g., ``"Europe/Istanbul"``).

        Returns
        -------
        list[OpenMeteoResponse]
            Response objects for the Open-Meteo API requests.
        """
        locations = validate_locations(locations)

        if forecast_days < 1:
            raise ValueError("forecast_days must be >= 1")

        responses: list[OpenMeteoResponse] = []

        for location in locations:
            resp = self._make_request(
                method="GET",
                url=f"{self.forecast_base_url}/forecast",
                params={
                    "latitude": location.lat,
                    "longitude": location.lon,
                    "hourly": "temperature_2m",
                    "forecast_days": forecast_days,
                    "timezone": timezone,
                },
            )
            responses.append(resp)

        return responses