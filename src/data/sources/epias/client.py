from typing import Any, Union, Optional
from datetime import date, timedelta
import os

from src.data.sources.common.client import BaseClient
from src.data.sources.epias.response import EpiasResponse
from src.data.sources.epias.tgt import TGT
from src.data.sources.utils import to_datetime
from src.validation.date import validate_date, validate_date_interval


class EpiasClient(BaseClient):
    """Client for interacting with the EPİAŞ Electricity Transparency Platform API."""

    response_type = EpiasResponse
    base_url = "https://seffaflik.epias.com.tr/electricity-service/v1"
        
    def __init__(self, username: str, password: str, *, timeout: float = 30.0):
        """
        Create an EPİAŞ client.

        Parameters
        ----------
        username : str
            EPİAŞ username.
        password : str
            EPİAŞ password.
        timeout : float, optional
            Request timeout in seconds.
        """
        super().__init__(timeout=timeout)
        self.username = username
        self.password = password
        self._tgt: Optional[TGT] = None

    @classmethod
    def from_env(
        cls,
        username_var: str = "EPIAS_USERNAME",
        password_var: str = "EPIAS_PASSWORD",
    ) -> "EpiasClient":
        """
        Create an EpiasClient from environment variables.

        Raises
        ------
        EnvironmentError
            If required environment variables are missing.
        """
        username = os.getenv(username_var)
        password = os.getenv(password_var)

        if not username or not password:
            raise ValueError(f"Missing EPİAŞ credentials: check {username_var} and {password_var} are set")

        return cls(username=username, password=password)

    # EPİAŞ endpoints typically restrict the maximum request window.
    MAX_RANGE = timedelta(days=365)

    def get_realtime_consumption(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
    ) -> EpiasResponse:
        """
        Fetch real-time electricity consumption data from EPİAŞ.

        Retrieves consumption values for the given date interval from the
        EPİAŞ Şeffaflık Platform Electricity Service.

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
            
        Returns
        -------
        EpiasResponse
            Response object for the EPİAŞ API request.
        """
        start_date, end_date = validate_date_interval(start_date, end_date, max_range=self.MAX_RANGE)
        
        return self._make_request_with_tgt(
            method="POST",
            url=f"{self.base_url}/consumption/data/realtime-consumption",
            json={
                "startDate": to_datetime(start_date).isoformat(),
                "endDate": to_datetime(end_date).isoformat(),
            }
        )
    
    def get_percentage_consumption_info(
        self, 
        period: Union[str, date]
        ) -> EpiasResponse:
        """
        Fetch percentage-based electricity consumption data from EPİAŞ.

        Retrieves provincial electricity consumption ratios for the given date
        from the EPİAŞ Şeffaflık Platform Electricity Service.

        Parameters
        ----------
        period : str or datetime.date
            Date for which percentage-based electricity consumption data is
            requested. May be provided either as a ``datetime.date`` object
            or as a string in ISO format (``YYYY-MM-DD``).

        Returns
        -------
        EpiasResponse
            Response object for the EPİAŞ API request.
        """
        period = validate_date(period)

        return self._make_request_with_tgt(
            method="POST",
            url=f"{self.base_url}/consumption/data/percentage-consumption-info",
            json={
                "period": to_datetime(period).isoformat()
            }
        )
    
    def get_load_estimation_plan(
        self, 
        start_date: Union[str, date],
        end_date: Union[str, date],
        ) -> EpiasResponse:
        """
        Fetch the hourly next-day demand forecast from EPİAŞ.

        Retrieves demand forecast values for the given date interval from the
        EPİAŞ Şeffaflık Platform Electricity Service.

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
            
        Returns
        -------
        EpiasResponse
            Response object for the EPİAŞ API request.
        """
        start_date, end_date = validate_date_interval(start_date, end_date, max_range=self.MAX_RANGE)

        return self._make_request_with_tgt(
            method="POST",
            url=f"{self.base_url}/consumption/data/load-estimation-plan",
            json={
                "startDate": to_datetime(start_date).isoformat(),
                "endDate": to_datetime(end_date).isoformat(),
            }
        )
    

    def _fetch_tgt(self) -> None:
        """Fetch and store a new TGT token."""
        if not self.username or not self.password:
            raise ValueError("Username and password must be provided for EPİAŞ client")
        
        response = self._make_request(
            method="POST",
            url="https://giris.epias.com.tr/cas/v1/tickets",
            data={
                "username": self.username,
                "password": self.password,
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "text/plain",
            },
        )

        self._tgt = TGT(response.text)

    def _make_request_with_tgt(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> EpiasResponse:
        """Make an authenticated EPİAŞ API request."""
        if self._tgt is None or self._tgt.is_expired:
            self._fetch_tgt()

        headers = {
            "Content-Type": "application/json",
            "TGT": self._tgt.value,
        }

        return self._make_request(method, url, headers=headers, **kwargs)