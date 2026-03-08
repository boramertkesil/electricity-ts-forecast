from typing import Optional

import requests

from src.data.sources.common.response import BaseResponse


class BaseClient:
    """Base HTTP client for API clients EPİAŞ and Open-Meteo."""

    response_type = BaseResponse

    def __init__(self, *, timeout: float = 30.0):
        self.timeout = timeout
        self._session: Optional[requests.Session] = None

    @property
    def session(self) -> requests.Session:
        """Return a reusable HTTP session."""
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def _make_request(
        self,
        method: str,
        url: str,
        **kwargs,
    ) -> BaseResponse:
        """Make an HTTP request and return a response for this client."""
        _response = self.session.request(
            method=method,
            url=url,
            timeout=self.timeout,
            **kwargs,
        )

        response = self.response_type(_response)
        response.raise_for_status()
        return response