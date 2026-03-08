from typing import Any

from requests import Response


class ResponseError(Exception):
    """Base exception for response-related errors."""

class HTTPError(ResponseError):
    """Raised when an HTTP response indicates failure."""

class BaseResponse:
    """A base response for API clients."""

    def __init__(self, response: Response):
        """Create a response object for this client."""
        self._response = response
        self._data = None

    @property
    def status_code(self) -> int:
        """Return the HTTP status code."""
        return self._response.status_code

    @property
    def ok(self) -> bool:
        """Return True if the request was successful."""
        return self._response.ok

    @property
    def url(self) -> str:
        """Return the request URL."""
        return self._response.url

    @property
    def text(self) -> str:
        """Return the response body as text."""
        return self._response.text

    @property
    def data(self) -> Any:
        """Return the response body as dict."""
        if self._data is None:
            self._data = self._response.json()
        return self._data
    
    # May add client level errors later.
    def raise_for_status(self) -> None:
        """
        Raise an exception if the HTTP response indicates failure.
        """
        if not self.ok:
            raise HTTPError(
                f"HTTP {self.status_code} for {self.url}"
            )