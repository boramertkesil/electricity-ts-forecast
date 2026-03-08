from datetime import datetime, timedelta


class TGT:
    """Represents a Ticket Granting Ticket (TGT) with an expiration time."""

    TGT_LIFETIME = timedelta(hours=1, minutes=45)

    def __init__(self, value: str):
        """
        Create a TGT instance from a ticket value.

        Parameters
        ----------
        value : str
            Raw TGT value returned by the CAS service.

        Raises
        ------
        ValueError
            If the TGT value is empty or has an invalid format.
        """
        if not value:
            raise ValueError("TGT value must be non-empty.")
        
        if not value.startswith("TGT-"):
            raise ValueError(f"Invalid TGT format: {value!r}")

        self._value = value
        self._expires_at = datetime.now() + self.TGT_LIFETIME

    @property
    def value(self) -> str:
        """Return the TGT string value."""
        return self._value
    
    @property
    def expires_at(self) -> datetime:
        """Return the expiration time of the TGT."""
        return self._expires_at

    @property
    def is_expired(self) -> bool:
        """Return True if the TGT has expired."""
        return datetime.now() >= self._expires_at
