from dataclasses import dataclass

@dataclass(frozen=True)
class Location:
    name: str
    lat: float
    lon: float