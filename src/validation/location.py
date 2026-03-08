from typing import Union

from src.data.sources.openmeteo.location import Location


class InvalidLocationError(ValueError):
    """Raised when a Location value is invalid."""

def validate_locations(
    locations: Union[Location, list[Location]]
) -> list[Location]:
    """
    Validate and normalize location input.

    Accepts either a single Location or a list of Location objects.
    Ensures all locations are valid and returns a normalized list.

    Parameters
    ----------
    locations : Location | list[Location]
        A single Location object or an list of Location objects.

    Returns
    -------
    list[Location]
        A list of validated Location objects.
    """
    if isinstance(locations, Location):
        locations = [locations]

    if not isinstance(locations, list):
        raise TypeError("locations must be a Location or a list of Locations")
    
    if not locations:
        raise ValueError("locations cannot be empty")
    
    for loc in locations:
        if not isinstance(loc, Location):
            raise TypeError("all items in locations must be Location instances")
        
        if not (-90 <= loc.lat <= 90):
            raise InvalidLocationError(f"invalid latitude for '{loc.name}': {loc.lat}")

        if not (-180 <= loc.lon <= 180):
            raise InvalidLocationError(f"invalid longitude for '{loc.name}': {loc.lon}")
        
    return locations