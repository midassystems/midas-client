import os
from datetime import datetime, timezone


def load_url(var: str) -> str:
    url = os.getenv(var)

    if url is None:
        raise ValueError(f"{var} environment variable is not set.")

    return url


def iso_to_unix(timestamp_str: str) -> int:
    """
    Converts an ISO 8601 formatted date string to a UNIX timestamp in nanoseconds.

    This function parses a provided ISO 8601 string, which may or may not include timezone information.
    If no timezone is specified, the function assumes the input is in UTC. It then converts this datetime object to
    the corresponding UNIX timestamp expressed in nanoseconds since the epoch (January 1, 1970, 00:00:00 UTC).

    Parameters:
    - timestamp_str (str): An ISO 8601 formatted datetime string.

    Returns:
    - int: The UNIX timestamp in nanoseconds corresponding to the given ISO 8601 datetime.
    """
    try:
        # Parse the ISO string
        dt = datetime.fromisoformat(timestamp_str)

        # If the datetime object is naive (no timezone), assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Convert to UTC if it has a timezone
            dt = dt.astimezone(timezone.utc)

        # Convert to Unix timestamp in nanoseconds
        unix_timestamp = int(dt.timestamp() * 1e9)
        return unix_timestamp

    except ValueError as e:
        raise ValueError(f"Invalid ISO 8601 format: {timestamp_str}") from e
