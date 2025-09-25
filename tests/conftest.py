import pytest
from unittest.mock import MagicMock
from datetime import datetime, timedelta
import pytz

@pytest.fixture
def mock_tps_response():
    """Fake TPS API response with multiple incidents on the same day."""
    TORONTO_TZ = pytz.timezone("America/Toronto")
    occ_date_dt = datetime(2024, 1, 1)  # January 1, 2024
    occ_timestamp_ms = int(TORONTO_TZ.localize(occ_date_dt).timestamp() * 1000)

    features = []
    # simulate 3 incidents on the same day
    for i, hour in enumerate([9, 12, 17]):  # OCC_HOUR for each incident
        features.append({
            "attributes": {
                "OBJECTID": i + 1,
                "EVENT_UNIQUE_ID": f"GO-{i+1}",
                "OCC_DATE": occ_timestamp_ms,   # same for all incidents
                "OCC_HOUR": str(hour),
                "DIVISION": "D1",
                "FATALITIES": 0,
                "INJURY_COLLISIONS": "NO",
                "FTR_COLLISIONS": "NO",
                "PD_COLLISIONS": "YES",
                "HOOD_158": "H1",
                "NEIGHBOURHOOD_158": "N1",
                "LONG_WGS84": -79.38 + i * 0.01,
                "LAT_WGS84": 43.65 + i * 0.01,
                "AUTOMOBILE": "YES",
                "MOTORCYCLE": "NO",
                "PASSENGER": "NO",
                "BICYCLE": "NO",
                "PEDESTRIAN": "NO",
            },
            "geometry": {"x": -79.38 + i * 0.01, "y": 43.65 + i * 0.01},
        })

    return {"features": features}


@pytest.fixture
def mock_db():
    """Fake psycopg2 connection + cursor"""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


@pytest.fixture
def mock_weather_response():
    """Fake Open-Meteo hourly weather data for one day."""
    hourly_data = {
        "time": [f"2024-01-01T{str(h).zfill(2)}:00:00" for h in range(24)],
        "temperature_2m": [0.1 * h for h in range(24)],
        "precipitation": [0.0 for _ in range(24)],
        "snowfall": [0.0 for _ in range(24)],
        "weathercode": [1 for _ in range(24)],
        "windspeed_10m": [0.5 for _ in range(24)],
        "cloudcover": [10 for _ in range(24)],
        "relative_humidity_2m": [50 for _ in range(24)],
    }
    return {"hourly": hourly_data}
