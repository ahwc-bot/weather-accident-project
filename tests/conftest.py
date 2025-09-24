import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_tps_response():
    """Fake TPS API response with a valid record"""
    return {
        "features": [
            {
                "attributes": {
                    "OBJECTID": 1,
                    "EVENT_UNIQUE_ID": "GO-1",
                    "OCC_DATE": 1704085200000,
                    "OCC_MONTH": "January",
                    "OCC_DOW": "Monday",
                    "OCC_YEAR": "2024",
                    "OCC_HOUR": "12",
                    "DIVISION": "D1",
                    "FATALITIES": 0,
                    "INJURY_COLLISIONS": "NO",
                    "FTR_COLLISIONS": "NO",
                    "PD_COLLISIONS": "YES",
                    "HOOD_158": "H1",
                    "NEIGHBOURHOOD_158": "N1",
                    "LONG_WGS84": -79.38,
                    "LAT_WGS84": 43.65,
                    "AUTOMOBILE": "YES",
                    "MOTORCYCLE": "NO",
                    "PASSENGER": "NO",
                    "BICYCLE": "NO",
                    "PEDESTRIAN": "NO",
                },
                "geometry": {"x": -79.38, "y": 43.65},
            }
        ]
    }


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
