import os
import sys
import time
import psycopg2
import psycopg2.extensions
import requests
import argparse
import logging
from typing import List, Tuple
from datetime import datetime, timedelta, date

from scripts.utils.logging_utils import log_run_start, log_run_end

# ========================
# Logger Setup
# ========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ========================
# CONFIG
# ========================
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds
SLEEP_BETWEEN_CALLS = 0.7

# DB params
DB_NAME = os.getenv("DB_NAME", "weather_accident_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))


def get_db_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


# ========================
# Helper: Build API URL
# ========================
def build_url(lat: float, lon: float, day: date) -> str:
    """Build Open-Meteo archive API URL for given day and location (UTC)."""
    start_date = day.strftime("%Y-%m-%d")
    end_date = day.strftime("%Y-%m-%d")
    params = {
        "latitude": round(lat, 2),
        "longitude": round(lon, 2),
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation,snowfall,weathercode,windspeed_10m,cloudcover,relative_humidity_2m",
        "timezone": "UTC",
    }
    return BASE_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())


# ========================
# Helper: Fetch with Retry
# ========================
def fetch_with_retry(url: str, retries: int = MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Error on attempt {attempt}: {e}")
            if attempt < retries:
                wait_time = RETRY_BACKOFF * attempt
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error("Max retries reached. Skipping.")
                return None


# ========================
# Upsert Weather Rows
# ========================
def upsert_weather_cache(conn, lat: float, lon: float, weather_json: dict):
    """Insert 24 rows of hourly weather into weather_cache."""
    if not weather_json or "hourly" not in weather_json:
        logger.warning("No hourly data in response")
        return 0

    hourly = weather_json["hourly"]
    times = hourly.get("time", [])
    row_count = 0

    with conn.cursor() as cur:
        for i, ts in enumerate(times):
            try:
                hour_utc = datetime.fromisoformat(ts)  # already UTC
            except ValueError as ve:
                logger.warning(f"Skipping malformed timestamp: {ts} ({ve})")
                continue

            cur.execute(
                """
                INSERT INTO weather_cache
                    (lat, lon, hour_utc, temperature, precipitation, snowfall, weathercode, windspeed, cloudcover, humidity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (lat, lon, hour_utc) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    precipitation = EXCLUDED.precipitation,
                    snowfall = EXCLUDED.snowfall,
                    weathercode = EXCLUDED.weathercode,
                    windspeed = EXCLUDED.windspeed,
                    cloudcover = EXCLUDED.cloudcover,
                    humidity = EXCLUDED.humidity
                """,
                (
                    round(lat, 2),
                    round(lon, 2),
                    hour_utc,
                    hourly["temperature_2m"][i],
                    hourly["precipitation"][i],
                    hourly["snowfall"][i],
                    hourly["weathercode"][i],
                    hourly["windspeed_10m"][i],
                    hourly["cloudcover"][i],
                    hourly["relative_humidity_2m"][i],
                ),
            )
            row_count += 1
    return row_count


# ========================
# Bulk: Find missing lat/lon/date
# ========================
def find_missing_triples(conn) -> List[Tuple[float, float, date]]:
    """
    Return distinct (lat, lon, day_utc) triples from raw_incidents that
    do not yet exist in weather_cache.
    
    Skips invalid coordinates (0,0).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT
                ROUND(lat::numeric, 2) AS lat_r,
                ROUND(lon::numeric, 2) AS lon_r,
                DATE(occ_date_utc) AS day_utc
            FROM raw_incidents ri
            WHERE lat IS NOT NULL AND lon IS NOT NULL
              AND NOT (lat = 0 AND lon = 0)  -- skip invalid 0,0 coords
              AND NOT EXISTS (
                  SELECT 1
                  FROM weather_cache wc
                  WHERE wc.lat = ROUND(ri.lat::numeric, 2)
                    AND wc.lon = ROUND(ri.lon::numeric, 2)
                    AND DATE(wc.hour_utc) = DATE(ri.occ_date_utc)
              )
            ORDER BY day_utc
            """
        )
        return cur.fetchall()  # List[Tuple[float, float, date]]


# ========================
# Main
# ========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lat", type=float, help="Latitude (force mode)")
    parser.add_argument("--lon", type=float, help="Longitude (force mode)")
    parser.add_argument("--date", type=str, help="Date YYYY-MM-DD (force mode)")
    parser.add_argument("--triggered-by", default="manual")
    args = parser.parse_args()

    conn = get_db_conn()
    run_id = log_run_start(conn, "weather_cache", args.triggered_by)

    try:
        if args.lat is not None and args.lon is not None and args.date is not None:
            targets = [(round(args.lat, 2), round(args.lon, 2), datetime.strptime(args.date, "%Y-%m-%d").date())]
        else:
            targets = find_missing_triples(conn)

        # Log info about what we are about to process
        if not targets:
            logger.info("No missing weather triples found. Nothing to fetch.")
        else:
            logger.info(f"Found {len(targets)} missing weather triples to fetch.")

        # Process each target triple
        total_rows = 0
        for lat, lon, day_utc in targets:
            logger.info(f"Fetching weather {day_utc} at ({lat}, {lon})")
            url = build_url(lat, lon, day_utc)
            data = fetch_with_retry(url)
            if data:
                rows = upsert_weather_cache(conn, lat, lon, data)
                total_rows += rows
            time.sleep(SLEEP_BETWEEN_CALLS)
        conn.commit()

        log_run_end(conn, run_id, "success", row_count=total_rows)

    except Exception as e:
        try:
            if conn and conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                conn.rollback()
        except Exception as rollback_exc:
            logger.error(f"Rollback failed: {rollback_exc}", exc_info=True)
        logger.error(f"Exception in main: {e}", exc_info=True)
        log_run_end(conn, run_id, "failure", error_message=str(e))
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
