import os
import sys
import time
import json
import psycopg2
import psycopg2.extensions
import requests
import argparse
import logging
from datetime import datetime, timedelta, time as dt_time
from urllib.parse import quote, urlencode
import pytz

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
BASE_URL = (
    "https://services.arcgis.com/S9th0jAJ7bqgIRjw/ArcGIS/rest/services/"
    "Traffic_Collisions_Open_Data/FeatureServer/0/query"
)
MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds
SLEEP_BETWEEN_CALLS = 0.7
TORONTO_TZ = pytz.timezone("America/Toronto")

# Earliest date from which incident data should be fetched.
PROJECT_BASELINE = TORONTO_TZ.localize(datetime(2024, 1, 1))

# Fetch DB connection parameters once
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
def build_url(date_local: datetime) -> str:
    """Build API URL for one day (Toronto local)."""
    start_local = date_local.strftime("%Y-%m-%d 00:00:00")
    end_local = (date_local + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

    where_clause = (
        f"OCC_DATE >= TIMESTAMP '{start_local}' "
        f"AND OCC_DATE < TIMESTAMP '{end_local}'"
    )

    params = {
        "where": where_clause,
        "outFields": "*",
        "f": "json",
        "resultRecordCount": 1000,
        "resultOffset": 0,
    }

    query_string = urlencode(params, quote_via=quote)
    return f"{BASE_URL}?{query_string}"


# ========================
# Helper: Fetch with Retry
# ========================
def fetch_with_retry(url: str, retries: int = MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Error on attempt {attempt}: {e}")
            if attempt < retries:
                wait_time = RETRY_BACKOFF * attempt
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error("Max retries reached. Skipping this date.")
                return None


# ========================
# Insert into DB
# ========================
def upsert_raw_incidents(conn, features):
    row_count = 0
    with conn.cursor() as cur:
        for f in features:
            attrs = f.get("attributes", {})
            geom = f.get("geometry", {})

            occ_date_utc = None
            # OCC_DATE is already a UTC timestamp at midnight, or a ms epoch at midnight
            occ_date_val = attrs.get("OCC_DATE")
            if occ_date_val:
                occ_dt = datetime.fromtimestamp(occ_date_val / 1000, tz=pytz.UTC)

                # Add OCC_HOUR if present
                occ_hour = attrs.get("OCC_HOUR")
                if occ_hour is not None:
                    # Convert UTC midnight to Toronto local
                    occ_local = occ_dt.astimezone(TORONTO_TZ)
                    # Combine date with OCC_HOUR explicitly
                    occ_local = TORONTO_TZ.localize(datetime.combine(occ_local.date(), dt_time(int(occ_hour), 0)))
                    # Convert back to UTC
                    occ_dt = occ_local.astimezone(pytz.UTC)

                occ_date_utc = occ_dt

            objectid = attrs.get("OBJECTID")
            lat = attrs.get("LAT_WGS84")
            lon = attrs.get("LONG_WGS84")

            if (lat == 0 and lon == 0) or (lat is None or lon is None):
                logger.warning(f"OBJECTID={objectid} has invalid/missing coords, saving as NULL")
                lat, lon = None, None

            # Extract stable unique event identifier
            event_id = attrs.get("EVENT_UNIQUE_ID")
            if not event_id:
                logger.warning(f"Skipping OBJECTID={objectid} because EVENT_UNIQUE_ID is missing")
                continue

            cur.execute(
                """
                INSERT INTO raw_incidents (event_id, objectid, raw, occ_date_utc, lat, lon)
                VALUES (%s, %s, %s::jsonb, %s, %s, %s)
                ON CONFLICT (event_id)
                DO UPDATE SET
                    objectid = EXCLUDED.objectid,
                    raw = EXCLUDED.raw,
                    occ_date_utc = EXCLUDED.occ_date_utc,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon
                """,
                (
                    event_id,
                    objectid,
                    json.dumps(f),
                    occ_date_utc,
                    lat,
                    lon,
                ),
            )
            row_count += 1
    return row_count


# ========================
# Main
# ========================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", type=str, help="Start date YYYY-MM-DD (Toronto local)")
    parser.add_argument("--end-date", type=str, help="End date YYYY-MM-DD (Toronto local)")
    parser.add_argument("--triggered-by", default="manual")
    args = parser.parse_args()

    conn = get_db_conn()
    run_id = log_run_start(conn, "tps_ingest", args.triggered_by)

    try:
        # Determine start_date
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(occ_date_utc) FROM raw_incidents")
            last_date = cur.fetchone()[0]

        if args.start_date:
            dt = datetime.strptime(args.start_date, "%Y-%m-%d")
            start_local = (
                TORONTO_TZ.localize(dt) if dt.tzinfo is None else dt.astimezone(TORONTO_TZ)
            )
        elif last_date:
            if last_date.tzinfo is None:
                last_date = pytz.UTC.localize(last_date)
            prev_day = last_date.astimezone(TORONTO_TZ).date() - timedelta(days=1)
            start_local = TORONTO_TZ.localize(datetime.combine(prev_day, datetime.min.time()))
        else:
            start_local = PROJECT_BASELINE

        # Determine end_date
        if args.end_date:
            end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
            end_local = (
                TORONTO_TZ.localize(end_dt) if end_dt.tzinfo is None else end_dt.astimezone(TORONTO_TZ)
            )
        else:
            month = ((start_local.month - 1) // 3 + 1) * 3
            if month >= 12:
                next_quarter = TORONTO_TZ.localize(datetime(start_local.year + 1, 1, 1))
            else:
                next_quarter = TORONTO_TZ.localize(datetime(start_local.year, month + 1, 1))
            end_local = next_quarter - timedelta(days=1)

        if end_local < start_local:
            raise ValueError("End date cannot be before start date.")

        logger.info(f"Fetching incidents {start_local.date()} → {end_local.date()}")

        total_rows = 0
        curr = start_local
        while curr <= end_local:
            query_url = build_url(curr)
            data = fetch_with_retry(query_url)

            if data and "features" in data:
                logger.info(f"{curr.date()} → {len(data['features'])} records")

                # === Save raw API response to disk ===
                year = curr.strftime("%Y")
                month = curr.strftime("%m")
                day = curr.strftime("%d")
                out_dir = os.path.join("data", "raw", f"year={year}", f"month={month}", f"day={day}")
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, "incidents.json")
                with open(out_path, "w") as f_out:
                    json.dump(data, f_out)

                # === Upsert into DB ===                
                rows = upsert_raw_incidents(conn, data["features"])
                total_rows += rows
                conn.commit()
            else:
                logger.warning(f"{curr.date()} → no data")

            time.sleep(SLEEP_BETWEEN_CALLS)
            curr += timedelta(days=1)

        # Mark run as success
        log_run_end(conn, run_id, "success", row_count=total_rows)

    except Exception as e:
        try:
            if conn and conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                conn.rollback()
        except Exception as rollback_exc:
            logger.error(f"Error during rollback: {rollback_exc}")
        logger.error(f"Exception during main: {e}", exc_info=True)
        log_run_end(conn, run_id, "failure", error_message=str(e))
        raise
    finally:
        if conn:
            conn.close()


# Entry Point
# ========================
if __name__ == "__main__":
    main()
