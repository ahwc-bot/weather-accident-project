import os
import sys
import logging
import argparse
import pandas as pd
import psycopg2

from scripts.fetch_tps_incidents import get_db_conn
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
# Main Export Function
# ========================
def export_for_tableau(output_path: str, triggered_by: str = "manual"):
    """Query fct_incidents_flat and export to CSV for Tableau."""
    conn = get_db_conn()
    run_id = log_run_start(conn, "export_for_tableau", triggered_by)

    try:
        logger.info("Querying fct_incidents_flat...")
        query = "SELECT * FROM dbt.fct_incidents_flat;"
        df = pd.read_sql_query(query, conn)
        logger.info(f"Fetched {len(df):,} rows.")

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Exported data to {output_path}")

        log_run_end(conn, run_id, "success", row_count=len(df))
    except Exception as e:
        logger.error(f"Error during export: {e}", exc_info=True)
        log_run_end(conn, run_id, "failure", error_message=str(e))
        raise
    finally:
        conn.close()


# ========================
# CLI Entry Point
# ========================
def main():
    parser = argparse.ArgumentParser(description="Export fct_incidents_flat for Tableau.")
    parser.add_argument(
        "--output",
        type=str,
        default="data/export/incidents.csv",
        help="Path for the exported CSV file (default: data/export/incidents.csv)",
    )
    parser.add_argument(
        "--triggered-by",
        type=str,
        default="manual",
        help="Source of trigger (manual, airflow, etc.)",
    )
    args = parser.parse_args()

    export_for_tableau(args.output, args.triggered_by)


if __name__ == "__main__":
    main()
