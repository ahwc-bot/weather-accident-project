import psycopg2, uuid, datetime
from datetime import UTC

def log_run_start(conn, pipeline_name, triggered_by="manual"):
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO run_log (run_id, pipeline_name, status, start_time, triggered_by)
            VALUES (%s, %s, 'running', %s, %s)
        """, (run_id, pipeline_name, datetime.datetime.now(UTC), triggered_by))
    conn.commit()
    return run_id

def log_run_end(conn, run_id, status, row_count=0, error_message=None):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE run_log
            SET end_time = %s,
                status = %s,
                row_count = %s,
                error_message = %s
            WHERE run_id = %s
            RETURNING duration
        """, (datetime.datetime.now(UTC), status, row_count, error_message, run_id))
        duration = cur.fetchone()[0]   # duration will be an interval or None
    conn.commit()
    return duration
