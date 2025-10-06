import os
import pandas as pd
from scripts.fetch_tps_incidents import get_db_conn
from scripts.export_for_tableau import export_for_tableau

def test_export_for_tableau_basic(tmp_path):
    """Integration test to ensure export_for_tableau runs and writes CSV."""
    output_path = tmp_path / "incidents_test.csv"
    conn = get_db_conn()

    # Run a quick sanity check query first
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM dbt.fct_incidents_flat LIMIT 1;")
        result = cur.fetchone()
        assert result is not None, "fct_incidents_flat has no data"

    # Run export (this uses log_run_start/log_run_end internally)
    export_for_tableau(str(output_path), triggered_by="pytest")

    # Check file exists and structure
    assert output_path.exists(), "Export file was not created"

    df = pd.read_csv(output_path)
    assert "event_id" in df.columns, "event_id missing in export"
    assert len(df) > 0, "Exported file is empty"
