import sys
from scripts import fetch_tps_incidents
from datetime import datetime, timedelta
import pytz


def test_upsert_raw_incidents_inserts_expected_rows(mock_tps_response, mock_db):
    conn, cursor = mock_db
    rows = fetch_tps_incidents.upsert_raw_incidents(conn, mock_tps_response["features"])
    # We have 3 incidents in the fixture
    assert rows == 3
    assert cursor.execute.call_count == 3
    # Check one row's SQL arguments
    args, kwargs = cursor.execute.call_args_list[0]
    assert args[1][0] is not None  # objectid
    assert args[1][4] is not None  # lat
    assert args[1][5] is not None  # lon


def test_upsert_raw_incidents_handles_missing_latlon(mock_db):
    conn, cursor = mock_db
    features = [
        {"attributes": {"OBJECTID": 99, "ACCNUM": "ACC-002"}, "geometry": {"x": None, "y": None}}
    ]
    rows = fetch_tps_incidents.upsert_raw_incidents(conn, features)
    assert rows == 1
    args, kwargs = cursor.execute.call_args
    # The lat/lon values in the SQL args should be None
    assert args[0].count("%s") == 6  # 6 parameters in query
    assert args[1][4] is None  # lat
    assert args[1][5] is None  # lon


def test_cli_entrypoint_runs(monkeypatch, mock_db, mock_tps_response):
    conn, cursor = mock_db

    # Mock DB connection
    monkeypatch.setattr(fetch_tps_incidents.psycopg2, "connect", lambda **_: conn)

    # Mock fetch_with_retry to return the fixed mock response
    monkeypatch.setattr(fetch_tps_incidents, "fetch_with_retry", lambda url: mock_tps_response)

    # Mock logging to suppress output
    monkeypatch.setattr(fetch_tps_incidents.logger, "info", lambda *args, **kwargs: None)
    monkeypatch.setattr(fetch_tps_incidents.logger, "warning", lambda *args, **kwargs: None)

    # Call main with args for a single day (force mode)
    sys.argv = [
        "fetch_tps_incidents.py",
        "--start-date", "2024-01-01",
        "--end-date", "2024-01-01",
        "--triggered-by", "test",
    ]
    fetch_tps_incidents.main()

    # cursor.execute should have been called 3 times (for 3 incidents)
    insert_calls = [
        call_args[0][0]
        for call_args in cursor.execute.call_args_list
        if "INSERT INTO raw_incidents" in call_args[0][0]
    ]
    assert len(insert_calls) == 3

    # conn.commit should be called at least once
    assert conn.commit.call_count >= 1
