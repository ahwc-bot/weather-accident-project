import sys
from datetime import date
from scripts import build_weather_cache


def test_upsert_weather_cache_inserts_expected_rows(mock_weather_response, mock_db):
    conn, cursor = mock_db
    lat, lon = 43.61, -79.56
    rows = build_weather_cache.upsert_weather_cache(conn, lat, lon, mock_weather_response)
    assert rows == 24  # 24 hourly rows expected
    assert cursor.execute.call_count == 24
    # Check one row's SQL arguments
    args, kwargs = cursor.execute.call_args_list[0]
    assert round(args[1][0], 2) == round(lat, 2)
    assert round(args[1][1], 2) == round(lon, 2)


def test_cli_force_mode_runs(monkeypatch, mock_db, mock_weather_response):
    conn, cursor = mock_db

    # Patch DB connection
    monkeypatch.setattr(build_weather_cache.psycopg2, "connect", lambda **_: conn)
    # Patch API call to return mock weather
    monkeypatch.setattr(build_weather_cache, "fetch_with_retry", lambda url: mock_weather_response)
    # Patch logging to suppress output
    monkeypatch.setattr(build_weather_cache.logger, "info", lambda *a, **kw: None)
    monkeypatch.setattr(build_weather_cache.logger, "warning", lambda *a, **kw: None)

    # Call CLI in force mode
    sys.argv = [
        "build_weather_cache.py",
        "--lat", "43.61",
        "--lon", "-79.56",
        "--date", "2024-01-01",
        "--triggered-by", "test",
    ]
    build_weather_cache.main()

    # Should insert 24 rows
    insert_calls = [
        call_args[0][0]
        for call_args in cursor.execute.call_args_list
        if "INSERT INTO weather_cache" in call_args[0][0]
    ]
    assert len(insert_calls) == 24
    # Commit should be called at least once
    assert conn.commit.call_count >= 1



def test_bulk_mode_runs(monkeypatch, mock_db, mock_weather_response):
    conn, cursor = mock_db

    # Patch DB connection
    monkeypatch.setattr(build_weather_cache.psycopg2, "connect", lambda **_: conn)
    # Patch API call
    monkeypatch.setattr(build_weather_cache, "fetch_with_retry", lambda url: mock_weather_response)
    # Patch find_missing_ranges to return one triple
    monkeypatch.setattr(build_weather_cache, "find_missing_ranges", lambda conn: [(43.61, -79.56, date(2024, 1, 1), date(2024, 1, 1))])
    # Patch logging
    monkeypatch.setattr(build_weather_cache.logger, "info", lambda *a, **kw: None)
    monkeypatch.setattr(build_weather_cache.logger, "warning", lambda *a, **kw: None)

    # Call CLI in bulk mode (no --lat/--lon/--date)
    sys.argv = ["build_weather_cache.py", "--triggered-by", "test"]
    build_weather_cache.main()

    # Should insert 24 rows for the one missing triple
    insert_calls = [
        call_args[0][0]
        for call_args in cursor.execute.call_args_list
        if "INSERT INTO weather_cache" in call_args[0][0]
    ]
    assert len(insert_calls) == 24
    assert conn.commit.call_count >= 1
