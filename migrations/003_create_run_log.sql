-- DDL for pipeline run logging

CREATE TABLE IF NOT EXISTS run_log (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    pipeline_name TEXT NOT NULL,      -- e.g. 'tps_ingest' or 'weather_ingest'
    start_time    TIMESTAMPTZ DEFAULT now(),
    end_time      TIMESTAMPTZ,
    duration INTERVAL GENERATED ALWAYS AS (end_time - start_time) STORED,

    status TEXT DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'failure')),

    row_count     INT,
    triggered_by  TEXT DEFAULT 'manual',   -- 'airflow', 'manual', etc.
    error_message TEXT
);

-- Optional convenience index: quickly find latest runs per pipeline
CREATE INDEX IF NOT EXISTS idx_run_log_pipeline_time
    ON run_log (pipeline_name, start_time DESC);
