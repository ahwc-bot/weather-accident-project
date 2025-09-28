-- DDL for storing TPS traffic incidents (raw ingest)

CREATE TABLE IF NOT EXISTS raw_incidents (
    event_id     TEXT PRIMARY KEY,                -- e.g. "GO-xxxx", stable key
    objectid     INTEGER,                         -- TPS internal record ID
    raw          JSONB,                           -- full TPS payload for audit/fallback

    -- Early extracted fields (for partitioning, joining, indexing)
    occ_date_utc TIMESTAMPTZ,                     -- UTC timestamp from TPS
    lat          NUMERIC(9,6),                    -- full precision latitude
    lon          NUMERIC(9,6),                    -- full precision longitude

    inserted_at  TIMESTAMPTZ DEFAULT now()
);

-- Index for time-based filtering
CREATE INDEX IF NOT EXISTS idx_raw_incidents_occ_date_utc
    ON raw_incidents (occ_date_utc);
