-- DDL for storing hourly weather observations (Open-Meteo)

-- Create with date_utc managed by trigger
CREATE TABLE weather_cache (
    lat           NUMERIC(8,5) NOT NULL,
    lon           NUMERIC(8,5) NOT NULL,
    hour_utc      TIMESTAMPTZ NOT NULL,
    date_utc      DATE, -- maintained by trigger
    temperature   REAL,
    precipitation REAL,
    snowfall      REAL,
    weathercode   INTEGER,
    windspeed     REAL,
    cloudcover    REAL,
    humidity      REAL,
    PRIMARY KEY (lat, lon, hour_utc)
);

-- Trigger function to auto-fill date_utc
CREATE OR REPLACE FUNCTION weather_cache_set_date()
RETURNS trigger AS $$
BEGIN
    NEW.date_utc := NEW.hour_utc::date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger
CREATE TRIGGER trg_set_date_utc
    BEFORE INSERT OR UPDATE ON weather_cache
    FOR EACH ROW
    EXECUTE FUNCTION weather_cache_set_date();

-- Indexes
-- 1. Date-level filtering
CREATE INDEX IF NOT EXISTS idx_weather_cache_date
    ON weather_cache (date_utc);

-- 2. Spatial filtering (optional, if you filter by lat/lon)
CREATE INDEX IF NOT EXISTS idx_weather_cache_lat_lon
    ON weather_cache (lat, lon);

-- 3. Time-based filtering
CREATE INDEX IF NOT EXISTS idx_weather_cache_hour
    ON weather_cache (hour_utc);
