-- Run this once against your Postgres instance before triggering the DAG.
-- Requires PostGIS if you want the geography column (optional, comment out if not installed).

CREATE TABLE IF NOT EXISTS usgs_earthquakes (
    id            TEXT        PRIMARY KEY,
    feature_type  TEXT,
    mag           FLOAT,
    place         TEXT,
    time          BIGINT,     -- Unix epoch milliseconds
    updated       BIGINT,     -- Unix epoch milliseconds
    tz            FLOAT,
    url           TEXT,
    detail        TEXT,
    felt          FLOAT,
    cdi           FLOAT,
    mmi           FLOAT,
    alert         TEXT,
    status        TEXT,
    tsunami       INT,
    sig           INT,
    net           TEXT,
    code          TEXT,
    ids           TEXT,
    sources       TEXT,
    types         TEXT,
    nst           FLOAT,
    dmin          FLOAT,
    rms           FLOAT,
    gap           FLOAT,
    mag_type      TEXT,
    event_type    TEXT,
    title         TEXT,
    longitude     FLOAT,
    latitude      FLOAT,
    depth_km      FLOAT
);

-- Optional: add a PostGIS geometry column for spatial queries in a GIS app.
-- Requires: CREATE EXTENSION IF NOT EXISTS postgis;
--
-- ALTER TABLE usgs_earthquakes
--     ADD COLUMN IF NOT EXISTS geom geography(Point, 4326)
--     GENERATED ALWAYS AS (
--         CASE
--             WHEN longitude IS NOT NULL AND latitude IS NOT NULL
--             THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
--             ELSE NULL
--         END
--     ) STORED;
--
-- CREATE INDEX IF NOT EXISTS idx_usgs_earthquakes_geom
--     ON usgs_earthquakes USING GIST (geom);

-- Useful indexes for a web app / LLM Text-to-SQL queries
CREATE INDEX IF NOT EXISTS idx_usgs_earthquakes_time  ON usgs_earthquakes (time);
CREATE INDEX IF NOT EXISTS idx_usgs_earthquakes_mag   ON usgs_earthquakes (mag);
CREATE INDEX IF NOT EXISTS idx_usgs_earthquakes_place ON usgs_earthquakes (place);
