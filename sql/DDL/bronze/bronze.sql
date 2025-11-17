
CREATE SCHEMA IF NOT EXISTS bronze;


CREATE TABLE IF NOT EXISTS bronze.covid (
    FIPS                 NUMERIC(10,2),   -- float64
    Admin2               VARCHAR(255),    -- object (string)
    Province_State       VARCHAR(255),    -- object (string)
    Country_Region       VARCHAR(255),    -- object (string)
    Last_Update          TIMESTAMP,       -- date/time as text -> timestamp
    Lat                  DOUBLE PRECISION,
    Long_                DOUBLE PRECISION,
    Confirmed            INTEGER,
    Deaths               INTEGER,
    Recovered            INTEGER,
    Active               INTEGER,
    Combined_Key         VARCHAR(500),    -- string
    Incident_Rate        DOUBLE PRECISION,
    Case_Fatality_Ratio  DOUBLE PRECISION
    ingested_at          Date
);
