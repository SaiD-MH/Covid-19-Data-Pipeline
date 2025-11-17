CREATE SCHEMA IF NOT EXISTS silver;



CREATE TABLE IF NOT EXISTS silver.covid (
    
    Province_State       VARCHAR(255) NULL,    -- object (string)
    Country_Region       VARCHAR(255) NOT NULL,    -- object (string)
    Confirmed            INTEGER NOT NULL,
    Deaths               INTEGER NOT NULL,
    Recovered            INTEGER NOT NULL,
    Active               INTEGER NOT NULL,
    Incident_Rate        DOUBLE PRECISION NOT NULL,
    Case_Fatality_Ratio  DOUBLE PRECISION NOT NULL ,
    ingested_at          Date NOT NULL
);
