import os
import sys

from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../..")))
load_dotenv()  # loads .env file
import pytest
from sqlalchemy import create_engine, text

from src.etl.transform import run_transformation


@pytest.fixture()
def database_schema_setup():
    query = """
                DROP SCHEMA IF EXISTS BRONZE CASCADE;

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
                    Case_Fatality_Ratio  DOUBLE PRECISION,
                    ingested_at          Date
                );

                INSERT INTO bronze.covid (
                    FIPS, Admin2, Province_State, Country_Region, Last_Update,
                    Lat, Long_, Confirmed, Deaths, Recovered, Active,
                    Combined_Key, Incident_Rate, Case_Fatality_Ratio, ingested_at
                )
                VALUES
                (
                    NULL, NULL, NULL, 'Egypt', '2025-03-25 05:22:52',
                    33.93911, 67.709953, 52586, 2211, 41727, 8648,
                    'Egypt', 135.0841023510352, 4.204541132620849, '2025-03-25'
                ),
                (
                    NULL, NULL, NULL, 'KSA', '2025-03-25 05:22:52',
                    41.1533, 20.1683, 58991, 1190, 34353, 23448,
                    'KSA', 2049.8644798109667, 2.017256869691987, '2025-03-25'
                );
                DROP SCHEMA IF EXISTS silver CASCADE;

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


                
            """

    engin = engine = create_engine("postgresql://covid:covid@localhost:8082/covid")
    with engin.connect() as connection:
        connection.execute(text(query))
        connection.commit()

    yield


def test_transformation_script(database_schema_setup):
    """
    Integration testing for the extract script using docker compose
    """
    # Given a row data in csv format with name 25-03-2025

    os.environ["APP_ENV"] = "TEST"

    # When run extraction script
    results = run_transformation()

    # Then
    expected = {"loaded_data": 2, "rows_transformed": 2, "status": "success"}

    # os.environ['APP_ENV'] = 'DEV'
    assert results == expected
