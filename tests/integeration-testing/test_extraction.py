import os
import sys

from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../..")))
load_dotenv()  # loads .env file
import pytest
from sqlalchemy import create_engine, text

from src.etl.extract import run_extraction


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
                
            """

    engin = engine = create_engine("postgresql://covid:covid@localhost:8082/covid")
    with engin.connect() as connection:
        connection.execute(text(query))
        connection.commit()
        connection.close()


def test_extraction_script(database_schema_setup):
    """
    Integration testing for the extract script using docker compose
    """
    # Given a row data in csv format with name 25-03-2025

    os.environ["APP_ENV"] = "TEST"

    # When run extraction script
    results = run_extraction("03-25-2025.csv")

    # Then
    expected = {
        "file_name": "03-25-2025.csv",
        "rows_read": 2,
        "loaded_data": 2,
        "status": "success",
    }

    # os.environ['APP_ENV'] = 'DEV'
    assert results == expected
