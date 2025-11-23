
from dotenv import load_dotenv
import os,sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))
load_dotenv()  # loads .env file
from sqlalchemy import create_engine , text
import pytest
from src.etl.load import run_load

@pytest.fixture()
def database_schema_setup():

    query = """
              
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

                DROP SCHEMA IF EXISTS gold CASCADE;
                CREATE SCHEMA IF NOT EXISTS gold;

                CREATE TABLE IF NOT EXISTS gold.region_dim (

                region_key  varchar(100) primary key,
                country_region varchar(200) not null,
                province_state varchar(200) null ,
                CONSTRAINT country_province UNIQUE (country_region , province_state)         
                );

                CREATE TABLE IF NOT EXISTS gold.date_dim(


                date_key int primary key ,
                full_date date not null ,
                day_of_week int not null,
                day_of_month int not null,
                day_name varchar(10) not null,
                week_of_year int not null,
                month int not null,
                month_name varchar(10) not null , 
                quarter int not null , 
                year int not null,
                is_weekend boolean not null 
            );

            CREATE TABLE IF NOT EXISTS gold.fact(

                confirmed bigint not null ,
                deaths bigint not null ,
                active bigint not null,
                recovered bigint not null,
                incident_rate float not null,
                case_fatality_ratio float not null, 
                region_key VARCHAR(100),
                date_key int not null,
                FOREIGN KEY (region_key) REFERENCES gold.region_dim(region_key),
                FOREIGN KEY (date_key) REFERENCES gold.date_dim(date_key)

            );
            INSERT INTO silver.covid (
                
                Country_Region,
                Confirmed,
                Deaths,
                Recovered,
                Active,
                Incident_Rate,
                Case_Fatality_Ratio,
                ingested_at
            )
            VALUES
            (
                 
                'Afghanistan',
                52513,
                2201,
                41727,
                8585,
                134.89657830525067,
                4.191343095995277,
                '2021-01-01'
            ),
            (
                
                'Albania',
                58316,
                1181,
                33634,
                23501,
                2026.409062478282,
                2.025173194320598,
                '2021-01-01'
            );

                
            """
    
    engin = engine = create_engine("postgresql://covid:covid@localhost:8082/covid")
    with engin.connect() as connection:
        
        connection.execute(text(query))
        connection.commit()
    
    yield



def test_loading_script(database_schema_setup):
    
    """
        Integration testing for the extract script using docker compose 
    """
    # Given a row data in csv format with name 25-03-2025

    os.environ['APP_ENV'] = 'TEST'

    # When run extraction script
    results = run_load()

    #Then 
    expected = {
        "date_dim_records" :1,
        "region_dim_records" : 2,
        "fact_records" :2
    }
    
    #os.environ['APP_ENV'] = 'DEV'
    assert results == expected





