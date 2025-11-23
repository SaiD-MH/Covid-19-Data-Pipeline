
import os
import sys
from datetime import datetime
# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from src.db_connection import DatabaseConnection
import uuid
import pandas as pd





def read_cleansed_data_from_silver(db_connection:DatabaseConnection) -> pd.DataFrame:

    """
        Read the cleansed data from the silver layer to fill dim & fact tables.
    
    """
    try:
        query = """
                    WITH latest_ingestion AS (
                        SELECT MAX(ingested_at) as max_date 
                        FROM silver.covid
                    )
                    SELECT * 
                    FROM silver.covid s
                    CROSS JOIN latest_ingestion l
                    WHERE s.ingested_at = l.max_date;
                """
        
        return db_connection.read_dataframe_from_db(query)
    except Exception as e:
        raise type(e) (f"Can't load the data from database: {e}")



def fill_date_dim_table(cleansed_data: pd.DataFrame , db_connection:DatabaseConnection) -> pd.DataFrame:
    """
    Fill date dimension table for the ingestion date.
    
    Returns:
        DataFrame with single date dimension record
    """
    # Parse date from string if needed
    ingested_date = pd.to_datetime(cleansed_data["ingested_at"].max())
    date_key = int(ingested_date.strftime('%Y%m%d'))

    date_exists = db_connection.read_dataframe_from_db(f"SELECT date_key from gold.date_dim where date_key = {date_key} ")
    if not date_exists.empty:
        # Date exists, return empty DataFrame with correct structure
        return pd.DataFrame(columns=[
            'date_key', 'full_date', 'day_of_week', 'day_of_month',
            'day_name', 'week_of_year', 'month', 'month_name',
            'quarter', 'year', 'is_weekend'
        ])



    # Extract date components
    iso_cal = ingested_date.isocalendar()
    
    date_record = {
        "date_key": int(ingested_date.strftime('%Y%m%d')),  # 20250117
        "full_date": ingested_date.date(),
        "day_of_week": ingested_date.weekday() + 1,  # Monday=1, Sunday=7
        "day_of_month": ingested_date.day,
        "day_name": ingested_date.strftime("%A"),
        "week_of_year": iso_cal.week,
        "month": ingested_date.month,
        "month_name": ingested_date.strftime("%B"),
        "quarter": (ingested_date.month - 1) // 3 + 1,
        "year": ingested_date.year,
        "is_weekend": ingested_date.weekday() >= 5  # Sat=5, Sun=6
    }
    
    return pd.DataFrame([date_record])

def fill_region_dim(cleansed_data:pd.DataFrame , db_connection:DatabaseConnection )  -> pd.DataFrame :
    
    """
        fill region dimension table
    """
    
    stored_regions = db_connection.read_dataframe_from_db("SELECT * FROM gold.region_dim;")
    
    new_regions = cleansed_data[['province_state' , 'country_region']]
    new_regions =  new_regions.drop_duplicates(subset=('province_state' , 'country_region'))
    

    delta_regions = new_regions.merge(stored_regions[['country_region', 'province_state']] ,on=['country_region', 'province_state'], how='left' ,indicator=True )
    delta_regions = delta_regions[delta_regions['_merge'] =='left_only']
    delta_regions = delta_regions.drop(columns=['_merge'])

    if delta_regions.empty:
        # Return empty DataFrame with correct structure
        return pd.DataFrame(columns=['province_state', 'country_region', 'region_key'])
    

    delta_regions['region_key'] = [str(uuid.uuid4()) for _ in range(len(delta_regions))]


    return delta_regions



def load_all_dimension(dims_tables: list , db_connection:DatabaseConnection) ->list:
    """
        Load all dimensions into the gold schema.
    """

    try:
        inserted = []
        for dim in dims_tables: 
            total_loaded = db_connection.load_dataframe_into_db(dim["table"],'gold',dim["name"])
            inserted.append(total_loaded)
        return inserted
    except pd.errors.DatabaseError as e:
        raise pd.errors.DatabaseError(f"Database error can't insert the dim table:{e}")    


def fill_fact_table(cleaned_data:pd.DataFrame ,date_dim:pd.DataFrame , db_connection:DatabaseConnection) -> pd.DataFrame:
    """
        fill fact table

    """
    all_regions = db_connection.read_dataframe_from_db("SELECT * FROM gold.region_dim;")

    cleaned_data = cleaned_data.merge(all_regions , how='left' , on=['province_state','country_region'])
    
    
    fact_data = cleaned_data[['confirmed' , 'deaths','recovered' ,'active' , 'incident_rate','case_fatality_ratio' , 'region_key']]
    
    if not date_dim.empty:
        fact_data['date_key'] = date_dim.loc[0,'date_key']
    else:
        ingested_date = pd.to_datetime(cleaned_data["ingested_at"].max())
        fact_data['date_key'] = int(ingested_date.strftime('%Y%m%d'))



    return fact_data



def load_fact_table(fact_data: pd.DataFrame , db_connection:DatabaseConnection) -> int:
    """
        Load the fact table to the database.
    """

    try:
        total_inserted = db_connection.load_dataframe_into_db(fact_data , 'gold' , 'fact')
        return total_inserted
    except pd.errors.DatabaseError as e:
        raise pd.errors.DatabaseError(f"Can't load the fact table:{e}")


def run_load()-> dict:


    """
        Combine all the loading logic to make it easy for orchestration.
    """

    with DatabaseConnection() as db_connection:
        cleansed_data = read_cleansed_data_from_silver(db_connection)

        date_dim = fill_date_dim_table(cleansed_data, db_connection)
        region_dim = fill_region_dim(cleansed_data , db_connection)


        dimension_list = [{"table" : date_dim , "name" :"date_dim"} , {"table" : region_dim , "name" :"region_dim"}]
        total_inserted = load_all_dimension(dimension_list , db_connection)


        fact_data = fill_fact_table(cleansed_data , date_dim , db_connection)

        fact_count = load_fact_table(fact_data , db_connection)

        return {

            "date_dim_records" : total_inserted[0],
            "region_dim_records":total_inserted[1],
            "fact_records":fact_count

        }