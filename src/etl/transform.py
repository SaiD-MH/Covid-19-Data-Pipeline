# %%
import pandas as pd
from io import StringIO
import sys
import os
import csv
from sqlalchemy import create_engine,text
import numpy as np

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
from src.db_connection import DatabaseConnection



def read_row_data_from_bronze() -> pd.DataFrame :
    
    """
        Read the row data from the bronze layer and load it into a pandas dataframe for processing.
        Raise an error if can't load the table
    """
    try:
        query = """
                    WITH latest_ingestion AS (
                        SELECT MAX(ingested_at) as max_date 
                        FROM bronze.covid
                    )
                    SELECT * 
                    FROM bronze.covid b
                    CROSS JOIN latest_ingestion l
                    WHERE b.ingested_at = l.max_date;
                """
        with DatabaseConnection() as db_connection:
            row_data = db_connection.read_dataframe_from_db(query)
            row_data = row_data.drop(columns=['max_date'])
            return row_data
    except Exception as e:
        raise type(e) (f"Can't load the the data from database: {e}")


def drop_columns(df: pd.DataFrame , columns_list: list) -> pd.DataFrame:
    """
        Drop list of columns from the dataframe and raise an error if can't drop the columns.
    """
    try:
        return  df.drop(columns=columns_list)
        
    except Exception as e:
        raise type(e)(f"Can't drop the columns , not exists: {e}")



def drop_null_values(df: pd.DataFrame , columns_list: list) -> pd.DataFrame:
    """
        drop rows that have null values from the dataframe, taking list of columns to drop the rows if this column have null value.
    """
    try:
        return df.dropna(subset=columns_list)
    except Exception as e:
        raise type(e)(f"Can't drop the rows the columns not exists , {e}")


def drop_negative_values(df:pd.DataFrame , columns_list : list) -> pd.DataFrame:
    """
        drop rows that have negative values from the dataframe, taking list of columns to drop the rows if this column have negative value
    """
    try:
        
        mask = (df[columns_list]>=0).all(axis=1)
        
        return df[mask]
    except Exception as e:
        raise type(e) (f"Can't drop the row the columns not exists : {e}")



def format_locations (df: pd.DataFrame , columns: list) -> pd.DataFrame:
    """
        format the locations columns to be in title format to be in a standard form.
    """

    formated_df = df.copy()

    try:
        for col in columns:
            formated_df[col] = (
                formated_df[col].str.strip()
                                          .str.lower()
                                          .str.title()
            )

        return formated_df
    except Exception as e:
        raise type(e) (f"The columns not exists: {e}")

def standardize_null_values(df: pd.DataFrame, columns_list: list) -> pd.DataFrame:
    """Standardize null value representations."""
    df_clean = df.copy()
    null_representations = ['None', 'Unknown', '', ' ', 'N/A', 'null']
    
    for column in columns_list:
        df_clean[column] = df_clean[column].replace(null_representations, pd.NA)
    
    return df_clean

def load_cleansed_data_into_silver(cleansed_data: pd.DataFrame):
    
    """
        Storing the transformed data into the database.
    """
    try:
        with DatabaseConnection() as db_connection:
            db_connection.load_dataframe_into_db(cleansed_data,"silver","covid")
    except Exception as e:
        raise type(e)(f"Can't load the transformed data into the database: {e}")



def run_transformation()-> dict:

    """
        Combine all the transformation logic to make it easy for orchestration.
        and return dictionary with information about no.loaded , tranformed, stored and status.
    """


    row_data = read_row_data_from_bronze()
    columns=["fips" , "admin2" ,"combined_key" , "last_update",'lat','long_'] 
    transformed_data = drop_columns(row_data , columns)



    columns=("country_region" , "confirmed" , "deaths","recovered","active","incident_rate" , "case_fatality_ratio")
    transformed_data = drop_null_values(transformed_data , columns)

    
    columns = ["deaths" , "confirmed" , "recovered" , "active" , "incident_rate"]
    transformed_data = drop_negative_values(transformed_data , columns)

    columns_locations = ["province_state" , "country_region"]
    transformed_data = format_locations(transformed_data , columns_locations)


    columns_list = ['province_state']
    transformed_data = standardize_null_values(transformed_data , columns_list)


    load_cleansed_data_into_silver(transformed_data)

    return {
        "loaded_data" :len(row_data),
        "rows_transformed" : len(transformed_data),
        "status" :"success"
    }


run_transformation()
