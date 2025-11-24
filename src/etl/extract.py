import os
import re

import pandas as pd

from src.db_connection import DatabaseConnection

# Location of this file: project_root/src/etl/extract.py
CURRENT_DIR = os.path.dirname(__file__)  # src/etl
SRC_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))  # src
PROJECT_ROOT = os.path.abspath(os.path.join(SRC_DIR, ".."))  # project root

DATA_DIR = os.path.join(PROJECT_ROOT, "data")

PATH = "../data/"


def read_data(file_name) -> pd.DataFrame:
    """
    Read CSV file and drop exception incase if reading the file failed
    """

    full_path = os.path.join(DATA_DIR, f"{file_name}.csv")

    try:
        row_data = pd.read_csv(full_path, quotechar='"', low_memory=False)

        if row_data.empty:
            raise ValueError("The dataset is empty.")

        return row_data

    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_name} is not exist")
    except pd.errors.EmptyDataError:
        raise ValueError("the file is corupted or empty")
    except Exception as e:
        raise type(e)(f"Failed to read the dataframe{e}")


def normalize_dataframe_columns_to_lowercase(row_data: pd.DataFrame) -> pd.DataFrame:
    """
    Turn the columns name of the dataset to lowercase to avoid any mismatch with the database columns name
    """
    if row_data.shape[1] == 0:
        raise pd.errors.EmptyDataError("Dataframe is empty.")

    to_process = row_data.copy()
    to_process.columns = to_process.columns.str.strip().str.lower()
    return to_process


def process_row_data(row_data: pd.DataFrame, file_date: str) -> pd.DataFrame:
    """
    Process the row data that read from the source and add ingested_at date for incremental load and processing
    to avoid full scan / processing.
    """
    to_process = row_data.copy()

    to_process["ingested_at"] = file_date
    return to_process


def load_row_into_db(row_data: pd.DataFrame) -> int:
    """
    Load the read dataset from source as it as after add new column ( ingested_at ) for delta and incremental processing.
    Raise database exception incase of failure of storing the dataset.
    """

    try:
        with DatabaseConnection() as db_connection:
            return db_connection.load_dataframe_into_db(row_data, "bronze", "covid")

    except Exception as e:
        raise type(e)(
            f"An error happened during storing the dataset into the database {e}"
        )


def get_date_from_file_name(file_name: str) -> str:
    """
    Return the date of the file by processing the filename and extract the date from the filename.
    in date format of day-month-year
    """

    content = file_name.split(".")

    if len(content) != 2:
        raise ValueError("The file name not as expected DD-MM-YYYY")

    file_date = content[0]

    file_name = file_date + "." + content[1]

    pattern = r"^\d{2}-\d{2}-\d{4}\.csv$"

    if not re.match(pattern, file_name):
        raise ValueError("The file name format not as expected.")

    return file_date


def run_extraction(file_name) -> dict:
    """
    Organizing the extraction process by run all the functions in order
    """

    to_load_file_name = get_date_from_file_name(file_name)
    loaded_data = read_data(to_load_file_name)
    row_data = normalize_dataframe_columns_to_lowercase(loaded_data)
    row_data = process_row_data(row_data, to_load_file_name)
    total_inserted = load_row_into_db(row_data)

    # Extraction Information

    return {
        "file_name": file_name,
        "rows_read": len(row_data),
        "loaded_data": total_inserted,
        "status": "success",
    }
