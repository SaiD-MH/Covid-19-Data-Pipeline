import sys
import os
from dateutil.parser import ParserError

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))
from src.etl.extract import (normalize_dataframe_columns_to_lowercase , get_date_from_file_name , process_row_data)
import pandas as pd
import sqlalchemy
import pytest

# Given 
@pytest.fixture
def dataset():
    df = {

        "Country" : ["EGY" , "KSA" , "USA"],
        "CASES" : [231,45,34321]
    }

    return pd.DataFrame(df)

@pytest.fixture
def file_name():
    return "01-01-2025.csv"

def test_normalize_dataframe_columns_to_lowercase(dataset):
    """
        Give a dataset 
        when performing standard column normalization for name 
        then return normlized columns name in a small cases
    """
    #When
    result = normalize_dataframe_columns_to_lowercase(dataset)
    
    
    expected = ["country" , "cases"]

    #Then
    assert expected == list(result.columns)

def test_normalize_empty_dataframe_columns_to_lowercase():
    """
        Give an empty dataset 
        when performing standard column normalization for name 
        then should rasie exception it's empty
    """
    #given 

    empty_dataframe = pd.DataFrame()


    #When & Then
    with pytest.raises(pd.errors.EmptyDataError ):
        normalize_dataframe_columns_to_lowercase(empty_dataframe)


def test_normalize_dataframe_columns_with_spaces_to_lowercase():
    """
        Give a dataset with spaces in columns name
        when performing standard column normalization for name 
        then return normlized columns name in a small cases
    """
    #given
    df = {

        "  Country " : ["EGY" , "KSA" , "USA"],
        " CASES" : [231,45,34321],
        "cOvEreD " : [231,45,34321]
    }


    #when
    result = normalize_dataframe_columns_to_lowercase(pd.DataFrame([df]))

    #Then

    expected = ['country' , 'cases' , 'covered']    

    assert expected == list(result.columns)


def test_get_date_from_file_name(file_name):

    """
    Given the file name 
    When performing getting date from file name
    Then return the date part from the file name
    """

    #When
    result = get_date_from_file_name(file_name)

    #Then
    excepted = "01-01-2025"
    assert result == excepted


def test_get_date_from_file_name_with_different_date_format():

    """
    Given the file name 
    When performing getting date from file name
    Then return the date part from the file name
    """

    # Given
    file_name = "12-30-2025.csv"

    #When
    result = get_date_from_file_name(file_name)

    #Then
    excepted = "12-30-2025"
    assert result == excepted



def test_get_date_from_file_name_with_invalid_format_special_char():

    """
    Given the file name 
    When performing getting date from file name
    Then return the date part from the file name
    """

    #When

    #Then
    with pytest.raises(ValueError):
        result = get_date_from_file_name("01-01|2025.csv")
        
def test_get_date_from_file_name_with_invalid_format_different_extention():

    """
    Given the file name 
    When performing getting date from file name
    Then return the date part from the file name
    """

    #When

    #Then
    with pytest.raises(ValueError):
        result = get_date_from_file_name("01-01-2025.pdf")

def test_get_date_from_file_name_with_multiple_dots():

    """
    Given the file name 
    When performing getting date from file name
    Then return the date part from the file name
    """

    #When

    #Then
    with pytest.raises(ValueError):
        result = get_date_from_file_name("01-01-2025....csv")
        




def test_add_ingested_date_column_to_row_data(dataset):
    """
        Give a dataset
        When performing action to add ingested_at date column
        Then returning dataframe has an ingested_at column contain the date
    """

    #When
    df1 = process_row_data(dataset , "01-01-2025")

    #Then
    assert df1.loc[0 , 'ingested_at'] == '01-01-2025'