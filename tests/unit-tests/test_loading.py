import os
import sys
import uuid

import pandas as pd
import pytest

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../..")))
from datetime import date
from unittest.mock import MagicMock, patch

from pandas.testing import assert_frame_equal

from src.etl.load import fill_date_dim_table, fill_fact_table, fill_region_dim


@pytest.fixture
def expected_date_record():
    return pd.DataFrame(
        [
            {
                "date_key": 20210101,
                "full_date": date(2021, 1, 1),
                "day_of_week": 5,
                "day_of_month": 1,
                "day_name": "Friday",
                "week_of_year": 53,
                "month": 1,
                "month_name": "January",
                "quarter": 1,
                "year": 2021,
                "is_weekend": False,
            }
        ]
    )


@pytest.fixture
def cleansed_data():
    return pd.DataFrame(
        [
            {
                "province_state": None,
                "country_region": "Egypt",
                "confirmed": 2,
                "deaths": 3,
                "recovered": 4,
                "active": 5,
                "incident_rate": 134.8,
                "case_fatality_ratio": 4.19,
                "ingested_at": "2021-01-01",
            }
        ]
    )


@pytest.fixture
def all_regions():
    return pd.DataFrame(
        {
            "region_key": ["11111111-1111-1111-1111-111111111111"],
            "province_state": [None],
            "country_region": ["Egypt"],
        }
    )


@pytest.fixture
def mock_db_connection():
    db_mock = MagicMock()
    return db_mock


def test_fill_date_dim_when_record_not_exists_in_db(
    mock_db_connection, cleansed_data, expected_date_record
):
    """
    Given a cleansed dataset and and mock to fake the db call that return empty df from db
    When calling fill date dim
    Then should return a valid date_dim from ingested_at column in the cleansed_dataset
    """

    mock_db_connection.read_dataframe_from_db.return_value = pd.DataFrame()

    result = fill_date_dim_table(cleansed_data, mock_db_connection)

    assert_frame_equal(
        expected_date_record.reset_index(drop=True),
        result.reset_index(drop=True),
        check_dtype=False,
    )


def test_fill_date_dim_when_record_exists_in_db(
    mock_db_connection, cleansed_data, expected_date_record
):
    """
    Given a cleansed dataset and and mock to fake the db call that a record from database
    When calling fill date dim
    Then should return an empty date_dim dataframe
    """

    mock_db_connection.read_dataframe_from_db.return_value = expected_date_record

    result = fill_date_dim_table(cleansed_data, mock_db_connection)
    true_result = pd.DataFrame(
        columns=[
            "date_key",
            "full_date",
            "day_of_week",
            "day_of_month",
            "day_name",
            "week_of_year",
            "month",
            "month_name",
            "quarter",
            "year",
            "is_weekend",
        ]
    )
    assert_frame_equal(
        true_result.reset_index(drop=True),
        result.reset_index(drop=True),
        check_dtype=False,
    )


def test_fill_region_dim_when_no_values_in_db(mock_db_connection, cleansed_data):
    # Given a new record from cleansed date
    fixed_uuid = str(uuid.UUID("11111111-1111-1111-1111-111111111111"))

    # When the database have no values regions stored yet
    mock_db_connection.read_dataframe_from_db.return_value = pd.DataFrame(
        {"region_key": [], "province_state": [], "country_region": []}
    )
    with patch("src.etl.load.uuid.uuid4", return_value=fixed_uuid):
        result = fill_region_dim(cleansed_data, mock_db_connection)

    # then

    expected = pd.DataFrame(
        {
            "province_state": [None],
            "country_region": ["Egypt"],
            "region_key": [fixed_uuid],
        }
    )

    assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_fill_region_dim_when_have_values_in_db(mock_db_connection, cleansed_data):
    # Given a new record from cleansed date
    fixed_uuid = str(uuid.UUID("11111111-1111-1111-1111-111111111111"))

    # When the database have  values regions stored yet
    mock_db_connection.read_dataframe_from_db.return_value = pd.DataFrame(
        {
            "region_key": [fixed_uuid],
            "province_state": [None],
            "country_region": ["Egypt"],
        }
    )
    with patch("src.etl.load.uuid.uuid4", return_value=fixed_uuid):
        result = fill_region_dim(cleansed_data, mock_db_connection)

    # Then deleta regions to add should be empty

    expected = pd.DataFrame(
        {"province_state": [], "country_region": [], "region_key": []}
    )

    assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


def test_fill_fact_table(
    mock_db_connection, cleansed_data, expected_date_record, all_regions
):
    # Given a new record from cleansed date
    fixed_uuid = str(uuid.UUID("11111111-1111-1111-1111-111111111111"))

    # When the database have  values regions stored yet
    mock_db_connection.read_dataframe_from_db.return_value = all_regions

    result = fill_fact_table(cleansed_data, expected_date_record, mock_db_connection)

    # Then deleta regions to add should be empty

    expected = cleansed_data[
        [
            "confirmed",
            "deaths",
            "recovered",
            "active",
            "incident_rate",
            "case_fatality_ratio",
        ]
    ]
    expected["region_key"] = fixed_uuid
    expected["date_key"] = 20210101

    assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )
