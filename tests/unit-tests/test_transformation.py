import os
import sys

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "../..")))
from src.etl.transform import (
    drop_columns,
    drop_negative_values,
    drop_null_values,
    format_locations,
    standardize_null_values,
)


@pytest.fixture
def dataframe():
    return pd.DataFrame(
        {"col1": ["1", "2", "3"], "col2": ["A", "B", "C"], "col3": [False, True, False]}
    )


@pytest.fixture
def null_df():
    return pd.DataFrame(
        {"col1": ["1", None, "3"], "col2": ["A", "B", "C"], "col3": [False, True, None]}
    )


def test_drop_columns(dataframe):
    """
    Given a dataframe and columns to drop
    When calling drop_columns()
    Then the columns are droped from the dataset
    """
    # When
    columns_to_drop = ["col1", "col3"]
    result = list(drop_columns(dataframe, columns_to_drop).columns)

    expected = ["col2"]

    assert expected == result


def test_drop_columns_from_empty_dataframe():
    """
    Given an empty dataframe and columns to drop
    When calling drop_columns()
    Then the should raise an exception
    """

    # Given
    empty_df = pd.DataFrame()

    # When

    with pytest.raises(ValueError):
        columns_to_drop = ["col1", "col2"]
        result = drop_columns(empty_df, columns_to_drop)


def test_drop_columns_when_columns_not_exists(dataframe):
    """
    Given a dataframe and columns not exists to drop
    When calling drop_columns()
    Then will be a raise
    """

    # When & Then
    with pytest.raises(KeyError):
        columns_to_drop = ["X", "Y"]
        result = list(drop_columns(dataframe, columns_to_drop).columns)


def test_drop_null_values(null_df):
    # Given df with null values
    columns_to_check = ["col1", "col2", "col3"]

    # When
    result = drop_null_values(null_df, columns_to_check)

    # Then

    expected = pd.DataFrame({"col1": ["1"], "col2": ["A"], "col3": [False]})

    # assert expected == result
    assert len(result) == 1
    assert_frame_equal(expected, result, check_dtype=False)


def test_drop_null_values_when_columns_not_exists(null_df):
    # Given df with null values
    columns_to_check = ["random_col1"]

    with pytest.raises(KeyError):
        # When
        result = drop_null_values(null_df, columns_to_check)


@pytest.mark.parametrize(
    "df , expected",
    [
        (
            pd.DataFrame({"c1": [1, 1, 1], "c2": [-2, 2, 2], "c3": [3, 3, -3]}),
            pd.DataFrame({"c1": [1], "c2": [2], "c3": [3]}),
        ),  # Minx Pos and Neg
        (
            pd.DataFrame({"c1": [1, 1, 1], "c2": [2, 2, 2], "c3": [3, 3, 3]}),
            pd.DataFrame({"c1": [1, 1, 1], "c2": [2, 2, 2], "c3": [3, 3, 3]}),
        ),  # Postive Only
        (
            pd.DataFrame({"c1": [-1, -1, -1], "c2": [-2, -2, -2], "c3": [-3, -3, -3]}),
            pd.DataFrame({"c1": [], "c2": [], "c3": []}),
        ),  # Neg only
    ],
)
def test_drop_negtive_values(df, expected):
    result = drop_negative_values(df, ["c1", "c2", "c3"])

    assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "df , expected",
    [
        (
            pd.DataFrame({"c1": ["dATa"], "c2": ["DATA"], "c3": ["data"]}),
            pd.DataFrame({"c1": ["Data"], "c2": ["Data"], "c3": ["Data"]}),
        ),
        (
            pd.DataFrame(
                {"c1": ["Data,data"], "c2": ["  DatA   "], "c3": ["data data   "]}
            ),
            pd.DataFrame({"c1": ["Data,Data"], "c2": ["Data"], "c3": ["Data Data"]}),
        ),
    ],
)
def test_formate_locations_columns(df, expected):
    result = format_locations(df, ["c1", "c2", "c3"])
    assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    "df , expected",
    [
        (
            pd.DataFrame({"c1": ["data", "None"], "c2": ["N/A", "null"]}),
            pd.DataFrame({"c1": ["data", "None"], "c2": ["None", "None"]}),
        ),
        (
            pd.DataFrame({"c1": ["Unknown", ""], "c2": ["value", "None"]}),
            pd.DataFrame({"c1": ["None", "None"], "c2": ["value", "None"]}),
        ),
    ],
)
def test_standerlized_null_values(df, expected):
    result = standardize_null_values(df, ["c1", "c2"])

    assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True),
        check_dtype=False,
    )
