import pytest

import pandera as pa
import pandas as pd

from dagster_pandera import pandera_schema_to_dagster_type
from dagster import check_dagster_type, DagsterType, TypeCheck

from modin.pandas import DataFrame as ModinDataFrame
from databricks.koalas import DataFrame as KoalasDataFrame

# @pytest.fixture(params=[pd.DataFrame, ModinDataFrame, KoalasDataFrame])
# def dataframe(request):
#     df_cls = request.param
#     return df_cls({
#         "a": [1, 4, 0, 10, 9],
#         "b": [-1.3, -1.4, -2.9, -10.1, -20.4],
#         "c": ["value_1", "value_2", "value_3", "value_2", "value_1"],
#     })

@pytest.fixture
def dataframe():
    df_cls = pd.DataFrame
    return df_cls({
        "a": [1, 4, 0, 10, 9],
        "b": [-1.3, -1.4, -2.9, -10.1, -20.4],
        "c": ["value_1", "value_2", "value_3", "value_2", "value_1"],
    })

@pytest.fixture
def schema():
    return pa.DataFrameSchema({
        "column1": pa.Column(int, checks=pa.Check.le(10)),
        "column2": pa.Column(float, checks=pa.Check.lt(-1.2)),
        "column3": pa.Column(str, checks=[
            pa.Check.str_startswith("value_"),
            # define custom checks as functions that take a series as input and
            # outputs a boolean or boolean Series
            pa.Check(lambda s: s.str.split("_", expand=True).shape[1] == 2)
        ]),
    })

@pytest.fixture
def dagster_type(schema):
    return pandera_schema_to_dagster_type(schema)

def test_pandera_schema_to_dagster_type(schema):
    dagster_type = pandera_schema_to_dagster_type(schema)
    assert(isinstance(dagster_type, DagsterType))

def test_validate_valid_dataframe(dagster_type, dataframe):
    result = check_dagster_type(dagster_type, dataframe)
    assert isinstance(result, TypeCheck)
    assert result.success
