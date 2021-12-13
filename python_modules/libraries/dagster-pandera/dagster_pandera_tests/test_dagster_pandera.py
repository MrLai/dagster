import pytest

from dagster_pandera import pandera_schema_to_dagster_type


def test_pandera_schema_to_dagster_type():
    
    assert pandera_schema_to_dagster_type({"type": "string"}) == "String"
    assert pandera_schema_to_dagster_type({"type": "integer"}) == "Int"
    assert pandera_schema_to_dagster_type({"type": "number"}) == "Float"
    assert pandera_schema_to_dagster_type({"type": "boolean"}) == "Bool"
