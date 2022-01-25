import pytest
from dagster import (
    AssetMaterialization,
    AssetObservation,
    DagsterEventType,
    EventMetadata,
    FloatMetadataEntryData,
    IntMetadataEntryData,
    PythonArtifactMetadataEntryData,
    TextMetadataEntryData,
    UrlMetadataEntryData,
    execute_pipeline,
    pipeline,
    solid,
)
from dagster.check import CheckError
from dagster.core.definitions.event_metadata import DagsterInvalidEventMetadata, EventMetadataEntry
from dagster.core.definitions.event_metadata.table import (
    TableColumn,
    TableColumnConstraints,
    TableConstraints,
    TableRecord,
    TableSchema,
)
from dagster.utils import frozendict


def solid_events_for_type(result, solid_name, event_type):
    solid_result = result.result_for_solid(solid_name)
    return [
        compute_step_event
        for compute_step_event in solid_result.compute_step_events
        if compute_step_event.event_type == event_type
    ]


def test_event_metadata_asset_materialization():
    @solid(output_defs=[])
    def the_solid(_context):
        yield AssetMaterialization(
            asset_key="foo",
            metadata={
                "text": "FOO",
                "int": 22,
                "url": EventMetadata.url("http://fake.com"),
                "float": 0.1,
                "python": EventMetadata.python_artifact(EventMetadata),
            },
        )

    @pipeline
    def the_pipeline():
        the_solid()

    result = execute_pipeline(the_pipeline)

    assert result
    assert result.success

    materialization_events = solid_events_for_type(
        result, "the_solid", DagsterEventType.ASSET_MATERIALIZATION
    )
    assert len(materialization_events) == 1
    materialization = materialization_events[0].event_specific_data.materialization
    assert len(materialization.metadata_entries) == 5
    entry_map = {
        entry.label: entry.entry_data.__class__ for entry in materialization.metadata_entries
    }
    assert entry_map["text"] == TextMetadataEntryData
    assert entry_map["int"] == IntMetadataEntryData
    assert entry_map["url"] == UrlMetadataEntryData
    assert entry_map["float"] == FloatMetadataEntryData
    assert entry_map["python"] == PythonArtifactMetadataEntryData


def test_event_metadata_asset_observation():
    @solid(output_defs=[])
    def the_solid(_context):
        yield AssetObservation(
            asset_key="foo",
            metadata={
                "text": "FOO",
                "int": 22,
                "url": EventMetadata.url("http://fake.com"),
                "float": 0.1,
                "python": EventMetadata.python_artifact(EventMetadata),
            },
        )

    @pipeline
    def the_pipeline():
        the_solid()

    result = execute_pipeline(the_pipeline)

    assert result
    assert result.success

    observation_events = solid_events_for_type(
        result, "the_solid", DagsterEventType.ASSET_OBSERVATION
    )
    assert len(observation_events) == 1
    observation = observation_events[0].event_specific_data.asset_observation
    assert len(observation.metadata_entries) == 5
    entry_map = {entry.label: entry.entry_data.__class__ for entry in observation.metadata_entries}
    assert entry_map["text"] == TextMetadataEntryData
    assert entry_map["int"] == IntMetadataEntryData
    assert entry_map["url"] == UrlMetadataEntryData
    assert entry_map["float"] == FloatMetadataEntryData
    assert entry_map["python"] == PythonArtifactMetadataEntryData


def test_unknown_metadata_value():
    @solid(output_defs=[])
    def the_solid(context):
        yield AssetMaterialization(
            asset_key="foo",
            metadata={"bad": context.instance},
        )

    @pipeline
    def the_pipeline():
        the_solid()

    with pytest.raises(DagsterInvalidEventMetadata) as exc_info:
        execute_pipeline(the_pipeline)

    assert str(exc_info.value) == (
        'Could not resolve the metadata value for "bad" to a known type. '
        "Its type was <class 'dagster.core.instance.DagsterInstance'>. "
        "Consider wrapping the value with the appropriate EventMetadata type."
    )


def test_bad_json_metadata_value():
    @solid(output_defs=[])
    def the_solid(context):
        yield AssetMaterialization(
            asset_key="foo",
            metadata={"bad": {"nested": context.instance}},
        )

    @pipeline
    def the_pipeline():
        the_solid()

    with pytest.raises(DagsterInvalidEventMetadata) as exc_info:
        execute_pipeline(the_pipeline)

    assert str(exc_info.value) == (
        'Could not resolve the metadata value for "bad" to a JSON serializable value. '
        "Consider wrapping the value with the appropriate EventMetadata type."
    )


def test_table_metadata_value_schema_inference():

    table_metadata_value = EventMetadataEntry.table(
        records=[
            TableRecord(name="foo", status=False),
            TableRecord(name="bar", status=True),
        ],
        label="foo",
    )

    schema = table_metadata_value.entry_data.schema
    assert isinstance(schema, TableSchema)
    assert schema.columns == [
        TableColumn(name="name", type="string"),
        TableColumn(name="status", type="bool"),
    ]


bad_values = frozendict(
    {
        "table_schema": {"columns": False, "constraints": False},
        "table_column": {"name": False, "type": False, "description": False, "constraints": False},
        "table_constraints": {"other": False},
        "table_column_constraints": {
            "required": "foo",
            "unique": "foo",
            "min_length": "foo",
            "max_length": "foo",
            # "minimum": None,  # not checked because the type depends on column type
            # "maximum": None,
            "pattern": False,
            "enum": False,
            "other": False,
        },
    }
)


def test_table_column_keys():
    with pytest.raises(TypeError):
        TableColumn(bad_key="foo", description="bar", type="string")  # type: ignore


@pytest.mark.parametrize("key,value", list(bad_values["table_column"].items()))
def test_table_column_values(key, value):
    kwargs = {
        "name": "foo",
        "type": "string",
        "description": "bar",
        "constraints": TableColumnConstraints(other=["foo"]),
    }
    kwargs[key] = value
    with pytest.raises(CheckError):
        TableColumn(**kwargs)


def test_table_constraints_keys():
    with pytest.raises(TypeError):
        TableColumn(bad_key="foo")  # type: ignore


@pytest.mark.parametrize("key,value", list(bad_values["table_constraints"].items()))
def test_table_constraints(key, value):
    kwargs = {"other": ["foo"]}
    kwargs[key] = value
    with pytest.raises(CheckError):
        TableConstraints(**kwargs)


def test_table_column_constraints_keys():
    with pytest.raises(TypeError):
        TableColumnConstraints(bad_key="foo")  # type: ignore


# minimum and maximum aren't checked because they depend on the type of the column
@pytest.mark.parametrize("key,value", list(bad_values["table_column_constraints"].items()))
def test_table_column_constraints_values(key, value):
    kwargs = {
        "required": True,
        "unique": True,
        "min_length": 2,
        "max_length": 10,
        "minimum": "a",
        "maximum": "z",
        "pattern": r"\w+",
        "enum": None,
        "other": ["foo"],
    }
    kwargs[key] = value
    with pytest.raises(CheckError):
        TableColumnConstraints(**kwargs)


def test_table_schema_keys():
    with pytest.raises(TypeError):
        TableSchema(bad_key="foo")  # type: ignore


@pytest.mark.parametrize("key,value", list(bad_values["table_schema"].items()))
def test_table_schema_values(key, value):
    kwargs = {
        "constraints": TableConstraints(other=["foo"]),
        "columns": [
            TableColumn(
                name="foo",
                type="string",
                description="bar",
                constraints=TableColumnConstraints(other=["foo"]),
            )
        ],
    }
    kwargs[key] = value
    with pytest.raises(CheckError):
        TableSchema(**kwargs)


def test_complex_table_schema():
    assert isinstance(
        TableSchema(
            columns=[
                TableColumn(
                    name="foo",
                    type="customtype",
                    constraints=TableColumnConstraints(
                        required=True,
                        unique=True,
                        minimum=object(),
                    ),
                ),
                TableColumn(
                    name="bar",
                    type="string",
                    description="bar",
                    constraints=TableColumnConstraints(
                        min_length=10,
                        other=["foo"],
                    ),
                ),
            ],
            constraints=TableConstraints(other=["foo"]),
        ),
        TableSchema,
    )
