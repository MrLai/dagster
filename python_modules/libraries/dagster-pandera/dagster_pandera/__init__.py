import itertools
import textwrap
from typing import TYPE_CHECKING, Dict, Generator, Optional, Tuple, Type, Union
from pkg_resources import working_set as pkg_resources_available, Requirement
import pandera as pa
import pandas as pd
import dask
from dagster import DagsterType, EventMetadataEntry, TypeCheck
from dagster.core.utils import check_dagster_package_version
from .version import __version__

if TYPE_CHECKING:
    import modin.pandas as mpd
    import databricks.koalas as ks

    # TODO
    # import dask
    # import ray
    ValidatableDataFrame = Union[pd.DataFrame, ks.DataFrame, mpd.DataFrame]


check_dagster_package_version("dagster-pandera", __version__)


def get_validatable_dataframe_classes() -> Tuple[type, ...]:
    classes = [pd.DataFrame]
    if pkg_resources_available.find(Requirement.parse("modin")) is not None:
        from modin.pandas import DataFrame as ModinDataFrame

        classes.append(ModinDataFrame)
    elif pkg_resources_available.find(Requirement.parse("koalas")) is not None:
        from databricks.koalas import DataFrame as KoalasDataFrame

        classes.append(KoalasDataFrame)
    return tuple(classes)


VALIDATABLE_DATA_FRAME_CLASSES = get_validatable_dataframe_classes()


def _anonymous_type_name_func() -> Generator[str, None, None]:
    for i in itertools.count(start=1):
        yield f"DagsterPandasDataframe{i}"


_anonymous_type_name = _anonymous_type_name_func()


def pandera_schema_to_dagster_type(
    schema: Union[pa.DataFrameSchema, Type[pa.SchemaModel]],
    name: Optional[str] = None,
    description: Optional[str] = None,
    column_descriptions: Dict[str, str] = None,
):

    column_descriptions = column_descriptions or {}

    if isinstance(schema, type) and issubclass(schema, pa.SchemaModel):
        name = name or schema.__name__
        schema = schema.to_schema()
    elif isinstance(schema, pa.DataFrameSchema):
        name = name or f"DagsterPanderaDataframe{next(_anonymous_type_name)}"
    else:
        raise TypeError("schema must be a DataFrameSchema or a subclass of SchemaModel")

    schema_desc = _build_schema_desc(schema, description, column_descriptions)

    def type_check_fn(_context, value: object) -> TypeCheck:
        if isinstance(value, VALIDATABLE_DATA_FRAME_CLASSES):
            try:
                # `lazy` instructs pandera to capture every (not just the first) validation error
                schema.validate(value, lazy=True)  # type: ignore [pandera type annotations wrong]
            except pa.errors.SchemaErrors as e:
                return TypeCheck(
                    success=False,
                    description=str(e),
                    metadata_entries=[
                        EventMetadataEntry.int(len(e.failure_cases), "num_failures"),
                        # TODO this will incorporate new Table event type
                        EventMetadataEntry.md(e.failure_cases.to_markdown(), "failure_cases"),
                    ],
                )
        else:
            return TypeCheck(
                success=False,
                description=f"Must be one of {VALIDATABLE_DATA_FRAME_CLASSES} not {type(value).__name__}.",
            )

        return TypeCheck(success=True)

    return DagsterType(
        type_check_fn=type_check_fn,
        name=name,
        description=schema_desc,
    )


def _build_schema_desc(
    schema: pa.DataFrameSchema, desc: Optional[str], column_descs: Dict[str, str]
):
    sections = [
        "### Columns",
        "\n".join(
            [
                _build_column_desc(column, column_descs.get(k, None))
                for k, column in schema.columns.items()
            ]
        ),
    ]
    if desc:
        sections.insert(0, desc)
    return "\n\n".join(sections)


def _build_column_desc(column: pa.Column, desc: Optional[str]) -> str:
    head = f"- **{column.name}** [{column.dtype}]"
    if desc:
        head += f" {desc}"
    lines = [head]
    for check in column.checks:
        lines.append(textwrap.indent(_build_check_desc(check), "    "))
    return "\n".join(lines)


def _build_check_desc(_check) -> str:
    return "- check"


__all__ = [
    "pandera_schema_to_dagster_type",
]
