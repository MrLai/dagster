import textwrap
from typing import Dict, Optional, Union
import pandera as pa
import pandas as pd
import dask
from pandera.schema_components import Column
from pandera.schemas import DataFrameSchema
from dagster import DagsterType, TypeCheck
from dagster.core.utils import check_dagster_package_version
from .version import __version__

check_dagster_package_version("dagster-pandera", __version__)

ValidatableDataFrame = Union[pd.DataFrame, 

def pandera_schema_to_dagster_type(
    schema: pa.DataFrameSchema,
    name: Optional[str] = None,
    description: Optional[str] = None,
    column_descriptions: Dict[str, str] = None,
):

    column_descriptions = column_descriptions or {}
    schema_desc = _build_schema_desc(schema, description, column_descriptions)

    def type_check_fn(_context, value):
        if not isinstance(value, pd.DataFrame):
            return TypeCheck(
                success=False,
                description=f"Must be pandas.DataFrame, not {type(value).__name__}.",
            )
        try:
            # `lazy` instructs pandera to capture every (not just the first) validation error
            schema.validate(value, lazy=True)
        except pa.errors.SchemaErrors as e:
            return TypeCheck(
                success=False,
                description=str(e),
                metadata={
                    "num_violations": len(e.failure_cases),
                },
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
        lines.append(_build_check_desc(check))
    return textwrap.indent("    ", "\n".join(lines))


def _build_check_desc(_check) -> str:
    return "- check"


__all__ = [
    "pandera_schema_to_dagster_type",
]
