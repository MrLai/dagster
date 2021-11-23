import pandera as pa
import pandas as pd
from dagster import DagsterType, TypeCheck
from dagster.core.utils import check_dagster_package_version
from .version import __version__

check_dagster_package_version("dagster-pandera", __version__)


def pandera_schema_to_dagster_type(schema: pa, name, description):
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
        description=description,
    )

__all__ = [
    "pandera_schema_to_dagster_type",
]


