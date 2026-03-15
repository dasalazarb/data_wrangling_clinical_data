from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import pandera as pa
from pandera import Check


@dataclass(frozen=True)
class DatasetSchemaSpec:
    required_columns: list[str]
    dtypes: dict[str, str]
    nullable_columns: list[str]
    allowed_values: dict[str, list[Any]]
    ranges: dict[str, dict[str, float]]


class SchemaRegistry:
    """Registry of dataset-level pandera schemas."""

    def __init__(self):
        self._registry: dict[str, pa.DataFrameSchema] = {}

    def register(self, dataset_id: str, spec: DatasetSchemaSpec) -> pa.DataFrameSchema:
        columns: dict[str, pa.Column] = {}

        for col in spec.required_columns:
            expected = spec.dtypes.get(col, "string")
            pandas_dtype = "str" if expected == "string" else expected
            checks = []

            if col in spec.allowed_values:
                allowed = set(spec.allowed_values[col])
                checks.append(Check.isin(allowed))

            if col in spec.ranges:
                bounds = spec.ranges[col]
                min_value = bounds.get("min")
                max_value = bounds.get("max")
                if min_value is not None:
                    checks.append(Check.ge(min_value))
                if max_value is not None:
                    checks.append(Check.le(max_value))

            columns[col] = pa.Column(
                pandas_dtype,
                nullable=col in spec.nullable_columns,
                required=True,
                checks=checks,
                coerce=True,
            )

        schema = pa.DataFrameSchema(columns=columns, strict=False)
        self._registry[dataset_id] = schema
        return schema

    def get(self, dataset_id: str) -> pa.DataFrameSchema:
        if dataset_id not in self._registry:
            raise KeyError(f"Schema not registered for dataset: {dataset_id}")
        return self._registry[dataset_id]

    def validate(self, dataset_id: str, df: pd.DataFrame) -> pd.DataFrame:
        schema = self.get(dataset_id)
        return schema.validate(df, lazy=True)


def build_schema_from_spec(spec: DatasetSchemaSpec) -> pa.DataFrameSchema:
    registry = SchemaRegistry()
    return registry.register("__anonymous__", spec)
