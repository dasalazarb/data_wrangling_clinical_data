from __future__ import annotations

from ..models import DatasetSpec


class SchemaRegistry:
    def __init__(self) -> None:
        self._registry: dict[str, DatasetSpec] = {}

    def register(self, spec: DatasetSpec) -> None:
        self._registry[spec.dataset_id] = spec

    def get(self, dataset_id: str) -> DatasetSpec:
        return self._registry[dataset_id]

    def as_dict(self) -> dict[str, dict[str, str]]:
        return {dataset_id: spec.expected_dtypes for dataset_id, spec in self._registry.items()}
