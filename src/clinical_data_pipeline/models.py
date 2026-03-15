from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ValidationIssue:
    dataset_id: str
    stage: str
    check_name: str
    severity: str
    message: str
    column_name: str | None = None
    row_count: int | None = None
    output_path: str | None = None


@dataclass
class StepResult:
    step_name: str
    success: bool
    started_at: str
    finished_at: str
    duration_seconds: float
    dataset_id: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)
    issues: list[ValidationIssue] = field(default_factory=list)


@dataclass
class DatasetSpec:
    dataset_id: str
    path: Path
    file_type: str
    primary_key: str | None
    required_columns: list[str]
    optional_columns: list[str]
    expected_dtypes: dict[str, str]
    allowed_values: dict[str, list[Any]] = field(default_factory=dict)
    rules: list[dict[str, Any]] = field(default_factory=list)
