from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd

from .models import DatasetSpec, StepResult, ValidationIssue
from .utils import now_ts, sha256_file


def _start(step_name: str, dataset_id: str | None = None):
    return time.perf_counter(), now_ts(), step_name, dataset_id


def _finish(t0: float, started_at: str, step_name: str, dataset_id: str | None, success: bool,
            metrics: dict[str, Any] | None = None, issues: list[ValidationIssue] | None = None,
            artifacts: dict[str, Any] | None = None) -> StepResult:
    return StepResult(
        step_name=step_name,
        success=success,
        started_at=started_at,
        finished_at=now_ts(),
        duration_seconds=round(time.perf_counter() - t0, 4),
        dataset_id=dataset_id,
        metrics=metrics or {},
        issues=issues or [],
        artifacts=artifacts or {},
    )


def validate_file_input(spec: DatasetSpec) -> StepResult:
    t0, started_at, step_name, dataset_id = _start(f"{spec.dataset_id}_input_validation", spec.dataset_id)
    issues: list[ValidationIssue] = []
    path = Path(spec.path)
    success = True
    if not path.exists():
        success = False
        issues.append(ValidationIssue(spec.dataset_id, "input", "file_exists", "ERROR", f"Missing file: {path}"))
    elif not path.is_file():
        success = False
        issues.append(ValidationIssue(spec.dataset_id, "input", "is_file", "ERROR", f"Path is not a file: {path}"))

    metrics = {}
    if success:
        metrics = {
            "path": str(path),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    return _finish(t0, started_at, step_name, dataset_id, success, metrics, issues)


def validate_columns(df: pd.DataFrame, spec: DatasetSpec) -> StepResult:
    t0, started_at, step_name, dataset_id = _start(f"{spec.dataset_id}_column_validation", spec.dataset_id)
    actual = set(df.columns)
    required = set(spec.required_columns)
    missing = sorted(required - actual)
    extras = sorted(actual - required - set(spec.optional_columns))
    issues = []
    success = not missing
    if missing:
        issues.append(ValidationIssue(spec.dataset_id, "structure", "missing_columns", "ERROR", f"Missing required columns: {missing}"))
    if extras:
        issues.append(ValidationIssue(spec.dataset_id, "structure", "unexpected_columns", "WARNING", f"Unexpected columns: {extras}"))
    metrics = {"n_columns": df.shape[1], "missing_required_count": len(missing), "unexpected_count": len(extras)}
    return _finish(t0, started_at, step_name, dataset_id, success, metrics, issues)


def validate_dtypes(df: pd.DataFrame, spec: DatasetSpec) -> StepResult:
    t0, started_at, step_name, dataset_id = _start(f"{spec.dataset_id}_dtype_validation", spec.dataset_id)
    issues = []
    metrics = {}
    success = True
    for col, expected in spec.expected_dtypes.items():
        if col not in df.columns:
            continue
        series = df[col]
        observed = str(series.dtype)
        metrics[col] = {"expected": expected, "observed": observed}
        if expected == "numeric":
            coerced = pd.to_numeric(series, errors="coerce")
            invalid = int(series.notna().sum() - coerced.notna().sum())
            if invalid > 0:
                success = False
                issues.append(ValidationIssue(spec.dataset_id, "dtype", "numeric_cast", "ERROR", f"{invalid} invalid numeric values in {col}", column_name=col, row_count=invalid))
        elif expected == "date":
            coerced = pd.to_datetime(series, errors="coerce")
            invalid = int(series.notna().sum() - coerced.notna().sum())
            if invalid > 0:
                success = False
                issues.append(ValidationIssue(spec.dataset_id, "dtype", "date_cast", "ERROR", f"{invalid} invalid date values in {col}", column_name=col, row_count=invalid))
    return _finish(t0, started_at, step_name, dataset_id, success, metrics, issues)


def profile_missingness(df: pd.DataFrame, spec: DatasetSpec) -> StepResult:
    t0, started_at, step_name, dataset_id = _start(f"{spec.dataset_id}_missingness_profile", spec.dataset_id)
    issues = []
    metrics = {}
    for col in df.columns:
        missing = int(df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum())
        metrics[col] = {
            "missing_count": missing,
            "missing_pct": round(missing / max(len(df), 1), 4),
        }
        if col in spec.required_columns and missing > 0:
            issues.append(ValidationIssue(spec.dataset_id, "missingness", "required_missing", "WARNING", f"Required column {col} has {missing} missing/blank values", column_name=col, row_count=missing))
    return _finish(t0, started_at, step_name, dataset_id, True, metrics, issues)


def validate_primary_key(df: pd.DataFrame, spec: DatasetSpec) -> StepResult:
    t0, started_at, step_name, dataset_id = _start(f"{spec.dataset_id}_primary_key_validation", spec.dataset_id)
    issues = []
    success = True
    metrics = {}
    if spec.primary_key and spec.primary_key in df.columns:
        s = df[spec.primary_key]
        blank = int(s.isna().sum() + (s.astype(str).str.strip() == "").sum())
        dup = int(df.duplicated(subset=[spec.primary_key]).sum())
        metrics = {
            "primary_key": spec.primary_key,
            "blank_key_count": blank,
            "duplicate_key_count": dup,
            "unique_key_count": int(s.nunique(dropna=True)),
        }
        if blank > 0:
            success = False
            issues.append(ValidationIssue(spec.dataset_id, "uniqueness", "blank_primary_key", "ERROR", f"{blank} blank/null primary key values", column_name=spec.primary_key, row_count=blank))
        if dup > 0:
            success = False
            issues.append(ValidationIssue(spec.dataset_id, "uniqueness", "duplicate_primary_key", "ERROR", f"{dup} duplicated primary key rows", column_name=spec.primary_key, row_count=dup))
    return _finish(t0, started_at, step_name, dataset_id, success, metrics, issues)


def validate_allowed_values(df: pd.DataFrame, spec: DatasetSpec) -> StepResult:
    t0, started_at, step_name, dataset_id = _start(f"{spec.dataset_id}_allowed_values_validation", spec.dataset_id)
    issues = []
    success = True
    metrics = {}
    for col, allowed in spec.allowed_values.items():
        if col not in df.columns:
            continue
        observed = set(df[col].dropna().astype(str).str.strip())
        invalid = sorted(x for x in observed if x not in set(map(str, allowed)))
        metrics[col] = {"allowed": allowed, "invalid_values": invalid}
        if invalid:
            success = False
            issues.append(ValidationIssue(spec.dataset_id, "domain", "invalid_domain_value", "ERROR", f"Invalid values in {col}: {invalid}", column_name=col, row_count=len(invalid)))
    return _finish(t0, started_at, step_name, dataset_id, success, metrics, issues)


def run_business_rules(df: pd.DataFrame, spec: DatasetSpec) -> StepResult:
    t0, started_at, step_name, dataset_id = _start(f"{spec.dataset_id}_business_rules", spec.dataset_id)
    issues = []
    success = True
    metrics = {}

    for rule in spec.rules:
        rule_name = rule["name"]
        rule_type = rule["type"]
        allow_na = bool(rule.get("allow_na", False))
        failing = 0

        if rule_type == "not_blank":
            col = rule["column"]
            mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
            failing = int(mask.sum())
        elif rule_type == "between":
            col = rule["column"]
            s = pd.to_numeric(df[col], errors="coerce")
            mask = (s < rule["min"]) | (s > rule["max"])
            if allow_na:
                mask = mask & s.notna()
            failing = int(mask.fillna(False).sum())
        elif rule_type == "in_set":
            col = rule["column"]
            allowed = set(map(str, rule["values"]))
            mask = ~df[col].astype(str).isin(allowed)
            if allow_na:
                mask = mask & df[col].notna()
            failing = int(mask.sum())
        elif rule_type == "date_not_future":
            col = rule["column"]
            s = pd.to_datetime(df[col], errors="coerce")
            mask = s > pd.Timestamp.today().normalize()
            failing = int(mask.fillna(False).sum())
        elif rule_type == "compare_dates":
            left = pd.to_datetime(df[rule["left"]], errors="coerce")
            right = pd.to_datetime(df[rule["right"]], errors="coerce")
            op = rule.get("operator", ">=")
            if op == ">=":
                mask = left < right
            elif op == ">":
                mask = left <= right
            else:
                raise ValueError(f"Unsupported date comparison operator: {op}")
            if allow_na:
                mask = mask & left.notna() & right.notna()
            else:
                mask = mask.fillna(True)
            failing = int(mask.sum())
        else:
            issues.append(ValidationIssue(spec.dataset_id, "business_rules", rule_name, "WARNING", f"Unsupported rule type: {rule_type}"))
            metrics[rule_name] = {"failing_rows": None, "rule_type": rule_type}
            continue

        metrics[rule_name] = {"rule_type": rule_type, "failing_rows": failing}
        if failing > 0:
            success = False
            issues.append(ValidationIssue(spec.dataset_id, "business_rules", rule_name, "ERROR", f"Rule {rule_name} failed for {failing} rows", row_count=failing))

    return _finish(t0, started_at, step_name, dataset_id, success, metrics, issues)


def run_sanity_checks(df: pd.DataFrame, spec: DatasetSpec) -> StepResult:
    t0, started_at, step_name, dataset_id = _start(f"{spec.dataset_id}_sanity_checks", spec.dataset_id)
    issues = []
    metrics = {
        "n_rows": int(df.shape[0]),
        "n_columns": int(df.shape[1]),
        "memory_bytes_estimate": int(df.memory_usage(deep=True).sum()),
    }
    if df.empty:
        issues.append(ValidationIssue(spec.dataset_id, "sanity", "empty_dataframe", "ERROR", "Dataset has zero rows"))
    return _finish(t0, started_at, step_name, dataset_id, len(issues) == 0, metrics, issues)


def cast_expected_types(df: pd.DataFrame, spec: DatasetSpec) -> pd.DataFrame:
    out = df.copy()
    for col, expected in spec.expected_dtypes.items():
        if col not in out.columns:
            continue
        if expected == "numeric":
            out[col] = pd.to_numeric(out[col], errors="coerce")
        elif expected == "date":
            out[col] = pd.to_datetime(out[col], errors="coerce")
        elif expected == "string":
            out[col] = out[col].astype("string")
    return out
