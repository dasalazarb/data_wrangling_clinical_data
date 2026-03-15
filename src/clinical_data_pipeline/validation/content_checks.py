from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from ..models import ValidationIssue


SEVERITY_INFO = "INFO"
SEVERITY_WARNING = "WARNING"
SEVERITY_ERROR = "ERROR"


def _issue(dataset_id: str, stage: str, check_name: str, severity: str, message: str,
           column_name: str | None = None, row_count: int | None = None) -> ValidationIssue:
    return ValidationIssue(
        dataset_id=dataset_id,
        stage=stage,
        check_name=check_name,
        severity=severity,
        message=message,
        column_name=column_name,
        row_count=row_count,
    )


def validate_required_fields(df: pd.DataFrame, required_fields: Iterable[str], dataset_id: str,
                             stage: str = "structure") -> tuple[bool, list[ValidationIssue]]:
    required = list(required_fields)
    missing_columns = [c for c in required if c not in df.columns]
    issues: list[ValidationIssue] = []

    if missing_columns:
        issues.append(_issue(dataset_id, stage, "missing_columns", SEVERITY_ERROR, f"Missing required columns: {missing_columns}"))
        return False, issues

    for col in required:
        blank_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        blank_count = int(blank_mask.sum())
        if blank_count > 0:
            issues.append(_issue(dataset_id, stage, "required_field_missing_values", SEVERITY_WARNING,
                                 f"Column {col} has {blank_count} missing/blank values", column_name=col, row_count=blank_count))

    if not issues:
        issues.append(_issue(dataset_id, stage, "required_fields", SEVERITY_INFO, "All required fields are present with no blank values."))

    success = all(i.severity != SEVERITY_ERROR for i in issues)
    return success, issues


def parse_dates_safely(series: pd.Series, dataset_id: str, column_name: str,
                       stage: str = "dtype") -> tuple[pd.Series, list[ValidationIssue]]:
    parsed = pd.to_datetime(series, errors="coerce")
    invalid_count = int(series.notna().sum() - parsed.notna().sum())
    issues: list[ValidationIssue] = []

    if invalid_count > 0:
        issues.append(_issue(dataset_id, stage, "date_parse", SEVERITY_WARNING,
                             f"{invalid_count} rows in {column_name} could not be parsed as dates",
                             column_name=column_name, row_count=invalid_count))
    else:
        issues.append(_issue(dataset_id, stage, "date_parse", SEVERITY_INFO,
                             f"Date parsing succeeded for {column_name}", column_name=column_name))
    return parsed, issues


def validate_domains(df: pd.DataFrame, domain_rules: dict[str, Iterable[str]], dataset_id: str,
                     stage: str = "domain") -> tuple[bool, list[ValidationIssue]]:
    issues: list[ValidationIssue] = []
    success = True
    for col, allowed_values in domain_rules.items():
        if col not in df.columns:
            issues.append(_issue(dataset_id, stage, "domain_column_missing", SEVERITY_WARNING,
                                 f"Domain validation skipped; column not found: {col}", column_name=col))
            continue

        allowed = {str(v).strip() for v in allowed_values}
        observed = set(df[col].dropna().astype(str).str.strip())
        invalid = sorted([v for v in observed if v not in allowed])

        if invalid:
            success = False
            issues.append(_issue(dataset_id, stage, "invalid_domain_value", SEVERITY_ERROR,
                                 f"Invalid domain values in {col}: {invalid}", column_name=col, row_count=len(invalid)))
        else:
            issues.append(_issue(dataset_id, stage, "domain_validation", SEVERITY_INFO,
                                 f"All values in {col} are within the expected domain.", column_name=col))

    return success, issues


def validate_ranges(df: pd.DataFrame, range_rules: dict[str, dict[str, float]], dataset_id: str,
                    stage: str = "business_rules") -> tuple[bool, list[ValidationIssue]]:
    issues: list[ValidationIssue] = []
    success = True

    for col, bounds in range_rules.items():
        if col not in df.columns:
            issues.append(_issue(dataset_id, stage, "range_column_missing", SEVERITY_WARNING,
                                 f"Range validation skipped; column not found: {col}", column_name=col))
            continue

        s = pd.to_numeric(df[col], errors="coerce")
        lower = bounds.get("min")
        upper = bounds.get("max")
        mask = pd.Series(False, index=s.index)
        if lower is not None:
            mask = mask | (s < float(lower))
        if upper is not None:
            mask = mask | (s > float(upper))

        invalid_count = int(mask.fillna(False).sum())
        if invalid_count > 0:
            success = False
            issues.append(_issue(dataset_id, stage, "range_violation", SEVERITY_ERROR,
                                 f"{invalid_count} rows in {col} violate range {bounds}",
                                 column_name=col, row_count=invalid_count))
        else:
            issues.append(_issue(dataset_id, stage, "range_validation", SEVERITY_INFO,
                                 f"Range validation passed for {col}", column_name=col))

    return success, issues


def detect_duplicates(df: pd.DataFrame, subset: list[str], dataset_id: str,
                      stage: str = "uniqueness") -> tuple[pd.DataFrame, list[ValidationIssue]]:
    duplicated_rows = df[df.duplicated(subset=subset, keep=False)].copy()
    dup_count = int(duplicated_rows.shape[0])

    if dup_count > 0:
        issues = [_issue(dataset_id, stage, "duplicate_rows", SEVERITY_ERROR,
                         f"Detected {dup_count} duplicated rows for subset={subset}", row_count=dup_count)]
    else:
        issues = [_issue(dataset_id, stage, "duplicate_rows", SEVERITY_INFO,
                         f"No duplicated rows for subset={subset}")]
    return duplicated_rows, issues


def validate_primary_key(df: pd.DataFrame, primary_key: str, dataset_id: str,
                         stage: str = "uniqueness") -> tuple[bool, list[ValidationIssue], dict[str, int | str]]:
    issues: list[ValidationIssue] = []

    if primary_key not in df.columns:
        issues.append(_issue(dataset_id, stage, "primary_key_missing", SEVERITY_ERROR,
                             f"Primary key column missing: {primary_key}", column_name=primary_key))
        return False, issues, {"primary_key": primary_key, "blank_key_count": 0, "duplicate_key_count": 0}

    series = df[primary_key]
    blank_count = int(series.isna().sum() + (series.astype(str).str.strip() == "").sum())
    duplicate_count = int(df.duplicated(subset=[primary_key]).sum())

    if blank_count > 0:
        issues.append(_issue(dataset_id, stage, "blank_primary_key", SEVERITY_ERROR,
                             f"{blank_count} blank/null primary key values", column_name=primary_key, row_count=blank_count))
    if duplicate_count > 0:
        issues.append(_issue(dataset_id, stage, "duplicate_primary_key", SEVERITY_ERROR,
                             f"{duplicate_count} duplicated primary key rows", column_name=primary_key, row_count=duplicate_count))

    if blank_count == 0 and duplicate_count == 0:
        issues.append(_issue(dataset_id, stage, "primary_key_validation", SEVERITY_INFO,
                             "Primary key validation passed", column_name=primary_key))

    success = (blank_count == 0 and duplicate_count == 0)
    metrics = {
        "primary_key": primary_key,
        "blank_key_count": blank_count,
        "duplicate_key_count": duplicate_count,
        "unique_key_count": int(series.nunique(dropna=True)),
    }
    return success, issues, metrics
