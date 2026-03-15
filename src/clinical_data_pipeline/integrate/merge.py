from __future__ import annotations

import time

import pandas as pd

from ..models import StepResult, ValidationIssue
from ..utils.core import now_ts


def validate_merge_keys(left_df: pd.DataFrame, right_df: pd.DataFrame, on: list[str], step_name: str) -> StepResult:
    t0 = time.perf_counter()
    started_at = now_ts()
    issues = []
    success = True

    for col in on:
        if col not in left_df.columns:
            success = False
            issues.append(ValidationIssue(step_name, "merge", "missing_left_key", "ERROR", f"Missing left key column: {col}", column_name=col))
        if col not in right_df.columns:
            success = False
            issues.append(ValidationIssue(step_name, "merge", "missing_right_key", "ERROR", f"Missing right key column: {col}", column_name=col))

    if success:
        left_null = int(left_df[on].isna().any(axis=1).sum())
        right_null = int(right_df[on].isna().any(axis=1).sum())
        if left_null > 0:
            issues.append(ValidationIssue(step_name, "merge", "left_null_keys", "WARNING", f"{left_null} left rows contain null merge keys", row_count=left_null))
        if right_null > 0:
            issues.append(ValidationIssue(step_name, "merge", "right_null_keys", "WARNING", f"{right_null} right rows contain null merge keys", row_count=right_null))

    return StepResult(
        step_name=f"{step_name}_merge_key_validation",
        success=success,
        started_at=started_at,
        finished_at=now_ts(),
        duration_seconds=round(time.perf_counter() - t0, 4),
        dataset_id=step_name,
        metrics={"merge_keys": on},
        issues=issues,
    )


def compute_merge_metrics(merged_df: pd.DataFrame, left_df: pd.DataFrame, right_df: pd.DataFrame,
                          on: list[str], expected_cardinality: str | None = None) -> dict[str, float | int | str | None]:
    both_count = int((merged_df["_merge"] == "both").sum())
    left_only_count = int((merged_df["_merge"] == "left_only").sum())
    right_only_count = int((merged_df["_merge"] == "right_only").sum())

    left_key_dups = int(left_df.duplicated(subset=on).sum())
    right_key_dups = int(right_df.duplicated(subset=on).sum())
    unexpected_duplication = int(max(len(merged_df) - max(len(left_df), len(right_df)), 0))

    observed_cardinality = "one_to_one"
    if left_key_dups > 0 and right_key_dups == 0:
        observed_cardinality = "many_to_one"
    elif left_key_dups == 0 and right_key_dups > 0:
        observed_cardinality = "one_to_many"
    elif left_key_dups > 0 and right_key_dups > 0:
        observed_cardinality = "many_to_many"

    return {
        "left_rows": int(len(left_df)),
        "right_rows": int(len(right_df)),
        "merged_rows": int(len(merged_df)),
        "both_count": both_count,
        "left_only_count": left_only_count,
        "right_only_count": right_only_count,
        "match_rate_against_left": round(both_count / max(len(left_df), 1), 4),
        "unmatched_left": left_only_count,
        "unmatched_right": right_only_count,
        "unexpected_duplication": unexpected_duplication,
        "observed_cardinality": observed_cardinality,
        "expected_cardinality": expected_cardinality,
        "cardinality_matches_expectation": (expected_cardinality == observed_cardinality) if expected_cardinality else None,
    }


def perform_merge(left_df: pd.DataFrame, right_df: pd.DataFrame, how: str, on: list[str], step_name: str,
                  expected_cardinality: str | None = None):
    t0 = time.perf_counter()
    started_at = now_ts()
    merged = left_df.merge(right_df, how=how, on=on, indicator=True, suffixes=("", "_right"))
    metrics = compute_merge_metrics(merged, left_df, right_df, on, expected_cardinality=expected_cardinality)

    issues: list[ValidationIssue] = []
    if metrics["unexpected_duplication"] > 0:
        issues.append(
            ValidationIssue(
                step_name,
                "merge",
                "unexpected_duplication",
                "WARNING",
                f"Merged table contains {metrics['unexpected_duplication']} unexpected duplicated rows compared to source tables",
                row_count=int(metrics["unexpected_duplication"]),
            )
        )
    if metrics["cardinality_matches_expectation"] is False:
        issues.append(
            ValidationIssue(
                step_name,
                "merge",
                "cardinality_mismatch",
                "ERROR",
                f"Observed cardinality {metrics['observed_cardinality']} does not match expected {metrics['expected_cardinality']}",
            )
        )

    result = StepResult(
        step_name=f"{step_name}_merge",
        success=not any(i.severity == "ERROR" for i in issues),
        started_at=started_at,
        finished_at=now_ts(),
        duration_seconds=round(time.perf_counter() - t0, 4),
        dataset_id=step_name,
        metrics=metrics,
        issues=issues,
    )
    return merged, result


def extract_unmatched(merged_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    left_only = merged_df[merged_df["_merge"] == "left_only"].copy()
    right_only = merged_df[merged_df["_merge"] == "right_only"].copy()
    return left_only, right_only
