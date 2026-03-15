from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

from .models import StepResult, ValidationIssue
from .utils import now_ts


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


def perform_merge(left_df: pd.DataFrame, right_df: pd.DataFrame, how: str, on: list[str], step_name: str):
    t0 = time.perf_counter()
    started_at = now_ts()
    merged = left_df.merge(right_df, how=how, on=on, indicator=True, suffixes=("", "_right"))
    metrics = {
        "left_rows": int(len(left_df)),
        "right_rows": int(len(right_df)),
        "merged_rows": int(len(merged)),
        "both_count": int((merged["_merge"] == "both").sum()),
        "left_only_count": int((merged["_merge"] == "left_only").sum()),
        "right_only_count": int((merged["_merge"] == "right_only").sum()),
        "match_rate_against_left": round(float((merged["_merge"] == "both").sum()) / max(len(left_df), 1), 4),
    }
    result = StepResult(
        step_name=f"{step_name}_merge",
        success=True,
        started_at=started_at,
        finished_at=now_ts(),
        duration_seconds=round(time.perf_counter() - t0, 4),
        dataset_id=step_name,
        metrics=metrics,
        issues=[],
    )
    return merged, result


def extract_unmatched(merged_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    left_only = merged_df[merged_df["_merge"] == "left_only"].copy()
    right_only = merged_df[merged_df["_merge"] == "right_only"].copy()
    return left_only, right_only
