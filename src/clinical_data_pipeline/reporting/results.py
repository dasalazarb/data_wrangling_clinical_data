from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Iterable

import pandas as pd

from ..models import StepResult, ValidationIssue
from ..utils.core import ensure_dir, to_json


def issues_to_frame(issues: Iterable[ValidationIssue]) -> pd.DataFrame:
    rows = [asdict(x) for x in issues]
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "dataset_id", "stage", "check_name", "severity", "message", "column_name", "row_count", "output_path"
    ])


def write_step_result(result: StepResult, out_dir: str | Path) -> dict[str, str]:
    out_dir = ensure_dir(out_dir)
    prefix = f"{result.step_name}"
    summary_path = out_dir / f"{prefix}_summary.json"
    issues_path = out_dir / f"{prefix}_issues.csv"

    payload = {
        "step_name": result.step_name,
        "success": result.success,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "duration_seconds": result.duration_seconds,
        "dataset_id": result.dataset_id,
        "metrics": result.metrics,
        "artifacts": result.artifacts,
        "issue_count": len(result.issues),
    }
    to_json(payload, summary_path)
    issues_to_frame(result.issues).to_csv(issues_path, index=False)
    return {"summary": str(summary_path), "issues": str(issues_path)}


def write_final_summary(summary: dict, out_dir: str | Path, filename: str) -> Path:
    out_dir = ensure_dir(out_dir)
    return to_json(summary, out_dir / filename)
