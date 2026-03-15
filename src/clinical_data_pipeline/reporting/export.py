from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..models import StepResult
from ..utils.core import ensure_dir, now_ts, to_json


def _serialize_step_results(results: list[StepResult]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for result in results:
        data = asdict(result)
        data["issue_count"] = len(result.issues)
        payload.append(data)
    return payload


def _extract_messages(results: list[StepResult], severity: str) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for result in results:
        for issue in result.issues:
            if issue.severity.upper() != severity:
                continue
            messages.append(
                {
                    "step_name": result.step_name,
                    "dataset_id": issue.dataset_id,
                    "check_name": issue.check_name,
                    "message": issue.message,
                    "row_count": issue.row_count,
                    "column_name": issue.column_name,
                }
            )
    return messages


def _count_by_stage(results: list[StepResult]) -> dict[str, Any]:
    counts: dict[str, dict[str, int]] = {}
    for result in results:
        stage = result.step_name
        if result.dataset_id:
            stage = result.dataset_id
        stage_entry = counts.setdefault(stage, {"executed": 0, "failed": 0, "issues": 0})
        stage_entry["executed"] += 1
        stage_entry["failed"] += int(not result.success)
        stage_entry["issues"] += len(result.issues)
    return counts


def write_run_manifest(
    run_context: dict[str, Any],
    *,
    files_read: list[dict[str, Any]],
    validation_results: list[StepResult],
    generated_datasets: dict[str, Any],
    generated_artifacts: dict[str, Any],
    exclusions: dict[str, Any],
    merge_metrics: dict[str, Any],
    duration_seconds: float,
    out_dir: str | Path,
) -> Path:
    out_dir = ensure_dir(out_dir)
    manifest_payload = {
        "run": run_context,
        "files_read": files_read,
        "validations": _serialize_step_results(validation_results),
        "errors": _extract_messages(validation_results, "ERROR"),
        "warnings": _extract_messages(validation_results, "WARNING"),
        "generated": {
            "datasets": generated_datasets,
            "artifacts": generated_artifacts,
        },
        "counts_by_stage": _count_by_stage(validation_results),
        "exclusions": exclusions,
        "merge_metrics": merge_metrics,
        "finished_at": now_ts(),
        "duration_seconds": round(duration_seconds, 4),
    }

    manifest_name = f"run_manifest_{run_context['run_id']}.json"
    return to_json(manifest_payload, Path(out_dir) / manifest_name)
