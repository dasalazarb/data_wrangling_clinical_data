from __future__ import annotations

from pathlib import Path

from ..models import ValidationIssue


def validate_file_exists(path: str | Path, dataset_id: str, stage: str = "input") -> tuple[bool, list[ValidationIssue]]:
    """Validate that an input path exists and points to a file."""
    issues: list[ValidationIssue] = []
    file_path = Path(path)

    if not file_path.exists():
        issues.append(
            ValidationIssue(
                dataset_id=dataset_id,
                stage=stage,
                check_name="file_exists",
                severity="ERROR",
                message=f"Missing file: {file_path}",
            )
        )
        return False, issues

    if not file_path.is_file():
        issues.append(
            ValidationIssue(
                dataset_id=dataset_id,
                stage=stage,
                check_name="is_file",
                severity="ERROR",
                message=f"Path is not a file: {file_path}",
            )
        )
        return False, issues

    issues.append(
        ValidationIssue(
            dataset_id=dataset_id,
            stage=stage,
            check_name="file_exists",
            severity="INFO",
            message=f"Input file located: {file_path}",
        )
    )
    return True, issues
