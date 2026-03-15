from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..models import DatasetSpec


def load_yaml(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_dataset_spec(path: str | Path) -> DatasetSpec:
    path = Path(path)
    cfg = load_yaml(path)
    if cfg.get("rules_path"):
        rules_path = Path(cfg["rules_path"])
        if not rules_path.is_absolute():
            candidate_local = path.parent / rules_path
            candidate_repo = path.parents[2] / rules_path
            rules_path = candidate_local if candidate_local.exists() else candidate_repo
        cfg["rules"] = load_yaml(rules_path).get("rules", [])
    return DatasetSpec(
        dataset_id=cfg["dataset_id"],
        path=Path(cfg["path"]),
        file_type=cfg.get("file_type", "csv"),
        primary_key=cfg.get("primary_key"),
        required_columns=cfg.get("required_columns", []),
        optional_columns=cfg.get("optional_columns", []),
        expected_dtypes=cfg.get("expected_dtypes", {}),
        allowed_values=cfg.get("allowed_values", {}),
        rules=cfg.get("rules", []),
    )
