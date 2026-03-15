from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import DatasetSpec


def load_yaml(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_dataset_spec(path: str | Path) -> DatasetSpec:
    cfg = load_yaml(path)
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
