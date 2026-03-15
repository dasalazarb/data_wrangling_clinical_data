from __future__ import annotations

from pathlib import Path

from .utils.core import now_ts, to_json


def build_run_manifest(run_id: str, config_path: str, success: bool, summary_path: str) -> dict:
    return {
        "run_id": run_id,
        "created_at": now_ts(),
        "config_path": config_path,
        "success": success,
        "summary_path": summary_path,
    }


def write_run_manifest(manifest: dict, out_dir: str | Path, filename: str) -> Path:
    return to_json(manifest, Path(out_dir) / filename)
