from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from ..utils.core import ensure_dir, now_ts


OUTPUT_ROOT_KEYS = ("staging_dir", "curated_dir", "analytic_dir", "excluded_dir", "reports_dir", "logs_dir")


def compute_file_hash(path: str | Path, algorithm: str = "sha256", chunk_size: int = 65536) -> str:
    path = Path(path)
    hasher = hashlib.new(algorithm)
    with path.open("rb") as file_handle:
        while True:
            chunk = file_handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def collect_file_metadata(path: str | Path, include_hash: bool = True) -> dict[str, Any]:
    file_path = Path(path)
    exists = file_path.exists()
    metadata: dict[str, Any] = {
        "path": str(file_path),
        "name": file_path.name,
        "exists": exists,
    }
    if not exists:
        return metadata

    stat = file_path.stat()
    metadata.update(
        {
            "size_bytes": stat.st_size,
            "modified_at": now_ts() if stat.st_mtime is None else None,
            "suffix": file_path.suffix.lower(),
        }
    )
    metadata["modified_at"] = metadata["modified_at"] or datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(
        timespec="seconds"
    )

    if include_hash and file_path.is_file():
        metadata["sha256"] = compute_file_hash(file_path)

    return metadata


def _hash_config(config: dict[str, Any]) -> str:
    canonical = json.dumps(config, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_run_context(config: dict[str, Any], config_path: str | Path) -> dict[str, Any]:
    started_at = now_ts()
    run_prefix = config.get("run_name_prefix", "pipeline")
    run_id = f"{run_prefix}_{started_at.replace(':', '').replace('-', '').replace('+', '_').replace('T', '_')}_{uuid.uuid4().hex[:8]}"

    base_paths = config.get("paths", {})
    versioned_roots: dict[str, str] = {}
    for root_key in OUTPUT_ROOT_KEYS:
        if root_key not in base_paths:
            continue
        versioned_roots[root_key] = str(ensure_dir(Path(base_paths[root_key]) / run_id))

    manifests_root = ensure_dir(Path(base_paths.get("reports_dir", "reports")) / "manifests")

    return {
        "run_id": run_id,
        "started_at": started_at,
        "config": {
            "path": str(config_path),
            "fingerprint": _hash_config(config),
        },
        "output_roots": {
            "base": {k: str(v) for k, v in base_paths.items()},
            "versioned": versioned_roots,
            "manifests_dir": str(manifests_root),
        },
    }
