from __future__ import annotations

from typing import Any


def build_final_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    run = manifest.get("run", {})
    validations = manifest.get("validations", [])
    errors = manifest.get("errors", [])
    warnings = manifest.get("warnings", [])

    return {
        "run_id": run.get("run_id"),
        "started_at": run.get("started_at"),
        "finished_at": manifest.get("finished_at"),
        "success": len(errors) == 0,
        "config_fingerprint": run.get("config", {}).get("fingerprint"),
        "validations_executed": len(validations),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "counts_by_stage": manifest.get("counts_by_stage", {}),
        "generated": manifest.get("generated", {}),
        "exclusions": manifest.get("exclusions", {}),
        "merge_metrics": manifest.get("merge_metrics", {}),
    }
