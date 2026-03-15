from __future__ import annotations

from pathlib import Path

import pandas as pd

from .io.config_loader import load_dataset_spec, load_yaml
from .io.readers import read_table
from .io.writers import write_table
from .integrate.enterprise import run_optional_enterprise_checks
from .integrate.merge import extract_unmatched, perform_merge, validate_merge_keys
from .manifest import build_run_manifest, write_run_manifest
from .reporting.results import write_final_summary, write_step_result
from .settings import get_settings, guess_project_root, resolve_path
from .transform.catalog import build_canonical_catalog
from .utils.core import compact_ts, ensure_dir
from .utils.logger import build_logger
from .validation.validators import (
    cast_expected_types,
    profile_missingness,
    run_business_rules,
    run_sanity_checks,
    validate_allowed_values,
    validate_columns,
    validate_dtypes,
    validate_file_input,
    validate_primary_key,
)


def _load_merge_steps(config: dict, project_root: Path) -> list[dict]:
    merge_plan_cfg = config.get("merge_plan")
    merge_plan_path = config.get("merge_plan_config")
    if merge_plan_path:
        merge_plan_cfg = load_yaml(resolve_path(merge_plan_path, project_root))
    return merge_plan_cfg.get("steps", []) if merge_plan_cfg else []


def run_variable_catalog_pipeline(config_path: str | Path, project_root: str | Path | None = None) -> dict:
    project_root = Path(project_root or guess_project_root(config_path))
    config_obj = get_settings(config_path=config_path, project_root=project_root)
    config = config_obj.model_dump(mode="json")
    run_id = f"{config.get('run_name_prefix', 'catalog')}_{compact_ts()}"
    logger = build_logger(config["paths"]["logs_dir"], run_id, config.get("settings", {}).get("log_level", "INFO"))
    logger.info("[INFO] Starting variable catalog pipeline")
    out_df, result = build_canonical_catalog(config, logger)
    write_step_result(result, Path(config["paths"]["reports_dir"]) / "step_results")
    write_final_summary(
        {
            "run_id": run_id,
            "started_at": result.started_at,
            "finished_at": result.finished_at,
            "metrics": result.metrics,
            "artifacts": result.artifacts,
        },
        Path(config["paths"]["reports_dir"]) / "final_summary",
        f"catalog_final_summary_{compact_ts()}.json",
    )
    return {"run_id": run_id, "rows": len(out_df), "artifacts": result.artifacts}


def run_patient_pipeline(config_path: str | Path, project_root: str | Path | None = None) -> dict:
    project_root = Path(project_root or guess_project_root(config_path))
    config_obj = get_settings(config_path=config_path, project_root=project_root)
    config = config_obj.model_dump(mode="json")
    run_id = f"{config.get('run_name_prefix', 'pipeline')}_{compact_ts()}"
    paths = config["paths"]
    logger = build_logger(paths["logs_dir"], run_id, config.get("settings", {}).get("log_level", "INFO"))
    logger.info("[INFO] Starting patient-data pipeline")

    for key in ["staging_dir", "curated_dir", "analytic_dir", "excluded_dir", "reports_dir", "logs_dir"]:
        ensure_dir(paths[key])

    all_results = []
    curated_tables: dict[str, pd.DataFrame] = {}

    for spec_path in config["datasets"]:
        spec = load_dataset_spec(resolve_path(spec_path, project_root))
        spec.path = resolve_path(spec.path, project_root)
        logger.info("[INFO] Processing dataset: %s", spec.dataset_id)

        input_result = validate_file_input(spec)
        all_results.append(input_result)
        write_step_result(input_result, Path(paths["reports_dir"]) / spec.dataset_id)
        if not input_result.success and config.get("settings", {}).get("fail_fast", False):
            raise FileNotFoundError(f"Critical input error for {spec.dataset_id}")

        df = read_table(spec.path, spec.file_type)
        df = cast_expected_types(df, spec)

        validations = [
            validate_columns(df, spec),
            validate_dtypes(df, spec),
            profile_missingness(df, spec),
            validate_primary_key(df, spec),
            validate_allowed_values(df, spec),
            run_business_rules(df, spec),
            run_sanity_checks(df, spec),
        ]
        for result in validations:
            all_results.append(result)
            write_step_result(result, Path(paths["reports_dir"]) / spec.dataset_id)

        staging_path = Path(paths["staging_dir"]) / f"{spec.dataset_id}_{compact_ts()}.parquet"
        write_table(df, staging_path)

        curated_path = Path(paths["curated_dir"]) / f"{spec.dataset_id}_{compact_ts()}.parquet"
        write_table(df, curated_path)
        curated_tables[spec.dataset_id] = df
        logger.info("[INFO] Staging dataset saved: %s", staging_path)
        logger.info("[INFO] Curated dataset saved: %s", curated_path)
        run_optional_enterprise_checks(logger, spec.dataset_id, paths["reports_dir"])

    merge_outputs = {}
    working_tables = dict(curated_tables)
    merge_steps = _load_merge_steps(config, project_root)
    for step in merge_steps:
        left_name = step["left_dataset"]
        right_name = step["right_dataset"]
        merge_name = step["name"]
        logger.info("[INFO] Running merge: %s", merge_name)

        merge_keys = step.get("on", step.get(True))
        if merge_keys is None:
            raise KeyError(f"Merge step {merge_name} is missing 'on' keys")
        key_result = validate_merge_keys(working_tables[left_name], working_tables[right_name], merge_keys, merge_name)
        all_results.append(key_result)
        write_step_result(key_result, Path(paths["reports_dir"]) / "merge_quality")

        merged, merge_result = perform_merge(working_tables[left_name], working_tables[right_name], step["how"], merge_keys, merge_name)
        all_results.append(merge_result)
        write_step_result(merge_result, Path(paths["reports_dir"]) / "merge_quality")

        left_only, right_only = extract_unmatched(merged)
        analytic_path = Path(paths["analytic_dir"]) / f"{merge_name}_{compact_ts()}.parquet"
        left_path = Path(paths["excluded_dir"]) / f"{merge_name}_left_only_{compact_ts()}.csv"
        right_path = Path(paths["excluded_dir"]) / f"{merge_name}_right_only_{compact_ts()}.csv"
        write_table(merged.drop(columns=["_merge"]), analytic_path)
        write_table(left_only, left_path)
        write_table(right_only, right_path)

        merge_outputs[merge_name] = {
            "analytic_path": str(analytic_path),
            "left_only_path": str(left_path),
            "right_only_path": str(right_path),
            "merge_metrics": merge_result.metrics,
        }

        working_tables[merge_name] = merged.drop(columns=["_merge"])

    success = all(r.success for r in all_results)
    final_summary = {
        "run_id": run_id,
        "project_name": config.get("project_name", "clinical_pipeline"),
        "success": success,
        "step_count": len(all_results),
        "steps_failed": [r.step_name for r in all_results if not r.success],
        "merge_outputs": merge_outputs,
        "report_root": paths["reports_dir"],
        "analytic_root": paths["analytic_dir"],
    }
    summary_path = write_final_summary(final_summary, Path(paths["reports_dir"]) / "final_summary", f"pipeline_final_summary_{compact_ts()}.json")
    manifest = build_run_manifest(run_id=run_id, config_path=str(config_path), success=success, summary_path=str(summary_path))
    write_run_manifest(manifest, Path(paths["reports_dir"]) / "final_summary", f"run_manifest_{compact_ts()}.json")
    logger.info("[INFO] Pipeline completed. Summary: %s", summary_path)
    return final_summary
