from __future__ import annotations

from pathlib import Path

import pandas as pd

from .catalog import build_canonical_catalog
from .config_loader import load_dataset_spec, load_yaml
from .enterprise import run_optional_enterprise_checks
from .io import read_table, write_table
from .logger import build_logger
from .merge import extract_unmatched, perform_merge, validate_merge_keys
from .reporting import write_final_summary, write_step_result
from .utils import compact_ts, ensure_dir, now_ts
from .validators import (
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


def _resolve(path: str | Path, project_root: Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else project_root / path


def _apply_path_prefixes(config: dict, project_root: Path) -> dict:
    for section in ["paths"]:
        if section in config:
            for key, value in list(config[section].items()):
                config[section][key] = str(_resolve(value, project_root))
    if "catalog" in config:
        config["catalog"]["input_path"] = str(_resolve(config["catalog"]["input_path"], project_root))
    return config


def _guess_project_root(config_path: str | Path) -> Path:
    config_path = Path(config_path).resolve()
    parent = config_path.parent
    if parent.name == "configs":
        return parent.parent
    if parent.parent.name == "configs":
        return parent.parent.parent
    return config_path.parent


def run_variable_catalog_pipeline(config_path: str | Path, project_root: str | Path | None = None) -> dict:
    project_root = Path(project_root or _guess_project_root(config_path))
    config = _apply_path_prefixes(load_yaml(config_path), project_root)
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
    project_root = Path(project_root or _guess_project_root(config_path))
    config = _apply_path_prefixes(load_yaml(config_path), project_root)
    run_id = f"{config.get('run_name_prefix', 'pipeline')}_{compact_ts()}"
    paths = config["paths"]
    logger = build_logger(paths["logs_dir"], run_id, config.get("settings", {}).get("log_level", "INFO"))
    logger.info("[INFO] Starting patient-data pipeline")

    for key in ["staging_dir", "curated_dir", "analytic_dir", "excluded_dir", "reports_dir", "logs_dir"]:
        ensure_dir(paths[key])

    all_results = []
    curated_tables: dict[str, pd.DataFrame] = {}

    for spec_path in config["datasets"]:
        spec = load_dataset_spec(_resolve(spec_path, project_root))
        spec.path = _resolve(spec.path, project_root)
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

        curated_path = Path(paths["curated_dir"]) / f"{spec.dataset_id}_{compact_ts()}.csv"
        write_table(df, curated_path)
        curated_tables[spec.dataset_id] = df
        logger.info("[INFO] Curated dataset saved: %s", curated_path)
        run_optional_enterprise_checks(logger, spec.dataset_id, paths["reports_dir"])

    merge_outputs = {}
    working_tables = dict(curated_tables)
    for step in config.get("merge_plan", {}).get("steps", []):
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
        left_only_path = Path(paths["excluded_dir"]) / f"{merge_name}_left_only_{compact_ts()}.csv"
        right_only_path = Path(paths["excluded_dir"]) / f"{merge_name}_right_only_{compact_ts()}.csv"
        left_only.to_csv(left_only_path, index=False)
        right_only.to_csv(right_only_path, index=False)

        analytic_path = Path(paths["analytic_dir"]) / f"{merge_name}_{compact_ts()}.csv"
        merged.to_csv(analytic_path, index=False)

        working_tables[merge_name] = merged.drop(columns=["_merge"], errors="ignore")
        merge_outputs[merge_name] = {
            "analytic_path": str(analytic_path),
            "left_only_path": str(left_only_path),
            "right_only_path": str(right_only_path),
            "metrics": merge_result.metrics,
        }

    summary = {
        "run_id": run_id,
        "started_at": now_ts(),
        "dataset_count": len(config["datasets"]),
        "result_count": len(all_results),
        "merge_outputs": merge_outputs,
        "report_root": paths["reports_dir"],
        "analytic_root": paths["analytic_dir"],
    }
    summary_path = write_final_summary(summary, Path(paths["reports_dir"]) / "final_summary", f"pipeline_final_summary_{compact_ts()}.json")
    logger.info("[INFO] Pipeline completed. Summary: %s", summary_path)
    return summary
