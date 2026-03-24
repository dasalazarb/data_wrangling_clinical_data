from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path

import pandas as pd

from .io.config_loader import load_dataset_spec, load_yaml
from .io.manifest import build_run_context, collect_file_metadata
from .io.readers import read_table_from_spec
from .io.writers import write_table
from .integrate.enterprise import run_optional_enterprise_checks
from .integrate.merge import extract_unmatched, perform_merge, validate_merge_keys
from .models import DatasetSpec
from .reporting.export import write_run_manifest
from .reporting.results import write_final_summary, write_step_result
from .reporting.summary import build_final_summary
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


def _normalize_single_workbook_base(df: pd.DataFrame, patient_id_column: str | None = "patient_id") -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(col).strip() for col in normalized.columns]
    normalized = normalized.dropna(how="all").reset_index(drop=True)
    if patient_id_column and patient_id_column in normalized.columns:
        normalized = normalized.dropna(subset=[patient_id_column]).reset_index(drop=True)
    return normalized


def _derive_domain_views(base_df: pd.DataFrame, domain_mappings: dict[str, dict]) -> dict[str, pd.DataFrame]:
    derived: dict[str, pd.DataFrame] = {}
    for domain_name, mapping in domain_mappings.items():
        columns = mapping.get("columns", [])
        rename_map = mapping.get("rename", {})
        missing = [col for col in columns if col not in base_df.columns]
        if missing:
            raise ValueError(f"Domain '{domain_name}' references missing columns in single workbook input: {missing}")
        view = base_df.loc[:, columns].rename(columns=rename_map).copy()
        derived[domain_name] = view
    return derived


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
    pipeline_t0 = time.perf_counter()
    project_root = Path(project_root or guess_project_root(config_path))
    config_obj = get_settings(config_path=config_path, project_root=project_root)
    config = config_obj.model_dump(mode="json")

    run_context = build_run_context(config, config_path)
    run_id = run_context["run_id"]
    paths = config["paths"]
    logger = build_logger(paths["logs_dir"], run_id, config.get("settings", {}).get("log_level", "INFO"))
    logger.info("[INFO] Starting patient-data pipeline")

    versioned_paths = run_context["output_roots"]["versioned"]
    for key in ["staging_dir", "curated_dir", "analytic_dir", "excluded_dir", "reports_dir", "logs_dir"]:
        ensure_dir(versioned_paths.get(key, paths[key]))

    all_results = []
    curated_tables: dict[str, pd.DataFrame] = {}
    files_read = [collect_file_metadata(config_path)]
    generated_datasets: dict[str, dict[str, str]] = {"staging": {}, "curated": {}, "analytic": {}, "excluded": {}}
    generated_artifacts: dict[str, str] = {}
    derived_views: dict[str, pd.DataFrame] = {}
    single_workbook_spec: DatasetSpec | None = None

    reports_root = Path(versioned_paths.get("reports_dir", paths["reports_dir"]))
    staging_root = Path(versioned_paths.get("staging_dir", paths["staging_dir"]))
    curated_root = Path(versioned_paths.get("curated_dir", paths["curated_dir"]))
    analytic_root = Path(versioned_paths.get("analytic_dir", paths["analytic_dir"]))
    excluded_root = Path(versioned_paths.get("excluded_dir", paths["excluded_dir"]))

    single_workbook_cfg = config.get("single_workbook_input", {})
    if single_workbook_cfg.get("enabled", False):
        workbook_path = resolve_path(single_workbook_cfg["path"], project_root)
        single_workbook_spec = DatasetSpec(
            dataset_id="single_workbook_input",
            path=workbook_path,
            file_type=single_workbook_cfg.get("file_type", "xlsx"),
            primary_key=single_workbook_cfg.get("primary_key"),
            required_columns=[],
            optional_columns=[],
            expected_dtypes={},
            sheet_name=single_workbook_cfg.get("sheet_name", 0),
            header_strategy=single_workbook_cfg.get("header_strategy"),
            demographics_column_end=single_workbook_cfg.get("demographics_column_end", "N"),
            demographics_header_row=single_workbook_cfg.get("demographics_header_row", 3),
            clinical_header_row=single_workbook_cfg.get("clinical_header_row", 2),
            skip_rows_after_header=single_workbook_cfg.get("skip_rows_after_header", 0),
        )
        files_read.append(collect_file_metadata(workbook_path))
        base_df = read_table_from_spec(single_workbook_spec)
        base_df = _normalize_single_workbook_base(base_df, single_workbook_cfg.get("patient_id_column", "patient_id"))
        derived_views = _derive_domain_views(base_df, single_workbook_cfg.get("domains", {}))

    for spec_path in config["datasets"]:
        spec = load_dataset_spec(resolve_path(spec_path, project_root))
        spec.path = resolve_path(spec.path, project_root)
        logger.info("[INFO] Processing dataset: %s", spec.dataset_id)

        files_read.append(collect_file_metadata(spec_path))
        if spec.dataset_id not in derived_views:
            files_read.append(collect_file_metadata(spec.path))

        input_spec = replace(spec, path=single_workbook_spec.path, file_type=single_workbook_spec.file_type) if single_workbook_spec and spec.dataset_id in derived_views else spec
        input_result = validate_file_input(input_spec)
        all_results.append(input_result)
        write_step_result(input_result, reports_root / spec.dataset_id)
        if not input_result.success and config.get("settings", {}).get("fail_fast", False):
            raise FileNotFoundError(f"Critical input error for {spec.dataset_id}")

        if spec.dataset_id in derived_views:
            df = derived_views[spec.dataset_id].copy()
        else:
            df = read_table_from_spec(spec)
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
            write_step_result(result, reports_root / spec.dataset_id)

        staging_path = staging_root / f"{spec.dataset_id}_{compact_ts()}.parquet"
        write_table(df, staging_path)
        generated_datasets["staging"][spec.dataset_id] = str(staging_path)

        curated_path = curated_root / f"{spec.dataset_id}_{compact_ts()}.parquet"
        write_table(df, curated_path)
        generated_datasets["curated"][spec.dataset_id] = str(curated_path)

        curated_tables[spec.dataset_id] = df
        logger.info("[INFO] Staging dataset saved: %s", staging_path)
        logger.info("[INFO] Curated dataset saved: %s", curated_path)
        run_optional_enterprise_checks(logger, spec.dataset_id, str(reports_root))

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
        write_step_result(key_result, reports_root / "merge_quality")

        merged, merge_result = perform_merge(working_tables[left_name], working_tables[right_name], step["how"], merge_keys, merge_name)
        all_results.append(merge_result)
        write_step_result(merge_result, reports_root / "merge_quality")

        left_only, right_only = extract_unmatched(merged)
        analytic_path = analytic_root / f"{merge_name}_{compact_ts()}.parquet"
        left_path = excluded_root / f"{merge_name}_left_only_{compact_ts()}.csv"
        right_path = excluded_root / f"{merge_name}_right_only_{compact_ts()}.csv"
        write_table(merged.drop(columns=["_merge"]), analytic_path)
        write_table(left_only, left_path)
        write_table(right_only, right_path)

        generated_datasets["analytic"][merge_name] = str(analytic_path)
        generated_datasets["excluded"][f"{merge_name}_left_only"] = str(left_path)
        generated_datasets["excluded"][f"{merge_name}_right_only"] = str(right_path)

        merge_outputs[merge_name] = {
            "analytic_path": str(analytic_path),
            "left_only_path": str(left_path),
            "right_only_path": str(right_path),
            "merge_metrics": merge_result.metrics,
        }

        working_tables[merge_name] = merged.drop(columns=["_merge"])

    duration_seconds = round(time.perf_counter() - pipeline_t0, 4)
    manifest_path = write_run_manifest(
        run_context,
        files_read=files_read,
        validation_results=all_results,
        generated_datasets=generated_datasets,
        generated_artifacts=generated_artifacts,
        exclusions={
            "left_only_total": int(sum(metrics.get("left_only_count", 0) for metrics in [m["merge_metrics"] for m in merge_outputs.values()])),
            "right_only_total": int(sum(metrics.get("right_only_count", 0) for metrics in [m["merge_metrics"] for m in merge_outputs.values()])),
            "paths": generated_datasets["excluded"],
        },
        merge_metrics={name: payload["merge_metrics"] for name, payload in merge_outputs.items()},
        duration_seconds=duration_seconds,
        out_dir=Path(paths["reports_dir"]) / "manifests",
    )
    generated_artifacts["run_manifest"] = str(manifest_path)

    manifest_payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    final_summary = build_final_summary(manifest_payload)
    final_summary.update(
        {
            "project_name": config.get("project_name", "clinical_pipeline"),
            "step_count": len(all_results),
            "steps_failed": [r.step_name for r in all_results if not r.success],
            "report_root": str(reports_root),
            "analytic_root": str(analytic_root),
            "manifest_path": str(manifest_path),
            "merge_outputs": merge_outputs,
        }
    )

    summary_path = write_final_summary(
        final_summary,
        reports_root / "final_summary",
        f"pipeline_final_summary_{compact_ts()}.json",
    )
    logger.info("[INFO] Pipeline completed. Summary: %s", summary_path)
    return final_summary
