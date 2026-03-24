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
from .models import DatasetSpec, StepResult, ValidationIssue
from .reporting.export import write_run_manifest
from .reporting.results import write_final_summary, write_step_result
from .reporting.summary import build_final_summary
from .settings import get_settings, guess_project_root, resolve_path
from .transform.catalog import build_canonical_catalog
from .utils.core import compact_ts, ensure_dir, now_ts
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


def _canonicalize_column_name(value: str) -> str:
    normalized = str(value).strip().lower()
    for token in ["\n", "\r", "\t", "/", "-", " "]:
        normalized = normalized.replace(token, "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def _apply_text_normalization(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    out = df.copy()
    replace_newlines_with = str(rules.get("replace_newlines_with", " "))
    uppercase_all = bool(rules.get("uppercase_all", False))
    uppercase_columns = set(rules.get("uppercase_columns", []))

    for col in out.columns:
        if not (pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col])):
            continue

        series = out[col].astype("string")
        if rules.get("trim", True):
            series = series.str.strip()
        if rules.get("replace_newlines", True):
            series = series.str.replace(r"[\r\n]+", replace_newlines_with, regex=True)
        if rules.get("collapse_whitespace", True):
            series = series.str.replace(r"\s+", " ", regex=True).str.strip()
        if uppercase_all or col in uppercase_columns:
            series = series.str.upper()
        out[col] = series

    return out


def _normalize_single_workbook_with_mapping(
    df: pd.DataFrame,
    mapping_config: dict,
    reports_root: Path,
) -> tuple[pd.DataFrame, dict[str, dict], list]:
    started_at = now_ts()
    t0 = time.perf_counter()
    mapping_audit: list[dict[str, str]] = []

    raw_to_canonical = mapping_config.get("raw_to_canonical", {})
    dataset_sections = mapping_config.get("datasets", {})
    text_rules = mapping_config.get("text_normalization", {})

    if not isinstance(raw_to_canonical, dict):
        raise ValueError("single workbook mapping requires a 'raw_to_canonical' dictionary")
    if not isinstance(dataset_sections, dict):
        raise ValueError("single workbook mapping requires a 'datasets' dictionary")

    normalized = df.copy()
    original_columns = [str(col) for col in normalized.columns]
    stripped_columns = [str(col).strip() for col in original_columns]
    normalized.columns = stripped_columns

    renamed_columns: dict[str, str] = {}
    mapped_targets = set()
    target_column_pool: set[str] = set()
    for dataset_cfg in dataset_sections.values():
        target_column_pool.update(dataset_cfg.get("target_columns", []))

    for column_name in stripped_columns:
        if column_name in raw_to_canonical:
            mapped_name = raw_to_canonical[column_name]
            renamed_columns[column_name] = mapped_name
            mapped_targets.add(mapped_name)
            mapping_audit.append(
                {
                    "raw_column": column_name,
                    "mapped_column": mapped_name,
                    "mapping_type": "manual",
                    "details": "mapped via raw_to_canonical",
                }
            )
            continue

        auto_candidate = _canonicalize_column_name(column_name)
        if auto_candidate and auto_candidate not in mapped_targets and (
            not target_column_pool or auto_candidate in target_column_pool
        ):
            renamed_columns[column_name] = auto_candidate
            mapped_targets.add(auto_candidate)
            mapping_audit.append(
                {
                    "raw_column": column_name,
                    "mapped_column": auto_candidate,
                    "mapping_type": "automatic",
                    "details": "mapped by canonicalized header fallback",
                }
            )
        else:
            mapping_audit.append(
                {
                    "raw_column": column_name,
                    "mapped_column": "",
                    "mapping_type": "unmapped",
                    "details": "no manual mapping and automatic mapping not applicable",
                }
            )

    normalized = normalized.rename(columns=renamed_columns)
    normalized = _apply_text_normalization(normalized, text_rules)

    domain_mappings: dict[str, dict] = {}
    required_issues = []
    for dataset_name, dataset_cfg in dataset_sections.items():
        target_columns = dataset_cfg.get("target_columns", [])
        required_columns = dataset_cfg.get("required_columns", [])
        fallback = str(dataset_cfg.get("on_missing_required", "error")).lower()
        missing_required = [col for col in required_columns if col not in normalized.columns]

        domain_mappings[dataset_name] = {
            "columns": target_columns,
            "rename": dataset_cfg.get("rename", {}),
        }

        if missing_required and fallback == "error":
            required_issues.append(
                {
                    "dataset_id": dataset_name,
                    "stage": "normalization",
                    "check_name": "required_columns_present",
                    "severity": "ERROR",
                    "message": f"Missing required mapped columns: {missing_required}",
                    "column_name": ",".join(missing_required),
                    "row_count": len(missing_required),
                    "output_path": None,
                }
            )

    audit_path = reports_root / "single_workbook" / f"single_workbook_column_mapping_audit_{compact_ts()}.csv"
    ensure_dir(audit_path.parent)
    pd.DataFrame(mapping_audit).to_csv(audit_path, index=False)

    auto_mapped_count = sum(1 for item in mapping_audit if item["mapping_type"] == "automatic")
    unmapped_count = sum(1 for item in mapping_audit if item["mapping_type"] == "unmapped")
    step_result = StepResult(
        step_name="single_workbook_normalization",
        success=len(required_issues) == 0,
        started_at=started_at,
        finished_at=now_ts(),
        duration_seconds=round(time.perf_counter() - t0, 4),
        dataset_id="single_workbook_input",
        metrics={
            "raw_column_count": len(stripped_columns),
            "mapped_column_count": len(renamed_columns),
            "automatic_mapped_count": auto_mapped_count,
            "unmapped_column_count": unmapped_count,
        },
        artifacts={"column_mapping_audit_csv": str(audit_path)},
        issues=[ValidationIssue(**issue) for issue in required_issues],
    )

    return normalized, domain_mappings, [step_result]


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
    raw_dir = resolve_path(paths["raw_dir"], project_root)
    auto_detect_path = raw_dir / "CTDB Data Download.xlsx"
    auto_detect_enabled = single_workbook_cfg.get("auto_detect", True)
    auto_detected_single_workbook = bool(auto_detect_enabled and auto_detect_path.exists())
    manual_single_workbook_enabled = bool(single_workbook_cfg.get("enabled", False))
    single_workbook_enabled = bool(auto_detected_single_workbook or manual_single_workbook_enabled)
    selected_input_source_path: Path | None = None
    selected_input_mode = "per_file_datasets"

    if auto_detected_single_workbook:
        selected_input_source_path = auto_detect_path
        selected_input_mode = "single_workbook"
        logger.info(
            "[INFO] Input source selected: single workbook auto-detected at %s; per-file inputs will be derived from this workbook.",
            selected_input_source_path,
        )
    elif manual_single_workbook_enabled:
        workbook_cfg_path = single_workbook_cfg.get("path")
        if workbook_cfg_path is None:
            raise ValueError("single_workbook_input.enabled is true but no 'path' is configured")
        selected_input_source_path = resolve_path(workbook_cfg_path, project_root)
        selected_input_mode = "single_workbook"
        logger.info(
            "[INFO] Input source selected: single workbook from config path %s.",
            selected_input_source_path,
        )
    else:
        logger.info(
            "[INFO] Input source selected: per-file datasets (single workbook auto-detect=%s, candidate=%s).",
            bool(auto_detect_enabled),
            auto_detect_path,
        )

    run_context["input_mode"] = {
        "mode": selected_input_mode,
        "single_workbook_auto_detected": auto_detected_single_workbook,
        "selected_source_path": str(selected_input_source_path) if selected_input_source_path is not None else None,
    }

    if single_workbook_enabled:
        input_layout = str(single_workbook_cfg.get("input_layout", "")).strip().lower()
        is_clean_dataframe_layout = input_layout == "clean_dataframe"

        workbook_path = selected_input_source_path if selected_input_source_path is not None else auto_detect_path
        workbook_file_type = single_workbook_cfg.get("file_type", "xlsx")
        workbook_header_strategy = single_workbook_cfg.get("header_strategy")
        workbook_demographics_column_end = "N"
        workbook_demographics_header_row = 3
        workbook_clinical_header_row = 2
        workbook_skip_rows_after_header = 0
        if is_clean_dataframe_layout:
            workbook_header_strategy = None
            if str(workbook_file_type).lower() == "ctdb_merged_excel":
                workbook_file_type = "xlsx"
        else:
            workbook_demographics_column_end = single_workbook_cfg.get("demographics_column_end", "N")
            workbook_demographics_header_row = single_workbook_cfg.get("demographics_header_row", 3)
            workbook_clinical_header_row = single_workbook_cfg.get("clinical_header_row", 2)
            workbook_skip_rows_after_header = single_workbook_cfg.get("skip_rows_after_header", 0)

        single_workbook_spec = DatasetSpec(
            dataset_id="single_workbook_input",
            path=workbook_path,
            file_type=workbook_file_type,
            primary_key=single_workbook_cfg.get("primary_key"),
            required_columns=[],
            optional_columns=[],
            expected_dtypes={},
            sheet_name=single_workbook_cfg.get("sheet_name", 0),
            header_strategy=workbook_header_strategy,
            demographics_column_end=workbook_demographics_column_end,
            demographics_header_row=workbook_demographics_header_row,
            clinical_header_row=workbook_clinical_header_row,
            skip_rows_after_header=workbook_skip_rows_after_header,
        )
        files_read.append(collect_file_metadata(workbook_path))
        base_df = read_table_from_spec(single_workbook_spec)
        base_df = _normalize_single_workbook_base(base_df, single_workbook_cfg.get("patient_id_column", "patient_id"))

        domain_mappings = single_workbook_cfg.get("domains", {})
        mapping_config_path = single_workbook_cfg.get("mapping_config")
        if mapping_config_path:
            mapping_cfg = load_yaml(resolve_path(mapping_config_path, project_root))
            base_df, mapping_domains, normalization_results = _normalize_single_workbook_with_mapping(
                base_df,
                mapping_cfg,
                reports_root,
            )
            domain_mappings = mapping_domains
            for result in normalization_results:
                all_results.append(result)
                generated_artifacts.update(result.artifacts)
                write_step_result(result, reports_root / "single_workbook")
            if config.get("settings", {}).get("fail_fast", False) and normalization_results and not normalization_results[0].success:
                raise ValueError("Single workbook normalization failed due to missing required columns")

        derived_views = _derive_domain_views(base_df, domain_mappings)

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
