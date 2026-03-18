from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

from ..models import StepResult, ValidationIssue
from ..utils.core import now_ts, ensure_dir


def _is_selected(value) -> bool:
    if pd.isna(value):
        return False
    value = str(value).strip().lower()
    return value in {"1", "true", "yes", "y", "x", "selected"}


def build_canonical_catalog(config: dict, logger):
    catalog_cfg = config["catalog"]
    path = Path(catalog_cfg["input_path"])
    sheet = catalog_cfg.get("input_sheet", "variables_merged")
    select_col = catalog_cfg.get("select_column", "SELECT_FOR_PIPELINE")
    qname_col = catalog_cfg.get("question_name_column", "QUESTION_NAME")
    default_mode = catalog_cfg.get("default_selection_mode", "both_studies")

    t0 = time.perf_counter()
    started_at = now_ts()
    df = pd.read_excel(path, sheet_name=sheet)
    issues: list[ValidationIssue] = []

    required_cols = [qname_col, "MERGE_STATUS", "PRESENT_ONCE", "PRESENT_QUINCE"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Catalog sheet is missing columns: {missing}")

    if select_col in df.columns:
        explicit = df[select_col].map(_is_selected)
    else:
        explicit = pd.Series(False, index=df.index)

    if default_mode == "both_studies":
        auto = df["MERGE_STATUS"].eq("both_studies")
    else:
        auto = pd.Series(True, index=df.index)

    df["selected_for_pipeline"] = explicit | auto
    df["canonical_variable_name"] = df[qname_col].astype(str).str.strip()
    df["source_studies"] = df[["PRESENT_ONCE", "PRESENT_QUINCE"]].apply(
        lambda r: ",".join([x for x, ok in zip(["once", "quince"], r.tolist()) if bool(ok)]), axis=1
    )
    df["needs_harmonization_review"] = (
        (df.get("QUESTION_TEXTS_MATCH", 1).fillna(1) != 1) |
        (df.get("ANSWER_FORMATS_MATCH", 1).fillna(1) != 1) |
        (df.get("ANSWER_RANGES_MATCH", 1).fillna(1) != 1) |
        (df.get("DISPLAY_OPTIONS_MATCH", 1).fillna(1) != 1) |
        (df.get("FORM_NAMES_MATCH", 1).fillna(1) != 1)
    )

    keep_cols = [
        "selected_for_pipeline",
        "canonical_variable_name",
        qname_col,
        "MERGE_STATUS",
        "source_studies",
        "needs_harmonization_review",
        "once_form_names",
        "quince_form_names",
        "once_question_texts",
        "quince_question_texts",
        "once_answer_formats",
        "quince_answer_formats",
        "once_answer_ranges",
        "quince_answer_ranges",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    out = df[keep_cols].copy().sort_values(["selected_for_pipeline", "canonical_variable_name"], ascending=[False, True])

    reports_dir = Path(config["paths"]["reports_dir"]) / "variable_catalog"
    curated_dir = Path(config["paths"].get("curated_dir", "data/curated")) / "variable_catalog"
    ensure_dir(reports_dir)
    ensure_dir(curated_dir)

    stamp = started_at[:19].replace(":", "").replace("-", "")
    curated_path = curated_dir / f"{catalog_cfg.get('output_basename', 'canonical_variable_catalog')}_{stamp}.parquet"
    summary_path = reports_dir / f"variable_catalog_summary_{stamp}.csv"
    out.to_parquet(curated_path, index=False)

    summary = pd.DataFrame([
        {"metric": "input_rows", "value": len(df)},
        {"metric": "selected_rows", "value": int(out["selected_for_pipeline"].sum())},
        {"metric": "both_studies", "value": int((df["MERGE_STATUS"] == "both_studies").sum())},
        {"metric": "once_only", "value": int((df["MERGE_STATUS"] == "once_only").sum())},
        {"metric": "quince_only", "value": int((df["MERGE_STATUS"] == "quince_only").sum())},
        {"metric": "harmonization_review_needed", "value": int(out["needs_harmonization_review"].sum())},
    ])
    summary.to_csv(summary_path, index=False)

    result = StepResult(
        step_name="variable_catalog_build",
        success=True,
        started_at=started_at,
        finished_at=now_ts(),
        duration_seconds=round(time.perf_counter() - t0, 4),
        dataset_id="variable_catalog",
        metrics={m["metric"]: m["value"] for m in summary.to_dict(orient="records")},
        issues=issues,
        artifacts={"catalog": str(curated_path), "summary": str(summary_path)},
    )
    logger.info("[INFO] Variable catalog built: %s", curated_path)
    return out, result
