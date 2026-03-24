import pandas as pd
import pytest

from clinical_data_pipeline.orchestrator import (
    _derive_domain_views,
    _normalize_single_workbook_base,
    _normalize_single_workbook_with_mapping,
)


def test_normalize_single_workbook_base_removes_empty_rows_and_missing_patients():
    raw = pd.DataFrame(
        {
            " patient_id ": [None, "P1", None, "P2"],
            " value ": [None, 10, None, 20],
        }
    )

    normalized = _normalize_single_workbook_base(raw, patient_id_column="patient_id")

    assert list(normalized.columns) == ["patient_id", "value"]
    assert normalized["patient_id"].tolist() == ["P1", "P2"]


def test_derive_domain_views_selects_expected_columns():
    base = pd.DataFrame(
        {
            "patient_id": ["P1"],
            "visit_id": ["V1"],
            "visit_date": ["2026-01-01"],
            "essdai": [3],
            "esspri": [2],
        }
    )
    mappings = {
        "visits": {
            "columns": ["visit_id", "patient_id", "visit_date", "essdai", "esspri"],
        }
    }

    derived = _derive_domain_views(base, mappings)

    assert "visits" in derived
    assert list(derived["visits"].columns) == ["visit_id", "patient_id", "visit_date", "essdai", "esspri"]


def test_derive_domain_views_raises_for_missing_columns():
    base = pd.DataFrame({"patient_id": ["P1"]})

    with pytest.raises(ValueError, match="missing columns"):
        _derive_domain_views(base, {"labs": {"columns": ["lab_id", "patient_id"]}})


def test_normalize_single_workbook_with_mapping_applies_manual_auto_and_audit(tmp_path):
    raw = pd.DataFrame(
        {
            "Patient ID": ["p-01"],
            "Visit ID": ["v-1"],
            "Visit Score": [" 12 "],
            "Notes Extra": [" line 1\nline 2 "],
        }
    )
    mapping = {
        "raw_to_canonical": {"Patient ID": "patient_id", "Visit ID": "visit_id"},
        "datasets": {
            "visits": {
                "target_columns": ["patient_id", "visit_id", "visit_score"],
                "required_columns": ["patient_id", "visit_id"],
                "on_missing_required": "error",
            }
        },
        "text_normalization": {"trim": True, "replace_newlines": True, "replace_newlines_with": " ", "collapse_whitespace": True},
    }

    normalized, domains, results = _normalize_single_workbook_with_mapping(raw, mapping, tmp_path)

    assert "patient_id" in normalized.columns
    assert "visit_id" in normalized.columns
    assert "visit_score" in normalized.columns
    assert "Notes Extra" in normalized.columns
    assert normalized.loc[0, "Notes Extra"] == "line 1 line 2"
    assert normalized.loc[0, "visit_score"] == "12"
    assert domains["visits"]["columns"] == ["patient_id", "visit_id", "visit_score"]
    assert results[0].success is True
    assert "column_mapping_audit_csv" in results[0].artifacts


def test_normalize_single_workbook_with_mapping_fails_early_on_missing_required(tmp_path):
    raw = pd.DataFrame({"Patient ID": ["p-01"]})
    mapping = {
        "raw_to_canonical": {"Patient ID": "patient_id"},
        "datasets": {
            "labs": {
                "target_columns": ["patient_id", "lab_id"],
                "required_columns": ["patient_id", "lab_id"],
                "on_missing_required": "error",
            }
        },
        "text_normalization": {"trim": True},
    }

    _normalized, _domains, results = _normalize_single_workbook_with_mapping(raw, mapping, tmp_path)

    assert results[0].success is False
    assert any("Missing required mapped columns" in issue.message for issue in results[0].issues)
