import pandas as pd
import pytest

from clinical_data_pipeline.orchestrator import _derive_domain_views, _normalize_single_workbook_base


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
