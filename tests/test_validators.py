import pandas as pd

from clinical_data_pipeline.models import DatasetSpec
from clinical_data_pipeline.validation.validators import validate_columns, validate_primary_key, run_business_rules


def test_validate_columns_detects_missing_required():
    spec = DatasetSpec(
        dataset_id="demo",
        path="demo.csv",
        file_type="csv",
        primary_key="patient_id",
        required_columns=["patient_id", "sex"],
        optional_columns=[],
        expected_dtypes={},
    )
    df = pd.DataFrame({"patient_id": ["P1"]})
    result = validate_columns(df, spec)
    assert result.success is False
    assert any(i.check_name == "missing_columns" for i in result.issues)


def test_validate_primary_key_detects_duplicates():
    spec = DatasetSpec(
        dataset_id="demo",
        path="demo.csv",
        file_type="csv",
        primary_key="patient_id",
        required_columns=["patient_id"],
        optional_columns=[],
        expected_dtypes={},
    )
    df = pd.DataFrame({"patient_id": ["P1", "P1"]})
    result = validate_primary_key(df, spec)
    assert result.success is False
    assert result.metrics["duplicate_key_count"] == 1


def test_business_rule_between_flags_out_of_range():
    spec = DatasetSpec(
        dataset_id="visits",
        path="visits.csv",
        file_type="csv",
        primary_key="visit_id",
        required_columns=[],
        optional_columns=[],
        expected_dtypes={},
        rules=[{"name": "esspri_range", "type": "between", "column": "esspri", "min": 0, "max": 10}],
    )
    df = pd.DataFrame({"esspri": [5, 12]})
    result = run_business_rules(df, spec)
    assert result.success is False
    assert result.metrics["esspri_range"]["failing_rows"] == 1
