import pandas as pd

from clinical_data_pipeline.integrate.merge import compute_merge_metrics, perform_merge, validate_merge_keys


def test_validate_merge_keys_success():
    left = pd.DataFrame({"patient_id": ["P1"]})
    right = pd.DataFrame({"patient_id": ["P1"]})
    result = validate_merge_keys(left, right, ["patient_id"], "demo")
    assert result.success is True


def test_perform_merge_metrics():
    left = pd.DataFrame({"patient_id": ["P1", "P2"]})
    right = pd.DataFrame({"patient_id": ["P1"]})
    merged, result = perform_merge(left, right, "left", ["patient_id"], "demo")
    assert len(merged) == 2
    assert result.metrics["both_count"] == 1
    assert result.metrics["left_only_count"] == 1


def test_compute_merge_metrics_cardinality():
    left = pd.DataFrame({"id": [1, 1, 2]})
    right = pd.DataFrame({"id": [1, 2]})
    merged = left.merge(right, on=["id"], how="left", indicator=True)
    metrics = compute_merge_metrics(merged, left, right, ["id"], expected_cardinality="one_to_one")
    assert metrics["observed_cardinality"] == "many_to_one"
    assert metrics["cardinality_matches_expectation"] is False
