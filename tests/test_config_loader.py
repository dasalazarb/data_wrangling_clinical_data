from pathlib import Path

from clinical_data_pipeline.io.config_loader import load_dataset_spec


def test_load_dataset_spec_applies_safe_defaults(tmp_path: Path):
    cfg_path = tmp_path / "dataset.yaml"
    cfg_path.write_text(
        "\n".join(
            [
                "dataset_id: demo",
                "path: data/raw/demo.csv",
                "file_type: csv",
                "primary_key: patient_id",
                "required_columns: [patient_id]",
                "optional_columns: []",
                "expected_dtypes: {}",
            ]
        ),
        encoding="utf-8",
    )

    spec = load_dataset_spec(cfg_path)

    assert spec.sheet_name is None
    assert spec.header_strategy is None
    assert spec.demographics_column_end == "N"
    assert spec.demographics_header_row == 3
    assert spec.clinical_header_row == 2
    assert spec.skip_rows_after_header == 0


def test_load_dataset_spec_reads_custom_reading_configuration(tmp_path: Path):
    cfg_path = tmp_path / "dataset.yaml"
    cfg_path.write_text(
        "\n".join(
            [
                "dataset_id: ctdb",
                "path: data/input_catalog/variables_merge_once_quince.xlsx",
                "file_type: xlsx",
                "primary_key: patient_id",
                "required_columns: [patient_id]",
                "optional_columns: []",
                "expected_dtypes: {}",
                "sheet_name: Sheet1",
                "header_strategy: ctdb_merged_v1",
                "demographics_column_end: N",
                "demographics_header_row: 3",
                "clinical_header_row: 2",
                "skip_rows_after_header: 1",
            ]
        ),
        encoding="utf-8",
    )

    spec = load_dataset_spec(cfg_path)

    assert spec.sheet_name == "Sheet1"
    assert spec.header_strategy == "ctdb_merged_v1"
    assert spec.demographics_column_end == "N"
    assert spec.demographics_header_row == 3
    assert spec.clinical_header_row == 2
    assert spec.skip_rows_after_header == 1
