import pytest
from pathlib import Path

import pandas as pd

from clinical_data_pipeline.io.readers import read_ctdb_merged_excel, read_table, read_table_from_spec
from clinical_data_pipeline.io.writers import write_table
from clinical_data_pipeline.models import DatasetSpec


def test_write_and_read_parquet_roundtrip(tmp_path: Path):
    pytest.importorskip("pyarrow")
    df = pd.DataFrame({"patient_id": [1, 2], "visit": ["A", "B"]})
    path = tmp_path / "roundtrip.parquet"

    write_table(df, path)
    loaded = read_table(path)

    pd.testing.assert_frame_equal(loaded, df)


def test_read_table_parquet_with_explicit_file_type(tmp_path: Path):
    pytest.importorskip("pyarrow")
    df = pd.DataFrame({"x": [1, 2, 3]})
    path = tmp_path / "table.data"
    df.to_parquet(path, index=False)

    loaded = read_table(path, "parquet")

    pd.testing.assert_frame_equal(loaded, df)


def test_read_ctdb_merged_excel_builds_headers_and_drops_header_rows(tmp_path: Path):
    path = tmp_path / "ctdb.xlsx"
    raw = pd.DataFrame(
        [
            ["meta"] * 16,
            [f"row2_{i}" for i in range(14)] + ["VAR A", "VAR A"],
            [f"Demo {i}" for i in range(14)] + ["desc a", "desc b"],
            ["p1"] + [1] * 15,
            ["p2"] + [2] * 15,
        ]
    )
    raw.to_excel(path, header=False, index=False)

    loaded = read_ctdb_merged_excel(path)

    assert loaded.shape == (2, 16)
    assert loaded.columns[0] == "Demo 0"
    assert loaded.columns[14] == "VAR A"
    assert loaded.columns[15] == "VAR A_2"
    assert loaded.iloc[0, 0] == "p1"
    assert loaded.iloc[1, 0] == "p2"


def test_read_table_routes_ctdb_merged_excel(tmp_path: Path):
    path = tmp_path / "ctdb.xlsx"
    raw = pd.DataFrame(
        [
            ["meta"] * 15,
            [f"row2_{i}" for i in range(14)] + ["VAR"],
            [f"Demo {i}" for i in range(14)] + ["desc"],
            ["p1"] + [1] * 14,
        ]
    )
    raw.to_excel(path, header=False, index=False)

    loaded = read_table(path, "ctdb_merged_excel")

    assert loaded.columns[14] == "VAR"
    assert loaded.iloc[0, 0] == "p1"


def test_read_table_from_spec_routes_ctdb_header_strategy(tmp_path: Path):
    path = tmp_path / "ctdb_strategy.xlsx"
    raw = pd.DataFrame(
        [
            ["meta"] * 5,
            ["ignore"] * 5,
            ["demo_1", "demo_2", "demo_3", "clin_a", "clin_b"],
            ["skip"] * 5,
            ["p1", "a", "b", 1, 2],
        ]
    )
    raw.to_excel(path, header=False, index=False)
    spec = DatasetSpec(
        dataset_id="ctdb",
        path=path,
        file_type="xlsx",
        primary_key=None,
        required_columns=[],
        optional_columns=[],
        expected_dtypes={},
        header_strategy="ctdb_merged_v1",
        demographics_column_end="C",
        demographics_header_row=3,
        clinical_header_row=3,
        skip_rows_after_header=1,
    )

    loaded = read_table_from_spec(spec)

    assert list(loaded.columns) == ["demo_1", "demo_2", "demo_3", "clin_a", "clin_b"]
    assert loaded.iloc[0, 0] == "p1"


def test_read_table_from_spec_keeps_backward_compat_for_normal_xlsx(tmp_path: Path):
    path = tmp_path / "normal.xlsx"
    expected = pd.DataFrame({"patient_id": ["p1"], "value": [10]})
    expected.to_excel(path, index=False)
    spec = DatasetSpec(
        dataset_id="normal",
        path=path,
        file_type="xlsx",
        primary_key=None,
        required_columns=[],
        optional_columns=[],
        expected_dtypes={},
    )

    loaded = read_table_from_spec(spec)

    pd.testing.assert_frame_equal(loaded, expected)


def test_read_ctdb_merged_excel_raises_on_missing_required_headers(tmp_path: Path):
    path = tmp_path / "ctdb_invalid.xlsx"
    raw = pd.DataFrame(
        [
            ["meta"] * 15,
            [f"row2_{i}" for i in range(14)] + [""],
            [f"Demo {i}" for i in range(13)] + [""] + ["desc"],
            ["p1"] + [1] * 14,
        ]
    )
    raw.to_excel(path, header=False, index=False)

    with pytest.raises(ValueError, match="demographic headers"):
        read_ctdb_merged_excel(path)
