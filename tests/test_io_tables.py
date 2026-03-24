import pytest
from pathlib import Path

import pandas as pd

from clinical_data_pipeline.io.readers import read_ctdb_merged_excel, read_table
from clinical_data_pipeline.io.writers import write_table


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
