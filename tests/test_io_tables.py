import pytest
from pathlib import Path

import pandas as pd

from clinical_data_pipeline.io.readers import read_table
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
