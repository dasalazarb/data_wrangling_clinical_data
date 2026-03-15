from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_table(path: str | Path, file_type: str | None = None) -> pd.DataFrame:
    path = Path(path)
    suffix = (file_type or path.suffix.lower().lstrip(".")).lower()
    if suffix == "csv":
        return pd.read_csv(path)
    if suffix in {"xlsx", "xls"}:
        return pd.read_excel(path)
    if suffix == "tsv":
        return pd.read_csv(path, sep="\t")
    if suffix == "parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {suffix}")
