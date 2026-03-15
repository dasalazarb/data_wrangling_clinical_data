from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_table(df: pd.DataFrame, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=False)
    elif suffix == ".xlsx":
        df.to_excel(path, index=False)
    elif suffix == ".parquet":
        df.to_parquet(path, index=False)
    else:
        raise ValueError(f"Unsupported output type: {suffix}")
    return path
