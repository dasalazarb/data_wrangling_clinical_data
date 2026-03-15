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
        return pd.read_csv(path, sep="	")
    raise ValueError(f"Unsupported file type: {suffix}")


def write_table(df: pd.DataFrame, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=False)
    elif suffix == ".xlsx":
        df.to_excel(path, index=False)
    else:
        raise ValueError(f"Unsupported output type: {suffix}")
    return path
