from __future__ import annotations

from pathlib import Path
import re
import unicodedata

import pandas as pd


def _clean_header_name(value: object, fallback: str) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        cleaned = ""
    else:
        normalized = unicodedata.normalize("NFKC", str(value))
        cleaned = re.sub(r"[\u0000-\u001F\u007F]+", " ", normalized)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or fallback


def _make_unique_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    unique: list[str] = []
    for name in headers:
        seen[name] = seen.get(name, 0) + 1
        if seen[name] == 1:
            unique.append(name)
            continue
        unique.append(f"{name}_{seen[name]}")
    return unique


def read_ctdb_merged_excel(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    raw_df = pd.read_excel(path, header=None)

    if raw_df.shape[0] < 3:
        raise ValueError(
            f"CTDB merged workbook '{path}' must include at least 3 header rows; got {raw_df.shape[0]} rows."
        )

    n_columns = raw_df.shape[1]
    if n_columns == 0:
        raise ValueError(f"CTDB merged workbook '{path}' does not contain any columns.")

    a_last_idx = min(13, n_columns - 1)
    headers: list[str] = []
    empty_a_columns: list[int] = []
    empty_o_plus_columns: list[int] = []

    for col_idx in range(n_columns):
        if col_idx <= a_last_idx:
            header_value = raw_df.iat[2, col_idx]
            header_name = _clean_header_name(header_value, f"col_{col_idx + 1}")
            if header_name == f"col_{col_idx + 1}":
                empty_a_columns.append(col_idx)
        else:
            header_value = raw_df.iat[1, col_idx]
            header_name = _clean_header_name(header_value, f"col_{col_idx + 1}")
            if header_name == f"col_{col_idx + 1}":
                empty_o_plus_columns.append(col_idx)
        headers.append(header_name)

    if empty_a_columns:
        a_cols = ", ".join(str(i + 1) for i in empty_a_columns)
        raise ValueError(
            f"CTDB merged workbook '{path}' has empty demographic headers in row 3 for columns: {a_cols}."
        )

    if empty_o_plus_columns:
        o_cols = ", ".join(str(i + 1) for i in empty_o_plus_columns)
        raise ValueError(
            f"CTDB merged workbook '{path}' has empty variable headers in row 2 for columns: {o_cols}."
        )

    data = raw_df.iloc[3:].reset_index(drop=True)
    data.columns = _make_unique_headers(headers)
    return data


def read_table(path: str | Path, file_type: str | None = None) -> pd.DataFrame:
    path = Path(path)
    suffix = (file_type or path.suffix.lower().lstrip(".")).lower()
    if suffix == "ctdb_merged_excel":
        return read_ctdb_merged_excel(path)
    if suffix == "csv":
        return pd.read_csv(path)
    if suffix in {"xlsx", "xls"}:
        return pd.read_excel(path)
    if suffix == "tsv":
        return pd.read_csv(path, sep="\t")
    if suffix == "parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {suffix}")
