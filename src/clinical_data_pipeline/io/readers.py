from __future__ import annotations

from pathlib import Path
import re
import unicodedata

import pandas as pd

from ..models import DatasetSpec


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


def _excel_column_to_index(column_name: str) -> int:
    normalized = column_name.strip().upper()
    if not normalized or not normalized.isalpha():
        raise ValueError(f"Invalid Excel column name: {column_name!r}")

    index = 0
    for char in normalized:
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def read_ctdb_merged_excel(
    path: str | Path,
    *,
    sheet_name: str | int | None = 0,
    demographics_column_end: str = "N",
    demographics_header_row: int = 3,
    clinical_header_row: int = 2,
    skip_rows_after_header: int = 0,
) -> pd.DataFrame:
    path = Path(path)
    raw_df = pd.read_excel(path, header=None, sheet_name=sheet_name)

    min_header_row = max(demographics_header_row, clinical_header_row)
    if raw_df.shape[0] < min_header_row:
        raise ValueError(
            f"CTDB merged workbook '{path}' must include at least {min_header_row} rows; got {raw_df.shape[0]} rows."
        )

    n_columns = raw_df.shape[1]
    if n_columns == 0:
        raise ValueError(f"CTDB merged workbook '{path}' does not contain any columns.")

    demographics_last_idx = min(_excel_column_to_index(demographics_column_end), n_columns - 1)
    demographics_header_idx = demographics_header_row - 1
    clinical_header_idx = clinical_header_row - 1
    headers: list[str] = []
    empty_a_columns: list[int] = []
    empty_o_plus_columns: list[int] = []

    for col_idx in range(n_columns):
        if col_idx <= demographics_last_idx:
            header_value = raw_df.iat[demographics_header_idx, col_idx]
            header_name = _clean_header_name(header_value, f"col_{col_idx + 1}")
            if header_name == f"col_{col_idx + 1}":
                empty_a_columns.append(col_idx)
        else:
            header_value = raw_df.iat[clinical_header_idx, col_idx]
            header_name = _clean_header_name(header_value, f"col_{col_idx + 1}")
            if header_name == f"col_{col_idx + 1}":
                empty_o_plus_columns.append(col_idx)
        headers.append(header_name)

    if empty_a_columns:
        a_cols = ", ".join(str(i + 1) for i in empty_a_columns)
        raise ValueError(
            f"CTDB merged workbook '{path}' has empty demographic headers in row {demographics_header_row} for columns: {a_cols}."
        )

    if empty_o_plus_columns:
        o_cols = ", ".join(str(i + 1) for i in empty_o_plus_columns)
        raise ValueError(
            f"CTDB merged workbook '{path}' has empty variable headers in row {clinical_header_row} for columns: {o_cols}."
        )

    data_start_idx = max(demographics_header_idx, clinical_header_idx) + 1 + max(skip_rows_after_header, 0)
    data = raw_df.iloc[data_start_idx:].reset_index(drop=True)
    data.columns = _make_unique_headers(headers)
    return data


def read_table(path: str | Path, file_type: str | None = None, sheet_name: str | int | None = None) -> pd.DataFrame:
    path = Path(path)
    suffix = (file_type or path.suffix.lower().lstrip(".")).lower()
    excel_sheet = 0 if sheet_name is None else sheet_name
    if suffix == "ctdb_merged_excel":
        return read_ctdb_merged_excel(path, sheet_name=excel_sheet)
    if suffix == "csv":
        return pd.read_csv(path)
    if suffix in {"xlsx", "xls"}:
        return pd.read_excel(path, sheet_name=excel_sheet)
    if suffix == "tsv":
        return pd.read_csv(path, sep="\t")
    if suffix == "parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {suffix}")


def read_table_from_spec(spec: DatasetSpec) -> pd.DataFrame:
    header_strategy = (spec.header_strategy or "").lower()
    if header_strategy == "ctdb_merged_v1" or spec.file_type.lower() == "ctdb_merged_excel":
        return read_ctdb_merged_excel(
            spec.path,
            sheet_name=spec.sheet_name if spec.sheet_name is not None else 0,
            demographics_column_end=spec.demographics_column_end,
            demographics_header_row=spec.demographics_header_row,
            clinical_header_row=spec.clinical_header_row,
            skip_rows_after_header=spec.skip_rows_after_header,
        )
    return read_table(spec.path, spec.file_type, sheet_name=spec.sheet_name)
