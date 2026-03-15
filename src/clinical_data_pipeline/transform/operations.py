from __future__ import annotations

from collections.abc import Callable

import pandas as pd


def standardize_names(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [
        str(c).strip().lower().replace(" ", "_").replace("/", "_").replace("-", "_")
        for c in out.columns
    ]
    return out


def normalize_categories(df: pd.DataFrame, mappings: dict[str, dict[str, str]]) -> pd.DataFrame:
    out = df.copy()
    for col, mapping in mappings.items():
        if col not in out.columns:
            continue
        normalized_map = {str(k).strip().lower(): v for k, v in mapping.items()}
        out[col] = out[col].map(lambda v: normalized_map.get(str(v).strip().lower(), v) if pd.notna(v) else v)
    return out


def cast_columns(df: pd.DataFrame, casts: dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    for col, target_type in casts.items():
        if col not in out.columns:
            continue
        if target_type == "numeric":
            out[col] = pd.to_numeric(out[col], errors="coerce")
        elif target_type == "date":
            out[col] = pd.to_datetime(out[col], errors="coerce")
        elif target_type == "string":
            out[col] = out[col].astype("string")
        elif target_type == "boolean":
            out[col] = out[col].astype("boolean")
        else:
            out[col] = out[col].astype(target_type)
    return out


def derive(df: pd.DataFrame, derivations: dict[str, Callable[[pd.DataFrame], pd.Series] | str]) -> pd.DataFrame:
    out = df.copy()
    for new_col, rule in derivations.items():
        if callable(rule):
            out[new_col] = rule(out)
        elif isinstance(rule, str):
            out[new_col] = out.eval(rule)
        else:
            raise TypeError(f"Unsupported derivation rule type for {new_col}: {type(rule)}")
    return out
