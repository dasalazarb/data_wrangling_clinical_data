from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml


TRACE_COLUMNS = [
    "source_variable",
    "harmonized_variable",
    "source_study",
    "rule_applied",
    "observations",
]


def _load_mappings(mappings: dict[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(mappings, dict):
        return mappings

    mapping_path = Path(mappings)
    with mapping_path.open("r", encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh) or {}
    if not isinstance(loaded, dict):
        raise ValueError("Harmonization mappings YAML must contain a top-level mapping/dictionary")
    return loaded


def harmonize_variables(df: pd.DataFrame, mappings: dict[str, Any] | str | Path,
                        source_study: str, question_name_col: str = "QUESTION_NAME") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Harmonize variables using YAML/dict mappings and produce traceability output."""
    mapping_obj = _load_mappings(mappings)
    variables_map = mapping_obj.get("variables", mapping_obj)

    out = df.copy()
    trace_rows: list[dict[str, Any]] = []

    # Step 1: QUESTION_NAME as initial equivalence key when available.
    if question_name_col in out.columns:
        initial_names = out[question_name_col].astype(str).str.strip()
    else:
        initial_names = pd.Series("", index=out.index)

    harmonized_names = []
    for idx, qname in initial_names.items():
        entry = variables_map.get(qname, None)
        if isinstance(entry, dict):
            harmonized_name = entry.get("harmonized", qname)
            rule_applied = entry.get("rule", "question_name_exact")
            observation = entry.get("notes", "")
        elif isinstance(entry, str):
            harmonized_name = entry
            rule_applied = "direct_mapping"
            observation = ""
        else:
            harmonized_name = qname if qname else f"row_{idx}"
            rule_applied = "fallback_identity"
            observation = "No mapping found; original variable retained"

        harmonized_names.append(harmonized_name)
        trace_rows.append(
            {
                "source_variable": qname,
                "harmonized_variable": harmonized_name,
                "source_study": source_study,
                "rule_applied": rule_applied,
                "observations": observation,
            }
        )

    out["HARMONIZED_VARIABLE"] = harmonized_names
    traceability = pd.DataFrame(trace_rows, columns=TRACE_COLUMNS)
    return out, traceability
