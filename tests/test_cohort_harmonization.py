from pathlib import Path

import pandas as pd

from clinical_data_pipeline.orchestrator import _build_harmonized_merged_dataset, _load_final_codebook


def test_load_final_codebook_uses_question_name_and_needs_review(tmp_path: Path):
    codebook_path = tmp_path / "codebook.xlsx"
    pd.DataFrame(
        {
            "QUESTION_NAME": ["var_a", "var_b", "var_a"],
            "needs_review": ["TRUE", "FALSE", "TRUE"],
        }
    ).to_excel(codebook_path, sheet_name="final_codebook", index=False)

    loaded = _load_final_codebook(codebook_path, "final_codebook", "QUESTION_NAME", "needs_review")

    assert loaded["QUESTION_NAME"].tolist() == ["var_a", "var_b"]
    assert loaded["needs_review"].tolist() == [True, False]


def test_build_harmonized_merged_dataset_keeps_dual_columns_and_flags_for_review():
    once_df = pd.DataFrame({"patient_id": ["P1"], "var_review": ["A"], "var_once_only": [1]})
    quince_df = pd.DataFrame({"patient_id": ["P1"], "var_review": ["B"], "var_quince_only": [2]})

    merged = _build_harmonized_merged_dataset(
        once_df,
        quince_df,
        patient_id_column="patient_id",
        questions=["patient_id", "var_review", "var_once_only", "var_quince_only"],
        needs_review_questions={"var_review"},
    )

    assert merged.loc[0, "var_review"] == "A"
    assert merged.loc[0, "var_review__once"] == "A"
    assert merged.loc[0, "var_review__quince"] == "B"
    assert bool(merged.loc[0, "var_review__needs_review_flag"]) is True
    assert bool(merged.loc[0, "var_review__source_conflict_flag"]) is True
