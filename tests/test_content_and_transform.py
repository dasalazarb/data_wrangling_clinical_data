from pathlib import Path

import pandas as pd

from clinical_data_pipeline.transform.harmonize import harmonize_variables
from clinical_data_pipeline.transform.operations import cast_columns, derive, normalize_categories, standardize_names
from clinical_data_pipeline.validation.content_checks import (
    detect_duplicates,
    parse_dates_safely,
    validate_domains,
    validate_primary_key,
    validate_ranges,
    validate_required_fields,
)
from clinical_data_pipeline.validation.file_checks import validate_file_exists


def test_validate_file_exists(tmp_path: Path):
    existing = tmp_path / "x.csv"
    existing.write_text("a,b\n1,2\n", encoding="utf-8")
    ok, issues = validate_file_exists(existing, dataset_id="demo")
    assert ok is True
    assert issues[0].severity == "INFO"


def test_content_checks_cover_required_functions():
    df = pd.DataFrame({"id": ["1", "1", ""], "sex": ["M", "X", "F"], "score": [3, 11, 5], "d": ["2020-01-01", "bad", None]})

    required_ok, required_issues = validate_required_fields(df, ["id", "sex"], dataset_id="demo")
    assert required_ok is True
    assert any(i.severity == "WARNING" for i in required_issues)

    parsed, date_issues = parse_dates_safely(df["d"], dataset_id="demo", column_name="d")
    assert parsed.isna().sum() >= 1
    assert date_issues[0].severity in {"INFO", "WARNING"}

    domain_ok, domain_issues = validate_domains(df, {"sex": ["M", "F"]}, dataset_id="demo")
    assert domain_ok is False
    assert domain_issues[0].severity == "ERROR"

    range_ok, range_issues = validate_ranges(df, {"score": {"min": 0, "max": 10}}, dataset_id="demo")
    assert range_ok is False
    assert any(i.check_name == "range_violation" for i in range_issues)

    duplicated_rows, duplicate_issues = detect_duplicates(df, ["id"], dataset_id="demo")
    assert len(duplicated_rows) >= 2
    assert duplicate_issues[0].severity == "ERROR"

    pk_ok, pk_issues, _ = validate_primary_key(df, "id", dataset_id="demo")
    assert pk_ok is False
    assert any(i.check_name == "blank_primary_key" for i in pk_issues)


def test_schema_registry_with_pandera():
    pytest = __import__("pytest")
    pytest.importorskip("pandera")
    from clinical_data_pipeline.validation.schema_registry import DatasetSchemaSpec, SchemaRegistry

    registry = SchemaRegistry()
    registry.register(
        "demo",
        DatasetSchemaSpec(
            required_columns=["id", "age"],
            dtypes={"id": "string", "age": "int64"},
            nullable_columns=["age"],
            allowed_values={},
            ranges={"age": {"min": 0, "max": 120}},
        ),
    )
    validated = registry.validate("demo", pd.DataFrame({"id": ["A1"], "age": [33]}))
    assert isinstance(validated, pd.DataFrame)


def test_transform_operations_and_harmonization(tmp_path: Path):
    df = pd.DataFrame({"Question Name": ["AGE", "SEX"], "cat": ["Masculino", "Femenino"], "v": ["1", "2"]})

    std = standardize_names(df)
    assert "question_name" in std.columns

    norm = normalize_categories(std, {"cat": {"masculino": "M", "femenino": "F"}})
    assert set(norm["cat"]) == {"M", "F"}

    casted = cast_columns(norm, {"v": "numeric"})
    assert str(casted["v"].dtype).startswith(("int", "float"))

    derived = derive(casted, {"v2": "v * 2"})
    assert derived["v2"].tolist() == [2, 4]

    mapping_path = tmp_path / "harmonize.yml"
    mapping_path.write_text("variables:\n  AGE:\n    harmonized: age_years\n    rule: mapped\n  SEX: sex\n", encoding="utf-8")
    harm_df = pd.DataFrame({"QUESTION_NAME": ["AGE", "SEX", "OTHER"]})
    harmonized, trace = harmonize_variables(harm_df, mapping_path, source_study="once")

    assert "HARMONIZED_VARIABLE" in harmonized.columns
    assert set(trace.columns) == {"source_variable", "harmonized_variable", "source_study", "rule_applied", "observations"}
    assert trace["source_study"].eq("once").all()
