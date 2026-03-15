import json
from pathlib import Path

from clinical_data_pipeline.io.manifest import build_run_context, collect_file_metadata, compute_file_hash
from clinical_data_pipeline.models import StepResult, ValidationIssue
from clinical_data_pipeline.reporting.export import write_run_manifest
from clinical_data_pipeline.reporting.summary import build_final_summary


def test_collect_file_metadata_and_hash(tmp_path: Path):
    file_path = tmp_path / "input.csv"
    file_path.write_text("id,value\n1,a\n", encoding="utf-8")

    metadata = collect_file_metadata(file_path)

    assert metadata["exists"] is True
    assert metadata["size_bytes"] > 0
    assert metadata["sha256"] == compute_file_hash(file_path)


def test_build_run_context_and_manifest_summary(tmp_path: Path):
    reports_dir = tmp_path / "reports"
    config = {
        "run_name_prefix": "pipeline",
        "paths": {
            "staging_dir": str(tmp_path / "staging"),
            "curated_dir": str(tmp_path / "curated"),
            "analytic_dir": str(tmp_path / "analytic"),
            "excluded_dir": str(tmp_path / "excluded"),
            "reports_dir": str(reports_dir),
            "logs_dir": str(tmp_path / "logs"),
        },
    }
    cfg_path = tmp_path / "pipeline.yaml"
    cfg_path.write_text("project_name: test\n", encoding="utf-8")

    run_context = build_run_context(config, cfg_path)

    result = StepResult(
        step_name="demographics_input_validation",
        success=False,
        started_at="2026-01-01T00:00:00+00:00",
        finished_at="2026-01-01T00:00:01+00:00",
        duration_seconds=1.0,
        dataset_id="demographics",
        issues=[
            ValidationIssue(
                dataset_id="demographics",
                stage="input",
                check_name="file_exists",
                severity="ERROR",
                message="Missing file",
            )
        ],
    )

    manifest_path = write_run_manifest(
        run_context,
        files_read=[collect_file_metadata(cfg_path)],
        validation_results=[result],
        generated_datasets={"curated": {}, "staging": {}, "analytic": {}, "excluded": {}},
        generated_artifacts={},
        exclusions={},
        merge_metrics={},
        duration_seconds=1.23,
        out_dir=reports_dir / "manifests",
    )

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    final_summary = build_final_summary(payload)

    assert manifest_path.exists()
    assert payload["run"]["run_id"] == run_context["run_id"]
    assert final_summary["success"] is False
    assert final_summary["error_count"] == 1
