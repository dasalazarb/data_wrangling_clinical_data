from __future__ import annotations

import argparse
from pathlib import Path

from .orchestrator import run_patient_pipeline, run_variable_catalog_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Clinical data wrangling and validation pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p1 = subparsers.add_parser("validate-catalog", help="Build canonical variable catalog from merged workbook")
    p1.add_argument("--config", required=True, help="Path to variable catalog config YAML")

    p2 = subparsers.add_parser("run-patient-pipeline", help="Run patient-level validation and merge pipeline")
    p2.add_argument("--config", required=True, help="Path to pipeline config YAML")

    args = parser.parse_args()
    if args.command == "validate-catalog":
        run_variable_catalog_pipeline(Path(args.config))
    elif args.command == "run-patient-pipeline":
        run_patient_pipeline(Path(args.config))


if __name__ == "__main__":
    main()
