# Clinical Pipeline

Robust Python pipeline for **clinical/patient data wrangling, validation, merge auditing, traceability, and reproducibility**, designed for small-to-medium datasets (for example, 50 to 500 patients) where **quality and auditability matter more than distributed compute**.

## What is included

- Config-driven pipeline for multiple input files
- Validation of file existence, readability, columns, types, missingness, uniqueness, ranges, and business rules
- Merge auditing with match rates and unmatched record exports
- Timestamped outputs, full logs, run manifest, and final summaries
- Variable-catalog harmonization flow for your merged workbook `variables_merge_once_quince.xlsx`
- Example sample data and configs
- Minimal tests
- Optional enterprise adapter hooks for Great Expectations / Pandera / DuckDB

## Recommended stack

### Minimal solid core
- pandas
- pydantic-settings
- PyYAML
- openpyxl
- Rich + standard logging
- pytest

### Optional enterprise layer
- Great Expectations for formal checkpoints and governance
- Pandera for declarative dataframe schema checks
- DuckDB for scalable profiling and merge diagnostics
- PyArrow / Parquet for stronger typed intermediate artifacts

## Project structure

```text
clinical_pipeline_repo/
├── configs/
├── data/
├── logs/
├── reports/
├── src/clinical_pipeline/
├── tests/
└── pyproject.toml
```

## Quick start

### 1) Install

```bash
pip install -e .
```

### 2) Run the variable catalog flow

This reads the merged workbook and produces a canonical variable registry plus audit reports.

```bash
clinical-pipeline validate-catalog   --config configs/catalog/variable_catalog.yaml
```

### 3) Run the patient-data demo flow

```bash
clinical-pipeline run-patient-pipeline   --config configs/pipeline.yaml
```

Outputs are written with timestamps into:
- `logs/`
- `reports/`
- `data/staging/`
- `data/curated/`
- `data/analytic/`
- `data/excluded/`

## Current deliverables in this repo

### A. Variable catalog flow (already aligned to your current work)
Uses:
- `data/input_catalog/variables_merge_once_quince.xlsx`
- `configs/catalog/variable_catalog.yaml`

Produces:
- canonical variable registry
- list of variables only in one study
- variables with metadata mismatches
- summary metrics and audit log

### B. Patient-level pipeline template
Uses example synthetic files:
- `demographics.csv`
- `visits.csv`
- `labs.csv`
- `outcomes.csv`

Replace these with your real extracts later.

## Suggested way to adapt to your studies

1. Keep the merged workbook as your **catalog of candidate variables**.
2. Mark `SELECT_FOR_PIPELINE` in `variables_merged` as needed.
3. Build a **canonical variable map**.
4. Point dataset YAML files to your real patient-level extracts.
5. Encode critical rules in YAML before analysis begins.
6. Freeze curated outputs before modeling.

## Notes

- This repo is intentionally optimized for **robustness and auditability**, not big-data infrastructure.
- It is the right architecture for your expected volume and the strongest foundation before adding modeling.
