# Clinical Data Pipeline

A Python pipeline for clinical/patient data processing with a **single-workbook-first** workflow, domain derivation, validation, merge auditing, and reproducible outputs.

## Codebook-driven cohort mode (default)

The default workflow now builds the final cohort directly from the two raw CTDB exports:

- `data/raw/CTDB Data Download 11D.xlsx`
- `data/raw/CTDB Data Download 15D.xlsx`

Variables are selected from `data/input_catalog/codebook_final_harmonized_once_quince.xlsx`, sheet `final_codebook`, using column `QUESTION_NAME`.
Rows with `needs_review = TRUE` are preserved in the merged output with additional flags, and (when available in both studies) both original source versions are retained as `__once` and `__quince` columns.

Outputs include:

1. analysis + quality validation for `11D`,
2. analysis + quality validation for `15D`,
3. analysis + quality validation for the merged harmonized dataset.

## Single workbook first (legacy mode)

Legacy mode (optional) can still run from a single canonical Excel workbook:

- If `data/raw/CTDB Data Download.xlsx` exists, the pipeline treats it as the canonical input source.
- The pipeline applies canonical mapping rules, derives domain datasets, validates each domain, and runs merge audits.
- Final curated artifacts and reports are written to configured output paths.

If that workbook is not present, the pipeline falls back to per-domain source files (for example demographics, visits, labs, outcomes) according to dataset configs.

## Quick Start

Requires **Python 3.10+**.

```bash
pip install -e .
clinical-data-pipeline validate-catalog --config configs/catalog/variable_catalog.yaml
clinical-data-pipeline run-patient-pipeline --config configs/pipeline.yaml
```

## Input selection behavior

1. If `data/raw/CTDB Data Download.xlsx` exists, the pipeline uses it as the canonical source.
2. Otherwise, the pipeline uses per-domain source files declared in `datasets` entries.

This behavior is controlled by the pipeline config and single-workbook settings.

## End-to-end flow (compact)

1. **Source ingestion**  
   Read the canonical workbook (preferred) or per-domain source files.
2. **Canonical mapping**  
   Standardize columns and data semantics using mapping configuration.
3. **Domain derivation**  
   Derive logical domain outputs: `demographics`, `visits`, `labs`, `outcomes`.
4. **Validation and merge audit outputs**  
   Run dataset quality checks and merge-audit checks; emit issue logs and summaries.
5. **Final artifacts and reports**  
   Write processed domain datasets and pipeline reports to configured directories.

## Minimal config example (clean input mode)

```yaml
project_name: sjogren_clinical_data_pipeline
run_name_prefix: sjogren_pipeline

paths:
  raw_dir: data/raw
  staging_dir: data/staging
  curated_dir: data/curated
  analytic_dir: data/analytic
  excluded_dir: data/excluded
  reports_dir: reports
  logs_dir: logs

settings:
  fail_fast: false
  log_level: INFO

single_workbook_input:
  enabled: true
  auto_detect: true
  input_layout: clean_dataframe
  path: data/raw/CTDB Data Download.xlsx
  sheet_name: 0
  patient_id_column: patient_id
  mapping_config: configs/mappings/ctdb_single_workbook.yaml
  domains:
    demographics:
      columns: [patient_id, sex, birth_date, race, ethnicity, death_date, index_date]
    visits:
      columns: [visit_id, patient_id, visit_date, essdai, esspri]
    labs:
      columns: [lab_id, patient_id, visit_date, ana_positive, rf_value, anti_ena]
    outcomes:
      columns: [patient_id, severe_disease, last_followup_date, event_date]

datasets:
  - configs/files/demographics.yaml
  - configs/files/visits.yaml
  - configs/files/labs.yaml
  - configs/files/outcomes.yaml

merge_plan_config: configs/merges/patient_pipeline_merges.yaml
```

## Legacy note

Older merged-header reconstruction details are intentionally removed from the main workflow documentation. Use legacy notes only when maintaining historical ingestion behavior.
