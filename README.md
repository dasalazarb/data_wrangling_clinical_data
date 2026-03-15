# Clinical Data Pipeline

Robust Python pipeline for **clinical/patient data wrangling, validation, merge auditing, traceability, and reproducibility**, designed for small-to-medium datasets where **quality and auditability matter more than distributed compute**.

## Project structure

```text
clinical_data_pipeline_repo/
├── configs/
│   ├── files/
│   ├── merges/
│   ├── rules/
│   └── catalog/
├── data/
├── logs/
├── reports/
├── src/clinical_data_pipeline/
│   ├── io/
│   ├── validation/
│   ├── transform/
│   ├── integrate/
│   ├── reporting/
│   └── utils/
├── tests/
└── pyproject.toml
```

## Quick start

```bash
pip install -e .
```

### Variable catalog flow

```bash
clinical-data-pipeline validate-catalog --config configs/catalog/variable_catalog.yaml
```

### Patient-data pipeline

```bash
clinical-data-pipeline run-patient-pipeline --config configs/pipeline.yaml
```

## Config layout

- Dataset definitions: `configs/files/*.yaml`
- Merge plan: `configs/merges/patient_pipeline_merges.yaml`
- Rules per dataset: `configs/rules/*_rules.yaml`
- Variable catalog: `configs/catalog/variable_catalog.yaml`

`configs/pipeline.yaml` references only the new tree under `configs/files`, `configs/merges`, and `configs/rules`.
