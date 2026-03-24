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

Requiere **Python 3.10 o superior**.

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

---

## Nuevo escenario soportado: input único en Excel (`CTDB Data Download.xlsx`)

Además del flujo tradicional por archivos individuales (`demographics.csv`, `visits.csv`, etc.), el pipeline está diseñado para incorporar un flujo de **fuente única** cuando la entrada viene en un solo workbook con encabezados combinados (merged).

### Objetivo

Tomar `CTDB Data Download.xlsx` como entrada de `run-patient-pipeline`, estandarizar sus encabezados y producir un dataset limpio donde:

1. La primera fila efectiva del output contiene los **nombres finales de variables**.
2. Las filas siguientes contienen la **información de pacientes**.
3. No se requiere generar archivos individuales manualmente antes del pipeline.

### Estructura esperada del Excel CTDB

Para la hoja de trabajo principal:

- **Columnas A:N**
  - Fila 1 y 2 suelen estar combinadas/merged por columna.
  - Los nombres de variables demográficas válidos se toman de la **fila 3**.
- **Columnas O en adelante**
  - La fila 1 puede estar merged por categoría (bloques).
  - Los nombres de variables clínicos se toman de la **fila 2**.
  - La fila 3 en este bloque contiene descripciones y se puede omitir para nombrado.

### Regla de construcción de encabezados

Al normalizar el workbook:

- Para índices de columna `0..13` (A:N): usar header de fila 3.
- Para índices de columna `>=14` (O+): usar header de fila 2.
- Limpiar nombres (`trim`, espacios dobles, caracteres invisibles) y asegurar unicidad.
- Eliminar filas de encabezado (1, 2 y 3) del cuerpo de datos final.

### Resultado esperado del preprocesamiento

Un DataFrame/tabular único con:

- columnas canónicas listas para validación,
- tipos iniciales casteables por reglas del dataset,
- registros de pacientes listos para el resto de validaciones y merges.

---

## Integración recomendada en `run-patient-pipeline`

Para cerrar el ciclo de los dos comandos sin romper el flujo actual:

1. `validate-catalog` mantiene su función para catálogo de variables.
2. `run-patient-pipeline` acepta un modo `single_workbook_input` para CTDB.
3. El pipeline deriva internamente las vistas lógicas (`demographics`, `visits`, `labs`, `outcomes`) desde el workbook ya normalizado.
4. Se reutilizan validadores, reportes y auditoría de merge existentes.

### Ejemplo de configuración (referencial)

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
  path: data/raw/CTDB Data Download.xlsx
  sheet_name: Sheet1
  header_strategy: ctdb_merged_v1
  demographics_column_end: N
  demographics_header_row: 3
  clinical_header_row: 2

# El resto de datasets/merges puede mantenerse para validación y ensamble
# siempre que se mapeen columnas canónicas desde la fuente única.
```

> Nota: el bloque anterior es una guía de configuración de entrada para este tipo de archivo. Ajusta nombres de hoja y mapeos según tu archivo real.

---

## Validación de calidad para el flujo CTDB

Checks recomendados tras normalizar el workbook:

- Verificar columnas requeridas por dominio (`required_columns`).
- Verificar dtypes esperados (`expected_dtypes`).
- Validar claves primarias y duplicados.
- Auditar dominios permitidos (`allowed_values`).
- Mantener reportes de issues y summary por dataset y por merge.

Esto permite mantener trazabilidad y reproducibilidad aun cuando el input llegue como archivo único merged.
