# Reporte de funcionamiento del repositorio

## 1) Resumen ejecutivo
Este repositorio implementa un pipeline clínico de datos con dos flujos principales:
- **`validate-catalog`**: construye un catálogo canónico de variables desde un Excel consolidado.
- **`run-patient-pipeline`**: valida datasets clínicos, genera capas staging/curated, ejecuta merges auditables y escribe resúmenes/manifiestos.

El punto de entrada de CLI está en `clinical-data-pipeline` y delega en `orchestrator.py`.

## 2) Flujo end-to-end
1. Se carga configuración YAML y se normalizan rutas (`settings.py`).
2. Se inicializa el contexto de corrida y logging (`io/manifest.py`, `utils/logger.py`).
3. Para cada dataset:
   - Se valida existencia/estructura/tipos/reglas (`validation/validators.py`).
   - Se escriben artefactos por etapa (`reporting/results.py`).
   - Se materializa salida en staging y curated (`io/writers.py`).
4. Se ejecutan pasos de merge con control de llaves y métricas (`integrate/merge.py`).
5. Se generan excluidos left_only/right_only y resumen final (`reporting/summary.py`, `reporting/export.py`).

## 3) Scripts/módulos y responsabilidad

### 3.1 Entrypoints
- `src/clinical_data_pipeline/main.py`: entrada mínima, invoca `cli.main()`.
- `src/clinical_data_pipeline/cli.py`: parser de argumentos; expone comandos `validate-catalog` y `run-patient-pipeline`.
- `src/clinical_data_pipeline/orchestrator.py`: orquesta ambos flujos completos.

### 3.2 Configuración y modelos
- `src/clinical_data_pipeline/settings.py`: modelos Pydantic de configuración, carga YAML/.env/env vars, valida directorios y referencias de datasets/merges.
- `src/clinical_data_pipeline/models.py`: dataclasses núcleo (`ValidationIssue`, `StepResult`, `DatasetSpec`).
- `src/clinical_data_pipeline/exceptions.py`: jerarquía base de excepciones del pipeline.

### 3.3 IO y trazabilidad
- `src/clinical_data_pipeline/io/config_loader.py`: lectura de YAMLs de configuración y specs.
- `src/clinical_data_pipeline/io/readers.py`: lectura tabular (csv/parquet/xlsx según extensión/tipo).
- `src/clinical_data_pipeline/io/writers.py`: escritura tabular y creación de directorios destino.
- `src/clinical_data_pipeline/io/manifest.py`: metadata/hash de archivos y contexto de corrida.
- `src/clinical_data_pipeline/manifest.py`: constructor/escritor de manifiesto simple de ejecución.
- `src/clinical_data_pipeline/io/tables.py`: utilidades tabulares compartidas.

### 3.4 Validación de calidad
- `src/clinical_data_pipeline/validation/validators.py`: validaciones principales por dataset (input, columnas, tipos, missingness, PK, dominios, reglas de negocio, sanity checks) y casteo de tipos.
- `src/clinical_data_pipeline/validation/file_checks.py`: validación puntual de existencia de archivo.
- `src/clinical_data_pipeline/validation/content_checks.py`: checks reutilizables de contenido (required fields, dominios, rangos, duplicados, PK).
- `src/clinical_data_pipeline/validation/schema_registry.py`: construcción de esquemas Pandera por dataset.
- `src/clinical_data_pipeline/schema_registry.py`: registro liviano de esquemas para resolver por id.

### 3.5 Transformaciones y catálogo
- `src/clinical_data_pipeline/transform/operations.py`: transformaciones dataframe genéricas (estandarizar nombres, normalizar categorías, cast, derivaciones).
- `src/clinical_data_pipeline/transform/harmonize.py`: armonización de variables usando mappings YAML/dict.
- `src/clinical_data_pipeline/transform/catalog.py`: construcción del catálogo canónico, selección de variables y métricas de resumen.

### 3.6 Integración / merge
- `src/clinical_data_pipeline/integrate/merge.py`: validación de llaves de merge, ejecución del merge con métricas de cardinalidad y extracción de no emparejados.
- `src/clinical_data_pipeline/integrate/enterprise.py`: placeholder para checks enterprise opcionales.

### 3.7 Reporting y observabilidad
- `src/clinical_data_pipeline/reporting/results.py`: serialización de issues por paso a CSV/JSON y escritura de summary final.
- `src/clinical_data_pipeline/reporting/export.py`: manifiesto detallado de corrida (inputs, resultados, datasets generados, métricas y duración).
- `src/clinical_data_pipeline/reporting/summary.py`: agrega un resumen final desde el manifiesto.
- `src/clinical_data_pipeline/utils/logger.py`: logger de ejecución con archivo por corrida.
- `src/clinical_data_pipeline/utils/core.py`: utilidades (timestamps, mkdir, hash, JSON).

### 3.8 Paquetes auxiliares
- `src/clinical_data_pipeline/__init__.py` y `__init__.py` de subpaquetes: exponen namespace del paquete.

## 4) Scripts de prueba (tests)
- `tests/test_validators.py`: cubre validadores de estructura/contenido.
- `tests/test_merge.py`: cubre reglas/métricas del merge y extracción de no match.
- `tests/test_io_tables.py`: cubre utilidades de lectura/escritura tabular.
- `tests/test_content_and_transform.py`: cubre checks de contenido y operaciones de transformación.
- `tests/test_observability_manifest.py`: cubre manifiestos y piezas de observabilidad/reporting.

## 5) Cómo ejecutarlo en práctica
1. Instalar en editable:
   - `python -m pip install -e .`
2. Ejecutar pruebas:
   - `python -m pytest -q`
3. Ejecutar catálogo:
   - `clinical-data-pipeline validate-catalog --config configs/catalog/variable_catalog.yaml`
4. Ejecutar pipeline clínico:
   - `clinical-data-pipeline run-patient-pipeline --config configs/pipeline.yaml`

## 6) Nota sobre `pytest -q`
- `pytest` es el runner de tests; `-q` = salida corta (quiet).
- Si `pytest` no aparece en PATH, usa siempre:
  - `python -m pytest -q`
  para forzar el uso del pytest del entorno Python activo.
