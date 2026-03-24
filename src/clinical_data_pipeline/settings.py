from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


ENV_PREFIX = "CDP_"
ENV_NESTED_DELIMITER = "__"
DEFAULT_CONFIG_PATH = Path("configs/pipeline.yaml")


class PathsSettings(BaseModel):
    raw_dir: Path = Path("data/raw")
    staging_dir: Path
    curated_dir: Path
    analytic_dir: Path = Path("data/analytic")
    excluded_dir: Path = Path("data/excluded")
    reports_dir: Path
    logs_dir: Path


class DatasetFileSpec(BaseModel):
    dataset_id: str
    path: Path
    file_type: str = "csv"
    rules_path: Path | None = None


class DQThresholds(BaseModel):
    max_missing_ratio: float = Field(default=1.0, ge=0.0, le=1.0)
    max_invalid_ratio: float = Field(default=1.0, ge=0.0, le=1.0)
    max_duplicate_ratio: float = Field(default=1.0, ge=0.0, le=1.0)


class BusinessRulesSettings(BaseModel):
    thresholds: DQThresholds = Field(default_factory=DQThresholds)


class MergeStepSettings(BaseModel):
    name: str
    left_dataset: str
    right_dataset: str
    how: Literal["left", "right", "inner", "outer", "cross"]
    on: list[str]
    expected_relationship: Literal["one_to_one", "one_to_many", "many_to_one", "many_to_many", "one_to_one_or_many"] | None = None


class MergePlanSettings(BaseModel):
    steps: list[MergeStepSettings] = Field(default_factory=list)


class SeverityPolicy(BaseModel):
    error: list[str] = Field(default_factory=list)
    warning: list[str] = Field(default_factory=list)
    info: list[str] = Field(default_factory=list)


class PipelinePolicySettings(BaseModel):
    fail_fast: bool = False
    severities: SeverityPolicy = Field(default_factory=SeverityPolicy)
    log_level: str = "INFO"


class PipelineSettings(BaseModel):
    model_config = ConfigDict(extra="allow")

    project_name: str = "clinical_pipeline"
    run_name_prefix: str = "pipeline"
    paths: PathsSettings
    datasets: list[Path] = Field(default_factory=list)
    business_rules: BusinessRulesSettings = Field(default_factory=BusinessRulesSettings)
    merge_plan_config: Path | None = None
    merge_plan: MergePlanSettings | None = None
    settings: PipelinePolicySettings = Field(default_factory=PipelinePolicySettings)
    catalog: dict[str, Any] | None = None
    cohort_harmonization: dict[str, Any] | None = None

    @model_validator(mode="after")
    def require_merge_configuration(self) -> "PipelineSettings":
        if self.catalog is not None:
            return self
        if self.cohort_harmonization is not None and self.cohort_harmonization.get("enabled", False):
            return self

        if self.merge_plan is None and self.merge_plan_config is None:
            raise ValueError("Either 'merge_plan' or 'merge_plan_config' must be configured")
        return self


def resolve_path(path: str | Path, project_root: Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else (project_root / path)


def apply_path_prefixes(config: dict[str, Any], project_root: Path) -> dict[str, Any]:
    if "paths" in config:
        for key, value in list(config["paths"].items()):
            config["paths"][key] = str(resolve_path(value, project_root))

    if "catalog" in config and "input_path" in config["catalog"]:
        config["catalog"]["input_path"] = str(resolve_path(config["catalog"]["input_path"], project_root))

    if "datasets" in config:
        config["datasets"] = [str(resolve_path(item, project_root)) for item in config["datasets"]]

    if config.get("merge_plan_config"):
        config["merge_plan_config"] = str(resolve_path(config["merge_plan_config"], project_root))

    return config


def guess_project_root(config_path: str | Path) -> Path:
    config_path = Path(config_path).resolve()
    parent = config_path.parent
    if parent.name == "configs":
        return parent.parent
    if parent.parent.name == "configs":
        return parent.parent.parent
    return config_path.parent


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file_handle:
        return yaml.safe_load(file_handle) or {}


def _parse_dotenv(dotenv_path: Path) -> dict[str, str]:
    if not dotenv_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def _to_nested_mapping(flat_values: dict[str, str]) -> dict[str, Any]:
    nested: dict[str, Any] = {}
    for env_key, raw_value in flat_values.items():
        if not env_key.startswith(ENV_PREFIX):
            continue
        key_path = env_key.removeprefix(ENV_PREFIX).lower().split(ENV_NESTED_DELIMITER)
        cursor = nested
        for segment in key_path[:-1]:
            cursor = cursor.setdefault(segment, {})
        cursor[key_path[-1]] = yaml.safe_load(raw_value)
    return nested


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_dataset_specs(dataset_paths: list[Path], project_root: Path) -> list[DatasetFileSpec]:
    specs: list[DatasetFileSpec] = []
    for dataset_path in dataset_paths:
        if not dataset_path.exists():
            raise FileNotFoundError(f"Dataset spec YAML not found: {dataset_path}")
        cfg = _load_yaml(dataset_path)
        spec = DatasetFileSpec.model_validate(cfg)
        spec.path = resolve_path(spec.path, project_root)
        if spec.rules_path is not None:
            spec.rules_path = resolve_path(spec.rules_path, project_root)
        specs.append(spec)
    return specs


def _load_merge_plan(settings: PipelineSettings, project_root: Path) -> MergePlanSettings:
    if settings.merge_plan is not None:
        return settings.merge_plan

    if settings.merge_plan_config is None:
        return MergePlanSettings()

    merge_cfg = _load_yaml(resolve_path(settings.merge_plan_config, project_root))
    return MergePlanSettings.model_validate(merge_cfg)


def _validate_base_directories(paths: PathsSettings) -> None:
    missing = [
        str(path)
        for path in [
            paths.raw_dir,
            paths.staging_dir,
            paths.curated_dir,
            paths.analytic_dir,
            paths.excluded_dir,
            paths.reports_dir,
            paths.logs_dir,
        ]
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError(f"Missing required base directories: {', '.join(missing)}")


def _validate_references(dataset_specs: list[DatasetFileSpec], merge_plan: MergePlanSettings) -> None:
    dataset_names = {spec.dataset_id for spec in dataset_specs}
    available_names = set(dataset_names)

    for spec in dataset_specs:
        if spec.rules_path is not None and not spec.rules_path.exists():
            raise FileNotFoundError(f"Rules YAML not found for dataset '{spec.dataset_id}': {spec.rules_path}")

    for step in merge_plan.steps:
        if step.left_dataset not in available_names:
            raise ValueError(f"Merge step '{step.name}' references unknown left dataset '{step.left_dataset}'")
        if step.right_dataset not in available_names:
            raise ValueError(f"Merge step '{step.name}' references unknown right dataset '{step.right_dataset}'")
        available_names.add(step.name)


@lru_cache(maxsize=4)
def get_settings(config_path: str | Path = DEFAULT_CONFIG_PATH, project_root: str | Path | None = None) -> PipelineSettings:
    return load_settings(config_path=config_path, project_root=project_root)


def load_settings(config_path: str | Path = DEFAULT_CONFIG_PATH, project_root: str | Path | None = None) -> PipelineSettings:
    config_path = Path(config_path)
    project_root = Path(project_root or guess_project_root(config_path)).resolve()

    yaml_config = apply_path_prefixes(_load_yaml(config_path), project_root)
    dotenv_values = _to_nested_mapping(_parse_dotenv(project_root / ".env"))
    env_values = _to_nested_mapping(dict(os.environ))

    merged_config = _deep_merge(yaml_config, dotenv_values)
    merged_config = _deep_merge(merged_config, env_values)

    try:
        settings = PipelineSettings.model_validate(merged_config)
    except ValidationError as exc:
        raise ValueError(f"Invalid pipeline settings: {exc}") from exc

    _validate_base_directories(settings.paths)
    dataset_specs = _load_dataset_specs(settings.datasets, project_root)
    merge_plan = _load_merge_plan(settings, project_root)
    _validate_references(dataset_specs, merge_plan)

    settings.merge_plan = merge_plan
    return settings
