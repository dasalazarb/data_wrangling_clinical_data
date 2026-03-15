from __future__ import annotations

from pathlib import Path


def resolve_path(path: str | Path, project_root: Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else project_root / path


def apply_path_prefixes(config: dict, project_root: Path) -> dict:
    if "paths" in config:
        for key, value in list(config["paths"].items()):
            config["paths"][key] = str(resolve_path(value, project_root))
    if "catalog" in config and "input_path" in config["catalog"]:
        config["catalog"]["input_path"] = str(resolve_path(config["catalog"]["input_path"], project_root))
    return config


def guess_project_root(config_path: str | Path) -> Path:
    config_path = Path(config_path).resolve()
    parent = config_path.parent
    if parent.name == "configs":
        return parent.parent
    if parent.parent.name == "configs":
        return parent.parent.parent
    return config_path.parent
