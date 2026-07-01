"""Configuration loading for the ARAMCO ETL framework.

All jobs (curated ingestion, BDH transforms, ADL marts) resolve their
runtime configuration through this module rather than hard-coding paths,
so the same code runs unchanged across DEV / UAT / PROD.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


def _expand_env(value: Any) -> Any:
    """Recursively expand ${ENV_VAR} placeholders found in config values."""
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def load_yaml(filename: str) -> Dict[str, Any]:
    path = CONFIG_DIR / filename
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return _expand_env(raw)


def load_sources() -> Dict[str, Any]:
    return load_yaml("sources.yaml")


def load_dq_thresholds() -> Dict[str, Any]:
    return load_yaml("dq_thresholds.yaml")


def get_source_config(source_name: str) -> Dict[str, Any]:
    sources = load_sources()["sources"]
    if source_name not in sources:
        raise KeyError(f"Unknown source '{source_name}'. Known sources: {sorted(sources)}")
    return sources[source_name]


@dataclass(frozen=True)
class Environment:
    """Resolved environment-level paths, driven entirely by env vars so the
    same jobs run in DEV / UAT / PROD without code changes."""

    name: str
    datalake_root: str
    landing_root: str

    @property
    def curated_root(self) -> str:
        return f"{self.datalake_root}/curated"

    @property
    def bdh_root(self) -> str:
        return f"{self.datalake_root}/bdh"

    @property
    def adl_root(self) -> str:
        return f"{self.datalake_root}/adl"

    @classmethod
    def from_env(cls) -> "Environment":
        return cls(
            name=os.environ.get("ARAMCO_ENV", "DEV"),
            datalake_root=os.environ.get("DATALAKE_ROOT", "/tmp/aramco_datalake"),
            landing_root=os.environ.get("LANDING_ROOT", "/tmp/aramco_landing"),
        )
