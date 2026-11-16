"""The lego registry the plugins read. For now a YAML file of legos; Postgres later.

The file is checked as a whole (hydrowf_core.registry.check) when it is loaded, so a broken
registry fails at startup instead of on a user's request.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from hydrowf_core.models import LEGOS, Pipeline
from hydrowf_core.models.lego import Lego
from hydrowf_core.registry import check


class RegistryError(ValueError):
    """The registry file has problems, or doesn't hold the lego asked for."""


def load_legos(path: str | Path) -> list[Lego]:
    legos = LEGOS.validate_python(yaml.safe_load(Path(path).read_text()))
    problems = check(legos)
    if problems:
        raise RegistryError(f"{path}: " + "; ".join(problems))
    return legos


def find_pipeline(legos: list[Lego], id: str, version: str) -> Pipeline:
    found = next((x for x in legos if isinstance(x, Pipeline) and (x.id, x.version) == (id, version)), None)
    if found is None:
        raise RegistryError(f"pipeline {id}@{version} is not in the registry")
    return found
