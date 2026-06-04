"""Cached terraform output JSON helper.

`terraform output -json` is invoked 13+ times
per deploy across deploy.py / destroy.py / pipeline_reset.py / asp_setup.py.
Each call is a 1-3 s subprocess round-trip and they all return the same
content within a deploy phase.

This module provides cached helpers keyed by the project root path. The
cache lives for the process lifetime; callers invoke ``_clear_cache()``
between terraform apply boundaries when they need fresh data.

Public API:
    get_core_outputs(root)    — terraform/core outputs
    get_atlas_outputs(root)   — terraform/atlas outputs
    get_agents_outputs(root)  — terraform/agents outputs (rare; debugging)
    _clear_cache()            — invalidate after `terraform apply`
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


_cache: dict[str, dict[str, Any]] = {}


def _get_outputs(root: Path, module: str) -> dict[str, Any]:
    """Return ``terraform output -json`` from a specific module dir.

    Cached by (project_root, module). Returns ``{}`` on any failure
    (missing tfstate, terraform error, JSON parse error) — does NOT
    cache the failure so a later call retries.
    """
    key = f"{Path(root).resolve()}::{module}"
    if key in _cache:
        return _cache[key]

    module_dir = Path(root) / "terraform" / module
    if not (module_dir / "terraform.tfstate").exists():
        return {}
    try:
        result = subprocess.run(
            ["terraform", "output", "-json"],
            cwd=module_dir, capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            return {}
        outputs = json.loads(result.stdout)
    except Exception:
        return {}

    _cache[key] = outputs
    return outputs


def get_core_outputs(root: Path) -> dict[str, Any]:
    """Return ``terraform output -json`` from terraform/core/."""
    return _get_outputs(root, "core")


def get_atlas_outputs(root: Path) -> dict[str, Any]:
    """Return ``terraform output -json`` from terraform/atlas/."""
    return _get_outputs(root, "atlas")


def get_agents_outputs(root: Path) -> dict[str, Any]:
    """Return ``terraform output -json`` from terraform/agents/."""
    return _get_outputs(root, "agents")


def _clear_cache() -> None:
    """Invalidate the cache after a terraform apply / replace boundary."""
    _cache.clear()
