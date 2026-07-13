"""Shared MongoDB URI resolution — the single source of truth for how the
dashboard AND the live SSE sidecar discover their Atlas connection string.

Resolution chain (first match wins), preserved verbatim from the dashboard's
original `_resolve_mongodb_uri` so behavior does not drift (spec INV-005):

  1. .env  -> TF_VAR_mongodb_connection_string (+ optional user/pw)
  2. terraform/agents/terraform.tfvars
  3. environment variable MONGODB_URI
  4. None (caller falls back — e.g. Streamlit sidebar prompt)

URI construction delegates to `scripts.common.mongo.build_uri` so all parsing
lives in one place (INV-003).
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

try:
    from dotenv import dotenv_values

    HAS_DOTENV = True
except ImportError:  # pragma: no cover - dotenv is a declared dependency
    HAS_DOTENV = False

from scripts.common.mongo import build_uri


def load_env_defaults(project_root: Optional[Path] = None) -> dict:
    """Load non-empty key/values from .env.

    When project_root is given, ONLY that directory's .env is read. Otherwise
    search upward from this file's directory.
    """
    if not HAS_DOTENV:
        return {}
    if project_root:
        env_file = project_root / ".env"
        if env_file.exists():
            return {k: v for k, v in dotenv_values(env_file).items() if v}
        return {}
    here = Path(__file__).resolve().parent
    for p in [here, *here.parents]:
        env_file = p / ".env"
        if env_file.exists():
            return {k: v for k, v in dotenv_values(env_file).items() if v}
    return {}


def parse_tfvars(tfvars_path: Path) -> dict:
    """Parse a terraform.tfvars file into a dict of quoted string assignments."""
    result: dict = {}
    if not tfvars_path.exists():
        return result
    for line in tfvars_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r'^(\w+)\s*=\s*"([^"]*)"', line)
        if match:
            result[match.group(1)] = match.group(2)
    return result


def _find_project_root() -> Optional[Path]:
    """Find the project root by walking up to the nearest pyproject.toml."""
    here = Path(__file__).resolve().parent
    for p in [here, *here.parents]:
        if (p / "pyproject.toml").exists():
            return p
    return None


def resolve_mongodb_uri(project_root: Optional[Path] = None) -> Optional[str]:
    """Resolve the MongoDB URI via the 4-source chain (first match wins)."""
    # Source 1: .env
    env = load_env_defaults(project_root)
    conn = env.get("TF_VAR_mongodb_connection_string")
    if conn:
        user = env.get("TF_VAR_mongodb_username", "")
        pwd = env.get("TF_VAR_mongodb_password", "")
        if "://" in conn and "@" in conn:
            return conn
        if user and pwd:
            return build_uri(conn, user, pwd)
        return conn

    # Source 2: terraform.tfvars
    root = project_root or _find_project_root()
    if root:
        tfvars_path = root / "terraform" / "agents" / "terraform.tfvars"
        tfvars = parse_tfvars(tfvars_path)
        conn = tfvars.get("mongodb_connection_string")
        user = tfvars.get("mongodb_username", "")
        pwd = tfvars.get("mongodb_password", "")
        if conn and user and pwd:
            return build_uri(conn, user, pwd)

    # Source 3: environment variable
    env_uri = os.environ.get("MONGODB_URI")
    if env_uri:
        return env_uri

    # Source 4: none
    return None
