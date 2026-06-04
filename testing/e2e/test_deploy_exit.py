"""Tests for SIGINT/SIGTERM handlers + atomic _save_env (rec #5).

Spec: REQ-E-312, 313, 315, 316.
"""

from __future__ import annotations

import importlib
import inspect
import os
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# TC-EXIT-001: _install_exit_handlers exists and is called
# ---------------------------------------------------------------------------

def test_TC_EXIT_001_install_exit_handlers_exists():
    deploy = importlib.import_module("scripts.deploy")
    assert hasattr(deploy, "_install_exit_handlers"), \
        "deploy.py must define _install_exit_handlers"
    sig = inspect.signature(deploy._install_exit_handlers)
    assert "env" in sig.parameters, \
        f"_install_exit_handlers must take env; got {list(sig.parameters)}"


def test_TC_EXIT_001_called_in_run_deployment():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.run_deployment)
    assert "_install_exit_handlers" in src, \
        "run_deployment must call _install_exit_handlers"


# ---------------------------------------------------------------------------
# TC-EXIT-002: SIGINT handler writes DEPLOY_LAST_INTERRUPTED_PHASE + exits 130
# ---------------------------------------------------------------------------

def test_TC_EXIT_002_signal_handler_writes_state_and_exits_130():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._install_exit_handlers)
    assert "DEPLOY_LAST_INTERRUPTED_PHASE" in src, \
        "signal handler must write DEPLOY_LAST_INTERRUPTED_PHASE"
    assert "signal.SIGINT" in src, "must register a SIGINT handler"
    assert "signal.SIGTERM" in src, "must register a SIGTERM handler"
    assert "130" in src, "signal handler must exit with status 130"


# ---------------------------------------------------------------------------
# TC-EXIT-003: _save_env_many is atomic via temp-file + os.replace
# ---------------------------------------------------------------------------

def test_TC_EXIT_003_save_env_uses_atomic_replace():
    """REQ-E-316: _save_env_many writes atomically via temp-file + replace.

    Pass-6 H-NEW-5: the atomic-write logic was extracted to
    scripts.common.env_file.atomic_write_env so deploy.py and
    mcp_deploy.py share a single canonical writer. Check the
    delegated module's source.
    """
    env_file = importlib.import_module("scripts.common.env_file")
    src = inspect.getsource(env_file.atomic_write_env)
    assert "os.replace" in src, \
        f"atomic_write_env must use os.replace; got source:\n{src[:500]}"
    # Must use a temp file (tempfile.mkstemp for unique path)
    assert "mkstemp" in src or ".tmp" in src, \
        "atomic_write_env must write to a temp file before replace"
    # And the deploy.py wrapper must delegate (not re-implement)
    deploy = importlib.import_module("scripts.deploy")
    deploy_src = inspect.getsource(deploy._save_env_many)
    assert "atomic_write_env" in deploy_src, \
        "_save_env_many must delegate to atomic_write_env"


def test_TC_EXIT_003_save_env_delegates_to_many():
    """REQ-E-316: _save_env should be a thin wrapper over _save_env_many."""
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._save_env)
    assert "_save_env_many" in src, \
        "_save_env should delegate to _save_env_many for atomicity"


def test_TC_EXIT_003_atomicity_under_simulated_failure(tmp_path, monkeypatch):
    """If the temp-file write fails, the live .env file is unchanged."""
    deploy = importlib.import_module("scripts.deploy")

    env_path = tmp_path / ".env"
    env_path.write_text("ORIGINAL=preserved\n")
    monkeypatch.setattr(deploy, "_env_path", lambda: env_path)

    # Force os.replace to raise — simulates atomicity failure mid-rename
    def boom(*_a, **_kw):
        raise OSError("simulated failure")
    monkeypatch.setattr(os, "replace", boom)

    with pytest.raises(OSError):
        deploy._save_env_many({"NEW_KEY": "value"})

    # The original file must be unchanged
    contents = env_path.read_text()
    assert "ORIGINAL=preserved" in contents
    assert "NEW_KEY" not in contents


# ---------------------------------------------------------------------------
# TC-EXIT-004: uncaught exception writes failure state before re-raising
# ---------------------------------------------------------------------------

def test_TC_EXIT_004_main_wraps_run_deployment_in_try_except():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    # The try/except wrapping run_deployment must record DEPLOY_LAST_FAILURE
    # and DEPLOY_LAST_FAILED_PHASE before re-raising.
    assert "DEPLOY_LAST_FAILURE" in src, \
        "main() must record DEPLOY_LAST_FAILURE on uncaught exception"
    assert "DEPLOY_LAST_FAILED_PHASE" in src, \
        "main() must record DEPLOY_LAST_FAILED_PHASE on uncaught exception"


# ---------------------------------------------------------------------------
# TC-EXIT-005 (REQ-E-312 best-effort): handler exits 130 even if state write fails
# ---------------------------------------------------------------------------

def test_TC_EXIT_005_handler_is_best_effort(monkeypatch, tmp_path):
    """Source-level proxy: signal handler must wrap state writes in try/except."""
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._install_exit_handlers)
    # The handler must catch exceptions around _load_env / _save_env_many
    # so a disk error doesn't prevent exit(130).
    assert "try:" in src and "except" in src, \
        "signal handler must use try/except for best-effort persistence"
