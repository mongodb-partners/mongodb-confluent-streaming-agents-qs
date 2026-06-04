"""Tests for the interactive resume prompt and destroy state cleanup.

Spec: REQ-E-321, REQ-E-326, REQ-E-327; rec #2 / Task 6.
"""

from __future__ import annotations

import importlib
import inspect

import pytest


# ---------------------------------------------------------------------------
# TC-RESUME-PROMPT helpers exist
# ---------------------------------------------------------------------------

def test_resume_prompt_helper_exists():
    deploy = importlib.import_module("scripts.deploy")
    assert hasattr(deploy, "_resume_prompt"), \
        "deploy.py must define _resume_prompt(env, args) -> str | None"
    sig = inspect.signature(deploy._resume_prompt)
    params = list(sig.parameters)
    assert "env" in params and "args" in params


def test_resume_prompt_skips_when_force_or_from_phase():
    """When --force or --from-phase is set, no prompt happens."""
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy._resume_prompt)
    assert "args.force" in src and "args.from_phase" in src, \
        "_resume_prompt must short-circuit when --force/--from-phase set"


def test_resume_prompt_skips_when_no_deploy_phase():
    """No DEPLOY_PHASE in env → no prompt (fresh deploy)."""
    from types import SimpleNamespace
    deploy = importlib.import_module("scripts.deploy")
    args = SimpleNamespace(force=False, from_phase=None)
    # Empty env: no DEPLOY_PHASE → returns None (no prompt needed)
    result = deploy._resume_prompt({}, args)
    assert result is None, \
        f"_resume_prompt should return None for empty env; got {result!r}"


def test_resume_prompt_handles_complete_state(monkeypatch):
    """When DEPLOY_PHASE=complete, prompt should ask user; non-TTY default cancels."""
    from types import SimpleNamespace
    deploy = importlib.import_module("scripts.deploy")

    # Force non-TTY so _select takes the default path silently.
    # The default for COMPLETE state is "show summary" per REQ-E-321.
    captured = {"choice": None}

    def stub_select(question, choices, default=None):
        captured["choice"] = (question, choices, default)
        return default if default else choices[0]

    monkeypatch.setattr(deploy, "_select", stub_select)
    args = SimpleNamespace(force=False, from_phase=None)
    env = {"DEPLOY_PHASE": "complete"}
    result = deploy._resume_prompt(env, args)
    # The prompt must have happened
    assert captured["choice"] is not None
    # Result is one of: 'summary', 'force', 'cancel' (string sentinels)
    assert result in ("summary", "force", "cancel", None)


def test_resume_prompt_returns_from_phase_for_mid_state(monkeypatch):
    """When DEPLOY_PHASE is in WORK_PHASES (not last), default is to set
    args.from_phase to the next work phase."""
    from types import SimpleNamespace
    deploy = importlib.import_module("scripts.deploy")

    # Stub _select to return whatever the default was
    def stub_select(question, choices, default=None):
        return default if default else choices[0]

    monkeypatch.setattr(deploy, "_select", stub_select)
    args = SimpleNamespace(force=False, from_phase=None)
    env = {"DEPLOY_PHASE": "credentials"}
    deploy._resume_prompt(env, args)
    # After prompt with default, args.from_phase should be set to next work phase
    assert args.from_phase == "publish_data", \
        f"prompt should set args.from_phase=publish_data; got {args.from_phase!r}"


# ---------------------------------------------------------------------------
# TC-RESUME-PROMPT-001: prompt is called from run_deployment
# ---------------------------------------------------------------------------

def test_TC_RESUME_PROMPT_001_run_deployment_invokes_prompt():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.run_deployment)
    assert "_resume_prompt" in src, \
        "run_deployment must invoke _resume_prompt before phase guards"


# ---------------------------------------------------------------------------
# TC-RESUME-007: destroy clears all DEPLOY_* state keys (REQ-E-327)
# ---------------------------------------------------------------------------

def test_TC_RESUME_007_destroy_clears_all_deploy_state():
    destroy = importlib.import_module("scripts.destroy")
    src = inspect.getsource(destroy._remove_stale_credentials)
    for key in (
        "DEPLOY_PHASE",
        "DEPLOY_LAST_INTERRUPTED_PHASE",
        "DEPLOY_LAST_FAILURE",
        "DEPLOY_LAST_FAILED_PHASE",
    ):
        assert key in src, \
            f"destroy._remove_stale_credentials must clear {key} (REQ-E-327)"
