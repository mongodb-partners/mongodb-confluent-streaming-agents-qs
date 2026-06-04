"""Tests for print_deployment_summary + --summary flag (rec #5 of deploy-robustness).

Spec: REQ-E-310, 311, 314, 315, 317.
"""

from __future__ import annotations

import importlib
import inspect
import sys

import pytest


@pytest.fixture
def cli_output(tmp_path):
    """Use the already-imported cli_output module (do NOT re-import — that
    breaks scripts.deploy's bound reference). Just (re-)initialize it."""
    mod = importlib.import_module("scripts.common.cli_output")
    mod.init(quiet=False, debug=False, log_dir=tmp_path)
    yield mod
    if mod._S.log_fh is not None:
        mod._S.log_fh.close()
        mod._S.log_fh = None


# ---------------------------------------------------------------------------
# TC-SUM-001: print_deployment_summary exists with correct signature
# ---------------------------------------------------------------------------

def test_TC_SUM_001_function_exists():
    deploy = importlib.import_module("scripts.deploy")
    assert hasattr(deploy, "print_deployment_summary"), \
        "deploy.py must define print_deployment_summary"
    sig = inspect.signature(deploy.print_deployment_summary)
    params = list(sig.parameters)
    assert "env" in params and "root" in params, \
        f"print_deployment_summary signature must include env, root; got {params}"


# ---------------------------------------------------------------------------
# TC-SUM-002: summary contains all 5 required sections
# ---------------------------------------------------------------------------

def test_TC_SUM_002_summary_sections(cli_output, tmp_path, monkeypatch):
    deploy = importlib.import_module("scripts.deploy")

    # Stub health.collect_report so we don't need a live deployment
    fake_report = {
        "overall": "healthy",
        "flink": [{"name": "x", "status": "ok"}],
        "asp": [],
        "kafka": [],
        "mongo": [],
    }
    health = importlib.import_module("scripts.health")
    monkeypatch.setattr(health, "collect_report", lambda: fake_report)

    env = {
        "TF_VAR_mcp_server_url": "https://example.com/mcp",
        "TF_VAR_mongodb_connection_string": "mongodb+srv://user:pass@cluster/",
    }
    with cli_output.capture() as (out, _log):
        deploy.print_deployment_summary(env, tmp_path)
    text = "\n".join(out)
    for required in ("Confluent", "Atlas", "MCP", "Dashboard", "Next steps"):
        assert required in text, f"summary must include section '{required}'; got:\n{text}"


# ---------------------------------------------------------------------------
# TC-SUM-003: summary calls health.collect_report
# ---------------------------------------------------------------------------

def test_TC_SUM_003_calls_health(cli_output, tmp_path, monkeypatch):
    deploy = importlib.import_module("scripts.deploy")
    health = importlib.import_module("scripts.health")
    called = {"n": 0}

    def fake_report():
        called["n"] += 1
        return {"overall": "healthy", "flink": [], "asp": [], "kafka": [], "mongo": []}

    monkeypatch.setattr(health, "collect_report", fake_report)
    with cli_output.capture():
        deploy.print_deployment_summary({}, tmp_path)
    assert called["n"] >= 1, "print_deployment_summary must call health.collect_report"


# ---------------------------------------------------------------------------
# TC-SUM-006: --summary flag prints summary without doing deploy work
# ---------------------------------------------------------------------------

def test_TC_SUM_006_summary_flag_in_main():
    """The --summary CLI flag is declared in main()."""
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    assert '"--summary"' in src, "main() must declare --summary flag"


# ---------------------------------------------------------------------------
# TC-SUM-007: DEPLOY_LAST_* keys cleared on completion
# ---------------------------------------------------------------------------

def test_TC_SUM_007_complete_clears_state():
    """When run_deployment writes DEPLOY_PHASE=complete, it clears DEPLOY_LAST_*
    (delegated to _clear_deploy_failure_state)."""
    deploy = importlib.import_module("scripts.deploy")
    run_src = inspect.getsource(deploy.run_deployment)
    # run_deployment must invoke the cleanup helper after DEPLOY_PHASE=complete
    assert "_clear_deploy_failure_state" in run_src, \
        "run_deployment must call _clear_deploy_failure_state on success"
    assert hasattr(deploy, "_clear_deploy_failure_state"), \
        "deploy.py must define _clear_deploy_failure_state"
    helper_src = inspect.getsource(deploy._clear_deploy_failure_state)
    for key in ("DEPLOY_LAST_INTERRUPTED_PHASE",
                "DEPLOY_LAST_FAILURE",
                "DEPLOY_LAST_FAILED_PHASE"):
        assert key in helper_src, \
            f"_clear_deploy_failure_state must clear {key}"


# ---------------------------------------------------------------------------
# TC-SUM-008: --summary mode does NOT launch dashboard
# ---------------------------------------------------------------------------

def test_TC_SUM_008_summary_mode_no_dashboard():
    """The --summary handler in main() does not call _launch_dashboard."""
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    # Find the --summary branch by string search; assert _launch_dashboard
    # is NOT in the same handler block.
    assert '"--summary"' in src
    # Source-level proxy: _handle_summary_mode (or equivalent inline branch)
    # exits via sys.exit(0) without calling run_deployment / _launch_dashboard.
    summary_idx = src.find('args.summary')
    if summary_idx == -1:
        summary_idx = src.find('"--summary"')
    # Take 600 chars after the summary check; that's the branch body.
    branch = src[summary_idx:summary_idx + 800] if summary_idx >= 0 else ""
    assert "_launch_dashboard" not in branch, \
        "--summary handler must not call _launch_dashboard"
    assert "run_deployment" not in branch, \
        "--summary handler must not call run_deployment"
