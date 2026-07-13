"""Tests for WORK_PHASES + real DEPLOY_PHASE resume (rec #2).

Spec: REQ-E-320, 322, 323, 324, 325, 328, 329; INV-301.
"""

from __future__ import annotations

import importlib
import inspect
from types import SimpleNamespace

import pytest

# ---------------------------------------------------------------------------
# TC-RESUME-001: WORK_PHASES + COMPLETE_MARKER constants
# ---------------------------------------------------------------------------

def test_TC_RESUME_001_constants_exist():
    deploy = importlib.import_module("scripts.deploy")
    assert hasattr(deploy, "WORK_PHASES"), "deploy must define WORK_PHASES"
    assert hasattr(deploy, "COMPLETE_MARKER"), "deploy must define COMPLETE_MARKER"


def test_TC_RESUME_001_work_phases_value():
    deploy = importlib.import_module("scripts.deploy")
    expected = (
        "atlas_terraform",
        "mcp_server",
        "terraform",
        "credentials",
        "publish_data",
        "asp_setup",
        "flink_dml",
    )
    assert tuple(deploy.WORK_PHASES) == expected, \
        f"WORK_PHASES must equal {expected}; got {deploy.WORK_PHASES!r}"
    assert deploy.COMPLETE_MARKER == "complete"
    assert "complete" not in deploy.WORK_PHASES, \
        "complete must NOT be in WORK_PHASES; it's a terminal marker"


# ---------------------------------------------------------------------------
# TC-RESUME-005: _should_run_phase precedence matrix
# ---------------------------------------------------------------------------

def test_TC_RESUME_005_should_run_phase_precedence():
    deploy = importlib.import_module("scripts.deploy")

    # --force trumps everything
    args = SimpleNamespace(force=True, from_phase=None)
    for phase in deploy.WORK_PHASES:
        assert deploy._should_run_phase(phase, {"DEPLOY_PHASE": "complete"}, args), \
            f"--force must always run phase {phase}"

    # --from-phase: skip earlier phases, run from the named one onwards
    args = SimpleNamespace(force=False, from_phase="asp_setup")
    assert not deploy._should_run_phase("mcp_server", {}, args)
    assert not deploy._should_run_phase("terraform", {}, args)
    assert deploy._should_run_phase("asp_setup", {}, args)
    assert deploy._should_run_phase("flink_dml", {}, args)

    # DEPLOY_PHASE alone: skip phases at-or-before recorded value, run later
    args = SimpleNamespace(force=False, from_phase=None)
    env = {"DEPLOY_PHASE": "credentials"}
    assert not deploy._should_run_phase("atlas_terraform", env, args)
    assert not deploy._should_run_phase("mcp_server", env, args)
    assert not deploy._should_run_phase("credentials", env, args)
    assert deploy._should_run_phase("publish_data", env, args)
    assert deploy._should_run_phase("flink_dml", env, args)

    # DEPLOY_PHASE=complete with no flags: every phase skipped
    env = {"DEPLOY_PHASE": "complete"}
    for phase in deploy.WORK_PHASES:
        assert not deploy._should_run_phase(phase, env, args), \
            f"complete state without --force must skip phase {phase}"

    # Empty env (fresh): run everything
    args = SimpleNamespace(force=False, from_phase=None)
    for phase in deploy.WORK_PHASES:
        assert deploy._should_run_phase(phase, {}, args), \
            f"fresh deploy must run phase {phase}"


# ---------------------------------------------------------------------------
# TC-RESUME-002: --from-phase argparse flag exists
# TC-RESUME-003: --from-phase + --force are mutex
# TC-RESUME-004: --force ignores DEPLOY_PHASE
# TC-RESUME-006: invalid phase = usage error
# TC-RESUME-008: --list-phases flag exists
# ---------------------------------------------------------------------------

def test_TC_RESUME_002_from_phase_flag_declared():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    assert '"--from-phase"' in src, "main() must declare --from-phase"


def test_TC_RESUME_003_force_and_from_phase_mutex():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    # The mutex check should be present (raise / sys.exit + a check that
    # both flags are set)
    assert "args.force" in src and "args.from_phase" in src, \
        "main() must consult both args.force and args.from_phase"
    # Some form of mutex enforcement: expect the words 'mutually exclusive'
    # or an explicit check that both are truthy.
    has_mutex = (
        "mutually exclusive" in src.lower()
        or ("args.force and args.from_phase" in src)
        or ("args.from_phase and args.force" in src)
    )
    assert has_mutex, "main() must enforce --force/--from-phase as mutually exclusive"


def test_TC_RESUME_004_force_flag_declared():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    assert '"--force"' in src, "main() must declare --force"


def test_TC_RESUME_006_invalid_from_phase_rejected():
    """argparse `choices=` should restrict --from-phase to WORK_PHASES values."""
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    # Either argparse choices=WORK_PHASES or an explicit validation
    has_validation = (
        "choices=" in src and "WORK_PHASES" in src
    ) or "from_phase not in" in src or "_phase_index" in src
    assert has_validation, \
        "main() must validate --from-phase against WORK_PHASES"


def test_TC_RESUME_008_list_phases_flag_declared():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    assert '"--list-phases"' in src, "main() must declare --list-phases"


# ---------------------------------------------------------------------------
# TC-RESUME-009: atlas_terraform respects TF_VAR_create_atlas_cluster
# ---------------------------------------------------------------------------

def test_TC_RESUME_009_atlas_terraform_conditional():
    """When TF_VAR_create_atlas_cluster is unset/false, atlas_terraform skips."""
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.run_deployment)
    # The original code had `if creating_cluster:` gating the atlas_terraform
    # phase. That gate must remain (REQ-E-329).
    assert "creating_cluster" in src, \
        "run_deployment must continue to gate atlas_terraform on creating_cluster"


# ---------------------------------------------------------------------------
# TC-RESUME-INV-001 (INV-301): all 8 _save_env(DEPLOY_PHASE,...) sites preserved
# ---------------------------------------------------------------------------

def test_TC_RESUME_INV_001_phase_writes_preserved():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.run_deployment)
    expected_phases = [
        "atlas_terraform", "mcp_server", "terraform", "credentials",
        "publish_data", "asp_setup", "flink_dml", "complete",
    ]
    for phase in expected_phases:
        # Each phase must be written to DEPLOY_PHASE somewhere in run_deployment
        assert f'"{phase}"' in src, \
            f"run_deployment must still record DEPLOY_PHASE={phase}"


# ---------------------------------------------------------------------------
# TC-RESUME-END-001: DEPLOY_PHASE marks the LAST COMPLETED phase
# ---------------------------------------------------------------------------
# A phase marker written at phase START is a bug: _should_run_phase uses a
# strictly-greater comparison and _next_work_phase returns the phase AFTER the
# recorded one, so both assume the recorded phase already COMPLETED. If a phase
# fails after writing its own marker, resume skips it. These tests pin that each
# work-phase marker is written only after that phase's work in run_deployment.

def _index_of(src: str, needle: str) -> int:
    i = src.find(needle)
    assert i != -1, f"expected to find {needle!r} in run_deployment source"
    return i


def test_TC_RESUME_END_001_flink_dml_marker_follows_work():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.run_deployment)
    # The gating call must precede the success marker for flink_dml.
    call_pos = _index_of(src, "_create_flink_dml_statements(root)")
    marker_pos = _index_of(src, '_save_env("DEPLOY_PHASE", "flink_dml")')
    assert call_pos < marker_pos, (
        "DEPLOY_PHASE=flink_dml must be written AFTER _create_flink_dml_statements "
        "so a failed phase is re-run on resume, not skipped"
    )


def test_TC_RESUME_END_002_publish_data_marker_follows_work():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.run_deployment)
    call_pos = _index_of(src, "_publish_local_data(root)")
    marker_pos = _index_of(src, '_save_env("DEPLOY_PHASE", "publish_data")')
    assert call_pos < marker_pos, (
        "DEPLOY_PHASE=publish_data must be written AFTER _publish_local_data"
    )


def test_TC_RESUME_END_003_terraform_marker_follows_apply():
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.run_deployment)
    apply_pos = _index_of(src, "All Terraform deployments completed successfully")
    marker_pos = _index_of(src, '_save_env("DEPLOY_PHASE", "terraform")')
    assert apply_pos < marker_pos, (
        "DEPLOY_PHASE=terraform must be written AFTER the apply loop succeeds"
    )


def test_TC_RESUME_END_004_failing_phase_is_rerun_not_skipped():
    """Semantic check: with DEPLOY_PHASE=credentials (last COMPLETED), the next
    phase (publish_data) must run and credentials must be skipped."""
    deploy = importlib.import_module("scripts.deploy")
    args = SimpleNamespace(force=False, from_phase=None)
    env = {"DEPLOY_PHASE": "credentials"}
    assert not deploy._should_run_phase("credentials", env, args)
    assert deploy._should_run_phase("publish_data", env, args)


# ---------------------------------------------------------------------------
# Helper: _phase_index / _next_work_phase
# ---------------------------------------------------------------------------

def test_phase_index_helper():
    deploy = importlib.import_module("scripts.deploy")
    assert deploy._phase_index("atlas_terraform") == 0
    assert deploy._phase_index("flink_dml") == len(deploy.WORK_PHASES) - 1
    with pytest.raises(ValueError):
        deploy._phase_index("complete")
    with pytest.raises(ValueError):
        deploy._phase_index("nonexistent")


def test_next_work_phase_helper():
    deploy = importlib.import_module("scripts.deploy")
    assert deploy._next_work_phase("atlas_terraform") == "mcp_server"
    assert deploy._next_work_phase("flink_dml") is None
    assert deploy._next_work_phase("complete") is None
    # Unknown -> first phase (helps fresh deploys)
    assert deploy._next_work_phase("") == "atlas_terraform"
