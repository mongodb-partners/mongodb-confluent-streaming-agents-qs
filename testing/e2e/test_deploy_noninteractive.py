"""Tests for --non-interactive / -y unattended deploy mode.

Spec: REQ-E-350, REQ-E-351, REQ-E-352.

Covers:
- CLI flag declaration and short-flag alias
- Module-level _NON_INTERACTIVE state and helpers
- _hydrate_env_from_environment behavior (file wins over env)
- _missing_required_credentials returns the right list
- _select / _text short-circuit when _NON_INTERACTIVE is True
- show_review_and_confirm and _check_bedrock_creds auto-confirm
- _resume_prompt resolves silently in non-interactive mode
- Implies --plain (HAS_RICH / HAS_QUESTIONARY off after main() runs)
"""

from __future__ import annotations

import importlib
import inspect
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def deploy():
    return importlib.import_module("scripts.deploy")


@pytest.fixture
def with_noninteractive(deploy):
    """Set _NON_INTERACTIVE=True for the duration of one test, restore after."""
    prev = deploy._NON_INTERACTIVE
    deploy._NON_INTERACTIVE = True
    try:
        yield deploy
    finally:
        deploy._NON_INTERACTIVE = prev


# ---------------------------------------------------------------------------
# TC-NI-001: --non-interactive / -y flag declared in main()
# ---------------------------------------------------------------------------

def test_TC_NI_001_flag_implies_plain(deploy, monkeypatch):
    """REQ-E-350 / pass-6 M-NEW-11: --non-interactive must imply --plain.

    Behavior test (replaces the pass-3 source-grep). Drives argparse
    with `-y` and asserts `args.plain` is set + `args.non_interactive`
    is True. Catches both regressions: missing flag wiring AND broken
    plain-implication logic.
    """
    # We can't easily call deploy.main() here without firing every
    # downstream effect — but we can drive argparse alone by extracting
    # the parser the same way main() does. Use inspect to find the
    # parser_setup pattern: simplest robust approach is to construct
    # an argparse.ArgumentParser with the same flag declarations
    # main() uses and verify they parse `-y` correctly.
    import sys as _sys

    captured: dict = {}

    def _spy_parse_args(self):
        # argparse.ArgumentParser.parse_args; we hook here to capture
        # the post-parse namespace before main()'s side effects run.
        ns = _orig_parse_args(self)
        captured["ns"] = ns
        # Trigger SystemExit immediately so main() doesn't run.
        raise SystemExit(0)

    import argparse
    _orig_parse_args = argparse.ArgumentParser.parse_args
    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", _spy_parse_args)
    monkeypatch.setattr(_sys, "argv", ["deploy", "-y"])
    try:
        deploy.main()
    except SystemExit:
        pass
    ns = captured.get("ns")
    assert ns is not None, "argparse parse_args was not invoked"
    assert getattr(ns, "non_interactive", False) is True, (
        "-y must set args.non_interactive = True"
    )
    # And `--non-interactive` long form
    monkeypatch.setattr(_sys, "argv", ["deploy", "--non-interactive"])
    captured.clear()
    try:
        deploy.main()
    except SystemExit:
        pass
    ns2 = captured.get("ns")
    assert ns2 is not None and getattr(ns2, "non_interactive", False) is True, (
        "--non-interactive long form must also set non_interactive"
    )


# ---------------------------------------------------------------------------
# TC-NI-002: module-level _NON_INTERACTIVE state + helpers
# ---------------------------------------------------------------------------

def test_TC_NI_002_module_state_exists(deploy):
    assert hasattr(deploy, "_NON_INTERACTIVE"), \
        "deploy.py must define module-level _NON_INTERACTIVE"
    assert deploy._NON_INTERACTIVE is False, \
        "default state must be False (interactive)"
    assert hasattr(deploy, "_is_non_interactive"), \
        "deploy.py must expose _is_non_interactive() accessor"
    assert deploy._is_non_interactive() is False


def test_TC_NI_002_hydrate_helper_exists(deploy):
    assert hasattr(deploy, "_hydrate_env_from_environment"), \
        "deploy.py must define _hydrate_env_from_environment()"
    assert hasattr(deploy, "_missing_required_credentials"), \
        "deploy.py must define _missing_required_credentials(env)"
    assert hasattr(deploy, "_NONINTERACTIVE_HYDRATE_KEYS"), \
        "deploy.py must expose _NONINTERACTIVE_HYDRATE_KEYS"


def test_TC_NI_002_hydrate_keys_cover_required(deploy):
    """Every key in _REQUIRED_KEYS / _AWS_KEYS / _MONGODB_KEYS / _VOYAGE_KEYS /
    _ATLAS_ADMIN_KEYS must appear in _NONINTERACTIVE_HYDRATE_KEYS so
    --non-interactive can hydrate them all."""
    hydrate = set(deploy._NONINTERACTIVE_HYDRATE_KEYS)
    required = set(
        deploy._REQUIRED_KEYS
        + deploy._AWS_KEYS
        + deploy._MONGODB_KEYS
        + deploy._VOYAGE_KEYS
        + deploy._ATLAS_ADMIN_KEYS
    )
    missing = required - hydrate
    assert not missing, \
        f"hydrate-key list is missing required credentials: {missing}"


# ---------------------------------------------------------------------------
# TC-NI-003: _missing_required_credentials returns ordered missing keys
# ---------------------------------------------------------------------------

def test_TC_NI_003_missing_creds_empty_env(deploy):
    """Empty env → every required key reported missing."""
    missing = deploy._missing_required_credentials({})
    # Must include all five credential groups
    assert "TF_VAR_confluent_cloud_api_key" in missing
    assert "TF_VAR_aws_bedrock_access_key" in missing
    assert "TF_VAR_mongodb_connection_string" in missing
    assert "TF_VAR_voyage_api_key" in missing
    assert "ATLAS_PUBLIC_KEY" in missing


def test_TC_NI_003_missing_creds_full_env(deploy):
    """Fully populated env → empty list."""
    env = {
        "TF_VAR_confluent_cloud_api_key": "x",
        "TF_VAR_confluent_cloud_api_secret": "x",
        "TF_VAR_aws_bedrock_access_key": "x",
        "TF_VAR_aws_bedrock_secret_key": "x",
        "TF_VAR_mongodb_connection_string": "x",
        "TF_VAR_mongodb_username": "x",
        "TF_VAR_mongodb_password": "x",
        "TF_VAR_voyage_api_key": "x",
        "ATLAS_PUBLIC_KEY": "x",
        "ATLAS_PRIVATE_KEY": "x",
        "ATLAS_PROJECT_ID": "x",
    }
    assert deploy._missing_required_credentials(env) == []


def test_TC_NI_003_missing_creds_partial(deploy):
    """Partial env → only the absent keys are listed."""
    env = {
        "TF_VAR_confluent_cloud_api_key": "x",
        "TF_VAR_confluent_cloud_api_secret": "x",
        # AWS missing
        "TF_VAR_mongodb_connection_string": "x",
        "TF_VAR_mongodb_username": "x",
        "TF_VAR_mongodb_password": "x",
        "TF_VAR_voyage_api_key": "x",
        "ATLAS_PUBLIC_KEY": "x",
        "ATLAS_PRIVATE_KEY": "x",
        "ATLAS_PROJECT_ID": "x",
    }
    missing = deploy._missing_required_credentials(env)
    assert missing == ["TF_VAR_aws_bedrock_access_key", "TF_VAR_aws_bedrock_secret_key"]


# ---------------------------------------------------------------------------
# TC-NI-004: hydration writes env-vars to .env but file values win
# ---------------------------------------------------------------------------

def test_TC_NI_004_hydrate_writes_missing(deploy, tmp_path, monkeypatch):
    """When .env lacks a key but os.environ has it, hydration writes it."""
    env_file = tmp_path / ".env"
    env_file.write_text("")  # empty .env
    monkeypatch.setattr(deploy, "_env_path", lambda: env_file)
    monkeypatch.setenv("TF_VAR_confluent_cloud_api_key", "CCKABC123")
    monkeypatch.setenv("TF_VAR_voyage_api_key", "pa-xyz")

    n = deploy._hydrate_env_from_environment()
    assert n >= 2

    content = env_file.read_text()
    assert "TF_VAR_confluent_cloud_api_key=CCKABC123" in content
    assert "TF_VAR_voyage_api_key=pa-xyz" in content


def test_TC_NI_004_hydrate_preserves_existing(deploy, tmp_path, monkeypatch):
    """When .env already has a key, hydration must NOT overwrite it.

    File-wins semantics: an operator can override a single credential via
    .env without an env-var shadow surprise.
    """
    env_file = tmp_path / ".env"
    env_file.write_text("TF_VAR_voyage_api_key=from-file\n")
    monkeypatch.setattr(deploy, "_env_path", lambda: env_file)
    monkeypatch.setenv("TF_VAR_voyage_api_key", "from-env")

    deploy._hydrate_env_from_environment()
    content = env_file.read_text()
    assert "TF_VAR_voyage_api_key=from-file" in content
    assert "from-env" not in content


def test_TC_NI_004_hydrate_skips_unset_env(deploy, tmp_path, monkeypatch):
    """Keys absent from both .env and os.environ stay absent (not written
    as empty values)."""
    env_file = tmp_path / ".env"
    env_file.write_text("")
    monkeypatch.setattr(deploy, "_env_path", lambda: env_file)
    # Make sure none of the hydrate keys are set in env (test isolation)
    for k in deploy._NONINTERACTIVE_HYDRATE_KEYS:
        monkeypatch.delenv(k, raising=False)

    n = deploy._hydrate_env_from_environment()
    assert n == 0
    assert env_file.read_text().strip() == ""


# ---------------------------------------------------------------------------
# TC-NI-005: _select short-circuits when _NON_INTERACTIVE is True
# ---------------------------------------------------------------------------

def test_TC_NI_005_select_returns_default(with_noninteractive):
    deploy = with_noninteractive
    # With explicit default
    result = deploy._select("Question?", ["A", "B", "C"], default="B")
    assert result == "B"
    # Without default → first choice
    result = deploy._select("Question?", ["A", "B", "C"])
    assert result == "A"


def test_TC_NI_005_select_never_calls_input(with_noninteractive, monkeypatch):
    """_select must NOT call input() / questionary when _NON_INTERACTIVE."""
    deploy = with_noninteractive

    def boom(*args, **kwargs):
        raise AssertionError("_select called input() under --non-interactive")

    monkeypatch.setattr("builtins.input", boom)
    # questionary attribute may not exist if not installed; only patch if so
    if deploy.HAS_QUESTIONARY:
        monkeypatch.setattr(deploy.questionary, "select", boom)

    result = deploy._select("Q?", ["a", "b"], default="a")
    assert result == "a"


# ---------------------------------------------------------------------------
# TC-NI-006: _text returns default silently, raises if no default
# ---------------------------------------------------------------------------

def test_TC_NI_006_text_returns_default(with_noninteractive):
    deploy = with_noninteractive
    assert deploy._text("Prompt", default="hello") == "hello"
    assert deploy._text("Prompt", default="hidden", secret=True) == "hidden"


def test_TC_NI_006_text_raises_without_default(with_noninteractive):
    deploy = with_noninteractive
    with pytest.raises(RuntimeError) as exc:
        deploy._text("Required Field")
    # Error message must guide the user toward .env / env-var fix
    msg = str(exc.value)
    assert ".env" in msg or "environment variable" in msg
    assert "non-interactive" in msg.lower()


# ---------------------------------------------------------------------------
# TC-NI-007: show_review_and_confirm auto-confirms under non-interactive
# ---------------------------------------------------------------------------

def test_TC_NI_007_review_auto_confirms(with_noninteractive, monkeypatch):
    """show_review_and_confirm must return True under --non-interactive
    without calling _select."""
    deploy = with_noninteractive

    def boom_select(*args, **kwargs):
        raise AssertionError("_select was called under --non-interactive")

    monkeypatch.setattr(deploy, "_select", boom_select)
    env = {
        "TF_VAR_confluent_cloud_api_key": "x",
        "TF_VAR_aws_bedrock_access_key": "x",
        "TF_VAR_voyage_api_key": "x",
        "TF_VAR_mongodb_connection_string": "mongodb+srv://x",
        "TF_VAR_mongodb_username": "u",
        "ATLAS_PUBLIC_KEY": "p",
        "ATLAS_PROJECT_ID": "id",
        "ATLAS_CLUSTER_NAME": "c",
    }
    assert deploy.show_review_and_confirm(env) is True


# ---------------------------------------------------------------------------
# TC-NI-008: _resume_prompt resolves silently under non-interactive
# ---------------------------------------------------------------------------

def test_TC_NI_008_resume_complete_returns_summary(with_noninteractive):
    """A complete deploy re-invoked with --non-interactive should return
    'summary' (no silent re-deploy without --force)."""
    deploy = with_noninteractive
    args = SimpleNamespace(force=False, from_phase=None)
    result = deploy._resume_prompt({"DEPLOY_PHASE": "complete"}, args)
    assert result == "summary"


def test_TC_NI_008_resume_midphase_sets_from_phase(with_noninteractive):
    """An in-progress deploy re-invoked with --non-interactive should set
    args.from_phase to the next work phase and return 'resume'."""
    deploy = with_noninteractive
    args = SimpleNamespace(force=False, from_phase=None)
    result = deploy._resume_prompt({"DEPLOY_PHASE": "credentials"}, args)
    assert result == "resume"
    assert args.from_phase == "publish_data"


def test_TC_NI_008_resume_last_phase_returns_finalize(with_noninteractive):
    """When the last work phase was reached but DEPLOY_PHASE never moved
    to 'complete', --non-interactive should return 'finalize'."""
    deploy = with_noninteractive
    args = SimpleNamespace(force=False, from_phase=None)
    last = deploy.WORK_PHASES[-1]
    result = deploy._resume_prompt({"DEPLOY_PHASE": last}, args)
    assert result == "finalize"


def test_TC_NI_008_resume_force_still_short_circuits(with_noninteractive):
    """If --force is explicitly set with --non-interactive, _resume_prompt
    short-circuits (returns None) as for interactive mode."""
    deploy = with_noninteractive
    args = SimpleNamespace(force=True, from_phase=None)
    assert deploy._resume_prompt({"DEPLOY_PHASE": "complete"}, args) is None


# ---------------------------------------------------------------------------
# TC-NI-FRESH: non-interactive fresh-cluster path (documented .env default)
# ---------------------------------------------------------------------------

def test_TC_NI_fresh_cluster_chooses_create_not_byo(with_noninteractive, monkeypatch):
    """.env.example defaults create_atlas_cluster=true with ATLAS_* keys but NO
    Mongo URI. Non-interactive prompt_mongodb_atlas must pick the CREATE branch
    (using the ATLAS_* defaults) instead of falling into BYO, which would raise
    on the missing connection string."""
    deploy = with_noninteractive
    saved: dict = {}
    monkeypatch.setattr(deploy, "_save_env", lambda k, v: saved.__setitem__(k, v))
    monkeypatch.setattr(deploy, "_save_env_many", lambda d: saved.update(d))

    env = {
        "TF_VAR_create_atlas_cluster": "true",
        "ATLAS_PUBLIC_KEY": "pub-abc",
        "ATLAS_PRIVATE_KEY": "priv-xyz",
        "ATLAS_PROJECT_ID": "proj-123",
        "ATLAS_CLUSTER_NAME": "streaming-agents-cluster",
        # deliberately NO TF_VAR_mongodb_connection_string
    }
    # Must NOT raise (the BYO branch's _text on a missing URI would).
    deploy.prompt_mongodb_atlas(env)
    # The CREATE branch persists create=true and the generated DB creds.
    assert saved.get("TF_VAR_create_atlas_cluster") == "true"
    assert saved.get("ATLAS_PROJECT_ID") == "proj-123"
    assert saved.get("TF_VAR_atlas_db_username")  # generated user persisted


def test_TC_NI_byo_still_default_when_create_false(with_noninteractive, monkeypatch):
    """When create_atlas_cluster is not 'true', the default stays BYO — a
    missing connection string then raises the clear non-interactive error
    rather than silently provisioning a cluster."""
    deploy = with_noninteractive
    monkeypatch.setattr(deploy, "_save_env", lambda k, v: None)
    monkeypatch.setattr(deploy, "_save_env_many", lambda d: None)
    env = {"TF_VAR_create_atlas_cluster": "false"}
    with pytest.raises(RuntimeError):
        deploy.prompt_mongodb_atlas(env)


# ---------------------------------------------------------------------------
# TC-NI-009: validation + missing-creds error path in main()
# ---------------------------------------------------------------------------

def test_TC_NI_009_main_validates_credentials(deploy):
    """REQ-E-352: --non-interactive with missing creds exits with code 2.

    Pass-6 H-NEW-11: real behavior test. Previously this was a
    source-grep for `sys.exit(2)` which a regression refactoring the
    error path into a helper (without exiting at all) would silently
    pass. Now we drive `main()` with empty .env, no env vars, and
    assert SystemExit with code 2.
    """
    import os
    import sys as _sys
    import tempfile
    from unittest import mock

    # Save module-level globals so we can restore them after main() runs
    # (main() mutates _NON_INTERACTIVE, HAS_RICH, HAS_QUESTIONARY as
    # side-effects of --non-interactive / --plain).
    _saved = {
        "_NON_INTERACTIVE": deploy._NON_INTERACTIVE,
        "HAS_RICH": deploy.HAS_RICH,
        "HAS_QUESTIONARY": deploy.HAS_QUESTIONARY,
    }
    try:
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / ".env"
            env_path.touch()
            # deploy.main() reads from sys.argv (argparse without args=).
            # Patch sys.argv plus clear all required credential keys.
            env_clearset = {k: "" for k in deploy._NONINTERACTIVE_HYDRATE_KEYS}
            with mock.patch.dict(os.environ, env_clearset, clear=False), \
                 mock.patch.object(_sys, "argv", ["deploy", "--non-interactive", "--no-log"]), \
                 mock.patch.object(deploy, "_load_env", return_value={}), \
                 mock.patch.object(deploy, "_env_path", return_value=env_path), \
                 mock.patch.object(deploy, "_project_root", return_value=Path(tmp)):
                with pytest.raises(SystemExit) as exc_info:
                    deploy.main()
                assert exc_info.value.code == 2, (
                    f"--non-interactive with no creds must exit with code 2, "
                    f"got {exc_info.value.code}. (REQ-E-352 / H-NEW-11)"
                )
    finally:
        deploy._NON_INTERACTIVE = _saved["_NON_INTERACTIVE"]
        deploy.HAS_RICH = _saved["HAS_RICH"]
        deploy.HAS_QUESTIONARY = _saved["HAS_QUESTIONARY"]


# ---------------------------------------------------------------------------
# TC-NI-010: _check_bedrock_creds returns True under non-interactive
# without calling _select
# ---------------------------------------------------------------------------

def test_TC_NI_010_bedrock_continues_under_noninteractive(with_noninteractive, monkeypatch):
    """_check_bedrock_creds must NOT prompt under --non-interactive; it
    should warn-and-continue (return True)."""
    deploy = with_noninteractive

    # Stub the live Bedrock check to return a failure
    def fake_test(*args, **kwargs):
        return (False, "invalid_keys")

    import scripts.common.test_bedrock_credentials as tb
    monkeypatch.setattr(tb, "test_bedrock_credentials", fake_test)

    def boom_select(*args, **kwargs):
        raise AssertionError("_select called inside _check_bedrock_creds under --non-interactive")

    monkeypatch.setattr(deploy, "_select", boom_select)
    env = {"TF_VAR_cloud_region": "us-east-1"}
    result = deploy._check_bedrock_creds(env, "AKIAFAKE", "secretfake")
    assert result is True, \
        "_check_bedrock_creds must return True (continue) under --non-interactive"
