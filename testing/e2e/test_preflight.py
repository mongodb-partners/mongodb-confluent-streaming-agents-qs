"""Tests for the preflight framework (rec #4).

Spec: REQ-E-330..339.
"""

from __future__ import annotations

import importlib
import inspect
from dataclasses import is_dataclass
from typing import get_args, get_origin

import pytest

# ---------------------------------------------------------------------------
# TC-PRE-INIT-001: module + public API
# ---------------------------------------------------------------------------

def test_TC_PRE_INIT_001_module_exists():
    pre = importlib.import_module("scripts.preflight")
    for name in ("Check", "CheckResult", "run_preflight", "main", "CHECKS"):
        assert hasattr(pre, name), f"preflight must expose {name}"


# ---------------------------------------------------------------------------
# TC-PRE-INIT-002: Check / CheckResult dataclasses
# ---------------------------------------------------------------------------

def test_TC_PRE_INIT_002_check_dataclass():
    pre = importlib.import_module("scripts.preflight")
    assert is_dataclass(pre.Check)
    assert is_dataclass(pre.CheckResult)
    fields = {f.name for f in pre.Check.__dataclass_fields__.values()}
    for required in ("name", "phases", "severity", "network", "run"):
        assert required in fields, f"Check missing field {required}"
    rfields = {f.name for f in pre.CheckResult.__dataclass_fields__.values()}
    for required in ("status", "message", "remediation"):
        assert required in rfields, f"CheckResult missing field {required}"


# ---------------------------------------------------------------------------
# TC-PRE-PHASE-001: phase filtering
# ---------------------------------------------------------------------------

def test_TC_PRE_PHASE_001_phase_filter():
    pre = importlib.import_module("scripts.preflight")

    def always_pass(env):
        return pre.CheckResult("pass", "ok")

    fake_checks = [
        pre.Check("a", (), "fail", network=False, run=always_pass),  # always-run
        pre.Check("b", ("flink_dml",), "fail", network=False, run=always_pass),
        pre.Check("c", ("mcp_server",), "fail", network=False, run=always_pass),
    ]
    # When phase=flink_dml, only a and b should run
    relevant = pre._filter_checks(fake_checks, phase="flink_dml", skip_network=False)
    names = {c.name for c in relevant}
    assert names == {"a", "b"}, f"phase filter wrong: got {names}"


# ---------------------------------------------------------------------------
# TC-PRE-SUM-001: returns (passed, warned, failed)
# ---------------------------------------------------------------------------

def test_TC_PRE_SUM_001_summary_tuple():
    pre = importlib.import_module("scripts.preflight")
    sig = inspect.signature(pre.run_preflight)
    # Returns a 3-tuple of ints
    # Convention check: three params phase, skip_network, env
    params = list(sig.parameters)
    for p in ("phase", "skip_network", "env"):
        assert p in params, f"run_preflight missing param {p}"


def test_TC_PRE_SUM_001_returns_counts(monkeypatch):
    pre = importlib.import_module("scripts.preflight")

    def f_pass(env):  return pre.CheckResult("pass", "ok")
    def f_warn(env):  return pre.CheckResult("warn", "meh")
    def f_fail(env):  return pre.CheckResult("fail", "no")

    fake_checks = [
        pre.Check("p", (), "fail", network=False, run=f_pass),
        pre.Check("w", (), "warn", network=False, run=f_warn),
        pre.Check("f", (), "fail", network=False, run=f_fail),
    ]
    monkeypatch.setattr(pre, "CHECKS", fake_checks)
    passed, warned, failed = pre.run_preflight(env={})
    assert (passed, warned, failed) == (1, 1, 1), \
        f"expected (1,1,1); got ({passed},{warned},{failed})"


# ---------------------------------------------------------------------------
# TC-PRE-CLI-001 / TC-PRE-CLI-002: CLI exit codes
# ---------------------------------------------------------------------------

def test_TC_PRE_CLI_001_main_returns_0_on_pass(monkeypatch):
    pre = importlib.import_module("scripts.preflight")

    def f_pass(env): return pre.CheckResult("pass", "ok")
    monkeypatch.setattr(pre, "CHECKS",
                        [pre.Check("a", (), "fail", network=False, run=f_pass)])
    with pytest.raises(SystemExit) as exc_info:
        pre.main([])
    assert exc_info.value.code == 0


def test_TC_PRE_CLI_001_main_returns_1_on_fail(monkeypatch):
    pre = importlib.import_module("scripts.preflight")

    def f_fail(env): return pre.CheckResult("fail", "broken")
    monkeypatch.setattr(pre, "CHECKS",
                        [pre.Check("a", (), "fail", network=False, run=f_fail)])
    with pytest.raises(SystemExit) as exc_info:
        pre.main([])
    assert exc_info.value.code == 1


def test_TC_PRE_CLI_002_json_flag(monkeypatch, capsys):
    pre = importlib.import_module("scripts.preflight")

    def f_pass(env): return pre.CheckResult("pass", "ok")
    monkeypatch.setattr(pre, "CHECKS",
                        [pre.Check("a", (), "fail", network=False, run=f_pass)])
    with pytest.raises(SystemExit):
        pre.main(["--json"])
    captured = capsys.readouterr()
    import json
    data = json.loads(captured.out)
    assert "checks" in data and "summary" in data, \
        f"JSON output must include 'checks' and 'summary'; got {data!r}"


# ---------------------------------------------------------------------------
# TC-PRE-TIMEOUT-001: 10s timeout caps each check
# ---------------------------------------------------------------------------

def test_TC_PRE_TIMEOUT_001_timeout(monkeypatch):
    pre = importlib.import_module("scripts.preflight")

    def hangs(env):
        import time
        time.sleep(15)
        return pre.CheckResult("pass", "ok")

    fake = [pre.Check("h", (), "fail", network=False, run=hangs)]
    monkeypatch.setattr(pre, "CHECKS", fake)
    # Use a short timeout for the test (1s)
    monkeypatch.setattr(pre, "CHECK_TIMEOUT_SECONDS", 1)
    passed, warned, failed = pre.run_preflight(env={})
    # Hung check should be counted as failed with a timeout message
    assert failed == 1
    assert passed == 0


# ---------------------------------------------------------------------------
# TC-PRE-SKIP-001: --skip-network
# ---------------------------------------------------------------------------

def test_TC_PRE_SKIP_001_skip_network(monkeypatch):
    pre = importlib.import_module("scripts.preflight")

    def f_pass(env): return pre.CheckResult("pass", "ok")
    fake = [
        pre.Check("offline", (), "fail", network=False, run=f_pass),
        pre.Check("online",  (), "fail", network=True,  run=f_pass),
    ]
    monkeypatch.setattr(pre, "CHECKS", fake)
    relevant = pre._filter_checks(fake, phase=None, skip_network=True)
    names = {c.name for c in relevant}
    assert "offline" in names
    assert "online" not in names, "skip_network should remove network=True checks"
