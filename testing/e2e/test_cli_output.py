"""Tests for scripts.common.cli_output (rec #3 of deploy-robustness-improvements).

Spec: specs/deploy-robustness-improvements/requirements.md REQ-E-300..309, REQ-E-300a.
"""

from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path

import pytest

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


@pytest.fixture
def cli_output(tmp_path):
    """Fresh import of cli_output with init() pointing at a tmp log dir."""
    mod = importlib.import_module("scripts.common.cli_output")
    mod.init(quiet=False, debug=False, log_dir=tmp_path)
    yield mod
    # close log file so tmp_path can be cleaned
    if mod._S.log_fh is not None:
        mod._S.log_fh.close()
        mod._S.log_fh = None


def _strip_ansi(s: str) -> str:
    return ANSI_RE.sub("", s)


# ---------------------------------------------------------------------------
# TC-CLI-001: public API
# ---------------------------------------------------------------------------

def test_TC_CLI_001_public_api():
    mod = importlib.import_module("scripts.common.cli_output")
    for name in ("info", "warn", "error", "success", "debug", "step",
                 "kv", "section", "subsection", "init", "capture"):
        assert hasattr(mod, name), f"cli_output missing public function: {name}"


# ---------------------------------------------------------------------------
# TC-CLI-CAPTURE-001 (REQ-E-300a): capture() context manager
# ---------------------------------------------------------------------------

def test_TC_CLI_CAPTURE_001_basic_capture(cli_output):
    with cli_output.capture() as (out, log):
        cli_output.info("hello")
    joined_out = " ".join(out)
    joined_log = " ".join(log)
    assert "[info]" in joined_out
    assert "hello" in joined_out
    assert "hello" in joined_log


def test_prefix_taxonomy_matches_existing_codebase(cli_output):
    """Prefixes are kept aligned with the existing [ok]/[FAIL]/[warn] convention
    so the refactor in REQ-E-307 is mechanical and preserves test contracts."""
    with cli_output.capture() as (out, _log):
        cli_output.info("i")
        cli_output.warn("w")
        cli_output.error("e")
        cli_output.success("s")
    joined = " ".join(out)
    assert "[info]" in joined
    assert "[warn]" in joined
    assert "[FAIL]" in joined
    assert "[ok]" in joined


def test_TC_CLI_CAPTURE_001_nested_capture(cli_output):
    with cli_output.capture() as (outer_out, outer_log):
        cli_output.info("outer")
        with cli_output.capture() as (inner_out, inner_log):
            cli_output.info("inner")
        # Outer continues to receive after inner exits
        cli_output.info("outer-again")

    inner_out_joined = " ".join(inner_out)
    outer_out_joined = " ".join(outer_out)
    assert "inner" in inner_out_joined
    assert "outer" not in inner_out_joined
    assert "outer-again" not in inner_out_joined
    assert "outer" in outer_out_joined
    assert "outer-again" in outer_out_joined
    # The inner line should also appear in outer (capture chains upward)
    assert "inner" in outer_out_joined


# ---------------------------------------------------------------------------
# TC-CLI-002: log file creation, naming, pruning
# ---------------------------------------------------------------------------

def test_TC_CLI_002_log_file_created(tmp_path):
    mod = importlib.import_module("scripts.common.cli_output")
    log_path = mod.init(log_dir=tmp_path)
    try:
        assert log_path.exists()
        assert log_path.name.startswith("deploy-")
        assert log_path.name.endswith(".log")
        # Timestamp pattern: deploy-YYYYMMDD-HHMMSS.log
        assert re.match(r"deploy-\d{8}-\d{6}\.log", log_path.name)
    finally:
        if mod._S.log_fh:
            mod._S.log_fh.close()
            mod._S.log_fh = None


def test_TC_CLI_002_log_pruning(tmp_path):
    """Files older than 7 days are removed; newer files are kept."""
    import os
    import time

    old = tmp_path / "deploy-19700101-000000.log"
    old.write_text("ancient")
    eight_days_ago = time.time() - (8 * 86400)
    os.utime(old, (eight_days_ago, eight_days_ago))

    new = tmp_path / "deploy-99991231-235959.log"
    new.write_text("recent")

    mod = importlib.import_module("scripts.common.cli_output")
    mod.init(log_dir=tmp_path)
    try:
        assert not old.exists(), "old log file should have been pruned"
        assert new.exists(), "recent log file should be preserved"
    finally:
        if mod._S.log_fh:
            mod._S.log_fh.close()
            mod._S.log_fh = None


# ---------------------------------------------------------------------------
# TC-CLI-003: --quiet suppresses info; other levels still emit
# ---------------------------------------------------------------------------

def test_TC_CLI_003_quiet_suppresses_info(tmp_path):
    mod = importlib.import_module("scripts.common.cli_output")
    mod.init(quiet=True, debug=False, log_dir=tmp_path)
    try:
        with mod.capture() as (out, log):
            mod.info("info-msg")
            mod.warn("warn-msg")
            mod.error("err-msg")
            mod.success("ok-msg")
            mod.step(1, 7, "step-msg")
            mod.kv("k", "v")
            mod.section("Title")
            mod.subsection("Sub")
        out_joined = " ".join(out)
        assert "info-msg" not in out_joined, "quiet should suppress info from stdout"
        assert "warn-msg" in out_joined
        assert "err-msg" in out_joined
        assert "ok-msg" in out_joined
        assert "step-msg" in out_joined
        assert "v" in out_joined
        assert "Title" in out_joined
        # Log file gets everything regardless
        log_joined = " ".join(log)
        assert "info-msg" in log_joined
    finally:
        if mod._S.log_fh:
            mod._S.log_fh.close()
            mod._S.log_fh = None


# ---------------------------------------------------------------------------
# TC-CLI-004: --debug controls debug visibility
# ---------------------------------------------------------------------------

def test_TC_CLI_004_debug_visibility(tmp_path):
    mod = importlib.import_module("scripts.common.cli_output")

    # Without --debug: debug appears in log only
    mod.init(quiet=False, debug=False, log_dir=tmp_path)
    try:
        with mod.capture() as (out, log):
            mod.debug("dbg-msg")
        assert "dbg-msg" not in " ".join(out)
        assert "dbg-msg" in " ".join(log)
    finally:
        if mod._S.log_fh:
            mod._S.log_fh.close()
            mod._S.log_fh = None

    # With --debug: debug appears in both
    mod = importlib.import_module("scripts.common.cli_output")
    mod.init(quiet=False, debug=True, log_dir=tmp_path)
    try:
        with mod.capture() as (out, log):
            mod.debug("dbg-msg")
        assert "dbg-msg" in " ".join(out)
        assert "dbg-msg" in " ".join(log)
    finally:
        if mod._S.log_fh:
            mod._S.log_fh.close()
            mod._S.log_fh = None


# ---------------------------------------------------------------------------
# TC-CLI-005: log file gets everything; ANSI stripped
# ---------------------------------------------------------------------------

def test_TC_CLI_005_log_file_unconditional_and_plain(cli_output):
    with cli_output.capture() as (_out, log):
        cli_output.info("info-line")
        cli_output.warn("warn-line")
        cli_output.error("error-line")
        cli_output.success("success-line")
        cli_output.step(1, 7, "step-line")

    log_joined = "\n".join(log)
    assert "info-line" in log_joined
    assert "warn-line" in log_joined
    assert "error-line" in log_joined
    assert "success-line" in log_joined
    assert "step-line" in log_joined
    # ANSI escape codes must not appear in log entries
    assert ANSI_RE.search(log_joined) is None, \
        "log file must be plain text without ANSI escape codes"


# ---------------------------------------------------------------------------
# TC-CLI-006: non-TTY stdout has no ANSI codes
# ---------------------------------------------------------------------------

def test_TC_CLI_006_non_tty_strips_ansi(tmp_path, monkeypatch):
    # Ensure stdout reports non-TTY
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False, raising=False)
    mod = importlib.import_module("scripts.common.cli_output")
    mod.init(log_dir=tmp_path)
    try:
        with mod.capture() as (out, _log):
            mod.info("plain")
            mod.error("err")
            mod.section("Title")
        joined = "\n".join(out)
        assert ANSI_RE.search(joined) is None, \
            f"non-TTY stdout must not contain ANSI codes, got: {joined!r}"
    finally:
        if mod._S.log_fh:
            mod._S.log_fh.close()
            mod._S.log_fh = None


# ---------------------------------------------------------------------------
# TC-CLI-007: step rendering
# ---------------------------------------------------------------------------

def test_TC_CLI_007_step_render(cli_output):
    with cli_output.capture() as (out, _log):
        cli_output.step(3, 7, "ASP setup")
    joined = _strip_ansi("\n".join(out))
    assert "[STEP 3/7]" in joined
    assert "ASP setup" in joined


# ---------------------------------------------------------------------------
# TC-CLI-008: kv rendering and truncation
# ---------------------------------------------------------------------------

def test_TC_CLI_008_kv_render(cli_output):
    with cli_output.capture() as (out, _log):
        cli_output.kv("Cluster", "lkc-abc123")
    plain = _strip_ansi("\n".join(out))
    assert "Cluster" in plain
    assert "lkc-abc123" in plain


def test_TC_CLI_008_kv_truncates_long_values(cli_output, monkeypatch):
    # Force a narrow terminal width
    monkeypatch.setattr("shutil.get_terminal_size",
                        lambda fallback=(80, 24): type("S", (), {"columns": 40, "lines": 24})())
    long_value = "x" * 200
    with cli_output.capture() as (out, _log):
        cli_output.kv("k", long_value)
    plain = _strip_ansi("\n".join(out))
    # Truncated line must be shorter than the original 200-char value
    assert len(plain.strip()) < 200
    assert "…" in plain or "..." in plain


# ---------------------------------------------------------------------------
# TC-CLI-009: section/subsection
# ---------------------------------------------------------------------------

def test_TC_CLI_009_section_render(cli_output):
    with cli_output.capture() as (out, _log):
        cli_output.section("Deployment summary")
    plain_lines = [_strip_ansi(line) for line in out]
    plain_joined = "\n".join(plain_lines)
    # Title appears
    assert "Deployment summary" in plain_joined
    # An underline line of dashes/equals follows the title
    assert any(set(line.strip()) <= {"-", "=", "─", "━"} and len(line.strip()) >= 3
               for line in plain_lines), \
        f"section should have an underline line; got: {plain_lines!r}"
    # Leading blank line: section should produce a blank entry before the title
    # (some renderers emit blank as empty string; others as "\n" — accept either)


def test_TC_CLI_009_subsection_render(cli_output):
    with cli_output.capture() as (out, _log):
        cli_output.subsection("Confluent")
    plain = _strip_ansi("\n".join(out))
    assert "Confluent" in plain
    # Subsection should NOT have an underline of repeated chars (3+)
    plain_lines = [_strip_ansi(line) for line in out]
    has_underline = any(
        len(line.strip()) >= 3 and set(line.strip()) <= {"-", "=", "─", "━"}
        for line in plain_lines
    )
    assert not has_underline, "subsection should not produce a separator line"


# ---------------------------------------------------------------------------
# TC-CLI-010: rich is importable (REQ-E-309)
# ---------------------------------------------------------------------------

def test_TC_CLI_010_rich_available():
    """rich is a hard dependency — pyproject.toml:49 declares rich>=13.0.0."""
    import rich  # noqa: F401
    import rich.console  # noqa: F401


# ---------------------------------------------------------------------------
# REQ-E-307: deploy.py + destroy.py wired up
# ---------------------------------------------------------------------------

def test_TC_CLI_REFACTOR_001_deploy_imports_cli_output():
    """deploy.py imports cli_output module."""
    import importlib
    import inspect
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy)
    assert "from scripts.common import cli_output" in src or \
        "from scripts.common.cli_output" in src, \
        "deploy.py must import cli_output"


def test_TC_CLI_REFACTOR_002_main_calls_cli_output_init():
    """main() initializes cli_output early so the session log is created."""
    import importlib
    import inspect
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    assert "cli_output.init(" in src, \
        "main() must call cli_output.init() to start the session log"


def test_TC_CLI_REFACTOR_003_quiet_and_debug_flags():
    """deploy.py declares --quiet and --debug CLI flags (REQ-E-302/303)."""
    import importlib
    import inspect
    deploy = importlib.import_module("scripts.deploy")
    src = inspect.getsource(deploy.main)
    assert '"--quiet"' in src, "deploy.py must declare --quiet"
    assert '"--debug"' in src, "deploy.py must declare --debug"


# ---------------------------------------------------------------------------
# TC-LOG-001..004: cli_output.init() name parameter + pytest pollution fix
# ---------------------------------------------------------------------------

def _cli_output_cleanup(co):
    """Close + null cli_output's log_fh so it doesn't bleed into the
    next test. Pairs with co.init() which reopens on demand."""
    if co._S.log_fh is not None:
        try:
            co._S.log_fh.close()
        except Exception:
            pass
    co._S.log_fh = None
    co._S.log_path = None


def test_TC_LOG_001_init_default_name_is_deploy(tmp_path, monkeypatch):
    """Backwards-compat: cli_output.init() default name is 'deploy'.

    Existing callers (deploy.py) rely on this so log filename stays
    `deploy-<UTC>.log`.
    """
    import importlib
    co = importlib.import_module("scripts.common.cli_output")
    # Force pytest-detection off so we test the production path
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    log_path = co.init(quiet=True, debug=False, log_dir=tmp_path)
    assert log_path.name.startswith("deploy-"), \
        f"default log name must be deploy-*, got {log_path.name!r}"
    assert log_path.suffix == ".log"
    _cli_output_cleanup(co)


def test_TC_LOG_002_init_accepts_name_parameter(tmp_path, monkeypatch):
    """Callers other than deploy can pass a custom name to avoid the
    misleading `deploy-*.log` filename. `uv run preflight` should
    produce `preflight-*.log`, not `deploy-*.log`."""
    import importlib
    co = importlib.import_module("scripts.common.cli_output")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    log_path = co.init(quiet=True, debug=False, log_dir=tmp_path, name="preflight")
    assert log_path.name.startswith("preflight-"), \
        f"named log must be preflight-*, got {log_path.name!r}"
    _cli_output_cleanup(co)


def test_TC_LOG_003_init_routes_to_tmp_when_pytest_running(monkeypatch):
    """REQ-LOG-001: when PYTEST_CURRENT_TEST is set in the environment
    (pytest is running), `cli_output.init()` MUST NOT create a log
    file in the project's `logs/` directory. Otherwise test runs
    pollute the directory with stub data the user can't distinguish
    from real deploy logs.

    The fix: route to a tempdir under tempfile.gettempdir() when
    pytest is detected. Production behavior is unchanged.
    """
    import importlib
    from pathlib import Path
    co = importlib.import_module("scripts.common.cli_output")
    # Simulate pytest running (we ARE in pytest, but be explicit)
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "fake::test::node::id")
    # Do NOT pass log_dir — exercise the default-resolution path
    log_path = co.init(quiet=True, debug=False)
    # The path must be under the system temp dir, NOT under <project>/logs
    project_logs = (Path(__file__).resolve().parent.parent.parent / "logs").resolve()
    assert project_logs not in log_path.resolve().parents, (
        f"cli_output.init under pytest must NOT write to {project_logs}, "
        f"got {log_path!r}"
    )
    assert log_path.exists(), "log file should still be created (in tmp)"
    _cli_output_cleanup(co)


def test_TC_LOG_004_preflight_emits_summary_on_fail(monkeypatch, capsys, tmp_path):
    """REQ-LOG-002: preflight CLI must emit a final error summary BEFORE
    sys.exit(1) on fail. Without this, the log ends abruptly at
    'Summary: ... N fail' and the operator sees no indication that
    the command exited non-zero or what to do next.
    """
    import importlib
    pre = importlib.import_module("scripts.preflight")
    co = importlib.import_module("scripts.common.cli_output")

    # Reset cli_output state so prior tests don't leak a closed file
    # handle into main()'s `if _S.log_fh is None` short-circuit. Then
    # initialize fresh so any writes have somewhere valid to go.
    if co._S.log_fh is not None:
        try:
            co._S.log_fh.close()
        except Exception:
            pass
    co._S.log_fh = None
    co.init(quiet=True, debug=False, log_dir=tmp_path, name="preflight")

    # Stub run_preflight to return one failure
    def fake_run(*args, **kwargs):
        return (0, 0, 1)  # passed=0, warned=0, failed=1
    monkeypatch.setattr(pre, "run_preflight", fake_run)

    with pytest.raises(SystemExit) as exc:
        pre.main(argv=[])
    assert exc.value.code == 1, "preflight must exit 1 on fail"

    # Read the session log — that's where cli_output.error() writes
    # regardless of stdout filtering (REQ-E-300 contract).
    co._S.log_fh.flush()
    log_content = co._S.log_path.read_text()
    assert "fail" in log_content.lower(), (
        "preflight CLI must emit a final error summary to the session log on fail. "
        f"Log content: {log_content!r}"
    )
    # Cleanup
    _cli_output_cleanup(co)
