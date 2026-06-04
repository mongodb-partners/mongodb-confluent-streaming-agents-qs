"""Typed CLI output for deploy / destroy / preflight / summary.

Public API:
    info(msg)         -> [INFO] msg
    warn(msg)         -> [WARN] msg
    error(msg)        -> [ERROR] msg
    success(msg)      -> [OK] msg
    debug(msg)        -> [DEBUG] msg (suppressed unless --debug)
    step(i, n, msg)   -> [STEP i/n] msg
    kv(key, value)    -> aligned key/value pair, terminal-width aware
    section(title)    -> blank line + colored title + underline
    subsection(title) -> single colored line, no underline
    init(quiet, debug, log_dir) -> set up session log file + flags
    capture()         -> context manager returning (stdout_lines, log_lines)

Log file (logs/deploy-<UTC>.log) receives every call unconditionally
(plain text, no ANSI). Stdout receives lines subject to quiet/debug
filters. `rich>=13.0.0` is a required dependency (pyproject.toml:49).
"""

from __future__ import annotations

import os
import re
import shutil
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Final

from .redaction import redact as _redact

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

_PREFIX: Final[dict[str, str]] = {
    "info":    "[info]",
    "warn":    "[warn]",
    "error":   "[FAIL]",
    "success": "[ok]  ",
    "debug":   "[dbg] ",
}

# ANSI color codes (used directly when stdout is a TTY). Kept minimal so
# the no-rich code path here is also small.
_COLOR: Final[dict[str, str]] = {
    "info":    "\x1b[0m",       # default
    "warn":    "\x1b[33m",      # yellow
    "error":   "\x1b[31m",      # red
    "success": "\x1b[32m",      # green
    "debug":   "\x1b[2m",       # dim
    "section": "\x1b[1;36m",    # bold cyan
    "subsection": "\x1b[36m",   # cyan
    "step":    "\x1b[1m",       # bold
    "reset":   "\x1b[0m",
}


class _State:
    quiet: bool = False
    debug_mode: bool = False
    log_fh = None
    log_path: Path | None = None
    # capture stack — list of (out_lines, log_lines) tuples; topmost is innermost.
    capture_stack: list = []
    # serialize log writes + capture-stack mutation
    # so concurrent threads (deploy + datagen daemon, etc.) don't
    # interleave bytes within a line.
    log_lock: threading.Lock = threading.Lock()


_S = _State()


# ---------------------------------------------------------------------------
# init / log file management
# ---------------------------------------------------------------------------

# canonical helper.
from .terraform import get_project_root as _get_project_root


def _resolve_root() -> Path:
    return _get_project_root(strict=False)


def _prune_old_logs(log_dir: Path, days: int = 7) -> None:
    """Delete log files older than `days` to bound `logs/` disk usage.

    previously only globbed `deploy-*.log`. JSONL
    pipeline-logger output, dashboard logs, and datagen logs accumulated
    forever on long-running workshop attendee laptops. Now prunes every
    `.log` and `.jsonl` file in the directory (mtime-based).
    """
    cutoff = time.time() - (days * 86400)
    suffixes = {".log", ".jsonl"}
    try:
        for entry in log_dir.iterdir():
            try:
                if not entry.is_file():
                    continue
                if entry.suffix not in suffixes:
                    continue
                if entry.stat().st_mtime < cutoff:
                    entry.unlink()
            except OSError:
                pass
    except OSError:
        pass


def init(
    quiet: bool = False,
    debug: bool = False,
    log_dir: Path | None = None,
    name: str = "deploy",
) -> Path:
    """Initialize cli_output and open the session log file.

    Args:
        quiet: Suppress `[info]` output to stdout (log file always gets it).
        debug: Enable `[dbg]` output to stdout.
        log_dir: Override the log directory. Default: `<project>/logs`,
            or a tempdir when pytest is running ( — see below).
        name: Script name used as the log filename prefix.
            Default `"deploy"` preserves backwards compatibility for
            existing callers. Other entry points (`preflight`, `health`,
            etc.) should pass their own name so the filename reflects
            which command produced the log — otherwise grep-by-filename
            diagnostics surface unrelated files.

    when pytest is running (`PYTEST_CURRENT_TEST` is set
    in os.environ), `cli_output.init()` routes to `tempfile.gettempdir()`
    instead of the project's `logs/` directory. Without this,
    test code that exercises `pre.main(...)` or any other entry-point
    leaks real log files into the operator-facing directory — the
    operator then can't distinguish their own deploy logs from
    pytest artifacts. The override is bypassed when `log_dir` is
    explicitly provided (tests can still target a specific
    directory via the existing `log_dir=tmp_path` pattern).
    """
    _S.quiet = quiet
    _S.debug_mode = debug
    _S.capture_stack = []
    if log_dir is None:
        # when pytest is the caller, route to tempdir so
        # production logs/ stays clean. Tests that NEED a deterministic
        # path should pass `log_dir=tmp_path` explicitly.
        if os.environ.get("PYTEST_CURRENT_TEST"):
            import tempfile
            log_dir = Path(tempfile.gettempdir()) / "streaming-agents-test-logs"
        else:
            log_dir = _resolve_root() / "logs"
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True, parents=True)
    _prune_old_logs(log_dir, days=7)
    log_path = log_dir / f"{name}-{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}.log"
    if _S.log_fh is not None:
        try:
            _S.log_fh.close()
        except OSError:
            pass
    _S.log_fh = open(log_path, "w", buffering=1)  # line-buffered
    _S.log_path = log_path
    return log_path


# ---------------------------------------------------------------------------
# capture (test seam)
# ---------------------------------------------------------------------------

@contextmanager
def capture():
    """Test seam. Returns (stdout_lines, log_lines).

    Both lists are populated as cli_output writers run inside the with-block.
    Capture stacks: a nested capture() also receives lines from outer captures
    (lines bubble upward through the stack).
    """
    out_lines: list[str] = []
    log_lines: list[str] = []
    _S.capture_stack.append((out_lines, log_lines))
    try:
        yield out_lines, log_lines
    finally:
        _S.capture_stack.pop()


# ---------------------------------------------------------------------------
# internal writers
# ---------------------------------------------------------------------------

def _stdout_is_tty() -> bool:
    try:
        return sys.stdout.isatty()
    except Exception:
        return False


def _emit(stdout_line: str | None, log_line: str) -> None:
    """Send `log_line` to log + every active capture log buffer.
    Send `stdout_line` (if non-None) to stdout + every active capture out buffer.

    apply secret redaction before any output.
    serialize concurrent calls under _S.log_lock.
    """
    # redact secrets first (idempotent, both sides see masked).
    log_line = _redact(log_line)
    if stdout_line is not None:
        stdout_line = _redact(stdout_line)
    # Log file always receives the plain (ANSI-stripped) line.
    plain_log = _ANSI_RE.sub("", log_line)
    with _S.log_lock:
        if _S.log_fh is not None:
            try:
                _S.log_fh.write(plain_log + "\n")
            except (OSError, ValueError):
                # ValueError is raised when writing to a closed file
                # handle. Production code never closes log_fh during a
                # session, but test infrastructure may; defending here
                # prevents test ordering from cascading into spurious
                # failures on unrelated tests downstream.
                pass
        # Push to every capture frame (so nested captures bubble up).
        for out_buf, log_buf in _S.capture_stack:
            log_buf.append(plain_log)
            if stdout_line is not None:
                out_buf.append(stdout_line)
    # Stdout (outside the lock — print() takes its own lock and we don't
    # want to hold _S.log_lock across a slow terminal flush).
    if stdout_line is not None:
        if not _stdout_is_tty():
            stdout_line = _ANSI_RE.sub("", stdout_line)
        print(stdout_line)


def _color_wrap(level: str, text: str) -> str:
    if not _stdout_is_tty():
        return text
    color = _COLOR.get(level, "")
    return f"{color}{text}{_COLOR['reset']}" if color else text


# ---------------------------------------------------------------------------
# public writers
# ---------------------------------------------------------------------------

def info(msg: str) -> None:
    line = f"{_PREFIX['info']} {msg}"
    out = None if _S.quiet else _color_wrap("info", line)
    _emit(out, line)


def warn(msg: str) -> None:
    line = f"{_PREFIX['warn']} {msg}"
    _emit(_color_wrap("warn", line), line)


def error(msg: str) -> None:
    line = f"{_PREFIX['error']} {msg}"
    _emit(_color_wrap("error", line), line)


def success(msg: str) -> None:
    line = f"{_PREFIX['success']} {msg}"
    _emit(_color_wrap("success", line), line)


def debug(msg: str) -> None:
    line = f"{_PREFIX['debug']} {msg}"
    out = _color_wrap("debug", line) if _S.debug_mode else None
    _emit(out, line)


def step(i: int, n: int, msg: str) -> None:
    prefix = f"[STEP {i}/{n}]"
    line = f"{prefix} {msg}"
    _emit(_color_wrap("step", line), line)


def kv(key: str, value: str) -> None:
    """Render aligned key/value. Truncate long values at terminal width.

    Format: "  <key>  : <value>"
    """
    try:
        cols = shutil.get_terminal_size(fallback=(80, 24)).columns
    except Exception:
        cols = 80
    indent = "  "
    sep = " : "
    overhead = len(indent) + len(key) + len(sep)
    max_value_width = max(10, cols - overhead - 1)
    rendered_value = str(value)
    if len(rendered_value) > max_value_width:
        rendered_value = rendered_value[: max_value_width - 1] + "…"
    plain = f"{indent}{key}{sep}{rendered_value}"
    # Light styling: dim the key so values stand out
    if _stdout_is_tty():
        styled = f"{indent}\x1b[2m{key}\x1b[0m{sep}{rendered_value}"
    else:
        styled = plain
    _emit(styled, plain)


def section(title: str) -> None:
    """Blank line + colored title + underline."""
    _emit("", "")
    underline = "─" * max(3, len(title))
    title_line = _color_wrap("section", title)
    underline_line = _color_wrap("section", underline)
    _emit(title_line, title)
    _emit(underline_line, underline)


def subsection(title: str) -> None:
    """Single colored line, no underline."""
    plain = title
    styled = _color_wrap("subsection", title)
    _emit(styled, plain)
