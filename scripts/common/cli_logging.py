"""CLI logging bootstrap.

Re-execs the current process under ``script(1)`` so all output — Python
prints, subprocess output (terraform, MCP build, etc.), and child threads —
is teed to ``logs/<name>-<timestamp>.log`` in the project root.

Used by ``scripts/deploy.py`` and ``scripts/destroy.py`` so a full transcript
of every interactive run is preserved on disk for triage. Pass ``--no-log``
to opt out.

The PTY tee captures raw
subprocess output (terraform plan output, AWS CLI errors) which can
include secrets that ``cli_output``'s redaction layer never sees. The
outer wrapper (the process that spawned script(1)) post-processes the
log AFTER script(1) exits, so the redaction doesn't race the parent's
final flushes. Earlier draft registered atexit inside the wrapped
inner process, which could drop trailing log content if script(1) was
still writing when atexit fired.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

_BOOTSTRAP_ENV = "STREAMING_AGENTS_LOGGING_ACTIVE"


# canonical project-root helper.
from .terraform import get_project_root as _get_project_root


def _resolve_root() -> Path:
    return _get_project_root(strict=False)


def _redact_log_file(log_path: Path) -> None:
    """Post-process a session log file: redact secrets line-by-line.

     / B2. Called from the OUTER wrapper after script(1)
    exits — never from inside the inner process via atexit.

    Best-effort: failure surfaces as a stderr warning so the operator
    knows the log was NOT scrubbed (M7).

    catches BaseException (not Exception) so a second
    Ctrl+C during redaction doesn't leave a partial `.redacting` file
    on disk with unredacted content from before the interrupt — that
    would defeat the entire B-1 hardening for the impatient-user case.
    """
    tmp_path = None
    try:
        if not log_path.exists() or log_path.stat().st_size == 0:
            return
        from .redaction import redact
        tmp_path = log_path.with_suffix(log_path.suffix + ".redacting")
        with open(log_path, "r", errors="replace") as src, \
             open(tmp_path, "w") as dst:
            for line in src:
                dst.write(redact(line))
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, log_path)
    except BaseException as e:
        # M7 + M-NEW-1: surface failures rather than silently leaving
        # secrets on disk. BaseException covers KeyboardInterrupt /
        # SystemExit too — a SIGINT mid-redaction now reliably unlinks
        # the partial tmp file.
        try:
            print(
                f"  [warn] log redaction failed: {e!r}. "
                f"Review {log_path} before sharing.",
                file=sys.stderr,
            )
            # Tidy up tmp.
            if tmp_path and tmp_path.exists():
                tmp_path.unlink()
        except BaseException:
            pass
        # If the original interrupt was KeyboardInterrupt, re-raise so
        # the outer process still exits — we just made sure no partial
        # file is left behind first.
        if isinstance(e, KeyboardInterrupt):
            raise


def bootstrap_logging(name: str) -> Path | None:
    """Tee CLI output to ``logs/<name>-<timestamp>.log``.

    Runs `script(1)` as a CHILD process (subprocess.run) and post-
    processes the resulting log AFTER script(1) exits. The previous
    `os.execvp` form replaced the current process with script(1) and
    relied on an atexit handler running in the inner Python — which
    raced script(1)'s final PTY flushes.

    Returns the log path, or None when wrapping is skipped (``--no-log``,
    non-tty stdin, or ``script(1)`` unavailable).

     / B2.
    """
    # If we're already the inner process under the wrapper, the env var
    # is set; just return the path so cli_output can write to it.
    existing = os.environ.get(_BOOTSTRAP_ENV)
    if existing:
        return Path(existing)

    if "--no-log" in sys.argv:
        sys.argv.remove("--no-log")
        return None

    if not sys.stdin.isatty():
        return None

    script_bin = shutil.which("script")
    if not script_bin:
        return None

    logs_dir = _resolve_root() / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # UTC timestamp for cross-process correlation with cli_output.
    log_path = logs_dir / f"{name}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.log"

    # Mark the inner invocation so the child returns immediately at the
    # `existing` branch above. Pass via env= rather than os.environ
    # mutation so sidecar subprocesses spawned from the outer Python
    # never see it. We still mutate os.environ briefly so
    # script(1) inherits it; popped in the finally below.
    os.environ[_BOOTSTRAP_ENV] = str(log_path)

    inner_cmd = [sys.executable, *sys.argv]
    if sys.platform == "darwin" or "bsd" in sys.platform:
        argv = [script_bin, "-q", str(log_path), *inner_cmd]
    else:
        import shlex
        joined = " ".join(shlex.quote(a) for a in inner_cmd)
        argv = [script_bin, "-q", "-c", joined, str(log_path)]

    print(f"  [log] Capturing CLI output to {log_path}")
    print("        (pass --no-log to disable)")
    sys.stdout.flush()

    # Run script(1) as a child and wait for it. Pass through the
    # current stdin/stdout/stderr so it's an interactive PTY.
    #
    # redact in a `finally:` block so SIGINT /
    # KeyboardInterrupt / any uncaught exception STILL scrubs the log
    # before the outer process exits. Without this, Ctrl+C — the most
    # common user-driven termination — leaves unredacted terraform
    # plan output and AWS CLI errors on disk.
    returncode = 1
    interrupted = False
    try:
        try:
            result = subprocess.run(argv)
            returncode = result.returncode
        except OSError as e:
            print(f"  [warn] Could not start logging wrapper: {e}")
            return None
        except KeyboardInterrupt:
            # POSIX convention: 128 + SIGINT(2) = 130.
            interrupted = True
            returncode = 130
    finally:
        _redact_log_file(log_path)
        # Pop the marker so a subsequent invocation in the same outer
        # shell (e.g. interactive Python) starts a fresh wrapper.
        os.environ.pop(_BOOTSTRAP_ENV, None)
    if interrupted:
        print("  [warn] Interrupted; log redacted before exit.", file=sys.stderr)
    sys.exit(returncode)
