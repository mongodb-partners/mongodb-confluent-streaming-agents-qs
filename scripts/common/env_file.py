"""Atomic .env writer — shared between deploy.py and mcp_deploy.py.

The previous _save_mcp_credentials had two
problems compared to deploy.py's `_save_env_many`:

1. **Non-atomic read-modify-write.** Between `read_text` and `os.replace`,
   another writer (e.g. a background MCP build thread updating
   DEPLOY_PHASE) could clobber the file. Two parallel `uv run deploy`
   processes against the same checkout had the same hazard.
2. **Deterministic temp filename** (`.env.mcp-tmp`). Two parallel
   processes raced on the same temp path.

This module centralises the safe pattern:
- write to `tempfile.mkstemp(..., dir=target.parent)` (unique per call,
  mode 0o600 set atomically via `os.open(O_CREAT, mode)`)
- merge updates into the existing `.env` line-by-line so comments and
  unrelated keys are preserved
- `os.replace` to swap atomically; cleanup on failure
- best-effort `os.chmod 0o600` on the destination in case it pre-existed
  with broader perms (some POSIX implementations preserve the
  destination's mode through replace)
- refuses values with embedded `\n` / `\r` (would corrupt the file's
  line-based parser)

Both `deploy._save_env_many` and `mcp_deploy._save_mcp_credentials`
delegate here. Single canonical writer = no race between callers.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path


def _format_line(k: str, v: str) -> str:
    """Render a single `KEY=value` line, quoting when needed."""
    if any(ch in v for ch in (" ", "\t", "#", "$", '"', "'")):
        escaped = v.replace("\\", "\\\\").replace('"', '\\"')
        return f'{k}="{escaped}"'
    return f"{k}={v}"


def atomic_write_env(env_path: Path, pairs: dict) -> None:
    """Merge `pairs` into the .env file at `env_path` atomically.

    - None values in `pairs` are skipped (no write, no delete).
    - Existing keys are updated in-place; new keys appended at the end.
    - Comments and unrelated keys are preserved verbatim.
    - The file is created with mode 0o600 (owner only) atomically.
    - Refuses to write values containing newline / carriage-return.

    Raises ValueError on illegal value content. Raises OSError on
    filesystem failures (temp file is cleaned up before re-raise).
    """
    env_path = Path(env_path)
    env_path.parent.mkdir(parents=True, exist_ok=True)
    if not env_path.exists():
        env_path.touch(mode=0o600)
    try:
        os.chmod(env_path, 0o600)
    except OSError:
        pass

    for k, v in pairs.items():
        if v is None:
            continue
        sv = str(v)
        if "\n" in sv or "\r" in sv:
            raise ValueError(
                f"atomic_write_env: value for {k!r} contains newline/CR "
                f"characters; refusing to write to {env_path}. "
                "Strip whitespace or quote the value differently."
            )

    updates = {k: str(v) for k, v in pairs.items() if v is not None}
    seen: set = set()
    out_lines: list[str] = []
    try:
        raw = env_path.read_text()
    except OSError:
        raw = ""
    for line in raw.splitlines():
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            out_lines.append(line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in updates:
            out_lines.append(_format_line(key, updates[key]))
            seen.add(key)
        else:
            out_lines.append(line)

    for k, v in updates.items():
        if k not in seen:
            out_lines.append(_format_line(k, v))

    # use mkstemp for a UNIQUE temp path so parallel
    # writers don't race on a deterministic .env.tmp name.
    fd, tmp_path = tempfile.mkstemp(
        dir=str(env_path.parent),
        prefix=env_path.name + ".",
        suffix=".tmp",
    )
    try:
        # Mode applied atomically via the open flags; mkstemp already
        # creates with 0o600 on POSIX, but we re-chmod defensively for
        # platforms where it's wider.
        os.chmod(tmp_path, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write("\n".join(out_lines))
            if out_lines:
                f.write("\n")
        os.replace(tmp_path, str(env_path))
    except Exception:
        # Clean up the temp file on any failure (write error, replace
        # failure, etc.) so no secret-bearing 0o600 file is left behind.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    try:
        os.chmod(env_path, 0o600)
    except OSError:
        pass
