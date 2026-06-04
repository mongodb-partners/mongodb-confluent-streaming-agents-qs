"""Step-level structured pipeline logging.

The streaming pipeline has many components (Flink, Kafka, ASP, MongoDB, MCP)
and many phases (deploy, reset, datagen, dashboard click). When something
breaks, triage requires tracing through ad-hoc Python one-liners against
each component. This logger captures step-by-step events to a single JSONL
file per run so triage is one `tail` away.

Record schema:
    {"ts": "2026-05-15T01:55:23.123456+00:00",
     "phase": "deploy",
     "step": "ensure_flink_topics",
     "status": "started" | "ok" | "warn" | "fail",
     "duration_ms": 4231,
     "meta": {"topics_recreated": 4, ...}}

Designed to be additive — does NOT replace `bootstrap_logging` (CLI tee) or
`setup_logging` (stdlib logger). Pipeline events are a separate concern from
free-form log records.
"""
from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


# canonical project-root helper.
from .terraform import get_project_root as _get_project_root


def _resolve_root() -> Path:
    return _get_project_root(strict=False)


class PipelineLogger:
    def __init__(self, name: str = "pipeline", root: Path | None = None):
        self._name = name
        self._root = Path(root) if root else _resolve_root()
        logs_dir = self._root / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        self._path = logs_dir / f"{name}-{ts}.jsonl"
        self._fh = open(self._path, "a", encoding="utf-8")
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def _write(self, record: dict) -> None:
        record["ts"] = datetime.now(timezone.utc).isoformat()
        line = json.dumps(record, default=str)
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()

    def event(self, phase: str, step: str, status: str, **meta) -> None:
        """One-shot event with no duration."""
        self._write({
            "phase": phase,
            "step": step,
            "status": status,
            "meta": dict(meta),
        })

    @contextmanager
    def step(self, phase: str, step: str, **meta):
        """Context manager: emits `started` on entry, `ok` on clean exit,
        `fail` on exception (re-raises). Records duration_ms on exit."""
        self._write({
            "phase": phase,
            "step": step,
            "status": "started",
            "meta": dict(meta),
        })
        t0 = time.monotonic()
        try:
            yield self
        except BaseException as exc:
            self._write({
                "phase": phase,
                "step": step,
                "status": "fail",
                "duration_ms": int((time.monotonic() - t0) * 1000),
                "meta": {**meta, "error": f"{type(exc).__name__}: {exc}"},
            })
            raise
        else:
            self._write({
                "phase": phase,
                "step": step,
                "status": "ok",
                "duration_ms": int((time.monotonic() - t0) * 1000),
                "meta": dict(meta),
            })

    def close(self) -> None:
        try:
            self._fh.close()
        except OSError:
            pass

    def __enter__(self) -> "PipelineLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        """Close the log file. Returns False to let any in-flight
        exception propagate (PEP 343).

        explicit return False rather than relying on
        the implicit `None` (which is falsy and was effectively the
        same behavior, but unclear contract for future maintainers).
        """
        self.close()
        return False
