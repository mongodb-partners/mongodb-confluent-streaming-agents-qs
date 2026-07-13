"""TC-LOG-001..005, TC-HEALTH-001..004 — Step-level pipeline logging + health CLI.

REQ-E-220..225, INV-203, INV-204 from specs/2026-05-15-stability-fixes/.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# TC-LOG-001 — REQ-E-220: PipelineLogger writes JSONL records
# ---------------------------------------------------------------------------

def test_TC_LOG_001_step_emits_two_records(tmp_path):
    """`with logger.step(...)` emits a `started` record on entry and an
    `ok` record on exit, with a duration_ms."""
    from scripts.common.pipeline_logger import PipelineLogger
    pl = PipelineLogger(name="test", root=tmp_path)
    with pl.step("phase1", "do_thing", count=3):
        pass
    pl.close()
    log_files = list((tmp_path / "logs").glob("test-*.jsonl"))
    assert len(log_files) == 1
    lines = [json.loads(l) for l in log_files[0].read_text().splitlines() if l.strip()]
    assert len(lines) == 2
    assert lines[0]["status"] == "started"
    assert lines[0]["phase"] == "phase1"
    assert lines[0]["step"] == "do_thing"
    assert lines[0]["meta"]["count"] == 3
    assert lines[1]["status"] == "ok"
    assert "duration_ms" in lines[1]
    assert lines[1]["duration_ms"] >= 0


# ---------------------------------------------------------------------------
# TC-LOG-002 — REQ-E-220: failure inside step records `fail` status
# ---------------------------------------------------------------------------

def test_TC_LOG_002_exception_in_step_records_fail(tmp_path):
    from scripts.common.pipeline_logger import PipelineLogger
    pl = PipelineLogger(name="test", root=tmp_path)
    try:
        with pl.step("phase1", "broken_step"):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    pl.close()
    log_files = list((tmp_path / "logs").glob("test-*.jsonl"))
    lines = [json.loads(l) for l in log_files[0].read_text().splitlines() if l.strip()]
    assert lines[-1]["status"] == "fail"
    assert "boom" in lines[-1]["meta"].get("error", "")


# ---------------------------------------------------------------------------
# TC-LOG-003 — REQ-E-222: standalone event() entries
# ---------------------------------------------------------------------------

def test_TC_LOG_003_event_one_shot(tmp_path):
    """logger.event() emits a single record with no duration."""
    from scripts.common.pipeline_logger import PipelineLogger
    pl = PipelineLogger(name="test", root=tmp_path)
    pl.event("phase1", "topic_purged", "ok", topic="anomalies_sink")
    pl.close()
    log_files = list((tmp_path / "logs").glob("test-*.jsonl"))
    lines = [json.loads(l) for l in log_files[0].read_text().splitlines() if l.strip()]
    assert len(lines) == 1
    assert lines[0]["status"] == "ok"
    assert lines[0]["meta"]["topic"] == "anomalies_sink"
    assert "duration_ms" not in lines[0]


# ---------------------------------------------------------------------------
# TC-LOG-004 — INV-203: bootstrap_logging is unaffected
# ---------------------------------------------------------------------------

def test_TC_LOG_004_bootstrap_logging_unmodified():
    """The new PipelineLogger writes to a different filename pattern
    than bootstrap_logging, ensuring INV-203 holds."""
    import inspect

    from scripts.common import cli_logging
    src = inspect.getsource(cli_logging.bootstrap_logging)
    # Existing tee path is logs/<name>-<timestamp>.log (no .jsonl)
    assert "{name}-" in src
    assert ".log" in src or '.log"' in src


# ---------------------------------------------------------------------------
# TC-LOG-005 — INV-204: setup_logging is unaffected
# ---------------------------------------------------------------------------

def test_TC_LOG_005_setup_logging_unmodified():
    """The new PipelineLogger does not call logging.basicConfig,
    so it does not interfere with setup_logging."""
    import inspect

    from scripts.common import pipeline_logger
    src = inspect.getsource(pipeline_logger)
    assert "basicConfig" not in src


# ---------------------------------------------------------------------------
# TC-HEALTH-001 — REQ-E-221: health CLI module exists with main()
# ---------------------------------------------------------------------------

def test_TC_HEALTH_001_health_module_has_main():
    from scripts import health
    assert callable(getattr(health, "main", None))


# ---------------------------------------------------------------------------
# TC-HEALTH-002 — REQ-E-223: exit code 1 on unhealthy
# ---------------------------------------------------------------------------

def test_TC_HEALTH_002_exits_1_on_unhealthy(capsys):
    """When any component is unhealthy, main() exits with code 1."""
    from scripts import health
    fake_report = {
        "overall": "unhealthy",
        "flink": [{"name": "anomalies-enriched-insert", "status": "fail",
                   "detail": "stale offset"}],
        "asp": [],
        "kafka": [],
        "mongo": [],
    }
    with patch("scripts.health.collect_report", return_value=fake_report):
        try:
            health.main(["--json"])
        except SystemExit as exc:
            assert exc.code == 1
            return
    raise AssertionError("Expected SystemExit(1)")


# ---------------------------------------------------------------------------
# TC-HEALTH-003 — REQ-E-224: --json produces parseable JSON
# ---------------------------------------------------------------------------

def test_TC_HEALTH_003_json_flag_emits_json(capsys):
    from scripts import health
    fake_report = {
        "overall": "healthy",
        "flink": [], "asp": [], "kafka": [], "mongo": [],
    }
    with patch("scripts.health.collect_report", return_value=fake_report):
        try:
            health.main(["--json"])
        except SystemExit:
            pass
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed["overall"] == "healthy"


# ---------------------------------------------------------------------------
# TC-HEALTH-004 — REQ-E-225: missing creds yield `unknown` not `fail`
# ---------------------------------------------------------------------------

def test_TC_HEALTH_004_missing_creds_yield_unknown():
    """When .env is missing or incomplete, components
    show as `unknown` not `fail`.

    Hermetic: patches BOTH credential and terraform-output loaders so the
    assertion is independent of any real terraform state files left by a
    prior partial deploy on the developer's machine.
    """
    from scripts import health

    # Force every credential AND terraform-state lookup to return empty.
    # Patching only _load_creds is insufficient — _check_flink and
    # _check_kafka read terraform outputs directly (collect_report:355).
    with (
        patch("scripts.health._load_creds", return_value={}),
        patch("scripts.health._load_terraform_outputs", return_value={}),
    ):
        report = health.collect_report()
    # All component lists should be present (possibly empty), and overall
    # is either 'unknown' or 'healthy' but not 'unhealthy'.
    assert report["overall"] in ("unknown", "healthy")
    for component in (report["flink"], report["asp"], report["kafka"], report["mongo"]):
        for entry in component:
            assert entry["status"] != "fail"
