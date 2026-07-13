"""Tests for the deterministic Surge Director (scripts/surge.py).

The director publishes a concentrated surge batch to Kafka whose event
timestamps are aligned to the CURRENT Flink tumbling window, so a real
anomaly -> RAG -> dispatch fires within ~1 window during a live demo.

Traceability:
  TC-E-031 window alignment, TC-E-032 batch shape, TC-E-035 dry-run,
  TC-E-030 publish (live-gated), TC-E-033 narration, TC-E-034 cred diag,
  TC-REG-004 never touches Mongo, TC-REG-006 reuses publish_data helpers.
"""

from __future__ import annotations

import importlib
import json
import os
from pathlib import Path

import pytest

surge = importlib.import_module("scripts.surge")
gbd = importlib.import_module("scripts.generate_batch_data")


# Track the surge director's actual default (shortened 5→1 alongside the Flink
# window change) so these window-math tests stay aligned with production.
WINDOW_MIN = surge.DEFAULT_WINDOW_MIN
WINDOW_MS = WINDOW_MIN * 60 * 1000


def test_default_window_min_matches_flink_window():
    """Surge must align to the same window Flink uses (1 min). If these drift,
    a surge burst spreads across multiple Flink windows and dilutes below the
    anomaly threshold."""
    assert surge.DEFAULT_WINDOW_MIN == 1


# --- TC-E-031: window alignment (pure, property-style) --------------------


def test_current_window_bounds_contains_now():
    now_ms = 1_700_000_123_456
    start, end = surge.current_window_bounds(now_ms, WINDOW_MIN)
    assert start <= now_ms < end
    assert end - start == WINDOW_MS
    assert start % WINDOW_MS == 0


@pytest.mark.parametrize("now_ms", [0, 1, WINDOW_MS - 1, WINDOW_MS, 1_700_000_000_000])
def test_all_surge_timestamps_fall_in_current_window(now_ms):
    """Every generated record's request_ts SHALL be in [floor(now,W), floor+W)."""
    records = surge.build_surge_records(
        zone="French Quarter", multiplier=10, now_ms=now_ms, window_min=WINDOW_MIN
    )
    start, end = surge.current_window_bounds(now_ms, WINDOW_MIN)
    assert records, "expected a non-empty surge batch"
    for r in records:
        assert start <= r["request_ts"] < end


# --- TC-E-032: batch shape reuses generate_batch_data record schema -------


def test_records_match_ride_request_schema_and_zone_and_multiplier():
    baseline = surge.BASELINE_REQUESTS_PER_WINDOW
    multiplier = 10
    records = surge.build_surge_records(
        zone="Bywater",
        multiplier=multiplier,
        now_ms=1_700_000_000_000,
        window_min=WINDOW_MIN,
        baseline=baseline,
    )
    # count reflects surge intensity
    assert len(records) == baseline * multiplier
    required = {f["name"] for f in gbd.SCHEMA["fields"]}
    for r in records:
        assert required.issubset(r.keys())
        assert r["pickup_zone"] == "Bywater"
        # record must Avro-encode with the shared wire format (no exception)
        assert isinstance(gbd._encode_record(r), str)


# --- TC-E-035: dry-run generates/validates without publishing -------------


def test_dry_run_returns_summary_without_publishing(capsys):
    rc = surge.main(["--dry-run", "--zone", "French Quarter", "--multiplier", "10"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "French Quarter" in out
    assert "DRY RUN" in out.upper()


def test_dry_run_writes_valid_jsonl_lines(tmp_path):
    path = tmp_path / "surge.jsonl"
    n = surge.write_surge_jsonl(
        surge.build_surge_records("Uptown", 5, 1_700_000_000_000, WINDOW_MIN),
        path,
    )
    lines = [l for l in path.read_text().splitlines() if l.strip()]
    assert n == len(lines) > 0
    for line in lines:
        obj = json.loads(line)  # publish_data consumes this shape
        assert set(obj.keys()) >= {"key", "value", "partition", "offset"}


# --- TC-REG-004: director NEVER constructs a MongoDB client ---------------


def test_dry_run_never_touches_mongodb(monkeypatch):
    """INV-004: the surge director only publishes to Kafka, never writes Atlas."""
    import scripts.common.mongo as mongo_mod

    calls = {"n": 0}

    def _boom(*a, **k):
        calls["n"] += 1
        raise AssertionError("surge director must not construct a MongoClient")

    monkeypatch.setattr(mongo_mod, "get_client", _boom)
    monkeypatch.setattr(mongo_mod, "MongoClient", _boom)
    rc = surge.main(["--dry-run", "--zone", "Marigny", "--multiplier", "8"])
    assert rc == 0
    assert calls["n"] == 0


# --- TC-E-034 / TC-REG-006: publish path reuses publish_data + diagnostics -


def test_publish_reuses_publish_data_producer(monkeypatch, tmp_path):
    """TC-REG-006: the live path SHALL use publish_data.DataPublisher, not a
    forked producer, and SHALL publish the aligned batch to the topic."""
    from scripts import publish_data as pd

    captured = {}

    class FakePublisher:
        def __init__(self, **kwargs):
            captured["init"] = kwargs

        def publish_jsonl_file(self, path, topic):
            captured["topic"] = topic
            lines = [l for l in Path(path).read_text().splitlines() if l.strip()]
            captured["lines"] = len(lines)
            return {"success": len(lines), "failed": 0, "total": len(lines)}

        def close(self):
            captured["closed"] = True

    monkeypatch.setattr(
        pd, "setup_logging", lambda v: __import__("logging").getLogger("t")
    )
    monkeypatch.setattr(pd, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(pd, "validate_terraform_state", lambda root: None)
    monkeypatch.setattr(
        pd,
        "extract_kafka_credentials",
        lambda root: {
            "bootstrap_servers": "b",
            "kafka_api_key": "k",
            "kafka_api_secret": "s",
            "cluster_id": "c",
        },
    )
    monkeypatch.setattr(pd, "DataPublisher", FakePublisher)

    rc = surge.main(["--zone", "Bywater", "--multiplier", "5", "--window-min", "5"])
    assert rc == 0
    assert captured["topic"] == "ride_requests"
    assert captured["lines"] == surge.BASELINE_REQUESTS_PER_WINDOW * 5
    assert captured.get("closed") is True


def test_publish_partial_failure_exits_nonzero(monkeypatch, tmp_path):
    """REQ-E-033: a partial publish (some records failed) is NOT a clean run and
    SHALL exit non-zero so a half-published surge doesn't look successful."""
    from scripts import publish_data as pd

    class PartialPublisher:
        def __init__(self, **kwargs):
            pass

        def publish_jsonl_file(self, path, topic):
            return {"success": 40, "failed": 20, "total": 60}

        def close(self):
            pass

    monkeypatch.setattr(
        pd, "setup_logging", lambda v: __import__("logging").getLogger("t")
    )
    monkeypatch.setattr(pd, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(pd, "validate_terraform_state", lambda root: None)
    monkeypatch.setattr(
        pd,
        "extract_kafka_credentials",
        lambda root: {
            "bootstrap_servers": "b",
            "kafka_api_key": "k",
            "kafka_api_secret": "s",
            "cluster_id": "c",
        },
    )
    monkeypatch.setattr(pd, "DataPublisher", PartialPublisher)

    rc = surge.main(["--zone", "Bywater", "--multiplier", "5"])
    assert rc == 1


def test_surge_rewrites_to_current_schema_id(monkeypatch, tmp_path):
    """The surge director MUST look up the current registered schema ID and
    pass it to DataPublisher so the stale hardcoded SCHEMA_ID (100008) in the
    wire header is rewritten. Without this, Flink can't deserialize the surge
    records and zone-traffic-sink-insert / anomaly-detection-insert FAIL."""
    from scripts import publish_data as pd

    captured = {}

    class FakePublisher:
        def __init__(self, **kwargs):
            captured["target_schema_id"] = kwargs.get("target_schema_id")

        def publish_jsonl_file(self, path, topic):
            return {"success": 10, "failed": 0, "total": 10}

        def close(self):
            pass

    monkeypatch.setattr(
        pd, "setup_logging", lambda v: __import__("logging").getLogger("t")
    )
    monkeypatch.setattr(pd, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(pd, "validate_terraform_state", lambda root: None)
    monkeypatch.setattr(
        pd,
        "extract_kafka_credentials",
        lambda root: {
            "bootstrap_servers": "b",
            "kafka_api_key": "k",
            "kafka_api_secret": "s",
            "cluster_id": "c",
            "schema_registry_url": "http://sr",
            "schema_registry_api_key": "sk",
            "schema_registry_api_secret": "ss",
        },
    )
    # Stub the SR lookup to return a "current" id different from the hardcoded one.
    monkeypatch.setattr(pd, "_get_current_schema_id", lambda *a, **k: 100015)
    monkeypatch.setattr(pd, "DataPublisher", FakePublisher)

    rc = surge.main(["--zone", "Bywater", "--multiplier", "5"])
    assert rc == 0
    assert captured["target_schema_id"] == 100015, (
        "surge must pass the looked-up current schema ID to DataPublisher so "
        "the stale wire-header ID gets rewritten"
    )


def test_publish_fails_clearly_on_missing_credentials(monkeypatch, tmp_path):
    """REQ-E-034: missing creds/terraform state SHALL exit non-zero, not hang."""
    from scripts import publish_data as pd

    monkeypatch.setattr(
        pd, "setup_logging", lambda v: __import__("logging").getLogger("t")
    )
    monkeypatch.setattr(pd, "get_project_root", lambda: tmp_path)

    def _fail(root):
        raise RuntimeError("terraform state not found")

    monkeypatch.setattr(pd, "validate_terraform_state", _fail)
    rc = surge.main(["--zone", "Uptown", "--multiplier", "5"])
    assert rc == 1


@pytest.mark.skipif(
    not os.environ.get("RUN_LIVE_KAFKA"),
    reason="live Kafka publish is env-gated (set RUN_LIVE_KAFKA=1)",
)
def test_live_publish_produces_to_topic():  # pragma: no cover - live only
    """TC-E-030 (crosses B4): against a real deployed cluster, publish succeeds."""
    rc = surge.main(["--zone", "French Quarter", "--multiplier", "10"])
    assert rc == 0
