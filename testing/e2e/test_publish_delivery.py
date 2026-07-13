"""Tests for publish_data broker-delivery accounting.

produce() only ENQUEUES a message; the broker reports delivery (or failure —
auth, unknown topic, quota) asynchronously via an on_delivery callback. Before
the fix, success was counted at enqueue time and delivery failures were
invisible, so an auth/topic error could still exit 0. These tests drive a fake
confluent-kafka Producer that fires delivery reports.
"""

from __future__ import annotations

import base64
import importlib
import json

pd = importlib.import_module("scripts.publish_data")


class FakeProducer:
    """Minimal confluent-kafka Producer stand-in.

    `outcomes` is a list of per-message errors (None = delivered OK). Each
    produce() stashes the on_delivery callback; flush() fires them all with the
    scripted outcome, mimicking async broker delivery reports.
    """

    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self._pending = []  # (callback,) queued but not yet reported

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self._pending.append(on_delivery)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        for i, cb in enumerate(self._pending):
            err = self._outcomes[i] if i < len(self._outcomes) else None
            if cb:
                cb(err, object())
        self._pending = []
        return 0  # nothing left in queue


def _write_jsonl(tmp_path, n):
    path = tmp_path / "msgs.jsonl"
    val = base64.b64encode(b"x").decode()
    with open(path, "w", encoding="utf-8") as f:
        for i in range(n):
            f.write(
                json.dumps({"key": None, "value": val, "partition": 0, "offset": i})
                + "\n"
            )
    return path


def _publisher_with(outcomes):
    p = pd.DataPublisher.__new__(pd.DataPublisher)
    # Minimal manual init to avoid constructing a real Producer.
    p.dry_run = False
    p.target_schema_id = None
    p.time_offset_ms = 0
    p._parsed_schema = None
    import logging

    p.logger = logging.getLogger("test-publish")
    p._delivery_failures = 0
    p._delivered = 0
    p.producer = FakeProducer(outcomes)
    return p


def test_all_delivered_counts_all_success(tmp_path):
    path = _write_jsonl(tmp_path, 5)
    p = _publisher_with([None] * 5)
    results = p.publish_jsonl_file(path, "ride_requests")
    assert results == {"success": 5, "failed": 0, "total": 5}


def test_broker_delivery_failures_move_to_failed(tmp_path):
    """REQ: async delivery errors must be reflected in failed count, not hidden."""
    path = _write_jsonl(tmp_path, 5)
    # 2 of 5 fail delivery at the broker (e.g. auth / unknown topic).
    p = _publisher_with([None, "AUTH", None, "UNKNOWN_TOPIC", None])
    results = p.publish_jsonl_file(path, "ride_requests")
    assert results["failed"] == 2
    assert results["success"] == 3
    assert results["total"] == 5


def test_total_delivery_failure_yields_zero_success(tmp_path):
    """A full auth failure must NOT report a clean run (exit-code driver)."""
    path = _write_jsonl(tmp_path, 3)
    p = _publisher_with(["AUTH", "AUTH", "AUTH"])
    results = p.publish_jsonl_file(path, "ride_requests")
    assert results["success"] == 0
    assert results["failed"] == 3
