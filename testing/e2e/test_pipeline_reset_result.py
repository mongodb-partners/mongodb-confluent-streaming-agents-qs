"""Tests that reset_pipeline reports failure instead of always returning True.

A reset that swallows a failed topic recreate or a stuck deletion would report
success while leaving stale partition data — the exact failure the reset exists
to prevent. These drive reset_pipeline with mocked REST helpers.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

pr = importlib.import_module("scripts.pipeline_reset")


@pytest.fixture
def mocked_reset(monkeypatch):
    """Stub every external helper so reset_pipeline runs offline. Returns a
    dict of knobs the test can flip to simulate failures."""
    knobs = {
        "delete_topic_ok": True,
        "create_topic_ok": True,
        "topic_gone_ok": True,
        "delete_stmt_ok": True,
    }
    monkeypatch.setattr(pr, "_get_terraform_outputs", lambda root: {"x": 1})
    monkeypatch.setattr(pr, "_get_flink_credentials", lambda o: {"f": 1})
    monkeypatch.setattr(pr, "_get_kafka_credentials", lambda o: {"k": 1})
    monkeypatch.setattr(pr, "_clear_mongodb_collections", lambda root: True)
    monkeypatch.setattr(pr, "_stop_flink_statement", lambda n, f: None)
    monkeypatch.setattr(
        pr, "_delete_flink_statement", lambda n, f: knobs["delete_stmt_ok"]
    )
    monkeypatch.setattr(
        pr, "_delete_kafka_topic", lambda t, k: knobs["delete_topic_ok"]
    )
    monkeypatch.setattr(
        pr,
        "_wait_for_kafka_topic_gone",
        lambda t, k, timeout=30: knobs["topic_gone_ok"],
    )
    monkeypatch.setattr(
        pr, "_create_kafka_topic", lambda t, k: knobs["create_topic_ok"]
    )
    monkeypatch.setattr(pr, "_delete_schema_subjects", lambda root: True)
    monkeypatch.setattr(pr, "_drop_flink_catalog_tables", lambda f: None)
    # datagen_helpers.reset_batch_counter is imported inside the function
    import scripts.common.datagen_helpers as dh

    monkeypatch.setattr(dh, "reset_batch_counter", lambda root: False)
    # Stub the ASP-restart helper so the test never does real network I/O
    # (it is imported lazily inside reset_pipeline and wrapped in try/except).
    import scripts.common.asp_restart as ar

    monkeypatch.setattr(
        ar,
        "restart_processors_for_topics",
        lambda **kwargs: None,
    )
    # Step 8 sleeps 5s for topic metadata propagation — collapse it so the
    # offline test is fast (all REST work is already mocked).
    monkeypatch.setattr(pr.time, "sleep", lambda *_a, **_k: None)
    return knobs


def test_reset_returns_true_when_all_ok(mocked_reset):
    assert pr.reset_pipeline(Path(".")) is True


def test_reset_returns_false_on_topic_recreate_failure(mocked_reset):
    mocked_reset["create_topic_ok"] = False
    assert pr.reset_pipeline(Path(".")) is False


def test_reset_returns_false_on_topic_delete_failure(mocked_reset):
    mocked_reset["delete_topic_ok"] = False
    assert pr.reset_pipeline(Path(".")) is False


def test_reset_returns_false_when_topic_not_gone(mocked_reset):
    mocked_reset["topic_gone_ok"] = False
    assert pr.reset_pipeline(Path(".")) is False


def test_reset_returns_false_on_statement_delete_failure(mocked_reset):
    mocked_reset["delete_stmt_ok"] = False
    assert pr.reset_pipeline(Path(".")) is False
