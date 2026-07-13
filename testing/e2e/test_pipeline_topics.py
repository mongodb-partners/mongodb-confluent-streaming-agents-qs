"""Tests for the canonical topic/collection registry (pipeline_topics.py).

Pins the group memberships that deploy/destroy/pipeline_reset/health derive
their views from, so a future edit that breaks one caller's expectation fails
loudly here rather than silently drifting.
"""

from __future__ import annotations

from scripts.common import pipeline_topics as pt


def test_reset_topics_exclude_ctas_and_event():
    reset = set(pt.RESET_TOPICS)
    assert "ride_requests" in reset
    # CTAS-owned topics must NOT be in the reset set (their CTAS owns them).
    assert "anomalies_enriched" not in reset
    assert "completed_actions" not in reset
    assert "event_documents" not in reset


def test_all_pipeline_topics_is_superset_of_every_group():
    everything = set(pt.ALL_PIPELINE_TOPICS)
    for group in (
        pt.INPUT_TOPICS,
        pt.STREAMING_TOPICS,
        pt.CTAS_TOPICS,
        pt.EVENT_TOPICS,
    ):
        assert set(group) <= everything
    # No duplicates across groups.
    combined = (
        list(pt.INPUT_TOPICS)
        + list(pt.STREAMING_TOPICS)
        + list(pt.CTAS_TOPICS)
        + list(pt.EVENT_TOPICS)
    )
    assert len(combined) == len(set(combined))


def test_health_topics_include_ctas_but_not_event():
    health = set(pt.HEALTH_TOPICS)
    assert "anomalies_enriched" in health
    assert "completed_actions" in health
    assert "event_documents" not in health


def test_sink_collections_subset_of_all_collections():
    assert set(pt.MONGODB_SINK_COLLECTIONS) <= set(pt.ALL_MONGODB_COLLECTIONS)
    assert ("analytics", "zone_traffic") in pt.MONGODB_SINK_COLLECTIONS
    assert ("events", "knowledge_base") in pt.ALL_MONGODB_COLLECTIONS


def test_callers_resolve_to_canonical_values():
    """The three refactored callers must expose the canonical values."""
    from scripts import pipeline_reset

    assert set(pipeline_reset.PIPELINE_TOPICS) == set(pt.RESET_TOPICS)
    assert {tuple(c) for c in pipeline_reset.MONGODB_SINK_COLLECTIONS} == set(
        pt.MONGODB_SINK_COLLECTIONS
    )
