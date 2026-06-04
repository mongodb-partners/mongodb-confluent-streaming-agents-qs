"""TC-ASP-001..009 — ASP processor restart on Kafka topic recreation.

REQ-E-210..215, INV-202, INV-205 from specs/2026-05-15-stability-fixes/.
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# TC-ASP-001 — REQ-E-213: KAFKA_SOURCE_PROCESSORS topology mapping
# ---------------------------------------------------------------------------

def test_TC_ASP_001_topology_module_exists():
    """The topology module exists and exports KAFKA_SOURCE_PROCESSORS."""
    from scripts.common.asp_topology import KAFKA_SOURCE_PROCESSORS
    assert isinstance(KAFKA_SOURCE_PROCESSORS, dict)
    assert KAFKA_SOURCE_PROCESSORS, "topology must not be empty"


def test_TC_ASP_002_topology_includes_pipeline_topics():
    """All known Kafka-source ASP processors are in the map."""
    from scripts.common.asp_topology import KAFKA_SOURCE_PROCESSORS
    # Must map zone_traffic_sink → zone_traffic_ingestion
    assert "zone_traffic_sink" in KAFKA_SOURCE_PROCESSORS
    assert "zone_traffic_ingestion" in KAFKA_SOURCE_PROCESSORS["zone_traffic_sink"]
    # Must map anomalies_sink → anomalies_ingestion
    assert "anomalies_sink" in KAFKA_SOURCE_PROCESSORS
    assert "anomalies_ingestion" in KAFKA_SOURCE_PROCESSORS["anomalies_sink"]
    # Must map completed_actions → dispatch_log_ingestion
    assert "completed_actions" in KAFKA_SOURCE_PROCESSORS
    assert "dispatch_log_ingestion" in KAFKA_SOURCE_PROCESSORS["completed_actions"]


# ---------------------------------------------------------------------------
# TC-ASP-003 — REQ-E-212: only restart processors for given topics
# ---------------------------------------------------------------------------

def test_TC_ASP_003_only_restarts_consumers_of_given_topics():
    """When called with one topic, only that topic's consumers are restarted —
    not unrelated processors like event_knowledge_base_population."""
    from scripts.common.asp_restart import restart_processors_for_topics
    posts = []

    def fake_post(url, **kwargs):
        posts.append(url)
        m = MagicMock()
        m.status_code = 200
        return m

    def fake_get(url, **kwargs):
        m = MagicMock()
        m.json.return_value = {"results": [
            {"name": "zone_traffic_ingestion", "state": "STOPPED"},
            {"name": "anomalies_ingestion", "state": "STARTED"},
            {"name": "event_knowledge_base_population", "state": "STARTED"},
        ]}
        m.status_code = 200
        return m

    with patch("scripts.common.asp_restart.requests.post", side_effect=fake_post), \
         patch("scripts.common.asp_restart.requests.get", side_effect=fake_get):
        restart_processors_for_topics(
            project_id="proj",
            instance="asp-instance",
            topics=["zone_traffic_sink"],
            auth=MagicMock(),
            poll_interval_s=0,
            timeout_per_processor=1,
        )
    # Only zone_traffic_ingestion should appear in restart calls
    joined = "\n".join(posts)
    assert "zone_traffic_ingestion" in joined
    assert "event_knowledge_base_population" not in joined


# ---------------------------------------------------------------------------
# TC-ASP-004 — REQ-E-212: explicit non-Kafka exclusions
# ---------------------------------------------------------------------------

def test_TC_ASP_004_change_stream_processor_excluded():
    """event_knowledge_base_population reads from a MongoDB change stream,
    NOT Kafka. It must never appear in any topology mapping value."""
    from scripts.common.asp_topology import KAFKA_SOURCE_PROCESSORS
    for topic, procs in KAFKA_SOURCE_PROCESSORS.items():
        assert "event_knowledge_base_population" not in procs, (
            f"{topic} → {procs}: change-stream processor must not be a Kafka consumer"
        )


# ---------------------------------------------------------------------------
# TC-ASP-005 — REQ-E-213: function signature matches spec
# ---------------------------------------------------------------------------

def test_TC_ASP_005_function_returns_state_dict():
    """restart_processors_for_topics returns {processor_name: final_state}."""
    from scripts.common.asp_restart import restart_processors_for_topics
    with patch("scripts.common.asp_restart.requests.post") as mock_post, \
         patch("scripts.common.asp_restart.requests.get") as mock_get:
        mock_post.return_value = MagicMock(status_code=200)
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"results": [
                {"name": "zone_traffic_ingestion", "state": "STARTED"}
            ]},
        )
        result = restart_processors_for_topics(
            project_id="p", instance="i",
            topics=["zone_traffic_sink"],
            auth=MagicMock(), poll_interval_s=0, timeout_per_processor=1,
        )
    assert isinstance(result, dict)
    assert "zone_traffic_ingestion" in result


# ---------------------------------------------------------------------------
# TC-ASP-006 — REQ-E-214: timeout / network error doesn't raise
# ---------------------------------------------------------------------------

def test_TC_ASP_006_network_error_does_not_raise():
    """When Atlas API is unreachable, the function logs and returns,
    not aborting the broader deploy/reset flow."""
    import requests
    from scripts.common.asp_restart import restart_processors_for_topics
    with patch("scripts.common.asp_restart.requests.post",
               side_effect=requests.exceptions.ConnectionError("boom")), \
         patch("scripts.common.asp_restart.requests.get",
               side_effect=requests.exceptions.ConnectionError("boom")):
        # Must not raise
        result = restart_processors_for_topics(
            project_id="p", instance="i",
            topics=["zone_traffic_sink"],
            auth=MagicMock(), poll_interval_s=0, timeout_per_processor=1,
        )
    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# TC-ASP-007 — REQ-E-215: FAILED → start still attempted
# ---------------------------------------------------------------------------

def test_TC_ASP_007_failed_processor_still_started():
    """A processor in FAILED state should still have :start issued
    (Atlas treats start-from-FAILED as a re-launch)."""
    from scripts.common.asp_restart import restart_processors_for_topics
    posts = []

    def fake_post(url, **kwargs):
        posts.append(url)
        return MagicMock(status_code=200)

    def fake_get(url, **kwargs):
        return MagicMock(
            status_code=200,
            json=lambda: {"results": [
                {"name": "dispatch_log_ingestion", "state": "FAILED"}
            ]},
        )

    with patch("scripts.common.asp_restart.requests.post", side_effect=fake_post), \
         patch("scripts.common.asp_restart.requests.get", side_effect=fake_get):
        restart_processors_for_topics(
            project_id="p", instance="i",
            topics=["completed_actions"],
            auth=MagicMock(), poll_interval_s=0, timeout_per_processor=1,
        )
    # At least one :start call for dispatch_log_ingestion
    starts = [u for u in posts if u.endswith(":start") and "dispatch_log_ingestion" in u]
    assert starts, "FAILED processor must still get :start call"


# ---------------------------------------------------------------------------
# TC-ASP-008 — INV-202: graceful when atlas unreachable in reset_pipeline
# ---------------------------------------------------------------------------

def test_TC_ASP_008_reset_pipeline_calls_restart_helper():
    """reset_pipeline() invokes restart_processors_for_topics() after
    recreating Kafka topics. We verify by checking that the call
    appears in the source code (structural check matching project's
    existing test pattern in test_integration.py)."""
    import inspect
    from scripts import pipeline_reset
    src = inspect.getsource(pipeline_reset.reset_pipeline)
    assert "restart_processors_for_topics" in src, (
        "reset_pipeline must call restart_processors_for_topics()"
    )


# ---------------------------------------------------------------------------
# TC-ASP-009 — INV-205: deploy._ensure_flink_topics calls restart helper
# ---------------------------------------------------------------------------

def test_TC_ASP_009_ensure_flink_topics_calls_restart_helper():
    """deploy._ensure_flink_topics also calls restart_processors_for_topics()
    after recreating output topics, so a re-deploy onto a working cluster
    doesn't silently wedge ASP consumer-group offsets."""
    import inspect
    from scripts import deploy
    src = inspect.getsource(deploy._create_flink_dml_statements)
    assert "restart_processors_for_topics" in src, (
        "deploy._create_flink_dml_statements (which contains _ensure_flink_topics) "
        "must trigger restart_processors_for_topics() after recreating topics"
    )


# ---------------------------------------------------------------------------
# TC-ASP-010..013 — Issue #4 (2026-05-29): :stop timeout leaves processor
# STOPPING, then :start races and 400s with "another operation has the lock".
# Observed in a real datagen run: zone_traffic_ingestion stuck STOPPING.
# ---------------------------------------------------------------------------

def test_TC_ASP_010_start_retries_on_lock_conflict():
    """When :start returns HTTP 400 with 'has the lock', the helper must
    retry (the stop is still finalizing) rather than giving up and
    leaving the processor STOPPING."""
    from scripts.common.asp_restart import restart_processors_for_topics

    post_calls = []
    # First :start gets the lock-conflict 400; second succeeds.
    start_attempts = {"n": 0}

    def fake_post(url, **kwargs):
        post_calls.append(url)
        if url.endswith(":start"):
            start_attempts["n"] += 1
            if start_attempts["n"] == 1:
                return MagicMock(
                    status_code=400,
                    text='{"detail":"... another operation '
                         '\\"FinishStopStreamProcessor-x\\" has the lock"}',
                )
            return MagicMock(status_code=200, text="")
        return MagicMock(status_code=200, text="")

    def fake_get(url, **kwargs):
        # Report STARTED so the wait loop terminates quickly
        return MagicMock(
            status_code=200,
            json=lambda: {"results": [
                {"name": "zone_traffic_ingestion", "state": "STARTED"}
            ]},
        )

    with patch("scripts.common.asp_restart.requests.post", side_effect=fake_post), \
         patch("scripts.common.asp_restart.requests.get", side_effect=fake_get), \
         patch("scripts.common.asp_restart.time.sleep"):  # no real backoff
        restart_processors_for_topics(
            project_id="p", instance="i",
            topics=["zone_traffic_sink"],
            auth=MagicMock(), poll_interval_s=0, timeout_per_processor=1,
        )

    start_calls = [u for u in post_calls if u.endswith(":start")]
    assert len(start_calls) >= 2, (
        "start must be retried after a 'has the lock' 400 conflict, "
        f"got {len(start_calls)} start call(s)"
    )


def test_TC_ASP_011_lock_conflict_detector_exists():
    """A helper that recognizes the Atlas 'has the lock' conflict must
    exist so the retry path is testable and explicit."""
    import scripts.common.asp_restart as ar
    assert hasattr(ar, "_is_lock_conflict"), \
        "asp_restart must expose _is_lock_conflict helper"
    # Positive + negative cases
    assert ar._is_lock_conflict(400, 'x "FinishStopStreamProcessor" has the lock y') is True
    assert ar._is_lock_conflict(400, "some other error") is False
    assert ar._is_lock_conflict(200, "has the lock") is False  # 2xx never a conflict


def test_TC_ASP_012_stop_timeout_does_not_block_full_window():
    """The :stop POST timeout must be bounded (not 120s). A long single-
    request timeout means a hung stop blocks the whole reset. The send
    timeout should be <= 60s."""
    import inspect
    import scripts.common.asp_restart as ar
    src = inspect.getsource(ar._send_action)
    # The default request_timeout must not be 120 anymore
    sig = inspect.signature(ar._send_action)
    rt = sig.parameters.get("request_timeout")
    assert rt is not None, "_send_action must keep a request_timeout param"
    assert rt.default <= 60, (
        f"_send_action request_timeout default must be <= 60s (was {rt.default}); "
        "a 120s single-request timeout lets a hung stop block the reset"
    )


def test_TC_ASP_013_waits_for_stopped_before_start():
    """The restart sequence must still wait for STOPPED before issuing
    :start (existing contract — guard against regression while adding
    the lock-retry)."""
    import inspect
    import scripts.common.asp_restart as ar
    src = inspect.getsource(ar.restart_processors_for_topics)
    stop_wait = src.find("_TERMINAL_STOPPED")
    start_loop = src.find("_start_with_lock_retry")
    assert stop_wait != -1 and start_loop != -1
    assert stop_wait < start_loop, (
        "must wait for STOPPED (terminal) before issuing :start"
    )
