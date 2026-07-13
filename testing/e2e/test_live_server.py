"""Tests for the live SSE sidecar (scripts/live_server.py).

Covers the change-stream hub, JSON-safe coercion, reconnect/backoff, bounded
fan-out, missing-URI exit, and the HTTP endpoints (/api/health, /api/stream).

Traceability: TC-E-001..007 (hub + endpoints), crossing boundaries B1/B2/B3.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import importlib
import json

import pytest

live = importlib.import_module("scripts.live_server")


# --- TC-E-005 / TC-E-005b: BSON -> JSON coercion (boundary B3) ------------


def test_json_safe_coerces_bson_types():
    from bson import ObjectId
    from bson.decimal128 import Decimal128

    oid = ObjectId()
    doc = {
        "_id": oid,
        "price": Decimal128("42.5"),
        "ts": dt.datetime(2026, 7, 11, 12, 0, tzinfo=dt.timezone.utc),
        "nested": {"zone": "Bywater", "vals": [Decimal128("1.5"), 2]},
    }
    safe = live._json_safe(doc)
    # round-trips through JSON without error (the whole point)
    encoded = json.dumps(safe)
    back = json.loads(encoded)
    assert back["_id"] == str(oid)
    assert back["price"] == 42.5
    assert back["ts"].startswith("2026-07-11")
    assert back["nested"]["vals"][0] == 1.5


def test_json_safe_passes_through_primitives():
    doc = {"a": 1, "b": "x", "c": None, "d": [1, 2], "e": 1.25, "f": True}
    assert live._json_safe(doc) == doc


# --- REQ-NF-SEC: credentials never leak into logged stream errors ---------


def test_redact_strips_credentials_from_uri_in_error_text():
    msg = "auth failed for mongodb+srv://admin:s3cr3t@cluster0.abc.mongodb.net/db"
    redacted = live._redact(msg)
    assert "s3cr3t" not in redacted
    assert "admin" not in redacted
    assert "<redacted>@cluster0.abc.mongodb.net" in redacted


# --- TC-E-006: bounded fan-out, drop-oldest, isolation --------------------


def test_hub_fanout_delivers_to_all_subscribers():
    async def scenario():
        hub = live.ChangeStreamHub()
        hub.bind_loop(asyncio.get_running_loop())
        q1 = hub.subscribe()
        q2 = hub.subscribe()
        hub._publish({"collection": "fleet.dispatch_log", "doc": {"zone": "Uptown"}})
        await asyncio.sleep(0.05)
        e1 = q1.get_nowait()
        e2 = q2.get_nowait()
        assert e1["collection"] == e2["collection"] == "fleet.dispatch_log"
        hub.unsubscribe(q1)
        # after unsubscribe, q1 no longer receives
        hub._publish({"collection": "analytics.zone_anomalies", "doc": {}})
        await asyncio.sleep(0.05)
        assert q2.get_nowait()["collection"] == "analytics.zone_anomalies"
        assert q1.empty()

    asyncio.run(scenario())


def test_hub_drops_oldest_when_queue_full():
    async def scenario():
        hub = live.ChangeStreamHub(queue_maxsize=2)
        hub.bind_loop(asyncio.get_running_loop())
        q = hub.subscribe()
        for i in range(5):
            hub._publish({"collection": "c", "doc": {"i": i}})
        await asyncio.sleep(0.05)
        drained = []
        while not q.empty():
            drained.append(q.get_nowait()["doc"]["i"])
        # bounded: never more than maxsize retained; keeps the most recent
        assert len(drained) <= 2
        assert 4 in drained  # newest survived

    asyncio.run(scenario())


# --- TC-E-001b: change-doc -> event transform (routing + filtering) -------


def test_dispatch_change_filters_and_shapes_events():
    async def scenario():
        hub = live.ChangeStreamHub()
        hub.bind_loop(asyncio.get_running_loop())
        q = hub.subscribe()

        # watched collection -> emitted with JSON-safe doc + metadata
        hub._dispatch_change(
            {
                "operationType": "insert",
                "ns": {"db": "fleet", "coll": "dispatch_log"},
                "fullDocument": {"zone": "Uptown", "boats": 2},
            }
        )
        # unwatched collection -> dropped
        hub._dispatch_change(
            {
                "operationType": "insert",
                "ns": {"db": "misc", "coll": "junk"},
                "fullDocument": {"x": 1},
            }
        )
        await asyncio.sleep(0.05)

        evt = q.get_nowait()
        assert evt["collection"] == "fleet.dispatch_log"
        assert evt["operationType"] == "insert"
        assert evt["doc"] == {"zone": "Uptown", "boats": 2}
        assert "ts" in evt
        assert q.empty()  # the unwatched change was filtered out

    asyncio.run(scenario())


# --- TC-E-004: reconnect with exponential backoff (state machine) ---------


def test_watch_loop_reconnects_with_exponential_backoff():
    hub = live.ChangeStreamHub()
    sleeps: list[float] = []
    attempts = {"n": 0}

    def failing_client_factory():
        attempts["n"] += 1
        if attempts["n"] >= 3:
            hub.request_stop()  # end the loop after a few failures
        raise RuntimeError("stream boom")

    hub._run_watch_loop(
        client_factory=failing_client_factory,
        sleep=lambda s: sleeps.append(s),
    )
    assert attempts["n"] >= 3
    # backoff grows and is capped at 30s
    assert sleeps[0] == 1.0
    assert sleeps[1] == 2.0
    assert all(s <= 30.0 for s in sleeps)
    assert hub.change_stream_connected is False


# --- TC-E-004b: watch is opened cluster-wide, not on a single database ----


def test_watch_loop_opens_cluster_level_stream():
    """REQ-E-004: the watcher SHALL open the change stream on the CLIENT
    (deployment-wide), never on a single database. The watched collections
    span analytics/fleet/events and the project URI has no default DB, so a
    db-scoped watch would both miss collections and raise ConfigurationError.
    """
    hub = live.ChangeStreamHub()
    calls = {"client_watch": 0, "get_default_database": 0}

    class FakeStream:
        def __enter__(self):
            return iter(())  # empty stream -> loop falls through

        def __exit__(self, *a):
            return False

    class FakeClient:
        def watch(self, **kwargs):
            calls["client_watch"] += 1
            hub.request_stop()  # stop after opening once
            assert kwargs.get("full_document") == "updateLookup"
            return FakeStream()

        def get_default_database(self):
            calls["get_default_database"] += 1
            raise AssertionError("must not resolve a single default database")

    hub._run_watch_loop(client_factory=lambda: FakeClient(), sleep=lambda s: None)
    assert calls["client_watch"] == 1
    assert calls["get_default_database"] == 0


# --- TC-E-007: missing URI exits non-zero, does not hang ------------------


def test_main_exits_nonzero_without_uri(monkeypatch):
    import scripts.common.mongo_uri as mu

    monkeypatch.setattr(mu, "resolve_mongodb_uri", lambda project_root=None: None)
    rc = live.main(["--no-serve-check"])
    assert rc != 0


# --- TC-E-001/002/003: HTTP endpoints (boundaries B1, B2) -----------------


def test_health_endpoint_shape():
    from starlette.testclient import TestClient

    app = live.create_app(start_stream=False)
    with TestClient(app) as client:
        r = client.get("/api/health")
        assert r.status_code == 200
        body = r.json()
        assert set(body) >= {"status", "change_stream_connected", "uptime_s", "clients"}


def test_sse_event_stream_emits_hello_then_change():
    """TC-E-002 (serialization half of boundary B2): the SSE generator yields a
    `hello` frame, then a `change` frame carrying a published event.

    Driven directly (no blocking HTTP client). The real over-the-wire HTTP
    crossing is covered by the spawned-process test in test_live_server_proc.
    """

    async def scenario():
        hub = live.ChangeStreamHub()
        hub.bind_loop(asyncio.get_running_loop())
        gen = live.sse_event_stream(hub, ping_timeout=5.0)

        first = await gen.__anext__()
        assert first["event"] == "hello"

        hub._publish(
            {
                "collection": "fleet.dispatch_log",
                "operationType": "insert",
                "doc": {"zone": "CBD"},
            }
        )
        second = await gen.__anext__()
        assert second["event"] == "change"
        payload = json.loads(second["data"])
        assert payload["collection"] == "fleet.dispatch_log"
        assert payload["doc"]["zone"] == "CBD"

        await gen.aclose()  # fires finally -> unsubscribe (REQ-E-006)
        assert hub.client_count == 0

    asyncio.run(scenario())


# NOTE: the real over-the-wire HTTP crossing of /api/stream (boundary B2:
# 200 + text/event-stream + live frames) is covered by the spawned-process
# curl test in test_live_server_proc.py. It is intentionally NOT tested via an
# in-process ASGI probe here — sse-starlette's EventSourceResponse requires a
# real server task lifecycle and blocks a bare ASGI call.


def test_health_reports_credentials_not_leaked():
    """REQ-NF-SEC: health/response never contains a connection string."""
    from starlette.testclient import TestClient

    app = live.create_app(start_stream=False)
    with TestClient(app) as client:
        body = client.get("/api/health").text
        assert "mongodb+srv://" not in body
        assert "@" not in body
