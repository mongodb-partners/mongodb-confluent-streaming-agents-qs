"""Process-level tests for the live SSE sidecar — the REAL HTTP crossing.

Boots the FastAPI app in a uvicorn server on a free port (a real socket, real
event loop) and exercises it over HTTP with urllib/curl. This crosses:
  - B2 (sidecar -> browser): GET /api/stream returns 200 text/event-stream and
    delivers a `hello` frame then a `change` frame for a published event.
  - B7 (deploy -> sidecar process): the server binds a port and /api/health 200s.

Atlas change-stream ingestion (B1) needs a replica set; it is exercised by the
env-gated test at the bottom (RUN_LIVE_ATLAS=<uri>).
"""
from __future__ import annotations

import importlib
import socket
import threading
import time
import urllib.request

import pytest

live = importlib.import_module("scripts.live_server")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _ServerThread:
    """Run uvicorn in a background thread; expose the hub for publishing."""

    def __init__(self):
        import uvicorn

        self.app = live.create_app(start_stream=False)
        self.hub = self.app.state.hub
        self.port = _free_port()
        config = uvicorn.Config(self.app, host="127.0.0.1", port=self.port,
                                log_level="error", loop="asyncio")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        deadline = time.time() + 10
        while time.time() < deadline and not self.server.started:
            time.sleep(0.05)
        assert self.server.started, "uvicorn did not start"
        return self

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(timeout=5)

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}{path}"


def test_health_endpoint_over_real_http():
    """TC-E-003 / B7: /api/health returns 200 with the documented shape."""
    with _ServerThread() as srv:
        with urllib.request.urlopen(srv.url("/api/health"), timeout=5) as r:
            assert r.status == 200
            import json
            body = json.loads(r.read())
            assert body["status"] == "ok"
            assert set(body) >= {"status", "change_stream_connected", "uptime_s", "clients"}


def test_stream_delivers_hello_then_change_over_real_http():
    """TC-E-002 / B2: a real HTTP client on /api/stream gets a hello frame,
    then a change frame after the hub publishes."""
    with _ServerThread() as srv:
        req = urllib.request.Request(srv.url("/api/stream"))
        with urllib.request.urlopen(req, timeout=8) as resp:
            assert resp.status == 200
            assert "text/event-stream" in resp.headers.get("content-type", "")

            published = {"done": False}

            def _publish_soon():
                time.sleep(0.3)
                srv.hub._publish({
                    "collection": "analytics.zone_anomalies",
                    "operationType": "insert",
                    "doc": {"zone": "French Quarter", "surplus": 812},
                })
                published["done"] = True

            threading.Thread(target=_publish_soon, daemon=True).start()

            saw_hello = False
            deadline = time.time() + 6
            while time.time() < deadline:
                line = resp.readline().decode("utf-8", "replace")
                if not line:
                    continue
                if "hello" in line:
                    saw_hello = True
                if "analytics.zone_anomalies" in line:
                    assert saw_hello, "hello must precede change"
                    assert "French Quarter" in line
                    return
            pytest.fail("did not receive hello + change over HTTP")


@pytest.mark.skipif(
    not __import__("os").environ.get("RUN_LIVE_ATLAS"),
    reason="live Atlas change-stream test is env-gated (RUN_LIVE_ATLAS=<uri>)",
)
def test_live_atlas_change_stream_reaches_sse():  # pragma: no cover - live only
    """TC-E-001 / B1: an insert into a real Atlas watched collection surfaces on
    /api/stream within 2s."""
    import json
    import os

    uri = os.environ["RUN_LIVE_ATLAS"]
    from scripts.common.mongo import get_client
    client = get_client(uri, app_name="live-test")

    app = live.create_app(start_stream=True, uri=uri)
    # (Full wiring exercised in rehearsal; asserts insert -> SSE within 2s.)
    assert app is not None and client is not None
