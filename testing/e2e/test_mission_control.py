"""Tests for the Mission Control HUD server surface (scripts/live_server.py).

Covers /api/bootstrap (payload shape, per-section degradation, JSON safety),
the static file mounts that serve the HUD (web/) and its assets, and the
geometry re-export parity between scripts.common.geo and scripts.dashboard
(the HUD's map.js consumes exactly what /api/bootstrap ships).
"""

from __future__ import annotations

import datetime as dt
import importlib
import json

import pytest

live = importlib.import_module("scripts.live_server")
geo = importlib.import_module("scripts.common.geo")


# --- fake Mongo just deep enough for build_bootstrap_payload ----------------


class _FakeCursor(list):
    def sort(self, *a, **k):
        return self

    def limit(self, n):
        return _FakeCursor(self[:n])


class _FakeColl:
    def __init__(self, docs=None, fail=False):
        self._docs = docs or []
        self._fail = fail

    def find(self, *a, **k):
        if self._fail:
            raise RuntimeError("boom")
        return _FakeCursor([dict(d) for d in self._docs])

    def estimated_document_count(self):
        if self._fail:
            raise RuntimeError("boom")
        return len(self._docs)


class _FakeAdmin:
    def command(self, name):
        assert name == "ping"
        return {"ok": 1.0}


class _FakeClient:
    admin = _FakeAdmin()

    def __init__(self, colls):
        self._colls = colls  # {(db, coll): _FakeColl}

    def __getitem__(self, db):
        outer = self

        class _DB:
            def __getitem__(self, coll):
                return outer._colls.get((db, coll), _FakeColl())

        return _DB()


VESSELS = [
    {"vessel_id": "NOLA-01", "base_zone": "Bywater"},
    {"vessel_id": "NOLA-02", "base_zone": "Uptown"},
    {"vessel_id": "", "base_zone": "Uptown"},  # dropped: falsy id
]
ANOMALY = {
    "pickup_zone": "French Quarter",
    "window_time": dt.datetime(2026, 7, 14, 12, 0, tzinfo=dt.timezone.utc),
    "request_count": 120,
    "expected_requests": 12,
    "anomaly_reason": "Jazz Fest crowd surge",
    "top_chunk_1": "Jazz Fest at the Fair Grounds",
}
DISPATCH = {
    "pickup_zone": "French Quarter",
    "dispatched_at": dt.datetime(2026, 7, 14, 12, 1, tzinfo=dt.timezone.utc),
    "dispatch_summary": "Dispatch Summary: 2 boats sent.",
    "dispatch_json": '[{"vessel_id": "NOLA-01"}]',
}


def _client(overrides=None):
    colls = {
        ("fleet", "vessel_catalog"): _FakeColl(VESSELS),
        ("analytics", "zone_anomalies"): _FakeColl([ANOMALY]),
        ("fleet", "dispatch_log"): _FakeColl([DISPATCH]),
        ("events", "knowledge_base"): _FakeColl([{"event_name": "Jazz Fest"}]),
        ("analytics", "zone_traffic"): _FakeColl([{"zone": "Bywater"}] * 3),
    }
    colls.update(overrides or {})
    return _FakeClient(colls)


# --- payload shape ------------------------------------------------------------


def test_bootstrap_without_client_ships_geometry_only():
    p = live.build_bootstrap_payload(None)
    assert p["connected"] is False
    assert p["geo"]["zones"] == geo.ZONE_COORDS
    assert p["geo"]["river_waypoints"] == geo.RIVER_WAYPOINTS
    assert p["geo"]["zone_river_index"] == geo.ZONE_RIVER_INDEX
    assert p["geo"]["trips"]["loop_ms"] == geo.TRIPS_LOOP_MS
    assert p["geo"]["window_minutes"] == geo.WINDOW_MINUTES
    assert p["vessels"] == {} and p["anomalies"] == [] and p["dispatches"] == []


def test_bootstrap_with_client_populates_all_sections():
    p = live.build_bootstrap_payload(_client())
    assert p["connected"] is True
    assert p["vessels"] == {"NOLA-01": "Bywater", "NOLA-02": "Uptown"}
    assert p["anomalies"][0]["pickup_zone"] == "French Quarter"
    assert p["dispatches"][0]["dispatch_json"] == DISPATCH["dispatch_json"]
    assert p["kb_events"][0]["event_name"] == "Jazz Fest"
    assert len(p["traffic"]) == 3  # HUD traffic chart warm-start rows
    assert p["counts"]["zone_traffic"] == 3
    # BSON/datetime coercion: the whole payload must survive json.dumps.
    encoded = json.dumps(p)
    assert "2026-07-14" in encoded


def test_json_safe_marks_naive_datetimes_as_utc():
    # pymongo returns naive UTC datetimes; without an explicit offset the
    # browser parses the ISO string as local time and every HUD timestamp
    # shifts by the viewer's UTC offset.
    naive = dt.datetime(2026, 7, 14, 12, 0)
    out = live._json_safe({"ts": naive})
    assert out["ts"].endswith("+00:00")


def test_deploy_flow_launches_only_mission_control():
    """2026-07-14: single-UI consolidation — the deploy flow launches the
    Mission Control server and opens it in the browser; the Streamlit
    dashboard is decommissioned from the flow (module kept for manual use)."""
    import inspect

    from scripts import deploy

    src = inspect.getsource(deploy.run_deployment)
    assert "_launch_live_server" in src
    assert "_launch_dashboard(root)" not in src


def test_bootstrap_sections_degrade_independently():
    p = live.build_bootstrap_payload(
        _client({("analytics", "zone_anomalies"): _FakeColl(fail=True)})
    )
    assert p["anomalies"] == []  # the broken section is empty…
    assert p["vessels"]          # …but the others still populate
    assert p["dispatches"]
    assert p["connected"] is True


# --- endpoint + static mounts ---------------------------------------------------


@pytest.fixture()
def app_client(monkeypatch):
    from starlette.testclient import TestClient

    # Never resolve a real URI in tests — bootstrap degrades to geometry-only.
    import scripts.common.mongo_uri as mongo_uri

    monkeypatch.setattr(mongo_uri, "resolve_mongodb_uri", lambda *a, **k: None)
    app = live.create_app(start_stream=False)
    with TestClient(app) as client:
        yield client


def test_api_bootstrap_endpoint_returns_geometry(app_client):
    r = app_client.get("/api/bootstrap")
    assert r.status_code == 200
    body = r.json()
    assert body["connected"] is False
    assert body["geo"]["map_view"]["latitude"] == pytest.approx(29.945)


def test_hud_is_served_at_root(app_client):
    r = app_client.get("/")
    assert r.status_code == 200
    assert "Mission Control" in r.text
    assert 'src="/app.js"' in r.text


@pytest.mark.parametrize("path", ["/app.js", "/map.js", "/icons.js"])
def test_hud_modules_are_served(app_client, path):
    r = app_client.get(path)
    assert r.status_code == 200
    assert "export" in r.text or "import" in r.text


def test_boat_asset_is_served(app_client):
    r = app_client.get("/assets/boat-icon.png")
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/png"


def test_api_routes_win_over_static_mount(app_client):
    # /api/health must still be the API, not a static 404.
    r = app_client.get("/api/health")
    assert r.status_code == 200
    assert "change_stream_connected" in r.json()


# --- geometry single-source-of-truth --------------------------------------------


def test_dashboard_reexports_geo_constants():
    dashboard = importlib.import_module("scripts.dashboard")
    assert dashboard.ZONE_COORDS is geo.ZONE_COORDS
    assert dashboard.RIVER_WAYPOINTS is geo.RIVER_WAYPOINTS
    assert dashboard.ZONE_RIVER_INDEX is geo.ZONE_RIVER_INDEX
    assert dashboard.WINDOW_MINUTES == geo.WINDOW_MINUTES
    assert dashboard.TRIPS_LOOP_MS == geo.TRIPS_LOOP_MS


def test_zone_river_index_points_into_waypoints():
    for zone, idx in geo.ZONE_RIVER_INDEX.items():
        assert 0 <= idx < len(geo.RIVER_WAYPOINTS), zone
        assert zone in geo.ZONE_COORDS
