"""TC-MAP-001..007 — Live Dispatch Map fallback when dispatch_json is unparseable.

REQ-E-200..203 + INV-201 from specs/2026-05-15-stability-fixes/.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# TC-MAP-001 + TC-MAP-002 — REQ-E-200: synthesize fallback trips
# ---------------------------------------------------------------------------

def test_TC_MAP_001_fallback_when_dispatch_json_is_string_none():
    """When dispatch_json is the literal string 'None', _build_dispatch_trips
    falls back to vessel_catalog-derived trips."""
    from scripts.dashboard import ZONE_COORDS, _build_dispatch_trips

    assert "Bywater" in ZONE_COORDS  # surge target
    assert "Uptown" in ZONE_COORDS   # vessel home
    dispatches = [{
        "pickup_zone": "Bywater",
        "dispatch_json": "None",
        "dispatched_at": "2026-05-15 01:00:00",
    }]
    vessel_home = {"VESSEL-1": "Uptown", "VESSEL-2": "Marigny", "VESSEL-3": "Bywater"}
    trips = _build_dispatch_trips(dispatches, vessel_home)
    assert len(trips) >= 1, "Expected at least one fallback trip"
    for t in trips:
        assert t["destination"] == "Bywater"
        # Origin must NOT be the surge zone itself
        assert t["path"][0] != ZONE_COORDS["Bywater"]


def test_TC_MAP_002_fallback_when_dispatch_json_missing_field():
    """When dispatch_json key is absent entirely, fallback still kicks in."""
    from scripts.dashboard import _build_dispatch_trips
    dispatches = [{"pickup_zone": "French Quarter"}]
    vessel_home = {"VESSEL-1": "Uptown", "VESSEL-2": "CBD"}
    trips = _build_dispatch_trips(dispatches, vessel_home)
    assert len(trips) >= 1


# ---------------------------------------------------------------------------
# TC-MAP-003 — REQ-E-201: deterministic vessel selection, capped at 3
# ---------------------------------------------------------------------------

def test_TC_MAP_003_deterministic_and_capped_fallback():
    """Same dispatch + vessel_home must produce same trips on repeated calls,
    and never more than 3 vessels per dispatch."""
    from scripts.dashboard import _build_dispatch_trips
    dispatches = [{"pickup_zone": "Bywater", "dispatch_json": "None"}]
    vessel_home = {f"VESSEL-{i:02d}": "Uptown" for i in range(10)}
    trips_a = _build_dispatch_trips(dispatches, vessel_home)
    trips_b = _build_dispatch_trips(dispatches, vessel_home)
    assert len(trips_a) <= 3, "Fallback must cap at 3 boats per dispatch"
    assert [t["vessel_id"] for t in trips_a] == [t["vessel_id"] for t in trips_b], (
        "Fallback selection must be deterministic"
    )


# ---------------------------------------------------------------------------
# TC-MAP-004 — REQ-E-202: prefer real dispatch_json over fallback
# ---------------------------------------------------------------------------

def test_TC_MAP_004_prefers_real_json_when_present():
    """When dispatch_json IS valid, use it instead of fallback."""
    from scripts.dashboard import _build_dispatch_trips
    dispatches = [{
        "pickup_zone": "Bywater",
        "dispatch_json": '[{"vessel_id": "VESSEL-99", "new_zone": "Bywater"}]',
    }]
    vessel_home = {
        "VESSEL-99": "Uptown",
        "VESSEL-1": "Marigny",
        "VESSEL-2": "CBD",
    }
    trips = _build_dispatch_trips(dispatches, vessel_home)
    vessel_ids = {t["vessel_id"] for t in trips}
    # Must include VESSEL-99 from the real JSON, NOT VESSEL-1 / VESSEL-2 fallback
    assert "VESSEL-99" in vessel_ids


# ---------------------------------------------------------------------------
# TC-MAP-005 — REQ-E-203: pickup_zone unknown → zero trips
# ---------------------------------------------------------------------------

def test_TC_MAP_005_unknown_pickup_zone_produces_no_trips():
    """A dispatch with a pickup_zone not in ZONE_COORDS contributes zero trips."""
    from scripts.dashboard import _build_dispatch_trips
    dispatches = [{"pickup_zone": "Atlantis", "dispatch_json": "None"}]
    vessel_home = {"VESSEL-1": "Uptown"}
    assert _build_dispatch_trips(dispatches, vessel_home) == []


# ---------------------------------------------------------------------------
# TC-MAP-006 — REQ-E-203: no available non-surge vessels → zero trips
# ---------------------------------------------------------------------------

def test_TC_MAP_006_no_available_vessels_in_other_zones():
    """When every vessel is in the surge zone, fallback returns zero trips."""
    from scripts.dashboard import _build_dispatch_trips
    dispatches = [{"pickup_zone": "Bywater", "dispatch_json": "None"}]
    vessel_home = {"VESSEL-1": "Bywater", "VESSEL-2": "Bywater"}
    assert _build_dispatch_trips(dispatches, vessel_home) == []


# ---------------------------------------------------------------------------
# TC-MAP-007 — INV-201: empty input still works
# ---------------------------------------------------------------------------

def test_TC_MAP_007_no_dispatches_returns_empty_list():
    """Empty dispatches list must not crash, returns []."""
    from scripts.dashboard import _build_dispatch_trips
    assert _build_dispatch_trips([], {}) == []
    assert _build_dispatch_trips([], {"VESSEL-1": "Uptown"}) == []


# ---------------------------------------------------------------------------
# TC-MAP-008 — REQ-E-204: map widens window when 15min is empty
# ---------------------------------------------------------------------------

def test_TC_MAP_008_fetch_dispatches_for_map_widens_when_empty(monkeypatch):
    """When _fetch_dispatches_for_map(client, ...) returns 0 rows for the
    short 15-minute window, the helper widens to the latest N regardless
    of age. Without this, a dashboard opened ~30 minutes after the last
    surge shows zero boats even though dispatch_log has rows."""
    from scripts.dashboard import _fetch_dispatches_for_map
    calls = []

    class _MockColl:
        def find(self, query=None):
            calls.append(query)
            return self

        def sort(self, *args, **kwargs):
            return self

        def limit(self, n):
            # First call (with cutoff) returns nothing; second (no cutoff)
            # returns one row.
            if calls and "dispatched_at" in (calls[-1] or {}):
                return iter([])
            return iter([{"pickup_zone": "Bywater", "dispatch_json": "None"}])

    class _MockDB:
        def __getitem__(self, _):
            return _MockColl()

    class _MockClient:
        def __getitem__(self, _):
            return _MockDB()

    out = _fetch_dispatches_for_map(_MockClient(), recent_window_minutes=15,
                                    fallback_limit=5)
    assert len(out) >= 1, "Expected helper to widen when 15-min window empty"
    # First call had a time filter; second was the fallback (no filter)
    assert any("dispatched_at" in (q or {}) for q in calls)
    assert any(q == {} or q is None for q in calls), (
        "Fallback query must not include a time filter"
    )


# ---------------------------------------------------------------------------
# TC-MAP-009 — boats route via dock coordinates (river-side), not zone centers
# ---------------------------------------------------------------------------

def test_TC_MAP_009_trips_use_river_dock_coords_not_zone_centers():
    """Boats must animate along the Mississippi centerline (multi-segment
    path) — endpoints at river-side dock points and intermediate
    waypoints between them. ZONE_DOCK_COORDS still anchors the
    endpoints; ZONE_COORDS only drives city-center label placement."""
    from scripts.dashboard import (
        ZONE_COORDS,
        ZONE_DOCK_COORDS,
        _build_dispatch_trips,
    )
    dispatches = [{"pickup_zone": "Bywater", "dispatch_json": "None"}]
    vessel_home = {"VESSEL-1": "Uptown"}
    trips = _build_dispatch_trips(dispatches, vessel_home)
    assert trips, "Sanity: fallback should produce a trip"
    path = trips[0]["path"]
    assert len(path) >= 2
    # Endpoints are dock coords for Uptown -> Bywater
    assert path[0] == ZONE_DOCK_COORDS["Uptown"]
    assert path[-1] == ZONE_DOCK_COORDS["Bywater"]
    # Multi-segment paths: at least one intermediate waypoint
    # (Uptown to Bywater spans the entire crescent)
    assert len(path) > 2, (
        f"Uptown→Bywater must traverse multiple river waypoints, got {len(path)}"
    )
    # Endpoints differ from city centers (regression: bug we fixed)
    assert path[0] != ZONE_COORDS["Uptown"]
    assert path[-1] != ZONE_COORDS["Bywater"]
    # Timestamps match path length (one timestamp per waypoint)
    ts = trips[0]["timestamps"]
    assert len(ts) == len(path)
    # Timestamps are monotonically increasing
    assert all(ts[i] <= ts[i + 1] for i in range(len(ts) - 1))


def test_TC_MAP_010_dock_coords_cover_all_known_zones():
    """Every zone the agent might dispatch to must have a dock coord.
    Without this, fallback for that zone would silently skip animations."""
    from scripts.dashboard import ZONE_COORDS, ZONE_DOCK_COORDS
    for zone in ZONE_COORDS:
        assert zone in ZONE_DOCK_COORDS, f"Missing dock coord for zone: {zone}"


def test_TC_MAP_011_dock_coords_are_distinct_from_zone_centers():
    """At least one dock coord must differ from its zone center, otherwise
    we haven't actually moved boats off land. The CBD/CBD alias share a
    city center coord by design (alias) so we don't test that pair."""
    from scripts.dashboard import ZONE_COORDS, ZONE_DOCK_COORDS
    distinct = sum(
        1 for z, c in ZONE_COORDS.items() if ZONE_DOCK_COORDS.get(z) != c
    )
    assert distinct >= 5, (
        f"Only {distinct} zones have river-distinct dock coords — "
        "boats will still appear on land for the others"
    )


def test_TC_MAP_012_boat_icon_asset_exists_and_is_a_png():
    """Boat IconLayer needs a real PNG, not a data-URI SVG (deck.gl rejected
    SVG data-URIs in some browsers). The repo must ship a valid PNG file."""
    from pathlib import Path
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").exists():
            asset = parent / "assets" / "boat-icon.png"
            assert asset.exists(), f"boat icon missing at {asset}"
            data = asset.read_bytes()
            # PNG magic
            assert data[:8] == b"\x89PNG\r\n\x1a\n", "asset is not a valid PNG"
            return
    raise AssertionError("could not locate project root")
