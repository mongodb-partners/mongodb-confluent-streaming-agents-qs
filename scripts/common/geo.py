"""New Orleans map geometry — single source of truth.

Zone anchor points, the Mississippi River centerline, and the zone→waypoint
mapping used to animate boat dispatches along the real river channel. Shared
by the Streamlit dashboard (scripts/dashboard.py, which re-exports these
names for backward compatibility) and the Mission Control HUD (served by
scripts/live_server.py via GET /api/bootstrap, consumed by web/map.js).

Coordinates are [longitude, latitude] — the order deck.gl expects.
"""

from __future__ import annotations

from typing import Dict, List

FALLBACK_ZONES = [
    "Bywater",
    "Central Business District (CBD)",
    "French Quarter",
    "Garden District",
    "Marigny",
    "Uptown",
    "Warehouse District",
]

# Mississippi-river-side anchor for each zone — boats animate between these
# points on the live dispatch map. Both the long form ("Central Business
# District (CBD)") and short ("CBD") names map to the same point because
# the Flink agent prompt produces "CBD" while vessel_catalog stores the
# long form.
ZONE_COORDS: Dict[str, List[float]] = {
    "Bywater": [-90.0469, 29.9626],
    "Central Business District (CBD)": [-90.0715, 29.9499],
    "CBD": [-90.0715, 29.9499],
    "French Quarter": [-90.0628, 29.9584],
    "Garden District": [-90.0840, 29.9290],
    "Marigny": [-90.0560, 29.9628],
    "Uptown": [-90.1040, 29.9320],
    "Warehouse District": [-90.0720, 29.9445],
}

# Mississippi River centerline through New Orleans, sourced from
# OpenStreetMap (way IDs 163557188 + 163762082 + 163762083). These are
# REAL river-channel points from the OSM "waterway=river"+"name=Mississippi
# River" geometry, not hand-estimated guesses. Order: east (Industrial
# Canal mouth, downstream) to west (Carrollton bend, upstream of NOLA).
# To regenerate, run scripts/build_river_waypoints.py (Overpass API query +
# nearest match per zone).
RIVER_WAYPOINTS: List[List[float]] = [
    [-89.95517, 29.92272],  #  0  Industrial Canal mouth (downstream)
    [-89.96030, 29.92375],  #  1
    [-89.98321, 29.92835],  #  2
    [-90.01381, 29.94717],  #  3  Lower 9th / Holy Cross stretch
    [-90.02858, 29.95300],  #  4
    [-90.03519, 29.95562],  #  5
    [-90.04530, 29.95877],  #  6
    [-90.04844, 29.95930],  #  7  Bywater / Press St wharves
    [-90.05118, 29.95943],  #  8
    [-90.05441, 29.95911],  #  9  Marigny / Esplanade wharf
    [-90.05654, 29.95794],  # 10
    [-90.05843, 29.95657],  # 11  French Quarter / Toulouse wharf
    [-90.05964, 29.95443],  # 12
    [-90.05980, 29.95154],  # 13  CBD / Spanish Plaza
    [-90.05810, 29.94243],  # 14  Warehouse District / Convention Center
    [-90.05829, 29.93750],  # 15
    [-90.05856, 29.93364],  # 16
    [-90.05933, 29.93101],  # 17  Mardi Gras World stretch
    [-90.06101, 29.92850],  # 18
    [-90.06334, 29.92480],  # 19
    [-90.06752, 29.92121],  # 20
    [-90.07571, 29.91709],  # 21  Garden District / Tchoupitoulas
    [-90.08574, 29.91419],  # 22
    [-90.09345, 29.91197],  # 23  Uptown / Audubon
    [-90.10516, 29.90865],  # 24
    [-90.11076, 29.90787],  # 25
    [-90.11577, 29.90775],  # 26
    [-90.12177, 29.90917],  # 27
    [-90.12829, 29.91077],  # 28
    [-90.13402, 29.91309],  # 29  Carrollton bend (river turns north)
    [-90.13722, 29.91705],  # 30
    [-90.13881, 29.92302],  # 31
    [-90.13898, 29.94599],  # 32
    [-90.14146, 29.95238],  # 33
    [-90.14496, 29.95547],  # 34
    [-90.15038, 29.95621],  # 35
    [-90.15676, 29.95488],  # 36
    [-90.18412, 29.93250],  # 37
    [-90.19763, 29.92420],  # 38  far west (upstream of NOLA)
]

# Map each zone to its nearest waypoint index on the OSM river.
# Boat trips use the subsequence of RIVER_WAYPOINTS between origin and
# destination indices, so paths automatically follow the river's true
# centerline rather than crossing land.
ZONE_RIVER_INDEX: Dict[str, int] = {
    "Bywater": 7,
    "Marigny": 9,
    "French Quarter": 11,
    "Central Business District (CBD)": 13,
    "CBD": 13,
    "Warehouse District": 14,
    "Garden District": 21,
    "Uptown": 23,
}

# Backward-compat single-point dock coords (used by some helpers and tests).
# Each is the river waypoint for that zone.
ZONE_DOCK_COORDS: Dict[str, List[float]] = {
    z: list(RIVER_WAYPOINTS[i]) for z, i in ZONE_RIVER_INDEX.items()
}

# 30-second loop window for the TripsLayer playhead (in milliseconds), and
# per-trip duration / trail fade. Shared so the HUD's continuous animation
# and the dashboard's rerun-playhead animation stay in step.
TRIPS_LOOP_MS = 30_000
TRIPS_DURATION_MS = 8_000
TRIPS_TRAIL_MS = 4_000

# Flink tumbling-window size (minutes). MUST match the INTERVAL in the
# windowed_traffic view (terraform/agents/main.tf) and surge.DEFAULT_WINDOW_MIN.
# Drives the "Next Window" countdown so it doesn't lie about when the next
# window closes. Shortened 5→1 so anomalies surface faster in demos.
WINDOW_MINUTES = 1

# Map camera home: midpoint of the river crescent, zoomed so the full
# Industrial-Canal-to-Audubon arc is visible.
MAP_VIEW = {
    "latitude": 29.945,
    "longitude": -90.075,
    "zoom": 11.4,
    "pitch": 30,
    "bearing": 0,
}
