"""Regenerate the Mississippi River centerline waypoints used by
`scripts/dashboard.py`'s Live Dispatch Map.

Pulls the real river geometry from OpenStreetMap via the Overpass API,
concatenates the 3 ways covering NOLA, filters to the city bounds, and
prints Python that can be pasted into `RIVER_WAYPOINTS`. This is the
authoritative source: hand-estimated coordinates were repeatedly off by
several hundred meters because the river bends sharply south past CBD.

Run when you need to update the polyline (zoom level changes, river
re-tagged in OSM, etc.):

    uv run python -m scripts.build_river_waypoints
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.parse
import urllib.request


OVERPASS_QUERY = """
[out:json][timeout:60];
(
  way["waterway"="river"]["name"="Mississippi River"](29.88,-90.20,30.00,-89.95);
);
out geom;
"""


# Way IDs in upstream-to-downstream order (verified by inspection).
WAYS_IN_ORDER = [163557188, 163762082, 163762083]

# Zone → city-center coordinates (matches dashboard.ZONE_COORDS).
ZONE_COORDS = {
    "Bywater": (-90.0469, 29.9626),
    "CBD": (-90.0715, 29.9499),
    "French Quarter": (-90.0628, 29.9584),
    "Garden District": (-90.0840, 29.9290),
    "Marigny": (-90.0560, 29.9628),
    "Uptown": (-90.1040, 29.9320),
    "Warehouse District": (-90.0720, 29.9445),
}


def fetch_river() -> list[tuple[float, float]]:
    """Fetch the river polyline from Overpass, return E→W ordered points
    inside the NOLA bounding box.

    on HTTP 429 (Overpass rate-limit, common when
    regenerating frequently), surface an actionable message rather
    than a raw stack trace. Maintainers see the recovery hint.
    """
    data = urllib.parse.urlencode({"data": OVERPASS_QUERY}).encode()
    req = urllib.request.Request(
        "https://overpass-api.de/api/interpreter",
        data=data,
        headers={"User-Agent": "streaming-agents-quickstart/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            retry_after = (e.headers or {}).get("Retry-After", "unknown")
            raise RuntimeError(
                f"Overpass rate-limited (HTTP 429). Retry-After header: "
                f"{retry_after}. Overpass throttles aggressively; wait "
                f"a few minutes then retry. See "
                f"https://wiki.openstreetmap.org/wiki/Overpass_API#Public_Overpass_API_instances "
                f"for alternate endpoints if needed."
            ) from e
        raise

    ways = {el["id"]: el for el in body.get("elements", [])
            if el.get("type") == "way"}
    if not all(wid in ways for wid in WAYS_IN_ORDER):
        missing = [wid for wid in WAYS_IN_ORDER if wid not in ways]
        raise RuntimeError(f"Overpass missing expected ways: {missing}")

    pts: list[tuple[float, float]] = []
    for wid in WAYS_IN_ORDER:
        geom = ways[wid].get("geometry", [])
        chunk = [(p["lon"], p["lat"]) for p in geom]
        if pts and chunk and pts[-1] == chunk[0]:
            chunk = chunk[1:]
        pts.extend(chunk)

    # Filter to NOLA bounds + reverse for E→W
    nola = [p for p in pts if 29.89 <= p[1] <= 29.97 and -90.20 <= p[0] <= -89.95]
    return list(reversed(nola))


def find_nearest(target: tuple[float, float],
                 pts: list[tuple[float, float]]) -> int:
    best_i, best_d = 0, float("inf")
    for i, p in enumerate(pts):
        d = (p[0] - target[0]) ** 2 + (p[1] - target[1]) ** 2
        if d < best_d:
            best_d = d
            best_i = i
    return best_i


def main() -> None:
    print("[build_river_waypoints] querying Overpass…", file=sys.stderr)
    pts = fetch_river()
    print(f"[build_river_waypoints] {len(pts)} river points retrieved",
          file=sys.stderr)

    print("\n# Paste into scripts/dashboard.py replacing RIVER_WAYPOINTS:\n")
    print("RIVER_WAYPOINTS: List[List[float]] = [")
    for i, (lon, lat) in enumerate(pts):
        print(f"    [{lon:.5f}, {lat:.5f}],   # {i:2d}")
    print("]")

    print("\nZONE_RIVER_INDEX: Dict[str, int] = {")
    for zone, target in ZONE_COORDS.items():
        idx = find_nearest(target, pts)
        print(f'    "{zone}": {idx},')
    cbd_idx = find_nearest(ZONE_COORDS["CBD"], pts)
    print(f'    "Central Business District (CBD)": {cbd_idx},')
    print("}")


if __name__ == "__main__":
    main()
