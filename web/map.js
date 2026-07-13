// Live dispatch map — deck.gl over CARTO dark tiles, animated continuously
// in the browser (no server reruns). Trip geometry is a 1:1 port of the
// Python helpers in scripts/dashboard.py (_river_path_between,
// _evenly_spaced_timestamps, _build_dispatch_trips + fallback) so the HUD
// and the analytics dashboard tell the same story; the geometry itself
// arrives from GET /api/bootstrap (scripts/common/geo.py).

/* global deck */

const MAX_DISPATCH_DOCS = 50;
const FALLBACK_MAX_BOATS = 3;
const FPS_INTERVAL = 1000 / 30;

let cfg = null;          // geo config from bootstrap
let vessels = {};        // vessel_id -> base_zone
let dispatchDocs = [];   // newest first, capped
let trips = [];          // TripsLayer-ready
let surgeZones = new Set();
let deckgl = null;
let baseLayer = null;
let zoneLayers = [];
let lastFrame = 0;
let boatCountEl = null;

// ---- trip building (ported from dashboard.py) -------------------------------

function djb2(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i++) h = ((h << 5) + h + str.charCodeAt(i)) >>> 0;
  return h;
}

function riverPathBetween(originZone, destZone) {
  const a = cfg.zone_river_index[originZone];
  const b = cfg.zone_river_index[destZone];
  if (a == null || b == null || a === b) return null;
  const wp = cfg.river_waypoints;
  if (a < b) return wp.slice(a, b + 1).map(p => [...p]);
  return wp.slice(b, a + 1).reverse().map(p => [...p]);
}

function evenlySpacedTimestamps(path, t0, durationMs) {
  if (path.length <= 1) return [t0];
  const segLens = [];
  for (let i = 0; i < path.length - 1; i++) {
    const dx = path[i + 1][0] - path[i][0];
    const dy = path[i + 1][1] - path[i][1];
    segLens.push(Math.sqrt(dx * dx + dy * dy));
  }
  const total = segLens.reduce((s, x) => s + x, 0) || 1;
  const out = [t0];
  let cum = 0;
  for (const len of segLens) {
    cum += len / total;
    out.push(Math.round(t0 + cum * durationMs));
  }
  return out;
}

function makeRiverTrip(vesselId, originZone, destZone) {
  if (originZone === destZone) return null;
  const path = riverPathBetween(originZone, destZone);
  if (!path || path.length < 2) return null;
  const { loop_ms, duration_ms } = cfg.trips;
  const t0 = djb2(vesselId) % Math.max(1, loop_ms - duration_ms);
  return {
    path,
    timestamps: evenlySpacedTimestamps(path, t0, duration_ms),
    vessel_id: vesselId,
    destination: destZone,
  };
}

function parseDispatchJson(raw) {
  if (typeof raw !== 'string') return raw;
  try { return JSON.parse(raw); } catch { return raw; }
}

function tripsForDispatch(doc) {
  const dest = doc.pickup_zone;
  if (cfg.zone_river_index[dest] == null) return [];
  const out = [];
  const boats = parseDispatchJson(doc.dispatch_json ?? '');
  if (Array.isArray(boats)) {
    for (const boat of boats) {
      const vid = boat && typeof boat === 'object' ? boat.vessel_id : null;
      if (!vid) continue;
      const origin = vessels[vid];
      if (!origin || cfg.zone_river_index[origin] == null) continue;
      const trip = makeRiverTrip(vid, origin, dest);
      if (trip) out.push(trip);
    }
  }
  if (out.length) return out;
  // Fallback (parity with dashboard): unparseable dispatch_json still animates —
  // deterministic vessel pick, sorted by id, never from the surge zone itself.
  const candidates = Object.keys(vessels)
    .filter(vid => vessels[vid] && vessels[vid] !== dest && cfg.zone_river_index[vessels[vid]] != null)
    .sort();
  for (const vid of candidates.slice(0, FALLBACK_MAX_BOATS)) {
    const trip = makeRiverTrip(vid, vessels[vid], dest);
    if (trip) out.push(trip);
  }
  return out;
}

function rebuildTrips() {
  trips = dispatchDocs.flatMap(tripsForDispatch);
}

function interpolateBoats(currentTime) {
  const icons = [];
  for (const trip of trips) {
    const { path, timestamps: ts } = trip;
    if (path.length < 2 || ts.length !== path.length) continue;
    if (currentTime < ts[0] || currentTime > ts[ts.length - 1]) continue;
    let seg = 0;
    for (let i = 0; i < ts.length - 1; i++) {
      if (ts[i] <= currentTime && currentTime <= ts[i + 1]) { seg = i; break; }
    }
    const [a, b] = [path[seg], path[seg + 1]];
    const denom = Math.max(1, ts[seg + 1] - ts[seg]);
    const r = (currentTime - ts[seg]) / denom;
    icons.push({
      position: [a[0] + (b[0] - a[0]) * r, a[1] + (b[1] - a[1]) * r],
      heading: (Math.atan2(b[1] - a[1], b[0] - a[0]) * 180) / Math.PI,
      vessel_id: trip.vessel_id,
      tooltip: `${trip.vessel_id} → ${trip.destination}`,
    });
  }
  return icons;
}

// ---- layers -----------------------------------------------------------------

const BOAT_ICON = {
  url: '/assets/boat-icon.png',
  width: 128, height: 128, anchorX: 64, anchorY: 64, mask: false,
};

function zoneMarkers() {
  const seen = new Set();
  const markers = [];
  for (const [name, coord] of Object.entries(cfg.zones)) {
    const key = coord.join(',');
    if (seen.has(key)) continue; // "CBD" aliases the long form
    seen.add(key);
    const active = surgeZones.has(name) ||
      (name === 'Central Business District (CBD)' && surgeZones.has('CBD'));
    markers.push({
      position: coord,
      name,
      active,
      color: active ? [0, 237, 100, 235] : [235, 242, 246, 150],
      radius: active ? 90 : 55,
      tooltip: name,
    });
  }
  return markers;
}

function buildZoneLayers() {
  const markers = zoneMarkers();
  zoneLayers = [
    new deck.ScatterplotLayer({
      id: 'zones', data: markers,
      getPosition: d => d.position, getFillColor: d => d.color, getRadius: d => d.radius,
      radiusMinPixels: 3, radiusMaxPixels: 7, opacity: 0.8, pickable: true,
    }),
  ];
  buildZoneLabelEls();
}

// Zone names are HTML overlay elements, not a deck TextLayer: this build's
// TextLayer never produces a font atlas (verified: iconManager.texture stays
// null even for a minimal probe layer), and HTML labels render the brand
// font natively anyway. Positions re-project every frame in tick().
let labelWrap = null;
let labelEls = [];

function buildZoneLabelEls() {
  if (!labelWrap) {
    labelWrap = document.createElement('div');
    labelWrap.id = 'zonelabels';
    labelWrap.style.cssText =
      'position:absolute;inset:0;pointer-events:none;overflow:hidden;z-index:1;';
    document.getElementById('mapbg')?.appendChild(labelWrap);
  }
  labelWrap.innerHTML = '';
  labelEls = zoneMarkers().map(m => {
    const el = document.createElement('span');
    el.textContent = m.name;
    // Marigny sits ~1km from Bywater — hoist its label above the dot so the
    // two don't collide at the home zoom.
    const dy = m.name === 'Marigny' ? 'translate(-50%,-26px)' : 'translate(-50%,9px)';
    el.style.cssText =
      `position:absolute;transform:${dy};white-space:nowrap;` +
      'font-family:"Euclid Circular A",sans-serif;font-size:11px;letter-spacing:.02em;' +
      'padding:1px 6px;border-radius:6px;background:rgba(5,11,16,.62);' +
      (m.active
        ? 'color:#00ED64;border:1px solid rgba(0,237,100,.4);'
        : 'color:rgba(234,242,246,.82);border:1px solid rgba(140,170,195,.15);');
    el.dataset.lon = m.position[0];
    el.dataset.lat = m.position[1];
    labelWrap.appendChild(el);
    return el;
  });
}

function repositionZoneLabels() {
  if (!deckgl || !labelEls.length) return;
  let vp = null;
  try { vp = deckgl.getViewports ? deckgl.getViewports()[0] : null; } catch { return; }
  if (!vp) return;
  for (const el of labelEls) {
    const [x, y] = vp.project([Number(el.dataset.lon), Number(el.dataset.lat)]);
    el.style.left = `${x}px`;
    el.style.top = `${y}px`;
    el.style.display =
      x < -60 || y < -20 || x > vp.width + 60 || y > vp.height + 20 ? 'none' : '';
  }
}

function frameLayers(currentTime) {
  const icons = interpolateBoats(currentTime);
  if (boatCountEl) boatCountEl.textContent = String(icons.length);
  return [
    baseLayer,
    ...zoneLayers,
    new deck.TripsLayer({
      id: 'trips', data: trips,
      getPath: d => d.path, getTimestamps: d => d.timestamps,
      getColor: [0, 237, 100], opacity: 0.9,
      widthMinPixels: 4, jointRounded: true, capRounded: true,
      trailLength: cfg.trips.trail_ms, currentTime,
    }),
    new deck.IconLayer({
      id: 'boats', data: icons,
      getIcon: () => BOAT_ICON,
      getPosition: d => d.position, getAngle: d => d.heading,
      getSize: 4, sizeScale: 10, sizeMinPixels: 22, sizeMaxPixels: 44,
      billboard: false, pickable: true,
    }),
  ];
}

function tick(ts) {
  if (ts - lastFrame >= FPS_INTERVAL) {
    lastFrame = ts;
    deckgl.setProps({ layers: frameLayers(Date.now() % cfg.trips.loop_ms) });
    repositionZoneLabels();
  }
  requestAnimationFrame(tick);
}

// ---- public API ----------------------------------------------------------------

export function initMap(geoCfg, vesselHomes) {
  cfg = geoCfg;
  vessels = vesselHomes || {};
  boatCountEl = document.getElementById('boatN');
  baseLayer = new deck.TileLayer({
    id: 'basemap',
    data: 'https://basemaps.cartocdn.com/rastertiles/dark_all/{z}/{x}/{y}@2x.png',
    minZoom: 0, maxZoom: 19, tileSize: 256,
    renderSubLayers: p => new deck.BitmapLayer(p, {
      data: null, image: p.data,
      bounds: [p.tile.bbox.west, p.tile.bbox.south, p.tile.bbox.east, p.tile.bbox.north],
    }),
  });
  buildZoneLayers();
  deckgl = new deck.DeckGL({
    container: 'map',
    initialViewState: cfg.map_view,
    controller: true,
    layers: [],
    getTooltip: ({ object }) => object?.tooltip && { text: object.tooltip },
  });
  window.__mcDeck = deckgl; // debug/verify handle
  requestAnimationFrame(tick);
}

export function setVessels(vesselHomes) {
  vessels = vesselHomes || {};
  rebuildTrips();
}

export function setDispatches(docs) {
  dispatchDocs = (docs || []).slice(0, MAX_DISPATCH_DOCS);
  rebuildTrips();
}

export function addDispatch(doc) {
  dispatchDocs.unshift(doc);
  if (dispatchDocs.length > MAX_DISPATCH_DOCS) dispatchDocs.pop();
  rebuildTrips();
}

export function setSurgeZones(zones) {
  surgeZones = new Set(zones);
  buildZoneLayers();
}
