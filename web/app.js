// Mission Control — orchestration. Boot from GET /api/bootstrap, then treat
// the UI as a pure projection of MongoDB Atlas change streams (/api/stream).
// Nothing on this screen is staged client-side: every pulse, banner and boat
// is a real pipeline write surfacing in real time.

import { icon } from '/icons.js';
import { initMap, setDispatches, addDispatch, setSurgeZones, setVessels } from '/map.js';

const $ = s => document.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const REDUCED = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

let GEO = null;

// ---- pipeline rail (the storytelling spine) ---------------------------------
const STAGES = [
  { key: 'requests', name: 'Ride requests', sys: 'Kafka · ride_requests', ico: 'stream', color: 'var(--kafka)' },
  { key: 'window', name: 'Windowing', sys: 'Flink · tumbling window', ico: 'window', color: 'var(--flink)' },
  { key: 'detect', name: 'Anomaly detect', sys: 'Flink SQL · baseline', ico: 'radar', color: 'var(--surge)' },
  { key: 'context', name: 'Context', sys: 'Atlas Vector Search', ico: 'vector', color: 'var(--asp)' },
  { key: 'agent', name: 'Agent decides', sys: 'Streaming agent · LLM', ico: 'agent', color: 'var(--amber)' },
  { key: 'dispatch', name: 'Dispatch', sys: 'Atlas · dispatch_log', ico: 'boat', color: 'var(--spring)' },
];
const stageCounts = {};

function renderRail() {
  $('#rail').innerHTML = STAGES.map((s, i) => `
    ${i ? `<div class="conduit" data-into="${s.key}" style="--c:${s.color}"><i></i></div>` : ''}
    <div class="stage-tile" data-stage="${s.key}" style="--c:${s.color}">
      <div class="ico">${icon(s.ico, 19)}</div>
      <div class="name">${s.name}</div>
      <div class="sys">${s.sys}</div>
      <div class="n" data-n="${s.key}">${stageCounts[s.key] ?? '—'}</div>
    </div>`).join('');
}

function setStageCount(key, value) {
  stageCounts[key] = value;
  const el = document.querySelector(`[data-n="${key}"]`);
  if (el) el.textContent = typeof value === 'number' ? value.toLocaleString() : value;
}

/** Fire the rail: a light travels into each stage in order, then the stage
 *  tile flashes and stays "hot". This is the sense→act pulse made visible. */
function fireStages(keys) {
  keys.forEach((key, i) => {
    const delay = REDUCED ? 0 : i * 170;
    setTimeout(() => {
      const conduit = document.querySelector(`.conduit[data-into="${key}"]`);
      if (conduit) {
        conduit.classList.remove('fire'); void conduit.offsetWidth;
        conduit.classList.add('fire');
      }
      const tile = document.querySelector(`.stage-tile[data-stage="${key}"]`);
      if (tile) {
        tile.classList.add('hot');
        tile.classList.remove('fire'); void tile.offsetWidth;
        tile.classList.add('fire');
      }
    }, delay);
  });
}

// ---- time helpers -------------------------------------------------------------
function toDate(v) {
  if (v == null) return null;
  if (typeof v === 'number') return new Date(v > 1e12 ? v : v * 1000);
  const d = new Date(v);
  return isNaN(d) ? null : d;
}
const fmtTime = v => { const d = toDate(v); return d ? d.toLocaleTimeString() : '—'; };

// ---- surge queue ----------------------------------------------------------------
const anomalies = [];          // newest first: {key, zone, when, actual, expected, reason, chunks, dispatch}
let selectedKey = null;        // user pin; null = follow latest
const QUEUE_CAP = 30;

const anomalyKey = d => `${d.pickup_zone}|${d.window_time}`;

function anomalyRecord(d) {
  const actual = Number(d.request_count ?? d.actual_count ?? 0) || 0;
  const expected = Number(d.expected_requests ?? d.expected_count ?? 0) || 0;
  return {
    key: anomalyKey(d),
    zone: d.pickup_zone ?? 'Unknown',
    when: d.window_time ?? d.detected_at,
    actual, expected,
    mult: expected > 0 ? actual / expected : null,
    reason: d.anomaly_reason ?? '',
    chunks: [d.top_chunk_1, d.top_chunk_2, d.top_chunk_3].filter(Boolean),
    dispatch: null,
  };
}

function renderQueue() {
  const q = $('#queue');
  $('#qcount').textContent = anomalies.length ? String(anomalies.length) : '';
  if (!anomalies.length) {
    q.innerHTML = `<div class="empty">No surges yet. The queue fills the moment Flink SQL
      flags a zone running hotter than its baseline.</div>`;
    return;
  }
  q.innerHTML = '';
  const activeKey = selectedKey ?? anomalies[0].key;
  for (const a of anomalies) {
    const el = document.createElement('div');
    el.className = 'surge-card' + (a.dispatch ? ' dispatched' : '') + (a.key === activeKey ? ' sel' : '');
    el.innerHTML = `
      <div class="row"><span class="zone">${esc(a.zone)}</span>
        <span class="pill ${a.dispatch ? 'dispatched' : 'detected'}">${a.dispatch ? 'dispatched' : 'detected'}</span></div>
      <div class="nums"><b>${a.actual.toLocaleString()}</b> req
        <span class="exp">vs ${a.expected.toLocaleString()} expected</span>
        ${a.mult ? ` <span class="pill mult">×${a.mult >= 10 ? Math.round(a.mult) : a.mult.toFixed(1)}</span>` : ''}</div>
      <div class="when" style="margin-top:5px">${fmtTime(a.when)}</div>`;
    el.onclick = () => {
      selectedKey = selectedKey === a.key ? null : a.key; // click again to re-follow latest
      renderQueue(); renderReason();
    };
    q.appendChild(el);
  }
}

// ---- agent reasoning panel ---------------------------------------------------------
function activeAnomaly() {
  if (!anomalies.length) return null;
  return anomalies.find(a => a.key === selectedKey) ?? anomalies[0];
}

let typeTimer = null;
function typewrite(el, text) {
  clearInterval(typeTimer);
  if (REDUCED || text.length < 40) { el.textContent = text; return; }
  let i = 0;
  const step = Math.max(2, Math.round(text.length / 60)); // ~1.2s total
  typeTimer = setInterval(() => {
    i = Math.min(text.length, i + step);
    el.textContent = text.slice(0, i);
    if (i >= text.length) clearInterval(typeTimer);
  }, 20);
}

// Port of dashboard._clean_dispatch_summary: strip the agent's tool-calling
// transcript and structured dumps; prefer the "Dispatch Summary:" paragraph.
function cleanDispatchSummary(raw) {
  if (!raw || typeof raw !== 'string') return '';
  let text = raw.replace(/<tool_call>[\s\S]*?<\/tool_call>/gi, '')
                .replace(/<tool_response>[\s\S]*?<\/tool_response>/gi, '');
  const strip = t => (t || '').trim().replace(/^[*_ \t\r\n]+|[*_ \t\r\n]+$/g, '').trim();
  const emph = '[*_]{0,2}';
  const m = text.match(new RegExp(`${emph}\\s*Dispatch\\s+Summary\\s*:?\\s*${emph}\\s*`, 'i'));
  if (m) {
    let after = text.slice(m.index + m[0].length);
    const stop = after.match(new RegExp(`\\n*\\s*${emph}\\s*(?:Dispatch\\s+JSON|API\\s+Response)\\s*:?`, 'i'));
    if (stop) after = after.slice(0, stop.index);
    const cleaned = strip(after);
    if (cleaned) return cleaned.replace(/\n{3,}/g, '\n\n');
  }
  for (const marker of ['Dispatch JSON', 'API Response']) {
    const idx = text.match(new RegExp(`\\n*\\s*${emph}\\s*${marker}\\s*:?`, 'i'));
    if (idx) text = text.slice(0, idx.index);
  }
  return strip(text).replace(/\n{3,}/g, '\n\n');
}

/** The HUD renders plain text (no markdown engine): drop the agent's inline
 *  `**bold**` markers and trailing `---` rules instead of showing them raw. */
function plainify(text) {
  return (text || '')
    .replace(/\*\*/g, '')
    .replace(/^[-–—]{3,}\s*$/gm, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function renderReason() {
  const a = activeAnomaly();
  const box = $('#reason');
  $('#reason-zone').textContent = a ? a.zone : '';
  if (!a) {
    box.innerHTML = `<div class="empty">When a surge lands, the agent's explanation appears
      here — grounded in event context retrieved with Atlas Vector Search.</div>`;
    return;
  }
  box.innerHTML = `
    <div class="beat">
      <div class="lbl">${icon('radar', 12)} Why it fired <span class="chip flink">Flink SQL</span></div>
      <div class="reasontext" id="reasontext"></div>
    </div>
    ${a.chunks.length ? `
    <div class="beat">
      <div class="lbl">${icon('vector', 12)} Context <span class="chip">$vectorSearch</span></div>
      ${a.chunks.map((c, i) => `<div class="evidence" style="animation-delay:${REDUCED ? 0 : i * 0.14}s">
        <b>Chunk ${i + 1}</b> — ${esc(c)}</div>`).join('')}
    </div>` : ''}
    ${a.dispatch ? `
    <div class="beat">
      <div class="lbl">${icon('boat', 12)} Action <span class="chip mongo">fleet.dispatch_log</span></div>
      <div class="actiontext">${esc(plainify(cleanDispatchSummary(a.dispatch.dispatch_summary))) || 'Boats dispatched to the surge zone.'}</div>
      <div class="stamp">Dispatched</div>
    </div>` : ''}`;
  typewrite($('#reasontext'), a.reason || 'Traffic exceeded the learned baseline for this zone.');
}

// ---- left panel tabs (Surges / Traffic / Events) ---------------------------------------
let activeTab = 'surges';
function initTabs() {
  document.querySelectorAll('.ltab').forEach(btn => {
    btn.addEventListener('click', () => {
      activeTab = btn.dataset.tab;
      document.querySelectorAll('.ltab').forEach(b => {
        const on = b === btn;
        b.classList.toggle('on', on);
        b.setAttribute('aria-selected', String(on));
      });
      document.querySelectorAll('.ltview').forEach(v =>
        v.classList.toggle('on', v.dataset.view === activeTab));
      if (activeTab === 'traffic') renderTraffic();
      if (activeTab === 'events') renderEvents();
    });
  });
}

// ---- zone traffic chart (identity over time → multi-line) -------------------------------
// Categorical palette: reference dark-mode set, validated (lightness band,
// chroma floor, contrast ≥3:1; CVD floor-band relieved by the tooltip and the
// latest-window value list). Hues assigned by FIXED alphabetical zone order —
// never by arrival order, so a zone keeps its color across sessions.
const ZONE_ORDER = ['Bywater', 'Central Business District (CBD)', 'French Quarter',
  'Garden District', 'Marigny', 'Uptown', 'Warehouse District'];
const ZONE_HUES = ['#3987e5', '#199e70', '#c98500', '#008300', '#9085e9', '#e66767', '#d55181'];
const shortZone = z => z === 'Central Business District (CBD)' ? 'CBD' : z;
const zoneColor = z => {
  const i = ZONE_ORDER.indexOf(z === 'CBD' ? 'Central Business District (CBD)' : z);
  return i >= 0 ? ZONE_HUES[i] : '#8AA3B0';
};

const TRAFFIC_CAP = 60;            // windows kept per zone (~1 hour at 1-min tumble)
const trafficByZone = new Map();   // zone -> [{t: ms, n: count}] oldest-first

function pushTraffic(doc) {
  const zone = doc.zone === 'CBD' ? 'Central Business District (CBD)' : doc.zone;
  const t = toDate(doc.window_start)?.getTime();
  const n = Number(doc.request_count ?? 0) || 0;
  if (!zone || !t) return;
  if (!trafficByZone.has(zone)) trafficByZone.set(zone, []);
  const arr = trafficByZone.get(zone);
  if (arr.length && arr[arr.length - 1].t === t) { arr[arr.length - 1].n = n; return; }
  arr.push({ t, n });
  if (arr.length > TRAFFIC_CAP) arr.shift();
}

const CHART_W = 320, CHART_H = 150, PAD = { l: 26, r: 8, t: 8, b: 16 };

function trafficDomain() {
  let t0 = Infinity, t1 = -Infinity, nMax = 1;
  for (const arr of trafficByZone.values()) {
    for (const p of arr) {
      if (p.t < t0) t0 = p.t;
      if (p.t > t1) t1 = p.t;
      if (p.n > nMax) nMax = p.n;
    }
  }
  return t0 === Infinity ? null : { t0, t1: Math.max(t1, t0 + 60000), nMax };
}

function renderTraffic() {
  const box = $('#traffic');
  const dom = trafficDomain();
  if (!dom) {
    box.innerHTML = '<div class="empty">Waiting for the first Flink traffic window…</div>';
    return;
  }
  const x = t => PAD.l + ((t - dom.t0) / (dom.t1 - dom.t0)) * (CHART_W - PAD.l - PAD.r);
  const y = n => CHART_H - PAD.b - (n / dom.nMax) * (CHART_H - PAD.t - PAD.b);
  const gridY = [0, 0.5, 1].map(f => Math.round(dom.nMax * f));
  const zones = ZONE_ORDER.filter(z => trafficByZone.get(z)?.length);
  const paths = zones.map(z => {
    const d = trafficByZone.get(z)
      .map((p, i) => `${i ? 'L' : 'M'}${x(p.t).toFixed(1)} ${y(p.n).toFixed(1)}`).join(' ');
    return `<path class="series" stroke="${zoneColor(z)}" d="${d}"/>`;
  }).join('');
  const fmtT = t => new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  box.innerHTML = `
    <svg id="trafficchart" viewBox="0 0 ${CHART_W} ${CHART_H}" role="img"
         aria-label="Ride requests per zone per one-minute Flink window">
      ${gridY.map(v => `<line class="grid" x1="${PAD.l}" x2="${CHART_W - PAD.r}"
          y1="${y(v)}" y2="${y(v)}"/>
        <text class="axis" x="${PAD.l - 4}" y="${y(v) + 3}" text-anchor="end">${v}</text>`).join('')}
      ${paths}
      <line id="tcross" class="xhair" y1="${PAD.t}" y2="${CHART_H - PAD.b}" style="display:none"/>
      <text class="axis" x="${PAD.l}" y="${CHART_H - 4}">${fmtT(dom.t0)}</text>
      <text class="axis" x="${CHART_W - PAD.r}" y="${CHART_H - 4}" text-anchor="end">${fmtT(dom.t1)}</text>
    </svg>
    <div id="tlegend">${zones.map(z =>
      `<span class="lg"><i style="background:${zoneColor(z)}"></i>${esc(shortZone(z))}</span>`).join('')}</div>
    <div id="tnowvals"><div class="lbl">Latest window</div>${zones.map(z => {
      const arr = trafficByZone.get(z); const last = arr[arr.length - 1];
      return `<div class="zrow"><i style="background:${zoneColor(z)}"></i>${esc(shortZone(z))}
        <b>${last.n.toLocaleString()}</b></div>`;
    }).join('')}</div>`;
  wireTrafficHover(dom, x);
}

function wireTrafficHover(dom, x) {
  const svg = $('#trafficchart'); const tip = $('#ttip'); const cross = $('#tcross');
  if (!svg) return;
  svg.addEventListener('mousemove', e => {
    const r = svg.getBoundingClientRect();
    const sx = ((e.clientX - r.left) / r.width) * CHART_W;
    const t = dom.t0 + ((sx - PAD.l) / (CHART_W - PAD.l - PAD.r)) * (dom.t1 - dom.t0);
    // snap to the nearest window across zones
    let best = null;
    for (const arr of trafficByZone.values()) {
      for (const p of arr) if (!best || Math.abs(p.t - t) < Math.abs(best - t)) best = p.t;
    }
    if (best == null) return;
    cross.style.display = '';
    cross.setAttribute('x1', x(best)); cross.setAttribute('x2', x(best));
    const rows = ZONE_ORDER.filter(z => trafficByZone.get(z)?.some(p => p.t === best))
      .map(z => {
        const p = trafficByZone.get(z).find(q => q.t === best);
        return `<div class="zrow"><i style="background:${zoneColor(z)}"></i>${esc(shortZone(z))}<b>${p.n}</b></div>`;
      }).join('');
    tip.innerHTML = `<div class="tt">${new Date(best).toLocaleTimeString()}</div>${rows}`;
    tip.style.display = 'block';
    tip.style.left = Math.min(e.clientX + 14, window.innerWidth - 240) + 'px';
    tip.style.top = Math.min(e.clientY + 14, window.innerHeight - 200) + 'px';
  });
  svg.addEventListener('mouseleave', () => {
    tip.style.display = 'none'; cross.style.display = 'none';
  });
}

// ---- events (knowledge base) --------------------------------------------------------------
let kbEvents = [];
function renderEvents() {
  const box = $('#events');
  if (!kbEvents.length) {
    box.innerHTML = `<div class="empty">No events seeded yet. These knowledge-base entries are
      what Atlas Vector Search retrieves as surge context.</div>`;
    return;
  }
  box.innerHTML = kbEvents.map(ev => {
    const impact = String(ev.impact_level || 'low').toLowerCase();
    return `<div class="kbcard ${esc(impact)}">
      <div class="row"><span class="name">${esc(ev.event_name || 'Event')}</span>
        <span class="pill impact-${esc(impact)}">${esc(impact)}</span></div>
      <div class="meta">${esc(ev.zone || '')}${ev.venue ? ` — ${esc(ev.venue)}` : ''}
        ${ev.expected_attendance ? ` · ${Number(ev.expected_attendance).toLocaleString()} expected` : ''}</div>
      ${ev.description ? `<div class="desc">${esc(String(ev.description).slice(0, 180))}</div>` : ''}
    </div>`;
  }).join('');
}

// ---- ops feed --------------------------------------------------------------------------
const FEED_CAP = 60;
function addFeed(ico, title, detail) {
  const feed = $('#feed');
  const empty = feed.querySelector('.empty');
  if (empty) empty.remove();
  const it = document.createElement('div');
  it.className = 'feed-item';
  it.innerHTML = `<div class="fico">${icon(ico, 15)}</div>
    <div class="fmain"><div class="row"><b>${esc(title)}</b>
      <span class="t">${new Date().toLocaleTimeString()}</span></div>
      ${detail ? `<div class="fdet">${esc(detail)}</div>` : ''}</div>`;
  feed.prepend(it);
  while (feed.childElementCount > FEED_CAP) feed.lastElementChild.remove();
}

// ---- bottom KPIs (odometer-eased) --------------------------------------------------------
const kpiDefs = [
  { key: 'windows', label: 'traffic windows' },
  { key: 'anomalies', label: 'anomalies' },
  { key: 'dispatches', label: 'dispatches' },
  { key: 'events', label: 'events this session' },
];
const kpiTarget = { windows: 0, anomalies: 0, dispatches: 0, events: 0 };
const kpiShown = { windows: 0, anomalies: 0, dispatches: 0, events: 0 };

function renderKpis() {
  $('#kpis').innerHTML = kpiDefs.map(k =>
    `<span class="kpi" data-kpi="${k.key}"><span>${k.label}</span><b>0</b></span>`).join('');
}
function bumpKpi(key, value) {
  kpiTarget[key] = value ?? kpiTarget[key] + 1;
  const el = document.querySelector(`[data-kpi="${key}"]`);
  if (el) { el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash'); }
}
function animateKpis() {
  for (const k of kpiDefs) {
    const diff = kpiTarget[k.key] - kpiShown[k.key];
    if (Math.abs(diff) >= 1) {
      kpiShown[k.key] += REDUCED ? diff : Math.sign(diff) * Math.max(1, Math.abs(diff) * 0.14);
      const el = document.querySelector(`[data-kpi="${k.key}"] b`);
      if (el) el.textContent = Math.round(kpiShown[k.key]).toLocaleString();
    }
  }
  requestAnimationFrame(animateKpis);
}

// ---- banners --------------------------------------------------------------------------------
let bannerTimer = null;
function showBanner(kind, text) {
  const b = $('#banner');
  b.className = kind; b.textContent = text; b.classList.add('show');
  clearTimeout(bannerTimer);
  bannerTimer = setTimeout(() => b.classList.remove('show'), 6000);
}

// ---- window countdown --------------------------------------------------------------------------
function tickCountdown() {
  const winS = (GEO?.window_minutes ?? 1) * 60;
  const s = winS - Math.floor(Date.now() / 1000) % winS;
  const b = $('#countdown b');
  if (b) b.textContent = s >= 60 ? `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}` : `0:${String(s).padStart(2, '0')}`;
}

// ---- connection status ------------------------------------------------------------------------
function setConn(cls, label) {
  const c = $('#conn'); c.className = cls; c.textContent = '● ' + label;
  $('#atlas').classList.toggle('on', cls === 'live');
}

// ---- event routing (SSE) ------------------------------------------------------------------------
function onTraffic(doc) {
  fireStages(['requests', 'window']);
  bumpKpi('windows', kpiTarget.windows + 1);
  setStageCount('window', (typeof stageCounts.window === 'number' ? stageCounts.window : 0) + 1);
  const reqs = Number(doc.request_count ?? 0) || 0;
  setStageCount('requests', (typeof stageCounts.requests === 'number' ? stageCounts.requests : 0) + reqs);
  addFeed('traffic', `window · ${doc.zone ?? ''}`, `${reqs} requests`);
  pushTraffic(doc);
  if (activeTab === 'traffic') renderTraffic();
}

function onAnomaly(doc) {
  const rec = anomalyRecord(doc);
  const idx = anomalies.findIndex(a => a.key === rec.key);
  if (idx !== -1) {
    // Enrichment update: the anomalies_enriched_ingestion ASP processor
    // $merges the LLM anomaly_reason + top_chunk_* evidence onto the same
    // document after the sink path wrote it. Refresh the card in place —
    // no new banner or KPI bump, this surge was already announced.
    const prev = anomalies[idx];
    rec.dispatch = prev.dispatch;
    const gainedContext = rec.chunks.length && !prev.chunks.length;
    anomalies[idx] = rec;
    if (gainedContext) {
      fireStages(['context']);
      setStageCount('context', (typeof stageCounts.context === 'number' ? stageCounts.context : 0) + 1);
      addFeed('vector', `context · ${rec.zone}`, `${rec.chunks.length} evidence chunk${rec.chunks.length > 1 ? 's' : ''} retrieved`);
    }
    renderQueue(); renderReason();
    return;
  }
  anomalies.unshift(rec);
  if (anomalies.length > QUEUE_CAP) anomalies.pop();
  fireStages(['requests', 'window', 'detect', 'context']);
  setStageCount('detect', (typeof stageCounts.detect === 'number' ? stageCounts.detect : 0) + 1);
  setStageCount('context', (typeof stageCounts.context === 'number' ? stageCounts.context : 0) + (rec.chunks.length ? 1 : 0));
  bumpKpi('anomalies', kpiTarget.anomalies + 1);
  showBanner('surgekind', `⚠ SURGE DETECTED — ${rec.zone.toUpperCase()}`);
  addFeed('surge', `surge · ${rec.zone}`, `${rec.actual} vs ${rec.expected} expected`);
  $('#hint').classList.remove('show');
  setSurgeZones(anomalies.slice(0, 5).filter(a => !a.dispatch).map(a => a.zone));
  renderQueue(); renderReason();
}

function onDispatch(doc) {
  fireStages(['agent', 'dispatch']);
  setStageCount('agent', (typeof stageCounts.agent === 'number' ? stageCounts.agent : 0) + 1);
  setStageCount('dispatch', (typeof stageCounts.dispatch === 'number' ? stageCounts.dispatch : 0) + 1);
  bumpKpi('dispatches', kpiTarget.dispatches + 1);
  const zone = doc.pickup_zone ?? '';
  showBanner('dispatchkind', `⛴ AGENT DISPATCHING — ${zone.toUpperCase() || 'FLEET'}`);
  addFeed('boat', `dispatch · ${zone}`, plainify(cleanDispatchSummary(doc.dispatch_summary)).slice(0, 90));
  addDispatch(doc);
  // Stamp the matching surge card (most recent undispatched anomaly in the zone).
  const match = anomalies.find(a => !a.dispatch &&
    (a.zone === zone || (a.zone === 'Central Business District (CBD)' && zone === 'CBD')));
  if (match) match.dispatch = doc;
  setSurgeZones(anomalies.slice(0, 5).filter(a => !a.dispatch).map(a => a.zone));
  renderQueue(); renderReason();
}

function connect() {
  const es = new EventSource('/api/stream');
  es.addEventListener('hello', () => setConn('live', 'LIVE'));
  es.addEventListener('ping', () => setConn('live', 'LIVE'));
  es.addEventListener('change', e => {
    setConn('live', 'LIVE');
    let ev; try { ev = JSON.parse(e.data); } catch { return; }
    if (ev.operationType === 'delete') return; // deletes carry no fullDocument
    bumpKpi('events');
    const doc = ev.doc || {};
    switch (ev.collection) {
      case 'analytics.zone_traffic': onTraffic(doc); break;
      case 'analytics.zone_anomalies': onAnomaly(doc); break;
      case 'fleet.dispatch_log': onDispatch(doc); break;
      case 'events.knowledge_base':
        addFeed('kb', 'knowledge base', doc.event_name ?? 'event updated');
        if (doc.event_name) {
          kbEvents = [doc, ...kbEvents.filter(k => k.event_name !== doc.event_name)];
          if (activeTab === 'events') renderEvents();
        }
        break;
      default: addFeed('insert', ev.collection ?? 'write', ev.operationType);
    }
  });
  es.onerror = () => {
    setConn('reconnecting', 'RECONNECTING');
    setTimeout(() => { if (es.readyState === 2) setConn('offline', 'OFFLINE'); }, 4000);
  };
}

// ---- guided tour -----------------------------------------------------------------------------------
const TOUR = [
  { sel: '#title', title: 'Welcome to Mission Control', body: 'A live fleet-operations console for New Orleans. Streaming ride requests flow through Confluent Cloud, Flink SQL detects demand surges, MongoDB Atlas Vector Search supplies the operational context, and an AI agent dispatches boats — with no human in the loop. Everything you see is the real pipeline, live.' },
  { sel: '#rail', title: 'The pipeline, end to end', body: 'Sense → reason → act, left to right: Kafka ride requests, Flink tumbling windows, anomaly detection in Flink SQL, context retrieval with Atlas Vector Search, the agent\'s decision, and the dispatch written to Atlas. Watch a light travel down this rail as each real event lands.' },
  { sel: '#stage', title: 'The live dispatch map', body: 'Boats animate along the actual Mississippi River centerline (OpenStreetMap geometry) from their home docks to the surge zone. Green trails are dispatches in flight; the pulsing green dot marks the zone under surge.' },
  { sel: '#queue-panel', title: 'Surges, traffic and events', body: 'Every anomaly Flink SQL flags, newest first — actual vs expected demand and the surge multiplier. A card flips from DETECTED to DISPATCHED the moment the agent acts on it. The tabs switch to the live per-zone Traffic chart (every 1-minute Flink window) and the Events knowledge base that Atlas Vector Search retrieves context from.' },
  { sel: '#reason-panel', title: 'Inside the agent\'s head', body: 'Why the anomaly fired, the event context retrieved with $vectorSearch (Jazz Fest, a Saints game…), and the action the agent took. This is the "reason" step of sense → reason → act, made visible.' },
  { sel: '#feed-panel', title: 'Live operations feed', body: 'A pure projection of MongoDB Atlas change streams — every row is a database write pushed to this screen the instant it lands. Nothing here is simulated client-side.' },
  { sel: '#bottom', title: 'The payoff readout', body: 'Live counts straight from the cluster: traffic windows, anomalies, autonomous dispatches, and every change-stream event received this session.' },
  { sel: '#countdown', title: 'Make it fire on cue', body: 'Flink windows close every minute. Run `uv run surge` in a terminal and the full loop — surge banner, reasoning, boats — lands within one window. That\'s the whole webinar story in sixty seconds.' },
];
let tourIx = 0;
function positionTour() {
  const s = TOUR[tourIx]; const el = document.querySelector(s.sel);
  if (!el) return;
  const r = el.getBoundingClientRect(); const pad = 8;
  const hole = $('#tourHole');
  hole.style.left = (r.left - pad) + 'px'; hole.style.top = (r.top - pad) + 'px';
  hole.style.width = (r.width + pad * 2) + 'px'; hole.style.height = (r.height + pad * 2) + 'px';
  const card = $('#tourCard'); card.style.display = 'block';
  const cw = 330, ch = card.offsetHeight || 210;
  let top = r.bottom + 14;
  if (top + ch > window.innerHeight - 12) top = Math.max(12, r.top - ch - 14);
  const left = Math.min(Math.max(12, r.left), window.innerWidth - cw - 12);
  card.style.top = top + 'px'; card.style.left = left + 'px';
  $('#tourStep').textContent = `Step ${tourIx + 1} of ${TOUR.length}`;
  $('#tourTitle').textContent = s.title; $('#tourBody').textContent = s.body;
  $('#tourDots').innerHTML = TOUR.map((_, i) => `<i class="${i === tourIx ? 'on' : ''}"></i>`).join('');
  $('#tourPrev').style.visibility = tourIx === 0 ? 'hidden' : 'visible';
  $('#tourNext').textContent = tourIx === TOUR.length - 1 ? 'Done ✓' : 'Next ›';
}
function startTour() { tourIx = 0; $('#tourMask').classList.add('on'); positionTour(); }
function endTour() {
  $('#tourMask').classList.remove('on'); $('#tourCard').style.display = 'none';
  localStorage.setItem('mc-tour-seen', '1');
}
function initTour() {
  $('#tourBtn').addEventListener('click', startTour);
  $('#tourSkip').addEventListener('click', endTour);
  $('#tourPrev').addEventListener('click', () => { if (tourIx > 0) { tourIx--; positionTour(); } });
  $('#tourNext').addEventListener('click', () => { tourIx === TOUR.length - 1 ? endTour() : (tourIx++, positionTour()); });
  window.addEventListener('resize', () => { if ($('#tourMask').classList.contains('on')) positionTour(); });
  const suppressed = new URLSearchParams(location.search).get('tour') === '0';
  if (!suppressed && !localStorage.getItem('mc-tour-seen')) setTimeout(startTour, 900);
}

// ---- boot -------------------------------------------------------------------------------------------
async function boot() {
  renderRail(); renderKpis(); animateKpis();
  setInterval(tickCountdown, 250); tickCountdown();

  let bs = null;
  try { bs = await fetch('/api/bootstrap').then(r => r.json()); } catch { /* offline boot */ }
  GEO = bs?.geo ?? null;
  if (GEO) initMap(GEO, bs.vessels || {});
  if (bs?.connected === false) $('#stale').textContent = 'atlas unreachable at boot — waiting for the stream';

  // Warm-start: recent history so the screen is alive before the first live event.
  const counts = bs?.counts ?? {};
  if (typeof counts.zone_traffic === 'number') { setStageCount('window', counts.zone_traffic); kpiTarget.windows = kpiShown.windows = counts.zone_traffic; }
  if (typeof counts.anomalies === 'number') { setStageCount('detect', counts.anomalies); setStageCount('context', counts.anomalies); kpiTarget.anomalies = kpiShown.anomalies = counts.anomalies; }
  if (typeof counts.dispatches === 'number') { setStageCount('agent', counts.dispatches); setStageCount('dispatch', counts.dispatches); kpiTarget.dispatches = kpiShown.dispatches = counts.dispatches; }
  document.querySelectorAll('.stage-tile').forEach(t => {
    const k = t.dataset.stage;
    if (typeof stageCounts[k] === 'number' && stageCounts[k] > 0) t.classList.add('hot');
  });
  renderKpis(); // re-render so warm counts paint immediately
  for (const k of kpiDefs) {
    const el = document.querySelector(`[data-kpi="${k.key}"] b`);
    if (el) el.textContent = Math.round(kpiShown[k.key]).toLocaleString();
  }

  for (const d of (bs?.anomalies ?? []).slice().reverse()) {
    const rec = anomalyRecord(d);
    if (!anomalies.some(a => a.key === rec.key)) anomalies.unshift(rec);
  }
  // Mark warm-start anomalies dispatched where a dispatch already exists.
  for (const d of bs?.dispatches ?? []) {
    const match = anomalies.find(a => !a.dispatch &&
      (a.zone === d.pickup_zone || (a.zone === 'Central Business District (CBD)' && d.pickup_zone === 'CBD')));
    if (match) match.dispatch = d;
  }
  setDispatches(bs?.dispatches ?? []);
  setSurgeZones(anomalies.slice(0, 5).filter(a => !a.dispatch).map(a => a.zone));
  for (const row of bs?.traffic ?? []) pushTraffic(row);
  kbEvents = bs?.kb_events ?? [];
  initTabs();
  renderQueue(); renderReason(); renderTraffic(); renderEvents();
  if (!anomalies.length) $('#hint').classList.add('show');

  initTour();
  connect();
}
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
