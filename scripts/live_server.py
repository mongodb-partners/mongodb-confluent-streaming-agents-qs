"""Live "DB is alive" SSE sidecar — `uv run live`.

Holds a single MongoDB Atlas change stream over the pipeline's output
collections and fans every change out to connected browsers as Server-Sent
Events. This is what makes the dashboard *push* — a dispatch appears the
instant the agent writes it to Atlas, not on the next poll tick.

Architecture (Path B, spec `specs/live-viz`):

    Atlas change stream ──(watcher thread)──▶ ChangeStreamHub
                                                   │ fan-out
                                        per-client asyncio.Queue
                                                   │
                              GET /api/stream (text/event-stream)  ──▶ browser

The hub reuses `scripts.common.mongo.get_client` (INV-003) and the shared URI
resolver (INV-005). BSON types are coerced to JSON primitives (REQ-E-005).
The design keeps pure, injectable seams (`_json_safe`, `_run_watch_loop` with
`client_factory`/`sleep`) so the whole thing is unit-testable without a real
Atlas connection or real sleeps.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import threading
import time
from typing import Any, Callable, Optional, Set

logger = logging.getLogger("live_server")

# Strip credentials from any mongodb URI that surfaces in an exception string,
# so a stream error is never logged with user:pass (REQ-NF-SEC).
_URI_CRED_RE = re.compile(r"(mongodb(?:\+srv)?://)[^@/\s]+@")


def _redact(text: str) -> str:
    return _URI_CRED_RE.sub(r"\1<redacted>@", text)


# Collections whose changes drive the live overlay.
WATCHED_COLLECTIONS = {
    "analytics.zone_anomalies",
    "fleet.dispatch_log",
    "analytics.zone_traffic",
    "events.knowledge_base",
}

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8502

# Project layout anchors for the static Mission Control HUD.
_PROJECT_ROOT = __import__("pathlib").Path(__file__).resolve().parent.parent
WEB_DIR = _PROJECT_ROOT / "web"
ASSETS_DIR = _PROJECT_ROOT / "assets"

# Bootstrap query limits — the HUD warm-starts from these, then goes live
# off the SSE stream. Small on purpose: the payload is a page-load cost.
_BOOT_ANOMALIES_LIMIT = 20
_BOOT_DISPATCH_WINDOW_MIN = 15
_BOOT_DISPATCH_LIMIT = 50
_BOOT_DISPATCH_FALLBACK = 5
_BOOT_KB_LIMIT = 12
# ~340 rows ≈ 45+ min of 7-zone/1-min windows for the HUD traffic chart.
_BOOT_TRAFFIC_LIMIT = 340
_BACKOFF_START = 1.0
_BACKOFF_CAP = 30.0
_QUEUE_MAXSIZE = 2000


def _json_safe(value: Any) -> Any:
    """Recursively coerce BSON/Mongo types into JSON-serializable primitives.

    Handles ObjectId, Decimal128, datetime, and nested dict/list. Anything
    already JSON-safe passes through unchanged (REQ-E-005, boundary B3).
    """
    # Lazy imports so this module imports even if bson is absent in a test env.
    try:
        from bson import ObjectId
        from bson.decimal128 import Decimal128
    except ImportError:  # pragma: no cover
        ObjectId = ()  # type: ignore
        Decimal128 = ()  # type: ignore

    import datetime as _dt

    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if ObjectId and isinstance(value, ObjectId):
        return str(value)
    if Decimal128 and isinstance(value, Decimal128):
        return float(value.to_decimal())
    if isinstance(value, _dt.datetime):
        # pymongo returns naive datetimes holding UTC values; without an
        # explicit offset the browser parses the ISO string as LOCAL time
        # and every timestamp shifts by the viewer's UTC offset.
        if value.tzinfo is None:
            value = value.replace(tzinfo=_dt.timezone.utc)
        return value.isoformat()
    return value


class ChangeStreamHub:
    """Fan-out hub: one watcher thread, many per-client asyncio queues."""

    def __init__(self, queue_maxsize: int = _QUEUE_MAXSIZE):
        self._subscribers: Set[asyncio.Queue] = set()
        self._listeners: list = []  # (coll, op, raw_doc) callbacks, any thread
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._queue_maxsize = queue_maxsize
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.change_stream_connected = False
        self._started_at = time.monotonic()

    # -- lifecycle ---------------------------------------------------------
    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def start(self, uri: str, app_name: str = "streaming-agents-live") -> None:
        """Start the background watcher thread against a real Atlas URI."""

        def factory():
            from scripts.common.mongo import get_client

            return get_client(uri, app_name=app_name)

        self._thread = threading.Thread(
            target=self._run_watch_loop,
            kwargs={"client_factory": factory},
            name="mongo-change-stream",
            daemon=True,
        )
        self._thread.start()

    def request_stop(self) -> None:
        self._stop.set()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    @property
    def uptime_s(self) -> float:
        return round(time.monotonic() - self._started_at, 1)

    @property
    def client_count(self) -> int:
        return len(self._subscribers)

    # -- pub/sub -----------------------------------------------------------
    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._queue_maxsize)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    def _publish(self, event: dict) -> None:
        """Fan an event out to every subscriber (drop-oldest on overflow).

        Thread-safe: when called from the watcher thread, schedules the put on
        the event loop; when called from within the loop (tests), enqueues
        directly.
        """
        for q in list(self._subscribers):
            self._enqueue(q, event)

    def _enqueue(self, q: asyncio.Queue, event: dict) -> None:
        def _put() -> None:
            if q.full():
                try:
                    q.get_nowait()  # drop oldest (REQ-NF-PERF, bounded memory)
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:  # pragma: no cover - defensive
                pass

        loop = self._loop
        running = None
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if loop is not None and running is not loop:
            # Called from another thread → hop onto the loop thread.
            loop.call_soon_threadsafe(_put)
        else:
            _put()

    # -- watcher -----------------------------------------------------------
    def _run_watch_loop(
        self,
        client_factory: Callable[[], Any],
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """Hold the change stream; reconnect with exponential backoff.

        Injectable `client_factory`/`sleep` make the reconnect/backoff logic
        unit-testable without a real Mongo or real waits (REQ-E-004).
        """
        backoff = _BACKOFF_START
        while not self._stop.is_set():
            try:
                client = client_factory()
                # Watch at the CLUSTER level, not a single database. The watched
                # collections span three DBs (analytics, fleet, events) and the
                # project URI carries no default database, so a db-scoped watch
                # would both miss collections and raise ConfigurationError.
                # `_dispatch_change` filters by ns.db+ns.coll (REQ-E-004).
                with client.watch(
                    full_document="updateLookup", max_await_time_ms=1000
                ) as stream:
                    self.change_stream_connected = True
                    backoff = _BACKOFF_START
                    for change in stream:
                        if self._stop.is_set():
                            break
                        self._dispatch_change(change)
            except Exception as exc:  # noqa: BLE001 — reconnect on any failure
                self.change_stream_connected = False
                if self._stop.is_set():
                    break
                # Log the (credential-redacted) reason so a misconfigured
                # sidecar is diagnosable instead of silently retrying forever.
                logger.warning(
                    "change stream disconnected (%s); retry in %.0fs",
                    _redact(f"{type(exc).__name__}: {exc}"),
                    backoff,
                )
                sleep(backoff)
                backoff = min(backoff * 2, _BACKOFF_CAP)
        self.change_stream_connected = False

    def add_listener(self, fn: Callable[[str, str, dict], None]) -> None:
        """Register an in-process observer called as fn(coll, op, raw_doc)
        from the watcher thread with the RAW (un-serialized) document.
        Listener errors are swallowed — observers must never break SSE."""
        self._listeners.append(fn)

    def _dispatch_change(self, change: dict) -> None:
        ns = change.get("ns", {}) or {}
        coll = f"{ns.get('db', '')}.{ns.get('coll', '')}"
        if coll not in WATCHED_COLLECTIONS:
            return
        doc = change.get("fullDocument") or change.get("documentKey") or {}
        op = change.get("operationType", "unknown")
        for fn in list(self._listeners):
            try:
                fn(coll, op, doc)
            except Exception:  # noqa: BLE001 — observers never break the SSE path
                logger.exception("change listener raised")
        self._publish(
            {
                "collection": coll,
                "operationType": op,
                "ts": time.time(),
                "doc": _json_safe(doc),
            }
        )


# --- RAG enrichment fallback --------------------------------------------------
#
# Primary RAG path: the anomalies-enriched-insert Flink statement (Voyage
# query embedding → VECTOR_SEARCH_AGG → LLM explanation) merged onto the
# anomaly doc by the anomalies_enriched_ingestion ASP processor. That
# statement's per-anomaly federated vector search reliably times out inside
# Flink ("Max retries exceeded") and the statement dies — it is best-effort
# by design. This worker is the guarantee: when an anomaly doc still has no
# evidence chunks after giving Flink a head start, it runs the SAME retrieval
# directly (Voyage query embedding + Atlas $vectorSearch on
# events.knowledge_base) and $sets top_chunk_1..3 onto the document. The HUD
# stays a pure projection of database writes — this worker WRITES real
# vector-search results to the database; the UI only renders them. Same
# precedent as KB seeding, which also moved from a broken streaming-native
# path ($https → Voyage HTTP 400) to Python.

VOYAGE_ENDPOINT_DEFAULT = "https://ai.mongodb.com/v1/embeddings"
RAG_FALLBACK_DELAY_S = float(os.environ.get("RAG_FALLBACK_DELAY_S", "40"))


class RagFallbackWorker:
    """Watches anomaly inserts (via ChangeStreamHub.add_listener) and
    backfills Vector Search evidence chunks the Flink path failed to merge."""

    def __init__(
        self,
        uri: str,
        voyage_api_key: str,
        voyage_api_endpoint: str = VOYAGE_ENDPOINT_DEFAULT,
        delay_s: float = RAG_FALLBACK_DELAY_S,
    ):
        import queue as _queue

        self._uri = uri
        self._voyage_key = voyage_api_key
        self._voyage_endpoint = voyage_api_endpoint
        self._delay_s = delay_s
        self._queue: "_queue.Queue" = _queue.Queue(maxsize=200)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._seen: dict = {}  # (zone, window_time) -> monotonic ts
        self._client = None
        self.enriched_count = 0

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="rag-fallback", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    # -- hub listener (watcher thread) ----------------------------------------
    def offer(self, coll: str, op: str, doc: dict) -> None:
        """Queue an anomaly doc that has no evidence chunks yet."""
        if coll != "analytics.zone_anomalies" or op == "delete":
            return
        if doc.get("top_chunk_1"):
            return
        zone = doc.get("pickup_zone")
        window_time = doc.get("window_time")
        if not zone or window_time is None:
            return
        key = (zone, str(window_time))
        now = time.monotonic()
        # prune + dedupe (re-emissions, replace events, our own update echo)
        self._seen = {k: t for k, t in self._seen.items() if now - t < 600}
        if key in self._seen:
            return
        self._seen[key] = now
        try:
            self._queue.put_nowait((zone, window_time, now))
        except Exception:  # full queue — drop; the next anomaly still flows
            pass

    # -- worker thread ---------------------------------------------------------
    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                zone, window_time, offered_at = self._queue.get(timeout=1.0)
            except Exception:
                continue
            # Give the Flink enrichment path a head start; if its merge lands
            # first, the re-fetch below sees the chunks and we do nothing.
            wait = self._delay_s - (time.monotonic() - offered_at)
            if wait > 0 and self._stop.wait(timeout=wait):
                break
            try:
                self._enrich(zone, window_time)
            except Exception as exc:  # noqa: BLE001 — per-doc failures are logged, not fatal
                logger.warning(
                    "rag-fallback enrich failed for %s: %s",
                    zone,
                    _redact(f"{type(exc).__name__}: {exc}"),
                )

    def _get_client(self):
        if self._client is None:
            from scripts.common.mongo import get_client

            self._client = get_client(self._uri, app_name="streaming-agents-ragfb")
        return self._client

    def _embed_query(self, text: str) -> list:
        import requests

        resp = requests.post(
            self._voyage_endpoint,
            headers={
                "Authorization": f"Bearer {self._voyage_key}",
                "Content-Type": "application/json",
            },
            json={"model": "voyage-4", "input": [text], "input_type": "query"},
            timeout=30,
        )
        if not resp.ok:
            raise RuntimeError(f"voyage embed HTTP {resp.status_code}")
        return resp.json()["data"][0]["embedding"]

    def _enrich(self, zone: str, window_time: Any) -> None:
        client = self._get_client()
        coll = client["analytics"]["zone_anomalies"]
        doc = coll.find_one({"pickup_zone": zone, "window_time": window_time})
        if not doc or doc.get("top_chunk_1"):
            return  # gone, or the Flink path already merged its enrichment

        actual = doc.get("request_count", "?")
        expected = doc.get("expected_requests", "?")
        when = window_time
        try:
            hhmm = when.strftime("%I:%M %p").lstrip("0")
        except Exception:
            hhmm = str(when)
        query = (
            f"Transportation demand surge in {zone} at {hhmm}. "
            f"Expected: {expected}, Actual: {actual}. What HIGH impact "
            f"events, festivals, or gatherings are active in {zone} "
            "during this time?"
        )
        vector = self._embed_query(query)
        hits = list(
            client["events"]["knowledge_base"].aggregate(
                [
                    {
                        "$vectorSearch": {
                            "index": "vector_index",
                            "path": "embedding",
                            "queryVector": vector,
                            "numCandidates": 20,
                            "limit": 3,
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "chunk": 1,
                            "event_name": 1,
                            "event_type": 1,
                            "impact_level": 1,
                        }
                    },
                ]
            )
        )
        if not hits:
            return
        import datetime as _dt

        update = {
            "enriched_by": "rag-fallback",
            "enriched_at": _dt.datetime.now(_dt.timezone.utc),
        }
        for i, hit in enumerate(hits[:3], start=1):
            update[f"top_chunk_{i}"] = hit.get("chunk") or ""
        top = hits[0]
        if top.get("event_name"):
            base = (doc.get("anomaly_reason") or "").rstrip(". ")
            update["anomaly_reason"] = (
                f"{base}. Likely cause: {top['event_name']}"
                f" ({top.get('event_type', 'event')},"
                f" {top.get('impact_level', 'unknown')} impact)."
            )
        coll.update_one({"_id": doc["_id"]}, {"$set": update})
        self.enriched_count += 1
        logger.info("rag-fallback enriched %s @ %s", zone, window_time)


def build_bootstrap_payload(client: Optional[Any]) -> dict:
    """Assemble the Mission Control HUD's warm-start payload.

    Geometry always ships (it is static); each Mongo-backed section degrades
    independently to an empty value so a broken collection never blanks the
    whole HUD. `client=None` (URI unresolvable / connect failure) returns the
    geometry-only shape with connected=False — the HUD renders empty states.
    """
    from scripts.common import geo

    payload: dict = {
        "connected": False,
        "geo": {
            "zones": geo.ZONE_COORDS,
            "river_waypoints": geo.RIVER_WAYPOINTS,
            "zone_river_index": geo.ZONE_RIVER_INDEX,
            "map_view": geo.MAP_VIEW,
            "trips": {
                "loop_ms": geo.TRIPS_LOOP_MS,
                "duration_ms": geo.TRIPS_DURATION_MS,
                "trail_ms": geo.TRIPS_TRAIL_MS,
            },
            "window_minutes": geo.WINDOW_MINUTES,
        },
        "vessels": {},
        "anomalies": [],
        "dispatches": [],
        "kb_events": [],
        "traffic": [],
        "counts": {},
    }
    if client is None:
        return payload

    import datetime as _dt

    def _section(fn: Callable[[], Any], default: Any) -> Any:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — degrade per-section
            logger.warning("bootstrap section failed: %s", _redact(str(exc)))
            return default

    def _vessels() -> dict:
        coll = client["fleet"]["vessel_catalog"]
        return {
            d["vessel_id"]: d["base_zone"]
            for d in coll.find({}, {"vessel_id": 1, "base_zone": 1, "_id": 0})
            if d.get("vessel_id") and d.get("base_zone")
        }

    def _anomalies() -> list:
        coll = client["analytics"]["zone_anomalies"]
        docs = list(
            coll.find({}, {"_id": 0}).sort("window_time", -1).limit(
                _BOOT_ANOMALIES_LIMIT
            )
        )
        return [_json_safe(d) for d in docs]

    def _dispatches() -> list:
        # Same recent-window-else-latest logic as the dashboard map: prefer
        # dispatches that just happened, never render an empty map when
        # dispatch_log has data.
        coll = client["fleet"]["dispatch_log"]
        cutoff = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(
            minutes=_BOOT_DISPATCH_WINDOW_MIN
        )
        docs = list(
            coll.find({"dispatched_at": {"$gte": cutoff}}, {"_id": 0})
            .sort("dispatched_at", -1)
            .limit(_BOOT_DISPATCH_LIMIT)
        )
        if not docs:
            docs = list(
                coll.find({}, {"_id": 0})
                .sort("dispatched_at", -1)
                .limit(_BOOT_DISPATCH_FALLBACK)
            )
        return [_json_safe(d) for d in docs]

    def _kb() -> list:
        coll = client["events"]["knowledge_base"]
        docs = list(coll.find({}, {"_id": 0, "embedding": 0}).limit(_BOOT_KB_LIMIT))
        return [_json_safe(d) for d in docs]

    def _traffic() -> list:
        # Newest N windows, returned oldest-first so the chart appends live
        # SSE rows without re-sorting.
        coll = client["analytics"]["zone_traffic"]
        docs = list(
            coll.find(
                {},
                {"_id": 0, "zone": 1, "window_start": 1, "request_count": 1},
            )
            .sort("window_start", -1)
            .limit(_BOOT_TRAFFIC_LIMIT)
        )
        docs.reverse()
        return [_json_safe(d) for d in docs]

    def _counts() -> dict:
        return {
            "zone_traffic": client["analytics"]["zone_traffic"].estimated_document_count(),
            "anomalies": client["analytics"]["zone_anomalies"].estimated_document_count(),
            "dispatches": client["fleet"]["dispatch_log"].estimated_document_count(),
            "knowledge_base": client["events"]["knowledge_base"].estimated_document_count(),
        }

    # "connected" means the bootstrap actually reached Mongo (ping), which is
    # independent of the change stream's state (/api/health reports that).
    payload["connected"] = _section(
        lambda: bool(client.admin.command("ping")), False
    )
    payload["vessels"] = _section(_vessels, {})
    payload["anomalies"] = _section(_anomalies, [])
    payload["dispatches"] = _section(_dispatches, [])
    payload["kb_events"] = _section(_kb, [])
    payload["traffic"] = _section(_traffic, [])
    payload["counts"] = _section(_counts, {})
    return payload


async def sse_event_stream(hub: "ChangeStreamHub", ping_timeout: float = 15.0):
    """Yield SSE frames for one subscriber: a `hello`, then `change` events as
    the hub publishes, with `ping` keep-alives on idle (REQ-E-002).

    Extracted as a module-level async generator so it is directly unit-testable
    (drive it with `anext`) without a blocking HTTP client. Always unsubscribes
    on exit (REQ-E-006).
    """
    q = hub.subscribe()
    try:
        yield {"event": "hello", "data": json.dumps({"ok": True})}
        while True:
            try:
                event = await asyncio.wait_for(q.get(), timeout=ping_timeout)
                yield {"event": "change", "data": json.dumps(event)}
            except asyncio.TimeoutError:
                yield {"event": "ping", "data": "{}"}  # keep-alive
    finally:
        hub.unsubscribe(q)


def create_app(start_stream: bool = True, uri: Optional[str] = None):
    """Build the FastAPI app. When start_stream is False (tests), the watcher
    thread is not started but the hub + endpoints are fully live."""
    from contextlib import asynccontextmanager

    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from sse_starlette.sse import EventSourceResponse

    hub = ChangeStreamHub()

    @asynccontextmanager
    async def lifespan(app: "FastAPI"):
        hub.bind_loop(asyncio.get_running_loop())
        rag_worker = None
        if start_stream:
            resolved = uri
            if resolved is None:
                from scripts.common.mongo_uri import resolve_mongodb_uri

                resolved = resolve_mongodb_uri()
            if resolved:
                # RAG fallback: backfill vector-search evidence chunks when
                # the (best-effort) Flink enrichment path doesn't merge them.
                # Requires the Voyage key; without it the HUD still runs,
                # anomaly cards just keep their synthesized reasons.
                from scripts.common.mongo_uri import load_env_defaults

                env = {**load_env_defaults(), **os.environ}
                voyage_key = (env.get("TF_VAR_voyage_api_key") or "").strip()
                if voyage_key:
                    rag_worker = RagFallbackWorker(
                        uri=resolved,
                        voyage_api_key=voyage_key,
                        voyage_api_endpoint=(
                            env.get("TF_VAR_voyage_api_endpoint")
                            or VOYAGE_ENDPOINT_DEFAULT
                        ),
                    )
                    hub.add_listener(rag_worker.offer)
                    rag_worker.start()
                    app.state.rag_worker = rag_worker
                else:
                    logger.info(
                        "rag-fallback disabled: TF_VAR_voyage_api_key not set"
                    )
                hub.start(resolved)
        yield
        if rag_worker:
            rag_worker.stop()
        hub.stop()

    app = FastAPI(title="Streaming Agents — Live SSE", lifespan=lifespan)
    app.state.hub = hub

    # CORS: the dashboard (Streamlit, default :8501) and this sidecar
    # (:8502) are different origins, so the browser's EventSource request is
    # a cross-origin request. Without an Access-Control-Allow-Origin header
    # the browser blocks the stream and the overlay sits in RECONNECTING
    # forever (curl works because it ignores CORS). Allow the local dashboard
    # origins; override/extend via LIVE_SSE_ALLOW_ORIGINS (comma-separated,
    # or "*" to allow any origin for non-local deployments).
    from fastapi.middleware.cors import CORSMiddleware

    _origins_env = os.environ.get("LIVE_SSE_ALLOW_ORIGINS", "").strip()
    if _origins_env == "*":
        _allow_origins = ["*"]
    elif _origins_env:
        _allow_origins = [o.strip() for o in _origins_env.split(",") if o.strip()]
    else:
        # Default: the Streamlit dashboard on localhost/127.0.0.1 across the
        # common port range (DASHBOARD_PORT is configurable via --port).
        _dash_port = os.environ.get("DASHBOARD_PORT", "8501").strip() or "8501"
        _ports = {"8501", "8502", _dash_port}
        _allow_origins = [
            f"http://{host}:{port}"
            for host in ("localhost", "127.0.0.1")
            for port in _ports
        ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_allow_origins,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    async def health():
        return JSONResponse(
            {
                "status": "ok",
                "change_stream_connected": hub.change_stream_connected,
                "uptime_s": hub.uptime_s,
                "clients": hub.client_count,
            }
        )

    @app.get("/api/stream")
    async def stream():
        # sse-starlette cancels this generator when the client disconnects,
        # which fires the `finally` in sse_event_stream (REQ-E-006). No explicit
        # Request param — some starlette builds mis-analyze it as a query field.
        return EventSourceResponse(sse_event_stream(hub))

    # -- Mission Control HUD -------------------------------------------------
    # The static SPA (web/) and its bootstrap endpoint live on this server so
    # the page and the SSE stream are same-origin — no CORS to misconfigure
    # on the webinar hero screen.

    def _bootstrap_client():
        """Lazily create + cache one Mongo client for bootstrap queries."""
        if getattr(app.state, "boot_client", None) is None:
            resolved = uri
            if resolved is None:
                from scripts.common.mongo_uri import resolve_mongodb_uri

                resolved = resolve_mongodb_uri()
            if not resolved:
                return None
            try:
                from scripts.common.mongo import get_client

                app.state.boot_client = get_client(
                    resolved, app_name="streaming-agents-hud"
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("bootstrap client failed: %s", _redact(str(exc)))
                return None
        return app.state.boot_client

    @app.get("/api/bootstrap")
    def bootstrap():  # sync def → FastAPI runs it in the threadpool
        return JSONResponse(build_bootstrap_payload(_bootstrap_client()))

    app.state.boot_client = None

    from fastapi.staticfiles import StaticFiles

    if ASSETS_DIR.is_dir():
        app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")
    if WEB_DIR.is_dir():
        # Mounted last: API routes above win; everything else serves the HUD.
        app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="hud")

    return app


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Live SSE sidecar for the dashboard.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--no-serve-check",
        action="store_true",
        help="Resolve URI and exit (used by tests / preflight).",
    )
    args = parser.parse_args(argv)

    from scripts.common.mongo_uri import resolve_mongodb_uri

    uri = resolve_mongodb_uri()
    if not uri:
        print(
            "ERROR: could not resolve a MongoDB URI (.env / tfvars / "
            "$MONGODB_URI). Sidecar cannot start.",
            file=sys.stderr,
        )
        return 2

    if args.no_serve_check:
        return 0

    import uvicorn

    app = create_app(start_stream=True, uri=uri)
    print(
        f"Mission Control HUD on http://{args.host}:{args.port} "
        f"(stream: /api/stream, health: /api/health, bootstrap: /api/bootstrap)"
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
