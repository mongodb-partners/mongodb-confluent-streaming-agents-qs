"""Restart Atlas Stream Processing processors after Kafka topic recreation.

Every code path that deletes + recreates a Kafka topic consumed by an ASP
processor MUST call `restart_processors_for_topics()` afterwards. Without
this, ASP consumer-group offsets point at non-existent positions on the
old topic generation, and the processor sits in `STARTED` state forever
without consuming.

This module is the canonical recovery: stop, wait for STOPPED, start, wait
for STARTED. All errors are logged and swallowed — the broader deploy or
reset flow must not fail because Atlas was momentarily unreachable.
"""
from __future__ import annotations

import logging
import time
from typing import Iterable

import requests

from .asp_topology import processors_for_topics

logger = logging.getLogger(__name__)

ATLAS_API = "https://cloud.mongodb.com/api/atlas/v2"
ACCEPT_HEADER = "application/vnd.atlas.2024-05-30+json"

_TERMINAL_STOPPED = {"STOPPED", "FAILED"}
_RUNNING = {"STARTED"}

# Atlas returns this when a :start races a still-
# finalizing :stop ("another operation FinishStopStreamProcessor-... has
# the lock"). The fix is to retry :start after a short backoff rather
# than leaving the processor STOPPING.
_LOCK_CONFLICT_MARKER = "has the lock"
_START_LOCK_RETRIES = 4
_START_LOCK_BACKOFF_S = 5


def _is_lock_conflict(status_code: int, body: str) -> bool:
    """True iff this is the Atlas 'operation has the lock' 4xx conflict.

    A 2xx is never a conflict regardless of body content.
    """
    if status_code < 400:
        return False
    return _LOCK_CONFLICT_MARKER in (body or "")


def _processors_endpoint(project_id: str, instance: str) -> str:
    return f"{ATLAS_API}/groups/{project_id}/streams/{instance}/processors"


def _processor_endpoint(project_id: str, instance: str, name: str) -> str:
    return f"{ATLAS_API}/groups/{project_id}/streams/{instance}/processor/{name}"


def _list_processors(project_id, instance, auth, request_timeout=30):
    url = _processors_endpoint(project_id, instance)
    headers = {"Accept": ACCEPT_HEADER}
    try:
        r = requests.get(url, auth=auth, headers=headers, timeout=request_timeout)
        if r.status_code != 200:
            logger.warning("ASP list-processors HTTP %s", r.status_code)
            return {}
        return {p.get("name"): p.get("state") for p in r.json().get("results", [])}
    except requests.RequestException as exc:
        logger.warning("ASP list-processors failed: %s", exc)
        return {}


def _send_action(project_id, instance, name, action, auth, request_timeout=60):
    """POST :stop or :start. Returns (ok, status_code, body).

    
    - request_timeout lowered 120 → 60. A 120s single-request timeout
      let a hung :stop block the whole reset; the async completion is
      handled by the state-polling loop, so the POST itself needn't
      wait that long.
    - Returns the status code + body (not just a bool) so callers can
      detect the 'has the lock' conflict and retry :start.
    """
    url = f"{_processor_endpoint(project_id, instance, name)}:{action}"
    headers = {"Accept": ACCEPT_HEADER, "Content-Type": "application/json"}
    try:
        r = requests.post(url, auth=auth, headers=headers, timeout=request_timeout)
        body = r.text[:300] if hasattr(r, "text") else ""
        if r.status_code >= 400:
            logger.warning("ASP %s %s HTTP %s: %s",
                           action, name, r.status_code, body[:200])
            return False, r.status_code, body
        return True, r.status_code, body
    except requests.RequestException as exc:
        logger.warning("ASP %s %s failed: %s", action, name, exc)
        return False, None, str(exc)


def _start_with_lock_retry(project_id, instance, name, auth):
    """Issue :start, retrying on the 'has the lock' conflict.

    When :start races a still-finalizing :stop, Atlas returns a 4xx with
    'another operation ... has the lock'. The stop completes within a few
    seconds, so retrying :start a handful of times with a short backoff
    resolves it — rather than leaving the processor stuck STOPPING.
    """
    for attempt in range(_START_LOCK_RETRIES):
        ok, code, body = _send_action(project_id, instance, name, "start", auth)
        if ok:
            return True
        if _is_lock_conflict(code or 0, body):
            logger.info(
                "  ASP start %s: lock conflict (stop still finalizing), "
                "retry %d/%d in %ds",
                name, attempt + 1, _START_LOCK_RETRIES, _START_LOCK_BACKOFF_S,
            )
            time.sleep(_START_LOCK_BACKOFF_S)
            continue
        # Non-lock failure → don't keep retrying
        return False
    logger.warning(
        "  ASP start %s: still lock-conflicted after %d retries",
        name, _START_LOCK_RETRIES,
    )
    return False


def _wait_for_state(project_id, instance, names, target_states, auth,
                    timeout_s, poll_interval_s):
    """Poll until every name in `names` reaches one of `target_states`,
    or the per-processor timeout elapses. Returns the final {name: state}
    snapshot. Best-effort; never raises."""
    deadline = time.time() + timeout_s
    states: dict[str, str] = {}
    while time.time() < deadline:
        snapshot = _list_processors(project_id, instance, auth)
        states = {n: snapshot.get(n, "UNKNOWN") for n in names}
        if all(s in target_states for s in states.values()):
            return states
        if poll_interval_s <= 0:
            # Test path: short-circuit after one poll
            return states
        time.sleep(poll_interval_s)
    return states


def restart_processors_for_topics(
    project_id: str,
    instance: str,
    topics: Iterable[str],
    auth,
    *,
    timeout_per_processor: int = 60,
    poll_interval_s: int = 5,
) -> dict[str, str]:
    """Stop and start ASP processors that consume any of `topics`.

    Returns a dict {processor_name: final_state}. Best-effort: catches all
    network errors and HTTP errors, logging warnings. Never raises so the
    broader deploy/reset flow can continue.

    Args:
        project_id: Atlas project ID.
        instance: ASP instance name (e.g. "asp-instance").
        topics: Kafka topics that were just recreated.
        auth: requests-compatible auth (HTTPDigestAuth).
        timeout_per_processor: seconds to wait for STOPPED, then STARTED.
        poll_interval_s: seconds between state-poll requests; pass 0 for
            tests to avoid sleeping.
    """
    procs = processors_for_topics(topics)
    if not procs:
        return {}

    logger.info("Restarting ASP processors %s after topic recreation: %s",
                procs, list(topics))

    # Step 1: stop all (best-effort; FAILED processors don't need stopping)
    snapshot = _list_processors(project_id, instance, auth)
    for name in procs:
        state = snapshot.get(name, "UNKNOWN")
        if state in _TERMINAL_STOPPED:
            continue
        _send_action(project_id, instance, name, "stop", auth)  # (ok, code, body) — best-effort

    # Step 2: wait until everything is STOPPED or FAILED
    _wait_for_state(
        project_id, instance, procs, _TERMINAL_STOPPED, auth,
        timeout_s=timeout_per_processor,
        poll_interval_s=poll_interval_s,
    )

    # Step 3: start all, retrying on the 'has the lock' conflict
    for name in procs:
        _start_with_lock_retry(project_id, instance, name, auth)

    # Step 4: wait for STARTED **or** FAILED — both are terminal. The
    # wait predicate is "all processors in target_states", so if FAILED
    # were excluded the loop would block until the full timeout
    # expired on every FAILED processor. still
    # holds: we DO surface FAILED below — we just don't conflate it
    # with success during the wait.
    #
    # An earlier fix removed FAILED from
    # this set, producing a 60s stall per FAILED processor.
    final = _wait_for_state(
        project_id, instance, procs, _RUNNING | {"FAILED"}, auth,
        timeout_s=timeout_per_processor,
        poll_interval_s=poll_interval_s,
    )

    failed = [n for n, s in final.items() if s == "FAILED"]
    for name, state in final.items():
        if state == "FAILED":
            logger.warning("  ASP %s -> FAILED (will not auto-recover)", name)
        else:
            logger.info("  ASP %s -> %s", name, state)
    if failed:
        logger.warning(
            "ASP restart left %d processor(s) in FAILED state: %s. "
            "Investigate via Atlas UI Stream Processing.",
            len(failed), failed,
        )
    return final
