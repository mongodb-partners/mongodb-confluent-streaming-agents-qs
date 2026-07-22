"""Baseline heartbeat — `uv run nudge`.

Publishes a light trickle of baseline ride requests across all zones so
Flink's event-time watermark keeps advancing when no other traffic is
flowing. Without this, a `uv run surge` fired into an idle pipeline is
never emitted: the ride_requests table uses
``WATERMARK FOR request_ts AS request_ts - INTERVAL '5' SECOND`` and a
tumbling window only closes when a LATER record arrives to push the
watermark past the window boundary.

Two modes:

- One-shot (default): publish ``--per-zone`` records per zone stamped in
  the recent past. Use this to unstick a surge window that already fired.

      uv run nudge

- Loop: publish 1 record per zone every ``--interval`` seconds for
  ``--minutes`` minutes. Run this in a background terminal during a live
  demo so every surge window closes on schedule and the map/charts show
  gentle baseline activity.

      uv run nudge --minutes 75

Loop mode also accepts ``--heal``: every ~2 minutes the heartbeat checks
the best-effort RAG statement (anomalies-enriched-insert) and recreates
it if it has FAILED — the same remedy `uv run surge` applies right before
firing, but applied continuously. Why this exists: the statement's
per-anomaly VECTOR_SEARCH_AGG against the Atlas federated table times out
intermittently (Open Preview; client_timeout/retry_count are already
maxed usefully — see terraform/agents/sql/anomalies-enriched-insert.sql),
and a FAILED Flink statement never restarts itself. A recreate starts
from the latest offset, so it cannot choke on backlog. The check's fast
path (statement healthy) is a single 10s-capped HTTP GET; the heal itself
never raises, so the heartbeat's watermark duty is never at risk.

Volume math: loop mode at the default 20s interval is 3 records/zone/min,
consistent with the replay dataset's baseline (1-6/zone/min) and far below
any anomaly threshold, so the heartbeat itself can never register as a
surge.

Like `uv run surge`, this only produces to Kafka. It never touches
MongoDB and never deletes anything (spec INV-004).
"""
from __future__ import annotations

import argparse
import time
from typing import List, Optional

from scripts.generate_batch_data import _generate_record
from scripts.surge import (
    RAG_STATEMENT,
    SURGE_EVENT_ZONES,
    _now_ms,
    _publish,
    heal_rag_statement,
)

# Heal-watchdog cadence (loop mode with --heal). The fast path is one
# 10s-capped HTTP GET against the Flink REST API; 120s keeps that noise
# negligible while bounding recreate churn to at most once per check.
HEAL_CHECK_EVERY_S = 120.0


def build_baseline_records(now_ms: int, per_zone: int = 2) -> List[dict]:
    """Generate `per_zone` records for every zone, stamped within the last
    ~30 seconds (all in the past, so the watermark advances to ~now-5s
    immediately, closing any pending earlier windows)."""
    records: List[dict] = []
    for z_idx, zone in enumerate(SURGE_EVENT_ZONES):
        for i in range(per_zone):
            # Spread stamps over the last 30s with per-zone jitter so
            # records don't all collide on one millisecond.
            ts = now_ms - (i * 15_000 + z_idx * 1_733) - 1_000
            records.append(
                _generate_record(f"BASE-{now_ms}-{z_idx}-{i}", zone, ts)
            )
    return records


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Publish baseline ride requests so the Flink watermark keeps "
            "advancing (one-shot), or continuously during a demo (--minutes)."
        ),
    )
    parser.add_argument(
        "--minutes",
        type=float,
        default=0,
        help="Loop for this many minutes (default 0 = one-shot publish)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=20,
        help="Seconds between publishes in loop mode (default: 20)",
    )
    parser.add_argument(
        "--per-zone",
        type=int,
        default=2,
        help="Records per zone per publish (default: 2; loop mode uses 1)",
    )
    parser.add_argument("--topic", default="ride_requests", help="Kafka topic")
    parser.add_argument(
        "--heal",
        action="store_true",
        help=(
            "Loop mode only: every ~2 min, check the best-effort RAG "
            "statement (anomalies-enriched-insert) and recreate it if "
            "FAILED — same remedy `uv run surge` applies before firing."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate + count records without publishing",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    if args.minutes <= 0:
        if args.heal:
            print(
                "  [note] --heal only applies in loop mode (--minutes N); "
                "one-shot ignores it. (`uv run surge` heals before firing.)"
            )
        records = build_baseline_records(_now_ms(), per_zone=args.per_zone)
        print(
            f"  NUDGE — publishing {len(records)} baseline records "
            f"({args.per_zone}/zone x {len(SURGE_EVENT_ZONES)} zones) to "
            f"advance the watermark to ~now."
        )
        if args.dry_run:
            print(f"[DRY RUN] Generated {len(records)} records. Not publishing.")
            return 0
        return _publish(records, args.topic, args.verbose)

    deadline = time.time() + args.minutes * 60
    tick = 0
    per_zone = 1  # loop mode: 1/zone/tick; at 20s interval = 3/zone/min
    print(
        f"  HEARTBEAT — {per_zone}/zone every {args.interval:.0f}s for "
        f"{args.minutes:.0f} min ({len(SURGE_EVENT_ZONES)} zones). Ctrl-C to stop."
    )
    if args.heal:
        print(
            f"  HEAL WATCHDOG — checking '{RAG_STATEMENT}' every "
            f"{HEAL_CHECK_EVERY_S:.0f}s; recreates it only if FAILED."
        )
    if args.dry_run:
        print("[DRY RUN] Not publishing.")
        return 0
    rc = 0
    next_heal_check = 0.0  # first check on the first tick
    while time.time() < deadline:
        tick += 1
        records = build_baseline_records(_now_ms(), per_zone=per_zone)
        rc = _publish(records, args.topic, args.verbose)
        status = "ok" if rc == 0 else f"FAILED (rc={rc})"
        print(
            f"  tick {tick}: {len(records)} records {status} "
            f"({time.strftime('%H:%M:%S')})"
        )
        if rc != 0:
            # Loud but keep trying: a transient publish failure must not
            # silently end watermark advancement mid-demo.
            print("  [warn] publish failed; retrying next tick")
        if args.heal and time.time() >= next_heal_check:
            # The check interval doubles as the recreate cooldown: at most
            # one delete+submit per HEAL_CHECK_EVERY_S, and only when the
            # statement is actually FAILED (heal_rag_statement returns
            # immediately on RUNNING/PENDING and never raises).
            next_heal_check = time.time() + HEAL_CHECK_EVERY_S
            if not heal_rag_statement():
                print(
                    "  [warn] heal check could not confirm RAG statement "
                    "healthy; will re-check in "
                    f"{HEAL_CHECK_EVERY_S:.0f}s"
                )
        time.sleep(max(1.0, args.interval))
    print("  HEARTBEAT done.")
    return rc


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
