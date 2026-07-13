"""Deterministic Surge Director — `uv run surge`.

Publishes a concentrated surge of ride requests to the `ride_requests` Kafka
topic, with every event timestamp aligned to the CURRENT Flink tumbling
window. Into a pre-warmed pipeline this makes the real anomaly -> RAG ->
dispatch fire within ~1 window, on cue, every run — with zero faking: the
director only produces to Kafka and never writes to MongoDB (spec INV-004).
It reuses `generate_batch_data`'s record schema/Avro wire format and, for the
live path, `publish_data`'s credential + producer helpers (INV-006).

Design: `docs`/`specs/live-viz`. This module keeps a pure, side-effect-free
core (window math + record generation) so it is fully unit-testable without
Kafka or Atlas; the publish path is thin glue over publish_data.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import List, Optional

from scripts.generate_batch_data import (
    _encode_record,
    _generate_record,
)

# Baseline requests per window per zone (matches generate_batch_data).
BASELINE_REQUESTS_PER_WINDOW = 12
# Must match the Flink windowed_traffic TUMBLE interval (terraform/agents/main.tf)
# and dashboard.WINDOW_MINUTES. If surge aligns to a wider window than Flink uses,
# the burst spreads across multiple Flink windows and dilutes below the anomaly
# threshold. Shortened 5→1 alongside the window change.
DEFAULT_WINDOW_MIN = 1
DEFAULT_MULTIPLIER = 10

# Zones that have seeded knowledge-base events (good surge targets). The
# default rotates deterministically by window so repeated demos vary the zone
# without an RNG seed dependency.
SURGE_EVENT_ZONES = [
    "French Quarter",
    "Central Business District (CBD)",
    "Bywater",
    "Marigny",
    "Uptown",
    "Garden District",
    "Warehouse District",
]


def current_window_bounds(now_ms: int, window_min: int = DEFAULT_WINDOW_MIN):
    """Return [start_ms, end_ms) of the tumbling window that contains now_ms.

    Windows are aligned to the epoch (start % window == 0), matching Flink's
    default tumbling-window alignment.
    """
    window_ms = window_min * 60 * 1000
    start = (now_ms // window_ms) * window_ms
    return start, start + window_ms


def default_zone_for(now_ms: int, window_min: int = DEFAULT_WINDOW_MIN) -> str:
    """Pick a deterministic surge zone based on the current window index."""
    window_ms = window_min * 60 * 1000
    idx = (now_ms // window_ms) % len(SURGE_EVENT_ZONES)
    return SURGE_EVENT_ZONES[idx]


def build_surge_records(
    zone: str,
    multiplier: int,
    now_ms: int,
    window_min: int = DEFAULT_WINDOW_MIN,
    baseline: int = BASELINE_REQUESTS_PER_WINDOW,
) -> List[dict]:
    """Generate `baseline * multiplier` ride-request records for `zone`, all
    timestamped inside the current tumbling window.

    Timestamps are placed in the first ~90% of the window so the surge lands
    before the window closes (leaving margin for clock skew / publish time).
    """
    start, end = current_window_bounds(now_ms, window_min)
    window_ms = end - start
    # Leave the last 10% of the window as publish/skew margin.
    usable_ms = max(1, int(window_ms * 0.9))
    count = baseline * multiplier
    records: List[dict] = []
    for i in range(count):
        ts = start + random.randint(0, usable_ms - 1)
        records.append(_generate_record(f"SURGE-{now_ms}-{i}", zone, ts))
    return records


def write_surge_jsonl(records: List[dict], path: Path) -> int:
    """Write records to a base64-Avro JSONL file consumable by publish_data.

    Returns the number of lines written.
    """
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for i, record in enumerate(records):
            line = json.dumps(
                {
                    "key": None,
                    "value": _encode_record(record),
                    "partition": i % 6,
                    "offset": i,
                }
            )
            f.write(line + "\n")
            n += 1
    return n


def _narrate(
    zone: str, multiplier: int, now_ms: int, window_min: int, count: int
) -> None:
    """Print presenter-facing narration + expected-anomaly countdown."""
    start, end = current_window_bounds(now_ms, window_min)
    secs_to_close = max(0, (end - now_ms) // 1000)
    print("=" * 64)
    print(f"  SURGE DIRECTOR — {zone}  ({multiplier}x baseline, {count} requests)")
    print(f"  Window: [{start} .. {end})  (aligned to current {window_min}-min window)")
    print(f"  Window closes in ~{secs_to_close}s — watch for the anomaly then.")
    print(f"  Expected: anomaly -> RAG explanation -> agent dispatch for {zone}.")
    print("=" * 64)


def _now_ms() -> int:
    return int(time.time() * 1000)


RAG_STATEMENT = "anomalies-enriched-insert"


def heal_rag_statement() -> bool:
    """Best-effort: recreate the RAG enrichment statement if it is FAILED.

    anomalies-enriched-insert dies on vector-search timeouts under bursty
    load ("Max retries exceeded") and NOTHING auto-heals it afterwards —
    it is best-effort/off the critical path by design. But surge exists to
    demo the FULL loop, including the LLM explanation + Vector Search
    evidence chunks that this statement produces, so check it right before
    firing. A freshly recreated statement starts from the latest offset,
    which is exactly right: it enriches the surge we are about to publish
    instead of choking on the anomaly backlog.

    Returns True if the statement is (now) healthy; False is non-fatal —
    the surge still runs, only the RAG overlay is at risk.
    """
    try:
        from pathlib import Path

        from scripts.pipeline_reset import (
            _delete_flink_statement,
            _get_flink_credentials,
            _get_terraform_outputs,
            _submit_flink_statement,
        )

        root = Path(__file__).resolve().parents[1]
        outputs = _get_terraform_outputs(root)
        flink = _get_flink_credentials(outputs) if outputs else None
        if not flink:
            return False

        import base64
        import json as _json
        import urllib.request

        auth = base64.b64encode(
            f"{flink['api_key']}:{flink['api_secret']}".encode()
        ).decode()
        req = urllib.request.Request(
            f"{flink['base_url']}/{RAG_STATEMENT}",
            headers={"Authorization": f"Basic {auth}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                phase = _json.loads(r.read()).get("status", {}).get("phase", "")
        except Exception:
            phase = "UNKNOWN"

        if phase in ("RUNNING", "PENDING"):
            return True

        print(
            f"  RAG statement '{RAG_STATEMENT}' is {phase} — recreating so "
            "the surge gets its LLM explanation + evidence chunks..."
        )
        _delete_flink_statement(RAG_STATEMENT, flink)
        time.sleep(10)
        sql_dir = root / "terraform" / "agents" / "sql"
        ok = _submit_flink_statement(RAG_STATEMENT, flink, sql_dir)
        print(f"  RAG statement recreate {'succeeded' if ok else 'FAILED'}.")
        return bool(ok)
    except Exception as e:  # never block the surge on the RAG leg
        print(f"  [warn] RAG statement check skipped: {e}")
        return False


def _publish(records: List[dict], topic: str, verbose: bool) -> int:
    """Publish records to Kafka by reusing publish_data's producer + creds.

    Returns process-style exit code (0 == success). Never touches MongoDB.
    """
    import tempfile

    # Import lazily so --dry-run works without Kafka libs / terraform state.
    from scripts import publish_data as pd

    logger = pd.setup_logging(verbose)

    try:
        project_root = pd.get_project_root()
        pd.validate_terraform_state(project_root)
        credentials = pd.extract_kafka_credentials(project_root)
    except Exception as e:  # mirror publish_data's clear diagnostics (REQ-E-034)
        logger.error(f"Surge director could not load Kafka credentials: {e}")
        return 1

    with tempfile.NamedTemporaryFile(
        "w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as tf:
        tmp_path = Path(tf.name)
    written = write_surge_jsonl(records, tmp_path)
    logger.info(f"Wrote {written} surge records to {tmp_path}")

    # Look up the CURRENT registered schema ID and rewrite the wire-header ID
    # in each surge record. generate_batch_data._encode_record stamps a
    # hardcoded SCHEMA_ID (100008) that rarely matches the schema Schema
    # Registry actually assigned on this deploy — without the rewrite Flink
    # cannot deserialize the records and zone-traffic-sink-insert /
    # anomaly-detection-insert FAIL with "schema mismatch". publish_data's CLI
    # does this rewrite; surge must too (it was the one publish path that
    # skipped it).
    target_schema_id = None
    sr_endpoint = credentials.get("schema_registry_url")
    sr_key = credentials.get("schema_registry_api_key")
    sr_secret = credentials.get("schema_registry_api_secret")
    if sr_endpoint and sr_key and sr_secret:
        subject = f"{topic}-value"
        target_schema_id = pd._get_current_schema_id(
            sr_endpoint, sr_key, sr_secret, subject
        )
        if target_schema_id is not None:
            logger.info(f"Current schema ID for '{subject}': {target_schema_id}")
        else:
            logger.warning(
                f"Could not look up schema ID for '{subject}' — surge records "
                "will keep their original (possibly stale) schema ID"
            )

    publisher = pd.DataPublisher(
        bootstrap_servers=credentials["bootstrap_servers"],
        kafka_api_key=credentials["kafka_api_key"],
        kafka_api_secret=credentials["kafka_api_secret"],
        target_schema_id=target_schema_id,
    )
    try:
        results = publisher.publish_jsonl_file(tmp_path, topic)
    finally:
        publisher.close()
        try:
            tmp_path.unlink()
        except OSError:
            pass

    failed = results.get("failed") or 0
    success = results.get("success") or 0
    if failed:
        # A partial failure during a live demo must be loud and must exit
        # non-zero — a half-published surge is NOT a clean run (REQ-E-033).
        logger.error(f"Surge publish INCOMPLETE: {success} ok, {failed} FAILED")
        return 1
    return 0 if success else 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish a deterministic, window-aligned demand surge to Kafka.",
    )
    parser.add_argument(
        "--zone",
        default=None,
        help="Surge zone (default: deterministic by current window)",
    )
    parser.add_argument(
        "--multiplier",
        type=int,
        default=DEFAULT_MULTIPLIER,
        help="Surge intensity vs baseline (default: 10)",
    )
    parser.add_argument(
        "--window-min",
        type=int,
        default=DEFAULT_WINDOW_MIN,
        help=f"Flink tumbling-window size in minutes (default: {DEFAULT_WINDOW_MIN})",
    )
    parser.add_argument("--topic", default="ride_requests", help="Kafka topic")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate + validate the batch without publishing",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    # Heal the RAG leg BEFORE aligning to the window: the recreate takes
    # ~15-40s, and doing it first keeps the surge inside a single window.
    # Never under pytest — the surge test-suite calls main() with the
    # publish layer mocked, and this would issue REAL Flink deletes.
    if not args.dry_run and "PYTEST_CURRENT_TEST" not in os.environ:
        heal_rag_statement()

    now_ms = _now_ms()
    zone = args.zone or default_zone_for(now_ms, args.window_min)
    records = build_surge_records(
        zone=zone,
        multiplier=args.multiplier,
        now_ms=now_ms,
        window_min=args.window_min,
    )
    _narrate(zone, args.multiplier, now_ms, args.window_min, len(records))

    if args.dry_run:
        print(
            f"[DRY RUN] Generated {len(records)} records for '{zone}'. "
            f"Not publishing."
        )
        return 0

    return _publish(records, args.topic, args.verbose)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
