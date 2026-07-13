"""Single source of truth for Kafka topic + MongoDB collection groupings.

deploy, destroy, pipeline_reset, health, and asp_setup each hand-maintained
their own topic / collection lists. The lists are NOT identical — they capture
different concerns — so this module models them as NAMED GROUPS rather than one
flat list, and derives the callers' views from those groups. This keeps the
memberships explicit and prevents silent drift when the pipeline changes.

Groups
------
- ``INPUT_TOPICS``      — pipeline ingress (produced into by publish_data / ShadowTraffic).
- ``STREAMING_TOPICS``  — Flink-owned intermediate/output topics that are SAFE to
                          delete+recreate (they are continuously repopulated).
- ``CTAS_TOPICS``       — created BY their CTAS DDL; must NOT be pre-created, but
                          ARE deleted on a full teardown.
- ``EVENT_TOPICS``      — knowledge-base / event-document ingress used by ASP.
"""

from __future__ import annotations

# Pipeline ingress.
INPUT_TOPICS: tuple[str, ...] = ("ride_requests",)

# Flink-owned streaming intermediates/outputs (safe to purge + recreate).
STREAMING_TOPICS: tuple[str, ...] = (
    "windowed_traffic",
    "anomalies_per_zone",
    "zone_traffic_sink",
    "anomalies_sink",
)

# Created by CTAS DDL — never pre-created, but removed on full teardown.
CTAS_TOPICS: tuple[str, ...] = (
    "anomalies_enriched",
    "completed_actions",
)

# Event / knowledge-base ingress topics used by ASP setup.
EVENT_TOPICS: tuple[str, ...] = ("event_documents",)


# ── Derived views (what each caller needs) ──────────────────────────────────

# reset_pipeline deletes + recreates these. Excludes CTAS topics (their CTAS
# owns their lifecycle) and event topics (not part of the streaming reset).
RESET_TOPICS: tuple[str, ...] = INPUT_TOPICS + STREAMING_TOPICS

# A full destroy removes every pipeline topic across all groups.
ALL_PIPELINE_TOPICS: tuple[str, ...] = (
    INPUT_TOPICS + STREAMING_TOPICS + CTAS_TOPICS + EVENT_TOPICS
)

# health monitors ingress + streaming + CTAS outputs (not event ingress).
HEALTH_TOPICS: tuple[str, ...] = INPUT_TOPICS + STREAMING_TOPICS + CTAS_TOPICS


# ── MongoDB collections ─────────────────────────────────────────────────────

# Sink collections written by ASP processors (cleared on a streaming reset).
MONGODB_SINK_COLLECTIONS: tuple[tuple[str, str], ...] = (
    ("analytics", "zone_traffic"),
    ("analytics", "zone_anomalies"),
    ("fleet", "dispatch_log"),
)

# All pipeline collections (dropped on a full teardown), including seeded /
# reference collections and dead-letter queues.
ALL_MONGODB_COLLECTIONS: tuple[tuple[str, str], ...] = MONGODB_SINK_COLLECTIONS + (
    ("fleet", "vessel_catalog"),
    ("fleet", "validation_dlq"),
    ("events", "knowledge_base"),
    ("events", "calendar"),
    ("events", "validation_dlq"),
)
