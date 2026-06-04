"""ASP processor topology — single source of truth for which Atlas Stream
Processing processors consume which Kafka topics.

When a Kafka topic is deleted and recreated (pipeline_reset.reset_pipeline,
deploy._ensure_flink_topics on stale-data purge, etc.), ASP processors
that consume that topic must be `:stop` + `:start`'d. Their consumer
group offsets point at the old topic generation; without restart they
remain `STARTED` but make zero progress — a particularly silent bug.

Processors that read from MongoDB change streams (not Kafka) — like
`event_knowledge_base_population` — are intentionally NOT in this map.
"""
from __future__ import annotations


KAFKA_SOURCE_PROCESSORS: dict[str, list[str]] = {
    "zone_traffic_sink":  ["zone_traffic_ingestion"],
    "anomalies_sink":     ["anomalies_ingestion"],
    "completed_actions":  ["dispatch_log_ingestion"],
}


def processors_for_topics(topics) -> list[str]:
    """Return the de-duplicated list of ASP processor names that consume any
    of the given Kafka topics. Topics not in the map are silently skipped."""
    result: list[str] = []
    seen: set[str] = set()
    for topic in topics:
        for proc in KAFKA_SOURCE_PROCESSORS.get(topic, []):
            if proc not in seen:
                result.append(proc)
                seen.add(proc)
    return result
