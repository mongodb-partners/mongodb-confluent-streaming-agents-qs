-- Anomaly sink — reads DIRECTLY from anomalies_per_zone (detection output),
-- NOT from anomalies_enriched.
--
-- Why: the RAG enrichment path (anomalies_enriched, fed by
-- anomalies-enriched-insert) does a per-anomaly VECTOR_SEARCH_AGG against the
-- Atlas Mongo federated table. That federated search reliably times out inside
-- Flink under streaming load ("Search table request timed out: Max retries
-- exceeded") even with client_timeout/retry_count raised — so enriched-insert
-- FAILS whenever a real anomaly arrives and NOTHING reaches the sink. That made
-- the dashboard's "Anomalies Detected" card permanently 0 despite detection
-- working. Reading detection output directly guarantees anomalies reach
-- analytics.zone_anomalies. The RAG explanation (anomaly_reason + top_chunk_*)
-- is now best-effort/optional and off the critical path.
--
-- anomaly_reason is synthesized from the detection numbers so the anomaly cards
-- still show a human-readable "why" (surge magnitude vs expected). top_chunk_*
-- are empty on this path; when the RAG path completes, the ASP processor
-- anomalies_enriched_ingestion merges the LLM reason + top_chunk_* onto the
-- same zone_anomalies document (keyed by pickup_zone + window_time).
INSERT INTO `{catalog}`.`{database}`.`anomalies_sink`
SELECT
    pickup_zone,
    window_time,
    request_count,
    expected_requests,
    CONCAT(
        'Surge detected in ', pickup_zone, ': ',
        CAST(request_count AS STRING), ' ride requests vs expected ',
        CAST(expected_requests AS STRING),
        COALESCE(
            CONCAT(
                ' (',
                CAST(ROUND(
                    CAST(request_count AS DOUBLE)
                    / NULLIF(CAST(expected_requests AS DOUBLE), 0), 1
                ) AS STRING),
                'x baseline)'
            ),
            ''
        )
    ) AS anomaly_reason,
    CAST(NULL AS STRING) AS top_chunk_1,
    CAST(NULL AS STRING) AS top_chunk_2,
    CAST(NULL AS STRING) AS top_chunk_3,
    CURRENT_TIMESTAMP AS detected_at
FROM `{catalog}`.`{database}`.`anomalies_per_zone`
WHERE is_surge = true
