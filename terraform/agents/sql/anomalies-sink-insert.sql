INSERT INTO `{catalog}`.`{database}`.`anomalies_sink`
SELECT
    pickup_zone,
    window_time,
    request_count,
    expected_requests,
    anomaly_reason,
    top_chunk_1,
    top_chunk_2,
    top_chunk_3,
    CURRENT_TIMESTAMP AS detected_at
FROM `{catalog}`.`{database}`.`anomalies_enriched`
