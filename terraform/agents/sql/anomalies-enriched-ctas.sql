CREATE TABLE IF NOT EXISTS `{catalog}`.`{database}`.`anomalies_enriched` (
    `pickup_zone` STRING,
    `window_time` TIMESTAMP(3),
    `request_count` BIGINT,
    `expected_requests` BIGINT,
    `anomaly_reason` STRING,
    `top_chunk_1` STRING,
    `top_chunk_2` STRING,
    `top_chunk_3` STRING
) WITH ('changelog.mode' = 'append')
