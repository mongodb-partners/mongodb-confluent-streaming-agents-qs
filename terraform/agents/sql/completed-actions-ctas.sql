CREATE TABLE IF NOT EXISTS `{catalog}`.`{database}`.`completed_actions` (
    pickup_zone STRING,
    window_time TIMESTAMP(3),
    request_count BIGINT,
    anomaly_reason STRING,
    dispatch_summary STRING,
    dispatch_json STRING,
    api_response STRING
) WITH ('changelog.mode' = 'append')
