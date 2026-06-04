INSERT INTO `{catalog}`.`{database}`.`completed_actions`
SELECT
    pickup_zone,
    window_time,
    request_count,
    CONCAT('Surge detected: ', CAST(request_count AS STRING), ' requests (expected ', CAST(expected_requests AS STRING), ')') AS anomaly_reason,
    COALESCE(
        TRIM(REGEXP_EXTRACT(CAST(response AS STRING), 'Dispatch Summary[:\s]*\n(.+?)(?=\n\n(?:Dispatch JSON|$))', 1)),
        CAST(response AS STRING)
    ) AS dispatch_summary,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), 'Dispatch JSON[:\s]*\n(?:```(?:json)?\s*)?([\\s\\S]+?)(?:```)?(?=\n\n(?:API Response|$))', 1)) AS dispatch_json,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), 'API Response[:\s]*\n(?:```(?:json)?\s*)?([\\s\\S]+?)(?:```)?\\s*$', 1)) AS api_response
FROM `{catalog}`.`{database}`.`anomalies_per_zone`,
LATERAL TABLE(AI_RUN_AGENT(
    `boat_dispatch_agent`,
    CONCAT('Demand surge in ', pickup_zone, ': ', CAST(request_count AS STRING), ' ride requests in 5 minutes (expected ', CAST(expected_requests AS STRING), '). Surge ratio: ', COALESCE(CAST(ROUND(CAST(request_count AS DOUBLE) / NULLIF(CAST(expected_requests AS DOUBLE), 0), 1) AS STRING), 'N/A'), 'x'),
    `pickup_zone`
))
WHERE is_surge = true
