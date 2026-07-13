INSERT INTO `{catalog}`.`{database}`.`anomalies_per_zone`
WITH anomaly_detection AS (
    SELECT
        pickup_zone, window_time, request_count, total_passengers, total_revenue,
        ML_DETECT_ANOMALIES(
            CAST(request_count AS DOUBLE), window_time,
            JSON_OBJECT(
                'minTrainingSize' VALUE 15, 'maxTrainingSize' VALUE 7000,
                'confidencePercentage' VALUE 99.999, 'enableStl' VALUE FALSE
            )
        ) OVER (PARTITION BY pickup_zone ORDER BY window_time
                RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS anomaly_result
    FROM `{catalog}`.`{database}`.`windowed_traffic`
)
SELECT
    pickup_zone, window_time, request_count, total_passengers, total_revenue,
    CAST(ROUND(anomaly_result.forecast_value) AS BIGINT) AS expected_requests,
    anomaly_result.upper_bound, anomaly_result.lower_bound,
    anomaly_result.is_anomaly AS is_surge
FROM anomaly_detection
WHERE anomaly_result.is_anomaly = true
  AND request_count > anomaly_result.upper_bound
