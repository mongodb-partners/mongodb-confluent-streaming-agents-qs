INSERT INTO `{catalog}`.`{database}`.`zone_traffic_sink`
SELECT
    pickup_zone AS zone,
    window_start,
    window_end,
    request_count,
    total_passengers,
    total_revenue
FROM `{catalog}`.`{database}`.`windowed_traffic`
