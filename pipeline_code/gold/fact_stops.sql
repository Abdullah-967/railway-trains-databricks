CREATE OR REFRESH LIVE TABLE fact_stops
COMMENT 'Stop-level facts. Grain: one stop at one station for one service.'
PARTITIONED BY (date_key)
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.zOrderCols' = 'station_key'
)
AS
SELECT
  -- Dimension keys
  CAST(date_format(s.service_date, 'yyyyMMdd') AS INT) AS date_key,
  COALESCE(st.station_key, -1) AS station_key,

  -- Degenerate dimensions (from service)
  s.service_id,
  s.stop_id,
  s.service_type,
  s.service_company,
  s.service_train_number,
  s.service_day_name,

  -- Time attributes
  s.stop_arrival_time,
  s.stop_departure_time,
  HOUR(COALESCE(s.stop_arrival_time, s.stop_departure_time)) AS hour_of_day,
  CASE
    WHEN HOUR(COALESCE(s.stop_arrival_time, s.stop_departure_time)) BETWEEN 6 AND 9 THEN 'Morning Rush'
    WHEN HOUR(COALESCE(s.stop_arrival_time, s.stop_departure_time)) BETWEEN 16 AND 19 THEN 'Evening Rush'
    WHEN HOUR(COALESCE(s.stop_arrival_time, s.stop_departure_time)) BETWEEN 10 AND 15 THEN 'Midday'
    WHEN HOUR(COALESCE(s.stop_arrival_time, s.stop_departure_time)) BETWEEN 20 AND 23 THEN 'Evening'
    ELSE 'Night'
  END AS time_period,

  -- Measures: Delays (already cast to INT in silver)
  s.stop_arrival_delay,
  s.stop_departure_delay,
  s.service_maximum_delay,
  COALESCE(s.stop_arrival_delay, 0) + COALESCE(s.stop_departure_delay, 0) AS total_delay_min,

  -- Measures: On-time flags (from silver derived columns)
  s.is_on_time_arrival,
  s.is_on_time_departure,

  -- Measures: Delay severity
  CASE
    WHEN s.stop_arrival_delay IS NULL OR s.stop_arrival_cancelled THEN 'N/A'
    WHEN s.stop_arrival_delay <= 0 THEN 'Early/On-time'
    WHEN s.stop_arrival_delay <= 5 THEN 'Minor (1-5 min)'
    WHEN s.stop_arrival_delay <= 15 THEN 'Moderate (6-15 min)'
    WHEN s.stop_arrival_delay <= 30 THEN 'Significant (16-30 min)'
    ELSE 'Severe (>30 min)'
  END AS arrival_delay_category,

  CASE
    WHEN s.stop_departure_delay IS NULL OR s.stop_departure_cancelled THEN 'N/A'
    WHEN s.stop_departure_delay <= 0 THEN 'Early/On-time'
    WHEN s.stop_departure_delay <= 5 THEN 'Minor (1-5 min)'
    WHEN s.stop_departure_delay <= 15 THEN 'Moderate (6-15 min)'
    WHEN s.stop_departure_delay <= 30 THEN 'Significant (16-30 min)'
    ELSE 'Severe (>30 min)'
  END AS departure_delay_category,

  -- Measures: Cancellation flags (already BOOLEAN in silver)
  s.stop_arrival_cancelled,
  s.stop_departure_cancelled,
  s.service_completely_cancelled,
  s.service_partly_cancelled,

  -- Measures: Platform
  s.stop_platform_changed,
  s.stop_planned_platform,
  s.stop_actual_platform,

  -- Station info (denormalized for convenience)
  s.stop_station_code,
  s.stop_station_name,

  -- Partition columns
  s.service_year,
  s.service_month,

  -- Metadata
  s.bronze_ingest_timestamp AS _source_timestamp,
  current_timestamp() AS _processed_at

FROM services.silver.services s
LEFT JOIN LIVE.dim_station st
  ON s.stop_station_code = st.station_code;