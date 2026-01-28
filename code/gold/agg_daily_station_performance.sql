CREATE OR REFRESH LIVE TABLE agg_daily_station_performance
COMMENT 'Daily station performance metrics. Grain: one station per day.'
PARTITIONED BY (year_month)
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.zOrderCols' = 'station_key'
)
AS
SELECT
  -- Keys
  f.date_key,
  f.station_key,

  -- Partition column
  CONCAT(d.year, '-', LPAD(d.month, 2, '0')) AS year_month,

  -- Date attributes (denormalized for easier querying)
  d.full_date,
  d.year,
  d.month,
  d.day_name,
  d.is_weekend,

  -- Station attributes (denormalized)
  s.station_code,
  s.station_name,
  s.station_category,
  s.is_major_hub,

  -- Volume metrics
  COUNT(*) AS total_stops,
  COUNT(DISTINCT f.service_id) AS unique_services,

  -- Arrival metrics
  COUNT(f.stop_arrival_time) AS arrival_count,
  SUM(CASE WHEN f.is_on_time_arrival = TRUE THEN 1 ELSE 0 END) AS on_time_arrivals,
  SUM(CASE WHEN f.is_on_time_arrival = FALSE THEN 1 ELSE 0 END) AS delayed_arrivals,
  SUM(CASE WHEN f.stop_arrival_cancelled = TRUE THEN 1 ELSE 0 END) AS cancelled_arrivals,
  ROUND(AVG(f.stop_arrival_delay), 2) AS avg_arrival_delay_min,
  MAX(f.stop_arrival_delay) AS max_arrival_delay_min,
  PERCENTILE_APPROX(f.stop_arrival_delay, 0.5) AS median_arrival_delay_min,
  PERCENTILE_APPROX(f.stop_arrival_delay, 0.95) AS p95_arrival_delay_min,

  -- Departure metrics
  COUNT(f.stop_departure_time) AS departure_count,
  SUM(CASE WHEN f.is_on_time_departure = TRUE THEN 1 ELSE 0 END) AS on_time_departures,
  SUM(CASE WHEN f.is_on_time_departure = FALSE THEN 1 ELSE 0 END) AS delayed_departures,
  SUM(CASE WHEN f.stop_departure_cancelled = TRUE THEN 1 ELSE 0 END) AS cancelled_departures,
  ROUND(AVG(f.stop_departure_delay), 2) AS avg_departure_delay_min,
  MAX(f.stop_departure_delay) AS max_departure_delay_min,
  PERCENTILE_APPROX(f.stop_departure_delay, 0.5) AS median_departure_delay_min,
  PERCENTILE_APPROX(f.stop_departure_delay, 0.95) AS p95_departure_delay_min,

  -- Platform changes
  SUM(CASE WHEN f.stop_platform_changed = TRUE THEN 1 ELSE 0 END) AS platform_changes,

  -- Calculated KPIs
  ROUND(
    SUM(CASE WHEN f.is_on_time_arrival = TRUE THEN 1 ELSE 0 END) * 100.0 /
    NULLIF(COUNT(f.stop_arrival_time) - SUM(CASE WHEN f.stop_arrival_cancelled THEN 1 ELSE 0 END), 0),
    2
  ) AS arrival_on_time_pct,

  ROUND(
    SUM(CASE WHEN f.is_on_time_departure = TRUE THEN 1 ELSE 0 END) * 100.0 /
    NULLIF(COUNT(f.stop_departure_time) - SUM(CASE WHEN f.stop_departure_cancelled THEN 1 ELSE 0 END), 0),
    2
  ) AS departure_on_time_pct,

  ROUND(
    SUM(CASE WHEN f.stop_arrival_cancelled OR f.stop_departure_cancelled THEN 1 ELSE 0 END) * 100.0 /
    NULLIF(COUNT(*), 0),
    2
  ) AS cancellation_rate_pct,

  ROUND(
    SUM(CASE WHEN f.stop_platform_changed = TRUE THEN 1 ELSE 0 END) * 100.0 /
    NULLIF(COUNT(*), 0),
    2
  ) AS platform_change_rate_pct,

  -- Metadata
  current_timestamp() AS _processed_at

FROM LIVE.fact_stops f
INNER JOIN LIVE.dim_date d ON f.date_key = d.date_key
INNER JOIN LIVE.dim_station s ON f.station_key = s.station_key
WHERE s.is_dutch = TRUE
GROUP BY
  f.date_key,
  f.station_key,
  d.full_date,
  d.year,
  d.month,
  d.day_name,
  d.is_weekend,
  s.station_code,
  s.station_name,
  s.station_category,
  s.is_major_hub;