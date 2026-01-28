CREATE OR REFRESH LIVE TABLE dim_date
COMMENT 'Date dimension for delay analytics (2022-2025)'
TBLPROPERTIES (
  'quality' = 'gold'
)
AS
SELECT
  -- Surrogate key (YYYYMMDD format)
  CAST(date_format(date_value, 'yyyyMMdd') AS INT) AS date_key,

  -- Date attributes
  date_value AS full_date,
  year(date_value) AS year,
  quarter(date_value) AS quarter,
  month(date_value) AS month,
  date_format(date_value, 'MMMM') AS month_name,
  weekofyear(date_value) AS week_of_year,
  dayofmonth(date_value) AS day_of_month,
  dayofweek(date_value) AS day_of_week,
  date_format(date_value, 'EEEE') AS day_name,
  dayofyear(date_value) AS day_of_year,

  -- Flags
  CASE WHEN dayofweek(date_value) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  CASE WHEN dayofweek(date_value) NOT IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekday,

  -- Dutch holidays (simplified - major holidays)
  CASE
    WHEN month(date_value) = 1 AND dayofmonth(date_value) = 1 THEN TRUE  -- New Year
    WHEN month(date_value) = 4 AND dayofmonth(date_value) = 27 THEN TRUE -- King's Day
    WHEN month(date_value) = 5 AND dayofmonth(date_value) = 5 THEN TRUE  -- Liberation Day
    WHEN month(date_value) = 12 AND dayofmonth(date_value) IN (25, 26) THEN TRUE -- Christmas
    ELSE FALSE
  END AS is_dutch_holiday,

  -- Period identifiers
  CONCAT(year(date_value), '-Q', quarter(date_value)) AS year_quarter,
  CONCAT(year(date_value), '-', LPAD(month(date_value), 2, '0')) AS year_month,
  CONCAT(year(date_value), '-W', LPAD(weekofyear(date_value), 2, '0')) AS year_week

FROM (
  SELECT explode(sequence(
    DATE '2022-01-01',
    DATE '2025-12-31',
    INTERVAL 1 DAY
  )) AS date_value
);