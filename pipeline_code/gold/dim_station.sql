CREATE OR REFRESH LIVE TABLE dim_station
COMMENT 'Station dimension with surrogate keys and business attributes'
TBLPROPERTIES (
  'quality' = 'gold'
)
AS
SELECT
  -- Surrogate key (using hash for reproducibility)
  CAST(abs(hash(station_code)) AS BIGINT) AS station_key,

  -- Natural key
  station_code,

  -- Business keys
  station_uic AS uic_code,

  -- Names
  station_name_short AS name_short,
  station_name_medium AS name_medium,
  station_name AS name_long,
  station_name,
  station_slug AS slug,

  -- Location
  station_country AS country_code,
  CASE station_country
    WHEN 'NL' THEN 'Netherlands'
    WHEN 'D' THEN 'Germany'
    WHEN 'B' THEN 'Belgium'
    WHEN 'F' THEN 'France'
    WHEN 'A' THEN 'Austria'
    WHEN 'CH' THEN 'Switzerland'
    WHEN 'GB' THEN 'United Kingdom'
    ELSE 'Other'
  END AS country_name,

  -- Geo coordinates
  station_lat AS latitude,
  station_lng AS longitude,

  -- Station classification
  station_type AS station_type_code,

  -- Station category (derived)
  CASE
    WHEN station_type = 'megastation' THEN 'Major Hub'
    WHEN station_type = 'knooppuntIntercitystation' THEN 'Intercity Junction'
    WHEN station_type = 'intercitystation' THEN 'Intercity'
    WHEN station_type = 'knooppuntSneltreinstation' THEN 'Fast Train Junction'
    WHEN station_type = 'sneltreinstation' THEN 'Fast Train'
    WHEN station_type = 'knooppuntStoptreinstation' THEN 'Local Junction'
    WHEN station_type = 'stoptreinstation' THEN 'Local'
    WHEN station_type = 'facultatiefStation' THEN 'Optional'
    ELSE 'Unknown'
  END AS station_category,

  -- Flags
  is_junction,
  is_intercity,
  is_major_hub,
  is_dutch,

  -- Hierarchy for roll-ups
  CASE
    WHEN is_major_hub THEN 1
    WHEN is_intercity AND is_junction THEN 2
    WHEN is_intercity THEN 3
    WHEN is_junction THEN 4
    ELSE 5
  END AS station_tier,

  -- SCD Type 1 metadata
  current_timestamp() AS _valid_from,
  bronze_ingest_timestamp AS _source_timestamp

FROM services.silver.station
WHERE station_code IS NOT NULL;