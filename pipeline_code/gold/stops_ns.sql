CREATE OR REFRESH MATERIALIZED VIEW services.gold.fact_stops_ns
AS (
  SELECT * 
  FROM services.gold.fact_stops
  WHERE service_company = 'NS'
);