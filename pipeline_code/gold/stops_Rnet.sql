CREATE OR REFRESH MATERIALIZED VIEW services.gold.stops_rnet
AS (
  SELECT * 
  FROM services.gold.fact_stops
  WHERE service_company = 'R-net'
);