CREATE OR REFRESH MATERIALIZED VIEW services.gold.stops_blauwnet
AS (
  SELECT * 
  FROM services.gold.fact_stops
  WHERE service_company = 'Blauwnet'
);