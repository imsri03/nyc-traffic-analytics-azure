-- Databricks notebook source
-- traffic_kpis_gold
CREATE OR REPLACE VIEW gold.traffic_kpis_gold AS
SELECT 
  ROUND(AVG(current_speed), 2) AS avg_current_speed,
  CASE 
    WHEN AVG(current_speed) > 45 THEN 'Low'
    WHEN AVG(current_speed) BETWEEN 30 AND 45 THEN 'Medium'
    ELSE 'High'
  END AS congestion_level,
  COUNT(CASE WHEN road_closure = TRUE THEN 1 END) AS active_incidents,
  ROUND((AVG(current_travel_time) - AVG(free_flow_travel_time)) / 60) AS predicted_delay_mins
FROM silver.nyc_traffic_silver;


-- route_summary_gold
CREATE OR REPLACE VIEW gold.route_summary_gold AS
SELECT 
  CONCAT('ROUTE_', CAST(monotonically_increasing_id() AS STRING)) AS route_id,
  ROUND(RAND() * 5 + 1, 2) AS distance_miles,
  ROUND(current_travel_time / 60.0) AS predicted_time,
  CASE 
    WHEN current_speed > 45 THEN 'Low'
    WHEN current_speed BETWEEN 30 AND 45 THEN 'Medium'
    ELSE 'High'
  END AS congestion
FROM silver.nyc_traffic_silver;


-- congested_routes_gold
CREATE OR REPLACE VIEW gold.congested_routes_gold AS
SELECT 
  CONCAT('SEG_', CAST(ROUND(latitude, 4) AS STRING), '_', CAST(ROUND(longitude, 4) AS STRING)) AS road_name,
  ROUND(AVG(current_speed), 2) AS avg_speed
FROM silver.nyc_traffic_silver
GROUP BY 
  ROUND(latitude, 4), 
  ROUND(longitude, 4)
ORDER BY avg_speed ASC
LIMIT 5;


-- incident_log_gold
CREATE OR REPLACE VIEW gold.incident_log_gold AS
SELECT 
  current_timestamp() AS timestamp,
  CASE 
    WHEN road_closure = TRUE THEN 'Accident'
    WHEN confidence < 0.5 THEN 'Construction'
    ELSE 'Heavy Congestion'
  END AS incident_type,
  CONCAT('Lat:', ROUND(latitude, 5), ', Lon:', ROUND(longitude, 5)) AS location,
  CASE 
    WHEN road_closure = TRUE THEN 'Active'
    ELSE 'Resolved'
  END AS status
FROM silver.nyc_traffic_silver
WHERE road_closure = TRUE OR confidence < 0.8;


-- hourly_traffic_trends_gold
CREATE OR REPLACE VIEW gold.hourly_traffic_trends_gold AS
SELECT
    HOUR(ingestion_time) AS hour,
    ROUND(AVG(current_speed), 2) AS avg_speed
FROM silver.nyc_traffic_silver
GROUP BY HOUR(ingestion_time)
ORDER BY hour;


-- COMMAND ----------

SHOW VIEWS IN gold;
