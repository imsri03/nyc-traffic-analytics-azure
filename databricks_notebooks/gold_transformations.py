# Databricks notebook source
# Create gold database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS gold_db")

# Create Dimension: Location (rounded lat/lon groups)
spark.sql("""
CREATE OR REPLACE VIEW gold_db.dim_location AS
SELECT DISTINCT
  ROUND(latitude, 2) AS lat_zone,
  ROUND(longitude, 2) AS lon_zone
FROM silver.nyc_traffic_silver
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
""")


# COMMAND ----------

# Create Dimension: Road Attributes
spark.sql("""
CREATE OR REPLACE VIEW gold_db.dim_road AS
SELECT DISTINCT
  functional_road_class,
  road_closure
FROM silver.nyc_traffic_silver
""")


# COMMAND ----------

# Create Fact Table: Traffic Data
spark.sql("""
CREATE OR REPLACE VIEW gold_db.fact_traffic AS
SELECT
  current_speed,
  current_travel_time,
  free_flow_speed,
  free_flow_travel_time,
  confidence,
  functional_road_class,
  road_closure,
  latitude,
  longitude
FROM silver.nyc_traffic_silver
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
""")


# COMMAND ----------

