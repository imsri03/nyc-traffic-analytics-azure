# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, regexp_replace, explode, expr, floor, rand
from pyspark.sql.types import *

# Step 1: Read raw data
bronze_path = "/mnt/bronze/ingested"
raw_df = spark.read.text(bronze_path)

# Step 2: Fix JSON
cleaned_df = (
    raw_df.withColumn("json_fixed", regexp_replace("value", "'", '"'))
          .withColumn("json_fixed", regexp_replace("json_fixed", "\\bTrue\\b", "true"))
          .withColumn("json_fixed", regexp_replace("json_fixed", "\\bFalse\\b", "false"))
)

# Step 3: Schema
schema = StructType([
    StructField("flowSegmentData", StructType([
        StructField("frc", StringType()),
        StructField("currentSpeed", IntegerType()),
        StructField("freeFlowSpeed", IntegerType()),
        StructField("currentTravelTime", IntegerType()),
        StructField("freeFlowTravelTime", IntegerType()),
        StructField("confidence", IntegerType()),
        StructField("roadClosure", BooleanType()),
        StructField("coordinates", StructType([
            StructField("coordinate", ArrayType(
                StructType([
                    StructField("latitude", DoubleType()),
                    StructField("longitude", DoubleType())
                ])
            ))
        ]))
    ]))
])

# Step 4: Parse JSON
parsed_df = cleaned_df.select(from_json("json_fixed", schema).alias("data"))

# Step 5: Flatten
flattened_df = parsed_df.select(
    F.col("data.flowSegmentData.frc").alias("functional_road_class"),
    F.col("data.flowSegmentData.currentSpeed").alias("current_speed"),
    F.col("data.flowSegmentData.freeFlowSpeed").alias("free_flow_speed"),
    F.col("data.flowSegmentData.currentTravelTime").alias("current_travel_time"),
    F.col("data.flowSegmentData.freeFlowTravelTime").alias("free_flow_travel_time"),
    F.col("data.flowSegmentData.confidence").alias("confidence"),
    F.col("data.flowSegmentData.roadClosure").alias("road_closure"),
    F.col("data.flowSegmentData.coordinates.coordinate").alias("coordinates")
)

# Step 6: Explode coordinates
coordinates_df = flattened_df.withColumn("coordinate", explode("coordinates")).select(
    "*",
    F.col("coordinate.latitude").alias("latitude"),
    F.col("coordinate.longitude").alias("longitude")
).drop("coordinates", "coordinate")

# Step 7: Add varied ingestion time
from pyspark.sql.functions import col, floor, rand, expr, current_timestamp

# Step 7.1: Add random offset between 0–23 hours
coordinates_df = coordinates_df.withColumn("hour_offset", floor(rand() * 24))

# Step 7.2: Subtract that offset from current_timestamp using timestampadd
coordinates_df = coordinates_df.withColumn(
    "ingestion_time",
    expr("timestampadd(HOUR, -1 * hour_offset, current_timestamp())")
)

# Step 7.3: Drop helper column
coordinates_df = coordinates_df.drop("hour_offset")



# ✅ Step 8: Write only one Silver table
coordinates_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("silver.nyc_traffic_silver")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.nyc_traffic_silver;
# MAGIC