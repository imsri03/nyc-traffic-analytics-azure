# Databricks notebook source
display(dbutils.fs.ls("/mnt/bronze"))


# COMMAND ----------

df_bronze = spark.read.option("multiline", "true").json("/mnt/bronze/trafficdata")
df_bronze.display()


# COMMAND ----------

df_bronze.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col

df_silver = df_bronze.select(
    col("flowSegmentData.@version").alias("version"),
    col("flowSegmentData.confidence").alias("confidence"),
    col("flowSegmentData.coordinates.coordinate").alias("coordinates"),
    col("flowSegmentData.currentSpeed").alias("current_speed"),
    col("flowSegmentData.currentTravelTime").alias("current_travel_time"),
    col("flowSegmentData.freeFlowSpeed").alias("free_flow_speed"),
    col("flowSegmentData.freeFlowTravelTime").alias("free_flow_travel_time"),
    col("flowSegmentData.frc").alias("functional_road_class"),
    col("flowSegmentData.roadClosure").alias("road_closure")
)

df_silver.display()


# COMMAND ----------

from pyspark.sql.functions import explode

df_exploded = df_silver.select(
    col("version"),
    col("confidence"),
    explode("coordinates").alias("coord"),
    col("current_speed"),
    col("current_travel_time"),
    col("free_flow_speed"),
    col("free_flow_travel_time"),
    col("functional_road_class"),
    col("road_closure")
)

df_final = df_exploded.select(
    "*",
    col("coord.latitude").alias("latitude"),
    col("coord.longitude").alias("longitude")
).drop("coord")

df_final.display()


# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("silver.nyc_traffic_silver")


# COMMAND ----------

spark.sql("SHOW TABLES IN silver").show()
