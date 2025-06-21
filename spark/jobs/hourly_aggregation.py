from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_hourly_aggregation():
    spark = SparkSession.builder.appName("TelemetryHourlyAggregation").getOrCreate()
    raw_events = spark.read.csv("data/raw/app_events_raw.csv", header=True)
    hourly_features = (raw_events
        .withColumn("window_start", date_trunc("hour", col("event_timestamp")))
        .withColumn("feature", regexp_extract(col("event_payload"), r'"feature":"([^"]*)"', 1))
        .groupBy("window_start", "user_id", "feature")
        .agg(count("*").alias("event_count"))
        .filter(col("event_count") > 0)
        .orderBy("window_start", "user_id", "feature")
    )
    (hourly_features
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("data/spark_processed/feature_usage_hourly")
    )
    spark.stop()

if __name__ == "__main__":
    create_hourly_aggregation()
