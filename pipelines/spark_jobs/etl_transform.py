
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window

spark = SparkSession.builder.appName("TelemetryETL").getOrCreate()

# Load sample data
df = spark.read.json("data/sample_events.json")

# Basic transformation
df_transformed = (
    df.withColumn("timestamp", to_timestamp("timestamp"))
      .groupBy("user_id", "feature", window("timestamp", "1 hour"))
      .count()
      .withColumnRenamed("count", "event_count")
)

# Save to Parquet (simulating a curated table)
df_transformed.write.mode("overwrite").parquet("warehouse/feature_usage_hourly")

spark.stop()
