from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def enrich_user_profiles():
    spark = SparkSession.builder.appName("UserProfileEnrichment").getOrCreate()
    raw_users = spark.read.csv("data/raw/user_registrations_raw.csv", header=True)
    enriched_users = (raw_users
        .withColumn("age", datediff(current_date(), col("birth_date").cast("date")) / 365)
        .withColumn("tenure_days", datediff(current_date(), col("join_date")))
        .withColumn("user_segment", 
            when(col("subscription_tier") == "premium", "Premium")
            .when(col("trial_end_date").isNull(), "Free")
            .otherwise("Trial")
        )
        .select("user_id", "gender", "age", "user_segment", "region", 
                "join_date", "is_active", "tenure_days")
    )
    (enriched_users
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("data/spark_processed/users")
    )

if __name__ == "__main__":
    enrich_user_profiles()
