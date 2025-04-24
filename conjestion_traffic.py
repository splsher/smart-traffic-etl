from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, when, col
from clean_data import df_clean
import os

spark = SparkSession.builder.appName("TrafficCongestionDetection").getOrCreate()

input_path = os.path.abspath("output/cleaned_traffic_data")
df_clean = spark.read.parquet(input_path)

#Detect congestion
df_congestion = df_clean.groupBy(
    window("timestamp", "5 minutes"),
    "road_id"
).agg(
    avg("speed").alias("avg_speed")
)


df_congestion = df_congestion.withColumn(
    "is_congested",
    when(col("avg_speed") < 20, True).otherwise(False)
)

print("Detected congestions:")
df_congestion.filter(col("is_congested") == True).show(truncate=False)