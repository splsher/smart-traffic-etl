from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
import os

# Created Spark Session for reading CSV file, clearing, and saving in .parquet format
spark = SparkSession.builder \
    .appName("TrafficDataCleaning") \
    .getOrCreate()

input_path = os.path.abspath("data/raw/traffic_data.csv")
print(f"Reading CSV from: {input_path}")

# Read CSV
df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)

print("Raw Data Schema:")
df.printSchema()

print("Sample data:")
df.show(5)

# Drop nulls
df_clean = df.dropna(subset=["timestamp", "car_id", "speed", "lat", "lon", "road_id"])

# Filtering
df_clean = df_clean.filter(
    (df_clean.speed >= 0) &
    (df_clean.speed <= 130) &
    (df_clean.lat != 0) &
    (df_clean.lon != 0)
)
df_clean = df_clean.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Count 
count = df_clean.count()
print(f"Cleaned records count: {count}")

# Check if not empty and then - save
if count > 0:
    output_path = os.path.abspath("data/processed/cleaned/")
    print(f"Saving cleaned data to: {output_path}")
    df_clean.write.mode('overwrite').parquet(output_path)
    print("Write complete.")
else:
    print("No valid records found after cleaning.")

print("Spark job completed.")
