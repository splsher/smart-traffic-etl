from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os

# Created Spark Session for reading CSV file, clearing, and saving in .parquet format
spark = SparkSession.builder \
    .appName("TrafficDataCleaning") \
    .config("spark.hadoop.hadoop.native.lib", "false") \
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.HadoopMapReduceCommitProtocol") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

input_path = os.path.abspath("data/raw/traffic_data.csv")
print(f"Reading CSV from: {input_path}")

#set True - because will do data clearing in next step
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("car_id", IntegerType(), True),
    StructField("speed", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("road_id", IntegerType(), True)
])
# Read CSV
df = spark.read.csv(
    input_path,
    header=True,
    schema=schema
)

print("Raw Data Schema:")
df.printSchema()

print("Sample data:")
df.show(5)

# Next step - clearing data -- dropping null values --
df_clean = df.dropna(subset=["timestamp", "car_id", "speed", "lat", "lon", "road_id"])

# Filtering
df_clean = df_clean.filter(
    (df_clean.speed >= 0) &
    (df_clean.speed <= 130) &
    (df_clean.lat != 0) &
    (df_clean.lon != 0)
)
df_clean = df_clean.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# Cache transformation to avoid recomputation before actions
df_clean = df_clean.cache()

# Count 
count = df_clean.count()
print(f"Cleaned records count: {count}")

# Check if not empty and then - save
if count > 0:
    output_path = os.path.abspath("output/cleaned_traffic_data/")
    print(f"Saving cleaned data to: {output_path}")

    # added coalesce to transformation to ensure that only one file will be written
    try:
       df_clean.coalesce(1).write.mode('overwrite').parquet(output_path)
       print("Successfully saved as one file")
    except Exception as e:
       print("Write failed:", e)

    df_clean.write.mode('overwrite').parquet(output_path)
    print("Write complete.")
else:
    print("No valid records found after cleaning.")

print("Spark job completed.")
