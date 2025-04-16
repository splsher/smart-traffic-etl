import pandas as pd 
import pyarrow.parquet as pq 

table = pq.read_table('data/processed/cleaned/part-00000-xxxx.snappy.parquet')
df = table.to_pandas()
print(df.head())