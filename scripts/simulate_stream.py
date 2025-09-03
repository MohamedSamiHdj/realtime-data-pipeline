import os
import shutil
from pyspark.sql import SparkSession

BRONZE = "data/bronze/yellow"
INBOX  = "data/stream_inbox"

os.makedirs(INBOX, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("sim_stream")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(BRONZE).select(
    "tpep_pickup_datetime","trip_distance","fare_amount"
)

# make small CSV batches
batches = 5
rows = df.count()
rows_per = max(1, rows // batches)

for i in range(batches):
    part = df.limit((i+1)*rows_per).tail(rows_per)
    if not part:  # safety
        break
    pdf = spark.createDataFrame(part).toPandas()
    target = os.path.join(INBOX, f"batch_{i}.csv")
    pdf.to_csv(target, index=False)
    print(f"Dropped {target}")

spark.stop()
