import argparse
from pyspark.sql import SparkSession, functions as F


## Args and Session
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--raw",    default="data/raw/yellow_tripdata_2023-01.parquet")
    p.add_argument("--bronze", default="data/bronze/yellow")
    p.add_argument("--target-partitions", type=int, default=16)     # number of output tasks/files
    p.add_argument("--shuffle-partitions", type=int, default=16)    # default 200 is too high for dev
    p.add_argument("--max-partition-bytes", default="64m")          # encourage parallel reads
    p.add_argument("--max-records-per-file", type=int, default=0)   # 0 = disabled; else cap file size by rows
    return p.parse_args()

args = parse_args()

spark = (
    SparkSession.builder
    .appName("BatchETL")
    .config("spark.master", "local[*]")  # use all cores locally
    .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    .config("spark.sql.files.maxPartitionBytes", args.max_partition_bytes)
    .config("spark.sql.caseSensitive", "false")
    .getOrCreate()
)

# Optional: cap output file rows (rough control of file size)
if args.max_records_per_file and args.max_records_per_file > 0:
    spark.conf.set("spark.sql.files.maxRecordsPerFile", str(args.max_records_per_file))


## Read, Transform, Repartition, Write
RAW = args.raw
BRONZE = args.bronze

# Read (Parquet is splittable; configs above increase parallelism)
df = spark.read.parquet(RAW)

# Basic hygiene (adapt to your schema)
# 1) normalize column names
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))

# 2) derive date (adjust to your timestamp column)
ts_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else None
if ts_col:
    df = df.withColumn("pickup_date", F.to_date(F.col(ts_col)))
else:
    # fallback: if you already have pickup_date, ensure it's a date
    if "pickup_date" in df.columns:
        df = df.withColumn("pickup_date", F.to_date("pickup_date"))
    else:
        raise ValueError("No pickup timestamp/date column found to derive 'pickup_date'.")

# (Optional) project needed columns
keep = [c for c in df.columns if c in {
    "pickup_date", "passenger_count", "trip_distance", "total_amount",
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "vendorid"
}]
if keep:
    df = df.select(*set(keep))  # set() avoids duplicates

# Log scale
print(f"Input partitions (read): {df.rdd.getNumPartitions()}")
print(f"Input rows: {df.count()}")

# Repartition for controlled parallelism & better output file sizes
# Option A: by count (uniform)
df_out = df.repartition(args.target_partitions)

# Option B (switch to this if you prefer partition alignment by key):
# df_out = df.repartition("pickup_date")

# Write bronze partitioned by date (good for pruning later)
(df_out
 .write
 .mode("overwrite")
 .partitionBy("pickup_date")
 .parquet(BRONZE)
)

print(f"Wrote bronze parquet to {BRONZE}")
spark.stop()
