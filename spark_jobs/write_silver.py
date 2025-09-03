# write_silver.py
import argparse
from pyspark.sql import SparkSession, functions as F

def parse_args():
    p = argparse.ArgumentParser(description="Write silver tables from bronze")
    p.add_argument("--bronze", default="data/bronze/yellow", help="Input bronze path (parquet)")
    p.add_argument("--silver", default="data/silver/yellow", help="Output silver path (parquet)")
    p.add_argument("--partitions", type=int, default=4, help="Number of output partitions")
    p.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Save mode")
    return p.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("WriteSilver")
        .getOrCreate()
    )

    df = spark.read.parquet(args.bronze)

    # Example light cleanup — tweak to your schema
    # 1) Trim string columns
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))

    # 2) Drop obvious temp columns if present
    to_drop = [c for c in df.columns if c.lower().startswith("_")]
    if to_drop:
        df = df.drop(*to_drop)

    # 3) Add a load timestamp
    df = df.withColumn("silver_loaded_at", F.current_timestamp())

    # Repartition for write
    if args.partitions and args.partitions > 0:
        df = df.repartition(args.partitions)

    df.write.mode(args.mode).parquet(args.silver)
    print(f"Wrote silver parquet to {args.silver}")

    spark.stop()

if __name__ == "__main__":
    main()
