import os
import json
import argparse
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

BRONZE = "data/bronze/yellow"
SILVER = "data/silver/yellow"
ARTIFACTS = "artifacts"


def build_parser():
    p = argparse.ArgumentParser(description="Run DQ checks against bronze and optionally write silver.")
    # Thresholds
    p.add_argument("--max-negatives", type=int, default=30000, help="Max allowed rows with negative/invalid fare/total.")
    p.add_argument("--max-early-dropoff", type=int, default=50, help="Max allowed rows where dropoff < pickup.")
    p.add_argument("--max-long-distance", type=int, default=200, help="Max allowed rows with trip_distance > 200 miles.")
    p.add_argument("--max-invalid-passengers", type=int, default=200, help="Max allowed rows with invalid passenger_count.")
    # Behavior flags
    p.add_argument("--warn-only", action="store_true", help="Do not fail the job even if thresholds are exceeded.")
    p.add_argument("--write-silver", action="store_true", help="Write cleaned silver output if set.")
    # IO
    p.add_argument("--bronze-path", default=BRONZE, help="Bronze Parquet root.")
    p.add_argument("--silver-path", default=SILVER, help="Silver Parquet output path.")
    p.add_argument("--artifacts-dir", default=ARTIFACTS, help="Directory to write DQ summary artifacts.")
    return p


def main(args):
    os.makedirs(args.artifacts_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("dq_checks")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.bronze_path)

    # Sanity: ensure columns exist
    cols = set(df.columns)
    required = {"pickup_datetime", "dropoff_datetime", "trip_distance", "fare_amount", "passenger_count"}
    missing = required - cols
    if missing:
        spark.stop()
        raise RuntimeError(f"Bronze dataset missing required columns: {missing}")

    # DQ predicates
    neg_fare = (F.col("fare_amount") < 0) | (F.col("fare_amount").isNull())
    early_drop = F.col("dropoff_datetime") < F.col("pickup_datetime")
    long_dist = F.col("trip_distance") > 200
    invalid_pass = (F.col("passenger_count") <= 0) | (F.col("passenger_count") > 8) | (F.col("passenger_count").isNull())

    # Count violations
    n_neg = df.filter(neg_fare).count()
    n_early = df.filter(early_drop).count()
    n_long = df.filter(long_dist).count()
    n_pass = df.filter(invalid_pass).count()

    # Print summary to console
    failed_any = any([
        n_neg > args.max_negatives,
        n_early > args.max_early_dropoff,
        n_long > args.max_long_distance,
        n_pass > args.max_invalid_passengers,
    ])

    if failed_any:
        print("DQ FAILED:")
    else:
        print("DQ PASSED:")

    print(f" - Negative fare/total rows: {n_neg} (limit {args.max_negatives})")
    print(f" - Dropoff before pickup rows: {n_early} (limit {args.max_early_dropoff})")
    print(f" - Unrealistic trip_distance > 200: {n_long} (limit {args.max_long_distance})")
    print(f" - Invalid passenger_count: {n_pass} (limit {args.max_invalid_passengers})")

    # Always write an artifact summary
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary = {
        "timestamp": ts,
        "bronze_path": args.bronze_path,
        "thresholds": {
            "max_negatives": args.max_negatives,
            "max_early_dropoff": args.max_early_dropoff,
            "max_long_distance": args.max_long_distance,
            "max_invalid_passengers": args.max_invalid_passengers,
        },
        "violations": {
            "negatives": n_neg,
            "early_dropoff": n_early,
            "long_distance": n_long,
            "invalid_passengers": n_pass,
        },
        "status": "FAILED" if failed_any else "PASSED",
    }
    with open(os.path.join(args.artifacts_dir, f"dq_summary_{ts}.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # write silver (cleaned) if requested
    if args.write_silver:
        clean = (
            df.filter(~neg_fare)
              .filter(~early_drop)
              .filter(~long_dist)
              .filter(~invalid_pass)
        )
        (
            clean.write
                 .mode("overwrite")
                 .parquet(args.silver_path)
        )
        print(f"Wrote silver parquet to {args.silver_path}")

    spark.stop()

    # Exit code policy
    if failed_any and not args.warn_only:
        raise SystemExit(2)


if __name__ == "__main__":
    main(build_parser().parse_args())
