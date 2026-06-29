from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CRIME_2010 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
PARQUET_FILENAME = "LA_Crime_Data.parquet"


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query 1 - DataFrame no UDF")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--output")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv")
    return parser.parse_args()


def main():
    args = parse_args()

    builder = SparkSession.builder.appName(f"DFQ1 - no UDF ({args.format})")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"DFQ1_{spark.sparkContext.applicationId}")

    if args.format == "parquet":
        crimes_df = spark.read.parquet(build_path(args.base_path, PARQUET_FILENAME))
    else:
        crimes_df = spark.read.csv([CRIME_2010, CRIME_2020], header=True, inferSchema=False)
    print(f"\nSELECTED FORMAT: {args.format}\n")

    street_df = crimes_df.filter(col("Premis Desc") == "STREET")

    time_col = col("TIME OCC").cast("integer")

    period_df = street_df.withColumn(
        "day_period",
        when((time_col >= 500) & (time_col <= 1159), "Morning")
         .when((time_col >= 1200) & (time_col <= 1659), "Afternoon")
         .when((time_col >= 1700) & (time_col <= 2059), "Evening")
         .when((time_col >= 2100) | (time_col <= 459), "Night")
    )

    period_df.cache()  # cached; reused by both actions below

    start = perf_counter()

    total = period_df.count()  # 1st action: triggers caching

    results = (
        period_df.groupBy("day_period")
        .count()
        .withColumn("percentage", round(col("count") / total * 100, 2))
        .orderBy(col("percentage").desc())
        .select("day_period", "percentage")
    )

    results.show()

    results.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    elapsed = perf_counter() - start
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")
    print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
