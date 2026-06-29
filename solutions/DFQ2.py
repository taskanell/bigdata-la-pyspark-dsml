from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, rank, to_timestamp, year
from pyspark.sql.window import Window

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CRIME_2010 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query 2 - DataFrame")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    builder = SparkSession.builder.appName("DFQ2")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"DFQ2_{spark.sparkContext.applicationId}")

    crimes_df = spark.read.csv([CRIME_2010, CRIME_2020], header=True, inferSchema=False)

    # DATE OCC format: "yyyy MMM dd hh:mm:ss a"
    ts = to_timestamp(col("DATE OCC"), "yyyy MMM dd hh:mm:ss a")
    date_df = (
        crimes_df
        .withColumn("year", year(ts))
        .withColumn("month", month(ts))
    )

    monthly_counts = date_df.groupBy("year", "month").agg(count("*").alias("crime_total"))

    window = Window.partitionBy("year").orderBy(col("crime_total").desc())
    ranked = monthly_counts.withColumn("ranking", rank().over(window))

    start = perf_counter()

    results = (
        ranked
        .filter(col("ranking") <= 3)
        .orderBy(col("year").asc(), col("ranking").asc())
        .select("year", "month", "crime_total", "ranking")
    )

    results.show(50) # (16x3) = 48 rows, plus header row
    results.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    elapsed = perf_counter() - start
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")
    print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
