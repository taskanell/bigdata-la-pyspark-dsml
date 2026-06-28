from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, udf
from pyspark.sql.types import StringType

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CRIME_2010 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def classify_period(time_occ: str):
    if time_occ is None:
        return None
    t = int(time_occ)
    if 500 <= t <= 1159:
        return "Morning"
    if 1200 <= t <= 1659:
        return "Afternoon"
    if 1700 <= t <= 2059:
        return "Evening"
    if t >= 2100 or t <= 459:
        return "Night"
    

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query 1 - DataFrame with UDF")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--output")
    return parser.parse_args()


def main():
    args = parse_args()

    builder = SparkSession.builder.appName("DFQ1 - UDF")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"DFQ1_udf_{spark.sparkContext.applicationId}")

    crimes_df = spark.read.csv([CRIME_2010, CRIME_2020], header=True, inferSchema=False)

    street_df = crimes_df.filter(col("Premis Desc") == "STREET")

    period_udf = udf(classify_period, StringType())
    period_df = street_df.withColumn("day_period", period_udf(col("TIME OCC")))

    start = perf_counter()

    total = period_df.count()

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
