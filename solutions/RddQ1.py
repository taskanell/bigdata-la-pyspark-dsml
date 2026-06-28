from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession

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
    parser = argparse.ArgumentParser(description="Query 1 - RDD")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    builder = SparkSession.builder.appName("RddQ1")

    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"RddQ1_{sc.applicationId}")

    # Read header from first file to resolve column indices by name
    header_line = sc.textFile(CRIME_2010).first()
    header = next(csv.reader(io.StringIO(header_line)))
    time_idx = header.index("TIME OCC")
    premis_idx = header.index("Premis Desc")

    crimes = sc.textFile(CRIME_2010).union(sc.textFile(CRIME_2020))
    street_rdd = (
        crimes
        .filter(lambda line: line != header_line)
        .map(lambda line: next(csv.reader(io.StringIO(line))))
        .filter(lambda row: row[premis_idx] == "STREET")
    )
    # cache() keeps the filtered rows in executor memory after the first action so that
    # the second action (reduceByKey) does not re-read and re-parse the CSVs from scratch.
    street_rdd.cache()

    start = perf_counter()

    # Two separate actions (count + collect) are needed because total must include ALL
    # street rows (denominator), while period_counts excludes unclassified ones.
    # Merging into one step would make percentages sum to exactly 100%, matching the DF version requires this split.
    total = street_rdd.count()

    period_counts = (
        street_rdd
        .map(lambda row: classify_period(row[time_idx]))
        .filter(lambda p: p is not None)
        .map(lambda p: (p, 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

    results = sorted(
        [(period, round(count / total * 100, 2)) for period, count in period_counts],
        key=lambda x: x[1],
        reverse=True,
    )

    header = [("day_period", "percentage")]
    sc.parallelize(header + results, 1).saveAsTextFile(output_path)

    elapsed = perf_counter() - start
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    for item in results:
        print(item)
    print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
