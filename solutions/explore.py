"""
Quick exploration script using SQL queries.
Usage:
    spark-submit solutions/explore.py
    spark-submit solutions/explore.py --file both
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CRIME_2010 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"


def sep(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master")
    parser.add_argument("--file", choices=["2010", "2020", "both"], default="2010",
                        help="Which file(s) to load (default: 2010 only, faster)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    builder = SparkSession.builder.appName("explore")
    if args.master:
        builder = builder.master(args.master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    paths = {
        "2010": [CRIME_2010],
        "2020": [CRIME_2020],
        "both": [CRIME_2010, CRIME_2020],
    }[args.file]

    spark.read.csv(paths, header=True, inferSchema=False).createOrReplaceTempView("crimes")

    # ── 1. Schema ─────────────────────────────────────────────────
    sep("Schema")
    spark.sql("DESCRIBE crimes").show(100, truncate=False)

    # ── 2. Row count ──────────────────────────────────────────────
    sep("Row count")
    spark.sql("SELECT COUNT(*) AS total FROM crimes").show()

    # ── 3. Raw DATE OCC samples ───────────────────────────────────
    sep("DATE OCC — 10 raw values")
    spark.sql("SELECT `DATE OCC` FROM crimes LIMIT 10").show(truncate=False)

    # ── 4. String length distribution (format sanity) ─────────────
    sep("DATE OCC — distinct string lengths")
    spark.sql("""
        SELECT LENGTH(`DATE OCC`) AS len, COUNT(*) AS cnt
        FROM crimes
        GROUP BY len
        ORDER BY len
    """).show()

    # ── 5. Verify timestamp parsing ───────────────────────────────
    sep("DATE OCC — parsed year and month (15 samples)")
    spark.sql("""
        SELECT
            `DATE OCC`,
            YEAR(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a'))  AS year,
            MONTH(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS month
        FROM crimes
        LIMIT 15
    """).show(truncate=False)

    # ── 6. Distinct years ─────────────────────────────────────────
    sep("Distinct years in dataset")
    spark.sql("""
        SELECT YEAR(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS year, COUNT(*) AS cnt
        FROM crimes
        GROUP BY year
        ORDER BY year
    """).show(20)

    # ── 7. TIME OCC samples ───────────────────────────────────────
    sep("TIME OCC — 10 raw values")
    spark.sql("SELECT `TIME OCC` FROM crimes LIMIT 10").show(truncate=False)

    # ── 8. Premis Desc top 10 ─────────────────────────────────────
    sep("Premis Desc — top 10 by count")
    spark.sql("""
        SELECT `Premis Desc`, COUNT(*) AS cnt
        FROM crimes
        GROUP BY `Premis Desc`
        ORDER BY cnt DESC
        LIMIT 10
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
