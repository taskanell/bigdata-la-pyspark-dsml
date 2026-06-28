from __future__ import annotations

import argparse
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query 2 - SQL")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--output", help="Explicit output path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    builder = SparkSession.builder.appName("SQLQ2")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None and args.base_path:
        output_path = build_path(args.base_path, f"SQLQ2_{spark.sparkContext.applicationId}")

    crimes_df = spark.read.csv([CRIME_2010, CRIME_2020], header=True, inferSchema=False)
    crimes_df.createOrReplaceTempView("crimes")

    start = perf_counter()

    results = spark.sql("""
        SELECT year, month, crime_total, ranking
        FROM (
            SELECT
                year, month, crime_total,
                RANK() OVER (
                    PARTITION BY year
                    ORDER BY crime_total DESC
                ) AS ranking
            FROM (
                SELECT
                    YEAR(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a'))  AS year,
                    MONTH(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS month,
                    COUNT(*) AS crime_total
                FROM crimes
                GROUP BY year, month
            )
        )
        WHERE ranking <= 3
        ORDER BY year ASC, ranking ASC
    """)

    results.show(50) # (16x3) = 48 rows, plus header row
    
    results.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    elapsed = perf_counter() - start
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")
    print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
