from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CRIME_2010 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
PARQUET_FILENAME = "LA_Crime_Data.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time conversion of LA Crime CSV data to Parquet on HDFS"
    )
    parser.add_argument("--base-path", required=True,
                        help="HDFS user directory, e.g. hdfs://.../user/username")
    return parser.parse_args()


def main():
    args = parse_args()
    parquet_path = f"{args.base_path.rstrip('/')}/{PARQUET_FILENAME}"

    spark = SparkSession.builder.appName("LA Crime CSV to Parquet").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.csv([CRIME_2010, CRIME_2020], header=True, inferSchema=False)

    df.write.mode("overwrite").parquet(parquet_path)

    count = spark.read.parquet(parquet_path).count()
    print(f"Done. Total rows written: {count}")
    print(f"Parquet file available at path: {parquet_path}")

    spark.stop()


if __name__ == "__main__":
    main()
