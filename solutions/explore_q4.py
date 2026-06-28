from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CRIME_2010 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
POLICE_STATIONS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Police_Stations.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Q4 inputs (crime LAT/LON + police stations)")
    parser.add_argument("--base-path", required=True)
    return parser.parse_args()


def main() -> None:
    parse_args()

    spark = SparkSession.builder.appName("ExploreQ4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Crime data schema (header-inferred). Both files share a schema; inspect one.
    print("\n=== Crime Data RAW schema (2010-2019 file) ===")
    crime_df = spark.read.csv(CRIME_2010, header=True, inferSchema=True)
    crime_df.printSchema()
    print("Columns + types:", crime_df.dtypes)

    # Focus on the geo columns we need for the nearest-station join
    geo_cols = [c for c in crime_df.columns if c.upper() in ("DR_NO", "LAT", "LON")]
    print("Geo columns found:", geo_cols)
    crime_df.select(*geo_cols).show(15, truncate=False)

    # 2. Quantify unusable coordinates (Null Island 0,0 and nulls) that must be filtered
    total = crime_df.count()
    null_coord = crime_df.filter(
        F.col("LAT").isNull() | F.col("LON").isNull() |
        ((F.col("LAT") == 0) & (F.col("LON") == 0))
    ).count()
    print(f"\nCrime rows (2010-2019): {total} total, {null_coord} with null/zero coordinates")

    # 3. Police stations: small file (21 rows) — show schema and ALL rows
    print("\n=== LA Police Stations RAW schema ===")
    stations_df = spark.read.csv(POLICE_STATIONS, header=True, inferSchema=True)
    stations_df.printSchema()
    print("Columns + types:", stations_df.dtypes)
    print(f"Station count: {stations_df.count()}")
    stations_df.show(30, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
