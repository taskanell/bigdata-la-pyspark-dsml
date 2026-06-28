from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CENSUS_BLOCKS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Census_Blocks_2020.geojson"
INCOME_CSV = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_income_2021.csv"

# Census Blocks property field names 
ZIPCODE_COL = "ZCTA20"      # ZIP Code Tabulation Area (5-digit zipcode)
POP_COL     = "POP20"       # 2020 Census population count
HH_COL      = "HOUSING20"   # Total housing units count

# Income CSV column names 
INCOME_ZIP_COL = "Zip Code"
INCOME_COL     = "Estimated Median Income" # format: "$52,806" 


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"

def parse_income(parts, zip_idx: int, income_idx: int):
    try:
        median_income = parts[income_idx].strip().replace(",", "").replace("$", "")
        return (parts[zip_idx].strip(), float(median_income))
    except (ValueError, IndexError):
        return (parts[zip_idx].strip(), 0.0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query 3 - RDD")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--output")
    return parser.parse_args()


def main():
    args = parse_args()

    builder = SparkSession.builder.appName("RddQ3")
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None:
        output_path = build_path(args.base_path, f"RddQ3_{sc.applicationId}")

    # Read GeoJSON via SparkSession (multiLine JSON cannot be parsed with sc.textFile),
    # flatten properties, then hand off to RDD operations
    census_blocks_raw = (
        spark.read
        .option("multiLine", "true")
        .json(CENSUS_BLOCKS)
        .selectExpr("explode(features) as features")
        .select("features.*")
    )
    prop_fields = census_blocks_raw.schema["properties"].dataType.fieldNames()
    flattened_blocks = census_blocks_raw.select([
        col(f"properties.{c}").alias(c) for c in prop_fields
    ])

    # Convert to RDD of (zipcode, (population, households)) pairs
    census_rdd = (
        flattened_blocks
        .select(ZIPCODE_COL, POP_COL, HH_COL)
        .rdd
        .map(lambda row: (
            str(row[ZIPCODE_COL] or ""),
            (int(row[POP_COL] or 0), int(row[HH_COL] or 0)),
        ))
        .filter(lambda kv: kv[0] != "")
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .filter(lambda kv: kv[1][0] > 0)   # drop zero-population ZIP codes
    )

    # Read income CSV: (zipcode, median_income) pairs
    income_raw = sc.textFile(INCOME_CSV)
    income_header = income_raw.first()
    header_parts = next(csv.reader(io.StringIO(income_header), delimiter=";"))
    zip_idx = header_parts.index(INCOME_ZIP_COL)
    income_idx = header_parts.index(INCOME_COL)


    income_rdd = (
        income_raw
        .filter(lambda line: line != income_header)
        .map(lambda line: next(csv.reader(io.StringIO(line), delimiter=";")))
        .map(lambda parts: parse_income(parts, zip_idx, income_idx))
        .filter(lambda kv: kv[1] > 0)
    )

    start = perf_counter()

    # Join on ZIP code, compute per_capita_income = (median_income × households) / population
    results = (
        census_rdd
        .join(income_rdd)
        .map(lambda kv: (
            kv[0],
            round(kv[1][1] * kv[1][0][1] / kv[1][0][0], 2),
        ))
        .sortBy(lambda kv: kv[0])
        .collect()
    )

    header = [("zipcode", "per_capita_income")]
    sc.parallelize(header + results, 1).saveAsTextFile(output_path)

    elapsed = perf_counter() - start
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

    for item in results:
        print(item)
    print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
