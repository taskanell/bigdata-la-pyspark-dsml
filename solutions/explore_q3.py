from __future__ import annotations

import argparse
import csv
import io
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CENSUS_BLOCKS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Census_Blocks_2020.geojson"
INCOME_CSV = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_income_2021.csv"

# Confirmed field names (see earlier explore run)
ZIPCODE_COL = "ZCTA20"
POP_COL     = "POP20"
HH_COL      = "HOUSING20"
INCOME_ZIP_COL = "Zip Code"
INCOME_COL     = "Estimated Median Income"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Q3 pipeline (parse, aggregate, join, compute)")
    parser.add_argument("--base-path", required=True)
    return parser.parse_args()


def main() -> None:
    parse_args()

    spark = SparkSession.builder.appName("ExploreQ3").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # 0a. Raw income CSV column types (before any parsing/casting)
    print("\n=== Income CSV RAW schema (sep=';', all strings) ===")
    income_raw_df = spark.read.csv(INCOME_CSV, header=True, inferSchema=False, sep=";")
    income_raw_df.printSchema()
    print("Columns + types:", income_raw_df.dtypes)
    income_raw_df.show(15, truncate=False)

    # 0b. Raw census block 'properties' column types (as inferred from the GeoJSON)
    print("\n=== Census Blocks RAW properties schema (inferred from GeoJSON) ===")
    blocks_raw0 = (
        spark.read
        .option("multiLine", "true")
        .json(CENSUS_BLOCKS)
        .selectExpr("explode(features) as features")
        .select("features.properties.*")
    )
    blocks_raw0.printSchema()
    print("Columns + types:", blocks_raw0.dtypes)
    blocks_raw0.show(15, truncate=False)
    
    # 1. Income CSV: parse with ';' and strip "$"/"," from income values
    print("\n=== Income CSV (parsed with sep=';') ===")
    income_df = (
        spark.read
        .csv(INCOME_CSV, header=True, inferSchema=False, sep=";")
        .withColumnRenamed(INCOME_ZIP_COL, "income_zip")
        .withColumn(
            "median_income",
            F.regexp_replace(F.col(INCOME_COL), "[$,]", "").cast("double"),
        )
        .select("income_zip", "median_income")
    )
    income_df.printSchema()
    income_df.show(15, truncate=False)
    total_income_rows = income_df.count()
    with_income = income_df.filter(F.col("median_income").isNotNull()).count()
    print(f"Income rows: {total_income_rows} total, {with_income} with a parseable income value")

    # 2. Census GeoJSON: explode features, flatten properties, aggregate per ZIP
    blocks_raw = (
        spark.read
        .option("multiLine", "true")
        .json(CENSUS_BLOCKS)
        .selectExpr("explode(features) as features")
        .select("features.*")
    )
    prop_fields = blocks_raw.schema["properties"].dataType.fieldNames()
    flattened = blocks_raw.select([
        F.col(f"properties.{c}").alias(c) for c in prop_fields
    ])

    zip_stats = (
        flattened
        .groupBy(ZIPCODE_COL)
        .agg(
            F.sum(F.col(POP_COL).cast("long")).alias("population"),
            F.sum(F.col(HH_COL).cast("long")).alias("households"),
        )
        .filter(F.col("population") > 0)
    )
    print("\n=== Census aggregated per ZIP (population, households) ===")
    zip_stats.orderBy(ZIPCODE_COL).show(15)
    print(f"Distinct ZIPs with population > 0: {zip_stats.count()}")

    # 3. Join + per-capita computation — this is the actual Q3 result
    result = (
        zip_stats
        .join(income_df.filter(F.col("median_income").isNotNull()),
              F.col(ZIPCODE_COL) == F.col("income_zip"))
        .withColumn(
            "per_capita_income",
            F.round(F.col("median_income") * F.col("households") / F.col("population"), 2),
        )
        .select(
            F.col(ZIPCODE_COL).alias("zipcode"),
            "population",
            "households",
            "median_income",
            "per_capita_income",
        )
        .orderBy("zipcode")
    )
    print("\n=== Q3 RESULT preview (join + per-capita) ===")
    result.show(25, truncate=False)
    matched = result.count()
    print(f"ZIPs matched between census and income: {matched}")

    # 4. Sanity: ZIPs present in income but missing from census (and vice versa)
    census_zips = zip_stats.select(F.col(ZIPCODE_COL).alias("z")).distinct()
    income_zips = income_df.filter(F.col("median_income").isNotNull()).select(F.col("income_zip").alias("z")).distinct()
    only_income = income_zips.join(census_zips, "z", "left_anti").count()
    only_census = census_zips.join(income_zips, "z", "left_anti").count()
    print(f"ZIPs only in income (no census match): {only_income}")
    print(f"ZIPs only in census (no income match): {only_census}")

    # 5. Show all non-"$" income values to detect unparseable cases
    print("\n=== Income CSV: non-dollar income values (unexpected formats) ===")
    income_raw_rdd = sc.textFile(INCOME_CSV)
    income_header_line = income_raw_rdd.first()
    header_parts = next(csv.reader(io.StringIO(income_header_line), delimiter=";"))
    zip_idx_r   = header_parts.index(INCOME_ZIP_COL)
    income_idx  = header_parts.index(INCOME_COL)
    non_dollar = (
        income_raw_rdd
        .filter(lambda line: line != income_header_line)
        .map(lambda line: next(csv.reader(io.StringIO(line), delimiter=";")))
        .filter(lambda parts: len(parts) > income_idx and not parts[income_idx].strip().startswith("$"))
        .map(lambda parts: (parts[zip_idx_r].strip(), parts[income_idx].strip()))
        .collect()
    )
    print(f"Count: {len(non_dollar)}")
    for zipcode, val in non_dollar:
        print(f"  ZIP {zipcode}: {repr(val)}")

    spark.stop()


if __name__ == "__main__":
    main()
