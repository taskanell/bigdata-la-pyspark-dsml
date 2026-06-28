from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, regexp_replace, round, sum

# Join strategies that can be forced via --join-strategy
JOIN_STRATEGIES = ["default", "broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query 3 - DataFrame")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--output")
    parser.add_argument(
        "--join-strategy",
        choices=JOIN_STRATEGIES,
        default="default",
        help="Force a join strategy. 'default' lets Catalyst choose.",
    )
    return parser.parse_args()


def join_zip_income(zip_stats_df, income_df, strategy: str):
    """Join census stats with income, forcing the requested join strategy."""
    cond = col(ZIPCODE_COL) == col(INCOME_ZIP_COL)
    if strategy == "default":
        return zip_stats_df.join(income_df, cond)
    if strategy == "broadcast":
        return zip_stats_df.join(broadcast(income_df), cond)
    # merge / shuffle_hash / shuffle_replicate_nl
    return zip_stats_df.join(income_df.hint(strategy), cond)


def main():
    args = parse_args()

    builder = SparkSession.builder.appName(f"DFQ3_{args.join_strategy}")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    output_path = args.output
    if output_path is None:
        output_path = build_path(args.base_path, 
                                f"DFQ3_{args.join_strategy}_{spark.sparkContext.applicationId}")

    # Read GeoJSON FeatureCollection: explode the features array, flatten properties
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

    # Aggregate population and occupied housing units (households) per ZIP code
    zip_stats_df = (
        flattened_blocks
        .groupBy(ZIPCODE_COL)
        .agg(
            sum(col(POP_COL).cast("long")).alias("population"),
            sum(col(HH_COL).cast("long")).alias("households"),
        )
        .filter(col("population") > 0) # avoid division by zero when computing per-capita income
    )

    # Median household income per ZIP code
    income_df = (
        spark.read
        .csv(INCOME_CSV, header=True, inferSchema=False, sep=";")
        # Strip $ and , before casting
        .withColumn(
            "median_income",
            regexp_replace(col(INCOME_COL), "[$,]", "").cast("double"),
        )
        .select(INCOME_ZIP_COL, "median_income")
        .filter(col("median_income").isNotNull())
    )

    # per_capita_income = (median_household_income × households) / population
    results = (
        join_zip_income(zip_stats_df, income_df, args.join_strategy)
        .withColumn(
            "per_capita_income",
            round(col("median_income") * col("households") / col("population"), 2),
        )
        .select(col(ZIPCODE_COL).alias("zipcode"), "per_capita_income")
        .orderBy("zipcode")
    )

    # Show the physical plan to verify which join strategy was actually used.
    print(f"Physical plan (join strategy = {args.join_strategy}): ")
    results.explain("formatted")

    start = perf_counter()

    rows = [(row.zipcode, row.per_capita_income) for row in results.collect()]
    print("(zipcode, per_capita_income)")
    for item in rows:
        print(item)

    results.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    elapsed = perf_counter() - start
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")
    print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
