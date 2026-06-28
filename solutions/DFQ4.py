from __future__ import annotations

import argparse
import os
import sys
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

CRIME_2010 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
POLICE_STATIONS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Police_Stations.csv"

CRIME_ID = "DR_NO"  # unique crime identifier
EARTH_RADIUS_KM = 6371.0

# Join strategies to experiment with. This Query demands a CROSS JOIN (no equi-key),
# so only the nested-loop strategies are applicable: 'broadcast' -> BroadcastNestedLoopJoin,
# 'shuffle_replicate_nl' -> CartesianProduct. 'merge' and 'shuffle_hash' require equi-keys
# and are silently ignored by Catalyst (the explain plan still shows a nested-loop join).
JOIN_STRATEGIES = ["default", "broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]


def build_path(base_path: str, relative_path: str) -> str:
    return f"{base_path.rstrip('/')}/{relative_path.lstrip('/')}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query 4 - DataFrame")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--output")
    parser.add_argument(
        "--join-strategy",
        choices=JOIN_STRATEGIES,
        default="default",
        help="Force a join strategy. 'default' lets Catalyst choose.",
    )
    return parser.parse_args()


def cross_join(crimes_df, stations_df, strategy: str):
    """Cross join (no condition) forcing the requested strategy.

    merge / shuffle_hash require equi-keys, so on this non-equi join Catalyst
    ignores them and falls back to a nested-loop plan (visible in explain).
    """
    if strategy == "default":
        return crimes_df.join(stations_df)
    if strategy == "broadcast":
        return crimes_df.join(F.broadcast(stations_df))
    # shuffle_replicate_nl (merge / shuffle_hash are resolved to 
    # BroadcastNestedLoopJoin by Catalyst on a non-equi join)
    return crimes_df.join(stations_df.hint(strategy))


def haversine_km(lat1, lon1, lat2, lon2):
    """
        Great-circle distance in km between two (lat, lon) points.
        Theoretical formula from https://www.movable-type.co.uk/scripts/latlong.html
    """
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    )
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return F.lit(EARTH_RADIUS_KM) * c


def main():
    args = parse_args()

    spark = SparkSession.builder.appName(f"DFQ4_{args.join_strategy}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # A condition-less join is a cross join; allow it explicitly.
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    output_path = args.output
    if output_path is None:
        output_path = build_path(args.base_path, 
                                f"DFQ4_{args.join_strategy}_{spark.sparkContext.applicationId}")

    # Crime data: union both periods, keep crime id + coordinates, drop null/zero coords
    crimes_df = (
        spark.read.csv([CRIME_2010, CRIME_2020], header=True)
        .select(
            F.col(CRIME_ID),
            F.col("LAT").cast("double").alias("LAT"),
            F.col("LON").cast("double").alias("LON"),
        )
        .filter(
            F.col("LAT").isNotNull() & F.col("LON").isNotNull()
            & ~((F.col("LAT") == 0) & (F.col("LON") == 0))
        )
    )

    # Police stations: X = longitude, Y = latitude 
    stations_df = (
        spark.read.csv(POLICE_STATIONS, header=True)
        .select(
            F.col("DIVISION").alias("division"),
            F.col("Y").cast("double").alias("st_lat"),
            F.col("X").cast("double").alias("st_lon"),
        )
    )

    # Cross join (no equi-key — "nearest" is a min over distance), trying to force the chosen
    # strategy. 
    dist_pairs_df = cross_join(crimes_df, stations_df, args.join_strategy).withColumn(
        "distance_km",
        haversine_km(F.col("LAT"), F.col("LON"), F.col("st_lat"), F.col("st_lon")),
    )

    min_dist_pairs_df = (
        dist_pairs_df.groupBy(CRIME_ID)
        # Struct comparison is lexicographic: distance_km (first field) drives the min,
        # carrying division along — single-pass argmin without a window sort.
        .agg(F.min(F.struct("distance_km", "division")).alias("min_dist_pair"))
        .select(
            F.col("min_dist_pair.division").alias("division"),
            F.col("min_dist_pair.distance_km").alias("distance_km"),
        )
    )

    # Per division: number of crimes nearest to it + their average distance
    results = (
        min_dist_pairs_df.groupBy("division")
        .agg(
            F.round(F.avg("distance_km"), 3).alias("average_distance"),
            F.count("*").alias("#"),
        )
        .orderBy(F.col("#").desc())
    )

    # Show the physical plan to verify which join strategy was actually used
    print(f"Physical plan (join strategy = {args.join_strategy}): ")
    results.explain("formatted")

    start = perf_counter()

    rows = [(row.division, row.average_distance, row["#"]) for row in results.collect()]
    print("(division, average_distance, #)")
    for item in rows:
        print(item)

    results.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    elapsed = perf_counter() - start
    print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")
    print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
