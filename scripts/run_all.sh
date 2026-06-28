#!/usr/bin/env bash
# Runs every benchmark script in scripts/ end to end.
#
# Each per-query script (run_q1..run_q4) takes a single [RUNS] argument and
# writes its own logs/q*_results.log. run_join takes [QUERY] [RUNS]; here it is
# driven with QUERY=both so Q3 and Q4 join sweeps are both produced.
#
# Usage: bash scripts/run_all.sh [RUNS]   (default: 3)
#   RUNS — runs per configuration/strategy, forwarded to every script.

set -u

usage() {
    cat <<EOF
Usage: bash scripts/run_all.sh [RUNS]

  RUNS   runs per configuration/strategy, forwarded to every script (default: 3)

Runs, in order:
  run_q1.sh   RUNS  -> logs/q1_results.log  (CSV runs for DFQ1, UDF, RDD, Parquet runs for DFQ1)
  run_q2.sh   RUNS  -> logs/q2_results.log
  run_q3.sh   RUNS  -> logs/q3_results.log
  run_q4.sh   RUNS  -> logs/q4_results.log  (scalability study)
  run_join.sh both RUNS -> logs/q3_join_results.log, logs/q4_join_results.log

Prerequisite: convert CSV to Parquet once before running run_all.sh:
  spark-submit --conf spark.pyspark.python=python3 \\
    solutions/convert_to_parquet.py --base-path hdfs://.../user/\$DSML_USER

Each script writes to its own log file; this is just a sequencing wrapper.
EOF
    exit 0
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
fi

RUNS=${1:-3}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# "script_name arg1 arg2 ..."
# Q1 runs twice: once with csv (ζητούμενο 2 API comparison) and once with parquet
# (ζητούμενο 1 format comparison). Parquet file must exist on HDFS beforehand.
JOBS=(
    "run_q1.sh $RUNS"
    "run_q2.sh $RUNS"
    "run_q3.sh $RUNS"
    "run_q4.sh $RUNS"
    "run_join.sh both $RUNS"
)

failed=()
for job in "${JOBS[@]}"; do
    read -r name args <<< "$job"
    echo ""
    echo "############################################################"
    echo "# $name $args"
    echo "############################################################"
    if bash "$SCRIPT_DIR/$name" $args; then
        echo "# $name DONE"
    else
        echo "# $name FAILED (exit $?)" >&2
        failed+=("$name")
    fi
done

echo ""
echo "=== run_all summary ==="
if [[ ${#failed[@]} -eq 0 ]]; then
    echo "All scripts completed."
else
    echo "Failed: ${failed[*]}"
    exit 1
fi
