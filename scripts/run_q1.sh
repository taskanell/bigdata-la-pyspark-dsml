#!/usr/bin/env bash
# Query 1 benchmark: DFQ1 (CSV), DFQ1 (Parquet), DFQ1_udf, RddQ1.
# Median elapsed per implementation -> logs/q1_results.log.
# Requires the Parquet dataset to exist (run scripts/run_convert.sh once first).
#
# Usage: bash scripts/run_q1.sh [RUNS]   (default: 3)
#   RUNS — runs per implementation

BASE_PATH="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER"
NAMESPACE="${DSML_USER}-priv"
RUNS=${1:-3}
LOG_FILE="$(dirname "$0")/../logs/q1_results.log"
SUBMIT_ARGS="--conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g"

# Submits one job, waits for it, logs output, prints elapsed seconds
# $1: script path   $2: extra spark-submit args (optional)
run_once() {
    local script=$1
    local extra_args=$2
    SUBMIT_OUT=$(spark-submit $SUBMIT_ARGS "$script" --base-path "$BASE_PATH" $extra_args 2>&1)
    SUBMISSION_ID=$(echo "$SUBMIT_OUT" | grep "submission ID" | grep -o 'submission ID [^ ]*' | awk '{print $3}')
    POD_NAME="${SUBMISSION_ID#*:}"

    if [[ -z "$POD_NAME" ]]; then
        echo "  ERROR: could not get pod name — VPN may have dropped. Skipping run." | tee -a "$LOG_FILE" >&2
        echo "$SUBMIT_OUT" >> "$LOG_FILE"
        return 1
    fi

    echo "  Pod: $POD_NAME" | tee -a "$LOG_FILE" >&2

    echo -n "  Waiting..." >&2
    while true; do
        PHASE=$(kubectl -n "$NAMESPACE" get pod "$POD_NAME" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Pending")
        if [[ "$PHASE" == "Succeeded" || "$PHASE" == "Failed" ]]; then
            echo " $PHASE" >&2
            break
        fi
        echo -n "." >&2
        sleep 5
    done

    POD_LOGS=$(kubectl -n "$NAMESPACE" logs "$POD_NAME" 2>&1)
    echo "$POD_LOGS" >> "$LOG_FILE"
    echo "$POD_LOGS" | grep -oP 'QUERY_ELAPSED_SECONDS=\K[\d.]+'
}

# Runs a script RUNS times, collects elapsed times, computes median
# $1: script path   $2: label   $3: extra spark-submit args (optional)
run_job() {
    local script=$1
    local label=$2
    local extra_args=$3
    echo "" | tee -a "$LOG_FILE"
    echo "========== $label ==========" | tee -a "$LOG_FILE"

    local times=()
    local i=1
    while [[ ${#times[@]} -lt $RUNS ]]; do
        echo "--- Run $i/$RUNS ---" | tee -a "$LOG_FILE"
        elapsed=$(run_once "$script" "$extra_args")
        if [[ $? -ne 0 || -z "$elapsed" ]]; then
            echo "  Run failed, retrying after VPN reconnect..." | tee -a "$LOG_FILE" >&2
            echo "  Reconnect VPN now, then press Enter to retry." >&2
            read -r
            continue
        fi
        echo "  Elapsed: ${elapsed}s" | tee -a "$LOG_FILE"
        times+=("$elapsed")
        i=$((i + 1))
    done

    # Sort the values and pick the middle one (works for any number of runs)
    mid=$(( (${#times[@]} + 1) / 2 ))
    median=$(printf '%s\n' "${times[@]}" | sort -n | sed -n "${mid}p")
    echo "  >>> Median: ${median}s" | tee -a "$LOG_FILE"
    echo "$label MEDIAN=$median" >> "$LOG_FILE"
}

> "$LOG_FILE"

run_job solutions/DFQ1.py     "DataFrame no UDF - CSV"     "--format csv"
run_job solutions/DFQ1.py     "DataFrame no UDF - Parquet" "--format parquet"
run_job solutions/DFQ1_udf.py "DataFrame with UDF"
run_job solutions/RddQ1.py    "RDD"

echo ""
echo "=== Summary ==="
grep "MEDIAN=" "$LOG_FILE"
