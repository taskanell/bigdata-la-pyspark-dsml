#!/usr/bin/env bash
# Query 4 scalability study. Runs the chosen implementation across the assignment's
# configurations, 3 runs each, reporting the median elapsed time per configuration.
#
#   Study A (vertical scaling, fixed 2 executors):
#     2 exec x 1 core x 2g  |  2 exec x 2 cores x 4g  |  2 exec x 4 cores x 8g
#   Study B (fixed budget: 8 cores, 16 GB total):
#     2 exec x 4 cores x 8g  |  4 exec x 2 cores x 4g  |  8 exec x 1 core x 2g
#
# The "2 exec x 4 cores x 8g" config is shared (top of A == first of B), so it is
# run once and reused in both tables -> 5 distinct configurations.
#
# Usage: bash solution_runners/run_q4.sh [RUNS]   (default: 3)

SCRIPT="solutions/DFQ4.py"
RUNS=${1:-3}
BASE_PATH="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER"
NAMESPACE="${DSML_USER}-priv"
LOG_FILE="$(dirname "$0")/../solution_logs/q4_results.log"

# "instances cores memory label"
CONFIGS=(
    "2 1 2g A_2x1c_2g"
    "2 2 4g A_2x2c_4g"
    "2 4 8g A3_B1_2x4c_8g"
    "4 2 4g B_4x2c_4g"
    "8 1 2g B_8x1c_2g"
)

# Submits one job with the given resource conf, waits, logs, prints elapsed seconds
run_once() {
    local instances=$1 cores=$2 memory=$3
    local submit_args="--conf spark.pyspark.python=python3 \
        --conf spark.pyspark.driver.python=python3 \
        --conf spark.executor.instances=$instances \
        --conf spark.executor.cores=$cores \
        --conf spark.executor.memory=$memory"

    SUBMIT_OUT=$(spark-submit $submit_args "$SCRIPT" --base-path "$BASE_PATH" 2>&1)
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

# Runs one configuration RUNS times, collects elapsed times, computes median
run_config() {
    local instances=$1 cores=$2 memory=$3 label=$4
    echo "" | tee -a "$LOG_FILE"
    echo "========== $label  (instances=$instances cores=$cores memory=$memory) ==========" | tee -a "$LOG_FILE"

    local times=()
    local i=1
    while [[ ${#times[@]} -lt $RUNS ]]; do
        echo "--- Run $i/$RUNS ---" | tee -a "$LOG_FILE"
        elapsed=$(run_once "$instances" "$cores" "$memory")
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

    # Sort the values and pick the middle one
    median=$(printf '%s\n' "${times[@]}" | sort -n | sed -n '2p')
    echo "  >>> Median: ${median}s" | tee -a "$LOG_FILE"
    echo "$label MEDIAN=$median" >> "$LOG_FILE"
}

> "$LOG_FILE"
echo "Script: $SCRIPT" | tee -a "$LOG_FILE"

for config in "${CONFIGS[@]}"; do
    read -r instances cores memory label <<< "$config"
    run_config "$instances" "$cores" "$memory" "$label"
done

echo ""
echo "=== Summary (median elapsed per configuration) ==="
grep "MEDIAN=" "$LOG_FILE"
