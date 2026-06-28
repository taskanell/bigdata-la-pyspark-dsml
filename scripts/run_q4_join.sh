#!/usr/bin/env bash
# Query 4 join-strategy experiment: runs the chosen implementation once per forced
# join strategy (broadcast, merge, shuffle_hash, shuffle_replicate_nl, default),
# 3 runs each, on a fixed resource config, reporting the median elapsed time.
#
# Q4's join is a non-equi CROSS JOIN, so only nested-loop strategies apply:
#   broadcast            -> BroadcastNestedLoopJoin
#   shuffle_replicate_nl -> CartesianProduct
#   merge / shuffle_hash -> hint ignored (plan falls back to a nested-loop join)
#
# Usage: bash solution_runners/run_q4_join.sh [SCRIPT] [RUNS]  (default: solutions/DFQ4.py, 3)

SCRIPT=${1:-solutions/DFQ4.py}
RUNS=${2:-3}
BASE_PATH="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER"
NAMESPACE="${DSML_USER}-priv"
LOG_FILE="$(dirname "$0")/../solution_logs/q4_join_results.log"
STRATEGIES=(default broadcast merge shuffle_hash shuffle_replicate_nl)

# Fixed resource config for the join-strategy comparison (8 cores, 16 GB total)
SUBMIT_ARGS="--conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.executor.instances=4 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=4g"

# Submits one job, waits for it, logs output, prints elapsed seconds
run_once() {
    local strategy=$1
    SUBMIT_OUT=$(spark-submit $SUBMIT_ARGS "$SCRIPT" \
        --base-path "$BASE_PATH" --join-strategy "$strategy" 2>&1)
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

# Runs one strategy RUNS times, collects elapsed times, computes median
run_strategy() {
    local strategy=$1
    echo "" | tee -a "$LOG_FILE"
    echo "========== STRATEGY: $strategy ==========" | tee -a "$LOG_FILE"

    local times=()
    local i=1
    while [[ ${#times[@]} -lt $RUNS ]]; do
        echo "--- Run $i/$RUNS ---" | tee -a "$LOG_FILE"
        elapsed=$(run_once "$strategy")
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
    echo "$strategy MEDIAN=$median" >> "$LOG_FILE"
}

> "$LOG_FILE"
echo "Script: $SCRIPT" | tee -a "$LOG_FILE"

for strategy in "${STRATEGIES[@]}"; do
    run_strategy "$strategy"
done

echo ""
echo "=== Summary (median elapsed per join strategy) ==="
grep "MEDIAN=" "$LOG_FILE"
