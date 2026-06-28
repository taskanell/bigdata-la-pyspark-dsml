#!/usr/bin/env bash
# Query 3 join-strategy experiment: runs DFQ3.py once per forced join strategy
# (broadcast, merge, shuffle_hash, shuffle_replicate_nl), 3 runs each,
# reporting the median elapsed time per strategy. The default strategy is not
# benchmarked here; Catalyst already picks BroadcastHashJoin (see DFQ3.py's
# explain output), so 'broadcast' represents the default case.

BASE_PATH="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER"
NAMESPACE="${DSML_USER}-priv"
LOG_FILE="$(dirname "$0")/../solution_logs/q3_join_results.log"
RUNS=${1:-3}
STRATEGIES=(broadcast merge shuffle_hash shuffle_replicate_nl)
SUBMIT_ARGS="--conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g"

# Submits one job, waits for it, logs output, prints elapsed seconds
run_once() {
    local strategy=$1
    SUBMIT_OUT=$(spark-submit $SUBMIT_ARGS solutions/DFQ3.py \
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

for strategy in "${STRATEGIES[@]}"; do
    run_strategy "$strategy"
done

echo ""
echo "=== Summary (median elapsed per join strategy) ==="
grep "MEDIAN=" "$LOG_FILE"
