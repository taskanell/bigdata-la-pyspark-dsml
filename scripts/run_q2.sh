#!/usr/bin/env bash

BASE_PATH="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER"
NAMESPACE="${DSML_USER}-priv"
LOG_FILE="$(dirname "$0")/../solution_logs/q2_results.log"
RUNS=${1:-3}
SUBMIT_ARGS="--conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.executor.instances=4 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g"

# Submits one job, waits for it, logs output, prints elapsed seconds
run_once() {
    local script=$1
    SUBMIT_OUT=$(spark-submit $SUBMIT_ARGS "$script" --base-path "$BASE_PATH" 2>&1)
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
run_job() {
    local script=$1
    local label=$2
    echo "" | tee -a "$LOG_FILE"
    echo "========== $label ==========" | tee -a "$LOG_FILE"

    local times=()
    local i=1
    while [[ ${#times[@]} -lt $RUNS ]]; do
        echo "--- Run $i/$RUNS ---" | tee -a "$LOG_FILE"
        elapsed=$(run_once "$script")
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

run_job solutions/DFQ2.py  "DataFrame"
run_job solutions/SQLQ2.py "SQL"

echo ""
echo "=== Summary ==="
grep "MEDIAN=" "$LOG_FILE"
