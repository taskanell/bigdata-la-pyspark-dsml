#!/usr/bin/env bash
# Unified join-strategy benchmark for Query 3 and/or Query 4.
#
# Q3 (equi-join on ZIP): all four hint strategies are meaningful.
#   broadcast         -> BroadcastHashJoin
#   merge             -> SortMergeJoin
#   shuffle_hash      -> ShuffledHashJoin
#   shuffle_replicate_nl -> CartesianProduct (hint forces nested-loop on equi-join)
#
# Q4 (non-equi cross join): only nested-loop strategies apply.
#   broadcast         -> BroadcastNestedLoopJoin
#   shuffle_replicate_nl -> CartesianProduct
#   merge / shuffle_hash require equi-keys; Catalyst ignores them silently.
#
# Usage: bash solution_runners/run_join.sh [QUERY] [RUNS]
#   QUERY — 3, 4, or both (default: both)
#   RUNS  — runs per strategy (default: 3)

usage() {
    cat <<EOF
Usage: bash solution_runners/run_join.sh [QUERY] [RUNS]

  QUERY  which query to benchmark: 3, 4, or both (default: both)
  RUNS   number of runs per strategy (default: 3)

Examples:
  bash solution_runners/run_join.sh            # both queries, 3 runs each
  bash solution_runners/run_join.sh 3          # Q3 only, 3 runs
  bash solution_runners/run_join.sh 4          # Q4 only, 3 runs
  bash solution_runners/run_join.sh both 1     # both queries, 1 run (quick sanity check)

Strategies tested:
  Q3 (equi-join on ZIP): broadcast  merge  shuffle_hash  shuffle_replicate_nl
  Q4 (cross join):       broadcast  shuffle_replicate_nl

Results are written to:
  logs/q3_join_results.log  — a Q3-only file
  logs/q4_join_results.log  — a Q4-only file
  (both files written when QUERY=both)
EOF
    exit 0
}

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
fi

QUERY=${1:-both}
RUNS=${2:-3}

BASE_PATH="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER"
NAMESPACE="${DSML_USER}-priv"

LOG_DIR="$(dirname "$0")/../logs"
LOG_FILE=""  # set per query in run_query()

# Q3: equi-join — all four hint strategies produce distinct physical plans
STRATEGIES_Q3=(broadcast merge shuffle_hash shuffle_replicate_nl)
SCRIPT_Q3="solutions/DFQ3.py"
SUBMIT_ARGS_Q3="--conf spark.pyspark.python=python3
  --conf spark.pyspark.driver.python=python3
  --conf spark.executor.instances=3
  --conf spark.executor.cores=1
  --conf spark.executor.memory=2g"

# Q4: non-equi cross join — only the two nested-loop strategies are valid
STRATEGIES_Q4=(broadcast shuffle_replicate_nl)
SCRIPT_Q4="solutions/DFQ4.py"
SUBMIT_ARGS_Q4="--conf spark.pyspark.python=python3
  --conf spark.pyspark.driver.python=python3
  --conf spark.executor.instances=4
  --conf spark.executor.cores=2
  --conf spark.executor.memory=4g"

run_once() {
    local qnum=$1 strategy=$2
    local submit_args script
    if [[ "$qnum" == "3" ]]; then
        submit_args="$SUBMIT_ARGS_Q3"
        script="$SCRIPT_Q3"
    else
        submit_args="$SUBMIT_ARGS_Q4"
        script="$SCRIPT_Q4"
    fi

    SUBMIT_OUT=$(spark-submit $submit_args "$script" \
        --base-path "$BASE_PATH" --join-strategy "$strategy" 2>&1)
    SUBMISSION_ID=$(echo "$SUBMIT_OUT" | grep "submission ID" | grep -o 'submission ID [^ ]*' | awk '{print $3}')
    POD_NAME="${SUBMISSION_ID#*:}"

    if [[ -z "$POD_NAME" ]]; then
        echo "  ERROR: could not get pod name — VPN may have dropped." | tee -a "$LOG_FILE" >&2
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

run_strategy() {
    local qnum=$1 strategy=$2
    echo "" | tee -a "$LOG_FILE"
    echo "========== Q${qnum} STRATEGY: $strategy ==========" | tee -a "$LOG_FILE"

    local times=()
    local i=1
    while [[ ${#times[@]} -lt $RUNS ]]; do
        echo "--- Run $i/$RUNS ---" | tee -a "$LOG_FILE"
        elapsed=$(run_once "$qnum" "$strategy")
        if [[ $? -ne 0 || -z "$elapsed" ]]; then
            echo "  Run failed — reconnect VPN then press Enter to retry." | tee -a "$LOG_FILE" >&2
            read -r
            continue
        fi
        echo "  Elapsed: ${elapsed}s" | tee -a "$LOG_FILE"
        times+=("$elapsed")
        i=$((i + 1))
    done

    mid=$(( (${#times[@]} + 1) / 2 ))
    median=$(printf '%s\n' "${times[@]}" | sort -n | sed -n "${mid}p")
    echo "  >>> Median: ${median}s" | tee -a "$LOG_FILE"
    echo "Q${qnum} $strategy MEDIAN=$median" >> "$LOG_FILE"
}

run_query() {
    local qnum=$1
    LOG_FILE="$LOG_DIR/q${qnum}_join_results.log"
    local strategies
    if [[ "$qnum" == "3" ]]; then
        strategies=("${STRATEGIES_Q3[@]}")
    else
        strategies=("${STRATEGIES_Q4[@]}")
    fi

    > "$LOG_FILE"
    for strategy in "${strategies[@]}"; do
        run_strategy "$qnum" "$strategy"
    done

    echo "" | tee -a "$LOG_FILE"
    echo "=== Q${qnum} summary (median elapsed per join strategy) ===" | tee -a "$LOG_FILE"
    grep "MEDIAN=" "$LOG_FILE"
}

echo "Query             : $QUERY"
echo "Runs per strategy : $RUNS"

if [[ "$QUERY" == "3" || "$QUERY" == "both" ]]; then
    run_query 3
fi
if [[ "$QUERY" == "4" || "$QUERY" == "both" ]]; then
    run_query 4
fi
