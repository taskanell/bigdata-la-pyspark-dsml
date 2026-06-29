#!/usr/bin/env bash
# One-time CSV->Parquet conversion of the LA Crime dataset on HDFS.
# Run this once before benchmarking DFQ1.py with --format parquet.
#
# Usage: bash scripts/run_convert.sh

BASE_PATH="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER"
NAMESPACE="${DSML_USER}-priv"
LOG_FILE="$(dirname "$0")/../logs/convert_results.log"
CONVERT_SCRIPT="$(dirname "$0")/convert_to_parquet.py"
SUBMIT_ARGS="--conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g"

> "$LOG_FILE"
echo "========== Generating Parquet dataset ==========" | tee -a "$LOG_FILE"

SUBMIT_OUT=$(spark-submit $SUBMIT_ARGS "$CONVERT_SCRIPT" --base-path "$BASE_PATH" 2>&1)
SUBMISSION_ID=$(echo "$SUBMIT_OUT" | grep "submission ID" | grep -o 'submission ID [^ ]*' | awk '{print $3}')
POD_NAME="${SUBMISSION_ID#*:}"

if [[ -z "$POD_NAME" ]]; then
    echo "  ERROR: could not submit convert job — VPN may have dropped." | tee -a "$LOG_FILE" >&2
    echo "$SUBMIT_OUT" >> "$LOG_FILE"
    exit 1
fi

echo "  Pod: $POD_NAME" | tee -a "$LOG_FILE"
echo -n "  Waiting..."
while true; do
    PHASE=$(kubectl -n "$NAMESPACE" get pod "$POD_NAME" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Pending")
    if [[ "$PHASE" == "Succeeded" || "$PHASE" == "Failed" ]]; then
        echo " $PHASE"
        break
    fi
    echo -n "."
    sleep 5
done

kubectl -n "$NAMESPACE" logs "$POD_NAME" >> "$LOG_FILE" 2>&1

if [[ "$PHASE" != "Succeeded" ]]; then
    echo "  ERROR: Parquet generation failed (phase=$PHASE). See $LOG_FILE" >&2
    exit 1
fi
echo "  Parquet dataset ready." | tee -a "$LOG_FILE"
