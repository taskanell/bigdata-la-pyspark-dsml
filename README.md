# bigdata-la-pyspark-dsml

PySpark solutions for the NTUA DSML _Large-Scale Data Management_ assignment,
analysing the LA Crime and Income datasets on a Kubernetes Spark cluster (HDFS storage).

## Queries

| Query | Question                                          | Implementations                      |
| ----- | ------------------------------------------------- | ------------------------------------ |
| Q1    | % of street crimes per time-of-day period         | `DFQ1.py`, `DFQ1_udf.py`, `RddQ1.py` |
| Q2    | Top-3 months per year by crime count              | `DFQ2.py`, `SQLQ2.py`                |
| Q3    | Per-capita income per ZIP code (census ⋈ income)  | `DFQ3.py`, `RddQ3.py`                |
| Q4    | Crimes nearest to each police station (Haversine) | `DFQ4.py`                            |

All solution scripts live in [`solutions/`](solutions/).

## Layout

```
solutions/     Spark jobs (DataFrame / SQL / RDD APIs)
scripts/       spark-submit runners + benchmark orchestration
logs/          benchmark output (one log per query)
report.pdf     project report
LLM_USAGE.md   declaration of LLM tool usage per section
```

## Prerequisites

- Access to the cluster (VPN) with `kubectl` configured.
- The LA Crime / census / income datasets present on HDFS under `/data/`.
- Set your cluster username in `~/bigdata-env.sh` (`export DSML_USER=<your-username>`),
  then load the environment in your shell:

  ```bash
  deactivate 2>/dev/null || true   # leave any active Python venv
  source ~/bigdata-env.sh          # DSML_USER, SPARK_HOME, HADOOP_HOME, PATH, ...
  hash -r                          # refresh PATH lookups (spark-submit, kubectl)
  ```

  `bigdata-env.sh` puts `spark-submit`/`kubectl` on `PATH` and exports `DSML_USER`,
  from which the runners derive the HDFS base path (`/user/$DSML_USER`) and the
  Kubernetes namespace (`$DSML_USER-priv`). On first use also generate the Spark
  config: `bigdata_write_spark_defaults`.

## Running

Each runner submits the job(s), waits for the pod(s) to finish, appends the
driver logs to `logs/`, and prints the **median** elapsed time. The optional
argument is the number of runs per configuration (default `3`).

```bash
# One-time CSV -> Parquet conversion (needed before Q1 --format parquet)
bash scripts/run_convert.sh

# Per-query benchmarks
bash scripts/run_q1.sh [RUNS]   # DFQ1 (CSV+Parquet), DFQ1_udf, RddQ1
bash scripts/run_q2.sh [RUNS]   # DFQ2, SQLQ2
bash scripts/run_q3.sh [RUNS]   # DFQ3, RddQ3
bash scripts/run_q4.sh [RUNS]   # DFQ4 scalability study (vertical + horizontal)

# Join-strategy sweep (hint + explain) for Q3 and/or Q4
bash scripts/run_join.sh [3|4|both] [RUNS]

# Everything end to end
bash scripts/run_all.sh [RUNS]
```

### Running a single job directly

```bash
spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  solutions/DFQ1.py --base-path "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER"
```

Useful job flags:

- `DFQ1.py`: `--format {csv,parquet}` — choose the input format.
- `DFQ3.py` / `DFQ4.py`: `--join-strategy {default,broadcast,merge,shuffle_hash,shuffle_replicate_nl}`
  — force a join strategy (the plan is printed via `explain`).
