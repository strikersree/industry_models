# Local Setup Guide

Runs the ARAMCO ETL framework against synthetic sample data on your machine,
then wires it into an existing local Airflow installation (standalone mode).

---

## 1. Prerequisites

- Python 3.10 or 3.11
- Java 11 or 17 (`java -version`) -- required by PySpark
- Outbound internet access to Maven Central (`repo1.maven.org`) the first time
  you run any Spark job, so `delta-spark` can pull the matching Delta Lake
  jars via Ivy. If your network sits behind a proxy, make sure `HTTPS_PROXY`
  is set before running any of the commands below.

## 2. Python environment

```bash
cd aramco-etl
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Set the environment variables every job reads

```bash
export DATALAKE_ROOT="$HOME/aramco_data/datalake"   # where curated/bdh/adl Delta tables live
export LANDING_ROOT="$HOME/aramco_data/landing"      # where "source system" files land
export ARAMCO_ENV=DEV
mkdir -p "$DATALAKE_ROOT" "$LANDING_ROOT"
```

## 4. Generate synthetic sample data

```bash
python scripts/generate_sample_data.py \
    --landing-root "$LANDING_ROOT" \
    --start-date 2026-06-22 \
    --days 10
```

This writes ~10 business days of realistic sample data for all six curated
sources (JSON for SCADA/pipeline/HSE, CSV for Maximo/LIMS, Parquet for SAP),
using fixed entity pools (20 wells, 30 assets, 13 cost centers, 5 refined
products, ...) so the same well/asset/cost-center shows up day after day --
including two deliberate mid-window attribute changes on assets `A3`/`A7`,
so `bdh.dim_asset`'s SCD2 versioning has something real to pick up.

## 5. Run the pipeline directly (no Airflow yet)

This is the fastest way to confirm everything works end-to-end -- it calls
the exact same job functions Airflow will call, just in-process:

```bash
python scripts/run_local_pipeline.py \
    --start-date 2026-06-22 \
    --days 10 \
    --run-id local-dev-1
```

It will:
1. Create the curated/BDH/ADL schemas (`sql/curated`, `sql/bdh`, `sql/adl`) if they don't exist yet.
2. Seed `bdh.dim_date` for 2020-2035.
3. For each business date: run curated ingestion (6 sources) -> BDH dimensions -> BDH facts (DQ-gated) -> ADL marts.
4. Print row counts for every BDH/ADL table at the end.

Re-running it is safe (idempotent) -- add `--skip-ddl` to skip re-creating
the schemas on subsequent runs.

Poke around the results with a quick PySpark shell:
```bash
python -c "
from aramco_etl.common.spark_session import get_spark_session
spark = get_spark_session('explore')
spark.table('adl.mart_executive_summary').show()
spark.table('adl.mart_production_kpi').orderBy('date_key').show()
spark.table('bdh.dim_asset').filter('asset_id in (\"A3\",\"A7\")').orderBy('asset_id','row_eff_date').show()
"
```
(run from the `aramco-etl` directory with `DATALAKE_ROOT`/`LANDING_ROOT` still exported)

## 6. Wire it into your existing local Airflow

You mentioned Airflow standalone is already set up at `/Users/strikersree/airflow_local`. Point it at this repo:

```bash
export AIRFLOW_HOME=/Users/strikersree/airflow_local

# Symlink the DAGs in rather than moving them, so `git pull` here keeps them in sync
ln -s "$(pwd)/dags" "$AIRFLOW_HOME/dags/aramco-etl"

# The DAGs resolve the repo path for spark-submit from this env var --
# add it wherever Airflow's scheduler/webserver process gets its environment
# (e.g. your shell profile, or airflow_local/airflow.cfg's env_vars if you use one)
export ARAMCO_REPO_ROOT="$(pwd)"
```

**Airflow Variables** (used by `dags/common/spark_operators.py` at task run time):
```bash
airflow variables set aramco_datalake_root "$DATALAKE_ROOT"
airflow variables set aramco_landing_root "$LANDING_ROOT"
airflow variables set aramco_env DEV
```

**Spark connection** -- the DAGs use `conn_id="spark_default"`. For a local, in-process Spark (no standalone cluster), point it at `local[*]`:
```bash
airflow connections add spark_default \
    --conn-type spark \
    --conn-host 'local[*]' \
    --conn-extra '{"queue": "default", "deploy-mode": "client"}' \
    2>/dev/null || \
airflow connections delete spark_default && airflow connections add spark_default \
    --conn-type spark --conn-host 'local[*]'
```
Make sure the same Python environment that has `pyspark`/`delta-spark` installed (step 2) is the one your Airflow scheduler runs in, so `spark-submit` is on `PATH`.

**Start (or restart) Airflow standalone**, then in the UI:
1. Confirm `aramco_curated_ingestion`, `aramco_bdh_transform`, `aramco_adl_mart`, and `aramco_master_backfill` all appear (Airflow needs `apache-airflow-providers-apache-spark` installed -- it's in `requirements.txt`).
2. Unpause `aramco_curated_ingestion`. `aramco_bdh_transform` and `aramco_adl_mart` are Dataset-triggered (`DatasetAll`) -- they fire automatically once all of their upstream datasets have been produced, no manual trigger needed.
3. To process one of the sample business dates end-to-end through real Airflow: trigger `aramco_master_backfill` with `{"business_date": "2026-06-25"}` (or any date you generated), which drives curated -> bdh -> adl in strict sequence and waits for each layer.

Requires Airflow **2.9+** for `DatasetAll` (checked via `airflow version`); on an older Airflow, replace the `DatasetAll(...)` schedules in `dags/aramco_bdh_transform_dag.py` / `dags/aramco_adl_mart_dag.py` with a `schedule_interval` and a `TriggerDagRunOperator` chain instead (same pattern as `aramco_master_backfill_dag.py`).

## Troubleshooting

- **`Unable to resolve io.delta:delta-spark_...` / hangs on Ivy resolution**: no network access to Maven Central, or a proxy is blocking it. Set `HTTPS_PROXY`/`HTTP_PROXY` as needed.
- **`SCHEMA_NOT_FOUND` when re-running DDL**: harmless if it's `DROP ... IF EXISTS` noise; if it's on a `CREATE`/`USE`, check `DATALAKE_ROOT` is actually exported in the shell that's running the job.
- **A curated ingestion task logs a WARNING and skips a table**: expected when a source genuinely has no file for that business date (see `base_ingestion.py._path_exists`) -- not an error.
- **A BDH fact task fails with `DQ gate FAILED`**: expected/by design when the batch's pass rate drops below `config/dq_thresholds.yaml` -- check `bdh.err_reject_log` for the rejected rows and `bdh.dq_audit_log` for the score.
