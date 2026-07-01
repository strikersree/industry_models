"""ARAMCO ETL -- Master backfill DAG (manual trigger only).

The three layer DAGs normally chain via Dataset scheduling for the daily
happy path. For historical reprocessing (e.g. a source replayed a corrected
extract for a past date), trigger this DAG with:

    airflow dags trigger aramco_master_backfill --conf '{"business_date": "2026-06-15"}'

It drives curated -> bdh -> adl for that single business date in strict
sequence, waiting for each layer to finish before starting the next --
Dataset-triggering is bypassed intentionally so a backfill doesn't fire
today's regular runs out of order.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common.default_args import DEFAULT_ARGS

with DAG(
    dag_id="aramco_master_backfill",
    description="Manual full-stack (curated -> bdh -> adl) backfill for a single business date",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Riyadh"),
    catchup=False,
    tags=["aramco", "backfill", "manual"],
    params={"business_date": "2026-01-01"},
) as dag:

    # The child DAGs derive their business date from the run's own logical
    # date (the `ds` macro), not from `conf` -- so the triggered run's
    # logical_date is pinned to params.business_date via `execution_date`.
    trigger_curated = TriggerDagRunOperator(
        task_id="trigger_curated_ingestion",
        trigger_dag_id="aramco_curated_ingestion",
        execution_date="{{ params.business_date }}",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_bdh = TriggerDagRunOperator(
        task_id="trigger_bdh_transform",
        trigger_dag_id="aramco_bdh_transform",
        execution_date="{{ params.business_date }}",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_adl = TriggerDagRunOperator(
        task_id="trigger_adl_mart",
        trigger_dag_id="aramco_adl_mart",
        execution_date="{{ params.business_date }}",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_curated >> trigger_bdh >> trigger_adl
