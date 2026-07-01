"""ARAMCO ETL -- CURATED (Bronze) ingestion DAG.

Runs one Spark ingestion job per source system in parallel, each landing
into its own curated_<source> schema. Each task emits a dataset on success;
the BDH transform DAG (aramco_bdh_transform_dag.py) is scheduled on those
datasets, so it fires automatically once all six sources have landed for
the day -- no manual TriggerDagRunOperator wiring required.

Schedule: daily at 01:00 AST, after the overnight source-system export
windows close (mirrors the STC Ab Initio batch window pattern).
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup

from common.alerts import alert_on_failure
from common.default_args import DEFAULT_ARGS
from common.spark_operators import build_spark_task

CURATED_SCADA_UPSTREAM = Dataset("aramco://curated/scada_upstream")
CURATED_MAXIMO_EAM = Dataset("aramco://curated/maximo_eam")
CURATED_SAP_ERP = Dataset("aramco://curated/sap_erp")
CURATED_PIPELINE_SCADA = Dataset("aramco://curated/pipeline_scada")
CURATED_REFINERY_LIMS = Dataset("aramco://curated/refinery_lims")
CURATED_HSE = Dataset("aramco://curated/hse")

BUSINESS_DATE = "{{ ds }}"
RUN_ID = "{{ run_id }}"

with DAG(
    dag_id="aramco_curated_ingestion",
    description="CURATED (Bronze) layer -- per-source ingestion into curated_<source> schemas",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Riyadh"),
    catchup=False,
    max_active_runs=1,
    tags=["aramco", "curated", "bronze"],
    on_failure_callback=alert_on_failure,
) as dag:

    with TaskGroup("ingest_sources") as ingest_sources:
        build_spark_task(
            task_id="ingest_scada_upstream",
            application="aramco_etl/curated/ingest_scada_upstream.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[CURATED_SCADA_UPSTREAM],
        )
        build_spark_task(
            task_id="ingest_maximo_eam",
            application="aramco_etl/curated/ingest_maximo_eam.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[CURATED_MAXIMO_EAM],
        )
        build_spark_task(
            task_id="ingest_sap_erp",
            application="aramco_etl/curated/ingest_sap_erp.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[CURATED_SAP_ERP],
        )
        build_spark_task(
            task_id="ingest_pipeline_scada",
            application="aramco_etl/curated/ingest_pipeline_scada.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[CURATED_PIPELINE_SCADA],
        )
        build_spark_task(
            task_id="ingest_refinery_lims",
            application="aramco_etl/curated/ingest_refinery_lims.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[CURATED_REFINERY_LIMS],
        )
        build_spark_task(
            task_id="ingest_hse",
            application="aramco_etl/curated/ingest_hse.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[CURATED_HSE],
        )
