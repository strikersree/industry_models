"""ARAMCO ETL -- BDH (Silver) transform DAG.

Triggered automatically once *all six* curated datasets have been refreshed
by aramco_curated_ingestion_dag (DatasetAll = AND-logic, not OR). Builds
SCD2/Type-1 dimensions first, then DQ-gated facts. A fact build task fails
its task (and blocks the corresponding BDH dataset + downstream ADL DAG)
if its DQ pass rate drops below the threshold in config/dq_thresholds.yaml
-- mirroring the STC framework's "halt the batch below 97% DQ" pattern.

dim_date is a slow-changing static calendar and is refreshed by a separate,
manually/quarterly-triggered maintenance DAG rather than on every run.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.datasets import Dataset, DatasetAll
from airflow.utils.task_group import TaskGroup

from common.alerts import alert_on_failure
from common.default_args import DEFAULT_ARGS
from common.spark_operators import build_spark_task

CURATED_DATASETS = DatasetAll(
    Dataset("aramco://curated/scada_upstream"),
    Dataset("aramco://curated/maximo_eam"),
    Dataset("aramco://curated/sap_erp"),
    Dataset("aramco://curated/pipeline_scada"),
    Dataset("aramco://curated/refinery_lims"),
    Dataset("aramco://curated/hse"),
)

BDH_DIM_ASSET = Dataset("aramco://bdh/dim_asset")
BDH_DIM_WELL_FIELD = Dataset("aramco://bdh/dim_well_field")
BDH_DIM_COST_CENTER = Dataset("aramco://bdh/dim_cost_center")
BDH_DIM_PRODUCT = Dataset("aramco://bdh/dim_product")
BDH_DIM_EMPLOYEE_VENDOR = Dataset("aramco://bdh/dim_employee_vendor")

BDH_FACT_PRODUCTION_DAILY = Dataset("aramco://bdh/fact_production_daily")
BDH_FACT_EQUIPMENT_DOWNTIME = Dataset("aramco://bdh/fact_equipment_downtime")
BDH_FACT_MAINTENANCE_WORKORDER = Dataset("aramco://bdh/fact_maintenance_workorder")
BDH_FACT_PIPELINE_FLOW = Dataset("aramco://bdh/fact_pipeline_flow")
BDH_FACT_REFINERY_YIELD = Dataset("aramco://bdh/fact_refinery_yield")
BDH_FACT_HSE_INCIDENT = Dataset("aramco://bdh/fact_hse_incident")
BDH_FACT_FINANCIAL_ACTUALS = Dataset("aramco://bdh/fact_financial_actuals")

BUSINESS_DATE = "{{ ds }}"
RUN_ID = "{{ run_id }}"

with DAG(
    dag_id="aramco_bdh_transform",
    description="BDH (Silver) layer -- conformed dimensions & DQ-gated facts",
    default_args=DEFAULT_ARGS,
    schedule=CURATED_DATASETS,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Riyadh"),
    catchup=False,
    max_active_runs=1,
    tags=["aramco", "bdh", "silver"],
    on_failure_callback=alert_on_failure,
) as dag:

    with TaskGroup("build_dimensions") as build_dimensions:
        build_spark_task(
            task_id="build_dim_asset",
            application="aramco_etl/bdh/build_dim_asset.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_DIM_ASSET],
        )
        build_spark_task(
            task_id="build_dim_well_field",
            application="aramco_etl/bdh/build_dim_well_field.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_DIM_WELL_FIELD],
        )
        build_spark_task(
            task_id="build_dim_cost_center",
            application="aramco_etl/bdh/build_dim_cost_center.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_DIM_COST_CENTER],
        )
        build_spark_task(
            task_id="build_dim_product",
            application="aramco_etl/bdh/build_dim_product.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_DIM_PRODUCT],
        )
        build_spark_task(
            task_id="build_dim_employee_vendor",
            application="aramco_etl/bdh/build_dim_employee_vendor.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_DIM_EMPLOYEE_VENDOR],
        )

    with TaskGroup("build_facts") as build_facts:
        build_spark_task(
            task_id="build_fact_production_daily",
            application="aramco_etl/bdh/build_fact_production_daily.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_FACT_PRODUCTION_DAILY],
        )
        build_spark_task(
            task_id="build_fact_equipment_downtime",
            application="aramco_etl/bdh/build_fact_equipment_downtime.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_FACT_EQUIPMENT_DOWNTIME],
        )
        build_spark_task(
            task_id="build_fact_maintenance_workorder",
            application="aramco_etl/bdh/build_fact_maintenance_workorder.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_FACT_MAINTENANCE_WORKORDER],
        )
        build_spark_task(
            task_id="build_fact_pipeline_flow",
            application="aramco_etl/bdh/build_fact_pipeline_flow.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_FACT_PIPELINE_FLOW],
        )
        build_spark_task(
            task_id="build_fact_refinery_yield",
            application="aramco_etl/bdh/build_fact_refinery_yield.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_FACT_REFINERY_YIELD],
        )
        build_spark_task(
            task_id="build_fact_hse_incident",
            application="aramco_etl/bdh/build_fact_hse_incident.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_FACT_HSE_INCIDENT],
        )
        build_spark_task(
            task_id="build_fact_financial_actuals",
            application="aramco_etl/bdh/build_fact_financial_actuals.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[BDH_FACT_FINANCIAL_ACTUALS],
        )

    build_dimensions >> build_facts
