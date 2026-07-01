"""ARAMCO ETL -- ADL (Gold) mart DAG.

Triggered automatically once every BDH fact dataset has been refreshed for
the run. Builds the five KPI marts in parallel, then rolls them all up into
the company-wide mart_executive_summary for the BI/exec dashboard.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.datasets import Dataset, DatasetAll
from airflow.utils.task_group import TaskGroup

from common.alerts import alert_on_failure
from common.default_args import DEFAULT_ARGS
from common.spark_operators import build_spark_task

BDH_FACT_DATASETS = DatasetAll(
    Dataset("aramco://bdh/fact_production_daily"),
    Dataset("aramco://bdh/fact_equipment_downtime"),
    Dataset("aramco://bdh/fact_maintenance_workorder"),
    Dataset("aramco://bdh/fact_pipeline_flow"),
    Dataset("aramco://bdh/fact_refinery_yield"),
    Dataset("aramco://bdh/fact_hse_incident"),
    Dataset("aramco://bdh/fact_financial_actuals"),
)

ADL_MART_PRODUCTION_KPI = Dataset("aramco://adl/mart_production_kpi")
ADL_MART_EQUIPMENT_RELIABILITY = Dataset("aramco://adl/mart_equipment_reliability")
ADL_MART_HSE_SCORECARD = Dataset("aramco://adl/mart_hse_scorecard")
ADL_MART_FINANCIAL_COST = Dataset("aramco://adl/mart_financial_cost")
ADL_MART_PIPELINE_INTEGRITY = Dataset("aramco://adl/mart_pipeline_integrity")
ADL_MART_EXECUTIVE_SUMMARY = Dataset("aramco://adl/mart_executive_summary")

BUSINESS_DATE = "{{ ds }}"
RUN_ID = "{{ run_id }}"

with DAG(
    dag_id="aramco_adl_mart",
    description="ADL (Gold) layer -- BI-ready KPI marts and executive roll-up",
    default_args=DEFAULT_ARGS,
    schedule=BDH_FACT_DATASETS,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Riyadh"),
    catchup=False,
    max_active_runs=1,
    tags=["aramco", "adl", "gold"],
    on_failure_callback=alert_on_failure,
) as dag:

    with TaskGroup("build_marts") as build_marts:
        production_kpi = build_spark_task(
            task_id="mart_production_kpi",
            application="aramco_etl/adl/mart_production_kpi.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[ADL_MART_PRODUCTION_KPI],
        )
        build_spark_task(
            task_id="mart_equipment_reliability",
            application="aramco_etl/adl/mart_equipment_reliability.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[ADL_MART_EQUIPMENT_RELIABILITY],
        )
        build_spark_task(
            task_id="mart_hse_scorecard",
            application="aramco_etl/adl/mart_hse_scorecard.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[ADL_MART_HSE_SCORECARD],
        )
        # mart_financial_cost reads adl.mart_production_kpi to compute cost_per_bbl_usd.
        financial_cost = build_spark_task(
            task_id="mart_financial_cost",
            application="aramco_etl/adl/mart_financial_cost.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[ADL_MART_FINANCIAL_COST],
        )
        production_kpi >> financial_cost
        build_spark_task(
            task_id="mart_pipeline_integrity",
            application="aramco_etl/adl/mart_pipeline_integrity.py",
            application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
            outlets=[ADL_MART_PIPELINE_INTEGRITY],
        )

    executive_summary = build_spark_task(
        task_id="mart_executive_summary",
        application="aramco_etl/adl/mart_executive_summary.py",
        application_args=["--business-date", BUSINESS_DATE, "--run-id", RUN_ID],
        outlets=[ADL_MART_EXECUTIVE_SUMMARY],
    )

    build_marts >> executive_summary
