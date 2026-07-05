"""Runs the full CURATED -> BDH -> ADL pipeline locally, in-process (one
shared SparkSession, no spark-submit / Airflow required).

Intended for local development and validation before wiring the same job
code into Airflow (see dags/) via SparkSubmitOperator. Idempotent: safe to
re-run for the same date range.

    python scripts/run_local_pipeline.py \\
        --start-date 2026-06-22 --days 10 --run-id local-dev-1

Requires DATALAKE_ROOT and LANDING_ROOT to already be set in the
environment (or pass --datalake-root / --landing-root), and the landing
folder to already be populated (see scripts/generate_sample_data.py).
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")


def daterange(start_date: str, days: int):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]


def run_ddl_setup(spark) -> None:
    print("=== Setting up curated / BDH / ADL schemas (DDL) ===")
    sql_files = (
        sorted(glob.glob(os.path.join(REPO_ROOT, "sql/curated/*.sql")))
        + sorted(glob.glob(os.path.join(REPO_ROOT, "sql/bdh/*.sql")))
        + sorted(glob.glob(os.path.join(REPO_ROOT, "sql/adl/*.sql")))
    )
    for path in sql_files:
        with open(path, "r", encoding="utf-8") as fh:
            text = os.path.expandvars(fh.read())
        for statement in text.split(";"):
            # A chunk can start with a "-- comment" line and still contain
            # real SQL after it; Spark's parser strips comments itself.
            statement = statement.strip()
            if statement:
                spark.sql(statement)
        print(f"  OK: {os.path.relpath(path, REPO_ROOT)}")


def run_curated_ingestion(business_date: str, run_id: str) -> None:
    from aramco_etl.curated.ingest_hse import HseIngestionJob
    from aramco_etl.curated.ingest_maximo_eam import MaximoEamIngestionJob
    from aramco_etl.curated.ingest_pipeline_scada import PipelineScadaIngestionJob
    from aramco_etl.curated.ingest_refinery_lims import RefineryLimsIngestionJob
    from aramco_etl.curated.ingest_sap_erp import SapErpIngestionJob
    from aramco_etl.curated.ingest_scada_upstream import ScadaUpstreamIngestionJob

    for job_cls in [
        ScadaUpstreamIngestionJob, MaximoEamIngestionJob, SapErpIngestionJob,
        PipelineScadaIngestionJob, RefineryLimsIngestionJob, HseIngestionJob,
    ]:
        job_cls(business_date=business_date, run_id=run_id).run()


def run_bdh_dimensions(business_date: str, run_id: str) -> None:
    from aramco_etl.bdh.build_dim_asset import build_dim_asset
    from aramco_etl.bdh.build_dim_cost_center import build_dim_cost_center
    from aramco_etl.bdh.build_dim_employee_vendor import build_dim_employee_vendor
    from aramco_etl.bdh.build_dim_product import build_dim_product
    from aramco_etl.bdh.build_dim_well_field import build_dim_well_field

    build_dim_asset(business_date, run_id)
    build_dim_well_field(business_date, run_id)
    build_dim_cost_center(business_date, run_id)
    build_dim_product(business_date, run_id)
    build_dim_employee_vendor(business_date, run_id)


def run_bdh_facts(business_date: str, run_id: str) -> None:
    from aramco_etl.bdh.build_fact_equipment_downtime import build_fact_equipment_downtime
    from aramco_etl.bdh.build_fact_financial_actuals import build_fact_financial_actuals
    from aramco_etl.bdh.build_fact_hse_incident import build_fact_hse_incident
    from aramco_etl.bdh.build_fact_maintenance_workorder import build_fact_maintenance_workorder
    from aramco_etl.bdh.build_fact_pipeline_flow import build_fact_pipeline_flow
    from aramco_etl.bdh.build_fact_production_daily import build_fact_production_daily
    from aramco_etl.bdh.build_fact_refinery_yield import build_fact_refinery_yield

    fact_builders = [
        ("fact_production_daily", build_fact_production_daily),
        ("fact_equipment_downtime", build_fact_equipment_downtime),
        ("fact_maintenance_workorder", build_fact_maintenance_workorder),
        ("fact_pipeline_flow", build_fact_pipeline_flow),
        ("fact_refinery_yield", build_fact_refinery_yield),
        ("fact_hse_incident", build_fact_hse_incident),
        ("fact_financial_actuals", build_fact_financial_actuals),
    ]
    for name, builder in fact_builders:
        try:
            builder(business_date, run_id)
        except RuntimeError as exc:
            print(f"  DQ GATE FAILED for {name} on {business_date}: {exc}")


def run_adl_marts(business_date: str, run_id: str) -> None:
    from aramco_etl.adl.mart_equipment_reliability import build_mart_equipment_reliability
    from aramco_etl.adl.mart_financial_cost import build_mart_financial_cost
    from aramco_etl.adl.mart_hse_scorecard import build_mart_hse_scorecard
    from aramco_etl.adl.mart_pipeline_integrity import build_mart_pipeline_integrity
    from aramco_etl.adl.mart_production_kpi import build_mart_production_kpi

    build_mart_production_kpi(business_date, run_id)
    build_mart_equipment_reliability(business_date, run_id)
    build_mart_hse_scorecard(business_date, run_id)
    build_mart_financial_cost(business_date, run_id)  # depends on mart_production_kpi above
    build_mart_pipeline_integrity(business_date, run_id)


def run_adl_executive_summary(business_date: str, run_id: str) -> None:
    from aramco_etl.adl.mart_executive_summary import build_mart_executive_summary
    build_mart_executive_summary(business_date, run_id)


def print_summary(spark) -> None:
    print("\n=== Row counts ===")
    for schema, tables in [
        ("bdh", [
            "dim_date", "dim_asset", "dim_well_field", "dim_cost_center", "dim_product",
            "dim_employee_vendor", "fact_production_daily", "fact_equipment_downtime",
            "fact_maintenance_workorder", "fact_pipeline_flow", "fact_refinery_yield",
            "fact_hse_incident", "fact_financial_actuals", "dq_audit_log", "err_reject_log",
        ]),
        ("adl", [
            "mart_production_kpi", "mart_equipment_reliability", "mart_hse_scorecard",
            "mart_financial_cost", "mart_pipeline_integrity", "mart_executive_summary",
        ]),
    ]:
        for table in tables:
            full_name = f"{schema}.{table}"
            if spark.catalog.tableExists(full_name):
                print(f"  {full_name}: {spark.table(full_name).count()} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the ARAMCO ETL pipeline locally, in-process")
    parser.add_argument("--start-date", required=True, help="yyyy-MM-dd, first business date to process")
    parser.add_argument("--days", type=int, default=10, help="Number of consecutive business dates to process")
    parser.add_argument("--run-id", default="local-dev", help="Correlation id stamped on every job run")
    parser.add_argument("--datalake-root", help="Overrides DATALAKE_ROOT env var")
    parser.add_argument("--landing-root", help="Overrides LANDING_ROOT env var")
    parser.add_argument("--skip-ddl", action="store_true", help="Skip DDL setup (schemas already created)")
    args = parser.parse_args()

    if args.datalake_root:
        os.environ["DATALAKE_ROOT"] = args.datalake_root
    if args.landing_root:
        os.environ["LANDING_ROOT"] = args.landing_root
    for var in ("DATALAKE_ROOT", "LANDING_ROOT"):
        if var not in os.environ:
            raise SystemExit(f"{var} must be set (env var or --{var.lower().replace('_', '-')})")

    from aramco_etl.bdh.build_dim_date import build_dim_date
    from aramco_etl.common.spark_session import get_spark_session

    spark = get_spark_session("aramco_run_local_pipeline")

    if not args.skip_ddl:
        run_ddl_setup(spark)
        build_dim_date("2020-01-01", "2035-12-31", args.run_id)

    dates = daterange(args.start_date, args.days)
    for business_date in dates:
        print(f"\n=== business_date={business_date} ===")
        print("-- curated ingestion --")
        run_curated_ingestion(business_date, args.run_id)
        print("-- bdh dimensions --")
        run_bdh_dimensions(business_date, args.run_id)
        print("-- bdh facts --")
        run_bdh_facts(business_date, args.run_id)
        print("-- adl marts --")
        run_adl_marts(business_date, args.run_id)

    # Executive summary rolls up the whole month-to-date; run once for the last date processed.
    run_adl_executive_summary(dates[-1], args.run_id)

    print_summary(spark)
    print(f"\nDone. Processed {len(dates)} business dates ({dates[0]} .. {dates[-1]}).")


if __name__ == "__main__":
    main()
