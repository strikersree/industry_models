# ARAMCO Enterprise ETL Framework
### PySpark + SQL (Delta Lake) | Medallion Architecture | Apache Airflow Orchestration

---

## Overview

This is a PySpark/SQL ETL framework for a Saudi Aramco-style oil & gas enterprise data platform, built on a **medallion (Bronze / Silver / Gold) architecture** on **Delta Lake**, orchestrated with **Apache Airflow**. It covers representative upstream, midstream, downstream, maintenance, HSE, and finance data domains.

| Attribute | Detail |
|---|---|
| **Compute** | Apache Spark 3.5 (PySpark) + Spark SQL |
| **Storage format** | Delta Lake |
| **Orchestration** | Apache Airflow 2.9 (Dataset-driven scheduling) |
| **Architecture** | Medallion: Curated (Bronze) -> BDH (Silver) -> ADL (Gold) |
| **Domain** | Upstream production, midstream pipelines, downstream refining, maintenance (EAM), HSE, finance |

> **Naming note:** this framework uses Aramco-style internal layer names throughout the code, SQL, and DAGs: **Curated** = Bronze, **BDH** (Business Data Hub) = Silver, **ADL** (Analytics Data Layer) = Gold. These are illustrative labels for this reference implementation, not a reproduction of any proprietary Aramco system design.

---

## Architecture

```
 ┌────────────────────────────────────────────────────────────────────┐
 │  SOURCE SYSTEMS                                                    │
 │  SCADA/OSIsoft PI (upstream) │ Pipeline SCADA (midstream)          │
 │  Refinery LIMS (downstream)  │ Maximo EAM │ SAP ERP │ HSE          │
 └───────────────────────────────┬──────────────────────────────────-┘
                                  │  PySpark ingestion jobs (per source)
                                  ▼
 ┌────────────────────────────────────────────────────────────────────┐
 │  CURATED (Bronze) -- one schema per source, raw/source-shaped      │
 │  curated_scada_upstream │ curated_maximo_eam │ curated_sap_erp     │
 │  curated_pipeline_scada │ curated_refinery_lims │ curated_hse      │
 └───────────────────────────────┬──────────────────────────────────-┘
                                  │  DQ-gated PySpark transforms (SCD2 / Type-1 dims, facts)
                                  ▼
 ┌────────────────────────────────────────────────────────────────────┐
 │  BDH (Silver) -- Business Data Hub: conformed dims + facts         │
 │  Single `bdh` schema. Rows failing a REJECT-severity DQ rule are   │
 │  diverted to bdh.err_reject_log; the batch fails if the pass rate  │
 │  drops below config/dq_thresholds.yaml.                            │
 └───────────────────────────────┬──────────────────────────────────-┘
                                  │  PySpark KPI aggregation
                                  ▼
 ┌────────────────────────────────────────────────────────────────────┐
 │  ADL (Gold) -- Analytics Data Layer: BI-ready KPI marts            │
 │  Single `adl` schema. Production, reliability, HSE, financial,     │
 │  pipeline-integrity marts + a company-wide executive summary.      │
 └────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
aramco-etl/
├── README.md
├── requirements.txt
├── pytest.ini
├── config/
│   ├── sources.yaml            <- source registry: curated schema, landing path, format per source
│   ├── dq_thresholds.yaml      <- DQ pass-rate gate per BDH/ADL target table
│   └── spark-defaults.conf     <- shared Spark/Delta runtime configuration
├── sql/
│   ├── curated/                <- one DDL file per source, each creating its own schema
│   ├── bdh/                    <- conformed dimensions, facts, DQ audit log
│   └── adl/                    <- KPI mart tables + executive views
├── src/aramco_etl/
│   ├── common/                 <- Spark session, config, Delta I/O, SCD2, Type-1 dim upsert, DQ engine
│   ├── curated/                <- one ingestion job per source (base_ingestion.py + subclasses)
│   ├── bdh/                    <- dimension builders (SCD2 / Type-1) + DQ-gated fact builders
│   └── adl/                    <- KPI mart builders + executive summary roll-up
├── dags/
│   ├── common/                 <- default_args, SparkSubmitOperator factory, failure-alert callback
│   ├── aramco_curated_ingestion_dag.py
│   ├── aramco_bdh_transform_dag.py
│   ├── aramco_adl_mart_dag.py
│   └── aramco_master_backfill_dag.py
└── tests/                      <- pytest unit tests for common/ (DQ engine, SCD2, Type-1 upsert)
```

---

## Source Systems (Curated / Bronze layer)

Each source lands in its **own Spark/Delta schema**, so access control, retention, and schema evolution are managed independently per source (see `config/sources.yaml`):

| Source | Curated Schema | Domain | Tables |
|---|---|---|---|
| SCADA / OSIsoft PI | `curated_scada_upstream` | Upstream | well production readings, field header pressure, wellhead events |
| IBM Maximo (EAM) | `curated_maximo_eam` | Maintenance | asset master, work orders, downtime events |
| SAP ERP | `curated_sap_erp` | Finance | GL journal, purchase orders, cost-center actuals |
| Pipeline SCADA | `curated_pipeline_scada` | Midstream | pipeline flow readings, leak-detection alarms |
| Refinery LIMS | `curated_refinery_lims` | Downstream | crude assay, product quality tests |
| HSE systems | `curated_hse` | HSE | incident reports, safety observations |

Every curated table carries standard audit columns: `_ingest_ts`, `_source_file`, `_business_date`, `_ingestion_run_id`, and is partitioned by `_business_date`.

---

## BDH (Silver) -- Business Data Hub

Single `bdh` schema conforming all sources into a shared dimensional model.

**Dimensions:**

| Table | Type | Source |
|---|---|---|
| `dim_date` | Static calendar | Generated |
| `dim_asset` | SCD2 | Maximo asset master |
| `dim_well_field` | SCD2 | SCADA upstream well/field identifiers |
| `dim_cost_center` | Type-1 | SAP cost-center actuals |
| `dim_product` | Type-1 | Refinery LIMS product codes |
| `dim_employee_vendor` | Type-1 | Maximo crews + HSE involved employees |

**Facts** (one BDH fact table per source domain, DQ-gated on write): `fact_production_daily`, `fact_equipment_downtime`, `fact_maintenance_workorder`, `fact_pipeline_flow`, `fact_refinery_yield`, `fact_hse_incident`, `fact_financial_actuals`.

**SCD2 pattern** (`src/aramco_etl/common/scd2.py`), mirrored from classic Kimball SCD2:
```
1. change_hash = sha2(concat_ws(tracked columns))
2. Compare incoming row's hash against the current_row_flag='Y' row for the same business key
3. Hash differs  -> expire old row (current_row_flag='N', row_exp_date = as_of_date - 1)
                     insert new version (current_row_flag='Y', row_eff_date = as_of_date)
4. New business key -> insert first version
5. Hash unchanged -> no-op
```

**Data Quality Gate** (`src/aramco_etl/common/data_quality.py`, `src/aramco_etl/bdh/dq_gate.py`): each fact builder runs a set of `REJECT` / `WARN` / `AUTO_CORRECT` rules against the incoming batch before writing:
- `REJECT` rows are excluded from the target table and written to `bdh.err_reject_log` with the full row payload and reason.
- The computed pass rate is written to `bdh.dq_audit_log`.
- If the pass rate is below the threshold configured in `config/dq_thresholds.yaml` for that table, the job raises and the Airflow task fails -- blocking that fact's dataset and, transitively, the downstream ADL DAG until it's fixed and rerun.

---

## ADL (Gold) -- Analytics Data Layer

Single `adl` schema of BI-ready KPI marts, built from BDH facts/dimensions:

| Mart | Grain | Key KPIs |
|---|---|---|
| `mart_production_kpi` | field x day | production efficiency %, uptime % |
| `mart_equipment_reliability` | asset x month | MTBF, MTTR, availability % |
| `mart_hse_scorecard` | facility x month | TRIR, LTIR (OSHA-style incidence rate) |
| `mart_financial_cost` | cost center x month | actual vs. budget variance, cost per barrel |
| `mart_pipeline_integrity` | pipeline segment x month | leak-alarm rate per 1,000 bbl |
| `mart_executive_summary` | company x month | roll-up of all the above for the exec dashboard |

> `mart_hse_scorecard.labor_hours_worked` and `mart_financial_cost.cost_per_bbl_usd` are proxied from booked maintenance labor hours and total upstream production respectively, since no payroll/timekeeping or per-cost-center production feed is modelled here. Replace these proxies with real source feeds before using TRIR/LTIR or cost-per-bbl for regulatory or financial reporting.

---

## Airflow Orchestration

Three layer DAGs are chained via **Airflow Datasets** (not manual `TriggerDagRunOperator` wiring): each curated ingestion task declares an output `Dataset`; `aramco_bdh_transform` is scheduled on `DatasetAll(...)` of all six curated datasets (AND-logic, not OR), and `aramco_adl_mart` is scheduled the same way on all seven BDH fact datasets.

| DAG | Trigger | What it does |
|---|---|---|
| `aramco_curated_ingestion` | `0 1 * * *` (daily, 01:00 AST) | One Spark job per source, in parallel, into `curated_<source>` |
| `aramco_bdh_transform` | Dataset (all 6 curated datasets) | Builds dimensions, then DQ-gated facts |
| `aramco_adl_mart` | Dataset (all 7 BDH fact datasets) | Builds 5 KPI marts in parallel, then the executive summary |
| `aramco_master_backfill` | Manual only | Drives curated -> bdh -> adl in strict sequence for one historical `business_date` |

`dim_date` is a slow-changing static calendar and is refreshed out-of-band (quarterly/manually via `src/aramco_etl/bdh/build_dim_date.py`), not on every daily run.

Failure handling mirrors the "halt the batch below threshold" pattern: a DQ-gate failure raises in the Spark job, which fails the Airflow task, which (via `on_failure_callback=alert_on_failure` in `dags/common/alerts.py`, plus the standard `email_on_failure` default) alerts Data Engineering and blocks downstream datasets from firing.

---

## Local Development

See [`LOCAL_SETUP.md`](./LOCAL_SETUP.md) for generating synthetic sample data, running the full pipeline in-process without Airflow (`scripts/run_local_pipeline.py`), and wiring the DAGs into a local Airflow standalone install.

## Deployment

**Prerequisites:**
- Apache Spark 3.5.x with Delta Lake 3.1.x (`io.delta:delta-spark_2.12:3.1.0`)
- Apache Airflow 2.9.x with `apache-airflow-providers-apache-spark` and a configured `spark_default` connection
- A Spark-submit-reachable path exposing this repo's `src/` and `config/` (referenced as `/opt/airflow/aramco-etl` in `dags/common/spark_operators.py` -- adjust to your deployment layout)
- Airflow Variables: `aramco_datalake_root`, `aramco_landing_root`, and optionally `aramco_env` (defaults to `DEV`)

**Initial setup (run once per environment):**
```bash
# 1. Create the curated (Bronze) schemas, one per source
spark-sql -f sql/curated/01_schema_scada_upstream.sql
spark-sql -f sql/curated/02_schema_maximo_eam.sql
spark-sql -f sql/curated/03_schema_sap_erp.sql
spark-sql -f sql/curated/04_schema_pipeline_scada.sql
spark-sql -f sql/curated/05_schema_refinery_lims.sql
spark-sql -f sql/curated/06_schema_hse.sql

# 2. Create the BDH (Silver) schema: dimensions, facts, DQ audit tables
spark-sql -f sql/bdh/07_bdh_dimensions.sql
spark-sql -f sql/bdh/08_bdh_facts.sql
spark-sql -f sql/bdh/09_bdh_dq_rules.sql

# 3. Create the ADL (Gold) schema: KPI marts and executive views
spark-sql -f sql/adl/10_adl_marts.sql
spark-sql -f sql/adl/11_adl_views.sql

# 4. Seed the static calendar dimension
spark-submit src/aramco_etl/bdh/build_dim_date.py --start-date 2020-01-01 --end-date 2035-12-31 --run-id init
```

Then deploy `dags/` into your Airflow `DAGS_FOLDER` and unpause `aramco_curated_ingestion` -- the BDH and ADL DAGs will follow automatically via Dataset scheduling.

**Backfilling a historical date:**
```bash
airflow dags trigger aramco_master_backfill --conf '{"business_date": "2026-06-15"}'
```

---

## Testing

```bash
pip install -r requirements.txt
pytest
```

Unit tests (`tests/`) run against a local, non-Hive Spark session with Delta Lake enabled (no external metastore required) and cover the reusable engine pieces: the DQ rule engine (`test_data_quality.py`), the SCD2 merge (`test_scd2.py`), and the Type-1 dimension upsert (`test_type1_dim.py`). For full curated -> BDH -> ADL integration testing against synthetic sample data, see [`LOCAL_SETUP.md`](./LOCAL_SETUP.md).

---

## Data Governance

| Concern | Treatment |
|---|---|
| Data retention | Curated: source-dependent rolling retention; BDH facts/dims: long-term for point-in-time (SCD2) and regulatory reporting; `err_reject_log` / `dq_audit_log`: retained for DQ triage and audit trail |
| Access control | Schema-per-source in Curated allows independent grants per source-system owner; `bdh` and `adl` schemas can be granted more broadly to analytics/BI roles |
| Data quality | Every BDH fact is DQ-gated on write (see above); rejects are never silently dropped -- they're logged with the full row payload and reason in `bdh.err_reject_log` |
| HSE reporting | `mart_hse_scorecard` computes TRIR/LTIR using the standard OSHA-style incidence-rate formula (rate per 200,000 hours worked) |

---

*This is a reference/demonstration ETL framework and does not represent, reproduce, or disclose any actual Saudi Aramco system, schema, or internal process.*
