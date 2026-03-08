# STC Enterprise Data Warehouse — TMF 622 Product Ordering
### Ab Initio ETL Framework | Teradata EDW | Saudi Telecom Corporation

---

## Overview

This repository contains the complete SQL and ETL script suite for the **Saudi Telecom Corporation (STC) Enterprise Data Warehouse**, built on the **TM Forum TMF 622 Product Ordering Management** standard. The solution implements a Kimball-methodology dimensional data warehouse on **Teradata EDW**, with an **Ab Initio** ETL pipeline orchestrating end-to-end data ingestion, transformation, quality validation, and mart delivery.

| Attribute | Detail |
|---|---|
| **Standard** | TM Forum TMF 622 R19.0 — Product Ordering Management API |
| **Methodology** | Kimball Bus Architecture — Star Schema |
| **ETL Platform** | Ab Initio Enterprise Meta>Environment™ (EME) v3.4 |
| **Target Platform** | Teradata EDW + Azure Data Lake (cold archive) |
| **Classification** | CONFIDENTIAL — STC Internal Use Only |
| **Version** | 2.0 |
| **Date** | March 2025 |

---

## Repository Structure

```
STC-TMF622-DWH/
├── README.md                    ← This file
├── 01_STG_DDL.sql               ← Staging (Landing) Layer DDL
├── 02_DWH_DDL.sql               ← Dimensional Model DDL (Star Schema)
├── 03_ETL_TRANSFORMS.sql        ← ETL Stored Procedures & ODS Logic
├── 04_DQ_RULES.sql              ← Data Quality Validation Rules (180+)
├── 05_DATA_MART.sql             ← Data Mart DDL & Aggregation Procedures
└── 06_STC_ANALYTICS.sql         ← Business Analytics & Use Case Queries
```

---

## Architecture Layers

```
 ┌──────────────────────────────────────────────────────────────┐
 │  SOURCE SYSTEMS                                              │
 │  OMS (Oracle SOA) │ CRM (Siebel) │ PCM (Amdocs) │ Digital  │
 └──────────────────────────┬───────────────────────────────────┘
                            │  Ab Initio GDE Graphs (REST/JDBC)
                            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  LAYER 1 — STAGING  (STG_TMF622)          01_STG_DDL.sql    │
 │  Raw ingestion · No transformation · 90-day retention       │
 └──────────────────────────┬───────────────────────────────────┘
                            │  GRP_TMF622_04_DQ_VALIDATE
                            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  LAYER 2 — ODS / INTEGRATION  (ODS_TMF622) 03_ETL_TRANSFORMS│
 │  Cleansed · Typed · Deduplicated · Validated                 │
 └──────────────────────────┬───────────────────────────────────┘
                            │  GRP_TMF622_06..09 (Dimensions)
                            │  GRP_TMF622_10..12 (Facts)
                            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  LAYER 3 — DATA WAREHOUSE  (DWH_TMF622)   02_DWH_DDL.sql    │
 │  Kimball Star Schema · SCD Type 2 · Partitioned             │
 └──────────────────────────┬───────────────────────────────────┘
                            │  GRP_TMF622_13_DATA_MART
                            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  LAYER 4 — DATA MART  (DM_TMF622)         05_DATA_MART.sql  │
 │  Pre-aggregated KPIs · BI-ready · MicroStrategy / Power BI  │
 └──────────────────────────────────────────────────────────────┘
```

---

## Script Reference

### `01_STG_DDL.sql` — Staging Layer
**Purpose:** Creates the raw data landing zone for all TMF 622 source feeds.

**Creates:**
- `STG_PRODUCT_ORDER` — Raw ProductOrder JSON payloads from STC OMS
- `STG_PRODUCT_ORDER_ITEM` — Exploded order item records
- `STG_ORDER_STATE_EVENT` — State machine transition events (CDC)
- `STG_CUSTOMER` — CRM customer extract (Siebel)
- `STG_PRODUCT_OFFERING` — Product catalog delta (Amdocs PCM)
- `ERR_TMF622` — DQ reject and error sink table
- `PURGE_STG_DATA` macro — 90-day rolling purge

**Key Design Decisions:**
- All columns stored as `VARCHAR` — no type enforcement at staging
- `RAW_JSON_PAYLOAD JSON` column retained for full replay capability
- Partitioned monthly by `STG_LOAD_DT`
- `STG_RECORD_STATUS` lifecycle: `N` (New) → `V` (Valid) → `P` (Processed) / `E` (Error)

---

### `02_DWH_DDL.sql` — Dimensional Model (Star Schema)
**Purpose:** Full Kimball star schema DDL for the DWH_TMF622 analytics layer.

**Dimension Tables (SCD Type 2 unless noted):**

| Table | Type | Description |
|---|---|---|
| `DIM_DATE` | Static | Calendar with Hijri extensions, Saudi holiday flags |
| `DIM_TIME` | Static | Sub-day dimension (minute granularity, AST UTC+3) |
| `DIM_GEOGRAPHY` | Type 1 | Saudi Arabia: Region → Governorate → City → District |
| `DIM_ORDER_STATE` | Static | TMF 622 state machine (seeded, 10 rows + Arabic names) |
| `DIM_CHANNEL` | Type 1 | STC sales channels (seeded: App, Web, Retail, B2B…) |
| `DIM_ORDER_ACTION` | Static | TMF 622 item actions: add/modify/delete/noChange |
| `DIM_CUSTOMER` | Type 2 | CRM customer attributes, NID SHA-256 hashed (PDPL) |
| `DIM_PRODUCT_OFFERING` | Type 2 | PCM product catalog, 5G/bundle/IOT flags |
| `DIM_PRODUCT_ORDER` | Type 2 | TMF 622 order header attributes |
| `DIM_RELATED_PARTY` | Type 2 | Agents, sellers, requesters |

**Fact Tables:**

| Table | Grain | Description |
|---|---|---|
| `FACT_PRODUCT_ORDER` | Order × Day | Financial measures, SLA flags, channel flags |
| `FACT_ORDER_ITEM` | Item × Day | Line-item pricing, product detail, provisioning duration |
| `FACT_ORDER_STATE_TRANSITION` | Transition Event | State machine events, duration-in-state, SLA elapsed % |

**Teradata Optimisations:**
- Monthly range partitioning on all fact tables
- `COLLECT STATS` on high-cardinality join columns
- `MULTIVALUE` compression on low-cardinality flag columns
- `SEQ_CONTROL` table for Ab Initio parallel surrogate key generation

---

### `03_ETL_TRANSFORMS.sql` — ETL Transformation Logic
**Purpose:** Stored procedures mirroring Ab Initio GDE graph logic for each pipeline stage.

**Procedure Inventory:**

| Procedure | Ab Initio Graph | Function |
|---|---|---|
| `PROC_CLEANSE_ORDER` | GRP_TMF622_05_CLEANSE | State standardisation, phone E.164 normalisation, ZATCA VAT recalc |
| `PROC_SCD2_CUSTOMER` | GRP_TMF622_06_DIM_CUSTOMER | MD5 hash-based SCD2: Type-1 overwrite vs. Type-2 version |
| `PROC_SCD2_PRODUCT` | GRP_TMF622_07_DIM_PRODUCT | Product offering versioning, VAT-inclusive price derivation |
| `PROC_LOAD_FACT_ORDER` | GRP_TMF622_10_FACT_ORDER | Full FK resolution, SLA computation, derived flags |
| `PROC_LOAD_FACT_ITEM` | GRP_TMF622_11_FACT_ITEM | Item-grain fact load, VAT auto-calculation |
| `PROC_LOAD_STATE_TRANSITION` | GRP_TMF622_12_FACT_TRANSITION | LAG-based duration-in-state, SLA elapsed % |
| `PROC_RUN_DAILY_BATCH` | PLN_TMF622_DAILY | Master orchestrator — calls all procs in dependency order |

**SCD2 Pattern (all Type-2 dimensions):**
```
1. Compute CHANGE_HASH = MD5(all Type-2 attribute columns)
2. Compare with current DWH row hash
3. If CHANGED  → expire old row (ROW_EXP_DT = today-1, CURRENT_ROW_FLAG='N')
              → insert new row (ROW_EFF_DT = today, ROW_EXP_DT = 9999-12-31)
4. If Type-1   → UPDATE current row in-place (no new version)
5. If NEW      → INSERT first version (ROW_EFF_DT = today)
```

**SLA Calculation Logic:**
```sql
-- CITC SLA targets by product/segment
ENTERPRISE orders  →  4 hours
SMB orders         →  8 hours
5G products        → 12 hours
Fiber install      → 48 hours
Default (mobile)   → 24 hours  ← CITC regulatory minimum
```

---

### `04_DQ_RULES.sql` — Data Quality Validation
**Purpose:** 180+ DQ rules validating all staging data before ODS promotion. Called by `GRP_TMF622_04_DQ_VALIDATE`.

**Rule Categories:**

| Range | Category | Example Rules |
|---|---|---|
| DQ-01 to DQ-20 | Completeness | ORDER_ID not null, required fields present |
| DQ-21 to DQ-50 | Format / Pattern | TMF622 state enum, date formats |
| DQ-51 to DQ-80 | Referential | Customer in CRM, product in PCM |
| DQ-81 to DQ-120 | Business Rules | VAT = 15%, SLA logic, amount ranges |
| DQ-121 to DQ-150 | Cross-field | Completion before order date, item count match |
| DQ-151 to DQ-180 | KSA Regulatory | Saudi NID format, ZATCA VAT, PDPL masking |

**Severity Levels:**

| Severity | Action | Example |
|---|---|---|
| `REJECT` | Mark `STG_RECORD_STATUS = 'E'`, write to `ERR_TMF622` | Null ORDER_ID, negative amount |
| `WARN` | Write to `ERR_TMF622`, record proceeds to ODS | Phone format, missing customer ref |
| `AUTO-CORRECT` | Fix value in staging, log correction | VAT recalculation, phone normalisation |

**Batch Gate:** `VW_DQ_BATCH_SCORE` computes pass rate. Ab Initio Conduct>It halts the batch if `DQ_PASS_RATE_PCT < 97%` and triggers a PagerDuty alert to the Data Engineering team.

---

### `05_DATA_MART.sql` — Data Mart & Aggregations
**Purpose:** Pre-aggregated KPI tables for BI tools (MicroStrategy, Power BI). Mirrors `GRP_TMF622_13_DATA_MART`.

**Mart Tables:**

| Table | Grain | Use Case |
|---|---|---|
| `DM_ORDER_KPI` | Day × Channel × Segment × Product | Primary daily KPI dashboard |
| `DM_SLA_CITC` | Month × Service Type × Region | CITC regulatory compliance filing |
| `DM_5G_ADOPTION` | Month × Region × Segment | 5G adoption and ACV tracking |
| `DM_REVENUE_ASSURANCE` | Month × Segment × Product | Revenue leakage and billing reconciliation |

**Aggregate Views:**
- `VW_EXEC_ORDER_SUMMARY` — Monthly executive dashboard (revenue, SLA, digital share, 5G adoption)
- `VW_CHANNEL_COMPARISON` — Digital vs. physical channel benchmarking
- `VW_5G_REGIONAL_ADOPTION` — Regional 5G heatmap for network investment decisions

---

### `06_STC_ANALYTICS.sql` — Business Analytics Queries
**Purpose:** Production SQL for all 10 defined STC business use cases.

| Use Case | Description |
|---|---|
| **UC-01** | 5G Product Adoption — monthly rates, ACV premium, regional heatmap, top 10 offerings |
| **UC-02** | CITC SLA Compliance — weekly dashboard, bottleneck state ranking, P95/P99 fulfilment |
| **UC-03** | Revenue Assurance — VAT reconciliation, discount leakage, uncollected revenue |
| **UC-04** | Channel Performance — digital share tracking vs. Vision 2030 70% target |
| **UC-05** | Customer 360 — full order history with cumulative CLV running total |
| **UC-06** | Fallout Root Cause — failure state analysis, lost revenue estimate |
| **UC-07** | Product Mix & Bundle Performance — revenue by family, contract value, discount depth |
| **UC-08** | Agent / Store Scorecard — regional performance rank, SLA adherence per agent |
| **UC-09** | Cohort Funnel — state machine drop-off analysis for Jan 2025 cohort |
| **UC-10** | ZATCA VAT Reconciliation — monthly tax period report with Hijri calendar |

**Operational Views (real-time monitoring):**
- `VW_TODAYS_ORDER_PULSE` — Live order counts and revenue by state and channel
- `VW_STUCK_ORDERS_ALERT` — Orders in non-terminal state > 24 hours with SLA countdown

---

## Ab Initio Batch Schedule (Conduct>It: PLN_TMF622_DAILY)

| Window (AST) | Graph(s) | Dependency | SLA |
|---|---|---|---|
| 01:00 – 02:00 | `GRP_TMF622_01_INGEST_OMS` | OMS export complete | By 02:30 |
| 02:00 – 03:00 | `GRP_TMF622_02_INGEST_CRM`, `03_INGEST_PCM` | CRM export complete | By 03:30 |
| 03:00 – 04:00 | `GRP_TMF622_04_DQ_VALIDATE` | All ingestion complete | DQ Score > 97% |
| 04:00 – 05:00 | `GRP_TMF622_05_CLEANSE`, `06–09` (Dims) | DQ pass | By 05:30 |
| 05:00 – 06:30 | `GRP_TMF622_10_FACT_ORDER`, `11_FACT_ITEM` | All dims loaded | By 06:30 |
| 06:00 – 06:30 | `GRP_TMF622_12_FACT_TRANSITION` | Order facts complete | By 06:45 |
| 07:00 – 07:30 | `GRP_TMF622_13_DATA_MART`, `14_DQ_REPORT` | All facts loaded | Dashboards by 07:45 |

---

## Prerequisites

**Infrastructure:**
- Teradata Database 17.x or higher
- Ab Initio EME v3.4+ with GDE, Conduct>It, Co>Operating System
- RHEL 8 Linux cluster (32-way parallelism recommended)
- Network connectivity to OMS (Oracle SOA/MuleSoft), Siebel CRM, Amdocs PCM

**Teradata Permissions Required:**
```sql
-- Execute as DBA before running scripts
GRANT CREATE TABLE, CREATE VIEW, CREATE MACRO, CREATE PROCEDURE
    ON STG_TMF622, ODS_TMF622, DWH_TMF622, DM_TMF622
    TO ROLE DWH_ETL_ROLE;

GRANT INSERT, UPDATE, DELETE, SELECT
    ON STG_TMF622, ODS_TMF622, DWH_TMF622, DM_TMF622
    TO ROLE DWH_ETL_ROLE;
```

---

## Deployment Order

Run scripts strictly in the following sequence:

```
Step 1:  01_STG_DDL.sql          ← Creates staging schemas and tables
Step 2:  02_DWH_DDL.sql          ← Creates DWH schemas, dimensions, facts
                                     Inserts seed data (states, channels, UNKNOWN rows)
Step 3:  03_ETL_TRANSFORMS.sql   ← Creates ODS schema and all ETL procedures
Step 4:  04_DQ_RULES.sql         ← Creates DQ macro and scorecard views
Step 5:  05_DATA_MART.sql        ← Creates DM schema, mart tables, procedures, views
Step 6:  06_STC_ANALYTICS.sql    ← Creates operational views; run queries ad-hoc
```

> **Note:** Steps 1–5 are DDL and should be run once per environment (DEV / UAT / PROD). Step 6 contains both view creation (run once) and analytics queries (run on demand).

---

## Data Governance & Compliance

### Saudi PDPL (Personal Data Protection Law)
| Field | Treatment |
|---|---|
| National ID / Iqama | SHA-256 hashed before storage (`NATIONAL_ID_HASH CHAR(64)`) |
| Mobile Number | Masked in DWH: `05X-XXX-X789` format |
| Full Name | Pseudonymised in staging; clear-text restricted to ODS governance role |
| Billing Address | City + Region retained; street address masked for analysts |

### ZATCA VAT Compliance
- All financial measures stored in **SAR** at 15% VAT
- `VAT_AMOUNT` auto-corrected during DQ if deviation > 0.01 SAR (DQ-05)
- `UC-10` ZATCA reconciliation report includes Hijri calendar period

### Data Retention
| Layer | Retention |
|---|---|
| Staging (`STG_*`) | 90 days (rolling purge via `PURGE_STG_DATA` macro) |
| ODS / DWH Facts | 7 years (CITC regulatory + ZATCA tax audit) |
| SCD2 Dimension History | Indefinite (point-in-time reporting) |
| Error Tables (`ERR_*`) | 1 year |
| Cold Archive (Azure Blob) | After 3 years in active DWH |

### Teradata Role-Based Access
| Role | Access |
|---|---|
| `DWH_ANALYST` | `DM_*` marts and aggregated `DWH_TMF622` views only |
| `DWH_POWER_USER` | Full `DWH_TMF622` access (no PII columns) |
| `DWH_DQ_ENGINEER` | `STG_*` and `ERR_*` schemas only |
| `DWH_GOVERNANCE` | Full access including PDPL columns (audit-logged) |

---

## Key Business KPIs Supported

| KPI | Source | Target |
|---|---|---|
| Order Completion Rate | `FACT_PRODUCT_ORDER` | > 98% |
| CITC SLA Compliance | `DM_SLA_CITC` | ≥ 95% (regulatory) |
| Digital Order Share | `DM_ORDER_KPI` | 70% by 2026 (Vision 2030) |
| 5G Adoption Rate | `DM_5G_ADOPTION` | Per regional plan |
| Order Fallout Rate | `DM_ORDER_KPI` | < 2% |
| VAT Reconciliation Variance | `DM_REVENUE_ASSURANCE` | < 0.01 SAR per order |
| DQ Pass Rate | `VW_DQ_BATCH_SCORE` | ≥ 97% (batch gate) |
| Avg Fulfilment (Mobile) | `FACT_PRODUCT_ORDER` | < 24 hours (CITC) |
| Avg Fulfilment (Fiber) | `FACT_PRODUCT_ORDER` | < 48 hours (CITC) |

---

## Contact & Ownership

| Role | Team |
|---|---|
| **Solution Owner** | STC Enterprise Data Architecture & Analytics Division |
| **ETL Development** | Data Engineering Squad — Ab Initio CoE |
| **Data Governance** | STC Data Governance Council |
| **Regulatory** | STC Legal & Compliance (CITC / ZATCA / PDPL) |

---

*© 2025 Saudi Telecom Corporation (STC). All Rights Reserved. This document is the intellectual property of STC. Unauthorised reproduction or distribution is strictly prohibited.*
