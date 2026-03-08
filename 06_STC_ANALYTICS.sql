/* ============================================================================
   SCRIPT : 06_STC_ANALYTICS.sql
   PURPOSE: STC Business Use Case Analytics Queries
            Production-grade SQL for MicroStrategy / Power BI / Ad-hoc
   SCHEMAS: DWH_TMF622 (star schema) + DM_TMF622 (pre-aggregated mart)
   PLATFORM: Teradata SQL
   PROJECT : STC Enterprise Data Warehouse — TMF 622 Product Ordering
   VERSION : 2.0
   DATE    : March 2025

   USE CASES COVERED:
     UC-01 : 5G Product Adoption Tracking
     UC-02 : CITC SLA Compliance Monitoring
     UC-03 : Revenue Assurance & Leakage Detection
     UC-04 : Channel Performance & Digital Transformation
     UC-05 : Customer 360 — Ordering Behaviour
     UC-06 : Order Fallout / Failure Root Cause Analysis
     UC-07 : Product Mix & Bundle Performance
     UC-08 : Agent / Store Performance Scorecard
     UC-09 : Cohort Funnel Analysis (Order State Machine)
     UC-10 : ZATCA VAT Reconciliation Report
   ============================================================================ */

/* ────────────────────────────────────────────────────────────────────────────
   UC-01: 5G PRODUCT ADOPTION TRACKING
   Business Question: How is STC's 5G portfolio being adopted across regions
   and segments? Which bundles drive highest contract value?
   ──────────────────────────────────────────────────────────────────────────── */

-- UC-01a: Monthly 5G vs 4G Adoption Rate by Region
SELECT
    D.YEAR_NUM                                                      AS YEAR,
    D.MONTH_NM_EN                                                   AS MONTH,
    D.MONTH_NUM,
    G.ADMIN_REGION_NM_EN                                            AS REGION,
    G.ADMIN_REGION_NM_AR,

    COUNT(DISTINCT FO.ORDER_SK)                                     AS TOTAL_ORDERS,
    SUM(CASE WHEN PO.TECHNOLOGY_TIER = '5G'    THEN 1 ELSE 0 END)  AS ORDERS_5G,
    SUM(CASE WHEN PO.TECHNOLOGY_TIER = '4G'    THEN 1 ELSE 0 END)  AS ORDERS_4G,
    SUM(CASE WHEN PO.TECHNOLOGY_TIER = 'FIBER' THEN 1 ELSE 0 END)  AS ORDERS_FIBER,

    -- Adoption Rates
    CAST(SUM(CASE WHEN PO.TECHNOLOGY_TIER = '5G' THEN 1 ELSE 0 END) AS DECIMAL(10,4))
      / NULLIF(COUNT(DISTINCT FO.ORDER_SK),0) * 100                 AS ADOPTION_RATE_5G_PCT,

    -- Revenue Breakdown
    SUM(CASE WHEN PO.TECHNOLOGY_TIER = '5G'
             THEN FO.TOTAL_ORDER_AMOUNT ELSE 0 END)                 AS REVENUE_5G_SAR,
    SUM(CASE WHEN PO.TECHNOLOGY_TIER = '4G'
             THEN FO.TOTAL_ORDER_AMOUNT ELSE 0 END)                 AS REVENUE_4G_SAR,

    -- Average Contract Value Comparison
    AVG(CASE WHEN PO.TECHNOLOGY_TIER = '5G'
             THEN FO.TOTAL_ORDER_AMOUNT END)                        AS ACV_5G_SAR,
    AVG(CASE WHEN PO.TECHNOLOGY_TIER = '4G'
             THEN FO.TOTAL_ORDER_AMOUNT END)                        AS ACV_4G_SAR,

    -- 5G Premium: How much more is a customer paying for 5G?
    AVG(CASE WHEN PO.TECHNOLOGY_TIER = '5G'  THEN FO.TOTAL_ORDER_AMOUNT END)
    - AVG(CASE WHEN PO.TECHNOLOGY_TIER = '4G' THEN FO.TOTAL_ORDER_AMOUNT END)
                                                                    AS ACV_5G_PREMIUM_SAR,

    -- Coverage Context
    AVG(CASE WHEN G.HAS_5G_COVERAGE = 'Y' THEN 1.0 ELSE 0 END) * 100 AS PCT_ORDERS_IN_5G_AREA

FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
JOIN DWH_TMF622.DIM_DATE D
    ON D.DATE_SK = FO.DATE_SK
LEFT JOIN DWH_TMF622.DIM_PRODUCT_OFFERING PO
    ON PO.PRODUCT_OFFERING_SK = FO.PRODUCT_OFFERING_SK
   AND PO.CURRENT_ROW_FLAG = 'Y'
LEFT JOIN DWH_TMF622.DIM_GEOGRAPHY G
    ON G.GEOGRAPHY_SK = FO.GEOGRAPHY_SK
LEFT JOIN DWH_TMF622.DIM_ORDER_STATE OS
    ON OS.ORDER_STATE_SK = FO.ORDER_STATE_SK

WHERE D.YEAR_NUM IN (2024, 2025)
  AND OS.ORDER_STATE_CD = 'completed'                               -- completed orders only

GROUP BY 1,2,3,4,5
ORDER BY D.YEAR_NUM, D.MONTH_NUM, G.ADMIN_REGION_NM_EN;

-- ─────────────────────────────────────────────────────────────
-- UC-01b: Top 10 5G Offerings by Volume and Revenue (YTD)
-- ─────────────────────────────────────────────────────────────
SELECT TOP 10
    PO.PRODUCT_OFFERING_NAME,
    PO.PRODUCT_FAMILY_NM,
    PO.CONTRACT_DURATION_MONTHS                                     AS CONTRACT_MONTHS,
    COUNT(FI.ORDER_ITEM_SK)                                         AS ORDER_VOLUME,
    SUM(FI.ITEM_TOTAL_AMOUNT)                                       AS TOTAL_REVENUE_SAR,
    AVG(FI.ITEM_TOTAL_AMOUNT)                                       AS AVG_ITEM_VALUE_SAR,
    SUM(FI.ITEM_MRC)                                                AS TOTAL_MRC_SAR,
    SUM(FI.ITEM_MRC * PO.CONTRACT_DURATION_MONTHS)                 AS EXPECTED_CONTRACT_VALUE_SAR,
    RANK() OVER (ORDER BY SUM(FI.ITEM_TOTAL_AMOUNT) DESC)           AS REVENUE_RANK

FROM DWH_TMF622.FACT_ORDER_ITEM FI
JOIN DWH_TMF622.DIM_PRODUCT_OFFERING PO
    ON PO.PRODUCT_OFFERING_SK = FI.PRODUCT_OFFERING_SK
   AND PO.CURRENT_ROW_FLAG = 'Y'
   AND PO.IS_5G_FLAG = 'Y'
JOIN DWH_TMF622.DIM_DATE D
    ON D.DATE_SK = FI.DATE_SK
   AND D.YEAR_NUM = 2025

GROUP BY 1,2,3
ORDER BY TOTAL_REVENUE_SAR DESC;

/* ────────────────────────────────────────────────────────────────────────────
   UC-02: CITC SLA COMPLIANCE MONITORING
   Business Question: Are STC orders meeting CITC-mandated SLAs?
   ──────────────────────────────────────────────────────────────────────────── */

-- UC-02a: Weekly SLA Compliance Dashboard
SELECT
    D.YEAR_NUM,
    D.WEEK_OF_YEAR                                                  AS ISO_WEEK,
    CH.CHANNEL_CD,
    COALESCE(ORD.STC_SEGMENT_CD,'CONSUMER')                         AS SEGMENT,
    CASE PO.PRODUCT_TYPE_CD
        WHEN 'FIBER'  THEN 48.0
        WHEN '5G'     THEN 12.0
        ELSE 24.0
    END                                                             AS CITC_SLA_HRS,

    COUNT(DISTINCT FO.ORDER_SK)                                     AS COMPLETED_ORDERS,
    SUM(CASE WHEN FO.SLA_BREACH_FLAG = 'N' THEN 1 ELSE 0 END)      AS WITHIN_SLA,
    SUM(CASE WHEN FO.SLA_BREACH_FLAG = 'Y' THEN 1 ELSE 0 END)      AS BREACHED_SLA,
    CAST(SUM(CASE WHEN FO.SLA_BREACH_FLAG = 'N' THEN 1 ELSE 0 END) AS DECIMAL(10,4))
      / NULLIF(COUNT(DISTINCT FO.ORDER_SK),0) * 100                 AS SLA_COMPLIANCE_PCT,

    -- Bench against CITC 95% minimum
    CASE
        WHEN CAST(SUM(CASE WHEN FO.SLA_BREACH_FLAG = 'N' THEN 1 ELSE 0 END) AS DECIMAL(10,4))
             / NULLIF(COUNT(DISTINCT FO.ORDER_SK),0) * 100 >= 95
        THEN 'CITC COMPLIANT'
        ELSE 'CITC NON-COMPLIANT'
    END                                                             AS CITC_STATUS,

    AVG(FO.FULFILMENT_DURATION_HRS)                                 AS AVG_FULFILMENT_HRS,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY FO.FULFILMENT_DURATION_HRS)
                                                                    AS P95_FULFILMENT_HRS,
    MAX(FO.FULFILMENT_DURATION_HRS)                                 AS MAX_FULFILMENT_HRS

FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
JOIN DWH_TMF622.DIM_DATE D       ON D.DATE_SK         = FO.DATE_SK
JOIN DWH_TMF622.DIM_CHANNEL CH   ON CH.CHANNEL_SK     = FO.CHANNEL_SK
JOIN DWH_TMF622.DIM_ORDER_STATE OS ON OS.ORDER_STATE_SK = FO.ORDER_STATE_SK
LEFT JOIN DWH_TMF622.DIM_PRODUCT_ORDER ORD
    ON ORD.ORDER_SK = FO.ORDER_SK AND ORD.CURRENT_ROW_FLAG = 'Y'
LEFT JOIN DWH_TMF622.DIM_PRODUCT_OFFERING PO
    ON PO.PRODUCT_OFFERING_SK = FO.PRODUCT_OFFERING_SK AND PO.CURRENT_ROW_FLAG = 'Y'

WHERE OS.ORDER_STATE_CD = 'completed'
  AND D.FULL_DATE >= CURRENT_DATE - INTERVAL '90' DAY

GROUP BY 1,2,3,4,5
ORDER BY D.YEAR_NUM, D.WEEK_OF_YEAR, SLA_COMPLIANCE_PCT ASC;  -- worst SLA first

-- ─────────────────────────────────────────────────────────────
-- UC-02b: SLA Bottleneck Analysis — Time Spent in Each State
-- ─────────────────────────────────────────────────────────────
SELECT
    DOS_FROM.ORDER_STATE_CD                                         AS FROM_STATE,
    DOS_TO.ORDER_STATE_CD                                           AS TO_STATE,
    COUNT(*)                                                        AS TRANSITION_COUNT,
    AVG(T.DURATION_IN_FROM_STATE_HRS)                               AS AVG_HRS_IN_STATE,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY T.DURATION_IN_FROM_STATE_HRS)
                                                                    AS P95_HRS_IN_STATE,
    MAX(T.DURATION_IN_FROM_STATE_HRS)                               AS MAX_HRS_IN_STATE,
    SUM(CASE WHEN T.IS_SLA_CRITICAL_STATE = 'Y' THEN 1 ELSE 0 END) AS ON_SLA_CRITICAL_PATH,

    -- Rank by where most time is lost
    RANK() OVER (ORDER BY AVG(T.DURATION_IN_FROM_STATE_HRS) DESC)   AS BOTTLENECK_RANK,

    -- Volume-weighted impact score
    COUNT(*) * AVG(T.DURATION_IN_FROM_STATE_HRS)                   AS TIME_IMPACT_SCORE

FROM DWH_TMF622.FACT_ORDER_STATE_TRANSITION T
JOIN DWH_TMF622.DIM_ORDER_STATE DOS_FROM
    ON DOS_FROM.ORDER_STATE_SK = T.FROM_STATE_SK
JOIN DWH_TMF622.DIM_ORDER_STATE DOS_TO
    ON DOS_TO.ORDER_STATE_SK = T.TO_STATE_SK
JOIN DWH_TMF622.DIM_DATE D
    ON D.DATE_SK = T.TRANSITION_DATE_SK
   AND D.FULL_DATE >= CURRENT_DATE - INTERVAL '30' DAY

WHERE T.DURATION_IN_FROM_STATE_HRS > 0

GROUP BY 1,2
ORDER BY BOTTLENECK_RANK;

/* ────────────────────────────────────────────────────────────────────────────
   UC-03: REVENUE ASSURANCE & LEAKAGE DETECTION
   ──────────────────────────────────────────────────────────────────────────── */

-- UC-03a: Monthly Revenue Summary with VAT Reconciliation
SELECT
    SUBSTR(CAST(D.FULL_DATE AS CHAR(10) FORMAT 'YYYY-MM-DD'),1,7)   AS REPORT_MONTH,
    COALESCE(ORD.STC_SEGMENT_CD,'CONSUMER')                         AS SEGMENT,
    COUNT(CASE WHEN OS.ORDER_STATE_CD = 'completed' THEN 1 END)     AS COMPLETED_ORDERS,

    -- Expected Revenue from DWH (order value)
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.TOTAL_ORDER_AMOUNT ELSE 0 END)                 AS EXPECTED_GROSS_REVENUE,
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.ORDER_AMOUNT_EXCL_VAT ELSE 0 END)              AS EXPECTED_NET_REVENUE,
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.VAT_AMOUNT ELSE 0 END)                         AS EXPECTED_VAT,
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.DISCOUNT_AMOUNT ELSE 0 END)                    AS TOTAL_DISCOUNTS,

    -- Expected MRC Revenue (annualised)
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.RECURRING_CHARGE_MRC * 12 ELSE 0 END)          AS EXPECTED_ANNUAL_MRC,

    -- Revenue assurance flags
    SUM(CASE WHEN FO.REVENUE_ASSURANCE_FLAG = 'Y' THEN 1 ELSE 0 END) AS ORDERS_WITH_BILLING_ISSUES,

    -- Effective VAT rate check (should always be 15.00% for ZATCA compliance)
    AVG(CASE
        WHEN FO.ORDER_AMOUNT_EXCL_VAT > 0
        THEN FO.VAT_AMOUNT / FO.ORDER_AMOUNT_EXCL_VAT * 100
    END)                                                            AS EFFECTIVE_VAT_RATE_PCT,

    -- Flag deviations from 15% VAT
    SUM(CASE
        WHEN FO.ORDER_AMOUNT_EXCL_VAT > 0
         AND ABS(FO.VAT_AMOUNT / FO.ORDER_AMOUNT_EXCL_VAT * 100 - 15.0) > 0.01
        THEN 1 ELSE 0
    END)                                                            AS VAT_DEVIATION_COUNT

FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
JOIN DWH_TMF622.DIM_DATE D
    ON D.DATE_SK = FO.DATE_SK
LEFT JOIN DWH_TMF622.DIM_ORDER_STATE OS
    ON OS.ORDER_STATE_SK = FO.ORDER_STATE_SK
LEFT JOIN DWH_TMF622.DIM_PRODUCT_ORDER ORD
    ON ORD.ORDER_SK = FO.ORDER_SK AND ORD.CURRENT_ROW_FLAG = 'Y'

WHERE D.YEAR_NUM = 2025

GROUP BY 1,2
ORDER BY REPORT_MONTH, SEGMENT;

/* ────────────────────────────────────────────────────────────────────────────
   UC-04: CHANNEL PERFORMANCE & DIGITAL TRANSFORMATION TRACKER
   Business Question: How are digital channels performing vs. physical?
   STC Vision 2030 Target: 70% of orders via digital by 2026.
   ──────────────────────────────────────────────────────────────────────────── */

SELECT
    D.YEAR_NUM,
    D.MONTH_NUM,
    D.MONTH_NM_EN,
    CH.CHANNEL_CD,
    CH.CHANNEL_TYPE_CD,                                             -- DIGITAL | PHYSICAL | ASSISTED
    CH.IS_DIGITAL_FLAG,
    CH.MEDIUM_CD,

    COUNT(DISTINCT FO.ORDER_SK)                                     AS TOTAL_ORDERS,
    SUM(FO.TOTAL_ORDER_AMOUNT)                                      AS GROSS_REVENUE_SAR,
    AVG(FO.TOTAL_ORDER_AMOUNT)                                      AS AVG_ORDER_VALUE_SAR,
    COUNT(DISTINCT FO.CUSTOMER_SK)                                  AS UNIQUE_CUSTOMERS,
    SUM(CASE WHEN FO.IS_NEW_CUSTOMER_ORDER = 'Y' THEN 1 ELSE 0 END) AS NEW_ACQUISITIONS,

    -- Completion rate
    CAST(SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed' THEN 1 ELSE 0 END)
         AS DECIMAL(10,4))
    / NULLIF(COUNT(DISTINCT FO.ORDER_SK),0) * 100                   AS COMPLETION_RATE_PCT,

    -- Fallout rate (quality indicator)
    CAST(SUM(CASE WHEN FO.FALLOUT_FLAG = 'Y' THEN 1 ELSE 0 END)
         AS DECIMAL(10,4))
    / NULLIF(COUNT(DISTINCT FO.ORDER_SK),0) * 100                   AS FALLOUT_RATE_PCT,

    -- SLA Performance
    AVG(FO.FULFILMENT_DURATION_HRS)                                 AS AVG_FULFILMENT_HRS,

    -- Channel Market Share
    CAST(COUNT(DISTINCT FO.ORDER_SK) AS DECIMAL(10,4))
    / NULLIF(SUM(COUNT(DISTINCT FO.ORDER_SK)) OVER (
        PARTITION BY D.YEAR_NUM, D.MONTH_NUM
    ),0) * 100                                                      AS CHANNEL_MARKET_SHARE_PCT,

    -- Vision 2030 Progress: Digital vs. Total
    CAST(SUM(CASE WHEN CH.IS_DIGITAL_FLAG = 'Y' THEN 1 ELSE 0 END) AS DECIMAL(10,4))
    / NULLIF(SUM(COUNT(DISTINCT FO.ORDER_SK)) OVER (
        PARTITION BY D.YEAR_NUM, D.MONTH_NUM
    ),0) * 100                                                      AS DIGITAL_SHARE_OF_TOTAL_PCT

FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
JOIN DWH_TMF622.DIM_DATE D      ON D.DATE_SK       = FO.DATE_SK
JOIN DWH_TMF622.DIM_CHANNEL CH  ON CH.CHANNEL_SK   = FO.CHANNEL_SK
JOIN DWH_TMF622.DIM_ORDER_STATE OS ON OS.ORDER_STATE_SK = FO.ORDER_STATE_SK
WHERE D.YEAR_NUM = 2025
GROUP BY 1,2,3,4,5,6,7
ORDER BY D.YEAR_NUM, D.MONTH_NUM, TOTAL_ORDERS DESC;

/* ────────────────────────────────────────────────────────────────────────────
   UC-05: CUSTOMER 360 — COMPLETE ORDERING HISTORY
   Business Question: Full product order history for a given customer.
   ──────────────────────────────────────────────────────────────────────────── */

-- UC-05a: Customer Order History Lookup
-- Replace :CUSTOMER_BK with the CRM account number
SELECT
    C.CUSTOMER_BK,
    C.CUSTOMER_TYPE_CD,
    C.CUSTOMER_SEGMENT_CD,
    C.CITY_NM,
    C.REGION_NM,
    C.ACTIVATION_DT                                                 AS CUSTOMER_SINCE,
    C.CHURN_RISK_SCORE,

    D.FULL_DATE                                                     AS ORDER_DATE,
    ORD.ORDER_BK                                                    AS ORDER_ID,
    ORD.ORDER_CATEGORY_CD                                           AS ORDER_TYPE,
    OS.ORDER_STATE_CD                                               AS CURRENT_STATE,
    CH.CHANNEL_CD                                                   AS ORDER_CHANNEL,
    PO.PRODUCT_OFFERING_NAME,
    PO.PRODUCT_TYPE_CD,
    PO.TECHNOLOGY_TIER,

    FO.TOTAL_ORDER_AMOUNT                                           AS ORDER_VALUE_SAR,
    FO.RECURRING_CHARGE_MRC                                         AS MONTHLY_CHARGE_SAR,
    FO.DISCOUNT_AMOUNT,
    FO.FULFILMENT_DURATION_HRS,
    FO.SLA_BREACH_FLAG,
    FO.ORDER_ITEM_COUNT,

    -- Running totals for CLV tracking
    SUM(FO.TOTAL_ORDER_AMOUNT) OVER (
        PARTITION BY FO.CUSTOMER_SK
        ORDER BY D.FULL_DATE
        ROWS UNBOUNDED PRECEDING
    )                                                               AS CUMULATIVE_SPEND_SAR,
    COUNT(*) OVER (
        PARTITION BY FO.CUSTOMER_SK
    )                                                               AS TOTAL_LIFETIME_ORDERS,
    ROW_NUMBER() OVER (
        PARTITION BY FO.CUSTOMER_SK
        ORDER BY D.FULL_DATE
    )                                                               AS ORDER_SEQUENCE_NUM

FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
JOIN DWH_TMF622.DIM_CUSTOMER C
    ON C.CUSTOMER_SK = FO.CUSTOMER_SK
   AND C.CUSTOMER_BK = :CUSTOMER_BK
   AND C.CURRENT_ROW_FLAG = 'Y'
JOIN DWH_TMF622.DIM_DATE D      ON D.DATE_SK       = FO.DATE_SK
JOIN DWH_TMF622.DIM_ORDER_STATE OS ON OS.ORDER_STATE_SK = FO.ORDER_STATE_SK
JOIN DWH_TMF622.DIM_PRODUCT_ORDER ORD
    ON ORD.ORDER_SK = FO.ORDER_SK AND ORD.CURRENT_ROW_FLAG = 'Y'
LEFT JOIN DWH_TMF622.DIM_CHANNEL CH
    ON CH.CHANNEL_SK = FO.CHANNEL_SK
LEFT JOIN DWH_TMF622.DIM_PRODUCT_OFFERING PO
    ON PO.PRODUCT_OFFERING_SK = FO.PRODUCT_OFFERING_SK AND PO.CURRENT_ROW_FLAG = 'Y'

ORDER BY D.FULL_DATE;

/* ────────────────────────────────────────────────────────────────────────────
   UC-06: ORDER FALLOUT / FAILURE ROOT CAUSE ANALYSIS
   ──────────────────────────────────────────────────────────────────────────── */

SELECT
    T.CHANGE_REASON_CD                                              AS FAILURE_REASON,
    T.CHANGE_REASON_DESC,
    DS_FROM.ORDER_STATE_CD                                          AS FAILED_AT_STATE,
    CH.CHANNEL_CD,
    G.ADMIN_REGION_NM_EN                                            AS REGION,
    PO.PRODUCT_TYPE_CD,

    COUNT(*)                                                        AS FALLOUT_COUNT,
    CAST(COUNT(*) AS DECIMAL(10,4))
    / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY T.CHANGE_REASON_CD),0) * 100
                                                                    AS PCT_OF_REASON,
    AVG(T.DURATION_IN_FROM_STATE_HRS)                               AS AVG_TIME_BEFORE_FALLOUT,

    -- Impact: lost revenue (estimate from failed orders)
    SUM(FO.TOTAL_ORDER_AMOUNT)                                      AS LOST_REVENUE_ESTIMATE_SAR,
    AVG(FO.TOTAL_ORDER_AMOUNT)                                      AS AVG_LOST_ORDER_VALUE

FROM DWH_TMF622.FACT_ORDER_STATE_TRANSITION T
JOIN DWH_TMF622.FACT_PRODUCT_ORDER FO
    ON FO.ORDER_SK = T.ORDER_SK
JOIN DWH_TMF622.DIM_ORDER_STATE DS_FROM
    ON DS_FROM.ORDER_STATE_SK = T.FROM_STATE_SK
JOIN DWH_TMF622.DIM_ORDER_STATE DS_TO
    ON DS_TO.ORDER_STATE_SK = T.TO_STATE_SK
   AND DS_TO.ORDER_STATE_CD IN ('failed','cancelled','rejected')   -- terminal failure states
LEFT JOIN DWH_TMF622.DIM_CHANNEL CH
    ON CH.CHANNEL_SK = FO.CHANNEL_SK
LEFT JOIN DWH_TMF622.DIM_GEOGRAPHY G
    ON G.GEOGRAPHY_SK = FO.GEOGRAPHY_SK
LEFT JOIN DWH_TMF622.DIM_PRODUCT_OFFERING PO
    ON PO.PRODUCT_OFFERING_SK = FO.PRODUCT_OFFERING_SK AND PO.CURRENT_ROW_FLAG = 'Y'
JOIN DWH_TMF622.DIM_DATE D
    ON D.DATE_SK = T.TRANSITION_DATE_SK
   AND D.FULL_DATE >= CURRENT_DATE - INTERVAL '30' DAY

WHERE T.CHANGE_REASON_CD IS NOT NULL

GROUP BY 1,2,3,4,5,6
HAVING COUNT(*) >= 5                                               -- suppress noise
ORDER BY FALLOUT_COUNT DESC;

/* ────────────────────────────────────────────────────────────────────────────
   UC-07: PRODUCT MIX & BUNDLE PERFORMANCE
   ──────────────────────────────────────────────────────────────────────────── */

SELECT
    PO.PRODUCT_FAMILY_NM,
    PO.PRODUCT_TYPE_CD,
    PO.IS_BUNDLE_FLAG,
    PO.IS_5G_FLAG,
    PO.CONTRACT_DURATION_MONTHS,
    G.ADMIN_REGION_NM_EN                                            AS REGION,
    C.CUSTOMER_SEGMENT_CD,
    D.YEAR_NUM,
    D.MONTH_NUM,

    COUNT(FI.ORDER_ITEM_SK)                                         AS UNITS_ORDERED,
    SUM(FI.ITEM_TOTAL_AMOUNT)                                       AS GROSS_REVENUE_SAR,
    SUM(FI.ITEM_MRC)                                                AS TOTAL_MRC_SAR,
    SUM(FI.ITEM_OTC)                                                AS TOTAL_OTC_SAR,
    AVG(FI.ITEM_TOTAL_AMOUNT)                                       AS AVG_ITEM_VALUE_SAR,
    AVG(FI.ITEM_MRC)                                                AS AVG_MRC_SAR,
    SUM(FI.ITEM_DISCOUNT_AMOUNT)                                    AS TOTAL_DISCOUNTS_SAR,
    CAST(SUM(FI.ITEM_DISCOUNT_AMOUNT) AS DECIMAL(10,4))
      / NULLIF(SUM(FI.ITEM_TOTAL_AMOUNT),0) * 100                  AS DISCOUNT_DEPTH_PCT,

    -- Contract value (MRC × months)
    SUM(FI.ITEM_MRC * COALESCE(FI.CONTRACT_MONTHS,12))             AS TOTAL_CONTRACT_VALUE_SAR,
    AVG(FI.ITEM_MRC * COALESCE(FI.CONTRACT_MONTHS,12))             AS AVG_CONTRACT_VALUE_SAR

FROM DWH_TMF622.FACT_ORDER_ITEM FI
JOIN DWH_TMF622.DIM_PRODUCT_OFFERING PO
    ON PO.PRODUCT_OFFERING_SK = FI.PRODUCT_OFFERING_SK
   AND PO.CURRENT_ROW_FLAG = 'Y'
JOIN DWH_TMF622.DIM_DATE D
    ON D.DATE_SK = FI.DATE_SK
LEFT JOIN DWH_TMF622.DIM_GEOGRAPHY G
    ON G.GEOGRAPHY_SK = FI.GEOGRAPHY_SK
LEFT JOIN DWH_TMF622.DIM_CUSTOMER C
    ON C.CUSTOMER_SK = FI.CUSTOMER_SK AND C.CURRENT_ROW_FLAG = 'Y'
JOIN DWH_TMF622.DIM_ORDER_STATE OS
    ON OS.ORDER_STATE_SK = FI.ITEM_STATE_SK
   AND OS.ORDER_STATE_CD = 'completed'

WHERE D.YEAR_NUM = 2025

GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY GROSS_REVENUE_SAR DESC;

/* ────────────────────────────────────────────────────────────────────────────
   UC-08: AGENT / RETAIL STORE PERFORMANCE SCORECARD
   ──────────────────────────────────────────────────────────────────────────── */

SELECT
    RP.AGENT_ID,
    RP.AGENT_STORE_CD,
    RP.PARTY_NAME                                                   AS AGENT_NAME,
    G.ADMIN_REGION_NM_EN                                            AS REGION,
    D.YEAR_NUM,
    D.MONTH_NUM,

    COUNT(DISTINCT FO.ORDER_SK)                                     AS ORDERS_HANDLED,
    SUM(FO.TOTAL_ORDER_AMOUNT)                                      AS REVENUE_GENERATED_SAR,
    AVG(FO.TOTAL_ORDER_AMOUNT)                                      AS AVG_ORDER_VALUE_SAR,

    -- Completion quality
    CAST(SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed' THEN 1 ELSE 0 END) AS DECIMAL(10,4))
    / NULLIF(COUNT(*),0) * 100                                      AS COMPLETION_RATE_PCT,

    -- SLA adherence
    CAST(SUM(CASE WHEN FO.SLA_BREACH_FLAG = 'N'
                   AND OS.ORDER_STATE_CD = 'completed'
                  THEN 1 ELSE 0 END) AS DECIMAL(10,4))
    / NULLIF(SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed' THEN 1 ELSE 0 END),0) * 100
                                                                    AS SLA_COMPLIANCE_PCT,

    SUM(CASE WHEN FO.FALLOUT_FLAG = 'Y' THEN 1 ELSE 0 END)         AS FALLOUT_COUNT,
    CAST(SUM(CASE WHEN FO.FALLOUT_FLAG = 'Y' THEN 1 ELSE 0 END) AS DECIMAL(10,4))
    / NULLIF(COUNT(*),0) * 100                                      AS FALLOUT_RATE_PCT,

    -- New customer acquisitions
    SUM(CASE WHEN FO.IS_NEW_CUSTOMER_ORDER = 'Y' THEN 1 ELSE 0 END) AS NEW_ACQUISITIONS,

    -- Rank within region
    RANK() OVER (
        PARTITION BY G.ADMIN_REGION_NM_EN, D.YEAR_NUM, D.MONTH_NUM
        ORDER BY SUM(FO.TOTAL_ORDER_AMOUNT) DESC
    )                                                               AS REGION_REVENUE_RANK

FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
JOIN DWH_TMF622.DIM_RELATED_PARTY RP
    ON RP.PARTY_SK = FO.SELLER_PARTY_SK
   AND RP.CURRENT_ROW_FLAG = 'Y'
   AND RP.PARTY_ROLE_CD = 'seller'
   AND RP.AGENT_ID IS NOT NULL
JOIN DWH_TMF622.DIM_DATE D      ON D.DATE_SK = FO.DATE_SK
JOIN DWH_TMF622.DIM_ORDER_STATE OS ON OS.ORDER_STATE_SK = FO.ORDER_STATE_SK
LEFT JOIN DWH_TMF622.DIM_GEOGRAPHY G
    ON G.GEOGRAPHY_SK = FO.GEOGRAPHY_SK

WHERE D.YEAR_NUM = 2025 AND D.MONTH_NUM = EXTRACT(MONTH FROM CURRENT_DATE)

GROUP BY 1,2,3,4,5,6
ORDER BY REVENUE_GENERATED_SAR DESC;

/* ────────────────────────────────────────────────────────────────────────────
   UC-09: ORDER STATE MACHINE FUNNEL / COHORT ANALYSIS
   ──────────────────────────────────────────────────────────────────────────── */

-- Order cohort: what % of orders placed in Jan 2025 reached each state?
WITH JAN_COHORT AS (
    SELECT DISTINCT FO.ORDER_SK
    FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
    JOIN DWH_TMF622.DIM_DATE D ON D.DATE_SK = FO.DATE_SK
    WHERE D.YEAR_NUM = 2025 AND D.MONTH_NUM = 1
),
COHORT_TRANSITIONS AS (
    SELECT
        T.ORDER_SK,
        DS_TO.ORDER_STATE_CD,
        DS_TO.SORT_ORDER,
        T.TRANSITION_DATE_SK,
        ROW_NUMBER() OVER (PARTITION BY T.ORDER_SK, DS_TO.ORDER_STATE_CD
                           ORDER BY T.TRANSITION_DATE_SK) AS FIRST_TIME_IN_STATE
    FROM DWH_TMF622.FACT_ORDER_STATE_TRANSITION T
    JOIN JAN_COHORT C ON C.ORDER_SK = T.ORDER_SK
    JOIN DWH_TMF622.DIM_ORDER_STATE DS_TO ON DS_TO.ORDER_STATE_SK = T.TO_STATE_SK
)
SELECT
    ORDER_STATE_CD,
    SORT_ORDER,
    COUNT(DISTINCT ORDER_SK)                                        AS ORDERS_REACHED_STATE,
    CAST(COUNT(DISTINCT ORDER_SK) AS DECIMAL(10,4))
    / (SELECT COUNT(*) FROM JAN_COHORT) * 100                       AS PCT_OF_COHORT,
    (SELECT COUNT(*) FROM JAN_COHORT)
    - COUNT(DISTINCT ORDER_SK)                                      AS DROPPED_BEFORE_STATE

FROM COHORT_TRANSITIONS
WHERE FIRST_TIME_IN_STATE = 1
GROUP BY 1,2
ORDER BY SORT_ORDER;

/* ────────────────────────────────────────────────────────────────────────────
   UC-10: ZATCA VAT RECONCILIATION REPORT
   Monthly VAT summary for tax filing
   ──────────────────────────────────────────────────────────────────────────── */

SELECT
    SUBSTR(CAST(D.FULL_DATE AS CHAR(10) FORMAT 'YYYY-MM-DD'),1,7)   AS TAX_PERIOD,
    D.HIJRI_MONTH_NM_AR                                             AS HIJRI_PERIOD,
    COALESCE(ORD.STC_SEGMENT_CD,'CONSUMER')                         AS SEGMENT,

    -- Transaction Counts
    COUNT(DISTINCT FO.ORDER_SK)                                     AS TOTAL_TAXABLE_TRANSACTIONS,
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed' THEN 1 ELSE 0 END) AS COMPLETED_TRANSACTIONS,
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'cancelled' THEN 1 ELSE 0 END) AS CANCELLED_TRANSACTIONS,

    -- Revenue Base (Net of VAT)
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.ORDER_AMOUNT_EXCL_VAT ELSE 0 END)              AS NET_TAXABLE_REVENUE_SAR,

    -- VAT at Standard Rate 15%
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.VAT_AMOUNT ELSE 0 END)                         AS VAT_OUTPUT_SAR,

    -- Verify: computed 15% VAT matches stored VAT
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.ORDER_AMOUNT_EXCL_VAT * 0.15 ELSE 0 END)      AS VAT_RECOMPUTED_SAR,

    -- Discrepancy (should be < 0.01 SAR per transaction)
    ABS(
        SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
                 THEN FO.VAT_AMOUNT ELSE 0 END)
        -
        SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
                 THEN FO.ORDER_AMOUNT_EXCL_VAT * 0.15 ELSE 0 END)
    )                                                               AS VAT_VARIANCE_SAR,

    -- Total inclusive of VAT (ZATCA filing line)
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.TOTAL_ORDER_AMOUNT ELSE 0 END)                 AS GROSS_REVENUE_INCL_VAT_SAR,

    -- Discounts (VAT treatment: VAT is calculated post-discount)
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.DISCOUNT_AMOUNT ELSE 0 END)                    AS DISCOUNTS_GIVEN_SAR,
    SUM(CASE WHEN OS.ORDER_STATE_CD = 'completed'
             THEN FO.DISCOUNT_AMOUNT * 0.15 ELSE 0 END)            AS VAT_ON_DISCOUNTS_SAR

FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
JOIN DWH_TMF622.DIM_DATE D
    ON D.DATE_SK = FO.DATE_SK
JOIN DWH_TMF622.DIM_ORDER_STATE OS
    ON OS.ORDER_STATE_SK = FO.ORDER_STATE_SK
LEFT JOIN DWH_TMF622.DIM_PRODUCT_ORDER ORD
    ON ORD.ORDER_SK = FO.ORDER_SK AND ORD.CURRENT_ROW_FLAG = 'Y'

WHERE D.YEAR_NUM = 2025

GROUP BY ROLLUP(
    SUBSTR(CAST(D.FULL_DATE AS CHAR(10) FORMAT 'YYYY-MM-DD'),1,7),
    D.HIJRI_MONTH_NM_AR,
    COALESCE(ORD.STC_SEGMENT_CD,'CONSUMER')
)
ORDER BY TAX_PERIOD, SEGMENT;

/* ────────────────────────────────────────────────────────────────────────────
   BONUS: QUICK OPERATIONAL VIEWS FOR DAILY MONITORING
   ──────────────────────────────────────────────────────────────────────────── */

-- Today's Live Order Pulse (Real-time dashboard feed)
REPLACE VIEW DWH_TMF622.VW_TODAYS_ORDER_PULSE AS
SELECT
    OS.ORDER_STATE_CD,
    CH.CHANNEL_CD,
    COALESCE(ORD.STC_SEGMENT_CD,'CONSUMER')     AS SEGMENT,
    COUNT(*)                                     AS ORDERS_COUNT,
    SUM(FO.TOTAL_ORDER_AMOUNT)                  AS REVENUE_SAR,
    AVG(FO.TOTAL_ORDER_AMOUNT)                  AS AVG_ORDER_SAR,
    SUM(CASE WHEN FO.SLA_BREACH_FLAG='Y' THEN 1 ELSE 0 END) AS SLA_BREACHES
FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
JOIN DWH_TMF622.DIM_DATE D    ON D.DATE_SK = FO.DATE_SK AND D.FULL_DATE = CURRENT_DATE
JOIN DWH_TMF622.DIM_ORDER_STATE OS ON OS.ORDER_STATE_SK = FO.ORDER_STATE_SK
JOIN DWH_TMF622.DIM_CHANNEL CH ON CH.CHANNEL_SK = FO.CHANNEL_SK
LEFT JOIN DWH_TMF622.DIM_PRODUCT_ORDER ORD ON ORD.ORDER_SK = FO.ORDER_SK AND ORD.CURRENT_ROW_FLAG='Y'
GROUP BY 1,2,3;

-- Stuck Orders Alert (orders in non-terminal state for > 24 hours)
REPLACE VIEW DWH_TMF622.VW_STUCK_ORDERS_ALERT AS
SELECT
    ORD.ORDER_BK                                AS ORDER_ID,
    ORD.ORDER_DATE,
    OS.ORDER_STATE_CD                           AS CURRENT_STATE,
    CH.CHANNEL_CD,
    COALESCE(ORD.STC_SEGMENT_CD,'CONSUMER')     AS SEGMENT,
    FO.TOTAL_ORDER_AMOUNT                       AS ORDER_VALUE_SAR,
    FO.SLA_TARGET_HRS,
    FO.SLA_REMAINING_HRS,
    CASE
        WHEN FO.SLA_REMAINING_HRS < 0 THEN 'BREACHED'
        WHEN FO.SLA_REMAINING_HRS < 2 THEN 'AT_RISK'
        ELSE 'OK'
    END                                         AS SLA_STATUS,
    (CURRENT_TIMESTAMP(6) - ORD.ORDER_DATE) HOUR(4) AS AGE_HOURS
FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
JOIN DWH_TMF622.DIM_PRODUCT_ORDER ORD
    ON ORD.ORDER_SK = FO.ORDER_SK AND ORD.CURRENT_ROW_FLAG = 'Y'
JOIN DWH_TMF622.DIM_ORDER_STATE OS
    ON OS.ORDER_STATE_SK = FO.ORDER_STATE_SK
   AND OS.IS_TERMINAL_STATE = 'N'              -- exclude completed/failed
JOIN DWH_TMF622.DIM_DATE D ON D.DATE_SK = FO.DATE_SK
LEFT JOIN DWH_TMF622.DIM_CHANNEL CH ON CH.CHANNEL_SK = FO.CHANNEL_SK
WHERE D.FULL_DATE >= CURRENT_DATE - INTERVAL '7' DAY
  AND (CURRENT_TIMESTAMP(6) - ORD.ORDER_DATE) HOUR(4) > 24
ORDER BY AGE_HOURS DESC;

/* ============================================================================
   END OF SCRIPT: 06_STC_ANALYTICS.sql
   ============================================================================ */
