/* ============================================================================
   SCRIPT : 05_DATA_MART.sql
   PURPOSE: Data Mart DDL + Aggregation Population Scripts
   SCHEMAS: DM_TMF622 (Aggregated KPI layer for BI / MicroStrategy / Power BI)
   PLATFORM: Teradata EDW
   PROJECT : STC Enterprise Data Warehouse — TMF 622 Product Ordering
   VERSION : 2.0
   DATE    : March 2025
   NOTES   : Mirrors GRP_TMF622_13_DATA_MART (Ab Initio Rollup components)
             All mart tables use MULTIVALUE compression and columnar hints.
             Refreshed daily at 07:00 AST by Conduct>It batch plan.
   ============================================================================ */

CREATE DATABASE DM_TMF622
    AS PERMANENT = 100000000000,
       SPOOL     = 20000000000;

-- ══════════════════════════════════════════════════════════════
--  DM_ORDER_KPI — Daily Order KPIs (primary analytics mart)
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DM_TMF622.DM_ORDER_KPI,
     NO FALLBACK,
     CHECKSUM = DEFAULT
(
    KPI_DATE_SK             INTEGER         NOT NULL,            -- YYYYMMDD
    KPI_DATE                DATE            NOT NULL,
    CHANNEL_CD              VARCHAR(30)     NOT NULL,
    ORDER_STATE_CD          VARCHAR(30)     NOT NULL,
    ORDER_CATEGORY_CD       VARCHAR(50),
    STC_SEGMENT_CD          VARCHAR(30)     NOT NULL,
    ADMIN_REGION_CD         VARCHAR(20),
    PRODUCT_TYPE_CD         VARCHAR(50),
    PRODUCT_FAMILY_NM       VARCHAR(100),
    TECHNOLOGY_TIER         VARCHAR(20),
    IS_5G_FLAG              CHAR(1),
    IS_BUNDLE_FLAG          CHAR(1),

    -- Volume KPIs
    TOTAL_ORDERS            INTEGER         NOT NULL DEFAULT 0,
    COMPLETED_ORDERS        INTEGER         NOT NULL DEFAULT 0,
    CANCELLED_ORDERS        INTEGER         NOT NULL DEFAULT 0,
    FAILED_ORDERS           INTEGER         NOT NULL DEFAULT 0,
    PENDING_ORDERS          INTEGER         NOT NULL DEFAULT 0,

    -- Revenue KPIs (SAR)
    GROSS_ORDER_VALUE       DECIMAL(18,2)   NOT NULL DEFAULT 0,
    NET_ORDER_VALUE         DECIMAL(18,2)   NOT NULL DEFAULT 0,  -- after discounts
    VAT_COLLECTED           DECIMAL(18,2)   NOT NULL DEFAULT 0,
    TOTAL_MRC               DECIMAL(18,2)   NOT NULL DEFAULT 0,
    TOTAL_OTC               DECIMAL(18,2)   NOT NULL DEFAULT 0,
    TOTAL_DISCOUNT          DECIMAL(18,2)   NOT NULL DEFAULT 0,
    AVG_ORDER_VALUE         DECIMAL(14,2),
    EXPECTED_ANNUAL_REVENUE DECIMAL(18,2)   NOT NULL DEFAULT 0,

    -- SLA KPIs
    SLA_BREACHED_ORDERS     INTEGER         NOT NULL DEFAULT 0,
    SLA_COMPLIANT_ORDERS    INTEGER         NOT NULL DEFAULT 0,
    SLA_COMPLIANCE_RATE_PCT DECIMAL(8,4),
    AVG_FULFILMENT_HRS      DECIMAL(10,4),
    MAX_FULFILMENT_HRS      DECIMAL(10,2),
    ORDERS_WITHIN_24HRS     INTEGER         NOT NULL DEFAULT 0,
    ORDERS_WITHIN_48HRS     INTEGER         NOT NULL DEFAULT 0,

    -- Quality KPIs
    FALLOUT_ORDERS          INTEGER         NOT NULL DEFAULT 0,
    FALLOUT_RATE_PCT        DECIMAL(8,4),

    -- Customer KPIs
    UNIQUE_CUSTOMERS        INTEGER         NOT NULL DEFAULT 0,
    NEW_CUSTOMER_ORDERS     INTEGER         NOT NULL DEFAULT 0,
    UPGRADE_ORDERS          INTEGER         NOT NULL DEFAULT 0,
    DIGITAL_ORDERS          INTEGER         NOT NULL DEFAULT 0,
    DIGITAL_ORDER_SHARE_PCT DECIMAL(8,4),
    BUNDLE_ORDERS           INTEGER         NOT NULL DEFAULT 0,
    ORDER_5G_COUNT          INTEGER         NOT NULL DEFAULT 0,

    -- Order Items
    TOTAL_ORDER_ITEMS       INTEGER         NOT NULL DEFAULT 0,
    AVG_ITEMS_PER_ORDER     DECIMAL(8,4),

    -- ETL Metadata
    ETL_LOAD_DT             TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6),
    ETL_BATCH_ID            VARCHAR(50)
)
PRIMARY INDEX (KPI_DATE_SK, CHANNEL_CD, STC_SEGMENT_CD)
PARTITION BY RANGE_N(KPI_DATE_SK
    BETWEEN 20200101 AND 20301231 EACH 10000,
    NO RANGE OR UNKNOWN);

-- ══════════════════════════════════════════════════════════════
--  DM_SLA_CITC — CITC SLA Compliance Reporting Mart
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DM_TMF622.DM_SLA_CITC,
     NO FALLBACK
(
    REPORT_DATE_SK          INTEGER         NOT NULL,
    REPORT_DATE             DATE            NOT NULL,
    REPORT_PERIOD           CHAR(7),                             -- YYYY-MM for monthly CITC filing
    SERVICE_TYPE            VARCHAR(50)     NOT NULL,            -- MOBILE|FIBER|5G|ENTERPRISE
    STC_SEGMENT_CD          VARCHAR(30)     NOT NULL,
    ADMIN_REGION_CD         VARCHAR(20),

    -- CITC Mandatory KPIs
    CITC_SLA_THRESHOLD_HRS  DECIMAL(6,1)    NOT NULL,            -- Regulatory threshold
    TOTAL_ORDERS            INTEGER         NOT NULL DEFAULT 0,
    ORDERS_WITHIN_SLA       INTEGER         NOT NULL DEFAULT 0,
    ORDERS_BREACHED_SLA     INTEGER         NOT NULL DEFAULT 0,
    SLA_COMPLIANCE_RATE     DECIMAL(8,4)    NOT NULL DEFAULT 0,  -- Must be >= 95% (CITC rule)
    CITC_TARGET_RATE        DECIMAL(5,2)    NOT NULL DEFAULT 95,
    IS_CITC_COMPLIANT       CHAR(1)         NOT NULL,            -- Y/N regulatory flag
    AVG_FULFILMENT_HRS      DECIMAL(10,4),
    P95_FULFILMENT_HRS      DECIMAL(10,4),                       -- 95th percentile fulfilment
    P99_FULFILMENT_HRS      DECIMAL(10,4),
    MAX_FULFILMENT_HRS      DECIMAL(10,2),

    -- Breach Analysis
    BREACH_0_6HRS           INTEGER         DEFAULT 0,           -- Slight breach (0-6h over SLA)
    BREACH_6_24HRS          INTEGER         DEFAULT 0,           -- Moderate breach
    BREACH_24HRS_PLUS       INTEGER         DEFAULT 0,           -- Severe breach
    MOST_COMMON_BREACH_STATE VARCHAR(50),                        -- State where orders get stuck
    MOST_COMMON_BREACH_REASON VARCHAR(200),

    ETL_LOAD_DT             TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6),
    ETL_BATCH_ID            VARCHAR(50)
)
PRIMARY INDEX (REPORT_DATE_SK, SERVICE_TYPE)
PARTITION BY RANGE_N(REPORT_DATE_SK
    BETWEEN 20200101 AND 20301231 EACH 10000,
    NO RANGE OR UNKNOWN);

-- ══════════════════════════════════════════════════════════════
--  DM_5G_ADOPTION — 5G Product Adoption Tracking Mart
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DM_TMF622.DM_5G_ADOPTION,
     NO FALLBACK
(
    REPORT_DATE_SK          INTEGER         NOT NULL,
    REPORT_DATE             DATE            NOT NULL,
    ADMIN_REGION_CD         VARCHAR(20)     NOT NULL,
    ADMIN_REGION_NM_EN      VARCHAR(100),
    STC_SEGMENT_CD          VARCHAR(30)     NOT NULL,
    PRODUCT_FAMILY_NM       VARCHAR(100),

    TOTAL_ORDERS_ALL        INTEGER         NOT NULL DEFAULT 0,
    ORDERS_5G               INTEGER         NOT NULL DEFAULT 0,
    ORDERS_4G               INTEGER         NOT NULL DEFAULT 0,
    ORDERS_FIBER            INTEGER         NOT NULL DEFAULT 0,
    ADOPTION_RATE_5G_PCT    DECIMAL(8,4),                        -- 5G as % of all orders
    ADOPTION_RATE_FIBER_PCT DECIMAL(8,4),

    -- Revenue
    REVENUE_5G              DECIMAL(18,2)   DEFAULT 0,
    REVENUE_4G              DECIMAL(18,2)   DEFAULT 0,
    AVG_CONTRACT_VALUE_5G   DECIMAL(14,2),
    AVG_CONTRACT_VALUE_4G   DECIMAL(14,2),
    ACV_UPLIFT_5G_VS_4G_PCT DECIMAL(8,4),                        -- % premium 5G carries

    -- Coverage Context
    PCT_ORDERS_IN_5G_COVERAGE DECIMAL(8,4),                      -- In covered areas

    ETL_LOAD_DT             TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6),
    ETL_BATCH_ID            VARCHAR(50)
)
PRIMARY INDEX (REPORT_DATE_SK, ADMIN_REGION_CD);

-- ══════════════════════════════════════════════════════════════
--  DM_REVENUE_ASSURANCE — Revenue Leakage Detection Mart
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DM_TMF622.DM_REVENUE_ASSURANCE,
     NO FALLBACK
(
    REPORT_DATE_SK          INTEGER         NOT NULL,
    REPORT_DATE             DATE            NOT NULL,
    REPORT_MONTH            CHAR(7),                             -- YYYY-MM
    STC_SEGMENT_CD          VARCHAR(30),
    PRODUCT_TYPE_CD         VARCHAR(50),

    -- Expected Revenue (from completed DWH orders)
    COMPLETED_ORDERS        INTEGER         NOT NULL DEFAULT 0,
    EXPECTED_TOTAL_REVENUE  DECIMAL(18,2)   DEFAULT 0,
    EXPECTED_MRC            DECIMAL(18,2)   DEFAULT 0,
    EXPECTED_OTC            DECIMAL(18,2)   DEFAULT 0,
    EXPECTED_VAT            DECIMAL(18,2)   DEFAULT 0,

    -- Reconciliation Flags
    ORDERS_NO_BILLING_RECORD INTEGER         DEFAULT 0,          -- Completed but no CBS charge
    ORDERS_BILLING_MISMATCH  INTEGER         DEFAULT 0,          -- Amount discrepancy > 1 SAR
    ORDERS_OVERBILLED        INTEGER         DEFAULT 0,
    ORDERS_UNDERBILLED       INTEGER         DEFAULT 0,

    -- Leakage Amounts
    UNDERBILLING_AMOUNT     DECIMAL(18,2)   DEFAULT 0,           -- Revenue not billed
    OVERBILLING_AMOUNT      DECIMAL(18,2)   DEFAULT 0,           -- Customer overcharged (liability)
    UNCOLLECTED_REVENUE     DECIMAL(18,2)   DEFAULT 0,           -- Completed orders with no bill
    TOTAL_LEAKAGE_AMOUNT    DECIMAL(18,2)   DEFAULT 0,           -- Sum of leakage

    -- ZATCA VAT Reconciliation
    VAT_EXPECTED            DECIMAL(18,2)   DEFAULT 0,
    VAT_BILLED              DECIMAL(18,2)   DEFAULT 0,
    VAT_VARIANCE            DECIMAL(18,2)   DEFAULT 0,

    LEAKAGE_RATE_PCT        DECIMAL(8,4),

    ETL_LOAD_DT             TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6),
    ETL_BATCH_ID            VARCHAR(50)
)
PRIMARY INDEX (REPORT_DATE_SK, STC_SEGMENT_CD);

-- ══════════════════════════════════════════════════════════════
--  PROC: POPULATE DM_ORDER_KPI
--  Mirrors Ab Initio GRP_TMF622_13_DATA_MART Rollup component
-- ══════════════════════════════════════════════════════════════
REPLACE PROCEDURE DM_TMF622.PROC_LOAD_ORDER_KPI (
    IN P_LOAD_DATE  DATE,
    IN P_BATCH_ID   VARCHAR(50)
)
BEGIN
    DECLARE V_DATE_SK INTEGER;
    SET V_DATE_SK = CAST(P_LOAD_DATE AS INTEGER FORMAT 'YYYYMMDD');

    -- Delete today's existing data (idempotent)
    DELETE FROM DM_TMF622.DM_ORDER_KPI WHERE KPI_DATE_SK = V_DATE_SK;

    INSERT INTO DM_TMF622.DM_ORDER_KPI
    SELECT
        D.DATE_SK                                               AS KPI_DATE_SK,
        D.FULL_DATE                                             AS KPI_DATE,
        COALESCE(CH.CHANNEL_CD, 'UNKNOWN')                      AS CHANNEL_CD,
        COALESCE(ST.ORDER_STATE_CD,'UNKNOWN')                   AS ORDER_STATE_CD,
        COALESCE(FO.ORDER_CATEGORY_CD,'UNKNOWN')                AS ORDER_CATEGORY_CD,
        COALESCE(ORD.STC_SEGMENT_CD,'CONSUMER')                 AS STC_SEGMENT_CD,
        COALESCE(GEO.ADMIN_REGION_CD,'UNKNOWN')                 AS ADMIN_REGION_CD,
        COALESCE(PO.PRODUCT_TYPE_CD,'UNKNOWN')                  AS PRODUCT_TYPE_CD,
        COALESCE(PO.PRODUCT_FAMILY_NM,'UNKNOWN')                AS PRODUCT_FAMILY_NM,
        COALESCE(PO.TECHNOLOGY_TIER,'UNKNOWN')                  AS TECHNOLOGY_TIER,
        COALESCE(FO.IS_5G_ORDER,'N')                            AS IS_5G_FLAG,
        COALESCE(FO.IS_BUNDLE_ORDER,'N')                        AS IS_BUNDLE_FLAG,

        -- ── Volume KPIs ───────────────────────────────────────
        COUNT(*)                                                AS TOTAL_ORDERS,
        SUM(CASE WHEN ST.ORDER_STATE_CD = 'completed' THEN 1 ELSE 0 END) AS COMPLETED_ORDERS,
        SUM(CASE WHEN ST.ORDER_STATE_CD = 'cancelled' THEN 1 ELSE 0 END) AS CANCELLED_ORDERS,
        SUM(CASE WHEN ST.ORDER_STATE_CD = 'failed'    THEN 1 ELSE 0 END) AS FAILED_ORDERS,
        SUM(CASE WHEN ST.ORDER_STATE_CD IN ('pending','held') THEN 1 ELSE 0 END) AS PENDING_ORDERS,

        -- ── Revenue KPIs ──────────────────────────────────────
        SUM(FO.TOTAL_ORDER_AMOUNT)                              AS GROSS_ORDER_VALUE,
        SUM(FO.TOTAL_ORDER_AMOUNT - FO.DISCOUNT_AMOUNT)        AS NET_ORDER_VALUE,
        SUM(FO.VAT_AMOUNT)                                      AS VAT_COLLECTED,
        SUM(FO.RECURRING_CHARGE_MRC)                            AS TOTAL_MRC,
        SUM(FO.ONE_TIME_CHARGE_OTC)                             AS TOTAL_OTC,
        SUM(FO.DISCOUNT_AMOUNT)                                 AS TOTAL_DISCOUNT,
        AVG(FO.TOTAL_ORDER_AMOUNT)                              AS AVG_ORDER_VALUE,
        SUM(FO.EXPECTED_ANNUAL_REVENUE)                         AS EXPECTED_ANNUAL_REVENUE,

        -- ── SLA KPIs ──────────────────────────────────────────
        SUM(CASE WHEN FO.SLA_BREACH_FLAG = 'Y' THEN 1 ELSE 0 END)
                                                                AS SLA_BREACHED_ORDERS,
        SUM(CASE WHEN FO.SLA_BREACH_FLAG = 'N'
                  AND ST.ORDER_STATE_CD  = 'completed' THEN 1 ELSE 0 END)
                                                                AS SLA_COMPLIANT_ORDERS,
        CAST(
            SUM(CASE WHEN FO.SLA_BREACH_FLAG = 'N'
                      AND ST.ORDER_STATE_CD  = 'completed' THEN 1 ELSE 0 END) AS DECIMAL(10,4)
        ) / NULLIF(
            SUM(CASE WHEN ST.ORDER_STATE_CD = 'completed' THEN 1 ELSE 0 END), 0
        ) * 100                                                 AS SLA_COMPLIANCE_RATE_PCT,
        AVG(FO.FULFILMENT_DURATION_HRS)                         AS AVG_FULFILMENT_HRS,
        MAX(FO.FULFILMENT_DURATION_HRS)                         AS MAX_FULFILMENT_HRS,
        SUM(CASE WHEN FO.FULFILMENT_DURATION_HRS <= 24 THEN 1 ELSE 0 END)
                                                                AS ORDERS_WITHIN_24HRS,
        SUM(CASE WHEN FO.FULFILMENT_DURATION_HRS <= 48 THEN 1 ELSE 0 END)
                                                                AS ORDERS_WITHIN_48HRS,

        -- ── Quality KPIs ──────────────────────────────────────
        SUM(CASE WHEN FO.FALLOUT_FLAG = 'Y' THEN 1 ELSE 0 END) AS FALLOUT_ORDERS,
        CAST(
            SUM(CASE WHEN FO.FALLOUT_FLAG = 'Y' THEN 1 ELSE 0 END) AS DECIMAL(10,4)
        ) / NULLIF(COUNT(*), 0) * 100                           AS FALLOUT_RATE_PCT,

        -- ── Customer KPIs ─────────────────────────────────────
        COUNT(DISTINCT FO.CUSTOMER_SK)                          AS UNIQUE_CUSTOMERS,
        SUM(CASE WHEN FO.IS_NEW_CUSTOMER_ORDER = 'Y' THEN 1 ELSE 0 END) AS NEW_CUSTOMER_ORDERS,
        SUM(CASE WHEN FO.IS_UPGRADE_ORDER = 'Y' THEN 1 ELSE 0 END) AS UPGRADE_ORDERS,
        SUM(CASE WHEN FO.IS_DIGITAL_CHANNEL = 'Y' THEN 1 ELSE 0 END) AS DIGITAL_ORDERS,
        CAST(
            SUM(CASE WHEN FO.IS_DIGITAL_CHANNEL = 'Y' THEN 1 ELSE 0 END) AS DECIMAL(10,4)
        ) / NULLIF(COUNT(*), 0) * 100                           AS DIGITAL_ORDER_SHARE_PCT,
        SUM(CASE WHEN FO.IS_BUNDLE_ORDER = 'Y' THEN 1 ELSE 0 END) AS BUNDLE_ORDERS,
        SUM(CASE WHEN FO.IS_5G_ORDER     = 'Y' THEN 1 ELSE 0 END) AS ORDER_5G_COUNT,

        -- ── Order Item KPIs ───────────────────────────────────
        SUM(FO.ORDER_ITEM_COUNT)                                AS TOTAL_ORDER_ITEMS,
        AVG(CAST(FO.ORDER_ITEM_COUNT AS DECIMAL(8,4)))          AS AVG_ITEMS_PER_ORDER,

        CURRENT_TIMESTAMP(6)                                    AS ETL_LOAD_DT,
        P_BATCH_ID                                              AS ETL_BATCH_ID

    FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
    JOIN DWH_TMF622.DIM_DATE D
        ON D.DATE_SK = FO.DATE_SK
       AND D.FULL_DATE = P_LOAD_DATE
    LEFT JOIN DWH_TMF622.DIM_CHANNEL CH
        ON CH.CHANNEL_SK = FO.CHANNEL_SK
    LEFT JOIN DWH_TMF622.DIM_ORDER_STATE ST
        ON ST.ORDER_STATE_SK = FO.ORDER_STATE_SK
    LEFT JOIN DWH_TMF622.DIM_PRODUCT_ORDER ORD
        ON ORD.ORDER_SK = FO.ORDER_SK AND ORD.CURRENT_ROW_FLAG = 'Y'
    LEFT JOIN DWH_TMF622.DIM_PRODUCT_OFFERING PO
        ON PO.PRODUCT_OFFERING_SK = FO.PRODUCT_OFFERING_SK AND PO.CURRENT_ROW_FLAG = 'Y'
    LEFT JOIN DWH_TMF622.DIM_GEOGRAPHY GEO
        ON GEO.GEOGRAPHY_SK = FO.GEOGRAPHY_SK

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12;

END;

-- ══════════════════════════════════════════════════════════════
--  PROC: POPULATE DM_SLA_CITC (Monthly CITC Regulatory Report)
-- ══════════════════════════════════════════════════════════════
REPLACE PROCEDURE DM_TMF622.PROC_LOAD_SLA_CITC (
    IN P_REPORT_MONTH CHAR(7),                                   -- YYYY-MM
    IN P_BATCH_ID     VARCHAR(50)
)
BEGIN
    DELETE FROM DM_TMF622.DM_SLA_CITC WHERE REPORT_MONTH = P_REPORT_MONTH;

    INSERT INTO DM_TMF622.DM_SLA_CITC
    WITH ORDER_SLA AS (
        SELECT
            FO.ORDER_SK,
            FO.FULFILMENT_DURATION_HRS,
            FO.SLA_BREACH_FLAG,
            FO.SLA_TARGET_HRS,
            FO.FALLOUT_FLAG,
            ORD.STC_SEGMENT_CD,
            PO.PRODUCT_TYPE_CD,
            GEO.ADMIN_REGION_CD,
            D.FULL_DATE,
            SUBSTR(CAST(D.FULL_DATE AS CHAR(10) FORMAT 'YYYY-MM-DD'),1,7) AS RPT_MONTH,
            -- Breach classification
            CASE
                WHEN FO.SLA_BREACH_FLAG = 'Y'
                 AND (FO.FULFILMENT_DURATION_HRS - FO.SLA_TARGET_HRS) <= 6  THEN 'SLIGHT'
                WHEN FO.SLA_BREACH_FLAG = 'Y'
                 AND (FO.FULFILMENT_DURATION_HRS - FO.SLA_TARGET_HRS) <= 24 THEN 'MODERATE'
                WHEN FO.SLA_BREACH_FLAG = 'Y'                               THEN 'SEVERE'
                ELSE 'NO_BREACH'
            END AS BREACH_CLASS
        FROM DWH_TMF622.FACT_PRODUCT_ORDER FO
        JOIN DWH_TMF622.DIM_DATE D ON D.DATE_SK = FO.DATE_SK
        LEFT JOIN DWH_TMF622.DIM_PRODUCT_ORDER ORD
            ON ORD.ORDER_SK = FO.ORDER_SK AND ORD.CURRENT_ROW_FLAG = 'Y'
        LEFT JOIN DWH_TMF622.DIM_PRODUCT_OFFERING PO
            ON PO.PRODUCT_OFFERING_SK = FO.PRODUCT_OFFERING_SK AND PO.CURRENT_ROW_FLAG = 'Y'
        LEFT JOIN DWH_TMF622.DIM_GEOGRAPHY GEO
            ON GEO.GEOGRAPHY_SK = FO.GEOGRAPHY_SK
        WHERE SUBSTR(CAST(D.FULL_DATE AS CHAR(10) FORMAT 'YYYY-MM-DD'),1,7) = P_REPORT_MONTH
          AND FO.ORDER_STATE_SK = (
              SELECT ORDER_STATE_SK FROM DWH_TMF622.DIM_ORDER_STATE
              WHERE ORDER_STATE_CD = 'completed'
          )
    )
    SELECT
        CAST(CAST(P_REPORT_MONTH || '-01' AS DATE FORMAT 'YYYY-MM-DD')
             AS INTEGER FORMAT 'YYYYMMDD')                      AS REPORT_DATE_SK,
        CAST(P_REPORT_MONTH || '-01' AS DATE FORMAT 'YYYY-MM-DD') AS REPORT_DATE,
        P_REPORT_MONTH                                          AS REPORT_PERIOD,

        COALESCE(PRODUCT_TYPE_CD,'ALL')                         AS SERVICE_TYPE,
        COALESCE(STC_SEGMENT_CD,'ALL')                          AS STC_SEGMENT_CD,
        COALESCE(ADMIN_REGION_CD,'ALL')                         AS ADMIN_REGION_CD,

        -- CITC threshold by service type
        CASE COALESCE(PRODUCT_TYPE_CD,'ALL')
            WHEN 'FIBER'    THEN 48.0
            WHEN '5G'       THEN 12.0
            WHEN 'MOBILE'   THEN 24.0
            ELSE 24.0
        END                                                     AS CITC_SLA_THRESHOLD_HRS,

        COUNT(*)                                                AS TOTAL_ORDERS,
        SUM(CASE WHEN SLA_BREACH_FLAG = 'N' THEN 1 ELSE 0 END) AS ORDERS_WITHIN_SLA,
        SUM(CASE WHEN SLA_BREACH_FLAG = 'Y' THEN 1 ELSE 0 END) AS ORDERS_BREACHED_SLA,

        CAST(SUM(CASE WHEN SLA_BREACH_FLAG = 'N' THEN 1 ELSE 0 END) AS DECIMAL(10,4))
        / NULLIF(COUNT(*),0) * 100                              AS SLA_COMPLIANCE_RATE,
        95.00                                                   AS CITC_TARGET_RATE,

        CASE WHEN CAST(SUM(CASE WHEN SLA_BREACH_FLAG = 'N' THEN 1 ELSE 0 END) AS DECIMAL(10,4))
                  / NULLIF(COUNT(*),0) * 100 >= 95
             THEN 'Y' ELSE 'N'
        END                                                     AS IS_CITC_COMPLIANT,

        AVG(FULFILMENT_DURATION_HRS)                            AS AVG_FULFILMENT_HRS,

        -- P95: approx via PERCENTILE_DISC
        PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY FULFILMENT_DURATION_HRS)
                                                                AS P95_FULFILMENT_HRS,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY FULFILMENT_DURATION_HRS)
                                                                AS P99_FULFILMENT_HRS,
        MAX(FULFILMENT_DURATION_HRS)                            AS MAX_FULFILMENT_HRS,

        SUM(CASE WHEN BREACH_CLASS = 'SLIGHT'   THEN 1 ELSE 0 END) AS BREACH_0_6HRS,
        SUM(CASE WHEN BREACH_CLASS = 'MODERATE' THEN 1 ELSE 0 END) AS BREACH_6_24HRS,
        SUM(CASE WHEN BREACH_CLASS = 'SEVERE'   THEN 1 ELSE 0 END) AS BREACH_24HRS_PLUS,
        NULL                                                    AS MOST_COMMON_BREACH_STATE,
        NULL                                                    AS MOST_COMMON_BREACH_REASON,

        CURRENT_TIMESTAMP(6)                                    AS ETL_LOAD_DT,
        P_BATCH_ID

    FROM ORDER_SLA
    GROUP BY ROLLUP(PRODUCT_TYPE_CD, STC_SEGMENT_CD, ADMIN_REGION_CD);

END;

-- ══════════════════════════════════════════════════════════════
--  USEFUL DATA MART VIEWS
-- ══════════════════════════════════════════════════════════════

-- Monthly Executive Summary View
REPLACE VIEW DM_TMF622.VW_EXEC_ORDER_SUMMARY AS
SELECT
    SUBSTR(CAST(KPI_DATE AS CHAR(10) FORMAT 'YYYY-MM-DD'),1,7) AS REPORT_MONTH,
    SUM(TOTAL_ORDERS)             AS MONTHLY_ORDERS,
    SUM(COMPLETED_ORDERS)         AS COMPLETED,
    SUM(CANCELLED_ORDERS)         AS CANCELLED,
    SUM(FAILED_ORDERS)            AS FAILED,
    SUM(GROSS_ORDER_VALUE)        AS GROSS_REVENUE_SAR,
    SUM(NET_ORDER_VALUE)          AS NET_REVENUE_SAR,
    SUM(VAT_COLLECTED)            AS VAT_SAR,
    SUM(TOTAL_DISCOUNT)           AS DISCOUNTS_SAR,
    AVG(AVG_ORDER_VALUE)          AS AVG_ORDER_VALUE_SAR,
    SUM(DIGITAL_ORDERS)           AS DIGITAL_ORDERS,
    CAST(SUM(DIGITAL_ORDERS) AS DECIMAL(10,4))
      / NULLIF(SUM(TOTAL_ORDERS),0) * 100
                                  AS DIGITAL_SHARE_PCT,
    SUM(ORDER_5G_COUNT)           AS ORDERS_5G,
    CAST(SUM(ORDER_5G_COUNT) AS DECIMAL(10,4))
      / NULLIF(SUM(TOTAL_ORDERS),0) * 100
                                  AS PCT_5G_ADOPTION,
    AVG(SLA_COMPLIANCE_RATE_PCT)  AS AVG_SLA_COMPLIANCE_PCT,
    SUM(FALLOUT_ORDERS)           AS FALLOUT_ORDERS,
    AVG(FALLOUT_RATE_PCT)         AS AVG_FALLOUT_RATE_PCT
FROM DM_TMF622.DM_ORDER_KPI
GROUP BY 1
ORDER BY 1 DESC;

-- Digital vs Physical Channel Comparison
REPLACE VIEW DM_TMF622.VW_CHANNEL_COMPARISON AS
SELECT
    KPI_DATE,
    CHANNEL_CD,
    CASE WHEN IS_5G_FLAG = 'Y' THEN 'DIGITAL' ELSE 'PHYSICAL' END AS CHANNEL_TYPE,
    SUM(TOTAL_ORDERS)             AS ORDERS,
    SUM(NET_ORDER_VALUE)          AS REVENUE_SAR,
    AVG(AVG_ORDER_VALUE)          AS AVG_ORDER_SAR,
    AVG(SLA_COMPLIANCE_RATE_PCT)  AS SLA_COMPLIANCE_PCT,
    AVG(FALLOUT_RATE_PCT)         AS FALLOUT_RATE_PCT
FROM DM_TMF622.DM_ORDER_KPI
GROUP BY 1,2,3;

-- Regional 5G Adoption Heatmap
REPLACE VIEW DM_TMF622.VW_5G_REGIONAL_ADOPTION AS
SELECT
    SUBSTR(CAST(KPI_DATE AS CHAR(10) FORMAT 'YYYY-MM-DD'),1,7) AS REPORT_MONTH,
    ADMIN_REGION_CD,
    SUM(TOTAL_ORDERS)             AS TOTAL_ORDERS,
    SUM(ORDER_5G_COUNT)           AS ORDERS_5G,
    CAST(SUM(ORDER_5G_COUNT) AS DECIMAL(10,4))
      / NULLIF(SUM(TOTAL_ORDERS),0) * 100
                                  AS PCT_5G_ADOPTION,
    SUM(GROSS_ORDER_VALUE)        AS TOTAL_REVENUE,
    SUM(CASE WHEN IS_5G_FLAG = 'Y' THEN GROSS_ORDER_VALUE ELSE 0 END)
                                  AS REVENUE_5G
FROM DM_TMF622.DM_ORDER_KPI
GROUP BY 1,2;

/* ============================================================================
   END OF SCRIPT: 05_DATA_MART.sql
   ============================================================================ */
