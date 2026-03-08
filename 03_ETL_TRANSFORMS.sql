/* ============================================================================
   SCRIPT : 03_ETL_TRANSFORMS.sql
   PURPOSE: ETL Transformation Logic — ODS Cleansing, SCD2 Processing,
            Fact Table population (mirrors Ab Initio GDE graph logic)
   SCHEMAS: STG_TMF622 → ODS_TMF622 → DWH_TMF622
   PLATFORM: Teradata BTEQ / Stored Procedures
   PROJECT : STC Enterprise Data Warehouse — TMF 622 Product Ordering
   VERSION : 2.0
   DATE    : March 2025

   GRAPH MAPPING:
     PROC_CLEANSE_ORDER         → GRP_TMF622_05_CLEANSE
     PROC_SCD2_CUSTOMER         → GRP_TMF622_06_DIM_CUSTOMER
     PROC_SCD2_PRODUCT          → GRP_TMF622_07_DIM_PRODUCT
     PROC_SCD2_ORDER_DIM        → GRP_TMF622_08_DIM_ORDER
     PROC_LOAD_FACT_ORDER       → GRP_TMF622_10_FACT_ORDER
     PROC_LOAD_FACT_ITEM        → GRP_TMF622_11_FACT_ITEM
     PROC_LOAD_STATE_TRANSITION → GRP_TMF622_12_FACT_TRANSITION
   ============================================================================ */

-- ─────────────────────────────────────────────────────────────
-- ODS DATABASE SETUP
-- ─────────────────────────────────────────────────────────────
CREATE DATABASE ODS_TMF622
    AS PERMANENT = 100000000000,
       SPOOL     = 20000000000;

-- ═══════════════════════════════════════════════════════════
--  ODS TABLES (cleansed, typed, deduplicated)
-- ═══════════════════════════════════════════════════════════

CREATE MULTISET TABLE ODS_TMF622.ODS_PRODUCT_ORDER,
     NO FALLBACK
(
    ORDER_BK                VARCHAR(100)    NOT NULL,
    ORDER_HREF              VARCHAR(500),
    EXTERNAL_ID             VARCHAR(100),
    ORDER_DATE              TIMESTAMP(6)    NOT NULL,
    REQUESTED_START_DT      DATE,
    REQUESTED_COMPLETION_DT DATE,
    EXPECTED_COMPLETION_DT  DATE,
    COMPLETION_DT           TIMESTAMP(6),
    ORDER_STATE_CD          VARCHAR(30)     NOT NULL,
    ORDER_CATEGORY_CD       VARCHAR(50),
    PRIORITY_CD             CHAR(1),
    CANCELLATION_REASON     VARCHAR(500),
    CANCELLATION_DT         DATE,
    NOTIFICATION_CONTACT    VARCHAR(200),
    CHANNEL_CD              VARCHAR(30),
    CUSTOMER_BK             VARCHAR(50),
    BILLING_ACCOUNT_ID      VARCHAR(50),
    SELLER_PARTY_BK         VARCHAR(50),
    STC_SEGMENT_CD          VARCHAR(30),
    ORDER_ITEM_COUNT        SMALLINT,
    TOTAL_ORDER_AMOUNT      DECIMAL(18,2),
    ORDER_AMOUNT_EXCL_VAT   DECIMAL(18,2),
    VAT_AMOUNT              DECIMAL(18,2),
    DISCOUNT_AMOUNT         DECIMAL(18,2),
    RECURRING_CHARGE_MRC    DECIMAL(14,2),
    ONE_TIME_CHARGE_OTC     DECIMAL(14,2),
    DELIVERY_CITY           VARCHAR(100),
    DELIVERY_REGION         VARCHAR(50),
    DELIVERY_POSTAL_CODE    CHAR(5),
    SOURCE_SYSTEM_CD        VARCHAR(30),
    STG_BATCH_ID            VARCHAR(50),
    ODS_LOAD_DT             TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    ODS_CHANGE_HASH         CHAR(32),
    RECORD_ACTION           CHAR(1)                              -- I=Insert, U=Update
)
PRIMARY INDEX (ORDER_BK)
INDEX (CUSTOMER_BK);

CREATE MULTISET TABLE ODS_TMF622.ODS_ORDER_ITEM,
     NO FALLBACK
(
    ORDER_BK                VARCHAR(100)    NOT NULL,
    ORDER_ITEM_ID           VARCHAR(100)    NOT NULL,
    ITEM_STATE_CD           VARCHAR(30)     NOT NULL,
    ACTION_CD               VARCHAR(20),
    PRODUCT_OFFERING_BK     VARCHAR(50),
    PRODUCT_SPEC_BK         VARCHAR(50),
    ITEM_QUANTITY           SMALLINT        NOT NULL DEFAULT 1,
    UNIT_PRICE_EXCL_VAT     DECIMAL(14,4),
    UNIT_PRICE_INCL_VAT     DECIMAL(14,4),
    ITEM_TOTAL_AMOUNT       DECIMAL(18,2),
    ITEM_DISCOUNT_AMOUNT    DECIMAL(18,2),
    ITEM_MRC                DECIMAL(14,2),
    ITEM_OTC                DECIMAL(14,2),
    ITEM_VAT_AMOUNT         DECIMAL(14,2),
    PRODUCT_MSISDN_MASKED   VARCHAR(20),
    SIM_TYPE_CD             VARCHAR(10),
    CONTRACT_MONTHS         SMALLINT,
    TECHNOLOGY_TIER         VARCHAR(20),
    IS_5G_ITEM              CHAR(1)         DEFAULT 'N',
    IS_BUNDLE_ITEM          CHAR(1)         DEFAULT 'N',
    DELIVERY_CITY           VARCHAR(100),
    DELIVERY_REGION         VARCHAR(50),
    DELIVERY_POSTAL_CODE    CHAR(5),
    ODS_LOAD_DT             TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (ORDER_BK, ORDER_ITEM_ID);

-- ═══════════════════════════════════════════════════════════
--  PROC 1: CLEANSE ORDER DATA
--  Mirrors: GRP_TMF622_05_CLEANSE
--  - Parse dates from VARCHAR
--  - Normalise KSA phone numbers to E.164
--  - Standardise states to TMF 622 enum
--  - Compute derived financial fields
--  - Deduplicate (keep MAX ETL load per ORDER_BK)
-- ═══════════════════════════════════════════════════════════
REPLACE PROCEDURE ODS_TMF622.PROC_CLEANSE_ORDER (
    IN P_BATCH_ID   VARCHAR(50)
)
BEGIN
    -- Step 1: Deduplicate staging (keep last-written record per order)
    --         Ab Initio handles this via Scan + Compare before calling DB
    --         Here we replicate via ROW_NUMBER window function
    INSERT INTO ODS_TMF622.ODS_PRODUCT_ORDER
    WITH DEDUPED AS (
        SELECT
            S.*,
            ROW_NUMBER() OVER (
                PARTITION BY S.ORDER_ID
                ORDER BY S.STG_LOAD_DT DESC
            ) AS RN
        FROM STG_TMF622.STG_PRODUCT_ORDER S
        WHERE S.STG_BATCH_ID     = P_BATCH_ID
          AND S.STG_RECORD_STATUS = 'V'                          -- DQ-validated records only
    )
    SELECT
        -- ── Natural Key ──────────────────────────────────────
        D.ORDER_ID                                                  AS ORDER_BK,
        D.ORDER_HREF,
        D.EXTERNAL_ID,

        -- ── Date Parsing (VARCHAR → TIMESTAMP) ───────────────
        CAST(D.ORDER_DATE AS TIMESTAMP(6) FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z')
                                                                    AS ORDER_DATE,
        CAST(D.REQUESTED_START_DATE AS DATE FORMAT 'YYYY-MM-DD')    AS REQUESTED_START_DT,
        CAST(D.REQUESTED_COMPLETION_DT AS DATE FORMAT 'YYYY-MM-DD') AS REQUESTED_COMPLETION_DT,
        CAST(D.EXPECTED_COMPLETION_DT AS DATE FORMAT 'YYYY-MM-DD')  AS EXPECTED_COMPLETION_DT,
        CASE
            WHEN D.COMPLETION_DATE IS NOT NULL AND D.COMPLETION_DATE <> ''
            THEN CAST(D.COMPLETION_DATE AS TIMESTAMP(6) FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z')
        END                                                         AS COMPLETION_DT,

        -- ── State Standardisation ─────────────────────────────
        -- Map any legacy STC OMS state codes to TMF 622 canonical values
        CASE UPPER(TRIM(D.ORDER_STATE))
            WHEN 'ACKNOWLEDGED'  THEN 'acknowledged'
            WHEN 'ACK'           THEN 'acknowledged'
            WHEN 'REJECTED'      THEN 'rejected'
            WHEN 'REJECT'        THEN 'rejected'
            WHEN 'PENDING'       THEN 'pending'
            WHEN 'HOLD'          THEN 'held'
            WHEN 'HELD'          THEN 'held'
            WHEN 'IN_PROGRESS'   THEN 'inProgress'
            WHEN 'INPROGRESS'    THEN 'inProgress'
            WHEN 'CANCELLED'     THEN 'cancelled'
            WHEN 'CANCEL'        THEN 'cancelled'
            WHEN 'COMPLETED'     THEN 'completed'
            WHEN 'COMPLETE'      THEN 'completed'
            WHEN 'FAILED'        THEN 'failed'
            WHEN 'FAIL'          THEN 'failed'
            WHEN 'PARTIAL'       THEN 'partial'
            ELSE                      'UNKNOWN'
        END                                                         AS ORDER_STATE_CD,

        -- ── Order Category Standardisation ───────────────────
        CASE UPPER(TRIM(D.CATEGORY))
            WHEN 'NEW'           THEN 'NEW'
            WHEN 'MODIFY'        THEN 'MODIFY'
            WHEN 'UPGRADE'       THEN 'MODIFY'
            WHEN 'DOWNGRADE'     THEN 'MODIFY'
            WHEN 'TERMINATE'     THEN 'TERMINATE'
            WHEN 'SUSPEND'       THEN 'SUSPEND'
            WHEN 'RESUME'        THEN 'RESUME'
            ELSE                      'NEW'
        END                                                         AS ORDER_CATEGORY_CD,

        D.PRIORITY                                                  AS PRIORITY_CD,
        D.CANCELLATION_REASON,
        NULL                                                        AS CANCELLATION_DT,

        -- ── Phone Normalisation (KSA E.164 format) ────────────
        -- Rule DQ-07: 05xxxxxxxx → +966-5x-XXXX-XXXX (masked for PDPL)
        CASE
            WHEN REGEXP_SIMILAR(D.NOTIFICATION_CONTACT, '^05[0-9]{8}$') = 1
            THEN SUBSTR(D.NOTIFICATION_CONTACT,1,3) || '-XXX-X'
                 || SUBSTR(D.NOTIFICATION_CONTACT,9,3)             -- mask middle digits
            WHEN REGEXP_SIMILAR(D.NOTIFICATION_CONTACT, '^9665[0-9]{8}$') = 1
            THEN SUBSTR(D.NOTIFICATION_CONTACT,1,6) || '-XXX-X'
                 || SUBSTR(D.NOTIFICATION_CONTACT,12,3)
            ELSE D.NOTIFICATION_CONTACT
        END                                                         AS NOTIFICATION_CONTACT,

        -- ── Channel Standardisation ───────────────────────────
        COALESCE(
            CASE UPPER(TRIM(D.CHANNEL_NAME))
                WHEN 'MYAPP'           THEN 'DIGITAL_APP'
                WHEN 'STC APP'         THEN 'DIGITAL_APP'
                WHEN 'JAWWY'           THEN 'DIGITAL_APP'
                WHEN 'WEB'             THEN 'DIGITAL_WEB'
                WHEN 'PORTAL'          THEN 'DIGITAL_WEB'
                WHEN 'STORE'           THEN 'RETAIL'
                WHEN 'RETAIL'          THEN 'RETAIL'
                WHEN 'TELESALES'       THEN 'TELESALES'
                WHEN 'CALL CENTER'     THEN 'TELESALES'
                WHEN 'B2B'             THEN 'B2B_PORTAL'
                WHEN 'PARTNER'         THEN 'PARTNER'
                ELSE D.CHANNEL_ID
            END,
            'UNKNOWN'
        )                                                           AS CHANNEL_CD,

        D.CUSTOMER_ID                                               AS CUSTOMER_BK,
        D.BILLING_ACCOUNT_ID,
        D.SELLER_ID                                                 AS SELLER_PARTY_BK,

        -- ── Segment Standardisation ───────────────────────────
        COALESCE(UPPER(TRIM(D.STC_SEGMENT)), 'CONSUMER')            AS STC_SEGMENT_CD,

        CAST(D.STC_ORDER_ITEM_COUNT AS SMALLINT)                    AS ORDER_ITEM_COUNT,

        -- ── Financial Fields (cast + VAT validation) ──────────
        CAST(COALESCE(D.STC_TOTAL_AMOUNT, '0') AS DECIMAL(18,2))    AS TOTAL_ORDER_AMOUNT,

        -- VAT-exclusive amount = total / 1.15 (ZATCA 15% VAT)
        CAST(COALESCE(D.STC_TOTAL_AMOUNT, '0') AS DECIMAL(18,2)) / 1.15
                                                                    AS ORDER_AMOUNT_EXCL_VAT,

        -- DQ-05: Use stored VAT if present, else recalculate
        CASE
            WHEN D.STC_VAT_AMOUNT IS NOT NULL AND CAST(D.STC_VAT_AMOUNT AS DECIMAL(18,2)) > 0
            THEN CAST(D.STC_VAT_AMOUNT AS DECIMAL(18,2))
            ELSE (CAST(COALESCE(D.STC_TOTAL_AMOUNT, '0') AS DECIMAL(18,2)) / 1.15) * 0.15
        END                                                         AS VAT_AMOUNT,

        CAST(COALESCE(D.STC_DISCOUNT_AMOUNT, '0') AS DECIMAL(18,2)) AS DISCOUNT_AMOUNT,
        0.00                                                        AS RECURRING_CHARGE_MRC,
        0.00                                                        AS ONE_TIME_CHARGE_OTC,

        -- ── Address Fields ────────────────────────────────────
        D.STC_REGION_CODE                                           AS DELIVERY_REGION,
        NULL                                                        AS DELIVERY_CITY,
        NULL                                                        AS DELIVERY_POSTAL_CODE,

        D.STG_SOURCE_SYSTEM                                         AS SOURCE_SYSTEM_CD,
        D.STG_BATCH_ID,
        CURRENT_TIMESTAMP(6)                                        AS ODS_LOAD_DT,

        -- ── Change Detection Hash (for SCD2 comparison) ───────
        -- Hash of all TMF 622 Type-2 attributes
        HASHBYTE('MD5',
            COALESCE(UPPER(TRIM(D.ORDER_STATE)), '')
            || COALESCE(D.EXPECTED_COMPLETION_DT, '')
            || COALESCE(D.REQUESTED_COMPLETION_DT, '')
            || COALESCE(UPPER(TRIM(D.STC_SEGMENT)), '')
        )                                                           AS ODS_CHANGE_HASH,

        'I'                                                         AS RECORD_ACTION

    FROM DEDUPED D
    WHERE RN = 1;

END;

-- ═══════════════════════════════════════════════════════════
--  PROC 2: SCD TYPE 2 — CUSTOMER DIMENSION
--  Mirrors: GRP_TMF622_06_DIM_CUSTOMER (Ab Initio Sort-Merge)
--  Algorithm:
--    1. Hash incoming Type-2 attributes
--    2. Compare with current DWH record hash
--    3. If changed: expire old row, insert new row
--    4. If unchanged Type-1 change: in-place UPDATE
--    5. If new record: INSERT new current row
-- ═══════════════════════════════════════════════════════════
REPLACE PROCEDURE DWH_TMF622.PROC_SCD2_CUSTOMER (
    IN P_BATCH_ID       VARCHAR(50),
    IN P_EFFECTIVE_DATE DATE
)
BEGIN
    DECLARE V_NEXT_SK BIGINT;

    -- ── Step A: Expire rows where Type-2 attributes changed ──
    UPDATE DWH_TMF622.DIM_CUSTOMER TGT
    FROM   ODS_TMF622.STG_CUSTOMER_DELTA SRC             -- created by Ab Initio Compare step
    SET    TGT.ROW_EXP_DT      = P_EFFECTIVE_DATE - INTERVAL '1' DAY,
           TGT.CURRENT_ROW_FLAG = 'N',
           TGT.ETL_UPDATED_DT   = CURRENT_TIMESTAMP(6)
    WHERE  TGT.CUSTOMER_BK     = SRC.CUSTOMER_BK
      AND  TGT.CURRENT_ROW_FLAG = 'Y'
      AND  SRC.CHANGE_TYPE       = 'SCD2_CHANGE'         -- flagged by Ab Initio hash compare
      AND  TGT.CHANGE_HASH      <> SRC.CHANGE_HASH;      -- hash mismatch = attribute changed

    -- ── Step B: Insert new SCD2 version for changed records ──
    -- Ab Initio Generate Sequence allocates a block of SKs; we replicate here
    INSERT INTO DWH_TMF622.DIM_CUSTOMER
    SELECT
        (SELECT NEXT_VAL FROM DWH_TMF622.SEQ_CONTROL WHERE SEQ_NAME = 'DIM_CUSTOMER')
            + ROW_NUMBER() OVER (ORDER BY S.CUSTOMER_ID)
                                                            AS CUSTOMER_SK,
        S.CUSTOMER_ID                                       AS CUSTOMER_BK,
        COALESCE(S.CUSTOMER_TYPE,'INDIVIDUAL')              AS CUSTOMER_TYPE_CD,

        -- PDPL: Hash National ID before storing
        CASE
            WHEN S.NATIONAL_ID IS NOT NULL AND S.NATIONAL_ID <> ''
            THEN HASHBYTE('SHA256', TRIM(S.NATIONAL_ID))
        END                                                 AS NATIONAL_ID_HASH,

        S.NATIONALITY_CODE                                  AS NATIONALITY_CD,
        S.CREDIT_RATING                                     AS CREDIT_RATING_CD,
        S.CUSTOMER_SEGMENT                                  AS CUSTOMER_SEGMENT_CD,
        S.LIFETIME_VALUE_TIER,
        S.CITY                                              AS CITY_NM,
        S.REGION                                            AS REGION_NM,
        S.POSTAL_CODE,
        COALESCE(S.PREFERRED_LANGUAGE,'AR')                 AS PREFERRED_LANGUAGE,
        CAST(S.ACTIVATION_DATE AS DATE FORMAT 'YYYY-MM-DD') AS ACTIVATION_DT,
        CAST(S.CHURN_RISK_SCORE AS DECIMAL(5,4))            AS CHURN_RISK_SCORE,
        CASE WHEN S.CUSTOMER_SEGMENT IN ('PLATINUM','VIP') THEN 'Y' ELSE 'N' END AS IS_VIP_FLAG,
        CASE WHEN S.CUSTOMER_TYPE = 'GOV' THEN 'Y' ELSE 'N' END AS IS_GOV_ENTITY,
        P_EFFECTIVE_DATE                                    AS ROW_EFF_DT,
        DATE '9999-12-31'                                   AS ROW_EXP_DT,
        'Y'                                                 AS CURRENT_ROW_FLAG,

        -- Hash of Type-2 attributes for future change detection
        HASHBYTE('MD5',
            COALESCE(S.CUSTOMER_SEGMENT,'')
            || COALESCE(S.LIFETIME_VALUE_TIER,'')
            || COALESCE(S.CREDIT_RATING,'')
            || COALESCE(S.REGION,'')
            || COALESCE(S.CITY,'')
        )                                                   AS CHANGE_HASH,

        CURRENT_TIMESTAMP(6)                                AS ETL_INSERT_DT,
        NULL                                                AS ETL_UPDATED_DT,
        'CRM_SIEBEL'                                        AS ETL_SOURCE_SYSTEM,
        P_BATCH_ID                                          AS ETL_BATCH_ID

    FROM ODS_TMF622.STG_CUSTOMER_DELTA S
    WHERE S.CHANGE_TYPE IN ('NEW', 'SCD2_CHANGE')
      AND S.BATCH_ID = P_BATCH_ID;

    -- ── Step C: Type-1 update (overwrite in-place, no new version) ──
    UPDATE DWH_TMF622.DIM_CUSTOMER TGT
    FROM   ODS_TMF622.STG_CUSTOMER_DELTA SRC
    SET    TGT.CHURN_RISK_SCORE  = CAST(SRC.CHURN_RISK_SCORE AS DECIMAL(5,4)),
           TGT.IS_VIP_FLAG       = CASE WHEN SRC.CUSTOMER_SEGMENT IN ('PLATINUM','VIP') THEN 'Y' ELSE 'N' END,
           TGT.ETL_UPDATED_DT    = CURRENT_TIMESTAMP(6)
    WHERE  TGT.CUSTOMER_BK      = SRC.CUSTOMER_BK
      AND  TGT.CURRENT_ROW_FLAG  = 'Y'
      AND  SRC.CHANGE_TYPE       = 'SCD1_CHANGE'
      AND  SRC.BATCH_ID          = P_BATCH_ID;

    -- Update sequence control table
    UPDATE DWH_TMF622.SEQ_CONTROL
    SET    NEXT_VAL    = NEXT_VAL + (
               SELECT COUNT(*) FROM ODS_TMF622.STG_CUSTOMER_DELTA
               WHERE CHANGE_TYPE IN ('NEW','SCD2_CHANGE') AND BATCH_ID = P_BATCH_ID
           ) + 1,
           UPDATED_DT  = CURRENT_TIMESTAMP(6)
    WHERE  SEQ_NAME = 'DIM_CUSTOMER';

END;

-- ═══════════════════════════════════════════════════════════
--  PROC 3: SCD TYPE 2 — PRODUCT OFFERING DIMENSION
--  Mirrors: GRP_TMF622_07_DIM_PRODUCT
-- ═══════════════════════════════════════════════════════════
REPLACE PROCEDURE DWH_TMF622.PROC_SCD2_PRODUCT (
    IN P_BATCH_ID       VARCHAR(50),
    IN P_EFFECTIVE_DATE DATE
)
BEGIN
    -- Expire changed rows
    UPDATE DWH_TMF622.DIM_PRODUCT_OFFERING TGT
    FROM   STG_TMF622.STG_PRODUCT_OFFERING SRC
    SET    TGT.ROW_EXP_DT      = P_EFFECTIVE_DATE - INTERVAL '1' DAY,
           TGT.CURRENT_ROW_FLAG = 'N'
    WHERE  TGT.PRODUCT_OFFERING_BK = SRC.PRODUCT_OFFERING_ID
      AND  TGT.CURRENT_ROW_FLAG     = 'Y'
      AND  TGT.CHANGE_HASH          <>
           HASHBYTE('MD5',
               COALESCE(SRC.PRODUCT_OFFERING_NAME,'')
               || COALESCE(SRC.PRODUCT_TYPE,'')
               || COALESCE(SRC.PRODUCT_FAMILY,'')
               || COALESCE(SRC.TECHNOLOGY_TIER,'')
               || COALESCE(SRC.CONTRACT_DURATION,'')
           );

    -- Insert new versions
    INSERT INTO DWH_TMF622.DIM_PRODUCT_OFFERING
    SELECT
        (SELECT NEXT_VAL FROM DWH_TMF622.SEQ_CONTROL WHERE SEQ_NAME = 'DIM_PRODUCT_OFFERING')
            + ROW_NUMBER() OVER (ORDER BY SRC.PRODUCT_OFFERING_ID),
        SRC.PRODUCT_OFFERING_ID,
        SRC.PRODUCT_OFFERING_NAME,
        SRC.PRODUCT_OFFERING_DESC,
        SRC.PRODUCT_SPEC_ID,
        SRC.PRODUCT_SPEC_NAME,
        UPPER(TRIM(SRC.PRODUCT_TYPE)),
        UPPER(TRIM(SRC.PRODUCT_CATEGORY)),
        SRC.PRODUCT_FAMILY,
        UPPER(TRIM(SRC.TECHNOLOGY_TIER)),
        SRC.IS_5G,
        SRC.IS_BUNDLE,
        CASE WHEN UPPER(SRC.PRODUCT_TYPE) = 'IOT' THEN 'Y' ELSE 'N' END,
        CAST(SRC.CONTRACT_DURATION AS SMALLINT),
        CAST(SRC.LAUNCH_DATE AS DATE FORMAT 'YYYY-MM-DD'),
        CASE WHEN SRC.END_OF_SALE_DATE IS NOT NULL
             THEN CAST(SRC.END_OF_SALE_DATE AS DATE FORMAT 'YYYY-MM-DD')
        END,
        CAST(SRC.BASE_PRICE AS DECIMAL(14,4)),
        CAST(SRC.BASE_PRICE AS DECIMAL(14,4)) * 1.15,        -- incl. 15% KSA VAT
        COALESCE(SRC.CURRENCY,'SAR'),
        P_EFFECTIVE_DATE,
        DATE '9999-12-31',
        'Y',
        HASHBYTE('MD5',
            COALESCE(SRC.PRODUCT_OFFERING_NAME,'')
            || COALESCE(SRC.PRODUCT_TYPE,'')
            || COALESCE(SRC.PRODUCT_FAMILY,'')
            || COALESCE(SRC.TECHNOLOGY_TIER,'')
            || COALESCE(SRC.CONTRACT_DURATION,'')
        ),
        CURRENT_TIMESTAMP(6),
        P_BATCH_ID

    FROM STG_TMF622.STG_PRODUCT_OFFERING SRC
    WHERE SRC.STG_BATCH_ID = P_BATCH_ID
      AND NOT EXISTS (
          SELECT 1 FROM DWH_TMF622.DIM_PRODUCT_OFFERING DIM
          WHERE  DIM.PRODUCT_OFFERING_BK = SRC.PRODUCT_OFFERING_ID
            AND  DIM.CURRENT_ROW_FLAG    = 'Y'
            AND  DIM.CHANGE_HASH         =
                 HASHBYTE('MD5',
                     COALESCE(SRC.PRODUCT_OFFERING_NAME,'')
                     || COALESCE(SRC.PRODUCT_TYPE,'')
                     || COALESCE(SRC.PRODUCT_FAMILY,'')
                     || COALESCE(SRC.TECHNOLOGY_TIER,'')
                     || COALESCE(SRC.CONTRACT_DURATION,'')
                 )
      );

END;

-- ═══════════════════════════════════════════════════════════
--  PROC 4: LOAD FACT_PRODUCT_ORDER
--  Mirrors: GRP_TMF622_10_FACT_ORDER
--  - Resolve all FK surrogate keys via Hash Join to dimensions
--  - Calculate derived measures (SLA, fulfilment duration, flags)
--  - Upsert (update if exists, insert if new)
-- ═══════════════════════════════════════════════════════════
REPLACE PROCEDURE DWH_TMF622.PROC_LOAD_FACT_ORDER (
    IN P_BATCH_ID   VARCHAR(50),
    IN P_LOAD_DATE  DATE
)
BEGIN
    -- ── Step 1: Delete existing snapshot rows for today (idempotent reload)
    DELETE FROM DWH_TMF622.FACT_PRODUCT_ORDER
    WHERE ETL_BATCH_ID = P_BATCH_ID;

    -- ── Step 2: Insert from ODS with full SK resolution ──────
    INSERT INTO DWH_TMF622.FACT_PRODUCT_ORDER
    WITH ORDER_BASE AS (
        SELECT
            O.*,
            -- Surrogate key lookup: DIM_DATE (resolved via YYYYMMDD integer cast)
            CAST(O.ORDER_DATE AS DATE FORMAT 'YYYY-MM-DD')
                (FORMAT 'YYYYMMDD') (INTEGER)                   AS ORDER_DATE_SK_CALC,
            -- Time key: HHMM integer
            (EXTRACT(HOUR FROM O.ORDER_DATE) * 100)
            + EXTRACT(MINUTE FROM O.ORDER_DATE)                 AS ORDER_TIME_SK_CALC
        FROM ODS_TMF622.ODS_PRODUCT_ORDER O
        WHERE O.STG_BATCH_ID = P_BATCH_ID
    )
    SELECT
        -- ── Fact Surrogate Key (ORDER_SK || DATE_SK compressed) ─
        (DIM_ORD.ORDER_SK * 100000000) + OB.ORDER_DATE_SK_CALC  AS ORDER_SNAPSHOT_KEY,

        -- ── Foreign Keys (resolved via Hash Joins) ──────────────
        DIM_ORD.ORDER_SK,
        OB.ORDER_DATE_SK_CALC                                   AS DATE_SK,
        COALESCE(DIM_CUST.CUSTOMER_SK,  -1)                     AS CUSTOMER_SK,
        COALESCE(DIM_PROD.PRODUCT_OFFERING_SK, -1)              AS PRODUCT_OFFERING_SK,
        COALESCE(DIM_CHAN.CHANNEL_SK, -1)                        AS CHANNEL_SK,
        COALESCE(DIM_GEO.GEOGRAPHY_SK, -1)                      AS GEOGRAPHY_SK,
        COALESCE(DIM_STATE.ORDER_STATE_SK, -1)                  AS ORDER_STATE_SK,
        OB.ORDER_TIME_SK_CALC                                   AS TIME_SK,
        COALESCE(DIM_PARTY.PARTY_SK, -1)                        AS SELLER_PARTY_SK,
        NULL                                                    AS BILLING_ACCOUNT_SK,

        -- ── Financial Measures ────────────────────────────────
        COALESCE(OB.TOTAL_ORDER_AMOUNT, 0)                      AS TOTAL_ORDER_AMOUNT,
        COALESCE(OB.ORDER_AMOUNT_EXCL_VAT, 0)                   AS ORDER_AMOUNT_EXCL_VAT,
        COALESCE(OB.VAT_AMOUNT, 0)                              AS VAT_AMOUNT,
        COALESCE(OB.DISCOUNT_AMOUNT, 0)                         AS DISCOUNT_AMOUNT,
        COALESCE(OB.RECURRING_CHARGE_MRC, 0)                    AS RECURRING_CHARGE_MRC,
        COALESCE(OB.ONE_TIME_CHARGE_OTC, 0)                     AS ONE_TIME_CHARGE_OTC,

        -- Expected Annual Revenue = MRC * contracted months (default 12)
        COALESCE(OB.RECURRING_CHARGE_MRC, 0) *
            COALESCE(DIM_PROD.CONTRACT_DURATION_MONTHS, 12)     AS EXPECTED_ANNUAL_REVENUE,

        -- ── Operational Measures ─────────────────────────────
        COALESCE(OB.ORDER_ITEM_COUNT, 0)                        AS ORDER_ITEM_COUNT,

        -- Fulfilment duration: hours from acknowledged to completed
        CASE
            WHEN OB.COMPLETION_DT IS NOT NULL AND OB.ORDER_DATE IS NOT NULL
            THEN CAST(
                (OB.COMPLETION_DT - OB.ORDER_DATE) HOUR(4)
                AS DECIMAL(10,2)
            )
        END                                                     AS FULFILMENT_DURATION_HRS,

        -- State transition count (from fact_state_transition — pre-computed in Ab Initio)
        COALESCE((
            SELECT COUNT(*)
            FROM   DWH_TMF622.FACT_ORDER_STATE_TRANSITION T
            JOIN   DWH_TMF622.DIM_PRODUCT_ORDER D2
                   ON D2.ORDER_SK = T.ORDER_SK
            WHERE  D2.ORDER_BK = OB.ORDER_BK
        ), 0)                                                   AS STATE_TRANSITION_COUNT,

        -- SLA target hours (CITC rules by segment and product type)
        CASE
            WHEN OB.STC_SEGMENT_CD = 'ENTERPRISE'  THEN 4.0   -- 4hr SLA for enterprise
            WHEN OB.STC_SEGMENT_CD = 'SMB'          THEN 8.0   -- 8hr for SMB
            WHEN DIM_PROD.IS_5G_FLAG = 'Y'          THEN 12.0  -- 12hr for 5G provisioning
            WHEN DIM_PROD.PRODUCT_TYPE_CD = 'FIBER' THEN 48.0  -- 48hr for fiber install
            ELSE                                         24.0   -- 24hr default (CITC mobile)
        END                                                     AS SLA_TARGET_HRS,

        -- SLA remaining hours (negative = already breached)
        CASE
            WHEN OB.COMPLETION_DT IS NOT NULL THEN NULL        -- completed, no SLA clock
            ELSE
                CASE
                    WHEN OB.STC_SEGMENT_CD = 'ENTERPRISE' THEN 4.0
                    WHEN OB.STC_SEGMENT_CD = 'SMB'         THEN 8.0
                    ELSE 24.0
                END -
                CAST(
                    (CURRENT_TIMESTAMP(6) - OB.ORDER_DATE) HOUR(4)
                    AS DECIMAL(10,2)
                )
        END                                                     AS SLA_REMAINING_HRS,

        -- ── Degenerate Dimensions ─────────────────────────────
        OB.PRIORITY_CD,
        OB.ORDER_CATEGORY_CD,

        -- ── Flag Measures ─────────────────────────────────────
        -- SLA breach: completed but exceeded threshold
        CASE
            WHEN OB.COMPLETION_DT IS NOT NULL
             AND (CAST((OB.COMPLETION_DT - OB.ORDER_DATE) HOUR(4) AS DECIMAL(10,2))) >
                 CASE
                     WHEN OB.STC_SEGMENT_CD = 'ENTERPRISE' THEN 4.0
                     WHEN OB.STC_SEGMENT_CD = 'SMB'         THEN 8.0
                     ELSE 24.0
                 END
            THEN 'Y'
            ELSE 'N'
        END                                                     AS SLA_BREACH_FLAG,

        CASE WHEN OB.ORDER_STATE_CD IN ('held','failed') THEN 'Y' ELSE 'N' END AS FALLOUT_FLAG,
        COALESCE(DIM_PROD.IS_BUNDLE_FLAG,'N')                   AS IS_BUNDLE_ORDER,
        COALESCE(DIM_PROD.IS_5G_FLAG,'N')                       AS IS_5G_ORDER,
        COALESCE(DIM_CHAN.IS_DIGITAL_FLAG,'N')                   AS IS_DIGITAL_CHANNEL,

        -- New customer: first-ever STC order
        CASE
            WHEN NOT EXISTS (
                SELECT 1 FROM DWH_TMF622.FACT_PRODUCT_ORDER FP
                WHERE FP.CUSTOMER_SK = COALESCE(DIM_CUST.CUSTOMER_SK,-1)
                  AND FP.DATE_SK     < OB.ORDER_DATE_SK_CALC
            ) THEN 'Y' ELSE 'N'
        END                                                     AS IS_NEW_CUSTOMER_ORDER,

        CASE WHEN OB.ORDER_CATEGORY_CD = 'MODIFY' THEN 'Y' ELSE 'N' END AS IS_UPGRADE_ORDER,
        'N'                                                     AS REVENUE_ASSURANCE_FLAG,

        -- ── ETL Metadata ──────────────────────────────────────
        CURRENT_TIMESTAMP(6)                                    AS ETL_INSERT_DT,
        P_BATCH_ID                                              AS ETL_BATCH_ID,
        OB.SOURCE_SYSTEM_CD,
        1                                                       AS RECORD_VERSION

    FROM ORDER_BASE OB

    -- ── FK Lookups (Hash Joins in Ab Initio / Nested Query here) ──
    JOIN DWH_TMF622.DIM_PRODUCT_ORDER DIM_ORD
        ON DIM_ORD.ORDER_BK         = OB.ORDER_BK
       AND DIM_ORD.CURRENT_ROW_FLAG  = 'Y'

    LEFT JOIN DWH_TMF622.DIM_CUSTOMER DIM_CUST
        ON DIM_CUST.CUSTOMER_BK     = OB.CUSTOMER_BK
       AND DIM_CUST.CURRENT_ROW_FLAG = 'Y'

    LEFT JOIN DWH_TMF622.DIM_PRODUCT_OFFERING DIM_PROD
        ON DIM_PROD.PRODUCT_OFFERING_BK = (
            SELECT TOP 1 OI.PRODUCT_OFFERING_BK
            FROM   ODS_TMF622.ODS_ORDER_ITEM OI
            WHERE  OI.ORDER_BK   = OB.ORDER_BK
            ORDER  BY OI.UNIT_PRICE_INCL_VAT DESC          -- primary product = highest value item
        )
       AND DIM_PROD.CURRENT_ROW_FLAG = 'Y'

    LEFT JOIN DWH_TMF622.DIM_CHANNEL DIM_CHAN
        ON DIM_CHAN.CHANNEL_CD = OB.CHANNEL_CD

    LEFT JOIN DWH_TMF622.DIM_GEOGRAPHY DIM_GEO
        ON DIM_GEO.GEOGRAPHY_BK = OB.DELIVERY_REGION
           || COALESCE(OB.DELIVERY_POSTAL_CODE,'')

    LEFT JOIN DWH_TMF622.DIM_ORDER_STATE DIM_STATE
        ON DIM_STATE.ORDER_STATE_CD = OB.ORDER_STATE_CD

    LEFT JOIN DWH_TMF622.DIM_RELATED_PARTY DIM_PARTY
        ON DIM_PARTY.PARTY_BK         = OB.SELLER_PARTY_BK
       AND DIM_PARTY.CURRENT_ROW_FLAG  = 'Y';

END;

-- ═══════════════════════════════════════════════════════════
--  PROC 5: LOAD FACT_ORDER_ITEM
--  Mirrors: GRP_TMF622_11_FACT_ITEM
-- ═══════════════════════════════════════════════════════════
REPLACE PROCEDURE DWH_TMF622.PROC_LOAD_FACT_ITEM (
    IN P_BATCH_ID   VARCHAR(50)
)
BEGIN
    DELETE FROM DWH_TMF622.FACT_ORDER_ITEM
    WHERE ETL_BATCH_ID = P_BATCH_ID;

    INSERT INTO DWH_TMF622.FACT_ORDER_ITEM
    SELECT
        (SELECT NEXT_VAL FROM DWH_TMF622.SEQ_CONTROL WHERE SEQ_NAME = 'FACT_ORDER_ITEM')
            + ROW_NUMBER() OVER (ORDER BY OI.ORDER_BK, OI.ORDER_ITEM_ID)
                                                                AS ORDER_ITEM_SK,
        DIM_ORD.ORDER_SK,
        CAST(DIM_ORD.ORDER_DATE AS DATE FORMAT 'YYYY-MM-DD')
            (FORMAT 'YYYYMMDD')(INTEGER)                        AS DATE_SK,
        COALESCE(DIM_CUST.CUSTOMER_SK, -1)                      AS CUSTOMER_SK,
        COALESCE(DIM_PROD.PRODUCT_OFFERING_SK, -1)              AS PRODUCT_OFFERING_SK,
        NULL                                                    AS PRODUCT_SPEC_SK,
        COALESCE(DIM_ISTATE.ORDER_STATE_SK, -1)                 AS ITEM_STATE_SK,
        COALESCE(DIM_ACT.ACTION_SK, -1)                         AS ACTION_SK,
        COALESCE(DIM_GEO.GEOGRAPHY_SK, -1)                      AS GEOGRAPHY_SK,
        COALESCE(DIM_CHAN.CHANNEL_SK, -1)                        AS CHANNEL_SK,
        OI.ORDER_ITEM_ID,

        -- Financial
        COALESCE(OI.UNIT_PRICE_EXCL_VAT, 0)                    AS UNIT_PRICE_EXCL_VAT,
        COALESCE(OI.UNIT_PRICE_INCL_VAT, 0)                    AS UNIT_PRICE_INCL_VAT,
        COALESCE(OI.ITEM_QUANTITY, 1)                           AS ITEM_QUANTITY,
        COALESCE(OI.ITEM_TOTAL_AMOUNT,
            OI.UNIT_PRICE_INCL_VAT * COALESCE(OI.ITEM_QUANTITY,1))
                                                                AS ITEM_TOTAL_AMOUNT,
        COALESCE(OI.ITEM_DISCOUNT_AMOUNT, 0)                    AS ITEM_DISCOUNT_AMOUNT,
        COALESCE(OI.ITEM_VAT_AMOUNT,
            OI.UNIT_PRICE_EXCL_VAT * 0.15 * COALESCE(OI.ITEM_QUANTITY,1))
                                                                AS ITEM_VAT_AMOUNT,
        COALESCE(OI.ITEM_MRC, 0)                                AS ITEM_MRC,
        COALESCE(OI.ITEM_OTC, 0)                                AS ITEM_OTC,

        -- Provisioning duration (computed from state transition fact if available)
        NULL                                                    AS PROVISIONING_DURATION_HRS,

        -- Item-level SLA (same logic as order but per-item basis)
        CASE
            WHEN DIM_PROD.PRODUCT_TYPE_CD = 'FIBER'  THEN 48.0
            WHEN DIM_PROD.IS_5G_FLAG      = 'Y'      THEN 12.0
            ELSE 24.0
        END                                                     AS ITEM_SLA_TARGET_HRS,
        'N'                                                     AS ITEM_SLA_BREACH_FLAG,

        OI.PRODUCT_MSISDN_MASKED,
        OI.SIM_TYPE_CD,
        OI.CONTRACT_MONTHS,
        OI.TECHNOLOGY_TIER,
        COALESCE(OI.IS_5G_ITEM, 'N')                            AS IS_5G_ITEM,
        COALESCE(OI.IS_BUNDLE_ITEM, 'N')                        AS IS_BUNDLE_ITEM,

        CURRENT_TIMESTAMP(6)                                    AS ETL_INSERT_DT,
        P_BATCH_ID                                              AS ETL_BATCH_ID

    FROM ODS_TMF622.ODS_ORDER_ITEM OI

    JOIN DWH_TMF622.DIM_PRODUCT_ORDER DIM_ORD
        ON DIM_ORD.ORDER_BK         = OI.ORDER_BK
       AND DIM_ORD.CURRENT_ROW_FLAG  = 'Y'

    LEFT JOIN DWH_TMF622.DIM_CUSTOMER DIM_CUST
        ON DIM_CUST.CUSTOMER_BK     = (
            SELECT CUSTOMER_BK FROM ODS_TMF622.ODS_PRODUCT_ORDER
            WHERE ORDER_BK = OI.ORDER_BK
        )
       AND DIM_CUST.CURRENT_ROW_FLAG = 'Y'

    LEFT JOIN DWH_TMF622.DIM_PRODUCT_OFFERING DIM_PROD
        ON DIM_PROD.PRODUCT_OFFERING_BK = OI.PRODUCT_OFFERING_BK
       AND DIM_PROD.CURRENT_ROW_FLAG    = 'Y'

    LEFT JOIN DWH_TMF622.DIM_ORDER_STATE DIM_ISTATE
        ON DIM_ISTATE.ORDER_STATE_CD = OI.ITEM_STATE_CD

    LEFT JOIN DWH_TMF622.DIM_ORDER_ACTION DIM_ACT
        ON DIM_ACT.ACTION_CD = OI.ACTION_CD

    LEFT JOIN DWH_TMF622.DIM_GEOGRAPHY DIM_GEO
        ON DIM_GEO.GEOGRAPHY_BK = OI.DELIVERY_REGION
           || COALESCE(OI.DELIVERY_POSTAL_CODE,'')

    LEFT JOIN DWH_TMF622.DIM_CHANNEL DIM_CHAN
        ON DIM_CHAN.CHANNEL_CD = (
            SELECT CHANNEL_CD FROM ODS_TMF622.ODS_PRODUCT_ORDER
            WHERE ORDER_BK = OI.ORDER_BK
        )

    WHERE OI.ODS_LOAD_DT >= CURRENT_DATE - INTERVAL '1' DAY;

END;

-- ═══════════════════════════════════════════════════════════
--  PROC 6: LOAD FACT_ORDER_STATE_TRANSITION
--  Mirrors: GRP_TMF622_12_FACT_TRANSITION
--  Ab Initio Scan component computes LAG state and duration
-- ═══════════════════════════════════════════════════════════
REPLACE PROCEDURE DWH_TMF622.PROC_LOAD_STATE_TRANSITION (
    IN P_BATCH_ID   VARCHAR(50)
)
BEGIN
    DELETE FROM DWH_TMF622.FACT_ORDER_STATE_TRANSITION
    WHERE ETL_BATCH_ID = P_BATCH_ID;

    INSERT INTO DWH_TMF622.FACT_ORDER_STATE_TRANSITION
    WITH ORDERED_EVENTS AS (
        SELECT
            E.ORDER_ID,
            E.ORDER_ITEM_ID,
            E.FROM_STATE,
            E.TO_STATE,
            CAST(E.EVENT_TIMESTAMP AS TIMESTAMP(6) FORMAT 'YYYY-MM-DDBHH:MI:SS') AS EVT_TS,
            E.CHANGE_REASON_CODE,
            E.CHANGE_REASON_DESC,
            E.IS_AUTOMATED_CHANGE,
            E.EVENT_SOURCE,
            E.CHANGED_BY_USER_ID,

            -- Ab Initio Scan: compute duration in FROM state
            -- = current event TS - previous event TS for same order
            LAG(CAST(E.EVENT_TIMESTAMP AS TIMESTAMP(6) FORMAT 'YYYY-MM-DDBHH:MI:SS'))
                OVER (PARTITION BY E.ORDER_ID ORDER BY E.EVENT_TIMESTAMP) AS PREV_EVT_TS

        FROM STG_TMF622.STG_ORDER_STATE_EVENT E
        WHERE E.STG_BATCH_ID     = P_BATCH_ID
          AND E.STG_RECORD_STATUS = 'V'
    )
    SELECT
        (SELECT NEXT_VAL FROM DWH_TMF622.SEQ_CONTROL WHERE SEQ_NAME = 'FACT_STATE_TRANSITION')
            + ROW_NUMBER() OVER (ORDER BY OE.ORDER_ID, OE.EVT_TS)  AS TRANSITION_SK,

        DIM_ORD.ORDER_SK,
        NULL                                                        AS ORDER_ITEM_SK, -- item-level handled separately

        COALESCE(DIM_FROM.ORDER_STATE_SK, -1)                       AS FROM_STATE_SK,
        COALESCE(DIM_TO.ORDER_STATE_SK,   -1)                       AS TO_STATE_SK,

        CAST(OE.EVT_TS AS DATE FORMAT 'YYYY-MM-DD')
            (FORMAT 'YYYYMMDD')(INTEGER)                            AS TRANSITION_DATE_SK,
        (EXTRACT(HOUR FROM OE.EVT_TS) * 100)
        + EXTRACT(MINUTE FROM OE.EVT_TS)                            AS TRANSITION_TIME_SK,

        COALESCE(DIM_PARTY.PARTY_SK, -1)                            AS CHANGED_BY_PARTY_SK,

        OE.ORDER_ID                                                 AS ORDER_BK,
        OE.CHANGE_REASON_CODE,
        OE.CHANGE_REASON_DESC,
        COALESCE(OE.IS_AUTOMATED_CHANGE, 'N')                       AS IS_AUTOMATED_CHANGE,
        OE.EVENT_SOURCE                                             AS EVENT_SOURCE_CD,

        -- Duration in FROM state (hours) — Ab Initio Scan LAG equivalent
        CASE
            WHEN OE.PREV_EVT_TS IS NOT NULL
            THEN CAST(
                (OE.EVT_TS - OE.PREV_EVT_TS) HOUR(4)
                AS DECIMAL(14,4)
            )
            ELSE 0
        END                                                         AS DURATION_IN_FROM_STATE_HRS,

        -- Is this state on the CITC SLA critical path?
        CASE
            WHEN OE.FROM_STATE IN ('acknowledged','inProgress','pending') THEN 'Y'
            ELSE 'N'
        END                                                         AS IS_SLA_CRITICAL_STATE,

        -- SLA elapsed % at time of transition
        CASE
            WHEN OE.PREV_EVT_TS IS NOT NULL
            THEN CAST(
                (OE.EVT_TS - (
                    SELECT MIN(CAST(E2.EVENT_TIMESTAMP AS TIMESTAMP(6) FORMAT 'YYYY-MM-DDBHH:MI:SS'))
                    FROM STG_TMF622.STG_ORDER_STATE_EVENT E2
                    WHERE E2.ORDER_ID = OE.ORDER_ID
                )) HOUR(4)
                AS DECIMAL(8,4)
            ) / 24.0 * 100                                         -- assuming 24hr SLA baseline
        END                                                         AS SLA_ELAPSED_PCT,

        CURRENT_TIMESTAMP(6)                                        AS ETL_INSERT_DT,
        P_BATCH_ID                                                  AS ETL_BATCH_ID

    FROM ORDERED_EVENTS OE

    JOIN DWH_TMF622.DIM_PRODUCT_ORDER DIM_ORD
        ON DIM_ORD.ORDER_BK         = OE.ORDER_ID
       AND DIM_ORD.CURRENT_ROW_FLAG  = 'Y'

    LEFT JOIN DWH_TMF622.DIM_ORDER_STATE DIM_FROM
        ON DIM_FROM.ORDER_STATE_CD = OE.FROM_STATE

    LEFT JOIN DWH_TMF622.DIM_ORDER_STATE DIM_TO
        ON DIM_TO.ORDER_STATE_CD = OE.TO_STATE

    LEFT JOIN DWH_TMF622.DIM_RELATED_PARTY DIM_PARTY
        ON DIM_PARTY.PARTY_BK         = OE.CHANGED_BY_USER_ID
       AND DIM_PARTY.CURRENT_ROW_FLAG  = 'Y';

END;

-- ═══════════════════════════════════════════════════════════
--  MASTER BATCH ORCHESTRATION PROCEDURE
--  Called by Ab Initio Conduct>It plan PLN_TMF622_DAILY
-- ═══════════════════════════════════════════════════════════
REPLACE PROCEDURE DWH_TMF622.PROC_RUN_DAILY_BATCH (
    IN P_BATCH_ID   VARCHAR(50),
    IN P_LOAD_DATE  DATE
)
BEGIN
    -- Step 1: Dimension loads (order matters — facts depend on dims)
    CALL DWH_TMF622.PROC_SCD2_CUSTOMER    (P_BATCH_ID, P_LOAD_DATE);
    CALL DWH_TMF622.PROC_SCD2_PRODUCT     (P_BATCH_ID, P_LOAD_DATE);
    CALL ODS_TMF622.PROC_CLEANSE_ORDER     (P_BATCH_ID);
    -- Geography dim is static — refreshed separately weekly

    -- Step 2: Load DIM_PRODUCT_ORDER (depends on cleansed ODS)
    CALL DWH_TMF622.PROC_SCD2_ORDER_DIM   (P_BATCH_ID, P_LOAD_DATE);

    -- Step 3: Load Fact tables (depend on all dims)
    CALL DWH_TMF622.PROC_LOAD_STATE_TRANSITION (P_BATCH_ID);  -- transitions first (used by order fact)
    CALL DWH_TMF622.PROC_LOAD_FACT_ORDER       (P_BATCH_ID, P_LOAD_DATE);
    CALL DWH_TMF622.PROC_LOAD_FACT_ITEM        (P_BATCH_ID);

    -- Step 4: Mark STG records as processed
    UPDATE STG_TMF622.STG_PRODUCT_ORDER
    SET    STG_RECORD_STATUS = 'P'
    WHERE  STG_BATCH_ID = P_BATCH_ID
      AND  STG_RECORD_STATUS = 'V';

    UPDATE STG_TMF622.STG_PRODUCT_ORDER_ITEM
    SET    STG_RECORD_STATUS = 'P'
    WHERE  STG_BATCH_ID = P_BATCH_ID
      AND  STG_RECORD_STATUS = 'V';

END;

/* ============================================================================
   END OF SCRIPT: 03_ETL_TRANSFORMS.sql
   ============================================================================ */
