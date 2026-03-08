/* ============================================================================
   SCRIPT : 01_STG_DDL.sql
   PURPOSE: Staging (Landing) Layer DDL — TMF 622 Product Ordering
   SCHEMA : STG_TMF622
   PLATFORM: Teradata EDW
   PROJECT : STC Enterprise Data Warehouse — TMF 622 Product Ordering
   VERSION : 2.0
   DATE    : March 2025
   NOTES   : Raw ingestion tables — no transformations applied.
             Data retained for 90 days via row-level TTL mechanism.
             All columns VARCHAR to accept dirty source data.
             Ab Initio GDE graphs write directly to these tables via TPT FastLoad.
   ============================================================================ */

-- ─────────────────────────────────────────────────────────────
-- DATABASE SETUP
-- ─────────────────────────────────────────────────────────────
CREATE DATABASE STG_TMF622
    AS PERMANENT = 50000000000,   -- 50 GB permanent space
       SPOOL     = 10000000000,   -- 10 GB spool space
       TEMPORARY = 5000000000;    -- 5 GB temp space

-- ─────────────────────────────────────────────────────────────
-- STG_PRODUCT_ORDER
-- Source: STC OMS REST API (TMF 622 JSON payload)
-- Grain : One row per ProductOrder per API pull
-- ─────────────────────────────────────────────────────────────
CREATE MULTISET TABLE STG_TMF622.STG_PRODUCT_ORDER,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
    -- ── Ingestion Metadata ────────────────────────────────────
    STG_RECORD_ID           BIGINT          NOT NULL GENERATED ALWAYS AS IDENTITY
                                             (START WITH 1 INCREMENT BY 1 NO CYCLE),
    STG_LOAD_DT             TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    STG_BATCH_ID            VARCHAR(50)     NOT NULL,            -- Ab Initio batch run ID
    STG_SOURCE_SYSTEM       VARCHAR(30)     NOT NULL,            -- OMS | CRM | DIGITAL | B2B | RETAIL
    STG_FILE_NAME           VARCHAR(200),                        -- Source MFS file path
    STG_RECORD_STATUS       CHAR(1)         DEFAULT 'N',         -- N=New, P=Processed, E=Error
    STG_ERROR_DESC          VARCHAR(500),                        -- DQ error description if any

    -- ── TMF 622: ProductOrder Core Fields ─────────────────────
    ORDER_ID                VARCHAR(100),                        -- TMF622: ProductOrder.id
    ORDER_HREF              VARCHAR(500),                        -- TMF622: ProductOrder.href
    EXTERNAL_ID             VARCHAR(100),                        -- CRM Opportunity / Quote ID
    ORDER_DATE              VARCHAR(50),                         -- Raw: parse in ODS layer
    REQUESTED_START_DATE    VARCHAR(50),
    REQUESTED_COMPLETION_DT VARCHAR(50),
    EXPECTED_COMPLETION_DT  VARCHAR(50),
    COMPLETION_DATE         VARCHAR(50),
    ORDER_STATE             VARCHAR(50),                         -- TMF622: acknowledged|inProgress|completed etc.
    PRIORITY                VARCHAR(10),                         -- 0-4
    DESCRIPTION             VARCHAR(2000),
    CATEGORY                VARCHAR(100),                        -- NEW|MODIFY|TERMINATE|SUSPEND|RESUME
    CANCELLATION_REASON     VARCHAR(500),
    CANCELLATION_DATE       VARCHAR(50),
    NOTIFICATION_CONTACT    VARCHAR(200),                        -- Email or mobile
    REQUESTED_RESPONSE_TYPE VARCHAR(50),                         -- sendNotification|sendPlus

    -- ── TMF 622: Channel Reference ────────────────────────────
    CHANNEL_ID              VARCHAR(50),
    CHANNEL_NAME            VARCHAR(100),
    CHANNEL_HREF            VARCHAR(500),

    -- ── TMF 622: Billing Account ──────────────────────────────
    BILLING_ACCOUNT_ID      VARCHAR(50),
    BILLING_ACCOUNT_NAME    VARCHAR(200),
    BILLING_ACCOUNT_HREF    VARCHAR(500),

    -- ── TMF 622: Related Party (JSON array — flattened) ───────
    CUSTOMER_ID             VARCHAR(50),
    CUSTOMER_NAME           VARCHAR(200),
    CUSTOMER_HREF           VARCHAR(500),
    SELLER_ID               VARCHAR(50),
    SELLER_NAME             VARCHAR(200),
    REQUESTER_ID            VARCHAR(50),
    REQUESTER_NAME          VARCHAR(200),

    -- ── TMF 622: Payment Method ───────────────────────────────
    PAYMENT_METHOD_ID       VARCHAR(50),
    PAYMENT_METHOD_TYPE     VARCHAR(50),

    -- ── STC-Specific Extensions ───────────────────────────────
    STC_SEGMENT             VARCHAR(30),                         -- CONSUMER|SMB|ENTERPRISE|WHOLESALE
    STC_REGION_CODE         VARCHAR(20),
    STC_STORE_CODE          VARCHAR(20),
    STC_AGENT_ID            VARCHAR(30),
    STC_ORDER_ITEM_COUNT    VARCHAR(10),                         -- Raw count from JSON array
    STC_TOTAL_AMOUNT        VARCHAR(30),                         -- Raw SAR amount
    STC_VAT_AMOUNT          VARCHAR(30),
    STC_DISCOUNT_AMOUNT     VARCHAR(30),
    RAW_JSON_PAYLOAD        JSON,                                -- Full original JSON for replay

    -- ── Audit ─────────────────────────────────────────────────
    CREATED_BY              VARCHAR(50)     DEFAULT 'ABINITIO_ETL'
)
PRIMARY INDEX (ORDER_ID)
PARTITION BY RANGE_N(CAST(STG_LOAD_DT AS DATE FORMAT 'YYYY-MM-DD')
    BETWEEN DATE '2024-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' MONTH,
    NO RANGE OR UNKNOWN);

-- ─────────────────────────────────────────────────────────────
-- STG_PRODUCT_ORDER_ITEM
-- Source: STC OMS — items array from TMF 622 payload (exploded)
-- Grain : One row per ProductOrderItem per API pull
-- ─────────────────────────────────────────────────────────────
CREATE MULTISET TABLE STG_TMF622.STG_PRODUCT_ORDER_ITEM,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL
(
    STG_RECORD_ID           BIGINT          NOT NULL GENERATED ALWAYS AS IDENTITY,
    STG_LOAD_DT             TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    STG_BATCH_ID            VARCHAR(50)     NOT NULL,
    STG_SOURCE_SYSTEM       VARCHAR(30)     NOT NULL,
    STG_RECORD_STATUS       CHAR(1)         DEFAULT 'N',
    STG_ERROR_DESC          VARCHAR(500),

    -- ── Parent Order Reference ────────────────────────────────
    ORDER_ID                VARCHAR(100),                        -- FK → STG_PRODUCT_ORDER
    ORDER_ITEM_ID           VARCHAR(100),                        -- TMF622: ProductOrderItem.id
    ORDER_ITEM_HREF         VARCHAR(500),
    ITEM_QUANTITY           VARCHAR(20),

    -- ── Item State ────────────────────────────────────────────
    ITEM_STATE              VARCHAR(50),                         -- Same state enum as order
    ACTION                  VARCHAR(30),                         -- add|modify|delete|noChange

    -- ── Product Offering Reference ────────────────────────────
    PRODUCT_OFFERING_ID     VARCHAR(50),                         -- TMF622: productOffering.id
    PRODUCT_OFFERING_NAME   VARCHAR(200),
    PRODUCT_OFFERING_HREF   VARCHAR(500),

    -- ── Product Specification Reference ──────────────────────
    PRODUCT_SPEC_ID         VARCHAR(50),
    PRODUCT_SPEC_NAME       VARCHAR(200),
    PRODUCT_SPEC_VERSION    VARCHAR(20),

    -- ── Pricing (TMF 622: ProductOrderPrice) ─────────────────
    PRICE_TYPE              VARCHAR(30),                         -- recurring|oneTime|usage
    PRICE_AMOUNT            VARCHAR(30),                         -- Raw amount string
    PRICE_CURRENCY          VARCHAR(10),                         -- SAR
    PRICE_TAX_INCLUDED_FLAG VARCHAR(5),
    TAX_RATE                VARCHAR(10),                         -- 15 (percent)
    DISCOUNT_AMOUNT         VARCHAR(30),
    UNIT_OF_MEASURE         VARCHAR(30),

    -- ── Product Characteristics (key-value pairs) ─────────────
    PRODUCT_MSISDN          VARCHAR(30),                         -- Mobile number if applicable
    PRODUCT_IMSI            VARCHAR(30),
    PRODUCT_IMEI            VARCHAR(30),
    PRODUCT_SIM_TYPE        VARCHAR(20),                         -- PHYSICAL|ESIM
    PRODUCT_DATA_ALLOWANCE  VARCHAR(30),                         -- GB value
    PRODUCT_SPEED_DOWN_MBPS VARCHAR(20),                         -- Fiber/5G speeds
    PRODUCT_SPEED_UP_MBPS   VARCHAR(20),
    PRODUCT_CONTRACT_MONTHS VARCHAR(10),
    PRODUCT_CUSTOM_ATTRS    JSON,                                -- Remaining characteristics

    -- ── Shipping / Service Address ────────────────────────────
    SHIPPING_CITY           VARCHAR(100),
    SHIPPING_REGION         VARCHAR(100),
    SHIPPING_STREET         VARCHAR(200),
    SHIPPING_POSTCODE       VARCHAR(10),
    SHIPPING_COUNTRY        VARCHAR(10)     DEFAULT 'SAU',

    -- ── STC Extensions ───────────────────────────────────────
    STC_TECHNOLOGY_TIER     VARCHAR(20),                         -- 2G|3G|4G|5G|FIBER
    STC_PRODUCT_FAMILY      VARCHAR(100),                        -- STC Nxt|Elite|Business+
    STC_IS_BUNDLE           VARCHAR(5),                          -- true|false
    STC_IS_5G               VARCHAR(5),
    STC_PROVISIONING_SYSTEM VARCHAR(50)                          -- Ericsson|Nokia|Huawei
)
PRIMARY INDEX (ORDER_ID, ORDER_ITEM_ID)
PARTITION BY RANGE_N(CAST(STG_LOAD_DT AS DATE FORMAT 'YYYY-MM-DD')
    BETWEEN DATE '2024-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' MONTH,
    NO RANGE OR UNKNOWN);

-- ─────────────────────────────────────────────────────────────
-- STG_ORDER_STATE_EVENT
-- Source: STC OMS state change events (Kafka CDC + batch)
-- Grain : One row per state transition event
-- ─────────────────────────────────────────────────────────────
CREATE MULTISET TABLE STG_TMF622.STG_ORDER_STATE_EVENT,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL
(
    STG_RECORD_ID           BIGINT          NOT NULL GENERATED ALWAYS AS IDENTITY,
    STG_LOAD_DT             TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    STG_BATCH_ID            VARCHAR(50)     NOT NULL,
    STG_RECORD_STATUS       CHAR(1)         DEFAULT 'N',

    ORDER_ID                VARCHAR(100),
    ORDER_ITEM_ID           VARCHAR(100),                        -- NULL if order-level event
    FROM_STATE              VARCHAR(50),
    TO_STATE                VARCHAR(50),
    EVENT_TIMESTAMP         VARCHAR(50),                         -- Raw TS string
    CHANGE_REASON_CODE      VARCHAR(100),
    CHANGE_REASON_DESC      VARCHAR(500),
    CHANGED_BY_USER_ID      VARCHAR(50),
    CHANGED_BY_SYSTEM       VARCHAR(50),
    IS_AUTOMATED_CHANGE     VARCHAR(5),
    EVENT_SOURCE            VARCHAR(50)                          -- OMS|CRM|PROVISIONING|API
)
PRIMARY INDEX (ORDER_ID)
PARTITION BY RANGE_N(CAST(STG_LOAD_DT AS DATE FORMAT 'YYYY-MM-DD')
    BETWEEN DATE '2024-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' MONTH,
    NO RANGE OR UNKNOWN);

-- ─────────────────────────────────────────────────────────────
-- STG_CUSTOMER (from CRM)
-- ─────────────────────────────────────────────────────────────
CREATE MULTISET TABLE STG_TMF622.STG_CUSTOMER,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL
(
    STG_RECORD_ID           BIGINT          NOT NULL GENERATED ALWAYS AS IDENTITY,
    STG_LOAD_DT             TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    STG_BATCH_ID            VARCHAR(50)     NOT NULL,
    STG_RECORD_STATUS       CHAR(1)         DEFAULT 'N',

    CUSTOMER_ID             VARCHAR(50),                         -- CRM Account Number
    CUSTOMER_TYPE           VARCHAR(30),                         -- INDIVIDUAL|SMB|ENTERPRISE|GOV
    NATIONAL_ID             VARCHAR(20),                         -- SHA-256 hash applied in ODS
    NATIONALITY_CODE        VARCHAR(5),
    CREDIT_RATING           VARCHAR(5),
    CUSTOMER_SEGMENT        VARCHAR(30),
    LIFETIME_VALUE_TIER     VARCHAR(20),
    CITY                    VARCHAR(100),
    REGION                  VARCHAR(50),
    POSTAL_CODE             VARCHAR(10),
    PREFERRED_LANGUAGE      CHAR(2),
    ACTIVATION_DATE         VARCHAR(50),
    CHURN_RISK_SCORE        VARCHAR(20),
    EMAIL_ADDRESS           VARCHAR(200),                        -- Masked in ODS
    PRIMARY_MSISDN          VARCHAR(30)                          -- Masked in ODS
)
PRIMARY INDEX (CUSTOMER_ID)
PARTITION BY RANGE_N(CAST(STG_LOAD_DT AS DATE FORMAT 'YYYY-MM-DD')
    BETWEEN DATE '2024-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' MONTH,
    NO RANGE OR UNKNOWN);

-- ─────────────────────────────────────────────────────────────
-- STG_PRODUCT_OFFERING (from Amdocs PCM)
-- ─────────────────────────────────────────────────────────────
CREATE MULTISET TABLE STG_TMF622.STG_PRODUCT_OFFERING,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL
(
    STG_RECORD_ID           BIGINT          NOT NULL GENERATED ALWAYS AS IDENTITY,
    STG_LOAD_DT             TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    STG_BATCH_ID            VARCHAR(50)     NOT NULL,
    STG_RECORD_STATUS       CHAR(1)         DEFAULT 'N',

    PRODUCT_OFFERING_ID     VARCHAR(50),
    PRODUCT_OFFERING_NAME   VARCHAR(200),
    PRODUCT_OFFERING_DESC   VARCHAR(2000),
    PRODUCT_SPEC_ID         VARCHAR(50),
    PRODUCT_SPEC_NAME       VARCHAR(200),
    PRODUCT_TYPE            VARCHAR(50),
    PRODUCT_CATEGORY        VARCHAR(50),
    PRODUCT_FAMILY          VARCHAR(100),
    TECHNOLOGY_TIER         VARCHAR(20),
    IS_5G                   VARCHAR(5),
    IS_BUNDLE               VARCHAR(5),
    CONTRACT_DURATION       VARCHAR(10),
    LAUNCH_DATE             VARCHAR(50),
    END_OF_SALE_DATE        VARCHAR(50),
    BASE_PRICE              VARCHAR(30),
    CURRENCY                VARCHAR(10),
    STATUS                  VARCHAR(30)
)
PRIMARY INDEX (PRODUCT_OFFERING_ID);

-- ─────────────────────────────────────────────────────────────
-- ERR_TMF622 — DQ Reject / Error Sink Table
-- ─────────────────────────────────────────────────────────────
CREATE MULTISET TABLE STG_TMF622.ERR_TMF622,
     NO FALLBACK
(
    ERR_RECORD_ID           BIGINT          NOT NULL GENERATED ALWAYS AS IDENTITY,
    ERR_DT                  TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    ERR_BATCH_ID            VARCHAR(50),
    ERR_SOURCE_TABLE        VARCHAR(100),
    ERR_GRAPH_NAME          VARCHAR(100),                        -- Ab Initio GDE graph name
    ERR_RULE_CODE           VARCHAR(20),                         -- DQ-01 through DQ-180
    ERR_RULE_NAME           VARCHAR(200),
    ERR_SEVERITY            VARCHAR(10),                         -- REJECT|WARN|FLAG
    ERR_FIELD_NAME          VARCHAR(100),
    ERR_FIELD_VALUE         VARCHAR(500),
    ORDER_ID                VARCHAR(100),
    ORDER_ITEM_ID           VARCHAR(100),
    ERR_DESCRIPTION         VARCHAR(1000),
    RAW_RECORD_DATA         VARCHAR(4000),
    RESOLUTION_STATUS       CHAR(1)         DEFAULT 'O',         -- O=Open, R=Resolved, W=Waived
    RESOLVED_BY             VARCHAR(50),
    RESOLVED_DT             TIMESTAMP(6)
)
PRIMARY INDEX (ORDER_ID)
PARTITION BY RANGE_N(CAST(ERR_DT AS DATE FORMAT 'YYYY-MM-DD')
    BETWEEN DATE '2024-01-01' AND DATE '2027-12-31' EACH INTERVAL '1' MONTH,
    NO RANGE OR UNKNOWN);

-- ─────────────────────────────────────────────────────────────
-- INDEXES on STG tables for ETL join performance
-- ─────────────────────────────────────────────────────────────
CREATE INDEX STG_PO_STATUS_IDX (STG_RECORD_STATUS) ON STG_TMF622.STG_PRODUCT_ORDER;
CREATE INDEX STG_POI_STATUS_IDX (STG_RECORD_STATUS) ON STG_TMF622.STG_PRODUCT_ORDER_ITEM;
CREATE INDEX STG_EVT_STATUS_IDX (STG_RECORD_STATUS) ON STG_TMF622.STG_ORDER_STATE_EVENT;

-- ─────────────────────────────────────────────────────────────
-- CLEANUP MACRO — purge staging data older than 90 days
-- ─────────────────────────────────────────────────────────────
REPLACE MACRO STG_TMF622.PURGE_STG_DATA AS (
    DELETE FROM STG_TMF622.STG_PRODUCT_ORDER
    WHERE CAST(STG_LOAD_DT AS DATE) < (CURRENT_DATE - INTERVAL '90' DAY);

    DELETE FROM STG_TMF622.STG_PRODUCT_ORDER_ITEM
    WHERE CAST(STG_LOAD_DT AS DATE) < (CURRENT_DATE - INTERVAL '90' DAY);

    DELETE FROM STG_TMF622.STG_ORDER_STATE_EVENT
    WHERE CAST(STG_LOAD_DT AS DATE) < (CURRENT_DATE - INTERVAL '90' DAY);

    DELETE FROM STG_TMF622.STG_CUSTOMER
    WHERE CAST(STG_LOAD_DT AS DATE) < (CURRENT_DATE - INTERVAL '90' DAY);
);

/* ============================================================================
   END OF SCRIPT: 01_STG_DDL.sql
   ============================================================================ */
