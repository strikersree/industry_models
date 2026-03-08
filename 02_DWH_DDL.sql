/* ============================================================================
   SCRIPT : 02_DWH_DDL.sql
   PURPOSE: Data Warehouse Dimensional Model DDL (Kimball Star Schema)
   SCHEMA : DWH_TMF622
   PLATFORM: Teradata EDW
   PROJECT : STC Enterprise Data Warehouse — TMF 622 Product Ordering
   VERSION : 2.0
   DATE    : March 2025
   NOTES   : Implements Kimball Bus Architecture with conformed dimensions.
             All Type-2 dimensions include ROW_EFF_DT / ROW_EXP_DT / CURRENT_ROW_FLAG.
             Teradata-specific: Partitioned by date, PI on surrogate or natural key.
             Compression applied to low-cardinality columns for storage efficiency.
   ============================================================================ */

CREATE DATABASE DWH_TMF622
    AS PERMANENT = 500000000000,  -- 500 GB permanent space
       SPOOL     = 50000000000,   -- 50 GB spool
       TEMPORARY = 20000000000;

-- ══════════════════════════════════════════════════════════════
--  SEQUENCE TABLES (Ab Initio surrogate key management)
--  Ab Initio reads these via Lookup File component to generate
--  surrogate keys in parallel without DB sequences.
-- ══════════════════════════════════════════════════════════════
CREATE TABLE DWH_TMF622.SEQ_CONTROL (
    SEQ_NAME    VARCHAR(100)    NOT NULL,
    NEXT_VAL    BIGINT          NOT NULL DEFAULT 1,
    STEP_SIZE   INT             NOT NULL DEFAULT 1000,           -- Ab Initio batch allocation
    UPDATED_DT  TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6)
) UNIQUE PRIMARY INDEX (SEQ_NAME);

INSERT INTO DWH_TMF622.SEQ_CONTROL VALUES ('DIM_PRODUCT_ORDER',     1, 1000, CURRENT_TIMESTAMP(6));
INSERT INTO DWH_TMF622.SEQ_CONTROL VALUES ('DIM_CUSTOMER',          1, 1000, CURRENT_TIMESTAMP(6));
INSERT INTO DWH_TMF622.SEQ_CONTROL VALUES ('DIM_PRODUCT_OFFERING',  1, 1000, CURRENT_TIMESTAMP(6));
INSERT INTO DWH_TMF622.SEQ_CONTROL VALUES ('DIM_RELATED_PARTY',     1, 1000, CURRENT_TIMESTAMP(6));
INSERT INTO DWH_TMF622.SEQ_CONTROL VALUES ('FACT_PRODUCT_ORDER',    1, 1000, CURRENT_TIMESTAMP(6));
INSERT INTO DWH_TMF622.SEQ_CONTROL VALUES ('FACT_ORDER_ITEM',       1, 1000, CURRENT_TIMESTAMP(6));
INSERT INTO DWH_TMF622.SEQ_CONTROL VALUES ('FACT_STATE_TRANSITION', 1, 1000, CURRENT_TIMESTAMP(6));

-- ══════════════════════════════════════════════════════════════
--  DIM_DATE — Calendar Dimension (KSA / Hijri Extended)
--  Populated once via Ab Initio generator; static table
-- ══════════════════════════════════════════════════════════════
CREATE SET TABLE DWH_TMF622.DIM_DATE,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT
(
    DATE_SK                 INTEGER         NOT NULL,            -- YYYYMMDD integer key
    FULL_DATE               DATE            NOT NULL,
    DAY_OF_WEEK_NUM         BYTEINT         NOT NULL,            -- 1=Sunday (KSA week starts Sun)
    DAY_OF_WEEK_NM_EN       VARCHAR(10)     NOT NULL,
    DAY_OF_WEEK_NM_AR       NVARCHAR(20)    NOT NULL,
    DAY_OF_MONTH            BYTEINT         NOT NULL,
    DAY_OF_YEAR             SMALLINT        NOT NULL,
    WEEK_OF_YEAR            BYTEINT         NOT NULL,
    MONTH_NUM               BYTEINT         NOT NULL,
    MONTH_NM_EN             VARCHAR(12)     NOT NULL,
    MONTH_NM_AR             NVARCHAR(20)    NOT NULL,
    QUARTER_NUM             BYTEINT         NOT NULL,
    QUARTER_NM              CHAR(6)         NOT NULL,            -- Q1 2025
    YEAR_NUM                SMALLINT        NOT NULL,
    FISCAL_YEAR             SMALLINT        NOT NULL,            -- STC fiscal year
    FISCAL_QUARTER          CHAR(7)         NOT NULL,            -- FY2025Q1
    FISCAL_MONTH            CHAR(8)         NOT NULL,            -- FY2025M01

    -- Hijri Calendar Extensions
    HIJRI_DATE              VARCHAR(15),                         -- DD/MM/YYYY-H
    HIJRI_DAY               BYTEINT,
    HIJRI_MONTH_NUM         BYTEINT,
    HIJRI_MONTH_NM_AR       NVARCHAR(30),                        -- محرم, صفر, ربيع الأول ...
    HIJRI_YEAR              SMALLINT,

    -- KSA Business Day Flags
    IS_WEEKDAY              CHAR(1)         NOT NULL,            -- Y=Mon-Fri in KSA (Sat-Thu historically)
    IS_WEEKEND              CHAR(1)         NOT NULL,            -- Y=Fri-Sat (KSA weekend)
    IS_SAUDI_HOLIDAY        CHAR(1)         NOT NULL DEFAULT 'N',
    HOLIDAY_NM              VARCHAR(100),                        -- National Day, Eid, Founding Day
    IS_RAMADAN              CHAR(1)         NOT NULL DEFAULT 'N',
    IS_HAJJ_PERIOD          CHAR(1)         NOT NULL DEFAULT 'N',
    IS_LAST_DAY_OF_MONTH    CHAR(1)         NOT NULL,
    IS_LAST_DAY_OF_QUARTER  CHAR(1)         NOT NULL
)
UNIQUE PRIMARY INDEX (DATE_SK);

-- ══════════════════════════════════════════════════════════════
--  DIM_TIME — Sub-day Time Dimension (minute granularity)
-- ══════════════════════════════════════════════════════════════
CREATE SET TABLE DWH_TMF622.DIM_TIME,
     NO FALLBACK
(
    TIME_SK                 INTEGER         NOT NULL,            -- HHMM integer
    HOUR_24                 BYTEINT         NOT NULL,
    MINUTE_OF_HOUR          BYTEINT         NOT NULL,
    TIME_OF_DAY_NM          VARCHAR(20)     NOT NULL,            -- Morning|Afternoon|Evening|Night
    BUSINESS_PERIOD         VARCHAR(30)     NOT NULL,            -- PEAK|OFF_PEAK|NIGHT_BATCH
    HALF_HOUR_BUCKET        VARCHAR(10)     NOT NULL,            -- 08:00-08:30
    AST_OFFSET_HOURS        BYTEINT         NOT NULL DEFAULT 3   -- UTC+3 Arabia Standard Time
)
UNIQUE PRIMARY INDEX (TIME_SK);

-- ══════════════════════════════════════════════════════════════
--  DIM_GEOGRAPHY — Saudi Arabia Geographic Hierarchy (Type 1)
-- ══════════════════════════════════════════════════════════════
CREATE SET TABLE DWH_TMF622.DIM_GEOGRAPHY,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL
(
    GEOGRAPHY_SK            INTEGER         NOT NULL GENERATED ALWAYS AS IDENTITY,
    GEOGRAPHY_BK            VARCHAR(30)     NOT NULL,            -- CITY_POSTCODE composite NK
    COUNTRY_CD              CHAR(3)         NOT NULL DEFAULT 'SAU',
    COUNTRY_NM              VARCHAR(50)     NOT NULL DEFAULT 'Saudi Arabia',

    -- Saudi Administrative Hierarchy
    ADMIN_REGION_CD         VARCHAR(20)     NOT NULL,            -- MAKKAH|RIYADH|EASTERN|ASIR|...
    ADMIN_REGION_NM_EN      VARCHAR(100)    NOT NULL,
    ADMIN_REGION_NM_AR      NVARCHAR(200)   NOT NULL,
    GOVERNORATE_CD          VARCHAR(20),
    GOVERNORATE_NM_EN       VARCHAR(100),
    GOVERNORATE_NM_AR       NVARCHAR(200),
    CITY_CD                 VARCHAR(20),
    CITY_NM_EN              VARCHAR(100)    NOT NULL,
    CITY_NM_AR              NVARCHAR(200),
    DISTRICT_NM_EN          VARCHAR(100),
    DISTRICT_NM_AR          NVARCHAR(200),
    POSTAL_CODE             CHAR(5),

    -- Geo-Spatial
    LATITUDE                DECIMAL(10,6),
    LONGITUDE               DECIMAL(10,6),

    -- Network Coverage
    BEST_COVERAGE_TYPE      VARCHAR(20),                         -- 5G|4G|3G|FIBER|FIXED
    HAS_5G_COVERAGE         CHAR(1)         DEFAULT 'N',
    HAS_FIBER_COVERAGE      CHAR(1)         DEFAULT 'N',

    -- STC Business Attributes
    SALES_TERRITORY_CD      VARCHAR(20),
    SALES_REGION_MANAGER    VARCHAR(100),
    IS_METRO_AREA           CHAR(1)         DEFAULT 'N',
    POPULATION_TIER         VARCHAR(10),                         -- LARGE|MEDIUM|SMALL

    -- SCD Type 1 Metadata
    ETL_LOAD_DT             TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6),
    ETL_UPDATED_DT          TIMESTAMP(6)
)
UNIQUE PRIMARY INDEX (GEOGRAPHY_SK)
INDEX (GEOGRAPHY_BK)
INDEX (CITY_CD, ADMIN_REGION_CD);

-- ══════════════════════════════════════════════════════════════
--  DIM_ORDER_STATE — TMF 622 State Machine Dimension (Static)
-- ══════════════════════════════════════════════════════════════
CREATE SET TABLE DWH_TMF622.DIM_ORDER_STATE,
     NO FALLBACK
(
    ORDER_STATE_SK          SMALLINT        NOT NULL,
    ORDER_STATE_CD          VARCHAR(30)     NOT NULL,            -- TMF622 enum value
    ORDER_STATE_NM          VARCHAR(50)     NOT NULL,
    ORDER_STATE_NM_AR       NVARCHAR(100),
    STATE_CATEGORY          VARCHAR(20)     NOT NULL,            -- ACTIVE|TERMINAL|PAUSED
    IS_TERMINAL_STATE       CHAR(1)         NOT NULL,            -- Y for completed/failed/cancelled
    IS_SLA_CLOCK_RUNNING    CHAR(1)         NOT NULL,            -- Y when SLA timer active
    IS_BILLABLE_STATE       CHAR(1)         NOT NULL,            -- Y when charges can accrue
    SORT_ORDER              BYTEINT         NOT NULL,            -- For funnel ordering
    STATE_COLOR_HEX         CHAR(7)                              -- Dashboard visualization
)
UNIQUE PRIMARY INDEX (ORDER_STATE_SK)
INDEX (ORDER_STATE_CD);

-- Seed the state dimension with TMF 622 states
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (1,  'acknowledged', 'Acknowledged',   N'مقبول',      'ACTIVE',   'N', 'Y', 'N', 1,  '#1E88E5');
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (2,  'rejected',     'Rejected',        N'مرفوض',      'TERMINAL', 'Y', 'N', 'N', 9,  '#E53935');
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (3,  'pending',      'Pending',         N'معلق',       'PAUSED',   'N', 'N', 'N', 3,  '#FB8C00');
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (4,  'held',         'Held',            N'محتجز',      'PAUSED',   'N', 'N', 'N', 4,  '#F4511E');
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (5,  'inProgress',   'In Progress',     N'قيد التنفيذ','ACTIVE',   'N', 'Y', 'N', 5,  '#43A047');
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (6,  'cancelled',    'Cancelled',       N'ملغى',       'TERMINAL', 'Y', 'N', 'N', 7,  '#757575');
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (7,  'completed',    'Completed',       N'مكتمل',      'TERMINAL', 'Y', 'N', 'Y', 6,  '#00897B');
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (8,  'failed',       'Failed',          N'فشل',        'TERMINAL', 'Y', 'N', 'N', 8,  '#C62828');
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (9,  'partial',      'Partial',         N'جزئي',       'TERMINAL', 'Y', 'N', 'Y', 6,  '#558B2F');
INSERT INTO DWH_TMF622.DIM_ORDER_STATE VALUES (-1, 'UNKNOWN',      'Unknown',         N'غير معروف',  'ACTIVE',   'N', 'N', 'N', 99, '#9E9E9E');

-- ══════════════════════════════════════════════════════════════
--  DIM_CHANNEL — Sales & Service Channel (Type 1 + Type 2 mix)
-- ══════════════════════════════════════════════════════════════
CREATE SET TABLE DWH_TMF622.DIM_CHANNEL,
     NO FALLBACK
(
    CHANNEL_SK              SMALLINT        NOT NULL GENERATED ALWAYS AS IDENTITY,
    CHANNEL_CD              VARCHAR(30)     NOT NULL,            -- Natural key
    CHANNEL_TYPE_CD         VARCHAR(30)     NOT NULL,            -- DIGITAL|PHYSICAL|ASSISTED
    CHANNEL_NM_EN           VARCHAR(100)    NOT NULL,
    CHANNEL_NM_AR           NVARCHAR(200),
    MEDIUM_CD               VARCHAR(30),                         -- MOBILE_APP|WEB|IVRS|IN_STORE
    IS_DIGITAL_FLAG         CHAR(1)         NOT NULL DEFAULT 'N',
    IS_SELF_SERVICE         CHAR(1)         NOT NULL DEFAULT 'N',
    PARTNER_CD              VARCHAR(50),                         -- MVNO or distributor
    PARTNER_NM              VARCHAR(100),
    STC_CHANNEL_GROUP       VARCHAR(30)                          -- DIRECT|INDIRECT|DIGITAL
)
UNIQUE PRIMARY INDEX (CHANNEL_SK)
INDEX (CHANNEL_CD);

-- Seed channel dimension
INSERT INTO DWH_TMF622.DIM_CHANNEL VALUES (DEFAULT,'DIGITAL_APP','DIGITAL','STC App',        N'تطبيق STC',         'MOBILE_APP','Y','Y',NULL,NULL,'DIGITAL');
INSERT INTO DWH_TMF622.DIM_CHANNEL VALUES (DEFAULT,'DIGITAL_WEB','DIGITAL','STC Web Portal', N'البوابة الإلكترونية','WEB',       'Y','Y',NULL,NULL,'DIGITAL');
INSERT INTO DWH_TMF622.DIM_CHANNEL VALUES (DEFAULT,'RETAIL',     'PHYSICAL','Retail Store',  N'متجر التجزئة',       'IN_STORE',  'N','N',NULL,NULL,'DIRECT');
INSERT INTO DWH_TMF622.DIM_CHANNEL VALUES (DEFAULT,'TELESALES',  'ASSISTED','Telesales',     N'المبيعات الهاتفية',  'IVRS',      'N','N',NULL,NULL,'DIRECT');
INSERT INTO DWH_TMF622.DIM_CHANNEL VALUES (DEFAULT,'B2B_PORTAL', 'DIGITAL','B2B Portal',     N'بوابة الأعمال',      'WEB',       'Y','Y',NULL,NULL,'DIGITAL');
INSERT INTO DWH_TMF622.DIM_CHANNEL VALUES (DEFAULT,'PARTNER',    'INDIRECT','Partner/MVNO',  N'الشريك',             'API',       'N','N',NULL,NULL,'INDIRECT');
INSERT INTO DWH_TMF622.DIM_CHANNEL VALUES (DEFAULT,'UNKNOWN',    'UNKNOWN', 'Unknown',       N'غير معروف',          'UNKNOWN',   'N','N',NULL,NULL,'UNKNOWN');

-- ══════════════════════════════════════════════════════════════
--  DIM_ORDER_ACTION — TMF 622 Order Item Action Dimension
-- ══════════════════════════════════════════════════════════════
CREATE SET TABLE DWH_TMF622.DIM_ORDER_ACTION,
     NO FALLBACK
(
    ACTION_SK               SMALLINT        NOT NULL,
    ACTION_CD               VARCHAR(20)     NOT NULL,
    ACTION_NM               VARCHAR(50)     NOT NULL,
    ACTION_NM_AR            NVARCHAR(100),
    IS_REVENUE_GENERATING   CHAR(1)         NOT NULL
)
UNIQUE PRIMARY INDEX (ACTION_SK);

INSERT INTO DWH_TMF622.DIM_ORDER_ACTION VALUES (1, 'add',      'Add (New Product)',     N'إضافة',     'Y');
INSERT INTO DWH_TMF622.DIM_ORDER_ACTION VALUES (2, 'modify',   'Modify (Upgrade)',       N'تعديل',     'Y');
INSERT INTO DWH_TMF622.DIM_ORDER_ACTION VALUES (3, 'delete',   'Delete (Terminate)',     N'حذف',       'N');
INSERT INTO DWH_TMF622.DIM_ORDER_ACTION VALUES (4, 'noChange', 'No Change (Unchanged)',  N'بدون تغيير','N');
INSERT INTO DWH_TMF622.DIM_ORDER_ACTION VALUES (-1,'UNKNOWN',  'Unknown',                N'غير معروف', 'N');

-- ══════════════════════════════════════════════════════════════
--  DIM_CUSTOMER — SCD Type 2 Customer Dimension
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DWH_TMF622.DIM_CUSTOMER,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT
(
    CUSTOMER_SK             BIGINT          NOT NULL,
    CUSTOMER_BK             VARCHAR(50)     NOT NULL,            -- CRM Account Number (NK)
    CUSTOMER_TYPE_CD        VARCHAR(20)     NOT NULL,            -- INDIVIDUAL|SMB|ENTERPRISE|GOV
    NATIONAL_ID_HASH        CHAR(64),                            -- SHA-256 of NID (PDPL compliance)
    NATIONALITY_CD          CHAR(3),                             -- ISO 3166-1
    CREDIT_RATING_CD        CHAR(2),                             -- A1|A2|B1|B2|C
    CUSTOMER_SEGMENT_CD     VARCHAR(30),                         -- PLATINUM|GOLD|SILVER|BRONZE|NEW
    LIFETIME_VALUE_TIER     VARCHAR(20),                         -- HIGH|MID|LOW
    CITY_NM                 VARCHAR(100),
    REGION_NM               VARCHAR(50),
    POSTAL_CODE             CHAR(5),
    PREFERRED_LANGUAGE      CHAR(2)         DEFAULT 'AR',
    ACTIVATION_DT           DATE,
    CHURN_RISK_SCORE        DECIMAL(5,4),                        -- 0.0000 to 1.0000
    IS_VIP_FLAG             CHAR(1)         DEFAULT 'N',
    IS_GOV_ENTITY           CHAR(1)         DEFAULT 'N',

    -- SCD Type 2 Versioning Columns
    ROW_EFF_DT              DATE            NOT NULL,
    ROW_EXP_DT              DATE            NOT NULL DEFAULT '9999-12-31' (DATE),
    CURRENT_ROW_FLAG        CHAR(1)         NOT NULL DEFAULT 'Y',
    CHANGE_HASH             CHAR(32),                            -- MD5 of Type-2 cols for change detection

    -- ETL Metadata
    ETL_INSERT_DT           TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6),
    ETL_UPDATED_DT          TIMESTAMP(6),
    ETL_SOURCE_SYSTEM       VARCHAR(30)     DEFAULT 'CRM_SIEBEL',
    ETL_BATCH_ID            VARCHAR(50)
)
PRIMARY INDEX (CUSTOMER_SK)
PARTITION BY RANGE_N(ROW_EFF_DT
    BETWEEN DATE '2020-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' YEAR,
    NO RANGE OR UNKNOWN)
INDEX (CUSTOMER_BK, CURRENT_ROW_FLAG);

-- ══════════════════════════════════════════════════════════════
--  DIM_PRODUCT_OFFERING — SCD Type 2 Product Catalog Dimension
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DWH_TMF622.DIM_PRODUCT_OFFERING,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL
(
    PRODUCT_OFFERING_SK     BIGINT          NOT NULL,
    PRODUCT_OFFERING_BK     VARCHAR(50)     NOT NULL,            -- PCM Offering ID (NK)
    PRODUCT_OFFERING_NAME   VARCHAR(200)    NOT NULL,
    PRODUCT_OFFERING_DESC   VARCHAR(2000),
    PRODUCT_SPEC_ID         VARCHAR(50),
    PRODUCT_SPEC_NAME       VARCHAR(200),
    PRODUCT_TYPE_CD         VARCHAR(50),                         -- MOBILE|FIBER|5G|IOT|BUNDLE|DEVICE
    PRODUCT_CATEGORY_CD     VARCHAR(50),                         -- PREPAID|POSTPAID|B2B|ROAMING|VAS
    PRODUCT_FAMILY_NM       VARCHAR(100),                        -- STC Nxt|Elite|Business+|Jawwy
    TECHNOLOGY_TIER         VARCHAR(20),                         -- 2G|3G|4G|5G|FIBER|FIXED
    IS_5G_FLAG              CHAR(1)         DEFAULT 'N',
    IS_BUNDLE_FLAG          CHAR(1)         DEFAULT 'N',
    IS_IOT_FLAG             CHAR(1)         DEFAULT 'N',
    CONTRACT_DURATION_MONTHS SMALLINT       DEFAULT 0,
    LAUNCH_DT               DATE,
    END_OF_SALE_DT          DATE,
    BASE_PRICE_SAR          DECIMAL(14,4),
    BASE_PRICE_INCL_VAT     DECIMAL(14,4),
    CURRENCY_CD             CHAR(3)         DEFAULT 'SAR',

    -- SCD Type 2
    ROW_EFF_DT              DATE            NOT NULL,
    ROW_EXP_DT              DATE            NOT NULL DEFAULT '9999-12-31' (DATE),
    CURRENT_ROW_FLAG        CHAR(1)         NOT NULL DEFAULT 'Y',
    CHANGE_HASH             CHAR(32),

    ETL_INSERT_DT           TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6),
    ETL_BATCH_ID            VARCHAR(50)
)
PRIMARY INDEX (PRODUCT_OFFERING_SK)
INDEX (PRODUCT_OFFERING_BK, CURRENT_ROW_FLAG)
INDEX (PRODUCT_TYPE_CD, IS_5G_FLAG);

-- ══════════════════════════════════════════════════════════════
--  DIM_PRODUCT_ORDER — SCD Type 2 Order Header Dimension
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DWH_TMF622.DIM_PRODUCT_ORDER,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL
(
    ORDER_SK                BIGINT          NOT NULL,
    ORDER_BK                VARCHAR(100)    NOT NULL,            -- TMF622: ProductOrder.id (NK)
    ORDER_HREF              VARCHAR(500),
    EXTERNAL_ID             VARCHAR(100),
    ORDER_DATE              TIMESTAMP(6),
    REQUESTED_START_DT      DATE,
    REQUESTED_COMPLETION_DT DATE,
    EXPECTED_COMPLETION_DT  DATE,
    COMPLETION_DT           TIMESTAMP(6),
    ORDER_STATE_CD          VARCHAR(30),
    ORDER_CATEGORY_CD       VARCHAR(50),                         -- NEW|MODIFY|TERMINATE|SUSPEND
    PRIORITY_CD             CHAR(1),                             -- 0-4
    CANCELLATION_REASON     VARCHAR(500),
    CANCELLATION_DT         DATE,
    NOTIFICATION_CONTACT    VARCHAR(200),
    STC_SEGMENT_CD          VARCHAR(30),
    BILLING_ACCOUNT_ID      VARCHAR(50),
    CHANNEL_CD              VARCHAR(30),

    -- SCD Type 2
    ROW_EFF_DT              DATE            NOT NULL,
    ROW_EXP_DT              DATE            NOT NULL DEFAULT '9999-12-31' (DATE),
    CURRENT_ROW_FLAG        CHAR(1)         NOT NULL DEFAULT 'Y',
    CHANGE_HASH             CHAR(32),

    ETL_INSERT_DT           TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6),
    ETL_BATCH_ID            VARCHAR(50)
)
PRIMARY INDEX (ORDER_SK)
PARTITION BY RANGE_N(CAST(ORDER_DATE AS DATE FORMAT 'YYYY-MM-DD')
    BETWEEN DATE '2020-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' MONTH,
    NO RANGE OR UNKNOWN)
INDEX (ORDER_BK, CURRENT_ROW_FLAG)
INDEX (ORDER_STATE_CD, CURRENT_ROW_FLAG);

-- ══════════════════════════════════════════════════════════════
--  DIM_RELATED_PARTY — SCD Type 2 Party Dimension
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DWH_TMF622.DIM_RELATED_PARTY,
     NO FALLBACK
(
    PARTY_SK                BIGINT          NOT NULL,
    PARTY_BK                VARCHAR(50)     NOT NULL,
    PARTY_ROLE_CD           VARCHAR(30),
    PARTY_TYPE_CD           VARCHAR(30),
    PARTY_NAME              VARCHAR(200),
    AGENT_ID                VARCHAR(30),
    AGENT_STORE_CD          VARCHAR(20),
    DEPARTMENT_CD           VARCHAR(50),

    ROW_EFF_DT              DATE            NOT NULL,
    ROW_EXP_DT              DATE            NOT NULL DEFAULT '9999-12-31' (DATE),
    CURRENT_ROW_FLAG        CHAR(1)         NOT NULL DEFAULT 'Y',

    ETL_INSERT_DT           TIMESTAMP(6)    DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (PARTY_SK)
INDEX (PARTY_BK, CURRENT_ROW_FLAG);

-- ══════════════════════════════════════════════════════════════
--  FACT_PRODUCT_ORDER — Order Header Fact (Daily Snapshot)
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DWH_TMF622.FACT_PRODUCT_ORDER,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT
(
    -- ── Surrogate Key ─────────────────────────────────────────
    ORDER_SNAPSHOT_KEY      BIGINT          NOT NULL,            -- FACT PK: ORDER_SK || DATE_SK

    -- ── Dimension Foreign Keys ────────────────────────────────
    ORDER_SK                BIGINT          NOT NULL,
    DATE_SK                 INTEGER         NOT NULL,            -- Order created date
    CUSTOMER_SK             BIGINT          NOT NULL,
    PRODUCT_OFFERING_SK     BIGINT          NOT NULL,            -- Primary product on order
    CHANNEL_SK              SMALLINT        NOT NULL,
    GEOGRAPHY_SK            INTEGER         NOT NULL,            -- Delivery/service region
    ORDER_STATE_SK          SMALLINT        NOT NULL,
    TIME_SK                 INTEGER         NOT NULL,            -- Hour of order creation
    SELLER_PARTY_SK         BIGINT,                              -- FK → DIM_RELATED_PARTY
    BILLING_ACCOUNT_SK      BIGINT,

    -- ── Additive Financial Measures (SAR) ─────────────────────
    TOTAL_ORDER_AMOUNT      DECIMAL(18,2)   NOT NULL DEFAULT 0,
    ORDER_AMOUNT_EXCL_VAT   DECIMAL(18,2)   NOT NULL DEFAULT 0,
    VAT_AMOUNT              DECIMAL(18,2)   NOT NULL DEFAULT 0,
    DISCOUNT_AMOUNT         DECIMAL(18,2)   NOT NULL DEFAULT 0,
    RECURRING_CHARGE_MRC    DECIMAL(14,2)   NOT NULL DEFAULT 0,
    ONE_TIME_CHARGE_OTC     DECIMAL(14,2)   NOT NULL DEFAULT 0,
    EXPECTED_ANNUAL_REVENUE DECIMAL(18,2)            DEFAULT 0, -- MRC * CONTRACT_MONTHS

    -- ── Semi-Additive / Non-Additive Measures ────────────────
    ORDER_ITEM_COUNT        SMALLINT        NOT NULL DEFAULT 0,
    FULFILMENT_DURATION_HRS DECIMAL(10,2),                       -- NULL if not yet completed
    STATE_TRANSITION_COUNT  SMALLINT        NOT NULL DEFAULT 0,
    SLA_TARGET_HRS          DECIMAL(10,2),                       -- CITC SLA for this order type
    SLA_REMAINING_HRS       DECIMAL(10,2),                       -- Negative = breached

    -- ── Degenerate Dimensions (Order Attributes) ──────────────
    PRIORITY_CD             CHAR(1),
    ORDER_CATEGORY_CD       VARCHAR(50),

    -- ── Flag Measures (Y/N) ───────────────────────────────────
    SLA_BREACH_FLAG         CHAR(1)         NOT NULL DEFAULT 'N',
    FALLOUT_FLAG            CHAR(1)         NOT NULL DEFAULT 'N',
    IS_BUNDLE_ORDER         CHAR(1)         NOT NULL DEFAULT 'N',
    IS_5G_ORDER             CHAR(1)         NOT NULL DEFAULT 'N',
    IS_DIGITAL_CHANNEL      CHAR(1)         NOT NULL DEFAULT 'N',
    IS_NEW_CUSTOMER_ORDER   CHAR(1)         NOT NULL DEFAULT 'N',  -- First ever order
    IS_UPGRADE_ORDER        CHAR(1)         NOT NULL DEFAULT 'N',
    REVENUE_ASSURANCE_FLAG  CHAR(1)         NOT NULL DEFAULT 'N',  -- Y = billing discrepancy

    -- ── ETL Metadata ──────────────────────────────────────────
    ETL_INSERT_DT           TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    ETL_BATCH_ID            VARCHAR(50)     NOT NULL,
    SOURCE_SYSTEM_CD        VARCHAR(30)     NOT NULL,
    RECORD_VERSION          SMALLINT        NOT NULL DEFAULT 1
)
PRIMARY INDEX (ORDER_SK)
PARTITION BY RANGE_N(DATE_SK
    BETWEEN 20200101 AND 20301231 EACH 10000,                    -- ~monthly partitions by date int
    NO RANGE OR UNKNOWN)
COLLECT STATS ON DWH_TMF622.FACT_PRODUCT_ORDER COLUMN (DATE_SK)
COLLECT STATS ON DWH_TMF622.FACT_PRODUCT_ORDER COLUMN (CUSTOMER_SK)
COLLECT STATS ON DWH_TMF622.FACT_PRODUCT_ORDER COLUMN (ORDER_STATE_SK);

-- ══════════════════════════════════════════════════════════════
--  FACT_ORDER_ITEM — Order Item Grain Fact
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DWH_TMF622.FACT_ORDER_ITEM,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL
(
    ORDER_ITEM_SK           BIGINT          NOT NULL,
    ORDER_SK                BIGINT          NOT NULL,
    DATE_SK                 INTEGER         NOT NULL,
    CUSTOMER_SK             BIGINT          NOT NULL,
    PRODUCT_OFFERING_SK     BIGINT          NOT NULL,
    PRODUCT_SPEC_SK         BIGINT,
    ITEM_STATE_SK           SMALLINT        NOT NULL,
    ACTION_SK               SMALLINT        NOT NULL,
    GEOGRAPHY_SK            INTEGER         NOT NULL,
    CHANNEL_SK              SMALLINT        NOT NULL,

    -- Degenerate: Item natural key
    ORDER_ITEM_ID           VARCHAR(100)    NOT NULL,

    -- Financial Measures (SAR)
    UNIT_PRICE_EXCL_VAT     DECIMAL(14,4)   NOT NULL DEFAULT 0,
    UNIT_PRICE_INCL_VAT     DECIMAL(14,4)   NOT NULL DEFAULT 0,
    ITEM_QUANTITY           SMALLINT        NOT NULL DEFAULT 1,
    ITEM_TOTAL_AMOUNT       DECIMAL(18,2)   NOT NULL DEFAULT 0,  -- unit_incl_vat * qty
    ITEM_DISCOUNT_AMOUNT    DECIMAL(18,2)   NOT NULL DEFAULT 0,
    ITEM_VAT_AMOUNT         DECIMAL(14,2)   NOT NULL DEFAULT 0,
    ITEM_MRC                DECIMAL(14,2)   NOT NULL DEFAULT 0,  -- Monthly recurring
    ITEM_OTC                DECIMAL(14,2)   NOT NULL DEFAULT 0,  -- One-time

    -- Operational Measures
    PROVISIONING_DURATION_HRS DECIMAL(10,2),
    ITEM_SLA_TARGET_HRS     DECIMAL(10,2),
    ITEM_SLA_BREACH_FLAG    CHAR(1)         NOT NULL DEFAULT 'N',

    -- Product Characteristics (degenerate)
    PRODUCT_MSISDN_MASKED   VARCHAR(20),                         -- 05X-XXX-X789 format
    SIM_TYPE_CD             VARCHAR(10),                         -- PHYSICAL|ESIM
    CONTRACT_MONTHS         SMALLINT,
    TECHNOLOGY_TIER         VARCHAR(20),
    IS_5G_ITEM              CHAR(1)         NOT NULL DEFAULT 'N',
    IS_BUNDLE_ITEM          CHAR(1)         NOT NULL DEFAULT 'N',

    ETL_INSERT_DT           TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    ETL_BATCH_ID            VARCHAR(50)     NOT NULL
)
PRIMARY INDEX (ORDER_SK, ORDER_ITEM_ID)
PARTITION BY RANGE_N(DATE_SK
    BETWEEN 20200101 AND 20301231 EACH 10000,
    NO RANGE OR UNKNOWN)
INDEX (PRODUCT_OFFERING_SK, DATE_SK)
INDEX (CUSTOMER_SK);

-- ══════════════════════════════════════════════════════════════
--  FACT_ORDER_STATE_TRANSITION — State Change Event Fact
-- ══════════════════════════════════════════════════════════════
CREATE MULTISET TABLE DWH_TMF622.FACT_ORDER_STATE_TRANSITION,
     NO FALLBACK,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL
(
    TRANSITION_SK           BIGINT          NOT NULL,
    ORDER_SK                BIGINT          NOT NULL,
    ORDER_ITEM_SK           BIGINT,                              -- NULL = order-level transition
    FROM_STATE_SK           SMALLINT        NOT NULL,
    TO_STATE_SK             SMALLINT        NOT NULL,
    TRANSITION_DATE_SK      INTEGER         NOT NULL,
    TRANSITION_TIME_SK      INTEGER         NOT NULL,
    CHANGED_BY_PARTY_SK     BIGINT,

    -- Degenerate
    ORDER_BK                VARCHAR(100)    NOT NULL,
    CHANGE_REASON_CD        VARCHAR(100),
    CHANGE_REASON_DESC      VARCHAR(500),
    IS_AUTOMATED_CHANGE     CHAR(1)         DEFAULT 'N',
    EVENT_SOURCE_CD         VARCHAR(30),                         -- OMS|CRM|PROVISIONING|API

    -- Duration Measure
    DURATION_IN_FROM_STATE_HRS DECIMAL(14,4) NOT NULL DEFAULT 0, -- Time spent in FROM state
    IS_SLA_CRITICAL_STATE   CHAR(1)         NOT NULL DEFAULT 'N',-- Y if on CITC SLA path
    SLA_ELAPSED_PCT         DECIMAL(8,4),                        -- % of SLA consumed at transition

    ETL_INSERT_DT           TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    ETL_BATCH_ID            VARCHAR(50)     NOT NULL
)
PRIMARY INDEX (ORDER_SK)
PARTITION BY RANGE_N(TRANSITION_DATE_SK
    BETWEEN 20200101 AND 20301231 EACH 10000,
    NO RANGE OR UNKNOWN)
INDEX (FROM_STATE_SK, TO_STATE_SK)
INDEX (CHANGED_BY_PARTY_SK);

-- ══════════════════════════════════════════════════════════════
--  DEFAULT / UNKNOWN ROW INSERTS for all dimensions
--  (Referential integrity placeholder for unresolved FKs)
-- ══════════════════════════════════════════════════════════════
INSERT INTO DWH_TMF622.DIM_CUSTOMER VALUES (
    -1,'UNKNOWN','UNKNOWN',NULL,NULL,NULL,'UNKNOWN','UNKNOWN',
    'Unknown','Unknown',NULL,'AR',NULL,NULL,'N','N',
    DATE '2000-01-01',DATE '9999-12-31','Y',NULL,
    CURRENT_TIMESTAMP(6),NULL,'SYSTEM',NULL
);

INSERT INTO DWH_TMF622.DIM_PRODUCT_OFFERING VALUES (
    -1,'UNKNOWN','Unknown Product',NULL,NULL,NULL,
    'UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','N','N','N',0,NULL,NULL,0,0,'SAR',
    DATE '2000-01-01',DATE '9999-12-31','Y',NULL,CURRENT_TIMESTAMP(6),NULL
);

INSERT INTO DWH_TMF622.DIM_GEOGRAPHY VALUES (
    DEFAULT,'UNKNOWN','SAU','Saudi Arabia',
    'UNKNOWN','Unknown Region',N'منطقة غير معروفة',
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    'UNKNOWN','N','N',NULL,NULL,'N','UNKNOWN',CURRENT_TIMESTAMP(6),NULL
);

INSERT INTO DWH_TMF622.DIM_RELATED_PARTY VALUES (
    -1,'UNKNOWN',NULL,NULL,'Unknown',NULL,NULL,NULL,
    DATE '2000-01-01',DATE '9999-12-31','Y',CURRENT_TIMESTAMP(6)
);

/* ============================================================================
   END OF SCRIPT: 02_DWH_DDL.sql
   ============================================================================ */
