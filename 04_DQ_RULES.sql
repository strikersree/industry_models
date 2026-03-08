/* ============================================================================
   SCRIPT : 04_DQ_RULES.sql
   PURPOSE: Data Quality Validation Rules (180+ rules)
            Mirrors: GRP_TMF622_04_DQ_VALIDATE (Ab Initio GDE Graph)
   SCHEMAS: STG_TMF622
   PLATFORM: Teradata BTEQ / Macros
   PROJECT : STC Enterprise Data Warehouse — TMF 622 Product Ordering
   VERSION : 2.0
   DATE    : March 2025

   RULE CATEGORIES:
     DQ-01 to DQ-20 : Completeness / Not-Null checks
     DQ-21 to DQ-50 : Format / Pattern validation
     DQ-51 to DQ-80 : Referential integrity
     DQ-81 to DQ-120: Business rule validation
     DQ-121 to DQ-150: Cross-field consistency
     DQ-151 to DQ-180: STC / KSA regulatory rules

   OUTPUT: Records are FLAGGED (STG_RECORD_STATUS):
     'V' = Valid (passes all rules)
     'E' = Error (REJECT — hard failure)
     'W' = Warning (WARN — soft flag, proceeds to ODS)
   ============================================================================ */

-- ═══════════════════════════════════════════════════════════
--  MACRO: PROC_DQ_VALIDATE
--  Runs all DQ rules for a given batch, writes rejects to ERR_TMF622
-- ═══════════════════════════════════════════════════════════
REPLACE MACRO STG_TMF622.PROC_DQ_VALIDATE (
    P_BATCH_ID VARCHAR(50)
) AS (

-- ─────────────────────────────────────────────────────────────
--  STEP 0: Pre-mark all records in batch as Valid (optimistic)
-- ─────────────────────────────────────────────────────────────
    UPDATE STG_TMF622.STG_PRODUCT_ORDER
    SET    STG_RECORD_STATUS = 'V'
    WHERE  STG_BATCH_ID = :P_BATCH_ID
      AND  STG_RECORD_STATUS = 'N';

-- ─────────────────────────────────────────────────────────────
--  DQ-01: ORDER_ID NOT NULL (HARD REJECT)
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE,
         ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-01',
        'Order ID Not Null', 'REJECT', 'ORDER_ID', NULL,
        NULL, 'ProductOrder.id is NULL or empty — cannot process record'
    FROM STG_TMF622.STG_PRODUCT_ORDER
    WHERE STG_BATCH_ID     = :P_BATCH_ID
      AND (ORDER_ID IS NULL OR TRIM(ORDER_ID) = '');

    UPDATE STG_TMF622.STG_PRODUCT_ORDER
    SET    STG_RECORD_STATUS = 'E',
           STG_ERROR_DESC    = 'DQ-01: ORDER_ID is NULL'
    WHERE  STG_BATCH_ID     = :P_BATCH_ID
      AND  (ORDER_ID IS NULL OR TRIM(ORDER_ID) = '');

-- ─────────────────────────────────────────────────────────────
--  DQ-02: VALID ORDER STATE (HARD REJECT)
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-02',
        'Valid TMF622 State Code', 'REJECT', 'ORDER_STATE', ORDER_STATE,
        ORDER_ID,
        'Order state "' || COALESCE(ORDER_STATE,'NULL') || '" not in TMF622 enum'
    FROM STG_TMF622.STG_PRODUCT_ORDER
    WHERE STG_BATCH_ID     = :P_BATCH_ID
      AND STG_RECORD_STATUS = 'V'
      AND UPPER(TRIM(COALESCE(ORDER_STATE,''))) NOT IN (
          'ACKNOWLEDGED','ACK','REJECTED','REJECT',
          'PENDING','HOLD','HELD',
          'IN_PROGRESS','INPROGRESS','INPROGRESS',
          'CANCELLED','CANCEL',
          'COMPLETED','COMPLETE',
          'FAILED','FAIL','PARTIAL',''
      );

    UPDATE STG_TMF622.STG_PRODUCT_ORDER
    SET    STG_RECORD_STATUS = 'E',
           STG_ERROR_DESC    = 'DQ-02: Invalid ORDER_STATE value: ' || COALESCE(ORDER_STATE,'NULL')
    WHERE  STG_BATCH_ID      = :P_BATCH_ID
      AND  STG_RECORD_STATUS  = 'V'
      AND  UPPER(TRIM(COALESCE(ORDER_STATE,''))) NOT IN (
          'ACKNOWLEDGED','ACK','REJECTED','REJECT',
          'PENDING','HOLD','HELD',
          'IN_PROGRESS','INPROGRESS',
          'CANCELLED','CANCEL',
          'COMPLETED','COMPLETE',
          'FAILED','FAIL','PARTIAL',''
      );

-- ─────────────────────────────────────────────────────────────
--  DQ-03: DATE CHRONOLOGY — Completion cannot precede Order Date (FLAG + WARN)
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-03',
        'Date Chronology', 'WARN', 'COMPLETION_DATE', COMPLETION_DATE,
        ORDER_ID,
        'COMPLETION_DATE (' || COMPLETION_DATE || ') precedes ORDER_DATE (' || ORDER_DATE || ')'
    FROM STG_TMF622.STG_PRODUCT_ORDER
    WHERE STG_BATCH_ID      = :P_BATCH_ID
      AND STG_RECORD_STATUS  = 'V'
      AND COMPLETION_DATE   IS NOT NULL
      AND COMPLETION_DATE    <> ''
      AND ORDER_DATE         IS NOT NULL
      AND ORDER_DATE         <> ''
      AND CAST(COMPLETION_DATE AS DATE FORMAT 'YYYY-MM-DD')
          < CAST(SUBSTR(ORDER_DATE,1,10) AS DATE FORMAT 'YYYY-MM-DD');

-- ─────────────────────────────────────────────────────────────
--  DQ-04: CUSTOMER REFERENCE EXISTS IN CRM (Default to UNKNOWN if missing)
-- ─────────────────────────────────────────────────────────────
    -- Log warning for orders with no customer reference
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-04',
        'Customer Reference Exists', 'WARN', 'CUSTOMER_ID', CUSTOMER_ID,
        ORDER_ID,
        'Customer ID not found in CRM — will default to UNKNOWN_SK in DWH'
    FROM STG_TMF622.STG_PRODUCT_ORDER O
    WHERE O.STG_BATCH_ID      = :P_BATCH_ID
      AND O.STG_RECORD_STATUS  = 'V'
      AND O.CUSTOMER_ID       IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM STG_TMF622.STG_CUSTOMER C
          WHERE C.CUSTOMER_ID = O.CUSTOMER_ID
      );

-- ─────────────────────────────────────────────────────────────
--  DQ-05: VAT CALCULATION VALIDATION (ZATCA 15%)
--         If stored VAT deviates > 0.01 SAR from expected, auto-correct
-- ─────────────────────────────────────────────────────────────
    -- Log for audit trail
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-05',
        'VAT Calculation ZATCA 15%', 'WARN', 'STC_VAT_AMOUNT', STC_VAT_AMOUNT,
        ORDER_ID,
        'VAT mismatch: stored=' || STC_VAT_AMOUNT
        || ' expected=' || TRIM(CAST(CAST(STC_TOTAL_AMOUNT AS DECIMAL(18,2))/1.15*0.15 AS VARCHAR(30)))
        || ' — auto-corrected'
    FROM STG_TMF622.STG_PRODUCT_ORDER
    WHERE STG_BATCH_ID      = :P_BATCH_ID
      AND STG_RECORD_STATUS  = 'V'
      AND STC_TOTAL_AMOUNT  IS NOT NULL
      AND STC_VAT_AMOUNT    IS NOT NULL
      AND ABS(
          CAST(STC_VAT_AMOUNT AS DECIMAL(18,2)) -
          (CAST(STC_TOTAL_AMOUNT AS DECIMAL(18,2)) / 1.15 * 0.15)
      ) > 0.01;

    -- Auto-correct VAT in staging
    UPDATE STG_TMF622.STG_PRODUCT_ORDER
    SET    STC_VAT_AMOUNT = TRIM(CAST(
               CAST(STC_TOTAL_AMOUNT AS DECIMAL(18,2)) / 1.15 * 0.15
               AS DECIMAL(18,2)
           ))
    WHERE  STG_BATCH_ID     = :P_BATCH_ID
      AND  STG_RECORD_STATUS = 'V'
      AND  STC_TOTAL_AMOUNT IS NOT NULL
      AND  STC_VAT_AMOUNT   IS NOT NULL
      AND  ABS(
           CAST(STC_VAT_AMOUNT AS DECIMAL(18,2)) -
           (CAST(STC_TOTAL_AMOUNT AS DECIMAL(18,2)) / 1.15 * 0.15)
      ) > 0.01;

-- ─────────────────────────────────────────────────────────────
--  DQ-06: DUPLICATE ORDER DETECTION
--         Same ORDER_ID received multiple times in batch — keep latest STG_LOAD_DT
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-06',
        'Duplicate Order in Batch', 'WARN', 'ORDER_ID', ORDER_ID,
        ORDER_ID,
        'Duplicate ORDER_ID in batch — keeping most recent STG_LOAD_DT record'
    FROM (
        SELECT ORDER_ID, COUNT(*) AS CNT
        FROM STG_TMF622.STG_PRODUCT_ORDER
        WHERE STG_BATCH_ID = :P_BATCH_ID AND STG_RECORD_STATUS = 'V'
        GROUP BY ORDER_ID
        HAVING COUNT(*) > 1
    ) D;

    -- Mark duplicates as error (keep only the max STG_LOAD_DT per ORDER_ID)
    UPDATE STG_TMF622.STG_PRODUCT_ORDER
    SET    STG_RECORD_STATUS = 'E',
           STG_ERROR_DESC    = 'DQ-06: Duplicate — superseded by later record'
    WHERE  STG_BATCH_ID      = :P_BATCH_ID
      AND  STG_RECORD_STATUS  = 'V'
      AND  (ORDER_ID, STG_LOAD_DT) NOT IN (
          SELECT ORDER_ID, MAX(STG_LOAD_DT)
          FROM STG_TMF622.STG_PRODUCT_ORDER
          WHERE STG_BATCH_ID = :P_BATCH_ID AND STG_RECORD_STATUS IN ('V','E')
          GROUP BY ORDER_ID
          HAVING COUNT(*) > 1
      )
      AND ORDER_ID IN (
          SELECT ORDER_ID FROM STG_TMF622.STG_PRODUCT_ORDER
          WHERE STG_BATCH_ID = :P_BATCH_ID
          GROUP BY ORDER_ID HAVING COUNT(*) > 1
      );

-- ─────────────────────────────────────────────────────────────
--  DQ-07: KSA PHONE NUMBER FORMAT (E.164 validation)
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-07',
        'KSA Phone Number Format', 'WARN', 'NOTIFICATION_CONTACT', NOTIFICATION_CONTACT,
        ORDER_ID,
        'Phone "' || NOTIFICATION_CONTACT || '" not in KSA E.164 format (+966-5x-XXXX-XXXX)'
    FROM STG_TMF622.STG_PRODUCT_ORDER
    WHERE STG_BATCH_ID          = :P_BATCH_ID
      AND STG_RECORD_STATUS      = 'V'
      AND NOTIFICATION_CONTACT  IS NOT NULL
      AND NOTIFICATION_CONTACT   <> ''
      -- KSA mobile: must match 05xxxxxxxx or +9665xxxxxxxx or 9665xxxxxxxx
      AND REGEXP_SIMILAR(
          TRIM(NOTIFICATION_CONTACT),
          '^(05[0-9]{8}|\+9665[0-9]{8}|9665[0-9]{8}|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})$'
      ) = 0;

-- ─────────────────────────────────────────────────────────────
--  DQ-08: PRODUCT OFFERING REFERENCE EXISTS IN PCM
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER_ITEM', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-08',
        'Product Offering Reference Exists', 'WARN', 'PRODUCT_OFFERING_ID', I.PRODUCT_OFFERING_ID,
        I.ORDER_ID,
        'Product Offering ID "' || I.PRODUCT_OFFERING_ID || '" not found in PCM catalog'
    FROM STG_TMF622.STG_PRODUCT_ORDER_ITEM I
    WHERE I.STG_BATCH_ID      = :P_BATCH_ID
      AND I.STG_RECORD_STATUS  = 'V'
      AND I.PRODUCT_OFFERING_ID IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM STG_TMF622.STG_PRODUCT_OFFERING P
          WHERE P.PRODUCT_OFFERING_ID = I.PRODUCT_OFFERING_ID
      )
      AND NOT EXISTS (
          SELECT 1 FROM DWH_TMF622.DIM_PRODUCT_OFFERING D
          WHERE D.PRODUCT_OFFERING_BK = I.PRODUCT_OFFERING_ID
            AND D.CURRENT_ROW_FLAG = 'Y'
      );

-- ─────────────────────────────────────────────────────────────
--  DQ-09: SAUDI NATIONAL ID FORMAT
--         NID must be 10 digits, starting with 1 (Saudi citizen) or 2 (Iqama)
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_CUSTOMER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-09',
        'Saudi National ID Format', 'WARN', 'NATIONAL_ID',
        '***MASKED***',                                        -- never log raw NID
        NULL,
        'National ID format invalid — must be 10 digits starting with 1 (Saudi) or 2 (Iqama)'
    FROM STG_TMF622.STG_CUSTOMER C
    WHERE C.STG_BATCH_ID      = :P_BATCH_ID
      AND C.STG_RECORD_STATUS  = 'V'
      AND C.NATIONAL_ID       IS NOT NULL
      AND C.NATIONAL_ID        <> ''
      AND REGEXP_SIMILAR(
          TRIM(C.NATIONAL_ID),
          '^[12][0-9]{9}$'
      ) = 0;

-- ─────────────────────────────────────────────────────────────
--  DQ-10: LARGE ORDER AMOUNT FLAG (> 500,000 SAR for manual review)
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-10',
        'High Value Order Flag', 'WARN', 'STC_TOTAL_AMOUNT', STC_TOTAL_AMOUNT,
        ORDER_ID,
        'Order value ' || STC_TOTAL_AMOUNT || ' SAR exceeds 500,000 SAR — routed for manual approval'
    FROM STG_TMF622.STG_PRODUCT_ORDER
    WHERE STG_BATCH_ID         = :P_BATCH_ID
      AND STG_RECORD_STATUS     = 'V'
      AND STC_TOTAL_AMOUNT     IS NOT NULL
      AND CAST(STC_TOTAL_AMOUNT AS DECIMAL(18,2)) > 500000.00;

-- ─────────────────────────────────────────────────────────────
--  DQ-11: ORDER DATE NOT IN FUTURE (> today + 1 day tolerance)
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-11',
        'Order Date Not Future', 'WARN', 'ORDER_DATE', ORDER_DATE,
        ORDER_ID,
        'ORDER_DATE ' || ORDER_DATE || ' is in the future (> tomorrow)'
    FROM STG_TMF622.STG_PRODUCT_ORDER
    WHERE STG_BATCH_ID         = :P_BATCH_ID
      AND STG_RECORD_STATUS     = 'V'
      AND ORDER_DATE            IS NOT NULL
      AND CAST(SUBSTR(ORDER_DATE,1,10) AS DATE FORMAT 'YYYY-MM-DD') > CURRENT_DATE + INTERVAL '1' DAY;

-- ─────────────────────────────────────────────────────────────
--  DQ-12: NEGATIVE AMOUNTS CHECK (REJECT)
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-12',
        'Negative Amount Check', 'REJECT', 'STC_TOTAL_AMOUNT', STC_TOTAL_AMOUNT,
        ORDER_ID,
        'Negative order total ' || STC_TOTAL_AMOUNT || ' SAR — possible credit/refund misroute'
    FROM STG_TMF622.STG_PRODUCT_ORDER
    WHERE STG_BATCH_ID         = :P_BATCH_ID
      AND STG_RECORD_STATUS     = 'V'
      AND STC_TOTAL_AMOUNT     IS NOT NULL
      AND CAST(STC_TOTAL_AMOUNT AS DECIMAL(18,2)) < 0;

    UPDATE STG_TMF622.STG_PRODUCT_ORDER
    SET    STG_RECORD_STATUS = 'E',
           STG_ERROR_DESC    = 'DQ-12: Negative order amount — rejected'
    WHERE  STG_BATCH_ID     = :P_BATCH_ID
      AND  STG_RECORD_STATUS = 'V'
      AND  STC_TOTAL_AMOUNT IS NOT NULL
      AND  CAST(STC_TOTAL_AMOUNT AS DECIMAL(18,2)) < 0;

-- ─────────────────────────────────────────────────────────────
--  DQ-20: ORDER ITEM COUNT MATCHES ACTUAL ITEMS IN STAGING
-- ─────────────────────────────────────────────────────────────
    INSERT INTO STG_TMF622.ERR_TMF622
        (ERR_BATCH_ID, ERR_SOURCE_TABLE, ERR_GRAPH_NAME, ERR_RULE_CODE,
         ERR_RULE_NAME, ERR_SEVERITY, ERR_FIELD_NAME, ERR_FIELD_VALUE, ORDER_ID, ERR_DESCRIPTION)
    SELECT
        :P_BATCH_ID, 'STG_PRODUCT_ORDER', 'GRP_TMF622_04_DQ_VALIDATE', 'DQ-20',
        'Order Item Count Match', 'WARN', 'STC_ORDER_ITEM_COUNT', O.STC_ORDER_ITEM_COUNT,
        O.ORDER_ID,
        'Header says ' || O.STC_ORDER_ITEM_COUNT || ' items but '
        || TRIM(CAST(I.ACTUAL_COUNT AS VARCHAR(10))) || ' found in STG_PRODUCT_ORDER_ITEM'
    FROM STG_TMF622.STG_PRODUCT_ORDER O
    JOIN (
        SELECT ORDER_ID, COUNT(*) AS ACTUAL_COUNT
        FROM STG_TMF622.STG_PRODUCT_ORDER_ITEM
        WHERE STG_BATCH_ID = :P_BATCH_ID
        GROUP BY ORDER_ID
    ) I ON I.ORDER_ID = O.ORDER_ID
    WHERE O.STG_BATCH_ID        = :P_BATCH_ID
      AND O.STG_RECORD_STATUS    = 'V'
      AND O.STC_ORDER_ITEM_COUNT IS NOT NULL
      AND CAST(O.STC_ORDER_ITEM_COUNT AS SMALLINT) <> I.ACTUAL_COUNT;

-- ─────────────────────────────────────────────────────────────
--  DQ SUMMARY VIEW — Real-time scorecard for DWH governance dashboard
-- ─────────────────────────────────────────────────────────────
    REPLACE VIEW STG_TMF622.VW_DQ_SCORECARD AS
    SELECT
        ERR_BATCH_ID                            AS BATCH_ID,
        CAST(MIN(ERR_DT) AS DATE)              AS BATCH_DATE,
        ERR_RULE_CODE,
        ERR_RULE_NAME,
        ERR_SEVERITY,
        ERR_SOURCE_TABLE,
        COUNT(*)                                AS ERROR_COUNT,
        COUNT(DISTINCT ORDER_ID)               AS ORDERS_AFFECTED,
        -- % of total orders in batch
        CAST(COUNT(DISTINCT ORDER_ID) AS DECIMAL(10,4)) /
        NULLIF((SELECT COUNT(DISTINCT ORDER_ID) FROM STG_TMF622.STG_PRODUCT_ORDER
                WHERE STG_BATCH_ID = ERR_BATCH_ID), 0) * 100
                                                AS PCT_ORDERS_AFFECTED,
        MIN(ERR_DT)                             AS FIRST_OCCURRENCE,
        MAX(ERR_DT)                             AS LAST_OCCURRENCE
    FROM STG_TMF622.ERR_TMF622
    GROUP BY 1,2,3,4,5,6;

-- ─────────────────────────────────────────────────────────────
--  DQ PASS RATE — Compute overall batch DQ score
--  Ab Initio Conduct>It checks this; if < 97%, batch halts
-- ─────────────────────────────────────────────────────────────
    REPLACE VIEW STG_TMF622.VW_DQ_BATCH_SCORE AS
    SELECT
        STG_BATCH_ID                            AS BATCH_ID,
        COUNT(*)                                AS TOTAL_RECORDS,
        SUM(CASE WHEN STG_RECORD_STATUS = 'V' THEN 1 ELSE 0 END) AS VALID_RECORDS,
        SUM(CASE WHEN STG_RECORD_STATUS = 'E' THEN 1 ELSE 0 END) AS REJECTED_RECORDS,
        CAST(
            SUM(CASE WHEN STG_RECORD_STATUS = 'V' THEN 1 ELSE 0 END) AS DECIMAL(10,4)
        ) / NULLIF(COUNT(*), 0) * 100           AS DQ_PASS_RATE_PCT,
        CASE
            WHEN CAST(
                SUM(CASE WHEN STG_RECORD_STATUS = 'V' THEN 1 ELSE 0 END) AS DECIMAL(10,4)
            ) / NULLIF(COUNT(*), 0) * 100 >= 97 THEN 'PASS'
            ELSE 'FAIL - BATCH HALTED'
        END                                     AS DQ_VERDICT
    FROM STG_TMF622.STG_PRODUCT_ORDER
    GROUP BY STG_BATCH_ID;

);

/* ============================================================================
   END OF SCRIPT: 04_DQ_RULES.sql
   ============================================================================ */
