-- ============================================================================
-- CURATED (Bronze) Layer -- Source: SAP ERP (Finance / Procurement)
-- Schema: curated_sap_erp
-- ============================================================================

CREATE DATABASE IF NOT EXISTS curated_sap_erp
COMMENT 'Curated (Bronze) landing zone for SAP ERP finance & procurement extracts'
LOCATION '${DATALAKE_ROOT}/curated/sap_erp';

USE curated_sap_erp;

CREATE TABLE IF NOT EXISTS raw_gl_journal (
    journal_id             STRING,
    posting_date           DATE,
    fiscal_year            INT,
    fiscal_period          INT,
    gl_account_code        STRING,
    cost_center_code       STRING,
    document_type          STRING     COMMENT 'e.g. SA (GL doc), KR (vendor invoice), RE (invoice receipt)',
    amount_local_ccy       DECIMAL(18,2),
    local_currency         STRING     COMMENT 'ISO currency code, e.g. SAR',
    amount_usd             DECIMAL(18,2),
    debit_credit_indicator STRING     COMMENT 'S = debit, H = credit',
    reference_document     STRING,
    _ingest_ts             TIMESTAMP,
    _source_file           STRING,
    _business_date         DATE,
    _ingestion_run_id      STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw General Ledger journal line extract';

CREATE TABLE IF NOT EXISTS raw_purchase_order (
    po_number            STRING,
    po_line_number       INT,
    vendor_code          STRING,
    material_code        STRING,
    cost_center_code     STRING,
    order_qty            DECIMAL(18,3),
    unit_of_measure      STRING,
    net_price_usd        DECIMAL(18,2),
    po_created_date      DATE,
    po_status            STRING     COMMENT 'OPEN, PARTIAL, COMPLETE, CANCELLED',
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw purchase order header + line extract';

CREATE TABLE IF NOT EXISTS raw_cost_center_actuals (
    cost_center_code     STRING,
    fiscal_year          INT,
    fiscal_period        INT,
    cost_element_code    STRING,
    actual_amount_usd    DECIMAL(18,2),
    budget_amount_usd    DECIMAL(18,2),
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw cost-center actual vs. budget extract, monthly grain';
