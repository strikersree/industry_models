-- ============================================================================
-- BDH (Silver) Layer -- Business Data Hub
-- Conformed dimensions, sourced across all curated schemas.
-- Type-2 (SCD2) dimensions carry: surrogate_key, row_eff_date, row_exp_date,
-- current_row_flag, change_hash. Built/merged by src/aramco_etl/bdh/build_dim_*.py
-- ============================================================================

CREATE DATABASE IF NOT EXISTS bdh
COMMENT 'BDH (Silver) -- conformed, DQ-gated dimensions and facts'
LOCATION '${DATALAKE_ROOT}/bdh';

USE bdh;

-- ---------------------------------------------------------------------------
-- DIM_DATE -- static calendar dimension
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key             INT         COMMENT 'yyyyMMdd surrogate key',
    calendar_date        DATE,
    day_of_week          STRING,
    day_of_month         INT,
    month_number         INT,
    month_name           STRING,
    quarter_number       INT,
    fiscal_year          INT,
    is_weekend           BOOLEAN,
    is_holiday_ksa       BOOLEAN     COMMENT 'Saudi public holiday flag'
)
USING DELTA
COMMENT 'Static calendar dimension, Gregorian with Saudi holiday flag';

-- ---------------------------------------------------------------------------
-- DIM_ASSET -- SCD2, conformed from curated_maximo_eam.raw_asset_master
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_asset (
    asset_sk             BIGINT      COMMENT 'Surrogate key',
    asset_id             STRING      COMMENT 'Business/natural key (Maximo ASSETNUM)',
    asset_description    STRING,
    asset_type           STRING,
    facility_id          STRING,
    location_code        STRING,
    manufacturer         STRING,
    install_date         DATE,
    criticality_rank     STRING,
    parent_asset_id      STRING,
    record_status        STRING,
    change_hash          STRING,
    row_eff_date         DATE,
    row_exp_date         DATE,
    current_row_flag     STRING
)
USING DELTA
COMMENT 'SCD2 conformed asset master dimension';

-- ---------------------------------------------------------------------------
-- DIM_WELL_FIELD -- SCD2, conformed from curated_scada_upstream
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_well_field (
    well_field_sk        BIGINT,
    well_id              STRING,
    field_id             STRING,
    business_domain      STRING      COMMENT 'upstream / midstream / downstream',
    change_hash          STRING,
    row_eff_date         DATE,
    row_exp_date         DATE,
    current_row_flag     STRING
)
USING DELTA
COMMENT 'SCD2 conformed well/field reference dimension';

-- ---------------------------------------------------------------------------
-- DIM_EMPLOYEE_VENDOR -- Type-1, conformed workforce/vendor reference
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_employee_vendor (
    party_sk             BIGINT,
    party_id             STRING      COMMENT 'employee_id or vendor_code',
    party_type           STRING      COMMENT 'EMPLOYEE / VENDOR / CONTRACTOR',
    party_name           STRING,
    department_code      STRING,
    last_updated_ts      TIMESTAMP
)
USING DELTA
COMMENT 'Type-1 conformed employee/vendor/contractor party dimension';

-- ---------------------------------------------------------------------------
-- DIM_COST_CENTER -- Type-1, conformed from curated_sap_erp
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_cost_center (
    cost_center_sk       BIGINT,
    cost_center_code     STRING,
    cost_center_name     STRING,
    business_domain      STRING,
    last_updated_ts      TIMESTAMP
)
USING DELTA
COMMENT 'Type-1 conformed SAP cost-center dimension';

-- ---------------------------------------------------------------------------
-- DIM_PRODUCT -- Type-1, conformed refinery product reference
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk           BIGINT,
    product_code         STRING,
    product_name         STRING,
    product_family       STRING      COMMENT 'e.g. LIGHT_DISTILLATE, MIDDLE_DISTILLATE',
    last_updated_ts      TIMESTAMP
)
USING DELTA
COMMENT 'Type-1 conformed refined-product reference dimension';
