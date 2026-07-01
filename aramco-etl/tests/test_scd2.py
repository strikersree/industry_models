from aramco_etl.common.scd2 import apply_scd2


def _seed_target(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.sql("DROP TABLE IF EXISTS test_db.dim_test_asset")
    spark.sql(
        """
        CREATE TABLE test_db.dim_test_asset (
            asset_sk BIGINT,
            asset_id STRING,
            status STRING,
            change_hash STRING,
            row_eff_date DATE,
            row_exp_date DATE,
            current_row_flag STRING
        ) USING DELTA
        """
    )
    spark.sql(
        """
        INSERT INTO test_db.dim_test_asset VALUES
        (1, 'A1', 'ACTIVE', sha2('ACTIVE', 256), DATE '2026-01-01', DATE '9999-12-31', 'Y')
        """
    )


def test_scd2_expires_changed_row_and_inserts_new_business_key(spark):
    _seed_target(spark)

    source = spark.createDataFrame(
        [("A1", "INACTIVE"), ("A2", "ACTIVE")], schema="asset_id string, status string"
    )

    apply_scd2(
        spark=spark,
        source_df=source,
        target_table="test_db.dim_test_asset",
        surrogate_key_col="asset_sk",
        business_key_cols=["asset_id"],
        tracked_columns=["status"],
        as_of_date="2026-01-02",
    )

    result = spark.table("test_db.dim_test_asset").orderBy("asset_id", "row_eff_date").collect()
    a1_rows = [r for r in result if r["asset_id"] == "A1"]
    a2_rows = [r for r in result if r["asset_id"] == "A2"]

    assert len(a1_rows) == 2, "changed business key should have two versions"
    expired = [r for r in a1_rows if r["current_row_flag"] == "N"][0]
    current = [r for r in a1_rows if r["current_row_flag"] == "Y"][0]
    assert str(expired["row_exp_date"]) == "2026-01-01"
    assert current["status"] == "INACTIVE"
    assert str(current["row_eff_date"]) == "2026-01-02"

    assert len(a2_rows) == 1, "new business key should be inserted as first version"
    assert a2_rows[0]["current_row_flag"] == "Y"


def test_scd2_is_a_no_op_when_nothing_changed(spark):
    _seed_target(spark)

    unchanged_source = spark.createDataFrame([("A1", "ACTIVE")], schema="asset_id string, status string")

    apply_scd2(
        spark=spark,
        source_df=unchanged_source,
        target_table="test_db.dim_test_asset",
        surrogate_key_col="asset_sk",
        business_key_cols=["asset_id"],
        tracked_columns=["status"],
        as_of_date="2026-01-02",
    )

    result = spark.table("test_db.dim_test_asset").collect()
    assert len(result) == 1
    assert result[0]["current_row_flag"] == "Y"
