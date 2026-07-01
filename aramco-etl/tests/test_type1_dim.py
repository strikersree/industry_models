from aramco_etl.common.type1_dim import upsert_type1_dimension


def test_seeds_empty_target_with_sequential_surrogate_keys(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.sql("DROP TABLE IF EXISTS test_db.dim_test_cc")
    spark.sql(
        "CREATE TABLE test_db.dim_test_cc "
        "(cost_center_sk BIGINT, cost_center_code STRING, cost_center_name STRING) USING DELTA"
    )

    source = spark.createDataFrame(
        [("CC1", "Upstream Ops"), ("CC2", "Downstream Ops")],
        schema="cost_center_code string, cost_center_name string",
    )

    upsert_type1_dimension(
        spark=spark,
        source_df=source,
        target_table="test_db.dim_test_cc",
        surrogate_key_col="cost_center_sk",
        business_key_cols=["cost_center_code"],
        update_columns=["cost_center_name"],
    )

    result = spark.table("test_db.dim_test_cc").orderBy("cost_center_code").collect()
    assert [r["cost_center_sk"] for r in result] == [1, 2]


def test_updates_existing_and_appends_new_business_keys(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.sql("DROP TABLE IF EXISTS test_db.dim_test_cc2")
    spark.sql(
        "CREATE TABLE test_db.dim_test_cc2 "
        "(cost_center_sk BIGINT, cost_center_code STRING, cost_center_name STRING) USING DELTA"
    )
    spark.sql("INSERT INTO test_db.dim_test_cc2 VALUES (1, 'CC1', 'Old Name')")

    source = spark.createDataFrame(
        [("CC1", "Renamed"), ("CC2", "Brand New")],
        schema="cost_center_code string, cost_center_name string",
    )

    upsert_type1_dimension(
        spark=spark,
        source_df=source,
        target_table="test_db.dim_test_cc2",
        surrogate_key_col="cost_center_sk",
        business_key_cols=["cost_center_code"],
        update_columns=["cost_center_name"],
    )

    result = {r["cost_center_code"]: r for r in spark.table("test_db.dim_test_cc2").collect()}
    assert result["CC1"]["cost_center_name"] == "Renamed"
    assert result["CC1"]["cost_center_sk"] == 1
    assert result["CC2"]["cost_center_name"] == "Brand New"
    assert result["CC2"]["cost_center_sk"] == 2
