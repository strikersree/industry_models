"""Shared upsert helper for Type-1 (overwrite-in-place) BDH dimensions.

Used by dim_cost_center, dim_product, and dim_employee_vendor: no history is
kept, existing rows are updated in place, and new business keys get a
surrogate key continuing the existing sequence.
"""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from aramco_etl.common.delta_io import merge_upsert


def upsert_type1_dimension(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    surrogate_key_col: str,
    business_key_cols: List[str],
    update_columns: List[str],
) -> None:
    if not spark.catalog.tableExists(target_table) or spark.table(target_table).isEmpty():
        seeded = source_df.withColumn(
            surrogate_key_col, F.row_number().over(Window.orderBy(*business_key_cols)).cast("bigint")
        )
        seeded.write.format("delta").mode("append").saveAsTable(target_table)
        return

    existing = spark.table(target_table)
    max_sk = existing.agg(F.max(surrogate_key_col)).collect()[0][0] or 0

    join_cond = [source_df[c] == existing[c] for c in business_key_cols]
    new_keys_df = (
        source_df.alias("src")
        .join(existing.alias("tgt"), on=join_cond, how="left_anti")
    )
    if not new_keys_df.rdd.isEmpty():
        new_rows = new_keys_df.withColumn(
            surrogate_key_col,
            (F.row_number().over(Window.orderBy(*business_key_cols)) + F.lit(max_sk)).cast("bigint"),
        )
        new_rows.write.format("delta").mode("append").saveAsTable(target_table)

    # New business keys were already appended above (with a surrogate key
    # source_df doesn't carry), so this merge only updates existing rows.
    merge_upsert(
        spark=spark,
        target_table=target_table,
        updates_df=source_df,
        merge_keys=business_key_cols,
        update_columns=update_columns,
        insert_new_rows=False,
    )
