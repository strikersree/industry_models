"""Slowly Changing Dimension Type 2 (SCD2) merge utility.

Mirrors the classic Kimball SCD2 pattern used across all BDH dimension
builders (dim_asset, dim_well_field, ...):

    1. Compute change_hash = sha2(concat_ws(tracked columns))
    2. MATCH incoming rows against the *current* (current_row_flag = 'Y') row
       for the same business key.
    3. If the hash differs           -> expire the old row, insert a new version.
    4. If the business key is brand new -> insert as a first version.
    5. If the hash is unchanged      -> no-op (row already reflects reality).

Surrogate keys are assigned by continuing the existing max surrogate key
sequence, so this is safe to call repeatedly / incrementally.
"""
from __future__ import annotations

from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

HIGH_DATE = "9999-12-31"


def with_change_hash(df: DataFrame, tracked_columns: List[str]) -> DataFrame:
    """Adds a `change_hash` column. NULL tracked columns are coalesced to ''
    before concatenation -- callers seeding a dimension for the first time
    MUST use this same helper rather than reimplementing the hash, or a
    later `apply_scd2` call will see every NULL-containing row as "changed"
    the moment a real SCD2 comparison runs (concat_ws silently drops NULL
    arguments instead of leaving a placeholder, so a hand-rolled version of
    this without the coalesce produces a different hash for the same row)."""
    return df.withColumn(
        "change_hash",
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_columns]), 256),
    )


def apply_scd2(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    surrogate_key_col: str,
    business_key_cols: List[str],
    tracked_columns: List[str],
    as_of_date: Optional[str] = None,
) -> None:
    """Merge `source_df` into `target_table` using SCD2 semantics.

    `as_of_date` (yyyy-MM-dd) sets the effective/expiry dates for this batch;
    defaults to the current date. Pass an explicit value when backfilling
    historical batches out of chronological order.
    """
    effective_date_expr = F.to_date(F.lit(as_of_date)) if as_of_date else F.current_date()
    source_hashed = with_change_hash(source_df, tracked_columns)

    target = DeltaTable.forName(spark, target_table)
    current_rows = target.toDF().filter(F.col("current_row_flag") == "Y")

    join_cond = [source_hashed[c] == current_rows[c] for c in business_key_cols]
    compared = source_hashed.alias("src").join(
        current_rows.alias("cur"), on=join_cond, how="left"
    )

    changed_or_new = compared.filter(
        F.col("cur.change_hash").isNull() | (F.col("src.change_hash") != F.col("cur.change_hash"))
    ).select("src.*")

    if changed_or_new.rdd.isEmpty():
        return

    # Step 1: expire current rows whose business key appears in changed_or_new.
    expire_keys = changed_or_new.select(*business_key_cols).distinct()
    merge_cond = " AND ".join(f"target.{c} = expire.{c}" for c in business_key_cols)
    (
        target.alias("target")
        .merge(expire_keys.alias("expire"), f"{merge_cond} AND target.current_row_flag = 'Y'")
        .whenMatchedUpdate(
            set={
                "current_row_flag": F.lit("N"),
                "row_exp_date": F.date_sub(effective_date_expr, 1),
            }
        )
        .execute()
    )

    # Step 2: assign new surrogate keys continuing the existing sequence.
    max_sk = target.toDF().agg(F.max(surrogate_key_col).alias("m")).collect()[0]["m"] or 0

    new_versions = (
        changed_or_new.withColumn("_rn", F.monotonically_increasing_id())
        .withColumn(surrogate_key_col, (F.col("_rn") + F.lit(max_sk) + 1).cast("bigint"))
        .drop("_rn")
        .withColumn("row_eff_date", effective_date_expr)
        .withColumn("row_exp_date", F.to_date(F.lit(HIGH_DATE)))
        .withColumn("current_row_flag", F.lit("Y"))
    )

    target_columns = target.toDF().columns
    new_versions = new_versions.select(*[c for c in target_columns if c in new_versions.columns])

    new_versions.write.format("delta").mode("append").saveAsTable(target_table)
