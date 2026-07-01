"""Delta Lake read/write helpers shared across curated, BDH, and ADL jobs."""
from __future__ import annotations

from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def table_exists(spark: SparkSession, table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def write_append(df: DataFrame, table_name: str, partition_by: Optional[List[str]] = None) -> None:
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_name)


def write_overwrite_partition(df: DataFrame, table_name: str, partition_by: Optional[List[str]] = None) -> None:
    """Dynamic-partition overwrite: replaces only the partitions present in df,
    leaving other partitions untouched.

    Deliberately uses `insertInto`, not `saveAsTable(mode="overwrite")`:
    `saveAsTable` overwrite against an *existing* Delta table resolves to a
    full `REPLACE TABLE AS SELECT`, which drops every other partition's data
    regardless of `partitionOverwriteMode`. `insertInto` is the only writer
    path that actually honours dynamic partition overwrite here.

    Column order in `df` must match the target table's column order --
    `insertInto` resolves columns positionally, not by name.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .insertInto(table_name)
    )


def merge_upsert(
    spark: SparkSession,
    target_table: str,
    updates_df: DataFrame,
    merge_keys: List[str],
    update_columns: Optional[List[str]] = None,
    insert_new_rows: bool = True,
) -> None:
    """Type-1 style upsert: match on merge_keys, update in place, insert if new.

    Set `insert_new_rows=False` when `updates_df` doesn't carry every target
    column (e.g. it has no surrogate key yet) -- `whenNotMatchedInsertAll`
    requires the source to resolve every target column at analysis time, so
    callers that assign surrogate keys separately before merging (see
    common.type1_dim) must disable it here to avoid an unresolved-column error.
    """
    target = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join(f"target.{k} = updates.{k}" for k in merge_keys)

    merge_builder = target.alias("target").merge(
        updates_df.alias("updates"), merge_condition
    )

    if update_columns:
        set_map = {c: f"updates.{c}" for c in update_columns}
        merge_builder = merge_builder.whenMatchedUpdate(set=set_map)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    if insert_new_rows:
        merge_builder = merge_builder.whenNotMatchedInsertAll()

    merge_builder.execute()
