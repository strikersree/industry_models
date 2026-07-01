"""Shared SparkSession factory for all ARAMCO ETL jobs (curated / BDH / ADL).

Centralising this means every job -- ingestion, transform, or mart build --
gets identical Delta Lake configuration, catalog wiring, and warehouse
location, whether it's run via spark-submit from Airflow or locally in tests.
"""
from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from aramco_etl.common.config import Environment


def get_spark_session(app_name: str, enable_hive_support: bool = False) -> SparkSession:
    env = Environment.from_env()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", f"{env.datalake_root}/warehouse")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if enable_hive_support:
        builder = builder.enableHiveSupport()

    # No-op when spark-submit already put the Delta jars on the classpath via
    # --packages; resolves them via Maven when running the driver directly
    # (local dev, pytest).
    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_test_spark_session(app_name: str = "aramco-etl-tests") -> SparkSession:
    """Lightweight local SparkSession for unit tests -- no external catalog."""
    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
