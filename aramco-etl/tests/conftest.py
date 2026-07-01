"""Pytest fixtures shared by all ARAMCO ETL unit tests.

Uses a local, non-Hive Spark session with Delta Lake enabled and a
per-test-session temp warehouse directory, so CREATE DATABASE / CREATE
TABLE ... USING DELTA work without any external metastore.
"""
from __future__ import annotations

import shutil
import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    warehouse_dir = tempfile.mkdtemp(prefix="aramco_etl_test_warehouse_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("aramco-etl-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.shuffle.partitions", "2")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(warehouse_dir, ignore_errors=True)
