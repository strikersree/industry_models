"""Factory for SparkSubmitOperator tasks shared across all three ARAMCO DAGs.

Centralises the Spark connection id, Delta Lake packages, and the
properties file so every curated/BDH/ADL job launches with identical
cluster configuration.
"""
from __future__ import annotations

from typing import List

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"
SRC_ROOT = "/opt/airflow/aramco-etl/src"
PROPERTIES_FILE = "/opt/airflow/aramco-etl/config/spark-defaults.conf"
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"


def build_spark_task(
    task_id: str,
    application: str,
    application_args: List[str],
    **kwargs,
) -> SparkSubmitOperator:
    """`application` is a path relative to SRC_ROOT, e.g.
    'aramco_etl/curated/ingest_scada_upstream.py'."""
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{SRC_ROOT}/{application}",
        application_args=application_args,
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
        properties_file=PROPERTIES_FILE,
        py_files=SRC_ROOT,
        env_vars={
            "DATALAKE_ROOT": "{{ var.value.aramco_datalake_root }}",
            "LANDING_ROOT": "{{ var.value.aramco_landing_root }}",
            "ARAMCO_ENV": "{{ var.value.get('aramco_env', 'DEV') }}",
        },
        verbose=False,
        **kwargs,
    )
