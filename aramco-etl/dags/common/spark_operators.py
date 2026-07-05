"""Factory for SparkSubmitOperator tasks shared across all three ARAMCO DAGs.

Centralises the Spark connection id, Delta Lake packages, and the
properties file so every curated/BDH/ADL job launches with identical
cluster configuration.
"""
from __future__ import annotations

import os
from typing import List

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark_default"

# Set ARAMCO_REPO_ROOT in the Airflow scheduler/webserver environment to point
# at wherever this repo is checked out (e.g. a local Airflow install: export
# ARAMCO_REPO_ROOT=/Users/you/path/to/aramco-etl). Falls back to a container
# path for the reference containerized deployment layout.
REPO_ROOT = os.environ.get("ARAMCO_REPO_ROOT", "/opt/airflow/aramco-etl")
SRC_ROOT = f"{REPO_ROOT}/src"
PROPERTIES_FILE = f"{REPO_ROOT}/config/spark-defaults.conf"
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
