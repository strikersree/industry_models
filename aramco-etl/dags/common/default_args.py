"""Shared Airflow DAG defaults for the ARAMCO ETL framework."""
from __future__ import annotations

from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "aramco-data-engineering",
    "depends_on_past": False,
    "email": ["data-engineering-alerts@aramco.example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
