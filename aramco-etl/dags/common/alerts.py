"""Failure-callback used on DQ-gate tasks (BDH/ADL layers).

Mirrors the STC framework's "halt the batch + page Data Engineering"
pattern: when a DQ gate fails (or any layer task fails), this callback logs
a structured alert. Wire in a real Slack/PagerDuty webhook by replacing the
body of `alert_on_failure` -- the Airflow `email_on_failure` default arg
already handles the email leg.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger("aramco_etl.alerts")


def alert_on_failure(context: Dict[str, Any]) -> None:
    task_instance = context["task_instance"]
    dag_run = context["dag_run"]
    logger.error(
        "ARAMCO ETL ALERT | dag=%s task=%s run_id=%s try=%d log_url=%s",
        task_instance.dag_id,
        task_instance.task_id,
        dag_run.run_id,
        task_instance.try_number,
        task_instance.log_url,
    )
