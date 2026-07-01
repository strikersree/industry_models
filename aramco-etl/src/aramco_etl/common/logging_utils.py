"""Standardised job logging for all ARAMCO ETL jobs.

Every curated ingestion / BDH transform / ADL mart job logs a consistent
start/end record so Airflow task logs and any downstream log aggregation
(e.g. Splunk, CloudWatch) can correlate runs by `ingestion_run_id`.
"""
from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from typing import Iterator


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


@contextmanager
def job_run(logger: logging.Logger, job_name: str, run_id: str) -> Iterator[None]:
    start = time.time()
    logger.info("START job=%s run_id=%s", job_name, run_id)
    try:
        yield
    except Exception:
        logger.exception("FAILED job=%s run_id=%s", job_name, run_id)
        raise
    else:
        elapsed = time.time() - start
        logger.info("SUCCESS job=%s run_id=%s elapsed_sec=%.1f", job_name, run_id, elapsed)
