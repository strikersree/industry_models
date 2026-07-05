"""Base class for CURATED (Bronze) ingestion jobs.

Each source system in config/sources.yaml gets one subclass. A subclass only
needs to say *how* to read its raw files (`read_raw`) and *which* tables it
owns (`tables`); this base class handles audit-column stamping, the
per-source schema routing, and standard job logging.
"""
from __future__ import annotations

import argparse
from abc import ABC, abstractmethod
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType

from aramco_etl.common.config import Environment, get_source_config
from aramco_etl.common.delta_io import write_append
from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session


class BaseIngestionJob(ABC):
    """One subclass per source system (config/sources.yaml `sources.<name>`)."""

    source_name: str

    # Explicit per-table schemas, keyed by table name. Both JSON and CSV
    # readers happily accept `spark.read.schema(...)`, and it's required for
    # JSON: unlike CSV, Spark's JSON schema inference does NOT detect
    # timestamp/date-looking strings and leaves them as StringType, which
    # then fails to append into the curated DDL's TIMESTAMP/DATE columns.
    TABLE_SCHEMAS: Dict[str, StructType] = {}

    def __init__(self, business_date: str, run_id: str):
        self.business_date = business_date
        self.run_id = run_id
        self.env = Environment.from_env()
        self.source_config = get_source_config(self.source_name)
        self.logger = get_logger(f"curated.{self.source_name}")
        self.spark: SparkSession = get_spark_session(f"curated_ingest_{self.source_name}")

    @abstractmethod
    def read_raw(self, table_name: str) -> DataFrame:
        """Read a single raw table for this source from its landing path,
        for `self.business_date`."""

    @abstractmethod
    def tables(self) -> List[str]:
        """Raw table names this source ingests (must match sources.yaml)."""

    def _landing_path(self, table_name: str) -> str:
        return f"{self.source_config['landing_path']}/{table_name}/business_date={self.business_date}"

    def _path_exists(self, path: str) -> bool:
        """Hadoop-FS-backed existence check -- works for local paths as well
        as s3a://, abfss://, dbfs:/, etc., unlike a plain os.path check."""
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        return fs.exists(self.spark._jvm.org.apache.hadoop.fs.Path(path))

    def _schema_for(self, table_name: str) -> StructType:
        try:
            return self.TABLE_SCHEMAS[table_name]
        except KeyError:
            raise KeyError(
                f"No explicit schema registered for table '{table_name}' in "
                f"{type(self).__name__}.TABLE_SCHEMAS"
            ) from None

    def _add_audit_columns(self, df: DataFrame, source_file: str) -> DataFrame:
        return (
            df.withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.lit(source_file))
            .withColumn("_business_date", F.to_date(F.lit(self.business_date)))
            .withColumn("_ingestion_run_id", F.lit(self.run_id))
        )

    def run(self) -> None:
        with job_run(self.logger, f"curated_ingest_{self.source_name}", self.run_id):
            schema = self.source_config["curated_schema"]
            for table in self.tables():
                landing_path = self._landing_path(table)
                if not self._path_exists(landing_path):
                    self.logger.warning(
                        "No landing data at %s for business_date=%s -- skipping %s.%s "
                        "(source sent nothing for this date)",
                        landing_path, self.business_date, schema, table,
                    )
                    continue
                self.logger.info(
                    "Ingesting %s.%s for business_date=%s", schema, table, self.business_date
                )
                raw_df = self.read_raw(table)
                audited_df = self._add_audit_columns(raw_df, source_file=landing_path)
                write_append(audited_df, f"{schema}.{table}", partition_by=["_business_date"])
                self.logger.info("Wrote %d rows to %s.%s", audited_df.count(), schema, table)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ARAMCO curated ingestion job")
    parser.add_argument("--business-date", required=True, help="yyyy-MM-dd business date of this batch")
    parser.add_argument("--run-id", required=True, help="Airflow run_id / correlation id")
    return parser.parse_args()
