"""CURATED ingestion job -- SCADA / OSIsoft PI upstream telemetry.

Run via spark-submit (see dags/aramco_curated_ingestion_dag.py):

    spark-submit --py-files aramco_etl.zip \\
        src/aramco_etl/curated/ingest_scada_upstream.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class ScadaUpstreamIngestionJob(BaseIngestionJob):
    source_name = "scada_upstream"

    TABLE_SCHEMAS = {
        "raw_well_production_reading": StructType([
            StructField("well_id", StringType()),
            StructField("field_id", StringType()),
            StructField("reading_ts", TimestampType()),
            StructField("oil_rate_bopd", DoubleType()),
            StructField("gas_rate_mscfd", DoubleType()),
            StructField("water_rate_bwpd", DoubleType()),
            StructField("tubing_pressure_psi", DoubleType()),
            StructField("casing_pressure_psi", DoubleType()),
            StructField("choke_size_64th", IntegerType()),
            StructField("well_status_code", StringType()),
        ]),
        "raw_field_header_pressure": StructType([
            StructField("field_id", StringType()),
            StructField("header_id", StringType()),
            StructField("reading_ts", TimestampType()),
            StructField("header_pressure_psi", DoubleType()),
            StructField("header_temp_f", DoubleType()),
        ]),
        "raw_wellhead_events": StructType([
            StructField("well_id", StringType()),
            StructField("event_ts", TimestampType()),
            StructField("event_type", StringType()),
            StructField("event_detail", StringType()),
            StructField("raw_payload", StringType()),
        ]),
    }

    def tables(self) -> List[str]:
        return ["raw_well_production_reading", "raw_field_header_pressure", "raw_wellhead_events"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.schema(self._schema_for(table_name)).json(path)


def main() -> None:
    args = parse_args()
    ScadaUpstreamIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
