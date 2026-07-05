"""CURATED ingestion job -- Midstream pipeline SCADA (flow, leak detection)."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class PipelineScadaIngestionJob(BaseIngestionJob):
    source_name = "pipeline_scada"

    TABLE_SCHEMAS = {
        "raw_pipeline_flow_reading": StructType([
            StructField("pipeline_segment_id", StringType()),
            StructField("reading_ts", TimestampType()),
            StructField("flow_rate_bpd", DoubleType()),
            StructField("line_pressure_psi", DoubleType()),
            StructField("line_temp_f", DoubleType()),
            StructField("pump_station_id", StringType()),
        ]),
        "raw_leak_detection_alarm": StructType([
            StructField("pipeline_segment_id", StringType()),
            StructField("alarm_ts", TimestampType()),
            StructField("alarm_type", StringType()),
            StructField("severity", StringType()),
            StructField("acknowledged_ts", TimestampType()),
            StructField("acknowledged_by", StringType()),
        ]),
    }

    def tables(self) -> List[str]:
        return ["raw_pipeline_flow_reading", "raw_leak_detection_alarm"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.schema(self._schema_for(table_name)).json(path)


def main() -> None:
    args = parse_args()
    PipelineScadaIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
