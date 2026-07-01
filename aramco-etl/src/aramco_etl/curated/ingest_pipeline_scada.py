"""CURATED ingestion job -- Midstream pipeline SCADA (flow, leak detection)."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class PipelineScadaIngestionJob(BaseIngestionJob):
    source_name = "pipeline_scada"

    def tables(self) -> List[str]:
        return ["raw_pipeline_flow_reading", "raw_leak_detection_alarm"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.json(path)


def main() -> None:
    args = parse_args()
    PipelineScadaIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
