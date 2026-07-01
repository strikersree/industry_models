"""CURATED ingestion job -- SCADA / OSIsoft PI upstream telemetry.

Run via spark-submit (see dags/aramco_curated_ingestion_dag.py):

    spark-submit --py-files aramco_etl.zip \\
        src/aramco_etl/curated/ingest_scada_upstream.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class ScadaUpstreamIngestionJob(BaseIngestionJob):
    source_name = "scada_upstream"

    def tables(self) -> List[str]:
        return ["raw_well_production_reading", "raw_field_header_pressure", "raw_wellhead_events"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.json(path)


def main() -> None:
    args = parse_args()
    ScadaUpstreamIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
