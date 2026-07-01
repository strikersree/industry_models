"""CURATED ingestion job -- HSE incidents and safety observations."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class HseIngestionJob(BaseIngestionJob):
    source_name = "hse"

    def tables(self) -> List[str]:
        return ["raw_incident_report", "raw_safety_observation"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.json(path)


def main() -> None:
    args = parse_args()
    HseIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
