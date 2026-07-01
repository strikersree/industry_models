"""CURATED ingestion job -- IBM Maximo EAM (assets, work orders, downtime)."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class MaximoEamIngestionJob(BaseIngestionJob):
    source_name = "maximo_eam"

    def tables(self) -> List[str]:
        return ["raw_asset_master", "raw_work_order", "raw_downtime_event"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def main() -> None:
    args = parse_args()
    MaximoEamIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
