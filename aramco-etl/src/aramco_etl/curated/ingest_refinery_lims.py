"""CURATED ingestion job -- Downstream refinery LIMS (crude assay, product quality)."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class RefineryLimsIngestionJob(BaseIngestionJob):
    source_name = "refinery_lims"

    def tables(self) -> List[str]:
        return ["raw_crude_assay", "raw_product_quality_test"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def main() -> None:
    args = parse_args()
    RefineryLimsIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
