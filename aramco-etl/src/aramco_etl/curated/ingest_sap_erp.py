"""CURATED ingestion job -- SAP ERP (GL journal, purchase orders, cost centers)."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class SapErpIngestionJob(BaseIngestionJob):
    source_name = "sap_erp"

    def tables(self) -> List[str]:
        return ["raw_gl_journal", "raw_purchase_order", "raw_cost_center_actuals"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.parquet(path)


def main() -> None:
    args = parse_args()
    SapErpIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
