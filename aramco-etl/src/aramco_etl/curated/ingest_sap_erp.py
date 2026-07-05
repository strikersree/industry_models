"""CURATED ingestion job -- SAP ERP (GL journal, purchase orders, cost centers)."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class SapErpIngestionJob(BaseIngestionJob):
    source_name = "sap_erp"

    TABLE_SCHEMAS = {
        "raw_gl_journal": StructType([
            StructField("journal_id", StringType()),
            StructField("posting_date", DateType()),
            StructField("fiscal_year", IntegerType()),
            StructField("fiscal_period", IntegerType()),
            StructField("gl_account_code", StringType()),
            StructField("cost_center_code", StringType()),
            StructField("document_type", StringType()),
            StructField("amount_local_ccy", DecimalType(18, 2)),
            StructField("local_currency", StringType()),
            StructField("amount_usd", DecimalType(18, 2)),
            StructField("debit_credit_indicator", StringType()),
            StructField("reference_document", StringType()),
        ]),
        "raw_purchase_order": StructType([
            StructField("po_number", StringType()),
            StructField("po_line_number", IntegerType()),
            StructField("vendor_code", StringType()),
            StructField("material_code", StringType()),
            StructField("cost_center_code", StringType()),
            StructField("order_qty", DecimalType(18, 3)),
            StructField("unit_of_measure", StringType()),
            StructField("net_price_usd", DecimalType(18, 2)),
            StructField("po_created_date", DateType()),
            StructField("po_status", StringType()),
        ]),
        "raw_cost_center_actuals": StructType([
            StructField("cost_center_code", StringType()),
            StructField("fiscal_year", IntegerType()),
            StructField("fiscal_period", IntegerType()),
            StructField("cost_element_code", StringType()),
            StructField("actual_amount_usd", DecimalType(18, 2)),
            StructField("budget_amount_usd", DecimalType(18, 2)),
        ]),
    }

    def tables(self) -> List[str]:
        return ["raw_gl_journal", "raw_purchase_order", "raw_cost_center_actuals"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.schema(self._schema_for(table_name)).parquet(path)


def main() -> None:
    args = parse_args()
    SapErpIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
