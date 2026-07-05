"""CURATED ingestion job -- Downstream refinery LIMS (crude assay, product quality)."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class RefineryLimsIngestionJob(BaseIngestionJob):
    source_name = "refinery_lims"

    TABLE_SCHEMAS = {
        "raw_crude_assay": StructType([
            StructField("refinery_id", StringType()),
            StructField("sample_id", StringType()),
            StructField("sample_ts", TimestampType()),
            StructField("crude_grade", StringType()),
            StructField("api_gravity", DoubleType()),
            StructField("sulfur_content_pct", DoubleType()),
            StructField("water_content_pct", DoubleType()),
        ]),
        "raw_product_quality_test": StructType([
            StructField("refinery_id", StringType()),
            StructField("product_code", StringType()),
            StructField("batch_id", StringType()),
            StructField("test_ts", TimestampType()),
            StructField("test_parameter", StringType()),
            StructField("test_result", DoubleType()),
            StructField("spec_min", DoubleType()),
            StructField("spec_max", DoubleType()),
            StructField("pass_fail_flag", StringType()),
        ]),
    }

    def tables(self) -> List[str]:
        return ["raw_crude_assay", "raw_product_quality_test"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.option("header", "true").schema(self._schema_for(table_name)).csv(path)


def main() -> None:
    args = parse_args()
    RefineryLimsIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
