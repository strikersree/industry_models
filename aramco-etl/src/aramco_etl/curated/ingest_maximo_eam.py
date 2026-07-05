"""CURATED ingestion job -- IBM Maximo EAM (assets, work orders, downtime)."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType, TimestampType

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class MaximoEamIngestionJob(BaseIngestionJob):
    source_name = "maximo_eam"

    TABLE_SCHEMAS = {
        "raw_asset_master": StructType([
            StructField("asset_id", StringType()),
            StructField("asset_description", StringType()),
            StructField("asset_type", StringType()),
            StructField("facility_id", StringType()),
            StructField("location_code", StringType()),
            StructField("manufacturer", StringType()),
            StructField("install_date", DateType()),
            StructField("criticality_rank", StringType()),
            StructField("parent_asset_id", StringType()),
            StructField("record_status", StringType()),
            StructField("last_modified_ts", TimestampType()),
        ]),
        "raw_work_order": StructType([
            StructField("work_order_id", StringType()),
            StructField("asset_id", StringType()),
            StructField("work_type", StringType()),
            StructField("priority", StringType()),
            StructField("status", StringType()),
            StructField("problem_code", StringType()),
            StructField("reported_ts", TimestampType()),
            StructField("scheduled_start_ts", TimestampType()),
            StructField("actual_start_ts", TimestampType()),
            StructField("actual_finish_ts", TimestampType()),
            StructField("labor_hours", DoubleType()),
            StructField("material_cost_usd", DoubleType()),
            StructField("assigned_crew_id", StringType()),
        ]),
        "raw_downtime_event": StructType([
            StructField("asset_id", StringType()),
            StructField("downtime_start_ts", TimestampType()),
            StructField("downtime_end_ts", TimestampType()),
            StructField("downtime_reason_code", StringType()),
            StructField("linked_work_order_id", StringType()),
        ]),
    }

    def tables(self) -> List[str]:
        return ["raw_asset_master", "raw_work_order", "raw_downtime_event"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.option("header", "true").schema(self._schema_for(table_name)).csv(path)


def main() -> None:
    args = parse_args()
    MaximoEamIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
