"""CURATED ingestion job -- HSE incidents and safety observations."""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from aramco_etl.curated.base_ingestion import BaseIngestionJob, parse_args


class HseIngestionJob(BaseIngestionJob):
    source_name = "hse"

    TABLE_SCHEMAS = {
        "raw_incident_report": StructType([
            StructField("incident_id", StringType()),
            StructField("facility_id", StringType()),
            StructField("incident_ts", TimestampType()),
            StructField("incident_type", StringType()),
            StructField("severity_rank", StringType()),
            StructField("lost_workdays", IntegerType()),
            StructField("involved_employee_id", StringType()),
            StructField("involved_contractor", StringType()),
            StructField("description", StringType()),
        ]),
        "raw_safety_observation": StructType([
            StructField("observation_id", StringType()),
            StructField("facility_id", StringType()),
            StructField("observation_ts", TimestampType()),
            StructField("observation_category", StringType()),
            StructField("observer_employee_id", StringType()),
            StructField("corrective_action", StringType()),
            StructField("closed_flag", BooleanType()),
        ]),
    }

    def tables(self) -> List[str]:
        return ["raw_incident_report", "raw_safety_observation"]

    def read_raw(self, table_name: str) -> DataFrame:
        path = self._landing_path(table_name)
        return self.spark.read.schema(self._schema_for(table_name)).json(path)


def main() -> None:
    args = parse_args()
    HseIngestionJob(business_date=args.business_date, run_id=args.run_id).run()


if __name__ == "__main__":
    main()
