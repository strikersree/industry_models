"""Generates synthetic sample data for every CURATED source, written into
LANDING_ROOT in the exact format/schema each ingestion job expects.

Entity pools (wells, assets, cost centers, products, ...) are fixed and
persist across the whole date range, with a couple of deliberate mid-window
attribute changes (asset criticality/status) so the BDH SCD2 dimensions
have something real to version.

    python scripts/generate_sample_data.py \\
        --landing-root /path/to/landing --start-date 2026-06-22 --days 10

Reuses the exact StructType schemas each ingestion job reads with (see
aramco_etl.curated.ingest_*.TABLE_SCHEMAS), so generated files are
guaranteed to match what the readers expect.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from aramco_etl.common.spark_session import get_spark_session  # noqa: E402
from aramco_etl.curated.ingest_hse import HseIngestionJob  # noqa: E402
from aramco_etl.curated.ingest_maximo_eam import MaximoEamIngestionJob  # noqa: E402
from aramco_etl.curated.ingest_pipeline_scada import PipelineScadaIngestionJob  # noqa: E402
from aramco_etl.curated.ingest_refinery_lims import RefineryLimsIngestionJob  # noqa: E402
from aramco_etl.curated.ingest_sap_erp import SapErpIngestionJob  # noqa: E402
from aramco_etl.curated.ingest_scada_upstream import ScadaUpstreamIngestionJob  # noqa: E402

RNG = random.Random(42)

# --- Fixed entity pools, persisted across the whole run -------------------
FIELDS = [f"F{i}" for i in range(1, 6)]
WELLS = {f"W{i}": RNG.choice(FIELDS) for i in range(1, 21)}
FACILITIES = [f"FAC{i}" for i in range(1, 6)]
ASSET_TYPES = ["PUMP", "COMPRESSOR", "VESSEL", "PIPELINE_SEGMENT"]
ASSETS = {
    f"A{i}": {
        "asset_type": RNG.choice(ASSET_TYPES),
        "facility_id": RNG.choice(FACILITIES),
        "criticality_rank": RNG.choice(["A", "B", "B", "C", "C"]),
        "record_status": "ACTIVE",
        "manufacturer": RNG.choice(["ACME", "GlobalTech", "PetroWorks", "IndustrialCo"]),
        "install_date": (datetime(2005, 1, 1) + timedelta(days=RNG.randint(0, 6500))).date(),
    }
    for i in range(1, 31)
}
COST_CENTERS = (
    [(f"UP-{100 + i}", "upstream") for i in range(5)]
    + [(f"MS-{200 + i}", "midstream") for i in range(4)]
    + [(f"DS-{300 + i}", "downstream") for i in range(4)]
)
PRODUCTS = ["GASOLINE_91", "GASOLINE_95", "DIESEL_ULSD", "DIESEL_EN590", "JET_A1"]
REFINERIES = ["REF1", "REF2", "REF3"]
PIPELINE_SEGMENTS = [f"PS{i}" for i in range(1, 9)]
CREWS = [f"CREW{i}" for i in range(1, 11)]
EMPLOYEES = [f"EMP{i}" for i in range(1, 21)]
VENDORS = [f"VEND{i}" for i in range(1, 11)]
MATERIALS = [f"MAT{i}" for i in range(1, 11)]

WELL_BASELINE = {w: RNG.uniform(200, 2000) for w in WELLS}
PIPELINE_BASELINE = {p: RNG.uniform(5000, 20000) for p in PIPELINE_SEGMENTS}
COST_CENTER_BASELINE = {cc: RNG.uniform(20000, 200000) for cc, _ in COST_CENTERS}


def daterange(start_date: str, days: int):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]


def ts(business_date: str, hour: int, minute: int = 0) -> datetime:
    d = datetime.strptime(business_date, "%Y-%m-%d")
    return d.replace(hour=hour, minute=minute)


# --- Per-source, per-table row generators ----------------------------------

def gen_well_production_reading(business_date: str):
    rows = []
    for well, field in WELLS.items():
        baseline = WELL_BASELINE[well]
        for hour in range(0, 24, 2):
            status = "PRODUCING" if RNG.random() > 0.08 else "SHUT_IN"
            oil = round(baseline * RNG.uniform(0.9, 1.1), 2) if status == "PRODUCING" else 0.0
            rows.append((
                well, field, ts(business_date, hour), oil, round(oil * 0.5, 2), round(oil * 0.15, 2),
                round(RNG.uniform(1500, 3000), 1), round(RNG.uniform(300, 800), 1),
                RNG.randint(16, 40), status,
            ))
    return rows


def gen_field_header_pressure(business_date: str):
    rows = []
    for field in FIELDS:
        for hour in range(0, 24, 4):
            rows.append((
                field, f"HDR-{field}", ts(business_date, hour),
                round(RNG.uniform(1000, 2500), 1), round(RNG.uniform(80, 150), 1),
            ))
    return rows


def gen_wellhead_events(business_date: str):
    rows = []
    for _ in range(RNG.randint(8, 18)):
        well = RNG.choice(list(WELLS))
        event_type = RNG.choice(["SHUT_IN", "START_UP", "CHOKE_CHANGE", "ALARM"])
        hour = RNG.randint(0, 23)
        rows.append((
            well, ts(business_date, hour, RNG.randint(0, 59)), event_type,
            f"{event_type} on {well}", f'{{"well_id":"{well}","event_type":"{event_type}"}}',
        ))
    return rows


def gen_asset_master(business_date: str, day_index: int):
    rows = []
    for asset_id, attrs in ASSETS.items():
        criticality = attrs["criticality_rank"]
        status = attrs["record_status"]
        # Mid-window change to exercise SCD2 on a couple of assets.
        if day_index == 5 and asset_id in ("A3", "A7"):
            criticality = "A" if asset_id == "A3" else attrs["criticality_rank"]
            status = "STANDBY" if asset_id == "A7" else status
            attrs["criticality_rank"], attrs["record_status"] = criticality, status
        rows.append((
            asset_id, f"{attrs['asset_type'].title()} {asset_id}", attrs["asset_type"], attrs["facility_id"],
            f"LOC-{attrs['facility_id']}", attrs["manufacturer"], attrs["install_date"], criticality,
            None, status, ts(business_date, 0),
        ))
    return rows


def gen_work_order(business_date: str):
    rows = []
    for i in range(RNG.randint(25, 45)):
        asset_id = RNG.choice(list(ASSETS))
        work_type = RNG.choice(["PM", "CM", "EM"])
        reported_hour = RNG.randint(0, 20)
        rows.append((
            f"WO-{business_date.replace('-', '')}-{i:04d}", asset_id, work_type,
            RNG.choice(["LOW", "MEDIUM", "HIGH"]), RNG.choice(["COMP", "CLOSE", "INPRG"]),
            f"P{RNG.randint(1, 20):02d}", ts(business_date, reported_hour),
            ts(business_date, reported_hour + 1), ts(business_date, reported_hour + 1, 10),
            ts(business_date, min(reported_hour + 3, 23), 30),
            round(RNG.uniform(1, 16), 1), round(RNG.uniform(50, 5000), 2), RNG.choice(CREWS),
        ))
    return rows


def gen_downtime_event(business_date: str):
    rows = []
    for i in range(RNG.randint(4, 14)):
        asset_id = RNG.choice(list(ASSETS))
        start_hour = RNG.randint(0, 18)
        duration = RNG.randint(1, 5)
        rows.append((
            asset_id, ts(business_date, start_hour), ts(business_date, min(start_hour + duration, 23)),
            RNG.choice(["MECH_FAILURE", "PLANNED_TURNAROUND", "POWER_LOSS"]),
            f"WO-{business_date.replace('-', '')}-{i:04d}",
        ))
    return rows


def gen_gl_journal(business_date: str):
    from decimal import Decimal
    d = datetime.strptime(business_date, "%Y-%m-%d")
    rows = []
    for i in range(RNG.randint(80, 150)):
        amount_sar = round(RNG.uniform(500, 50000), 2)
        rows.append((
            f"JRN-{business_date.replace('-', '')}-{i:05d}", d.date(), d.year, d.month,
            f"GL{RNG.randint(1000, 1999)}", RNG.choice(COST_CENTERS)[0], RNG.choice(["SA", "KR", "RE"]),
            Decimal(str(amount_sar)), "SAR", Decimal(str(round(amount_sar / 3.75, 2))),
            RNG.choice(["S", "H"]), f"REF-{i:05d}",
        ))
    return rows


def gen_purchase_order(business_date: str):
    from decimal import Decimal
    d = datetime.strptime(business_date, "%Y-%m-%d")
    rows = []
    for i in range(RNG.randint(15, 40)):
        qty = RNG.randint(1, 500)
        price = round(RNG.uniform(10, 10000), 2)
        rows.append((
            f"PO-{business_date.replace('-', '')}-{i:04d}", RNG.randint(1, 5), RNG.choice(VENDORS),
            RNG.choice(MATERIALS), RNG.choice(COST_CENTERS)[0], Decimal(str(qty)), "EA",
            Decimal(str(price)), d.date(), RNG.choice(["OPEN", "PARTIAL", "COMPLETE"]),
        ))
    return rows


def gen_cost_center_actuals(business_date: str):
    from decimal import Decimal
    d = datetime.strptime(business_date, "%Y-%m-%d")
    rows = []
    for cc, _domain in COST_CENTERS:
        baseline = COST_CENTER_BASELINE[cc]
        actual = round(baseline * RNG.uniform(0.95, 1.1), 2)
        budget = round(baseline * 1.05, 2)
        rows.append((
            cc, d.year, d.month, f"CE{RNG.randint(1, 20):02d}", Decimal(str(actual)), Decimal(str(budget)),
        ))
    return rows


def gen_pipeline_flow_reading(business_date: str):
    rows = []
    for segment in PIPELINE_SEGMENTS:
        baseline = PIPELINE_BASELINE[segment]
        for hour in range(0, 24, 2):
            rows.append((
                segment, ts(business_date, hour), round(baseline * RNG.uniform(0.9, 1.1), 1),
                round(RNG.uniform(500, 1500), 1), round(RNG.uniform(60, 120), 1), f"PUMP-{segment}",
            ))
    return rows


def gen_leak_detection_alarm(business_date: str):
    rows = []
    for _ in range(RNG.randint(3, 10)):
        segment = RNG.choice(PIPELINE_SEGMENTS)
        hour = RNG.randint(0, 23)
        severity = RNG.choice(["LOW", "LOW", "MEDIUM", "MEDIUM", "HIGH", "CRITICAL"])
        acked_hour = min(hour + RNG.randint(0, 2), 23)
        rows.append((
            segment, ts(business_date, hour), RNG.choice(["PRESSURE_DROP", "MASS_BALANCE_DEVIATION", "ACOUSTIC"]),
            severity, ts(business_date, acked_hour), RNG.choice(EMPLOYEES),
        ))
    return rows


def gen_crude_assay(business_date: str):
    rows = []
    for refinery in REFINERIES:
        for i in range(RNG.randint(3, 6)):
            rows.append((
                refinery, f"SMP-{business_date.replace('-', '')}-{refinery}-{i}",
                ts(business_date, RNG.randint(0, 23)),
                RNG.choice(["ARABIAN_LIGHT", "ARABIAN_HEAVY", "ARABIAN_EXTRA_LIGHT"]),
                round(RNG.uniform(28, 40), 2), round(RNG.uniform(0.5, 3.0), 2), round(RNG.uniform(0, 1), 2),
            ))
    return rows


def gen_product_quality_test(business_date: str):
    rows = []
    spec_by_family = {
        "GASOLINE": ("OCTANE_NUMBER", 91.0, 98.0),
        "DIESEL": ("CETANE_NUMBER", 45.0, 55.0),
        "JET": ("FLASH_POINT", 38.0, 60.0),
    }
    for refinery in REFINERIES:
        for product in PRODUCTS:
            family = next(k for k in spec_by_family if product.startswith(k))
            param, spec_min, spec_max = spec_by_family[family]
            for i in range(RNG.randint(2, 4)):
                result = round(RNG.uniform(spec_min - 3, spec_max + 3), 2)
                rows.append((
                    refinery, product, f"BATCH-{business_date.replace('-', '')}-{product}-{i}",
                    ts(business_date, RNG.randint(0, 23)), param, result, spec_min, spec_max,
                    "PASS" if spec_min <= result <= spec_max else "FAIL",
                ))
    return rows


def gen_incident_report(business_date: str):
    rows = []
    if RNG.random() < 0.35:  # HSE incidents are rare events
        for i in range(RNG.randint(1, 2)):
            incident_type = RNG.choices(
                ["LTI", "MTI", "FAI", "NEAR_MISS"], weights=[5, 15, 30, 50]
            )[0]
            lost_days = RNG.randint(1, 10) if incident_type == "LTI" else 0
            rows.append((
                f"INC-{business_date.replace('-', '')}-{i}", RNG.choice(FACILITIES),
                ts(business_date, RNG.randint(0, 23)), incident_type,
                RNG.choice(["MINOR", "SERIOUS", "MAJOR"]), lost_days, RNG.choice(EMPLOYEES), None,
                f"{incident_type} incident at facility",
            ))
    return rows


def gen_safety_observation(business_date: str):
    rows = []
    for i in range(RNG.randint(10, 25)):
        rows.append((
            f"OBS-{business_date.replace('-', '')}-{i:04d}", RNG.choice(FACILITIES),
            ts(business_date, RNG.randint(0, 23)),
            RNG.choice(["UNSAFE_ACT", "UNSAFE_CONDITION", "POSITIVE_OBSERVATION"]),
            RNG.choice(EMPLOYEES), "Reviewed with crew" if RNG.random() > 0.3 else None, RNG.random() > 0.2,
        ))
    return rows


# --- Source registry: (job class, table -> (generator, extra generator args)) ----

SOURCES = [
    ("scada_upstream", ScadaUpstreamIngestionJob, "json", {
        "raw_well_production_reading": lambda bd, di: gen_well_production_reading(bd),
        "raw_field_header_pressure": lambda bd, di: gen_field_header_pressure(bd),
        "raw_wellhead_events": lambda bd, di: gen_wellhead_events(bd),
    }),
    ("maximo_eam", MaximoEamIngestionJob, "csv", {
        "raw_asset_master": lambda bd, di: gen_asset_master(bd, di),
        "raw_work_order": lambda bd, di: gen_work_order(bd),
        "raw_downtime_event": lambda bd, di: gen_downtime_event(bd),
    }),
    ("sap_erp", SapErpIngestionJob, "parquet", {
        "raw_gl_journal": lambda bd, di: gen_gl_journal(bd),
        "raw_purchase_order": lambda bd, di: gen_purchase_order(bd),
        "raw_cost_center_actuals": lambda bd, di: gen_cost_center_actuals(bd),
    }),
    ("pipeline_scada", PipelineScadaIngestionJob, "json", {
        "raw_pipeline_flow_reading": lambda bd, di: gen_pipeline_flow_reading(bd),
        "raw_leak_detection_alarm": lambda bd, di: gen_leak_detection_alarm(bd),
    }),
    ("refinery_lims", RefineryLimsIngestionJob, "csv", {
        "raw_crude_assay": lambda bd, di: gen_crude_assay(bd),
        "raw_product_quality_test": lambda bd, di: gen_product_quality_test(bd),
    }),
    ("hse", HseIngestionJob, "json", {
        "raw_incident_report": lambda bd, di: gen_incident_report(bd),
        "raw_safety_observation": lambda bd, di: gen_safety_observation(bd),
    }),
]


def write_table(spark, df_rows, schema, fmt: str, path: str) -> int:
    # Always write a file, even with zero rows: a source can legitimately have
    # no records for a given day (e.g. no HSE incidents), and the landing
    # path must still exist for the ingestion job to read from.
    df = spark.createDataFrame(df_rows, schema=schema)
    writer = df.coalesce(1).write.mode("overwrite")
    if fmt == "csv":
        writer = writer.option("header", "true")
    writer.format(fmt).save(path)
    return df.count()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic ARAMCO curated-layer sample data")
    parser.add_argument("--landing-root", required=True, help="Local folder to write landing files into")
    parser.add_argument("--start-date", required=True, help="yyyy-MM-dd, first business date to generate")
    parser.add_argument("--days", type=int, default=10, help="Number of consecutive business dates to generate")
    args = parser.parse_args()

    spark = get_spark_session("aramco_generate_sample_data")
    dates = daterange(args.start_date, args.days)

    for source_name, job_cls, fmt, table_generators in SOURCES:
        for table_name, generator in table_generators.items():
            schema = job_cls.TABLE_SCHEMAS[table_name]
            for day_index, business_date in enumerate(dates):
                rows = generator(business_date, day_index)
                path = f"{args.landing_root}/{source_name}/{table_name}/business_date={business_date}"
                count = write_table(spark, rows, schema, fmt, path)
                print(f"{source_name}.{table_name} [{business_date}]: {count} rows -> {path}")

    print(f"\nDone. Generated {len(dates)} business dates ({dates[0]} .. {dates[-1]}) into {args.landing_root}")


if __name__ == "__main__":
    main()
