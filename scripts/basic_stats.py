"""
Basic statistics analysis for SFFD Fire Incidents dataset (FIR-0001).
Expects data/raw/fire_incidents.parquet (created by download_data.py).
Uses Polars for efficient handling of large datasets.
"""

import polars as pl
import sys
from pathlib import Path
from datetime import datetime

ROOT     = Path(__file__).parent.parent
DATA_RAW = ROOT / "data" / "raw"
RESULTS  = ROOT / "results"
RESULTS.mkdir(exist_ok=True)

# ── Column groups (FIR-0001 schema, snake_case) ───────────────────────────────
DATETIME_COLS = ["incident_date", "alarm_dttm", "arrival_dttm", "close_dttm"]

CATEGORICAL_COLS = [
    "primary_situation", "property_use", "mutual_aid",
    "action_taken_primary", "action_taken_secondary",
    "ignition_cause", "ignition_factor_primary", "heat_source",
    "item_first_ignited", "fire_spread",
    "structure_type", "structure_status",
    "detectors_present", "detector_effectiveness",
    "automatic_extinguishing_system_present",
    "battalion", "station_area", "city",
    "supervisor_district", "neighborhood_district",
]

NUMERIC_COLS = [
    "suppression_units", "suppression_personnel",
    "ems_units", "ems_personnel",
    "other_units", "other_personnel",
    "fire_fatalities", "fire_injuries",
    "civilian_fatalities", "civilian_injuries",
    "number_of_alarms", "exposure_number",
    "estimated_property_loss", "estimated_contents_loss",
    "floor_of_fire_origin",
    "number_of_floors_with_minimum_damage",
    "number_of_floors_with_significant_damage",
    "number_of_floors_with_heavy_damage",
    "number_of_floors_with_extreme_damage",
    "number_of_sprinkler_heads_operating",
]

DT_FMT = "%Y-%m-%dT%H:%M:%S%.3f"

RESPONSE_PAIRS = [
    ("Alarm → Arrival",  "alarm_dttm", "arrival_dttm"),
    ("Alarm → Close",    "alarm_dttm", "close_dttm"),
]


def find_data() -> Path:
    parquet = DATA_RAW / "fire_incidents.parquet"
    if parquet.exists():
        return parquet
    csvs = list(DATA_RAW.glob("*.csv"))
    if csvs:
        return csvs[0]
    print(f"ERROR: No data file found in {DATA_RAW}")
    print("       Run scripts/download_data.py first.")
    sys.exit(1)


def load(data_path: Path) -> pl.LazyFrame:
    print(f"Loading: {data_path.name}")
    if data_path.suffix == ".parquet":
        lf = pl.scan_parquet(data_path)
    else:
        lf = pl.scan_csv(
            data_path,
            infer_schema_length=10_000,
            null_values=["", "NA", "N/A", "null"],
            ignore_errors=True,
        )
    # Cast numeric columns eagerly after collect
    return lf


def section(title: str, out: list[str]) -> None:
    line = f"\n{'=' * 70}\n  {title}\n{'=' * 70}"
    print(line)
    out.append(line)


def log(text: str, out: list[str]) -> None:
    print(text)
    out.append(text)


def run() -> None:
    out: list[str] = [
        "SFFD Fire Incidents (FIR-0001) — Basic Statistics",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    ]

    data_path = find_data()
    lf = load(data_path)

    # ── 1. Schema ──────────────────────────────────────────────────────────────
    section("1. SCHEMA", out)
    schema = lf.collect_schema()
    for col, dtype in schema.items():
        log(f"  {col:<50} {str(dtype)}", out)

    # ── 2. Shape ───────────────────────────────────────────────────────────────
    section("2. SHAPE", out)
    df = lf.collect()

    # Cast numerics
    for col in NUMERIC_COLS:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    rows, cols = df.shape
    log(f"  Rows   : {rows:>12,}", out)
    log(f"  Columns: {cols:>12,}", out)
    log(f"  Memory : {df.estimated_size('mb'):>11.1f} MB", out)

    # ── 3. Missing values ──────────────────────────────────────────────────────
    section("3. MISSING VALUES (columns with any null)", out)
    null_counts = df.null_count()
    has_nulls = {
        col: null_counts[col][0]
        for col in null_counts.columns
        if null_counts[col][0] > 0
    }
    if has_nulls:
        for col, n in sorted(has_nulls.items(), key=lambda x: -x[1]):
            pct = n / rows * 100
            log(f"  {col:<50} {n:>9,}  ({pct:5.1f}%)", out)
    else:
        log("  No missing values.", out)

    # ── 4. Categorical distributions ──────────────────────────────────────────
    section("4. CATEGORICAL COLUMNS — TOP 10 VALUES", out)
    for col in CATEGORICAL_COLS:
        if col not in df.columns:
            continue
        counts = (
            df.filter(pl.col(col).is_not_null())
            .group_by(col)
            .len()
            .sort("len", descending=True)
            .head(10)
        )
        n_unique = df[col].n_unique()
        log(f"\n  {col}  (unique: {n_unique:,})", out)
        for row in counts.iter_rows():
            val, cnt = row
            pct = cnt / rows * 100
            log(f"    {str(val):<50} {cnt:>9,}  ({pct:5.1f}%)", out)

    # ── 5. Numeric statistics ──────────────────────────────────────────────────
    section("5. NUMERIC COLUMNS — DESCRIPTIVE STATISTICS", out)
    existing_num = [c for c in NUMERIC_COLS if c in df.columns]
    if existing_num:
        desc = df.select(existing_num).describe()
        log(str(desc), out)

    # ── 6. Temporal coverage ───────────────────────────────────────────────────
    section("6. TEMPORAL COVERAGE", out)
    for col in DATETIME_COLS:
        if col in df.columns:
            parsed = df[col].str.to_datetime(format=DT_FMT, strict=False)
            valid = parsed.drop_nulls()
            if valid.len() > 0:
                log(f"  {col:<20}  {valid.min()}  →  {valid.max()}", out)

    # ── 7. Response time metrics ───────────────────────────────────────────────
    section("7. RESPONSE TIME METRICS (minutes)", out)
    for label, col_start, col_end in RESPONSE_PAIRS:
        if col_start not in df.columns or col_end not in df.columns:
            log(f"  {label}: columns not found, skipping.", out)
            continue
        try:
            duration = (
                df.with_columns([
                    pl.col(col_start).str.to_datetime(format=DT_FMT, strict=False),
                    pl.col(col_end).str.to_datetime(format=DT_FMT, strict=False),
                ])
                .with_columns(
                    ((pl.col(col_end) - pl.col(col_start))
                     .dt.total_seconds() / 60.0)
                    .alias("_min")
                )
                .filter(pl.col("_min").is_between(0, 480))
                ["_min"]
            )
            log(f"\n  {label}", out)
            log(f"    n valid  : {duration.len():>10,}", out)
            log(f"    mean     : {duration.mean():>10.2f} min", out)
            log(f"    median   : {duration.median():>10.2f} min", out)
            log(f"    90th pct : {duration.quantile(0.90):>10.2f} min", out)
            log(f"    95th pct : {duration.quantile(0.95):>10.2f} min", out)
            log(f"    max      : {duration.max():>10.2f} min", out)
        except Exception as e:
            log(f"  {label}: could not compute ({e})", out)

    # ── 8. Casualty summary ────────────────────────────────────────────────────
    section("8. CASUALTY SUMMARY", out)
    for col in ["fire_fatalities", "fire_injuries", "civilian_fatalities", "civilian_injuries"]:
        if col in df.columns:
            total = int(df[col].drop_nulls().sum())
            incidents_with = int(df.filter(pl.col(col) > 0).shape[0])
            log(f"  {col:<35}  total: {total:>6,}  incidents with >0: {incidents_with:>6,}", out)

    # ── Save ───────────────────────────────────────────────────────────────────
    out_file = RESULTS / "basic_stats_summary.txt"
    out_file.write_text("\n".join(out), encoding="utf-8")
    print(f"\nSummary saved to: {out_file}")


if __name__ == "__main__":
    run()
