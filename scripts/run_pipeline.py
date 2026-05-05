"""
run_pipeline.py - Orchestrator
==============================
Fuehrt die 3 Pipeline-Schritte hintereinander aus und zeigt
am Ende eine Uebersicht aller erzeugten Tabellen.

Schritte:
  01_fetch_data.py      Daten einladen   -> data/raw/
  02_join_data.py       Daten joinen     -> data/processed/sf_fire_incidents_base.parquet
  03_compute_features.py Daten berechnen -> data/processed/sf_fire_risk_features.parquet

Ausfuehren:
  python scripts/run_pipeline.py
"""

import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

ROOT    = Path(__file__).parent.parent
SCRIPTS = Path(__file__).parent


def run_step(label: str, script: Path) -> None:
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  {label}")
    print(f"{sep}\n")
    t0 = time.time()
    result = subprocess.run([sys.executable, str(script)])
    elapsed = time.time() - t0
    if result.returncode != 0:
        print(f"\n  FEHLER in {script.name} (exit {result.returncode}). Pipeline gestoppt.")
        sys.exit(result.returncode)
    print(f"\n  Dauer: {elapsed:.1f}s")


def show_tables() -> None:
    sep = "=" * 80
    print(f"\n{sep}")
    print("  ERGEBNIS: Tabellenubersicht")
    print(f"{sep}")

    # ── Raw-Dateien ───────────────────────────────────────────────────────────
    print("\n  data/raw/  (Rohdaten von APIs):")
    raw_files = [
        ("fire_incidents.parquet",     "SFFD Einsaetze"),
        ("crosswalk.csv",              "Tract-Neighborhood-Crosswalk"),
        ("crime_raw.parquet",          "SFPD Crime (monatlich)"),
        ("land_use_2020_raw.parquet",  "Land Use Parzellen"),
        ("neighborhoods.geojson",      "Neighborhood Boundaries"),
    ]
    for fname, desc in raw_files:
        p = ROOT / "data" / "raw" / fname
        if not p.exists():
            print(f"    {fname:<35} (nicht vorhanden)")
            continue
        size_mb = p.stat().st_size / 1_048_576
        try:
            if fname.endswith(".parquet"):
                df = pd.read_parquet(p)
                print(f"    {fname:<35} {len(df):>8,} Zeilen  |  {size_mb:>6.1f} MB  ({desc})")
            elif fname.endswith(".csv"):
                df = pd.read_csv(p)
                print(f"    {fname:<35} {len(df):>8,} Zeilen  |  {size_mb:>6.1f} MB  ({desc})")
            else:
                import json
                d = json.loads(p.read_text(encoding="utf-8"))
                n = len(d.get("features", []))
                print(f"    {fname:<35} {n:>8} Features|  {size_mb:>6.1f} MB  ({desc})")
        except Exception as e:
            print(f"    {fname:<35} Fehler: {e}")

    for year in [2009, 2014, 2019, 2021, 2023]:
        p = ROOT / "data" / "raw" / f"acs_tracts_{year}.csv"
        if p.exists():
            df = pd.read_csv(p)
            print(f"    acs_tracts_{year}.csv               {len(df):>8,} Tracts   "
                  f"|  {p.stat().st_size/1_048_576:>5.1f} MB  (ACS {year})")

    # ── Processed: Zwischenprodukte ──────────────────────────────────────────
    print("\n  data/processed/  (Neighborhood-Aggregate nach Schritt 2):")
    proc_files = [
        ("crime_neighborhoods.csv",           "Crime pro Neighborhood"),
        ("land_use_2020_neighborhoods.csv",   "Land Use pro Neighborhood"),
    ]
    for fname, desc in proc_files:
        p = ROOT / "data" / "processed" / fname
        if p.exists():
            df = pd.read_csv(p)
            print(f"    {fname:<40} {len(df):>4} Neighborhoods  ({desc})")
            print(f"      Spalten: {list(df.columns)}")

    for year in [2009, 2014, 2019, 2021, 2023]:
        p = ROOT / "data" / "processed" / f"acs_neighborhoods_{year}.csv"
        if p.exists():
            df = pd.read_csv(p)
            print(f"    acs_neighborhoods_{year}.csv          "
                  f"{len(df):>4} Neighborhoods  (ACS {year})")

    # ── Processed: Haupttabellen ─────────────────────────────────────────────
    print("\n  data/processed/  (Haupttabellen):")

    base_path = ROOT / "data" / "processed" / "sf_fire_incidents_base.parquet"
    if base_path.exists():
        base = pd.read_parquet(base_path)
        size_mb = base_path.stat().st_size / 1_048_576
        print(f"\n  [Schritt 2] sf_fire_incidents_base.parquet")
        print(f"    {len(base):,} Zeilen  |  {len(base.columns)} Spalten  |  {size_mb:.1f} MB")
        print(f"    Spalten: {list(base.columns)}")
        print(f"\n    Stichprobe (5 Zeilen):")
        sample_cols = ["incident_number", "incident_date", "neighborhood", "year",
                       "total_crimes", "parcel_count", "total_area_sqft",
                       "high_risk_commercial_area_sqft", "total_population",
                       "poverty_below", "poverty_universe_total"]
        show = [c for c in sample_cols if c in base.columns]
        print(base[show].sample(5, random_state=42).to_string(index=False))

    feat_path = ROOT / "data" / "processed" / "sf_fire_risk_features.parquet"
    if feat_path.exists():
        feat = pd.read_parquet(feat_path)
        size_mb = feat_path.stat().st_size / 1_048_576
        print(f"\n  [Schritt 3] sf_fire_risk_features.parquet")
        print(f"    {len(feat):,} Zeilen  |  {len(feat.columns)} Spalten  |  {size_mb:.1f} MB")
        derived = ["poverty_rate", "bachelor_rate", "vacancy_rate",
                   "pct_violent_crime", "pct_property_crime",
                   "pct_pre1940", "pct_pre1960", "pct_residential",
                   "pct_high_risk_commercial_area"]
        existing = [c for c in derived if c in feat.columns]
        print(f"    Neue Variablen ({len(existing)}): {existing}")
        print(f"\n    Statistik neue Variablen:")
        print(f"    {'Variable':<35} {'Mean':>8} {'Median':>8} {'Min':>8} {'Max':>8} {'NaN%':>6}")
        for col in existing:
            s = feat[col].dropna()
            null_pct = feat[col].isna().mean() * 100
            if s.empty:
                print(f"    {col:<35} alle NaN")
            else:
                print(f"    {col:<35} {s.mean():>8.2f} {s.median():>8.2f} "
                      f"{s.min():>8.2f} {s.max():>8.2f} {null_pct:>5.1f}%")

    print(f"\n{sep}")


if __name__ == "__main__":
    t_total = time.time()

    run_step("SCHRITT 1/3: Daten einladen (01_fetch_data.py)", SCRIPTS / "01_fetch_data.py")
    run_step("SCHRITT 2/3: Daten joinen  (02_join_data.py)",   SCRIPTS / "02_join_data.py")
    run_step("SCHRITT 3/3: Daten berechnen (03_compute_features.py)", SCRIPTS / "03_compute_features.py")

    show_tables()
    print(f"\n  Gesamtdauer: {time.time() - t_total:.1f}s")
