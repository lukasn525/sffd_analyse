"""
Orchestriert die ETL-Pipeline (01 -> 02 -> 03 + Sample-Export).

Ausfuehren:
  python pipeline/run_pipeline.py
"""
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

from column_names import spalten_deutsch

ROOT      = Path(__file__).parent.parent
SCRIPTS   = Path(__file__).parent
PROCESSED = ROOT / "data" / "processed"
SAMPLE    = ROOT / "data" / "sample"

HAUPTTABELLEN = [
    "sf_fire_incidents_base.parquet",
    "sf_fire_risk_features.parquet",
    "sf_fire_risk_features_cleaned.parquet",
]


def run_step(label: str, script: Path) -> None:
    print(f"\n{'=' * 80}\n  {label}\n{'=' * 80}\n")
    t0 = time.time()
    result = subprocess.run([sys.executable, str(script)])
    if result.returncode != 0:
        print(f"\n  FEHLER in {script.name} (exit {result.returncode}). Pipeline gestoppt.")
        sys.exit(result.returncode)
    print(f"\n  Dauer: {time.time() - t0:.1f}s")


def export_samples() -> None:
    print(f"\n{'=' * 80}\n  SCHRITT 4/4: Sample-Export\n{'=' * 80}\n")
    SAMPLE.mkdir(parents=True, exist_ok=True)
    for name in HAUPTTABELLEN:
        src = PROCESSED / name
        dst = SAMPLE / name.replace(".parquet", "_sample100.csv")
        if not src.exists():
            print(f"  FEHLER: {src.relative_to(ROOT)} nicht gefunden.")
            continue
        df = pd.read_parquet(src).head(100).rename(columns=spalten_deutsch)
        df.to_csv(dst, index=False, sep=";")
        print(f"  => {dst.relative_to(ROOT)}  ({df.shape[0]} Zeilen, {df.shape[1]} Spalten)")


def show_tables() -> None:
    print(f"\n{'=' * 80}\n  ERGEBNIS\n{'=' * 80}\n")
    for name in HAUPTTABELLEN:
        path = PROCESSED / name
        if not path.exists():
            print(f"  {name:<45} (fehlt)")
            continue
        df = pd.read_parquet(path)
        size_mb = path.stat().st_size / 1_048_576
        print(f"  {name:<45} {len(df):>8,} Zeilen | {len(df.columns):>3} Spalten | {size_mb:>5.1f} MB")


if __name__ == "__main__":
    t_total = time.time()
    run_step("SCHRITT 1/4: Daten einladen (01_fetch.py)",   SCRIPTS / "01_fetch.py")
    run_step("SCHRITT 2/4: Daten joinen  (02_join.py)",     SCRIPTS / "02_join.py")
    run_step("SCHRITT 3/4: Features      (03_features.py)", SCRIPTS / "03_features.py")
    export_samples()
    show_tables()
    print(f"\n  Gesamtdauer: {time.time() - t_total:.1f}s")
