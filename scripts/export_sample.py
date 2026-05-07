"""
Exportiert die ersten 100 Zeilen aus den Haupttabellen als CSV (deutsche Spaltennamen).

Input:
  data/processed/sf_fire_incidents_base.parquet   (englische Spaltennamen, Zwischenprodukt)
  data/processed/sf_fire_risk_features.parquet    (deutsche Spaltennamen, finales Produkt)

Output:
  data/sample/sf_fire_incidents_base_sample100.csv
  data/sample/sf_fire_risk_features_sample100.csv

Ausfuehren:
  python scripts/export_sample.py
"""
import pandas as pd
from pathlib import Path

from column_names import spalten_deutsch

ROOT      = Path(__file__).parent.parent
PROCESSED = ROOT / "data" / "processed"
SAMPLE    = ROOT / "data" / "sample"
SAMPLE.mkdir(parents=True, exist_ok=True)


def export_sample(parquet_path: Path, csv_path: Path, rename: bool = False) -> None:
    df = pd.read_parquet(parquet_path)
    sample = df.head(100)
    if rename:
        sample = sample.rename(columns=spalten_deutsch)
    sample.to_csv(csv_path, index=False, sep=";")
    print(f"  => {csv_path.relative_to(ROOT)}  ({sample.shape[0]} Zeilen, {sample.shape[1]} Spalten)")


if __name__ == "__main__":
    print("=" * 60)
    print("  Sample-Export (100 Zeilen, deutsche Spaltennamen)")
    print("=" * 60)

    exports = [
        (PROCESSED / "sf_fire_incidents_base.parquet",          SAMPLE / "sf_fire_incidents_base_sample100.csv",          True),
        (PROCESSED / "sf_fire_risk_features.parquet",           SAMPLE / "sf_fire_risk_features_sample100.csv",           True),
        (PROCESSED / "sf_fire_risk_features_cleaned.parquet",   SAMPLE / "sf_fire_risk_features_cleaned_sample100.csv",   True),
    ]

    for src, dst, do_rename in exports:
        if not src.exists():
            print(f"  FEHLER: {src.relative_to(ROOT)} nicht gefunden.")
            continue
        export_sample(src, dst, rename=do_rename)

    print("=" * 60)
