"""
Deskriptive Statistik auf dem finalen Modelldatensatz.

Liest:
  data/processed/sf_fire_risk_features_cleaned.parquet  (23 Spalten, deutsch)

Schreibt:
  results/deskriptiv_summary.txt

Ausgabe:
  - Numerische Spalten:  arithmetisches Mittel, Minimum, Maximum, NaN-Anteil
  - Kategorische Spalten: Anzahl unique, Top-5 haeufigste Werte

Ausfuehren:
  python analyse/deskriptiv.py
"""
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).parent.parent
DATA_PATH  = ROOT / "data" / "processed" / "sf_fire_risk_features_cleaned.parquet"
OUT_PATH   = ROOT / "results" / "deskriptiv_summary.txt"


def lade_daten() -> pd.DataFrame:
    try:
        return pd.read_parquet(DATA_PATH)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Datensatz nicht gefunden: {DATA_PATH.relative_to(ROOT)}\n"
            f"Pipeline zuerst ausfuehren: python pipeline/run_pipeline.py"
        )


def numerische_statistik(df: pd.DataFrame) -> pd.DataFrame:
    num = df.select_dtypes(include="number")
    stats = pd.DataFrame({
        "mittelwert": num.mean().round(2),
        "min":        num.min().round(2),
        "max":        num.max().round(2),
        "nan_pct":    (num.isna().mean() * 100).round(1),
    })
    return stats


def kategorische_statistik(df: pd.DataFrame, top_n: int = 5) -> list[str]:
    blocks = []
    cat_cols = df.select_dtypes(include=["object", "string", "category"]).columns
    for col in cat_cols:
        counts = df[col].value_counts(dropna=True).head(top_n)
        blocks.append(f"\n  {col}  (unique: {df[col].nunique()})")
        for wert, n in counts.items():
            anteil = n / len(df) * 100
            blocks.append(f"    {str(wert)[:50]:<50} {n:>9,}  ({anteil:5.1f}%)")
    return blocks


def schreibe_bericht(df: pd.DataFrame) -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    zeilen: list[str] = [
        "Deskriptive Statistik – sf_fire_risk_features_cleaned",
        f"Erstellt: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"Zeilen:  {len(df):,}",
        f"Spalten: {len(df.columns)}",
        "",
        "=" * 78,
        "  Numerische Variablen",
        "=" * 78,
        numerische_statistik(df).to_string(),
        "",
        "=" * 78,
        "  Kategorische Variablen (Top 5 je Spalte)",
        "=" * 78,
    ]
    zeilen.extend(kategorische_statistik(df))

    text = "\n".join(zeilen)
    OUT_PATH.write_text(text, encoding="utf-8")
    print(text)
    print(f"\n  => {OUT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    df = lade_daten()
    schreibe_bericht(df)
