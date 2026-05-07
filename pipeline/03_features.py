"""
Schritt 3: Raten berechnen, Spalten auf Deutsch umbenennen,
Modell-Datensatz auf 23 Spalten reduzieren.

Input:  data/processed/sf_fire_incidents_base.parquet
Output: data/processed/sf_fire_risk_features.parquet         (53 Spalten, deutsch)
        data/processed/sf_fire_risk_features_cleaned.parquet (23 Spalten, deutsch)
"""
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from column_names import spalten_deutsch

warnings.filterwarnings("ignore")

ROOT          = Path(__file__).parent.parent
PROCESSED_DIR = ROOT / "data" / "processed"
BASE_PATH     = PROCESSED_DIR / "sf_fire_incidents_base.parquet"
FEATURES_PATH = PROCESSED_DIR / "sf_fire_risk_features.parquet"
CLEANED_PATH  = PROCESSED_DIR / "sf_fire_risk_features_cleaned.parquet"

CLEANED_COLS = [
    "stadtteil", "acs_jahr", "jahr", "stunde", "ist_wochenende", "ist_nacht",
    "antwortzeit_min",
    "einsatzart", "bataillon", "alarmstufe", "schaetzung_sachschaden_usd",
    "gesamtbevoelkerung", "median_haushaltseinkommen", "armutsquote_pct",
    "akademikerquote_pct", "median_miete", "leerstandsquote_pct",
    "anteil_gewaltdelikte_pct", "anteil_eigentumsdelikte_pct",
    "gesamtzahl_wohneinheiten", "anteil_altbau_vor_1940_pct",
    "anteil_wohngebaeude_pct", "anteil_risikogewerbe_pct",
]

INT64_COLS = [
    "total_population", "poverty_below", "poverty_universe_total",
    "bachelor_degree_count", "education_universe_total",
    "vacant_housing_units", "total_housing_units", "total_resunits",
]

NEUE_VARIABLEN = [
    "poverty_rate", "bachelor_rate", "vacancy_rate",
    "pct_violent_crime", "pct_property_crime",
    "pct_pre1940", "pct_pre1960", "pct_residential",
    "pct_high_risk_commercial_area",
]


def safe_ratio(zaehler: pd.Series, nenner: pd.Series) -> pd.Series:
    z = pd.to_numeric(zaehler, errors="coerce").astype(float)
    n = pd.to_numeric(nenner,  errors="coerce").astype(float).where(lambda x: x > 0, np.nan)
    return (z / n).round(4)


def _add_ratio(df: pd.DataFrame, neu: str, zaehler: str, nenner: str) -> None:
    if {zaehler, nenner}.issubset(df.columns):
        df[neu] = safe_ratio(df[zaehler], df[nenner])


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    _add_ratio(df, "poverty_rate",      "poverty_below",         "poverty_universe_total")
    _add_ratio(df, "bachelor_rate",     "bachelor_degree_count", "education_universe_total")
    _add_ratio(df, "vacancy_rate",      "vacant_housing_units",  "total_housing_units")
    _add_ratio(df, "pct_violent_crime", "violent_crime_count",   "total_crimes")
    _add_ratio(df, "pct_property_crime","property_crime_count",  "total_crimes")
    _add_ratio(df, "pct_pre1940",       "pre1940_count",         "yrbuilt_count")
    _add_ratio(df, "pct_pre1960",       "pre1960_count",         "yrbuilt_count")
    _add_ratio(df, "pct_residential",   "residential_count",     "parcel_count")
    _add_ratio(df, "pct_high_risk_commercial_area",
               "high_risk_commercial_area_sqft", "total_area_sqft")
    for col in INT64_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(0).astype("Int64")
    return df


def summarize(df: pd.DataFrame, cols: list[str]) -> None:
    print(f"\n  {'Variable':<35} {'Mean':>10} {'Median':>10} {'Min':>10} {'Max':>10} {'NaN%':>7}")
    for col in cols:
        if col not in df.columns:
            continue
        s = df[col].dropna()
        nan_pct = df[col].isna().mean() * 100
        if s.empty:
            print(f"  {col:<35}  (alle NaN)")
        else:
            print(f"  {col:<35} {s.mean():>10.4f} {s.median():>10.4f} "
                  f"{s.min():>10.4f} {s.max():>10.4f} {nan_pct:>6.1f}%")


def run_compute() -> pd.DataFrame:
    if not BASE_PATH.exists():
        raise FileNotFoundError(
            f"{BASE_PATH.relative_to(ROOT)} nicht gefunden. "
            f"Erst 'python pipeline/02_join.py' ausfuehren.")

    print("Schritt 3: Daten berechnen\n")
    base = pd.read_parquet(BASE_PATH)
    print(f"  Eingang: {len(base):,} Zeilen | {len(base.columns)} Spalten")

    features = compute_features(base)
    hinzugefuegt = [c for c in NEUE_VARIABLEN if c in features.columns]
    print(f"  Hinzugefuegt: {len(hinzugefuegt)} Spalten -> {hinzugefuegt}")
    summarize(features, hinzugefuegt)

    features = features.rename(columns=spalten_deutsch)
    features.to_parquet(FEATURES_PATH, index=False)

    cleaned = features[[c for c in CLEANED_COLS if c in features.columns]]
    cleaned.to_parquet(CLEANED_PATH, index=False)

    print(f"\n=> {FEATURES_PATH.relative_to(ROOT)}  "
          f"({len(features):,} Zeilen | {len(features.columns)} Spalten)")
    print(f"=> {CLEANED_PATH.relative_to(ROOT)}  "
          f"({len(cleaned):,} Zeilen | {len(cleaned.columns)} Spalten)")
    return features


if __name__ == "__main__":
    run_compute()
