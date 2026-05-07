"""
Schritt 3: Daten berechnen
==========================
Liest die Roh-Basistabelle und berechnet alle abgeleiteten Variablen
(Prozentanteile, Raten, Verhaeltnisse).

Input:
  data/processed/sf_fire_incidents_base.parquet

Output:
  data/processed/sf_fire_risk_features.parquet         (alle 53 Spalten, deutsch)
  data/processed/sf_fire_risk_features_cleaned.parquet (23 Modellspalten, deutsch)

Berechnete Variablen:

ACS (sozio-oekonomisch):
  poverty_rate        = poverty_below / poverty_universe_total * 100
  bachelor_rate       = bachelor_degree_count / education_universe_total * 100
  vacancy_rate        = vacant_housing_units / total_housing_units * 100

Crime:
  pct_violent_crime   = violent_crime_count / total_crimes * 100
  pct_property_crime  = property_crime_count / total_crimes * 100

Land Use:
  pct_pre1940                    = pre1940_count / yrbuilt_count * 100
  pct_pre1960                    = pre1960_count / yrbuilt_count * 100
  pct_residential                = residential_count / parcel_count * 100
  pct_high_risk_commercial_area  = high_risk_commercial_area_sqft / total_area_sqft * 100

Ausfuehren:
  python scripts/03_compute_features.py
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

# Spalten fuer das Analysemodell (behalten + optional + VIF-Kandidaten), deutsche Namen
CLEANED_COLS = [
    # Identifikation / Kontrolle
    "stadtteil",
    "acs_jahr",
    "jahr",
    "stunde",
    "ist_wochenende",
    "ist_nacht",
    # Zielvariable
    "antwortzeit_min",
    # Einsatzcharakteristik
    "einsatzart",
    "bataillon",             # optional
    "alarmstufe",            # optional
    "schaetzung_sachschaden_usd",  # optional
    # ACS Soziooekonomie
    "gesamtbevoelkerung",
    "median_haushaltseinkommen",
    "armutsquote_pct",
    "akademikerquote_pct",
    "median_miete",
    "leerstandsquote_pct",
    # Crime
    "anteil_gewaltdelikte_pct",    # VIF pruefen
    "anteil_eigentumsdelikte_pct", # VIF pruefen
    # Land Use
    "gesamtzahl_wohneinheiten",
    "anteil_altbau_vor_1940_pct",
    "anteil_wohngebaeude_pct",
    "anteil_risikogewerbe_pct",
]


def safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Gibt Verhältnis als Anteil [0, 1] zurück (4 Dezimalstellen)."""
    num = pd.to_numeric(numerator,   errors="coerce").astype(float)
    den = pd.to_numeric(denominator, errors="coerce").astype(float)
    den = den.where(den > 0, np.nan)
    return (num / den).round(4)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # ── ACS-Raten ─────────────────────────────────────────────────────────────
    if {"poverty_below", "poverty_universe_total"}.issubset(out.columns):
        out["poverty_rate"] = safe_ratio(out["poverty_below"], out["poverty_universe_total"])
    if {"bachelor_degree_count", "education_universe_total"}.issubset(out.columns):
        out["bachelor_rate"] = safe_ratio(out["bachelor_degree_count"], out["education_universe_total"])
    if {"vacant_housing_units", "total_housing_units"}.issubset(out.columns):
        out["vacancy_rate"] = safe_ratio(out["vacant_housing_units"], out["total_housing_units"])

    # ── Crime-Anteile ─────────────────────────────────────────────────────────
    if {"violent_crime_count", "total_crimes"}.issubset(out.columns):
        out["pct_violent_crime"] = safe_ratio(out["violent_crime_count"], out["total_crimes"])
    if {"property_crime_count", "total_crimes"}.issubset(out.columns):
        out["pct_property_crime"] = safe_ratio(out["property_crime_count"], out["total_crimes"])

    # ── Land-Use-Anteile ──────────────────────────────────────────────────────
    if {"pre1940_count", "yrbuilt_count"}.issubset(out.columns):
        out["pct_pre1940"] = safe_ratio(out["pre1940_count"], out["yrbuilt_count"])
    if {"pre1960_count", "yrbuilt_count"}.issubset(out.columns):
        out["pct_pre1960"] = safe_ratio(out["pre1960_count"], out["yrbuilt_count"])
    if {"residential_count", "parcel_count"}.issubset(out.columns):
        out["pct_residential"] = safe_ratio(out["residential_count"], out["parcel_count"])
    if {"high_risk_commercial_area_sqft", "total_area_sqft"}.issubset(out.columns):
        out["pct_high_risk_commercial_area"] = safe_ratio(
            out["high_risk_commercial_area_sqft"], out["total_area_sqft"]
        )

    # ── Typ-Korrektur: Ganzzahl-Spalten auf Int64 (NaN-faehig) ───────────────
    int_cols = [
        "total_population", "poverty_below", "poverty_universe_total",
        "bachelor_degree_count", "education_universe_total",
        "vacant_housing_units", "total_housing_units", "total_resunits",
    ]
    for col in int_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(0).astype("Int64")

    return out


def summarize(df: pd.DataFrame, new_cols: list[str]) -> None:
    print("\n  Verteilung neuer Variablen (auf Einsatz-Ebene):")
    print(f"    {'Variable':<35} {'Mean':>10} {'Median':>10} {'Min':>10} {'Max':>10} {'NaN%':>7}")
    for col in new_cols:
        if col not in df.columns:
            continue
        s = df[col].dropna()
        if s.empty:
            print(f"    {col:<35}  (alle NaN)")
            continue
        nan_pct = df[col].isna().mean() * 100
        print(f"    {col:<35} {s.mean():>10.4f} {s.median():>10.4f} "
              f"{s.min():>10.4f} {s.max():>10.4f} {nan_pct:>6.1f}%")


def run_compute():
    if not BASE_PATH.exists():
        raise FileNotFoundError(
            f"{BASE_PATH.relative_to(ROOT)} nicht gefunden. "
            f"Erst 'python scripts/02_join_data.py' ausfuehren."
        )

    print("=" * 80)
    print("  Schritt 3: Daten berechnen")
    print("=" * 80)

    print(f"\n  Lese {BASE_PATH.relative_to(ROOT)}...")
    base = pd.read_parquet(BASE_PATH)
    print(f"  {len(base):,} Zeilen  |  {len(base.columns)} Spalten")

    print("\n  Berechne abgeleitete Variablen...")
    features = compute_features(base)

    new_cols = [
        "poverty_rate", "bachelor_rate", "vacancy_rate",
        "pct_violent_crime", "pct_property_crime",
        "pct_pre1940", "pct_pre1960", "pct_residential",
        "pct_high_risk_commercial_area",
    ]
    added = [c for c in new_cols if c in features.columns]
    print(f"  Hinzugefuegt: {len(added)} Spalten -> {added}")

    summarize(features, added)

    features = features.rename(columns=spalten_deutsch)
    features.to_parquet(FEATURES_PATH, index=False)

    cleaned_cols = [c for c in CLEANED_COLS if c in features.columns]
    cleaned = features[cleaned_cols]
    cleaned.to_parquet(CLEANED_PATH, index=False)

    print("\n" + "=" * 80)
    print(f"  => {FEATURES_PATH.relative_to(ROOT)}  "
          f"({len(features):,} Zeilen  |  {len(features.columns)} Spalten)")
    print(f"  => {CLEANED_PATH.relative_to(ROOT)}  "
          f"({len(cleaned):,} Zeilen  |  {len(cleaned.columns)} Spalten)")
    print("=" * 80)
    return features


if __name__ == "__main__":
    run_compute()
