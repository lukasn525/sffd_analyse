"""
SFFD + ACS Datenpipeline
Bachelorarbeit: Sozioökonomische Einflüsse auf Feuerwehreinsätze in SF

Lädt und verknüpft drei Datenquellen:
  1. SFFD Fire Incidents  → DataSF SODA API (data.sfgov.org)
  2. Neighborhood-Crosswalk → DataSF (Census Tract → Neighborhood)
  3. ACS 5-Year Estimates → US Census API (Einkommen, Armut, Bildung)

Ergebnis:
  data/raw/fire_incidents.parquet      – bereinigte SFFD-Rohdaten
  data/raw/crosswalk.csv               – Census Tract ↔ Neighborhood
  data/raw/acs_tracts_2021.csv         – ACS-Daten auf Tract-Ebene
  data/processed/acs_neighborhoods.csv – ACS aggregiert auf Neighborhood
  data/processed/sffd_acs_joined.csv   – Analysedatei (SFFD + ACS)

Ausführen:
  python scripts/download_data.py          # volle Pipeline
  python scripts/download_data.py test     # APIs ohne Keys testen
"""

import pandas as pd
import requests
import time
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

# ── API-Keys ──────────────────────────────────────────────────────────────────
CENSUS_API_KEY   = "f5cb8b553da8a01e351b3804e56e7fe664e12c98"
DATASF_APP_TOKEN = None   # optional – data.sfgov.org/profile/app_tokens

# ── Download-Steuerung ────────────────────────────────────────────────────────
# True  → Daten frisch herunterladen und Datei überschreiben
# False → vorhandene Datei aus data/raw/ verwenden (kein API-Aufruf)
DOWNLOAD_SFFD      = False   # data/raw/fire_incidents.parquet
DOWNLOAD_CROSSWALK = True    # data/raw/crosswalk.csv
DOWNLOAD_ACS       = True    # data/raw/acs_tracts_2021.csv

# ── Pfade ─────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).parent.parent
RAW_DIR       = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"


# ══════════════════════════════════════════════════════════════════════════════
# SCHRITT 1: SFFD Fire Incidents (DataSF SODA API)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_sffd_incidents(app_token: str = None) -> pd.DataFrame:
    """Lädt alle SFFD-Einsätze via SODA API mit Pagination."""
    base_url = "https://data.sfgov.org/resource/wr8u-xric.json"

    select_fields = ",".join([
        "incident_number", "incident_date", "alarm_dttm", "arrival_dttm",
        "neighborhood_district", "battalion", "primary_situation",
        "suppression_units", "suppression_personnel", "ems_units",
        "number_of_alarms", "civilian_fatalities", "civilian_injuries",
        "estimated_property_loss", "no_flame_spread",
    ])

    headers = {"X-App-Token": app_token} if app_token else {}
    all_rows, offset, limit = [], 0, 50_000

    print("  Lade SFFD-Daten von DataSF...")
    while True:
        params = {
            "$select": select_fields,
            "$where":  "neighborhood_district IS NOT NULL AND arrival_dttm IS NOT NULL",
            "$limit":  limit,
            "$offset": offset,
            "$order":  ":id",
        }
        resp = requests.get(base_url, params=params, headers=headers, timeout=60)
        if not resp.ok:
            print(f"\n  HTTP {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        offset += limit
        print(f"  {len(all_rows):>7,} Einträge geladen...", end="\r")
        time.sleep(0.3)

    print(f"\n  Fertig: {len(all_rows):,} Einträge total.")
    df = pd.DataFrame(all_rows)

    for col in ["alarm_dttm", "arrival_dttm", "incident_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["suppression_units", "suppression_personnel", "ems_units",
                "number_of_alarms", "civilian_fatalities", "civilian_injuries",
                "estimated_property_loss"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["response_time_min"] = (
        df["arrival_dttm"] - df["alarm_dttm"]
    ).dt.total_seconds() / 60
    df = df[(df["response_time_min"] >= 0) & (df["response_time_min"] <= 60)]

    df["year"]       = df["incident_date"].dt.year
    df["month"]      = df["incident_date"].dt.month
    df["hour"]       = df["alarm_dttm"].dt.hour
    df["weekday"]    = df["alarm_dttm"].dt.dayofweek
    df["is_weekend"] = df["weekday"].isin([5, 6]).astype(int)
    df["is_night"]   = ((df["hour"] >= 22) | (df["hour"] <= 5)).astype(int)
    df["neighborhood"] = df["neighborhood_district"].str.strip().str.title()
    return df


def load_sffd() -> pd.DataFrame:
    """Gibt SFFD-Daten zurück — neu laden oder Cache verwenden."""
    path = RAW_DIR / "fire_incidents.parquet"
    if DOWNLOAD_SFFD:
        df = fetch_sffd_incidents(app_token=DATASF_APP_TOKEN)
        df.to_parquet(path, index=False)
        print(f"  Gespeichert: {path.relative_to(ROOT)}")
    else:
        if not path.exists():
            raise FileNotFoundError(f"{path} nicht gefunden. DOWNLOAD_SFFD=True setzen.")
        print(f"  Verwende Cache: {path.relative_to(ROOT)}")
        df = pd.read_parquet(path)
    print(f"  {len(df):,} Zeilen")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SCHRITT 2: Neighborhood → Census Tract Crosswalk (DataSF sevw-6tgi)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_neighborhood_crosswalk(app_token: str = None) -> pd.DataFrame:
    """Lädt Census Tract ↔ Neighborhood Crosswalk von DataSF."""
    headers = {"X-App-Token": app_token} if app_token else {}
    resp = requests.get(
        "https://data.sfgov.org/resource/sevw-6tgi.json",
        params={
            "$select": "geoid,neighborhoods_analysis_boundaries",
            "$limit":  300,
        },
        headers=headers,
        timeout=30,
    )
    if not resp.ok:
        print(f"\n  HTTP {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()

    df = pd.DataFrame(resp.json())
    df.columns = ["geoid", "neighborhood"]
    df["geoid"]        = df["geoid"].astype(str).str.zfill(11)
    df["neighborhood"] = df["neighborhood"].str.strip().str.title()

    print(f"  {len(df)} Tract-Neighborhood-Paare, "
          f"{df['neighborhood'].nunique()} Neighborhoods")
    return df


def load_crosswalk() -> pd.DataFrame:
    """Gibt Crosswalk zurück — neu laden oder Cache verwenden."""
    path = RAW_DIR / "crosswalk.csv"
    if DOWNLOAD_CROSSWALK:
        df = fetch_neighborhood_crosswalk(app_token=DATASF_APP_TOKEN)
        df.to_csv(path, index=False)
        print(f"  Gespeichert: {path.relative_to(ROOT)}")
    else:
        if not path.exists():
            raise FileNotFoundError(f"{path} nicht gefunden. DOWNLOAD_CROSSWALK=True setzen.")
        print(f"  Verwende Cache: {path.relative_to(ROOT)}")
        df = pd.read_csv(path, dtype={"geoid": str})
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SCHRITT 3: ACS 5-Year Estimates (US Census API)
# ══════════════════════════════════════════════════════════════════════════════
#
# State FIPS: Californien = "06", County FIPS: San Francisco = "075"

ACS_VARIABLES = {
    "B19013_001E": "median_household_income",
    "B17001_001E": "poverty_universe_total",
    "B17001_002E": "poverty_below",
    "B01003_001E": "total_population",
    "B15003_022E": "bachelor_degree_count",
    "B15003_001E": "education_universe_total",
    "B25064_001E": "median_gross_rent",
    "B25002_003E": "vacant_housing_units",
    "B25002_001E": "total_housing_units",
}


def fetch_acs_sf_tracts(year: int, api_key: str) -> pd.DataFrame:
    """Lädt ACS 5-Year Schätzwerte für alle Census Tracts in San Francisco."""
    variables = ",".join(["NAME"] + list(ACS_VARIABLES.keys()))
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={variables}"
        f"&for=tract:*"
        f"&in=state:06%20county:075"
        f"&key={api_key}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    df   = pd.DataFrame(data[1:], columns=data[0])
    df["geoid"] = df["state"] + df["county"] + df["tract"]
    df.rename(columns=ACS_VARIABLES, inplace=True)

    for col in ACS_VARIABLES.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] < -999, col] = pd.NA

    print(f"  ACS {year}: {len(df)} Census Tracts geladen.")
    
    # Rückgabe: Rohdaten (für Aggregation) + direkte Werte (Income, Rent)
    return df[["geoid", "total_population", "median_household_income", "median_gross_rent",
               "poverty_below", "poverty_universe_total",
               "bachelor_degree_count", "education_universe_total",
               "vacant_housing_units", "total_housing_units"]]


def load_acs() -> pd.DataFrame:
    """Gibt ACS-Tract-Daten zurück — neu laden oder Cache verwenden."""
    path = RAW_DIR / "acs_tracts_2021.csv"
    if DOWNLOAD_ACS:
        df = fetch_acs_sf_tracts(year=2021, api_key=CENSUS_API_KEY)
        df.to_csv(path, index=False)
        print(f"  Gespeichert: {path.relative_to(ROOT)}")
    else:
        if not path.exists():
            raise FileNotFoundError(f"{path} nicht gefunden. DOWNLOAD_ACS=True setzen.")
        print(f"  Verwende Cache: {path.relative_to(ROOT)}")
        df = pd.read_csv(path, dtype={"geoid": str})
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SCHRITT 4: Aggregation und Join
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_acs_to_neighborhood(acs_df: pd.DataFrame, crosswalk_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregiert ACS Tract-Daten auf Neighborhood-Ebene (bevölkerungsgewichtet)."""
    merged = acs_df.merge(crosswalk_df, on="geoid", how="left").dropna(subset=["neighborhood"])

    result_rows = []
    for hood, grp in merged.groupby("neighborhood"):
        pop_total = grp["total_population"].sum()
        
        # 1. Direkte Summen / gewichtete Mittel
        row = {
            "neighborhood": hood,
            "total_population": int(pop_total),
            "median_household_income": (
                (grp["median_household_income"] * grp["total_population"]).sum() / pop_total
            ) if pop_total > 0 else pd.NA,
            "median_gross_rent": (
                (grp["median_gross_rent"] * grp["total_population"]).sum() / pop_total
            ) if pop_total > 0 else pd.NA,
        }
        
        # 2. Quoten aus aggregierten Rohdaten neu berechnen
        poverty_total = grp["poverty_below"].sum()
        poverty_universe = grp["poverty_universe_total"].sum()
        row["poverty_rate"] = (poverty_total / poverty_universe * 100) if poverty_universe > 0 else pd.NA
        
        bachelor_total = grp["bachelor_degree_count"].sum()
        education_universe = grp["education_universe_total"].sum()
        row["bachelor_rate"] = (bachelor_total / education_universe * 100) if education_universe > 0 else pd.NA
        
        vacant_total = grp["vacant_housing_units"].sum()
        housing_total = grp["total_housing_units"].sum()
        row["vacancy_rate"] = (vacant_total / housing_total * 100) if housing_total > 0 else pd.NA
        
        result_rows.append(row)

    acs_neighborhood = pd.DataFrame(result_rows)

    # Datentypen normalisieren
    # Income & Rent: ganze Zahlen
    for col in ["median_household_income", "median_gross_rent"]:
        acs_neighborhood[col] = (
            pd.to_numeric(acs_neighborhood[col], errors="coerce").round(0).astype("Int64")
        )
    # Quoten: 2 Dezimalstellen
    for col in ["poverty_rate", "bachelor_rate", "vacancy_rate"]:
        acs_neighborhood[col] = (
            pd.to_numeric(acs_neighborhood[col], errors="coerce").round(2)
        )

    print(f"  ACS auf {len(acs_neighborhood)} Neighborhoods aggregiert.")
    return acs_neighborhood


def aggregate_sffd_to_neighborhood(sffd_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregiert SFFD-Incidents auf Neighborhood-Ebene."""
    agg = sffd_df.groupby("neighborhood").agg(
        incident_count          = ("incident_number",         "count"),
        mean_response_time      = ("response_time_min",       "mean"),
        median_response_time    = ("response_time_min",       "median"),
        p90_response_time       = ("response_time_min",       lambda x: x.quantile(0.9)),
        pct_over_5min           = ("response_time_min",       lambda x: (x > 5).mean() * 100),
        mean_suppression_units  = ("suppression_units",       "mean"),
        mean_ems_units          = ("ems_units",               "mean"),
        total_property_loss     = ("estimated_property_loss", "sum"),
        total_civilian_injuries = ("civilian_injuries",       "sum"),
    ).reset_index().round(3)

    print(f"  SFFD auf {len(agg)} Neighborhoods aggregiert.")
    return agg


# ══════════════════════════════════════════════════════════════════════════════
# HAUPTPIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print("  SFFD + ACS Datenpipeline")
    print("=" * 65)
    print(f"\n  Download-Flags: SFFD={DOWNLOAD_SFFD}  "
          f"CROSSWALK={DOWNLOAD_CROSSWALK}  ACS={DOWNLOAD_ACS}")

    # 1. SFFD
    print("\n[1/4] SFFD Fire Incidents...")
    sffd_raw = load_sffd()

    # 2. Crosswalk
    print("\n[2/4] Neighborhood-Crosswalk...")
    crosswalk = load_crosswalk()

    # 3. ACS
    print("\n[3/4] ACS 5-Year (2021) Census Daten...")
    acs_tracts = load_acs()
    acs_neighborhood = aggregate_acs_to_neighborhood(acs_tracts, crosswalk)
    acs_neighborhood.to_csv(PROCESSED_DIR / "acs_neighborhoods.csv", index=False)
    print(f"  Gespeichert: data/processed/acs_neighborhoods.csv")

    # 4. Zusammenführen
    print("\n[4/4] Zusammenführen...")

    # ACS-Daten an jeden einzelnen Einsatz joinen (nicht aggregiert)
    final = sffd_raw.merge(acs_neighborhood, on="neighborhood", how="left")

    # Typen bereinigen
    acs_int_cols   = ["total_population", "median_household_income", "median_gross_rent"]
    acs_float_cols = ["poverty_rate", "bachelor_rate", "vacancy_rate"]

    for col in acs_int_cols:
        if col in final.columns:
            final[col] = pd.to_numeric(final[col], errors="coerce").round(0)
            if final[col].notna().all():
                final[col] = final[col].astype("int64")
    for col in acs_float_cols:
        if col in final.columns:
            final[col] = pd.to_numeric(final[col], errors="coerce").round(2)

    final.to_parquet(PROCESSED_DIR / "sffd_acs_joined.parquet", index=False)

    print("\n" + "=" * 65)
    print("  Pipeline abgeschlossen!")
    print("=" * 65)
    print(f"\n  data/raw/fire_incidents.parquet         ({len(sffd_raw):>7,} Zeilen — SFFD-Rohdaten)")
    print(f"  data/raw/crosswalk.csv                  ({len(crosswalk):>7,} Zeilen — Tract↔Neighborhood)")
    print(f"  data/processed/acs_neighborhoods.csv    ({len(acs_neighborhood):>7,} Zeilen — ACS pro Neighborhood)")
    print(f"  data/processed/sffd_acs_joined.parquet  ({len(final):>7,} Zeilen — Analysedatei)")

    print("\n  Spalten mit fehlenden Werten (Analysedatei):")
    for col in final.columns:
        null_pct = final[col].isna().mean() * 100
        if null_pct > 0:
            print(f"    {col:<40} {null_pct:5.1f}%")

    return final


# ══════════════════════════════════════════════════════════════════════════════
# SCHNELLTEST — APIs ohne Keys prüfen
# ══════════════════════════════════════════════════════════════════════════════

def quick_test():
    print("=== Schnelltest ===\n")

    resp = requests.get(
        "https://data.sfgov.org/resource/wr8u-xric.json",
        params={
            "$select": "incident_number,neighborhood_district,alarm_dttm,arrival_dttm",
            "$where":  "neighborhood_district IS NOT NULL",
            "$limit":  3,
        },
        timeout=15,
    )
    if resp.ok:
        print("SFFD API: OK")
        print(pd.DataFrame(resp.json())[["incident_number", "neighborhood_district"]].to_string(index=False))
    else:
        print(f"SFFD API: Fehler {resp.status_code}")

    print()

    resp2 = requests.get(
        "https://data.sfgov.org/resource/sevw-6tgi.json",
        params={"$select": "geoid,neighborhoods_analysis_boundaries", "$limit": 3},
        timeout=15,
    )
    if resp2.ok:
        print("Crosswalk API: OK")
        print(pd.DataFrame(resp2.json()).to_string(index=False))
    else:
        print(f"Crosswalk API: Fehler {resp2.status_code}")

    print()

    resp3 = requests.get(
        "https://api.census.gov/data/2021/acs/acs5"
        "?get=NAME,B19013_001E,B17001_002E"
        "&for=tract:*"
        "&in=state:06%20county:075",
        timeout=15,
    )
    if resp3.ok:
        data = resp3.json()
        print(f"ACS Census API: OK ({len(data)-1} Tracts gefunden)")
        print(f"Beispiel: {data[1]}")
    else:
        print(f"ACS Census API: Fehler {resp3.status_code}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        quick_test()
    else:
        run_pipeline()
