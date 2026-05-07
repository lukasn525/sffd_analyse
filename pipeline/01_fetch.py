"""
Schritt 1: Daten einladen
=========================
Laedt alle Rohdaten von externen APIs nach data/raw/.
Keine Aggregation, kein Join - nur Download und Persistierung.

Datenquellen:
  1. SFFD Fire Incidents     -> DataSF (wr8u-xric)
  2. Neighborhood-Crosswalk  -> DataSF (sevw-6tgi)
  3. ACS 5-Year Estimates    -> US Census API (mehrere Jahrgaenge)
  4. SFPD Crime Data         -> DataSF (e3si-785i)
  5. Land Use 2020           -> DataSF (ygi5-84iq)
  6. Neighborhood Boundaries -> DataSF (j2bu-swwd)

Outputs:
  data/raw/fire_incidents.parquet
  data/raw/crosswalk.csv
  data/raw/acs_tracts_{year}.csv
  data/raw/crime_raw.parquet
  data/raw/land_use_2020_raw.parquet
  data/raw/neighborhoods.geojson

Ausfuehren:
  python scripts/01_fetch_data.py          # vollstaendiger Download
  python scripts/01_fetch_data.py test     # nur API-Verfuegbarkeit testen
"""

import time
import warnings
from pathlib import Path

import pandas as pd
import requests

warnings.filterwarnings("ignore")

# ── API-Keys ──────────────────────────────────────────────────────────────────
CENSUS_API_KEY   = "f5cb8b553da8a01e351b3804e56e7fe664e12c98"
DATASF_APP_TOKEN = None

# ── ACS-Jahrgaenge ────────────────────────────────────────────────────────────
ACS_YEARS = [2009, 2014, 2019, 2021, 2023]

# ── Download-Steuerung (True = neu laden, False = ueberspringen) ──────────────
DOWNLOAD_SFFD          = False
DOWNLOAD_CROSSWALK     = False
DOWNLOAD_ACS           = False
DOWNLOAD_CRIME         = False
DOWNLOAD_LAND_USE_2020 = False
DOWNLOAD_NEIGHBORHOODS = False

# ── Pfade ─────────────────────────────────────────────────────────────────────
ROOT    = Path(__file__).parent.parent
RAW_DIR = ROOT / "data" / "raw"


# ══════════════════════════════════════════════════════════════════════════════
# SFFD Fire Incidents (wr8u-xric)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_sffd_incidents(app_token: str = None) -> pd.DataFrame:
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
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        offset += limit
        print(f"  {len(all_rows):>7,} Eintraege geladen...", end="\r")
        time.sleep(0.3)

    print(f"\n  Fertig: {len(all_rows):,} Eintraege total.")
    df = pd.DataFrame(all_rows)

    for col in ["alarm_dttm", "arrival_dttm", "incident_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["suppression_units", "suppression_personnel", "ems_units",
                "number_of_alarms", "civilian_fatalities", "civilian_injuries",
                "estimated_property_loss"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Neighborhood Crosswalk (sevw-6tgi)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_neighborhood_crosswalk(app_token: str = None) -> pd.DataFrame:
    headers = {"X-App-Token": app_token} if app_token else {}
    resp = requests.get(
        "https://data.sfgov.org/resource/sevw-6tgi.json",
        params={"$select": "geoid,neighborhoods_analysis_boundaries", "$limit": 300},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    df.columns = ["geoid", "neighborhood"]
    df["geoid"]        = df["geoid"].astype(str).str.zfill(11)
    df["neighborhood"] = df["neighborhood"].str.strip().str.title()
    print(f"  {len(df)} Tract-Neighborhood-Paare, {df['neighborhood'].nunique()} Neighborhoods")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# ACS 5-Year Estimates (US Census API)
# ══════════════════════════════════════════════════════════════════════════════

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

ACS_VAR_GROUPS = {
    "income_pop": ["B19013_001E", "B01003_001E"],
    "poverty":    ["B17001_001E", "B17001_002E"],
    "education":  ["B15003_022E", "B15003_001E"],
    "rent":       ["B25064_001E"],
    "vacancy":    ["B25002_003E", "B25002_001E"],
}

OUTPUT_COLS = [
    "geoid", "total_population", "median_household_income", "median_gross_rent",
    "poverty_below", "poverty_universe_total",
    "bachelor_degree_count", "education_universe_total",
    "vacant_housing_units", "total_housing_units",
]


def _acs_request(year: int, api_key: str, var_codes: list[str]) -> pd.DataFrame | None:
    var_str = ",".join(["NAME"] + var_codes)
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={var_str}&for=tract:*&in=state:06%20county:075&key={api_key}"
    )
    resp = requests.get(url, timeout=30)
    if resp.status_code in (400, 404):
        return None
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    df["geoid"] = df["state"] + df["county"] + df["tract"]
    return df[["geoid"] + [c for c in var_codes if c in df.columns]]


def fetch_acs_sf_tracts(year: int, api_key: str) -> pd.DataFrame:
    df = _acs_request(year, api_key, list(ACS_VARIABLES.keys()))
    if df is None:
        print(f"  ACS {year}: Vollrequest fehlgeschlagen -> lade gruppenweise...")
        df = None
        for group_name, codes in ACS_VAR_GROUPS.items():
            part = _acs_request(year, api_key, codes)
            if part is None:
                print(f"    Gruppe '{group_name}' nicht verfuegbar -> NaN")
                continue
            df = part if df is None else df.merge(part, on="geoid", how="outer")
        if df is None:
            raise RuntimeError(f"ACS {year}: Keine Variablengruppe konnte geladen werden.")

    df.rename(columns=ACS_VARIABLES, inplace=True)
    for col in ACS_VARIABLES.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] < -999, col] = pd.NA
        else:
            df[col] = pd.NA

    print(f"  ACS {year}: {len(df)} Census Tracts")
    return df[[c for c in OUTPUT_COLS if c in df.columns]]


# ══════════════════════════════════════════════════════════════════════════════
# SFPD Crime Data (e3si-785i) - monatlich voraggregiert
# ══════════════════════════════════════════════════════════════════════════════

def fetch_crime_data(app_token: str = None) -> pd.DataFrame:
    base_url = "https://data.sfgov.org/resource/e3si-785i.json"
    headers = {"X-App-Token": app_token} if app_token else {}
    all_rows, offset, limit = [], 0, 50_000

    print("  Lade SFPD Crime-Daten von DataSF...")
    while True:
        params = {
            "$select": "by_month_incident_date,analysis_neighborhood,incident_category,count",
            "$where":  "analysis_neighborhood IS NOT NULL",
            "$limit":  limit,
            "$offset": offset,
            "$order":  ":id",
        }
        resp = requests.get(base_url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        offset += limit
        print(f"  {len(all_rows):>7,} Datensaetze geladen...", end="\r")
        time.sleep(0.3)

    print(f"\n  Fertig: {len(all_rows):,} Monats-Snapshots total.")
    df = pd.DataFrame(all_rows)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)
    # Roh-Schema persistieren - Aufbereitung erfolgt in 02_join_data.py
    return df[["by_month_incident_date", "analysis_neighborhood",
               "incident_category", "count"]]


# ══════════════════════════════════════════════════════════════════════════════
# Land Use 2020 (ygi5-84iq) - inkl. st_area_sh
# ══════════════════════════════════════════════════════════════════════════════

def fetch_land_use_2020(app_token: str = None) -> pd.DataFrame:
    base_url = "https://data.sfgov.org/resource/ygi5-84iq.json"
    headers = {"X-App-Token": app_token} if app_token else {}
    all_rows, offset, limit = [], 0, 50_000

    print("  Lade Land Use 2020 Parzellen von DataSF...")
    while True:
        params = {
            "$select": "the_geom,blklot,yrbuilt,landuse,resunits,st_area_sh",
            "$where":  "the_geom IS NOT NULL",
            "$limit":  limit,
            "$offset": offset,
            "$order":  "blklot ASC",
        }
        resp = requests.get(base_url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        offset += limit
        print(f"  {len(all_rows):>7,} Parzellen geladen...", end="\r")
        time.sleep(0.3)

    print(f"\n  Fertig: {len(all_rows):,} Parzellen total.")
    df = pd.DataFrame(all_rows)

    for col in ["yrbuilt", "resunits", "st_area_sh"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.loc[~df["yrbuilt"].between(1800, 2025), "yrbuilt"] = pd.NA
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Neighborhood Boundaries (j2bu-swwd) - 41 Analysis-Polygone
# ══════════════════════════════════════════════════════════════════════════════

def fetch_neighborhood_boundaries(app_token: str = None) -> str:
    """Gibt rohen GeoJSON-String zurueck."""
    headers = {"X-App-Token": app_token} if app_token else {}
    print("  Lade Neighborhood Boundaries (j2bu-swwd)...")
    resp = requests.get(
        "https://data.sfgov.org/resource/j2bu-swwd.geojson",
        params={"$limit": 100},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    print(f"  GeoJSON empfangen ({len(resp.text):,} Bytes).")
    return resp.text


# ══════════════════════════════════════════════════════════════════════════════
# HAUPTPIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_fetch():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("  Schritt 1: Daten einladen")
    print("=" * 80)
    print(f"  Download-Flags: SFFD={DOWNLOAD_SFFD}  CROSSWALK={DOWNLOAD_CROSSWALK}  "
          f"ACS={DOWNLOAD_ACS}  CRIME={DOWNLOAD_CRIME}  "
          f"LAND_USE={DOWNLOAD_LAND_USE_2020}  NEIGHBORHOODS={DOWNLOAD_NEIGHBORHOODS}")

    if DOWNLOAD_SFFD:
        print("\n[1/6] SFFD Fire Incidents...")
        df = fetch_sffd_incidents(app_token=DATASF_APP_TOKEN)
        path = RAW_DIR / "fire_incidents.parquet"
        df.to_parquet(path, index=False)
        print(f"  -> {path.relative_to(ROOT)}")
    else:
        print("\n[1/6] SFFD: uebersprungen (Cache wird in Schritt 2 verwendet)")

    if DOWNLOAD_CROSSWALK:
        print("\n[2/6] Neighborhood Crosswalk...")
        df = fetch_neighborhood_crosswalk(app_token=DATASF_APP_TOKEN)
        path = RAW_DIR / "crosswalk.csv"
        df.to_csv(path, index=False)
        print(f"  -> {path.relative_to(ROOT)}")
    else:
        print("\n[2/6] Crosswalk: uebersprungen")

    if DOWNLOAD_ACS:
        print(f"\n[3/6] ACS 5-Year - {len(ACS_YEARS)} Jahrgaenge...")
        for year in ACS_YEARS:
            df = fetch_acs_sf_tracts(year=year, api_key=CENSUS_API_KEY)
            path = RAW_DIR / f"acs_tracts_{year}.csv"
            df.to_csv(path, index=False)
            print(f"  -> {path.relative_to(ROOT)}")
    else:
        print("\n[3/6] ACS: uebersprungen")

    if DOWNLOAD_CRIME:
        print("\n[4/6] SFPD Crime Data...")
        df = fetch_crime_data(app_token=DATASF_APP_TOKEN)
        path = RAW_DIR / "crime_raw.parquet"
        df.to_parquet(path, index=False)
        print(f"  -> {path.relative_to(ROOT)}")
    else:
        print("\n[4/6] Crime: uebersprungen")

    if DOWNLOAD_LAND_USE_2020:
        print("\n[5/6] Land Use 2020...")
        df = fetch_land_use_2020(app_token=DATASF_APP_TOKEN)
        path = RAW_DIR / "land_use_2020_raw.parquet"
        df.to_parquet(path, index=False)
        print(f"  -> {path.relative_to(ROOT)}")
    else:
        print("\n[5/6] Land Use: uebersprungen")

    if DOWNLOAD_NEIGHBORHOODS:
        print("\n[6/6] Neighborhood Boundaries (GeoJSON)...")
        geojson_text = fetch_neighborhood_boundaries(app_token=DATASF_APP_TOKEN)
        path = RAW_DIR / "neighborhoods.geojson"
        path.write_text(geojson_text, encoding="utf-8")
        print(f"  -> {path.relative_to(ROOT)}")
    else:
        print("\n[6/6] Neighborhoods: uebersprungen")

    print("\n" + "=" * 80)
    print("  Fertig. Naechster Schritt: python scripts/02_join_data.py")
    print("=" * 80)


# ══════════════════════════════════════════════════════════════════════════════
# API-SCHNELLTEST
# ══════════════════════════════════════════════════════════════════════════════

def quick_test():
    print("=" * 80)
    print("  API-Verfuegbarkeitstest (kein Download)")
    print("=" * 80 + "\n")

    tests = [
        ("SFFD (wr8u-xric)", "https://data.sfgov.org/resource/wr8u-xric.json",
         {"$select": "incident_number,neighborhood_district", "$limit": 2,
          "$where": "neighborhood_district IS NOT NULL"}),
        ("Crosswalk (sevw-6tgi)", "https://data.sfgov.org/resource/sevw-6tgi.json",
         {"$select": "geoid,neighborhoods_analysis_boundaries", "$limit": 2}),
        ("SFPD Crime (e3si-785i)", "https://data.sfgov.org/resource/e3si-785i.json",
         {"$select": "by_month_incident_date,analysis_neighborhood,incident_category,count",
          "$limit": 2}),
        ("Land Use 2020 (ygi5-84iq)", "https://data.sfgov.org/resource/ygi5-84iq.json",
         {"$select": "blklot,yrbuilt,landuse,resunits,st_area_sh",
          "$where": "yrbuilt IS NOT NULL", "$limit": 2}),
        ("Neighborhoods (j2bu-swwd)", "https://data.sfgov.org/resource/j2bu-swwd.geojson",
         {"$limit": 2}),
    ]

    for name, url, params in tests:
        try:
            r = requests.get(url, params=params, timeout=15)
            status = "OK" if r.ok else f"FAIL {r.status_code}"
            if name.endswith("(j2bu-swwd)"):
                import json
                d = json.loads(r.text) if r.ok else {}
                n = len(d.get("features", []))
                fields = list(d["features"][0].get("properties", {}).keys()) if n else []
            else:
                rows = r.json() if r.ok else []
                n = len(rows)
                fields = list(rows[0].keys()) if rows else []
            print(f"  {name:<32} {status}  ({n} Rows)")
            if fields:
                print(f"      Felder: {fields}")
        except Exception as e:
            print(f"  {name:<32} EXCEPTION {e}")

    print()
    for year in ACS_YEARS:
        try:
            r = requests.get(
                f"https://api.census.gov/data/{year}/acs/acs5"
                "?get=NAME,B19013_001E&for=tract:*&in=state:06%20county:075",
                timeout=15,
            )
            n = len(r.json()) - 1 if r.ok else 0
            print(f"  ACS Census {year:<8} {'OK' if r.ok else f'FAIL {r.status_code}'}  ({n} Tracts)")
        except Exception as e:
            print(f"  ACS Census {year:<8} EXCEPTION {e}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        quick_test()
    else:
        run_fetch()
