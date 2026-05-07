"""
Schritt 1: Rohdaten von DataSF und Census API laden -> data/raw/

Ausfuehren:
  python pipeline/01_fetch.py          # vollstaendiger Download
  python pipeline/01_fetch.py test     # nur API-Verfuegbarkeit testen
"""
import json
import sys
import time
import warnings
from pathlib import Path

import pandas as pd
import requests

warnings.filterwarnings("ignore")

CENSUS_API_KEY   = "f5cb8b553da8a01e351b3804e56e7fe664e12c98"
DATASF_APP_TOKEN = None
ACS_YEARS        = [2009, 2014, 2019, 2021, 2023]

DOWNLOAD_SFFD          = False
DOWNLOAD_CROSSWALK     = False
DOWNLOAD_ACS           = False
DOWNLOAD_CRIME         = False
DOWNLOAD_LAND_USE_2020 = False
DOWNLOAD_NEIGHBORHOODS = False

ROOT    = Path(__file__).parent.parent
RAW_DIR = ROOT / "data" / "raw"

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

ACS_OUTPUT_COLS = ["geoid"] + list(ACS_VARIABLES.values())


def _paginiere_datasf(url: str, base_params: dict, app_token: str | None,
                       beschreibung: str, limit: int = 50_000) -> list[dict]:
    headers = {"X-App-Token": app_token} if app_token else {}
    rows, offset = [], 0
    print(f"  Lade {beschreibung}...")
    while True:
        params = {**base_params, "$limit": limit, "$offset": offset}
        resp = requests.get(url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        offset += limit
        print(f"  {len(rows):>7,} Eintraege geladen...", end="\r")
        time.sleep(0.3)
    print(f"\n  Fertig: {len(rows):,} Eintraege total.")
    return rows


def fetch_sffd_incidents(app_token: str | None = None) -> pd.DataFrame:
    fields = ",".join([
        "incident_number", "incident_date", "alarm_dttm", "arrival_dttm",
        "neighborhood_district", "battalion", "primary_situation",
        "suppression_units", "suppression_personnel", "ems_units",
        "number_of_alarms", "civilian_fatalities", "civilian_injuries",
        "estimated_property_loss", "no_flame_spread",
    ])
    rows = _paginiere_datasf(
        "https://data.sfgov.org/resource/wr8u-xric.json",
        {"$select": fields,
         "$where":  "neighborhood_district IS NOT NULL AND arrival_dttm IS NOT NULL",
         "$order":  ":id"},
        app_token, "SFFD-Daten von DataSF",
    )
    df = pd.DataFrame(rows)
    for col in ["alarm_dttm", "arrival_dttm", "incident_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["suppression_units", "suppression_personnel", "ems_units",
                "number_of_alarms", "civilian_fatalities", "civilian_injuries",
                "estimated_property_loss"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def fetch_neighborhood_crosswalk(app_token: str | None = None) -> pd.DataFrame:
    headers = {"X-App-Token": app_token} if app_token else {}
    resp = requests.get(
        "https://data.sfgov.org/resource/sevw-6tgi.json",
        params={"$select": "geoid,neighborhoods_analysis_boundaries", "$limit": 300},
        headers=headers, timeout=30,
    )
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    df.columns = ["geoid", "neighborhood"]
    df["geoid"]        = df["geoid"].astype(str).str.zfill(11)
    df["neighborhood"] = df["neighborhood"].str.strip().str.title()
    print(f"  {len(df)} Tract-Neighborhood-Paare, {df['neighborhood'].nunique()} Neighborhoods")
    return df


def fetch_acs_sf_tracts(year: int, api_key: str) -> pd.DataFrame:
    var_codes = list(ACS_VARIABLES.keys())
    var_str = ",".join(["NAME"] + var_codes)
    url = (f"https://api.census.gov/data/{year}/acs/acs5"
           f"?get={var_str}&for=tract:*&in=state:06%20county:075&key={api_key}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    df["geoid"] = df["state"] + df["county"] + df["tract"]
    df = df[["geoid"] + var_codes].rename(columns=ACS_VARIABLES)
    for col in ACS_VARIABLES.values():
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col] < -999, col] = pd.NA
    print(f"  ACS {year}: {len(df)} Census Tracts")
    return df[ACS_OUTPUT_COLS]


def fetch_crime_data(app_token: str | None = None) -> pd.DataFrame:
    rows = _paginiere_datasf(
        "https://data.sfgov.org/resource/e3si-785i.json",
        {"$select": "by_month_incident_date,analysis_neighborhood,incident_category,count",
         "$where":  "analysis_neighborhood IS NOT NULL",
         "$order":  ":id"},
        app_token, "SFPD Crime-Daten von DataSF",
    )
    df = pd.DataFrame(rows)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)
    return df[["by_month_incident_date", "analysis_neighborhood",
               "incident_category", "count"]]


def fetch_land_use_2020(app_token: str | None = None) -> pd.DataFrame:
    rows = _paginiere_datasf(
        "https://data.sfgov.org/resource/ygi5-84iq.json",
        {"$select": "the_geom,blklot,yrbuilt,landuse,resunits,st_area_sh",
         "$where":  "the_geom IS NOT NULL",
         "$order":  "blklot ASC"},
        app_token, "Land Use 2020 Parzellen von DataSF",
    )
    df = pd.DataFrame(rows)
    for col in ["yrbuilt", "resunits", "st_area_sh"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.loc[~df["yrbuilt"].between(1800, 2025), "yrbuilt"] = pd.NA
    return df


def fetch_neighborhood_boundaries(app_token: str | None = None) -> str:
    headers = {"X-App-Token": app_token} if app_token else {}
    resp = requests.get(
        "https://data.sfgov.org/resource/j2bu-swwd.geojson",
        params={"$limit": 100}, headers=headers, timeout=30,
    )
    resp.raise_for_status()
    print(f"  GeoJSON empfangen ({len(resp.text):,} Bytes).")
    return resp.text


def run_fetch():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print("Schritt 1: Daten einladen")
    print(f"Flags: SFFD={DOWNLOAD_SFFD} CROSSWALK={DOWNLOAD_CROSSWALK} "
          f"ACS={DOWNLOAD_ACS} CRIME={DOWNLOAD_CRIME} "
          f"LAND_USE={DOWNLOAD_LAND_USE_2020} NEIGHBORHOODS={DOWNLOAD_NEIGHBORHOODS}\n")

    if DOWNLOAD_SFFD:
        print("[1/6] SFFD Fire Incidents...")
        fetch_sffd_incidents(DATASF_APP_TOKEN).to_parquet(
            RAW_DIR / "fire_incidents.parquet", index=False)
    if DOWNLOAD_CROSSWALK:
        print("[2/6] Neighborhood Crosswalk...")
        fetch_neighborhood_crosswalk(DATASF_APP_TOKEN).to_csv(
            RAW_DIR / "crosswalk.csv", index=False)
    if DOWNLOAD_ACS:
        print(f"[3/6] ACS 5-Year - {len(ACS_YEARS)} Jahrgaenge...")
        for year in ACS_YEARS:
            fetch_acs_sf_tracts(year, CENSUS_API_KEY).to_csv(
                RAW_DIR / f"acs_tracts_{year}.csv", index=False)
    if DOWNLOAD_CRIME:
        print("[4/6] SFPD Crime Data...")
        fetch_crime_data(DATASF_APP_TOKEN).to_parquet(
            RAW_DIR / "crime_raw.parquet", index=False)
    if DOWNLOAD_LAND_USE_2020:
        print("[5/6] Land Use 2020...")
        fetch_land_use_2020(DATASF_APP_TOKEN).to_parquet(
            RAW_DIR / "land_use_2020_raw.parquet", index=False)
    if DOWNLOAD_NEIGHBORHOODS:
        print("[6/6] Neighborhood Boundaries...")
        (RAW_DIR / "neighborhoods.geojson").write_text(
            fetch_neighborhood_boundaries(DATASF_APP_TOKEN), encoding="utf-8")

    print("\nFertig. Naechster Schritt: python pipeline/02_join.py")


def quick_test():
    print("API-Verfuegbarkeitstest (kein Download)\n")
    endpoints = [
        ("SFFD",         "https://data.sfgov.org/resource/wr8u-xric.json",
         {"$select": "incident_number,neighborhood_district", "$limit": 2,
          "$where":  "neighborhood_district IS NOT NULL"}),
        ("Crosswalk",    "https://data.sfgov.org/resource/sevw-6tgi.json",
         {"$select": "geoid,neighborhoods_analysis_boundaries", "$limit": 2}),
        ("SFPD Crime",   "https://data.sfgov.org/resource/e3si-785i.json",
         {"$select": "by_month_incident_date,analysis_neighborhood,incident_category,count",
          "$limit": 2}),
        ("Land Use",     "https://data.sfgov.org/resource/ygi5-84iq.json",
         {"$select": "blklot,yrbuilt,landuse,resunits,st_area_sh",
          "$where":  "yrbuilt IS NOT NULL", "$limit": 2}),
        ("Neighborhoods", "https://data.sfgov.org/resource/j2bu-swwd.geojson",
         {"$limit": 2}),
    ]
    for name, url, params in endpoints:
        r = requests.get(url, params=params, timeout=15)
        if name == "Neighborhoods":
            n = len(json.loads(r.text).get("features", [])) if r.ok else 0
        else:
            n = len(r.json()) if r.ok else 0
        print(f"  {name:<14} {'OK' if r.ok else f'FAIL {r.status_code}'}  ({n} Rows)")

    for year in ACS_YEARS:
        r = requests.get(
            f"https://api.census.gov/data/{year}/acs/acs5"
            "?get=NAME,B19013_001E&for=tract:*&in=state:06%20county:075",
            timeout=15)
        n = len(r.json()) - 1 if r.ok else 0
        print(f"  ACS {year:<10} {'OK' if r.ok else f'FAIL {r.status_code}'}  ({n} Tracts)")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        quick_test()
    else:
        run_fetch()
