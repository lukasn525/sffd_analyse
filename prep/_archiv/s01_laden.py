"""
Schritt 1 der Aufbereitung: Rohdaten von DataSF und der Census API laden.

Ausgabe: data/raw/*

Gesteuert wird ausschliesslich ueber die DOWNLOAD_*-Schalter in prep/config.py.
Stehen dort alle auf False (Default), laeuft dieser Schritt folgenlos durch -
`python prep/build.py` arbeitet dann allein aus dem vorhandenen data/raw und
braucht weder Internet noch API-Key.

Ausfuehren:
  python prep/s01_laden.py          # laedt, was in config.py auf True steht
  python prep/s01_laden.py test     # nur Erreichbarkeit der Quellen pruefen
"""
import json
import sys
import time
import warnings

import pandas as pd
import requests

from config import (ACS_VARIABLES, ACS_YEARS, CENSUS_API_KEY,
                    CRIME_HISTORISCH_AB, CRIME_HISTORISCH_BIS,
                    DATASF_APP_TOKEN, DOWNLOAD_ACS, DOWNLOAD_CRIME,
                    DOWNLOAD_CRIME_HISTORISCH, DOWNLOAD_CROSSWALK,
                    DOWNLOAD_LAND_USE, DOWNLOAD_NEIGHBORHOODS, DOWNLOAD_SFFD,
                    RAW_DIR, ROOT)

warnings.filterwarnings("ignore")

ACS_OUTPUT_COLS = ["geoid"] + list(ACS_VARIABLES.values())


def _paginiere_datasf(url: str, base_params: dict, beschreibung: str,
                      limit: int = 50_000) -> list[dict]:
    headers = {"X-App-Token": DATASF_APP_TOKEN} if DATASF_APP_TOKEN else {}
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


# --------------------------------------------------------------------------
# Einzelne Quellen
# --------------------------------------------------------------------------
def fetch_sffd_incidents() -> pd.DataFrame:
    """SFFD Fire Incidents (wr8u-xric), ~720.000 Einsaetze ab 2003."""
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
        "SFFD-Daten von DataSF")
    df = pd.DataFrame(rows)
    for col in ["alarm_dttm", "arrival_dttm", "incident_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["suppression_units", "suppression_personnel", "ems_units",
                "number_of_alarms", "civilian_fatalities", "civilian_injuries",
                "estimated_property_loss"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def fetch_neighborhood_crosswalk() -> pd.DataFrame:
    """Census-Tract <-> Neighborhood (sevw-6tgi)."""
    headers = {"X-App-Token": DATASF_APP_TOKEN} if DATASF_APP_TOKEN else {}
    resp = requests.get(
        "https://data.sfgov.org/resource/sevw-6tgi.json",
        params={"$select": "geoid,neighborhoods_analysis_boundaries", "$limit": 300},
        headers=headers, timeout=30)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    df.columns = ["geoid", "neighborhood"]
    df["geoid"]        = df["geoid"].astype(str).str.zfill(11)
    df["neighborhood"] = df["neighborhood"].str.strip().str.title()
    print(f"  {len(df)} Tract-Neighborhood-Paare, "
          f"{df['neighborhood'].nunique()} Neighborhoods")
    return df


def fetch_acs_sf_tracts(year: int) -> pd.DataFrame:
    """ACS 5-Year Estimates auf Tract-Ebene fuer San Francisco County."""
    var_codes = list(ACS_VARIABLES.keys())
    var_str = ",".join(["NAME"] + var_codes)
    url = (f"https://api.census.gov/data/{year}/acs/acs5"
           f"?get={var_str}&for=tract:*&in=state:06%20county:075&key={CENSUS_API_KEY}")
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


def fetch_crime_modern() -> pd.DataFrame:
    """SFPD Incident Reports ab 2018-01 (e3si-785i), monatlich voraggregiert."""
    rows = _paginiere_datasf(
        "https://data.sfgov.org/resource/e3si-785i.json",
        {"$select": "by_month_incident_date,analysis_neighborhood,incident_category,count",
         "$where":  "analysis_neighborhood IS NOT NULL",
         "$order":  ":id"},
        "SFPD Crime-Daten von DataSF")
    df = pd.DataFrame(rows)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)
    return df[["by_month_incident_date", "analysis_neighborhood",
               "incident_category", "count"]]


def fetch_crime_historisch() -> pd.DataFrame:
    """SFPD Incident Reports 2003 - Mai 2018 (tmnf-yvry), gefiltert.

    Warum ein zweiter Crime-Datensatz? Der aktuelle (e3si-785i) beginnt erst
    2018-01, der Analysezeitraum aber 2015-01. Ohne diese Quelle gaebe es fuer
    2015-2017 keine Kriminalitaetswerte.

    Der historische Datensatz enthaelt KEINE Stadtteilspalte, wohl aber
    Koordinaten -> Zuordnung in s02_einsaetze.py per Spatial Join gegen dieselbe
    Neighborhood-Geometrie wie bei den Land-Use-Daten.

    Limitation fuer Kap. 6: Im Mai 2018 hat SFPD von CABLE auf das Crime Data
    Warehouse umgestellt. Absolute Fallzahlen sind ueber diesen Bruch hinweg
    nicht vergleichbar - genau deshalb bildet s02_einsaetze.py einen RELATIVEN Index.
    """
    rows = _paginiere_datasf(
        "https://data.sfgov.org/resource/tmnf-yvry.json",
        {"$select": "date,x,y",
         "$where":  f"date >= '{CRIME_HISTORISCH_AB}' "
                    f"AND date < '{CRIME_HISTORISCH_BIS}' "
                    f"AND x IS NOT NULL AND y IS NOT NULL",
         "$order":  ":id"},
        f"SFPD Crime historisch ({CRIME_HISTORISCH_AB[:4]}-2017)")
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in ["x", "y"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "x", "y"])
    # SF liegt etwa bei -122.5..-122.3 / 37.7..37.85; grobe Plausibilitaets-
    # grenzen entfernen die bekannten (0,90)-Platzhalter der Quelldaten.
    df = df[df["x"].between(-123.2, -122.2) & df["y"].between(37.6, 37.95)]
    print(f"  {len(df):,} Delikte mit plausiblen Koordinaten "
          f"({df['date'].min():%Y-%m} bis {df['date'].max():%Y-%m})")
    return df[["date", "x", "y"]]


def fetch_land_use() -> pd.DataFrame:
    """Land Use 2020 (ygi5-84iq), Parzellen mit Geometrie."""
    rows = _paginiere_datasf(
        "https://data.sfgov.org/resource/ygi5-84iq.json",
        {"$select": "the_geom,blklot,yrbuilt,landuse,resunits,st_area_sh",
         "$where":  "the_geom IS NOT NULL",
         "$order":  "blklot ASC"},
        "Land Use 2020 Parzellen von DataSF")
    df = pd.DataFrame(rows)
    for col in ["yrbuilt", "resunits", "st_area_sh"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.loc[~df["yrbuilt"].between(1800, 2025), "yrbuilt"] = pd.NA
    return df


def fetch_neighborhood_boundaries() -> str:
    """Neighborhood-Grenzen als GeoJSON (j2bu-swwd) fuer die Spatial Joins."""
    headers = {"X-App-Token": DATASF_APP_TOKEN} if DATASF_APP_TOKEN else {}
    resp = requests.get("https://data.sfgov.org/resource/j2bu-swwd.geojson",
                        params={"$limit": 100}, headers=headers, timeout=30)
    resp.raise_for_status()
    print(f"  GeoJSON empfangen ({len(resp.text):,} Bytes).")
    return resp.text


# --------------------------------------------------------------------------
# Ablauf
# --------------------------------------------------------------------------
def run_download() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    aktiv = {
        "SFFD":          DOWNLOAD_SFFD,
        "CROSSWALK":     DOWNLOAD_CROSSWALK,
        "ACS":           DOWNLOAD_ACS,
        "CRIME":         DOWNLOAD_CRIME,
        "CRIME_HIST":    DOWNLOAD_CRIME_HISTORISCH,
        "LAND_USE":      DOWNLOAD_LAND_USE,
        "NEIGHBORHOODS": DOWNLOAD_NEIGHBORHOODS,
    }
    if not any(aktiv.values()):
        print("  Alle DOWNLOAD_*-Schalter in config.py stehen auf False "
              "-> nutze vorhandene Rohdaten in data/raw.")
        return

    print("  Aktive Downloads: "
          + ", ".join(k for k, v in aktiv.items() if v) + "\n")

    if DOWNLOAD_SFFD:
        print("  SFFD Fire Incidents...")
        fetch_sffd_incidents().to_parquet(RAW_DIR / "fire_incidents.parquet",
                                          index=False)
    if DOWNLOAD_CROSSWALK:
        print("  Neighborhood Crosswalk...")
        fetch_neighborhood_crosswalk().to_csv(RAW_DIR / "crosswalk.csv",
                                              index=False)
    if DOWNLOAD_ACS:
        print(f"  ACS 5-Year, {len(ACS_YEARS)} Jahrgaenge...")
        for year in ACS_YEARS:
            fetch_acs_sf_tracts(year).to_csv(
                RAW_DIR / f"acs_tracts_{year}.csv", index=False)
    if DOWNLOAD_CRIME:
        print("  SFPD Crime ab 2018...")
        fetch_crime_modern().to_parquet(RAW_DIR / "crime_raw.parquet",
                                        index=False)
    if DOWNLOAD_CRIME_HISTORISCH:
        print("  SFPD Crime historisch...")
        fetch_crime_historisch().to_parquet(
            RAW_DIR / "crime_historisch_raw.parquet", index=False)
    if DOWNLOAD_LAND_USE:
        print("  Land Use 2020...")
        fetch_land_use().to_parquet(RAW_DIR / "land_use_2020_raw.parquet",
                                    index=False)
    if DOWNLOAD_NEIGHBORHOODS:
        print("  Neighborhood Boundaries...")
        (RAW_DIR / "neighborhoods.geojson").write_text(
            fetch_neighborhood_boundaries(), encoding="utf-8")

    # Nach einem Crime- oder ACS-Neu-Download ist der Index-Cache ungueltig.
    if DOWNLOAD_CRIME or DOWNLOAD_CRIME_HISTORISCH or DOWNLOAD_ACS:
        cache = ROOT / "data" / "processed" / "crime_index_monatlich.csv"
        if cache.exists():
            cache.unlink()
            print("  Cache crime_index_monatlich.csv verworfen "
                  "(wird in s02_einsaetze.py neu berechnet).")


def quick_test() -> None:
    """Erreichbarkeit aller Quellen pruefen, ohne etwas zu laden."""
    print("API-Verfuegbarkeitstest (kein Download)\n")
    endpoints = [
        ("SFFD", "https://data.sfgov.org/resource/wr8u-xric.json",
         {"$select": "incident_number,neighborhood_district", "$limit": 2,
          "$where": "neighborhood_district IS NOT NULL"}),
        ("Crosswalk", "https://data.sfgov.org/resource/sevw-6tgi.json",
         {"$select": "geoid,neighborhoods_analysis_boundaries", "$limit": 2}),
        ("SFPD Crime", "https://data.sfgov.org/resource/e3si-785i.json",
         {"$select": "by_month_incident_date,analysis_neighborhood,incident_category,count",
          "$limit": 2}),
        ("SFPD hist.", "https://data.sfgov.org/resource/tmnf-yvry.json",
         {"$select": "date,x,y",
          "$where": f"date >= '{CRIME_HISTORISCH_AB}' AND x IS NOT NULL",
          "$limit": 2}),
        ("Land Use", "https://data.sfgov.org/resource/ygi5-84iq.json",
         {"$select": "blklot,yrbuilt,landuse,resunits,st_area_sh",
          "$where": "yrbuilt IS NOT NULL", "$limit": 2}),
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
        run_download()
