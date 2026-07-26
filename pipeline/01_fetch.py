"""
Schritt 1: Rohdaten von DataSF und Census API laden -> data/raw/

Ausfuehren:
  python pipeline/01_fetch.py test           # nur API-Verfuegbarkeit pruefen
  python pipeline/01_fetch.py crime          # nur die Kriminalitaetsdaten
  python pipeline/01_fetch.py sffd acs       # gezielt einzelne Quellen
  python pipeline/01_fetch.py alle           # alles neu herunterladen

Ohne Argument gelten die DOWNLOAD_*-Schalter unten (Default: alles False,
damit ein versehentlicher Lauf nichts ueberschreibt).

Verfuegbare Namen: sffd, crosswalk, acs, crime, crime_hist, landuse,
neighborhoods, crime_alle (= crime + crime_hist), alle
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

DOWNLOAD_SFFD             = False
DOWNLOAD_CROSSWALK        = False
DOWNLOAD_ACS              = False
DOWNLOAD_CRIME            = False   # SFPD ab 2018 (e3si-785i), MIT Datumsspalte
DOWNLOAD_CRIME_HISTORISCH = False   # SFPD 2014-2017 (tmnf-yvry), fuer den Index
DOWNLOAD_LAND_USE_2020    = False
DOWNLOAD_NEIGHBORHOODS    = False

# Startjahr der historischen Crime-Daten. Der Kriminalitaetsindex nutzt ein
# rollierendes 12-Monats-Fenster, das im VORMONAT endet; fuer den ersten
# Analysemonat 2015-01 werden daher die Monate 2014-01 bis 2014-12 benoetigt.
CRIME_HISTORISCH_AB = "2014-01-01"
# Der historische SFPD-Datensatz endet im Mai 2018, der moderne beginnt im
# Januar 2018. Sauberer Kalenderschnitt ohne Ueberlappung:
CRIME_HISTORISCH_BIS = "2018-01-01"   # exklusiv

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


def fetch_crime_historisch(app_token: str | None = None) -> pd.DataFrame:
    """SFPD Incident Reports 2003 - Mai 2018 (tmnf-yvry), gefiltert ab 2014.

    Warum ein zweiter Crime-Datensatz? Der aktuelle Datensatz (e3si-785i)
    beginnt erst 2018-01. Der Analysezeitraum der Arbeit ist 2015-01 bis
    2025-12; der Kriminalitaetsindex braucht wegen des rollierenden
    12-Monats-Fensters zusaetzlich das Jahr 2014. Ohne diesen Datensatz gaebe
    es fuer 2015-2017 keine Kriminalitaetswerte.

    Der historische Datensatz enthaelt KEINE Stadtteilspalte, wohl aber
    Koordinaten -> die Zuordnung erfolgt in 02_join.py per Spatial Join gegen
    dieselbe Neighborhood-Geometrie wie bei den Land-Use-Daten (identische
    Gebietsdefinition, damit beide Quellen vergleichbar bleiben).

    WICHTIG (Limitation fuer Kap. 6): Im Mai 2018 hat SFPD von der
    Alt-Anwendung CABLE auf das Crime Data Warehouse umgestellt. Absolute
    Fallzahlen sind ueber diesen Bruch hinweg nicht direkt vergleichbar -
    genau deshalb wird in 02_join.py ein RELATIVER Index (Stadtteil gegen
    Stadtdurchschnitt desselben Monats) gebildet und nicht die Rohzahl.
    """
    rows = _paginiere_datasf(
        "https://data.sfgov.org/resource/tmnf-yvry.json",
        {"$select": "date,x,y",
         "$where":  f"date >= '{CRIME_HISTORISCH_AB}' "
                    f"AND date < '{CRIME_HISTORISCH_BIS}' "
                    f"AND x IS NOT NULL AND y IS NOT NULL",
         "$order":  ":id"},
        app_token, f"SFPD Crime historisch ({CRIME_HISTORISCH_AB[:4]}-2017)",
    )
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
          f"CRIME_HIST={DOWNLOAD_CRIME_HISTORISCH} "
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
        print("[4/6] SFPD Crime Data (ab 2018)...")
        fetch_crime_data(DATASF_APP_TOKEN).to_parquet(
            RAW_DIR / "crime_raw.parquet", index=False)
    if DOWNLOAD_CRIME_HISTORISCH:
        print("[4b/6] SFPD Crime Data (historisch 2014-2017)...")
        fetch_crime_historisch(DATASF_APP_TOKEN).to_parquet(
            RAW_DIR / "crime_historisch_raw.parquet", index=False)
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
        ("SFPD hist.",   "https://data.sfgov.org/resource/tmnf-yvry.json",
         {"$select": "date,x,y",
          "$where":  f"date >= '{CRIME_HISTORISCH_AB}' AND x IS NOT NULL",
          "$limit": 2}),
        ("Land Use",     "https://data.sfgov.org/resource/ygi5-84iq.json",
         {"$select": "blklot,yrbuilt,landuse,resunits,st_area_sh",
          "$where":  "yrbuilt IS NOT NULL", "$limit": 2}),
        ("Neighborhoods", "https://data.sfgov.org/resource/j2bu-swwd.geojson",
         {"$limit": 2}),
    ]
    def _melde(name: str, r, zaehler) -> None:
        """Ein Endpunkt darf den ganzen Test nicht abbrechen."""
        if not r.ok:
            print(f"  {name:<14} FAIL {r.status_code}")
            return
        try:
            print(f"  {name:<14} OK  ({zaehler(r)} Rows)")
        except Exception:
            # Antwort kam an, ist aber kein JSON (z. B. Census-Rate-Limit oder
            # HTML-Fehlerseite trotz Status 200).
            print(f"  {name:<14} WARNUNG: Antwort ist kein JSON "
                  f"(erste 60 Zeichen: {r.text[:60]!r})")

    for name, url, params in endpoints:
        try:
            r = requests.get(url, params=params, timeout=15)
        except requests.RequestException as e:
            print(f"  {name:<14} FEHLER: {type(e).__name__}")
            continue
        if name == "Neighborhoods":
            _melde(name, r, lambda x: len(json.loads(x.text).get("features", [])))
        else:
            _melde(name, r, lambda x: len(x.json()))

    for year in ACS_YEARS:
        # MIT API-Key testen - ohne Key antwortet die Census-API bei haeufigen
        # Abfragen mit einer HTML-Fehlerseite statt JSON.
        try:
            r = requests.get(
                f"https://api.census.gov/data/{year}/acs/acs5"
                f"?get=NAME,B19013_001E&for=tract:*&in=state:06%20county:075"
                f"&key={CENSUS_API_KEY}", timeout=15)
        except requests.RequestException as e:
            print(f"  ACS {year:<10} FEHLER: {type(e).__name__}")
            continue
        _melde(f"ACS {year}", r, lambda x: len(x.json()) - 1)


ARG_ZU_FLAG = {
    "sffd":          ["DOWNLOAD_SFFD"],
    "crosswalk":     ["DOWNLOAD_CROSSWALK"],
    "acs":           ["DOWNLOAD_ACS"],
    "crime":         ["DOWNLOAD_CRIME", "DOWNLOAD_CRIME_HISTORISCH"],
    "crime_neu":     ["DOWNLOAD_CRIME"],
    "crime_hist":    ["DOWNLOAD_CRIME_HISTORISCH"],
    "landuse":       ["DOWNLOAD_LAND_USE_2020"],
    "neighborhoods": ["DOWNLOAD_NEIGHBORHOODS"],
}


def _flags_aus_argumenten(argumente: list[str]) -> None:
    """Setzt die DOWNLOAD_*-Schalter anhand der Kommandozeile.

    Erspart das Editieren der Datei vor und nach jedem Lauf - der haeufigste
    Weg, versehentlich einen Download stehen zu lassen oder zu vergessen.
    """
    global_ = globals()
    if "alle" in argumente:
        for flags in ARG_ZU_FLAG.values():
            for f in flags:
                global_[f] = True
        return
    unbekannt = [a for a in argumente if a not in ARG_ZU_FLAG]
    if unbekannt:
        raise SystemExit(f"Unbekanntes Argument: {', '.join(unbekannt)}\n"
                         f"Erlaubt: {', '.join(ARG_ZU_FLAG)}, alle, test")
    for a in argumente:
        for f in ARG_ZU_FLAG[a]:
            global_[f] = True


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "test":
        quick_test()
    else:
        if args:
            _flags_aus_argumenten(args)
        run_fetch()
