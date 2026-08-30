"""
Schritt 1: Rohdaten beschaffen und auf Einsatzebene zusammenfuehren.

Eingang:  DataSF- und Census-APIs (nur wenn DOWNLOAD_* in config.py True ist)
          data/raw/*
Ausgang:  data/processed/einsaetze.parquet   (ein Einsatz je Zeile, ~720.000)

Vier Quellen: ACS (soziooekonomisch, je Stadtteil x Jahrgang, mit
Publikationsversatz), Crime (relativer Index je Stadtteil x Monat), Land Use
(baulich, Snapshot 2020) und die Neighborhood-Geometrie fuer beide Spatial
Joins. Die Ebene bleibt der EINZELEINSATZ - aggregiert wird erst in
s2_datensaetze.py.

Ausfuehren:
  python prep/s1_daten.py          # Download (soweit aktiviert) + Join
  python prep/s1_daten.py join     # nur Join
"""
import sys
import time
import warnings

import numpy as np
import pandas as pd
import requests

from config import (ACS_PUBLIKATIONS_LAG, ACS_VARIABLES, ACS_YEARS,
                    ANTWORTZEIT_MAX, ANTWORTZEIT_MIN, CENSUS_API_KEY,
                    CRIME_FENSTER_MONATE, CRIME_HISTORISCH_AB,
                    CRIME_HISTORISCH_BIS, DATASF_APP_TOKEN, DOWNLOAD_ACS,
                    DOWNLOAD_CRIME, DOWNLOAD_CRIME_HISTORISCH,
                    DOWNLOAD_CROSSWALK, DOWNLOAD_LAND_USE,
                    DOWNLOAD_NEIGHBORHOODS, DOWNLOAD_SFFD,
                    HIGH_RISK_COMMERCIAL, PFAD_EINSAETZE, PROCESSED_DIR,
                    RAW_DIR, RESIDENTIAL, ROOT, spalten_deutsch)

warnings.filterwarnings("ignore")

# Caches: teure, deterministische Zwischenergebnisse. Zum Neuberechnen die CSV
# loeschen; nach einem Crime- oder ACS-Neu-Download geschieht das automatisch.
CACHE_CRIME    = PROCESSED_DIR / "crime_index_monatlich.csv"
CACHE_LAND_USE = PROCESSED_DIR / "land_use_2020_neighborhoods.csv"

ACS_NUM_COLS = [
    "total_population", "median_household_income", "median_gross_rent",
    "poverty_below", "poverty_universe_total", "bachelor_degree_count",
    "education_universe_total", "vacant_housing_units", "total_housing_units",
]
ACS_SUMMEN = ACS_NUM_COLS[3:]        # Zaehler: summiert
ACS_GEWICHTET = ACS_NUM_COLS[1:3]    # Mediane: bevoelkerungsgewichtet

# Zaehler-Spalten, die als ganze Zahl gespeichert werden.
INT64_COLS = ACS_SUMMEN + ["total_population", "total_resunits"]

# Rohdateien und der Schalter, der sie erzeugt - fuer die Fehlermeldung.
BENOETIGT = {
    "fire_incidents.parquet":      "DOWNLOAD_SFFD",
    "crosswalk.csv":               "DOWNLOAD_CROSSWALK",
    "crime_raw.parquet":           "DOWNLOAD_CRIME",
    "crime_historisch_raw.parquet": "DOWNLOAD_CRIME_HISTORISCH",
    "land_use_2020_raw.parquet":   "DOWNLOAD_LAND_USE",
    "neighborhoods.geojson":       "DOWNLOAD_NEIGHBORHOODS",
}


# ==========================================================================
# TEIL A  DOWNLOAD
# ==========================================================================
# Gesteuert allein ueber die DOWNLOAD_*-Schalter in config.py. Stehen alle auf
# False (Default), arbeitet die Aufbereitung aus data/raw und braucht weder
# Internet noch API-Key.
# ==========================================================================
# Deklarative Quellenliste. Jede DataSF-Quelle unterscheidet sich nur in
# Ressourcen-ID, Feldauswahl, Filter und Nachbearbeitung der Spaltentypen -
# deshalb eine Tabelle und eine Schleife statt sechs fast gleicher Funktionen.
SFFD_ZAHLEN = ["suppression_units", "suppression_personnel", "ems_units",
               "number_of_alarms", "civilian_fatalities", "civilian_injuries",
               "estimated_property_loss"]

QUELLEN = {
    # Schalter: (Ressource, Felder, Filter, Sortierung, Zieldatei,
    #            Datumsspalten, Zahlenspalten)
    "SFFD": (
        DOWNLOAD_SFFD, "wr8u-xric",
        ",".join(["incident_number", "incident_date", "alarm_dttm",
                  "arrival_dttm", "neighborhood_district", "battalion",
                  "primary_situation", "no_flame_spread"] + SFFD_ZAHLEN),
        "neighborhood_district IS NOT NULL AND arrival_dttm IS NOT NULL",
        ":id", "fire_incidents.parquet",
        ["alarm_dttm", "arrival_dttm", "incident_date"], SFFD_ZAHLEN),
    "CROSSWALK": (
        DOWNLOAD_CROSSWALK, "sevw-6tgi",
        "geoid,neighborhoods_analysis_boundaries", "", ":id",
        "crosswalk.csv", [], []),
    "CRIME": (
        DOWNLOAD_CRIME, "e3si-785i",
        "by_month_incident_date,analysis_neighborhood,incident_category,count",
        "analysis_neighborhood IS NOT NULL", ":id", "crime_raw.parquet",
        [], ["count"]),
    "CRIME_HIST": (
        DOWNLOAD_CRIME_HISTORISCH, "tmnf-yvry", "date,x,y",
        f"date >= '{CRIME_HISTORISCH_AB}' AND date < '{CRIME_HISTORISCH_BIS}' "
        f"AND x IS NOT NULL AND y IS NOT NULL",
        ":id", "crime_historisch_raw.parquet", ["date"], ["x", "y"]),
    "LAND_USE": (
        DOWNLOAD_LAND_USE, "ygi5-84iq",
        "the_geom,blklot,yrbuilt,landuse,resunits,st_area_sh",
        "the_geom IS NOT NULL", "blklot ASC", "land_use_2020_raw.parquet",
        [], ["yrbuilt", "resunits", "st_area_sh"]),
}


def _get(url: str, params: dict) -> requests.Response:
    """Ruft eine URL mit Wiederholversuchen ab.

    Ein:  URL, optionale Parameter
    Aus:  Antwort der Anfrage
    """
    headers = {"X-App-Token": DATASF_APP_TOKEN} if DATASF_APP_TOKEN else {}
    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r


def lade_datasf(name: str, limit: int = 50_000) -> pd.DataFrame:
    """Holt eine DataSF-Quelle vollstaendig und setzt die Spaltentypen.

    Ein:  Datensatz-ID der Socrata-API, Zielpfad, optionale Typangaben
    Aus:  Parquet-Datei in data/raw; Rueckgabe der Zeilenzahl

    - paginiert, weil die API je Anfrage deckelt
    - die Typen werden explizit gesetzt: sonst raet pandas bei GEOIDs auf int
      und die fuehrende Null faellt weg
    """
    _, pfad, select, where, order, _, datum, zahlen = QUELLEN[name]
    url = f"https://data.sfgov.org/resource/{pfad}.json"
    basis = {"$select": select, "$order": order}
    if where:
        basis["$where"] = where

    rows, offset = [], 0
    print(f"  Lade {name}...")
    while True:
        batch = _get(url, {**basis, "$limit": limit, "$offset": offset}).json()
        if not batch:
            break
        rows.extend(batch)
        offset += limit
        print(f"  {len(rows):>7,} Eintraege geladen...", end="\r")
        time.sleep(0.3)
    print(f"\n  Fertig: {len(rows):,} Eintraege total.")

    df = pd.DataFrame(rows)
    for col in datum:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in zahlen:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if name == "SFFD":
        df[zahlen] = df[zahlen].fillna(0)
    elif name == "CRIME":
        df["count"] = df["count"].fillna(0)
    elif name == "CRIME_HIST":
        # Grobe Plausibilitaetsgrenzen entfernen die (0,90)-Platzhalter der
        # Quelle; SF liegt etwa bei -122,5..-122,3 / 37,7..37,85.
        df = df.dropna(subset=["date", "x", "y"])
        df = df[df["x"].between(-123.2, -122.2) & df["y"].between(37.6, 37.95)]
    elif name == "LAND_USE":
        df.loc[~df["yrbuilt"].between(1800, 2025), "yrbuilt"] = pd.NA
    elif name == "CROSSWALK":
        df.columns = ["geoid", "neighborhood"]
        df["geoid"]        = df["geoid"].astype(str).str.zfill(11)
        df["neighborhood"] = df["neighborhood"].str.strip().str.title()
    return df


def lade_acs(year: int) -> pd.DataFrame:
    """Holt die ACS 5-Year Estimates auf Tract-Ebene fuer San Francisco County.

    Ein:  Jahrgang, Variablenliste aus config.py
    Aus:  eine CSV je Jahrgang in data/raw

    - eigene Funktion, weil die Census-API ein anderes Format liefert als
      DataSF: Kopfzeile plus Datenzeilen als verschachtelte Liste
    """
    codes = list(ACS_VARIABLES)
    r = requests.get(
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={','.join(['NAME'] + codes)}&for=tract:*"
        f"&in=state:06%20county:075&key={CENSUS_API_KEY}", timeout=30)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    df["geoid"] = df["state"] + df["county"] + df["tract"]
    df = df[["geoid"] + codes].rename(columns=ACS_VARIABLES)
    for col in ACS_VARIABLES.values():
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col] < -999, col] = pd.NA
    print(f"  ACS {year}: {len(df)} Census Tracts")
    return df[["geoid"] + list(ACS_VARIABLES.values())]


def run_download() -> None:
    """Laedt alle Rohquellen nach data/raw.

    Ein:  die Quellen-IDs und ACS-Jahrgaenge aus config.py
    Aus:  Parquet- und CSV-Dateien in data/raw; Exitcode

    - Schritt 1a von prep/build.py
    - vorhandene Dateien werden uebersprungen, damit ein Teillauf nicht erneut
      ueber die APIs geht
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    aktiv = [n for n, q in QUELLEN.items() if q[0]]
    if DOWNLOAD_ACS:
        aktiv.append("ACS")
    if DOWNLOAD_NEIGHBORHOODS:
        aktiv.append("NEIGHBORHOODS")
    if not aktiv:
        print("  Alle DOWNLOAD_*-Schalter in config.py stehen auf False "
              "-> nutze vorhandene Rohdaten in data/raw.")
        return
    print("  Aktive Downloads: " + ", ".join(aktiv) + "\n")

    for name, quelle in QUELLEN.items():
        if not quelle[0]:
            continue
        ziel = RAW_DIR / quelle[5]
        df = lade_datasf(name)
        if ziel.suffix == ".csv":
            df.to_csv(ziel, index=False)
        else:
            df.to_parquet(ziel, index=False)

    if DOWNLOAD_ACS:
        for year in ACS_YEARS:
            lade_acs(year).to_csv(RAW_DIR / f"acs_tracts_{year}.csv", index=False)
    if DOWNLOAD_NEIGHBORHOODS:
        (RAW_DIR / "neighborhoods.geojson").write_text(
            _get("https://data.sfgov.org/resource/j2bu-swwd.geojson",
                 {"$limit": 100}).text, encoding="utf-8")

    # Nach einem Crime- oder ACS-Neu-Download ist der Index-Cache ungueltig.
    if (DOWNLOAD_CRIME or DOWNLOAD_CRIME_HISTORISCH or DOWNLOAD_ACS) \
            and CACHE_CRIME.exists():
        CACHE_CRIME.unlink()
        print(f"  Cache {CACHE_CRIME.name} verworfen (wird neu berechnet).")


# ==========================================================================
# TEIL B  SFFD - Einsatzdaten
# ==========================================================================
def prepare_sffd(df: pd.DataFrame) -> pd.DataFrame:
    """Bereitet Dedup, Antwortzeit, Zeitmerkmale und Stadtteilnamen auf.

    Ein:  die rohe SFFD-Tabelle
    Aus:  dieselbe Tabelle, bereinigt und um Zeitspalten ergaenzt

    - der Dedup laeuft ueber die Einsatznummer und wird gezaehlt ausgegeben
    - doppelte Meldungen desselben Einsatzes waeren sonst zwei Zeilen
    """
    n_vorher = len(df)
    df = df.drop_duplicates(subset=["incident_number"], keep="first").copy()
    if n_vorher - len(df):
        print(f"  Dedup: {n_vorher - len(df):,} doppelte Einsatznummern entfernt.")

    df["response_time_min"] = (
        (df["arrival_dttm"] - df["alarm_dttm"]).dt.total_seconds() / 60)
    df = df[df["response_time_min"].between(ANTWORTZEIT_MIN, ANTWORTZEIT_MAX)]

    df["year"]         = df["incident_date"].dt.year
    df["month"]        = df["incident_date"].dt.month
    df["hour"]         = df["alarm_dttm"].dt.hour
    df["weekday"]      = df["alarm_dttm"].dt.dayofweek
    df["is_weekend"]   = df["weekday"].isin([5, 6]).astype(int)
    df["is_night"]     = ((df["hour"] >= 22) | (df["hour"] <= 5)).astype(int)
    df["neighborhood"] = df["neighborhood_district"].str.strip().str.title()
    return df


# ==========================================================================
# TEIL C  ACS - soziooekonomische Merkmale
# ==========================================================================
def tract_zu_stadtteil(geoids: pd.Series, crosswalk: pd.DataFrame) -> pd.Series:
    """Ordnet Census Tracts einem Stadtteil zu, auch ueber Zensusgrenzen hinweg.

    Ein:  GEOID-Spalte einer ACS-Tabelle, Crosswalk
    Aus:  Stadtteilspalte, fehlend wo keine Zuordnung moeglich ist

    - der Crosswalk beruht auf den Tract-Grenzen des Zensus 2020 (242 Tracts).
      Die Jahrgaenge 2009 bis 2019 tragen die Grenzen der Zensus 2000 bzw. 2010
      und damit teils andere GEOIDs. Ein reiner Gleichheitsverbund verwarf
      deshalb 40 der 197 Tracts von 2014/2019 - und mit ihnen 27 % der
      Wohnbevoelkerung, konzentriert in den geteilten Downtown-Tracts
    - Rueckfallebene ist der vierstellige Basiscode der Tract-Nummer: Eine
      Teilung im Zensus 2020 behaelt ihn bei und aendert nur das zweistellige
      Suffix (012400 -> 012401 + 012402). Die Kinder eines geteilten Tracts
      liegen im selben Stadtteil, weil Stadtteilgrenzen entlang der
      Hauptstrassen verlaufen und Teilungen innerhalb dieser Flaechen erfolgen
    - die Rueckfallebene greift nur, wenn ALLE 2020er Tracts desselben
      Basiscodes in denselben Stadtteil fallen; sonst bleibt der Tract offen.
      Fuer 2014/2019 ist das bei allen 39 betroffenen Tracts der Fall
    - offen bleiben danach nur die Wasserflaechen-Tracts (99xx) ohne Bewohner
    """
    direkt = dict(zip(crosswalk["geoid"], crosswalk["neighborhood"]))
    basis = (crosswalk.assign(basis=crosswalk["geoid"].str[5:9])
                      .groupby("basis")["neighborhood"]
                      .agg(lambda s: s.iloc[0] if s.nunique() == 1 else None)
                      .dropna().to_dict())
    g = geoids.astype(str).str.zfill(11)
    return g.map(direkt).fillna(g.str[5:9].map(basis))


def acs_je_neighborhood(acs: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Aggregiert Census Tracts auf Stadtteile.

    Ein:  ACS-Tabelle auf Tract-Ebene, Crosswalk
    Aus:  eine Zeile je Stadtteil und Jahrgang

    - Mediane werden bevoelkerungsgewichtet gemittelt, Zaehlgroessen summiert
    - ein Median laesst sich nicht addieren, daher die Gewichtung
    - die Zuordnung laeuft ueber tract_zu_stadtteil(); die Abbruchschwelle
      sichert, dass kein Jahrgang mit unvollstaendiger Bevoelkerung in die
      Exposition geraet - genau das war der Fehler vor dem Basiscode-Fallback
    """
    m = acs.copy()
    m["neighborhood"] = tract_zu_stadtteil(m["geoid"], crosswalk)
    bev_gesamt = pd.to_numeric(m["total_population"], errors="coerce").fillna(0)
    zugeordnet = bev_gesamt[m["neighborhood"].notna()].sum()
    quote = zugeordnet / bev_gesamt.sum() if bev_gesamt.sum() else 0.0
    print(f"  ACS-Zuordnung: {m['neighborhood'].notna().sum()}/{len(m)} Tracts, "
          f"{quote:.1%} der Wohnbevoelkerung")
    assert quote >= 0.95, (
        f"ACS-Zuordnung deckt nur {quote:.1%} der Wohnbevoelkerung - "
        f"Crosswalk gegen die Tract-Grenzen des Jahrgangs pruefen.")
    m = m.dropna(subset=["neighborhood"])
    for col in ACS_GEWICHTET:
        m[f"_w_{col}"] = m[col] * m["total_population"]

    g = m.groupby("neighborhood")
    pop = g["total_population"].sum()
    nb = pd.DataFrame({"total_population": pop.astype(int)})
    for col in ACS_GEWICHTET:
        # Nenner 0 -> kein gewichteter Median moeglich, bleibt fehlend.
        nb[col] = (g[f"_w_{col}"].sum() / pop.where(pop > 0)).round(0).astype("Int64")
    for col in ACS_SUMMEN:
        nb[col] = g[col].sum().round(0)
    return nb.reset_index()


def acs_snapshot(jahr: int, acs_years: list[int]) -> int:
    """Waehlt den zum Prognosezeitpunkt tatsaechlich publizierten ACS-Jahrgang.

    Ein:  Einsatzjahr, verfuegbare ACS-Jahrgaenge
    Aus:  der zu verwendende Jahrgang

    - Bedingung: acs_jahr <= Einsatzjahr - ACS_PUBLIKATIONS_LAG
    - zwei Stufen der Absicherung: "letzter verfuegbarer" statt "zeitlich
      naechster" Snapshot (#4) und zusaetzlich die reale
      Publikationsverzoegerung von rund einem Jahr (#11)
    - ohne die zweite haette ein Einsatz aus 2023 den Jahrgang 2023 bekommen,
      der erst Ende 2024 erschienen ist
    - vor dem ersten Snapshot gibt es keinen vergangenen Jahrgang; Rueckgriff auf
      den aeltesten als dokumentierte Limitation, die Hauptanalyse beginnt 2015
    """
    return max([a for a in acs_years if a <= int(jahr) - ACS_PUBLIKATIONS_LAG],
               default=min(acs_years))


def join_acs(sffd: pd.DataFrame, nb_per_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """Fuegt jedem Einsatz den passenden ACS-Jahrgang an.

    Ein:  Einsatztabelle, ACS-Jahrgaenge je Stadtteil
    Aus:  Einsatztabelle mit den soziooekonomischen Merkmalen

    - massgeblich ist der Jahrgang, der zum Einsatzzeitpunkt veroeffentlicht war
      (ACS_PUBLIKATIONS_LAG)
    - sonst stuende im Modell Information, die es damals nicht gab
    """
    jahrgaenge = sorted(nb_per_year)
    sffd = sffd.copy()
    sffd["acs_year"] = sffd["year"].apply(lambda y: acs_snapshot(y, jahrgaenge))

    print("\n  Einsaetze nach zugeordnetem ACS-Snapshot:")
    for acs_y, info in sffd.groupby("acs_year")["year"].agg(
            lambda x: f"{x.min()}-{x.max()} ({len(x):,} Einsaetze)").items():
        print(f"    ACS {acs_y}  -> Einsatzjahre {info}")

    final = pd.concat([grp.merge(nb_per_year[acs_y], on="neighborhood", how="left")
                       for acs_y, grp in sffd.groupby("acs_year")]).sort_index()
    for col in ACS_NUM_COLS:
        final[col] = pd.to_numeric(final[col], errors="coerce").round(0)
    return final


# ==========================================================================
# TEIL D  GEOMETRIE
# ==========================================================================
def neighborhoods_gdf():
    """Laedt die Neighborhood-Polygone.

    Ein:  nichts
    Aus:  GeoDataFrame der Stadtteilgeometrien

    - beide Spatial Joins nutzen dieselbe Geometrie, damit sich Kriminalitaets-
      und Baumerkmale auf identische Flaechen beziehen
    """
    import geopandas as gpd
    gdf = gpd.read_file(RAW_DIR / "neighborhoods.geojson")
    gdf["neighborhood"] = gdf["nhood"].str.strip().str.title()
    return gdf[["neighborhood", "geometry"]].to_crs("EPSG:4326")


# ==========================================================================
# TEIL E  CRIME - relativer Kriminalitaetsindex je Stadtteil x Monat
# ==========================================================================
def crime_monatlich(hist: pd.DataFrame, neu: pd.DataFrame,
                    crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Zaehlt Delikte je Stadtteil und Monat aus beiden SFPD-Quellen.

    Ein:  historische Tabelle (bis 2017), moderne Tabelle (ab 2018-01),
          Crosswalk fuer den Spatial Join
    Aus:  eine Zeile je Stadtteil und Monat mit der Deliktzahl

    - zwei Quellen mit Schnitt 2018
    - die moderne ist voraggregiert und hat eine Stadtteilspalte, die historische
      nicht; dort ein Spatial Join der Koordinaten ins Polygon
    - der Index zaehlt alle Straftaten, deshalb werden die Kategorien summiert;
      eine Harmonisierung der Kategorienschemata eruebrigt sich damit
    """
    import geopandas as gpd

    hist = pd.read_parquet(RAW_DIR / "crime_historisch_raw.parquet")
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
    hist = hist.dropna(subset=["date", "x", "y"])
    print("  Spatial Join (Deliktkoordinate -> Neighborhood-Polygon)...")
    punkte = gpd.GeoDataFrame(hist[["date"]].copy(),
                              geometry=gpd.points_from_xy(hist["x"], hist["y"]),
                              crs="EPSG:4326")
    joined = gpd.sjoin(punkte, neighborhoods_gdf(), how="left", predicate="within")
    quote = joined["neighborhood"].notna().mean()
    print(f"  Match-Rate: {quote * 100:.1f}% "
          f"({joined['neighborhood'].notna().sum():,}/{len(joined):,})")
    assert quote >= 0.90, f"Match-Rate nur {quote:.1%} - Geometrie pruefen."

    j = pd.DataFrame(joined.dropna(subset=["neighborhood"]).drop(columns="geometry"))
    j["neighborhood"] = j["neighborhood"].str.strip().str.title()
    alt = (j.assign(jahr=j["date"].dt.year, monat=j["date"].dt.month)
             .groupby(["neighborhood", "jahr", "monat"]).size()
             .reset_index(name="delikte"))
    print(f"  Historisch (tmnf-yvry): {alt['delikte'].sum():,.0f} Delikte, "
          f"{alt['jahr'].min()}-{alt['jahr'].max()}")

    roh = pd.read_parquet(RAW_DIR / "crime_raw.parquet")
    assert "by_month_incident_date" in roh.columns, (
        "crime_raw.parquet ohne Datumsspalte (alter Download). Bitte "
        "prep/s1_daten.py mit DOWNLOAD_CRIME=True neu ausfuehren - ohne Datum "
        "laesst sich kein zeitbewusster Kriminalitaetsindex bilden.")
    roh["count"] = pd.to_numeric(roh["count"], errors="coerce").fillna(0)
    spalte = "neighborhood" if "neighborhood" in roh.columns else "analysis_neighborhood"
    roh["neighborhood"] = roh[spalte].str.strip().str.title()
    d = pd.to_datetime(roh["by_month_incident_date"], errors="coerce")
    neu = (roh.assign(jahr=d.dt.year, monat=d.dt.month).dropna(subset=["jahr"])
              .groupby(["neighborhood", "jahr", "monat"])["count"].sum()
              .reset_index(name="delikte"))
    neu[["jahr", "monat"]] = neu[["jahr", "monat"]].astype(int)
    print(f"  Modern (e3si-785i): {neu['delikte'].sum():,.0f} Delikte, "
          f"{neu['jahr'].min()}-{neu['jahr'].max()}")

    # max statt sum: Sicherheitsnetz gegen Ueberlappung der beiden Quellen.
    return (pd.concat([alt, neu], ignore_index=True)
              .groupby(["neighborhood", "jahr", "monat"])["delikte"]
              .max().reset_index())


def kriminalitaetsindex(nb_per_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """Berechnet den relativen Kriminalitaetsindex je Stadtteil und Monat.

    Ein:  monatliche Deliktzahlen, Einwohnerzahlen, Fensterlaenge
    Aus:  Indexspalte je Stadtteil-Monat, dazu crime_rate_raw

    Definition (Location Quotient der Kriminalitaetsbelastung):

        rate(i,t)     = Delikte(i, Fenster endend in t-1) / Einwohner(i)
        rate(Stadt,t) = Delikte(Stadt, gleiches Fenster) / Einwohner(Stadt)
        index(i,t)    = rate(i,t) / rate(Stadt,t)

    - Lesart: 1,0 = Belastung wie im Stadtdurchschnitt desselben Monats
    - relativ statt absolut, weil der SFPD-Systemwechsel im Mai 2018 das
      stadtweite Niveau veraendert; ein multiplikativer Niveausprung wirkt auf
      Zaehler und Nenner gleich und kuerzt sich heraus
    - verbleibende Limitation: eine Verschiebung in der ZUSAMMENSETZUNG der
      erfassten Delikte, die einzelne Stadtteile staerker trifft, kuerzt sich
      nicht heraus (Kap. 6.3)
    - kein Leakage: das Fenster endet strikt im Vormonat
    - crime_rate_raw (Delikte je 1.000 Ew.) ist nur deskriptiv fuer Kapitel 5.1
      und kein Modellmerkmal - sie enthaelt den Bruch von 2018
    """
    monatlich = crime_monatlich()

    # Vollstaendiges Raster Neighborhood x Monat (Monate ohne Delikt = 0)
    jahre = range(int(monatlich["jahr"].min()), int(monatlich["jahr"].max()) + 1)
    idx = pd.MultiIndex.from_product(
        [sorted(monatlich["neighborhood"].unique()), jahre, range(1, 13)],
        names=["neighborhood", "jahr", "monat"])
    raster = (monatlich.set_index(["neighborhood", "jahr", "monat"])
                       .reindex(idx).fillna({"delikte": 0}).reset_index()
                       .sort_values(["neighborhood", "jahr", "monat"]))

    # Rollierendes Fenster, endend im VORMONAT: shift(1) VOR rolling()
    raster["delikte_fenster"] = (
        raster.groupby("neighborhood")["delikte"]
              .transform(lambda s: s.shift(1).rolling(CRIME_FENSTER_MONATE).sum()))
    raster = raster.dropna(subset=["delikte_fenster"])

    # Einwohnerzahl mit demselben ACS-Versatz wie das Exposure-Merkmal, damit
    # Nenner des Index und Modellmerkmal auf identischen Werten beruhen.
    jahrgaenge = sorted(nb_per_year)
    bev = pd.concat([nb_per_year[acs_snapshot(j, jahrgaenge)]
                     [["neighborhood", "total_population"]].assign(jahr=j)
                     for j in sorted(raster["jahr"].unique())], ignore_index=True)
    raster = raster.merge(bev, on=["neighborhood", "jahr"], how="left")
    raster = raster[raster["total_population"] > 0]

    stadt = (raster.groupby(["jahr", "monat"])
                   .agg(stadt_delikte=("delikte_fenster", "sum"),
                        stadt_bev=("total_population", "sum")).reset_index())
    raster = raster.merge(stadt, on=["jahr", "monat"], how="left")
    raster["crime_rate_raw"] = (raster["delikte_fenster"]
                                / raster["total_population"] * 1000).round(3)
    raster["crime_index"] = (raster["crime_rate_raw"]
                             / (raster["stadt_delikte"] / raster["stadt_bev"] * 1000)
                             ).round(4)

    out = raster[["neighborhood", "jahr", "monat", "crime_index", "crime_rate_raw"]]
    print(f"  Kriminalitaetsindex: {out['neighborhood'].nunique()} Neighborhoods "
          f"x {len(out.groupby(['jahr', 'monat']))} Monate "
          f"({out['jahr'].min()}-{out['jahr'].max()})")
    print(f"    Index Median {out['crime_index'].median():.2f}, "
          f"Spanne {out['crime_index'].min():.2f}-{out['crime_index'].max():.2f}")
    return out


# ==========================================================================
# TEIL F  LAND USE - bauliche Merkmale
# ==========================================================================
def land_use_je_neighborhood() -> pd.DataFrame:
    """Ordnet Parzellen-Centroide Stadtteilen zu und aggregiert je Stadtteil.

    Ein:  Parzellentabelle, Stadtteilgeometrien
    Aus:  eine Zeile je Stadtteil mit den baulichen Merkmalen

    - statisch: Snapshot 2020, der einzige verfuegbare Jahrgang
    """
    import geopandas as gpd
    from shapely.geometry import shape

    parcels = pd.read_parquet(RAW_DIR / "land_use_2020_raw.parquet")
    parcels["geometry"] = parcels["the_geom"].apply(
        lambda g: shape(g).centroid if isinstance(g, dict) else None)
    parcels = parcels.dropna(subset=["geometry"])
    gdf = gpd.GeoDataFrame(
        parcels[["blklot", "yrbuilt", "landuse", "resunits", "st_area_sh", "geometry"]],
        geometry="geometry", crs="EPSG:4326")

    print("  Spatial Join (Parzellen-Centroid -> Neighborhood-Polygon)...")
    joined = gpd.sjoin(gdf, neighborhoods_gdf(), how="left", predicate="within")
    matched = joined["neighborhood"].notna().sum()
    print(f"  Match-Rate: {matched / len(joined) * 100:.1f}% "
          f"({matched:,}/{len(joined):,})")

    df = pd.DataFrame(joined.dropna(subset=["neighborhood"])
                            .drop(columns=["geometry", "index_right"], errors="ignore"))
    df["is_residential"]      = df["landuse"].isin(RESIDENTIAL).astype(int)
    df["has_yrbuilt"]         = df["yrbuilt"].notna().astype(int)
    df["is_pre1940"]          = (df["yrbuilt"] < 1940).fillna(False).astype(int)
    df["is_pre1960"]          = (df["yrbuilt"] < 1960).fillna(False).astype(int)
    df["high_risk_area_sqft"] = (df["st_area_sh"].fillna(0)
                                 * df["landuse"].isin(HIGH_RISK_COMMERCIAL))

    agg = df.groupby("neighborhood").agg(
        parcel_count                   =("blklot",              "count"),
        yrbuilt_count                  =("has_yrbuilt",         "sum"),
        pre1940_count                  =("is_pre1940",          "sum"),
        pre1960_count                  =("is_pre1960",          "sum"),
        total_resunits                 =("resunits",            "sum"),
        residential_count              =("is_residential",      "sum"),
        total_area_sqft                =("st_area_sh",          "sum"),
        high_risk_commercial_area_sqft =("high_risk_area_sqft", "sum"),
    ).reset_index()
    for col in ["parcel_count", "yrbuilt_count", "pre1940_count",
                "pre1960_count", "residential_count"]:
        agg[col] = agg[col].astype(int)
    agg["total_resunits"] = agg["total_resunits"].round(0)
    print(f"  {len(agg)} Neighborhoods | Parzellen: {agg['parcel_count'].sum():,} "
          f"| Hochrisiko-Anteil: "
          f"{agg['high_risk_commercial_area_sqft'].sum() / agg['total_area_sqft'].sum() * 100:.1f}%")
    return agg


# ==========================================================================
# TEIL G  QUOTEN
# ==========================================================================
QUOTEN = [
    ("poverty_rate",     "poverty_below",         "poverty_universe_total"),
    ("bachelor_rate",    "bachelor_degree_count", "education_universe_total"),
    ("vacancy_rate",     "vacant_housing_units",  "total_housing_units"),
    ("pct_pre1940",      "pre1940_count",         "yrbuilt_count"),
    ("pct_pre1960",      "pre1960_count",         "yrbuilt_count"),
    ("pct_residential",  "residential_count",     "parcel_count"),
    ("pct_high_risk_commercial_area",
     "high_risk_commercial_area_sqft", "total_area_sqft"),
]


def berechne_quoten(df: pd.DataFrame) -> pd.DataFrame:
    """Rechnet Anteilswerte in [0,1].

    Ein:  Zaehler- und Nennerspalten
    Aus:  Anteilsspalten; Nenner <= 0 ergibt NaN statt Division durch Null

    - Kriminalitaet taucht hier nicht auf: Sie geht als relativer Index je
      Stadtteil x Monat ein (#17), nicht als Anteil
    """
    for name, zaehler, nenner in QUOTEN:
        z = pd.to_numeric(df[zaehler], errors="coerce").astype(float)
        n = pd.to_numeric(df[nenner], errors="coerce").astype(float)
        df[name] = (z / n.where(n > 0, np.nan)).round(4)
    for col in INT64_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(0).astype("Int64")
    print(f"  {len(QUOTEN)} Quoten berechnet: {[q[0] for q in QUOTEN]}")
    return df


# ==========================================================================
# Ablauf
# ==========================================================================
def run_join() -> pd.DataFrame:
    """Fuehrt alle Rohquellen zur Einsatztabelle zusammen.

    Ein:  die Dateien aus run_download()
    Aus:  data/processed/einsaetze.parquet; Exitcode

    - Reihenfolge: SFFD aufbereiten, ACS anfuegen, Kriminalitaetsindex und
      Baumerkmale ueber die Stadtteilgeometrie anspielen, Quoten rechnen
    - der Kriminalitaetsindex ist die einzige monatlich variierende
      Merkmalsquelle; ACS ist jaehrlich, Land Use ein Snapshot 2020
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    fehlend = [f"{d} (Schalter {s})" for d, s in BENOETIGT.items()
               if not (RAW_DIR / d).exists()]
    if fehlend:
        raise FileNotFoundError(
            "Rohdaten fehlen in data/raw: " + ", ".join(fehlend)
            + ". Den jeweiligen Schalter in prep/config.py auf True setzen "
              "und prep/s1_daten.py erneut ausfuehren.")

    print("[1/6] SFFD: Rohdaten, Dedup, Zeit-Features...")
    sffd = prepare_sffd(pd.read_parquet(RAW_DIR / "fire_incidents.parquet"))
    print(f"  {len(sffd):,} Einsaetze | Jahre "
          f"{int(sffd['year'].min())}-{int(sffd['year'].max())}")

    print(f"\n[2/6] ACS: Tract -> Neighborhood ({len(ACS_YEARS)} Jahrgaenge)...")
    crosswalk = pd.read_csv(RAW_DIR / "crosswalk.csv", dtype={"geoid": str})
    nb_per_year = {}
    for year in ACS_YEARS:
        nb = acs_je_neighborhood(
            pd.read_csv(RAW_DIR / f"acs_tracts_{year}.csv", dtype={"geoid": str}),
            crosswalk)
        nb.to_csv(PROCESSED_DIR / f"acs_neighborhoods_{year}.csv", index=False)
        nb_per_year[year] = nb
        print(f"  ACS {year}: {len(nb)} Neighborhoods")

    print(f"\n[3/6] Zeitbewusster ACS-Join (Versatz {ACS_PUBLIKATIONS_LAG} Jahr)...")
    base = join_acs(sffd, nb_per_year)

    # Kein statischer Fallback: Ein statischer Crime-Join wuerde Delikte aus dem
    # Testzeitraum in die Trainingsmerkmale tragen (Leakage) und jede
    # Zeitvarianz beseitigen.
    print(f"\n[4/6] Crime: relativer Index (Fenster {CRIME_FENSTER_MONATE} Monate, "
          f"endend im Vormonat)...")
    if CACHE_CRIME.exists():
        print(f"  Nutze Cache: {CACHE_CRIME.name} (loeschen zum Neuberechnen)")
        crime = pd.read_csv(CACHE_CRIME)
    else:
        crime = kriminalitaetsindex(nb_per_year)
        crime.to_csv(CACHE_CRIME, index=False)
    vorher = len(base)
    base = base.merge(crime, left_on=["neighborhood", "year", "month"],
                      right_on=["neighborhood", "jahr", "monat"],
                      how="left").drop(columns=["jahr", "monat"])
    assert len(base) == vorher, "Crime-Join hat Zeilen dupliziert (1:n-Beziehung!)."
    print(f"  Join auf Einsatz-Ebene: "
          f"{base['crime_index'].notna().mean() * 100:.1f}% der Einsaetze mit "
          f"Index (fehlend v. a. vor {int(crime['jahr'].min())})")

    # Der Spatial Join ist der teuerste Schritt und deterministisch -> gecacht.
    print("\n[5/6] Land Use: Aggregation je Neighborhood...")
    if CACHE_LAND_USE.exists():
        print(f"  Nutze Cache: {CACHE_LAND_USE.name} (loeschen zum Neuberechnen)")
        land_use = pd.read_csv(CACHE_LAND_USE)
    else:
        land_use = land_use_je_neighborhood()
        land_use.to_csv(CACHE_LAND_USE, index=False)
    base = base.merge(land_use, on="neighborhood", how="left")

    print("\n[6/6] Quoten berechnen und Spalten eindeutschen...")
    base = berechne_quoten(base).rename(columns=spalten_deutsch)

    base.to_parquet(PFAD_EINSAETZE, index=False)
    print(f"\n  => {PFAD_EINSAETZE.relative_to(ROOT)}  "
          f"({len(base):,} Zeilen | {len(base.columns)} Spalten)")
    return base


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] != "join":
        run_download()
    run_join()
