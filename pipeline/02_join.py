"""
Schritt 2: Rohdaten joinen und auf Neighborhood-Ebene aggregieren.

Input:  data/raw/*
Output: data/processed/sf_fire_incidents_base.parquet (44 Spalten, englisch)
"""
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

ROOT          = Path(__file__).parent.parent
RAW_DIR       = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"

ACS_YEARS = [2009, 2014, 2019, 2021, 2023]

# Publikationsverzoegerung der ACS-5-Jahres-Schaetzungen: der Jahrgang y wird
# erst ca. Dezember y+1 veroeffentlicht. Ein Modell, das im Jahr y bereits den
# ACS-Jahrgang y nutzt, waere zum Prognosezeitpunkt nicht implementierbar.
# Daher wird der Snapshot zusaetzlich um ACS_PUBLIKATIONS_LAG Jahre verzoegert
# (Entscheidung 2026-07-26, abgestimmt mit Lukas; Decision Log #11).
ACS_PUBLIKATIONS_LAG = 1

HIGH_RISK_COMMERCIAL = {"RETAIL/ENT", "PDR"}
RESIDENTIAL          = {"RESIDENT", "MIXRES"}

# --------------------------------------------------------------------------
# Kriminalitaetsindex (Decision Log #17, 2026-07-26)
# --------------------------------------------------------------------------
# Laenge des rollierenden Fensters in Monaten. Das Fenster endet im VORMONAT
# des jeweiligen Analysemonats -> strikt rueckwaertsgerichtet, kein Leakage.
# 12 Monate glaetten die starke Monatsschwankung kleiner Stadtteile und
# entsprechen der jaehrlichen Taktung der uebrigen Merkmale (ACS).
CRIME_FENSTER_MONATE = 12

ACS_NUM_COLS = [
    "total_population", "median_household_income", "median_gross_rent",
    "poverty_below", "poverty_universe_total", "bachelor_degree_count",
    "education_universe_total", "vacant_housing_units", "total_housing_units",
]


def require(path: Path, hint: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{path.relative_to(ROOT)} nicht gefunden. {hint}")


def prepare_sffd(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Bereinigung: mehrfach gemeldete Einsatznummern aus den DataSF-Quelldaten
    # entfernen (269 Zeilen / 0,04 %, davon 50 vollstaendig identisch; Befund:
    # results/eignungspruefung/). Abgestimmt mit Lukas am 2026-07-18.
    n_vorher = len(df)
    df = df.drop_duplicates(subset=["incident_number"], keep="first")
    if n_vorher - len(df):
        print(f"  Dedup: {n_vorher - len(df):,} doppelte Einsatznummern entfernt.")
    df["response_time_min"] = (df["arrival_dttm"] - df["alarm_dttm"]).dt.total_seconds() / 60
    df = df[(df["response_time_min"] >= 0) & (df["response_time_min"] <= 60)]
    df["year"]         = df["incident_date"].dt.year
    df["month"]        = df["incident_date"].dt.month
    df["hour"]         = df["alarm_dttm"].dt.hour
    df["weekday"]      = df["alarm_dttm"].dt.dayofweek
    df["is_weekend"]   = df["weekday"].isin([5, 6]).astype(int)
    df["is_night"]     = ((df["hour"] >= 22) | (df["hour"] <= 5)).astype(int)
    df["neighborhood"] = df["neighborhood_district"].str.strip().str.title()
    return df


def aggregate_acs_to_neighborhood(acs: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    merged = acs.merge(crosswalk, on="geoid", how="left").dropna(subset=["neighborhood"])
    rows = []
    for hood, grp in merged.groupby("neighborhood"):
        pop = grp["total_population"].sum()
        gewichtet = lambda col: ((grp[col] * grp["total_population"]).sum() / pop) if pop > 0 else pd.NA
        rows.append({
            "neighborhood":             hood,
            "total_population":         int(pop),
            "median_household_income":  gewichtet("median_household_income"),
            "median_gross_rent":        gewichtet("median_gross_rent"),
            "poverty_below":            grp["poverty_below"].sum(),
            "poverty_universe_total":   grp["poverty_universe_total"].sum(),
            "bachelor_degree_count":    grp["bachelor_degree_count"].sum(),
            "education_universe_total": grp["education_universe_total"].sum(),
            "vacant_housing_units":     grp["vacant_housing_units"].sum(),
            "total_housing_units":      grp["total_housing_units"].sum(),
        })
    nb = pd.DataFrame(rows)
    for col in ["median_household_income", "median_gross_rent"]:
        nb[col] = pd.to_numeric(nb[col], errors="coerce").round(0).astype("Int64")
    for col in ACS_NUM_COLS[3:]:
        nb[col] = pd.to_numeric(nb[col], errors="coerce").round(0)
    return nb


def year_aware_join(sffd: pd.DataFrame, nb_per_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    acs_years = sorted(nb_per_year.keys())
    sffd = sffd.copy()
    # Strikt prognostischer Join: jeder Einsatz erhaelt den LETZTEN zum
    # Prognosezeitpunkt TATSAECHLICH PUBLIZIERTEN ACS-Snapshot.
    #   Bedingung: acs_jahr <= Einsatzjahr - ACS_PUBLIKATIONS_LAG
    # Stufe 1 (Decision Log #4, 2026-07-18): "letzter verfuegbarer" statt
    #   "zeitlich naechster" Snapshot -> kein Zukunfts-Leakage mehr.
    # Stufe 2 (Decision Log #11, 2026-07-26): zusaetzlich die reale
    #   Publikationsverzoegerung von ~1 Jahr beruecksichtigt. Ohne diesen
    #   Versatz haette ein Einsatz aus 2023 den ACS-Jahrgang 2023 erhalten,
    #   der erst Ende 2024 erschienen ist - das Modell waere nicht
    #   implementierbar gewesen.
    # Fuer Jahre vor dem ersten publizierten Snapshot (2003-2009) existiert kein
    # vergangener Jahrgang; Rueckgriff auf ACS 2009 als dokumentierte Limitation
    # (Hauptanalyse beginnt ohnehin 2015, vgl. CLAUDE.md / Leitfaden A3).
    sffd["acs_year"] = sffd["year"].apply(
        lambda y: max([a for a in acs_years if a <= int(y) - ACS_PUBLIKATIONS_LAG],
                      default=min(acs_years))
    )
    print("\n  Einsaetze nach zugeordnetem ACS-Snapshot:")
    for acs_y, info in sffd.groupby("acs_year")["year"].agg(
        lambda x: f"{x.min()}-{x.max()} ({len(x):,} Einsaetze)"
    ).items():
        print(f"    ACS {acs_y}  -> Einsatzjahre {info}")

    parts = [grp.merge(nb_per_year[acs_y], on="neighborhood", how="left")
             for acs_y, grp in sffd.groupby("acs_year")]
    final = pd.concat(parts).sort_index()
    for col in ACS_NUM_COLS:
        if col in final.columns:
            final[col] = pd.to_numeric(final[col], errors="coerce").round(0)
    return final


def _crime_monatlich_modern(path: Path) -> pd.DataFrame:
    """Monatliche Deliktzahlen je Neighborhood aus e3si-785i (ab 2018-01).

    Der Datensatz ist bereits auf Monat x Neighborhood x Kategorie
    voraggregiert; da der Index ALLE Straftaten zaehlt, werden die Kategorien
    schlicht aufsummiert (keine Schema-Harmonisierung noetig).
    """
    raw = pd.read_parquet(path)
    if "by_month_incident_date" not in raw.columns:
        raise RuntimeError(
            "crime_raw.parquet enthaelt keine Spalte 'by_month_incident_date'. "
            "Das ist ein aelterer Download ohne Datum. Bitte 01_fetch.py mit "
            "DOWNLOAD_CRIME=True neu ausfuehren - ohne Datum laesst sich kein "
            "zeitbewusster Kriminalitaetsindex bilden.")
    df = raw.copy()
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)
    quelle = "neighborhood" if "neighborhood" in df.columns else "analysis_neighborhood"
    df["neighborhood"] = df[quelle].str.strip().str.title()
    d = pd.to_datetime(df["by_month_incident_date"], errors="coerce")
    df = df.assign(jahr=d.dt.year, monat=d.dt.month).dropna(subset=["jahr"])
    out = (df.groupby(["neighborhood", "jahr", "monat"])["count"].sum()
             .reset_index(name="delikte"))
    out[["jahr", "monat"]] = out[["jahr", "monat"]].astype(int)
    print(f"  Modern (e3si-785i): {out['delikte'].sum():,.0f} Delikte, "
          f"{out['jahr'].min()}-{out['jahr'].max()}")
    return out


def _crime_monatlich_historisch(path: Path) -> pd.DataFrame:
    """Monatliche Deliktzahlen je Neighborhood aus tmnf-yvry (2014-2017).

    Der historische Datensatz hat keine Stadtteilspalte -> Spatial Join der
    Koordinaten gegen dieselbe Neighborhood-Geometrie, die auch fuer die
    Land-Use-Daten verwendet wird. Damit ist die Gebietsdefinition ueber alle
    Quellen hinweg identisch.
    """
    import geopandas as gpd

    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "x", "y"])

    punkte = gpd.GeoDataFrame(
        df[["date"]].copy(),
        geometry=gpd.points_from_xy(df["x"], df["y"]),
        crs="EPSG:4326")
    print("  Spatial Join (Deliktkoordinate -> Neighborhood-Polygon)...")
    joined = gpd.sjoin(punkte, load_neighborhoods_gdf(), how="left", predicate="within")
    quote = joined["neighborhood"].notna().mean()
    print(f"  Match-Rate: {quote*100:.1f}% "
          f"({joined['neighborhood'].notna().sum():,}/{len(joined):,})")
    if quote < 0.90:
        raise RuntimeError(f"Match-Rate des historischen Crime-Joins nur "
                           f"{quote:.1%} - Geometrie oder Koordinaten pruefen.")

    j = pd.DataFrame(joined.dropna(subset=["neighborhood"]).drop(columns="geometry"))
    j["neighborhood"] = j["neighborhood"].str.strip().str.title()
    out = (j.assign(jahr=j["date"].dt.year, monat=j["date"].dt.month)
             .groupby(["neighborhood", "jahr", "monat"]).size()
             .reset_index(name="delikte"))
    print(f"  Historisch (tmnf-yvry): {out['delikte'].sum():,.0f} Delikte, "
          f"{out['jahr'].min()}-{out['jahr'].max()}")
    return out


def _bevoelkerung_je_jahr(nb_per_year: dict[int, pd.DataFrame],
                          jahre: list[int]) -> pd.DataFrame:
    """Einwohnerzahl je Neighborhood und Jahr, mit demselben ACS-Versatz.

    Dieselbe Snapshot-Regel wie beim Einsatz-Join (year_aware_join), damit die
    Nenner des Kriminalitaetsindex und das Exposure-Merkmal des Modells auf
    identischen Bevoelkerungswerten beruhen.
    """
    acs_years = sorted(nb_per_year.keys())
    zeilen = []
    for jahr in jahre:
        acs_y = max([a for a in acs_years if a <= jahr - ACS_PUBLIKATIONS_LAG],
                    default=min(acs_years))
        nb = nb_per_year[acs_y][["neighborhood", "total_population"]].copy()
        zeilen.append(nb.assign(jahr=jahr))
    return pd.concat(zeilen, ignore_index=True)


def berechne_kriminalitaetsindex(nb_per_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """Relativer Kriminalitaetsindex je Neighborhood und Monat.

    Definition (Location Quotient der Kriminalitaetsbelastung):

        rate(i,t)       = Delikte(i, Fenster endend in t-1) / Einwohner(i)
        rate(Stadt,t)   = Delikte(Stadt, gleiches Fenster) / Einwohner(Stadt)
        index(i,t)      = rate(i,t) / rate(Stadt,t)

    Lesart: 1,0 = Kriminalitaetsbelastung wie im Stadtdurchschnitt desselben
    Monats, 2,0 = doppelt so hoch, 0,5 = halb so hoch.

    Warum relativ statt absolut? Der SFPD-Systemwechsel im Mai 2018 (CABLE ->
    Crime Data Warehouse) veraendert das stadtweite Niveau der erfassten
    Faelle. Ein solcher multiplikativer Niveauspruung wirkt auf Zaehler und
    Nenner gleichermassen und kuerzt sich im Quotienten heraus - der Index ist
    dadurch ueber den Bruch hinweg vergleichbar, die Rohrate waere es nicht.
    Verbleibende Limitation: Eine Verschiebung in der ZUSAMMENSETZUNG der
    erfassten Delikte, die einzelne Stadtteile staerker trifft als andere,
    kuerzt sich nicht heraus (Kap. 6.3).

    Kein Leakage: Das Fenster endet strikt im Vormonat; fuer den ersten
    Analysemonat 2015-01 werden die Delikte aus 2014-01 bis 2014-12 verwendet.

    Zusaetzlich wird `crime_rate_raw` (Delikte je 1.000 Einwohner, gleiches
    Fenster) zurueckgegeben - NUR als deskriptive Groesse fuer Kapitel 5.1,
    nicht als Modellmerkmal, weil sie den Strukturbruch 2018 enthaelt.
    """
    modern_path = RAW_DIR / "crime_raw.parquet"
    hist_path   = RAW_DIR / "crime_historisch_raw.parquet"
    require(modern_path, "01_fetch.py mit DOWNLOAD_CRIME=True ausfuehren.")
    require(hist_path,   "01_fetch.py mit DOWNLOAD_CRIME_HISTORISCH=True ausfuehren.")

    teile = [_crime_monatlich_historisch(hist_path), _crime_monatlich_modern(modern_path)]
    monatlich = pd.concat(teile, ignore_index=True)
    # Sicherheitsnetz gegen Ueberlappung der beiden Quellen
    monatlich = (monatlich.groupby(["neighborhood", "jahr", "monat"])["delikte"]
                          .max().reset_index())

    # Vollstaendiges Raster Neighborhood x Monat (Monate ohne Delikt = 0)
    jahre = list(range(int(monatlich["jahr"].min()), int(monatlich["jahr"].max()) + 1))
    idx = pd.MultiIndex.from_product(
        [sorted(monatlich["neighborhood"].unique()), jahre, range(1, 13)],
        names=["neighborhood", "jahr", "monat"])
    raster = (monatlich.set_index(["neighborhood", "jahr", "monat"])
                       .reindex(idx).fillna({"delikte": 0}).reset_index())

    # Rollierendes Fenster, endend im VORMONAT (shift(1) VOR rolling)
    raster = raster.sort_values(["neighborhood", "jahr", "monat"])
    raster["delikte_fenster"] = (
        raster.groupby("neighborhood")["delikte"]
              .transform(lambda s: s.shift(1).rolling(CRIME_FENSTER_MONATE).sum()))
    raster = raster.dropna(subset=["delikte_fenster"])

    # Einwohner je Neighborhood und Jahr (gleicher ACS-Versatz wie im Modell)
    bev = _bevoelkerung_je_jahr(nb_per_year, sorted(raster["jahr"].unique()))
    raster = raster.merge(bev, on=["neighborhood", "jahr"], how="left")
    raster = raster[raster["total_population"] > 0]

    # Stadtweite Referenz je Monat (Summe ueber alle Neighborhoods mit Werten)
    stadt = (raster.groupby(["jahr", "monat"])
                   .agg(stadt_delikte=("delikte_fenster", "sum"),
                        stadt_bev=("total_population", "sum")).reset_index())
    raster = raster.merge(stadt, on=["jahr", "monat"], how="left")

    raster["crime_rate_raw"] = (raster["delikte_fenster"]
                                / raster["total_population"] * 1000).round(3)
    stadt_rate = raster["stadt_delikte"] / raster["stadt_bev"] * 1000
    raster["crime_index"] = (raster["crime_rate_raw"] / stadt_rate).round(4)

    out = raster[["neighborhood", "jahr", "monat", "crime_index", "crime_rate_raw"]]
    print(f"  Kriminalitaetsindex: {out['neighborhood'].nunique()} Neighborhoods "
          f"x {len(out.groupby(['jahr','monat']))} Monate "
          f"({out['jahr'].min()}-{out['jahr'].max()})")
    print(f"    Index Median {out['crime_index'].median():.2f}, "
          f"Spanne {out['crime_index'].min():.2f}-{out['crime_index'].max():.2f}")
    return out


def spatial_join_land_use(parcels: pd.DataFrame, neighborhoods_gdf):
    import geopandas as gpd
    from shapely.geometry import shape

    parcels = parcels.copy()
    parcels["geometry"] = parcels["the_geom"].apply(
        lambda g: shape(g).centroid if isinstance(g, dict) else None)
    parcels = parcels.dropna(subset=["geometry"])
    keep = ["blklot", "yrbuilt", "landuse", "resunits", "st_area_sh", "geometry"]
    parcels_gdf = gpd.GeoDataFrame(parcels[keep], geometry="geometry", crs="EPSG:4326")

    print("  Spatial Join (Centroid -> Neighborhood-Polygon)...")
    joined = gpd.sjoin(parcels_gdf, neighborhoods_gdf, how="left", predicate="within")
    joined = joined.drop(columns=["index_right"], errors="ignore")
    matched = joined["neighborhood"].notna().sum()
    print(f"  Match-Rate: {matched/len(joined)*100:.1f}% ({matched:,}/{len(joined):,})")
    return joined.dropna(subset=["neighborhood"])


def aggregate_land_use_to_neighborhood(gdf) -> pd.DataFrame:
    df = pd.DataFrame(gdf.drop(columns=["geometry"], errors="ignore"))
    df["is_residential"]      = df["landuse"].isin(RESIDENTIAL).astype(int)
    df["is_high_risk"]        = df["landuse"].isin(HIGH_RISK_COMMERCIAL).astype(int)
    df["has_yrbuilt"]         = df["yrbuilt"].notna().astype(int)
    df["is_pre1940"]          = (df["yrbuilt"] < 1940).fillna(False).astype(int)
    df["is_pre1960"]          = (df["yrbuilt"] < 1960).fillna(False).astype(int)
    df["high_risk_area_sqft"] = df["st_area_sh"].fillna(0) * df["is_high_risk"]

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
    risk_anteil = agg["high_risk_commercial_area_sqft"].sum() / agg["total_area_sqft"].sum() * 100
    print(f"  {len(agg)} Neighborhoods | Parzellen: {agg['parcel_count'].sum():,} "
          f"| Hochrisiko-Anteil: {risk_anteil:.1f}%")
    return agg


def load_neighborhoods_gdf():
    import geopandas as gpd
    path = RAW_DIR / "neighborhoods.geojson"
    require(path, "01_fetch.py mit DOWNLOAD_NEIGHBORHOODS=True ausfuehren.")
    gdf = gpd.read_file(path)
    gdf["neighborhood"] = gdf["nhood"].str.strip().str.title()
    gdf = gdf[["neighborhood", "geometry"]].to_crs("EPSG:4326")
    print(f"  {len(gdf)} Neighborhoods aus {path.relative_to(ROOT)}")
    return gdf


def _lade_acs_alle_jahre(crosswalk: pd.DataFrame) -> dict[int, pd.DataFrame]:
    nb_per_year = {}
    for year in ACS_YEARS:
        path = RAW_DIR / f"acs_tracts_{year}.csv"
        require(path, "01_fetch.py mit DOWNLOAD_ACS=True ausfuehren.")
        nb = aggregate_acs_to_neighborhood(
            pd.read_csv(path, dtype={"geoid": str}), crosswalk)
        nb_per_year[year] = nb
        nb.to_csv(PROCESSED_DIR / f"acs_neighborhoods_{year}.csv", index=False)
        print(f"  ACS {year}: {len(nb)} Neighborhoods")
    return nb_per_year


def _join_crime(base: pd.DataFrame,
                nb_per_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """Join des monatlichen Kriminalitaetsindex auf die Einsatz-Ebene.

    Es gibt bewusst KEINEN statischen Fallback mehr: Ein statischer Join wuerde
    Delikte aus dem Testzeitraum in die Trainingsmerkmale tragen (Leakage) und
    zugleich jede Zeitvarianz beseitigen. Fehlen die Rohdaten, bricht die
    Pipeline mit einer Anleitung ab.
    """
    crime = berechne_kriminalitaetsindex(nb_per_year)
    crime.to_csv(PROCESSED_DIR / "crime_index_monatlich.csv", index=False)
    vorher = len(base)
    out = base.merge(crime, left_on=["neighborhood", "year", "month"],
                     right_on=["neighborhood", "jahr", "monat"],
                     how="left").drop(columns=["jahr", "monat"])
    assert len(out) == vorher, "Crime-Join hat Zeilen dupliziert (1:n-Beziehung!)."
    fehlend = out["crime_index"].isna().mean()
    print(f"  Join auf Einsatz-Ebene: {(1-fehlend)*100:.1f}% der Einsaetze mit "
          f"Index (fehlend v. a. vor {int(crime['jahr'].min())})")
    return out


def _join_landuse(base: pd.DataFrame) -> pd.DataFrame:
    # Cache: Der Spatial Join ist der teuerste Schritt und deterministisch
    # (statischer Snapshot 2020). Liegt die aggregierte Tabelle bereits vor,
    # wird sie wiederverwendet; zum Neuberechnen CSV loeschen.
    cache = PROCESSED_DIR / "land_use_2020_neighborhoods.csv"
    if cache.exists():
        print(f"  Nutze vorhandene Aggregation: {cache.name} (loeschen zum Neuberechnen)")
        lu = pd.read_csv(cache)
        return base.merge(lu, on="neighborhood", how="left")
    path = RAW_DIR / "land_use_2020_raw.parquet"
    require(path, "01_fetch.py mit DOWNLOAD_LAND_USE_2020=True ausfuehren.")
    parcels = pd.read_parquet(path)
    if "st_area_sh" not in parcels.columns:
        raise RuntimeError("land_use_2020_raw.parquet enthaelt kein st_area_sh.")
    lu = aggregate_land_use_to_neighborhood(
        spatial_join_land_use(parcels, load_neighborhoods_gdf()))
    lu.to_csv(cache, index=False)
    return base.merge(lu, on="neighborhood", how="left")


def run_join():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    print("Schritt 2: Daten verarbeiten und joinen\n")

    print("[1/5] SFFD: Rohdaten + Zeit-Features...")
    require(RAW_DIR / "fire_incidents.parquet", "01_fetch.py mit DOWNLOAD_SFFD=True.")
    sffd = prepare_sffd(pd.read_parquet(RAW_DIR / "fire_incidents.parquet"))
    print(f"  {len(sffd):,} Einsaetze | Jahre: {int(sffd['year'].min())}-{int(sffd['year'].max())}")

    print(f"\n[2/5] ACS: Aggregation Tract -> Neighborhood ({len(ACS_YEARS)} Jahrgaenge)...")
    require(RAW_DIR / "crosswalk.csv", "01_fetch.py mit DOWNLOAD_CROSSWALK=True.")
    crosswalk = pd.read_csv(RAW_DIR / "crosswalk.csv", dtype={"geoid": str})
    nb_per_year = _lade_acs_alle_jahre(crosswalk)

    print(f"\n[3/5] Zeitbewusster Join (Einsatz -> letzter publizierter "
          f"ACS-Snapshot, Versatz {ACS_PUBLIKATIONS_LAG} Jahr)...")
    base = year_aware_join(sffd, nb_per_year)

    print(f"\n[4/5] Crime: relativer Index (Fenster {CRIME_FENSTER_MONATE} Monate, "
          f"endend im Vormonat) + Join...")
    base = _join_crime(base, nb_per_year)

    print("\n[5/5] Land Use: Spatial Join + Aggregation + Join...")
    base = _join_landuse(base)

    out_path = PROCESSED_DIR / "sf_fire_incidents_base.parquet"
    base.to_parquet(out_path, index=False)
    print(f"\n=> {out_path.relative_to(ROOT)}  "
          f"({len(base):,} Zeilen | {len(base.columns)} Spalten)")
    print("\n  Naechster Schritt: python pipeline/03_features.py")
    return base


if __name__ == "__main__":
    run_join()
