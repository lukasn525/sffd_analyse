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

VIOLENT_CATEGORIES   = {"Assault", "Homicide", "Robbery", "Rape",
                        "Kidnapping", "Weapons Offenses"}
PROPERTY_CATEGORIES  = {"Burglary", "Theft", "Motor Vehicle Theft",
                        "Arson", "Vandalism"}
HIGH_RISK_COMMERCIAL = {"RETAIL/ENT", "PDR"}
RESIDENTIAL          = {"RESIDENT", "MIXRES"}

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
    sffd["acs_year"] = sffd["year"].apply(
        lambda y: min(acs_years, key=lambda a: abs(a - int(y)))
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


def aggregate_crime_to_neighborhood(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)
    quelle = "neighborhood" if "neighborhood" in df.columns else "analysis_neighborhood"
    df["neighborhood"] = df[quelle].str.strip().str.title()
    df["incident_category"] = df["incident_category"].fillna("").str.strip()
    df = df[df["incident_category"] != ""]

    df["violent_count"]  = df["count"] * df["incident_category"].isin(VIOLENT_CATEGORIES)
    df["property_count"] = df["count"] * df["incident_category"].isin(PROPERTY_CATEGORIES)

    agg = df.groupby("neighborhood").agg(
        total_crimes         =("count",          "sum"),
        violent_crime_count  =("violent_count",  "sum"),
        property_crime_count =("property_count", "sum"),
    ).reset_index()
    for col in ["total_crimes", "violent_crime_count", "property_crime_count"]:
        agg[col] = agg[col].astype(int)
    print(f"  {len(agg)} Neighborhoods | Delikte gesamt: {agg['total_crimes'].sum():,} "
          f"(Gewalt: {agg['violent_crime_count'].sum():,})")
    return agg


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


def _join_crime(base: pd.DataFrame) -> pd.DataFrame:
    path = RAW_DIR / "crime_raw.parquet"
    require(path, "01_fetch.py mit DOWNLOAD_CRIME=True ausfuehren.")
    crime = aggregate_crime_to_neighborhood(pd.read_parquet(path))
    crime.to_csv(PROCESSED_DIR / "crime_neighborhoods.csv", index=False)
    return base.merge(crime, on="neighborhood", how="left")


def _join_landuse(base: pd.DataFrame) -> pd.DataFrame:
    path = RAW_DIR / "land_use_2020_raw.parquet"
    require(path, "01_fetch.py mit DOWNLOAD_LAND_USE_2020=True ausfuehren.")
    parcels = pd.read_parquet(path)
    if "st_area_sh" not in parcels.columns:
        raise RuntimeError("land_use_2020_raw.parquet enthaelt kein st_area_sh.")
    lu = aggregate_land_use_to_neighborhood(
        spatial_join_land_use(parcels, load_neighborhoods_gdf()))
    lu.to_csv(PROCESSED_DIR / "land_use_2020_neighborhoods.csv", index=False)
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

    print("\n[3/5] Zeitbewusster Join (Einsatz -> naechster ACS-Snapshot)...")
    base = year_aware_join(sffd, nb_per_year)

    print("\n[4/5] Crime: Aggregation + Join...")
    base = _join_crime(base)

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
