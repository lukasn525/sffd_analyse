"""
Schritt 2: Daten verarbeiten und joinen
=======================================
Liest die Rohdaten aus data/raw/, aggregiert auf Neighborhood-Ebene,
fuehrt Spatial Join (Land Use) und zeitbewussten Join (SFFD <-> ACS) durch.

KEINE abgeleiteten Variablen (pct_*, Raten) - die kommen in Schritt 3.

Inputs (von 01_fetch_data.py):
  data/raw/fire_incidents.parquet
  data/raw/crosswalk.csv
  data/raw/acs_tracts_{year}.csv
  data/raw/crime_raw.parquet
  data/raw/land_use_2020_raw.parquet
  data/raw/neighborhoods.geojson

Outputs:
  data/processed/acs_neighborhoods_{year}.csv     (Zwischenstaende)
  data/processed/crime_neighborhoods.csv
  data/processed/land_use_2020_neighborhoods.csv
  data/processed/sf_fire_incidents_base.parquet   <- HAUPT-OUTPUT

Ausfuehren:
  python scripts/02_join_data.py
"""

import io
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

# ── Pfade ─────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).parent.parent
RAW_DIR       = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"

ACS_YEARS = [2009, 2014, 2019, 2021, 2023]

# ── Crime-Kategorien ──────────────────────────────────────────────────────────
VIOLENT_CATEGORIES  = {"Assault", "Homicide", "Robbery", "Rape",
                       "Kidnapping", "Weapons Offenses"}
PROPERTY_CATEGORIES = {"Burglary", "Theft", "Motor Vehicle Theft",
                       "Arson", "Vandalism"}

# ── Land-Use-Kategorien ───────────────────────────────────────────────────────
HIGH_RISK_COMMERCIAL = {"RETAIL/ENT", "PDR"}
RESIDENTIAL          = {"RESIDENT", "MIXRES"}


# ══════════════════════════════════════════════════════════════════════════════
# SFFD: Roh-Aufbereitung (Zeit-Features, neighborhood-Normalisierung)
# ══════════════════════════════════════════════════════════════════════════════

def prepare_sffd(sffd_df: pd.DataFrame) -> pd.DataFrame:
    df = sffd_df.copy()
    df["response_time_min"] = (
        df["arrival_dttm"] - df["alarm_dttm"]
    ).dt.total_seconds() / 60
    df = df[(df["response_time_min"] >= 0) & (df["response_time_min"] <= 60)]

    df["year"]         = df["incident_date"].dt.year
    df["month"]        = df["incident_date"].dt.month
    df["hour"]         = df["alarm_dttm"].dt.hour
    df["weekday"]      = df["alarm_dttm"].dt.dayofweek
    df["is_weekend"]   = df["weekday"].isin([5, 6]).astype(int)
    df["is_night"]     = ((df["hour"] >= 22) | (df["hour"] <= 5)).astype(int)
    df["neighborhood"] = df["neighborhood_district"].str.strip().str.title()
    return df


# ══════════════════════════════════════════════════════════════════════════════
# ACS: Tract -> Neighborhood (nur Rohkomponenten, keine Raten)
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_acs_to_neighborhood(acs_df: pd.DataFrame,
                                  crosswalk_df: pd.DataFrame) -> pd.DataFrame:
    merged = acs_df.merge(crosswalk_df, on="geoid", how="left").dropna(subset=["neighborhood"])

    rows = []
    for hood, grp in merged.groupby("neighborhood"):
        pop_total = grp["total_population"].sum()
        rows.append({
            "neighborhood": hood,
            "total_population": int(pop_total),
            "median_household_income": (
                (grp["median_household_income"] * grp["total_population"]).sum() / pop_total
            ) if pop_total > 0 else pd.NA,
            "median_gross_rent": (
                (grp["median_gross_rent"] * grp["total_population"]).sum() / pop_total
            ) if pop_total > 0 else pd.NA,
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
    for col in ["poverty_below", "poverty_universe_total", "bachelor_degree_count",
                "education_universe_total", "vacant_housing_units", "total_housing_units"]:
        nb[col] = pd.to_numeric(nb[col], errors="coerce").round(0)
    return nb


# ══════════════════════════════════════════════════════════════════════════════
# Zeitbewusster ACS-Join (jeder Einsatz -> naechster ACS-Snapshot)
# ══════════════════════════════════════════════════════════════════════════════

def nearest_acs_year(incident_year: int, acs_years: list[int]) -> int:
    return min(acs_years, key=lambda y: abs(y - incident_year))


def year_aware_join(sffd_df: pd.DataFrame,
                    nb_per_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    acs_years = sorted(nb_per_year.keys())
    sffd_df = sffd_df.copy()
    sffd_df["acs_year"] = sffd_df["year"].apply(lambda y: nearest_acs_year(int(y), acs_years))

    print(f"\n  Einsaetze nach zugeordnetem ACS-Snapshot:")
    mapping = sffd_df.groupby("acs_year")["year"].agg(
        lambda x: f"{x.min()}-{x.max()} ({len(x):,} Einsaetze)"
    )
    for acs_y, info in mapping.items():
        print(f"    ACS {acs_y}  -> Einsatzjahre {info}")

    parts = []
    for acs_year, group in sffd_df.groupby("acs_year"):
        nb = nb_per_year[acs_year].copy()
        merged = group.merge(nb, on="neighborhood", how="left")
        parts.append(merged)
    final = pd.concat(parts).sort_index()

    acs_num_cols = [
        "total_population", "median_household_income", "median_gross_rent",
        "poverty_below", "poverty_universe_total", "bachelor_degree_count",
        "education_universe_total", "vacant_housing_units", "total_housing_units",
    ]
    for col in acs_num_cols:
        if col in final.columns:
            final[col] = pd.to_numeric(final[col], errors="coerce").round(0)
    return final


# ══════════════════════════════════════════════════════════════════════════════
# Crime: Aggregation auf Neighborhood-Ebene (nur Rohcounts)
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_crime_to_neighborhood(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)

    # Robust gegen zwei Schemas: rohes API-Schema (analysis_neighborhood)
    # ODER bereits aufbereitetes Schema (neighborhood).
    if "neighborhood" not in df.columns and "analysis_neighborhood" in df.columns:
        df["neighborhood"] = df["analysis_neighborhood"].str.strip().str.title()
    else:
        df["neighborhood"] = df["neighborhood"].str.strip().str.title()

    df["incident_category"] = df["incident_category"].fillna("").str.strip()
    df = df[df["incident_category"] != ""].copy()

    if "is_violent" not in df.columns:
        df["is_violent"] = df["incident_category"].isin(VIOLENT_CATEGORIES).astype(int)
    if "is_property" not in df.columns:
        df["is_property"] = df["incident_category"].isin(PROPERTY_CATEGORIES).astype(int)

    df["violent_count"]  = df["count"] * df["is_violent"]
    df["property_count"] = df["count"] * df["is_property"]

    agg = df.groupby("neighborhood").agg(
        total_crimes         =("count",          "sum"),
        violent_crime_count  =("violent_count",  "sum"),
        property_crime_count =("property_count", "sum"),
    ).reset_index()

    for col in ["total_crimes", "violent_crime_count", "property_crime_count"]:
        agg[col] = agg[col].astype(int)

    print(f"  Neighborhoods mit Crime-Daten: {len(agg)}")
    print(f"  Gesamtdelikte: {agg['total_crimes'].sum():,}  "
          f"(davon Gewalt: {agg['violent_crime_count'].sum():,})")
    return agg


# ══════════════════════════════════════════════════════════════════════════════
# Land Use: Spatial Join + Aggregation (nur Rohsummen)
# ══════════════════════════════════════════════════════════════════════════════

def spatial_join_land_use(parcels_df: pd.DataFrame,
                           neighborhoods_gdf):
    import geopandas as gpd
    from shapely.geometry import shape

    def _centroid(g):
        try:
            return shape(g).centroid if isinstance(g, dict) else None
        except Exception:
            return None

    parcels_df = parcels_df.copy()
    parcels_df["geometry"] = parcels_df["the_geom"].apply(_centroid)
    parcels_df = parcels_df.dropna(subset=["geometry"])

    keep_cols = ["blklot", "yrbuilt", "landuse", "resunits", "st_area_sh", "geometry"]
    parcels_gdf = gpd.GeoDataFrame(
        parcels_df[[c for c in keep_cols if c in parcels_df.columns]],
        geometry="geometry", crs="EPSG:4326",
    )

    print("  Spatial Join (Centroid -> Neighborhood-Polygon)...")
    joined = gpd.sjoin(parcels_gdf, neighborhoods_gdf, how="left", predicate="within")
    joined = joined.drop(columns=["index_right"], errors="ignore")

    n_matched = joined["neighborhood"].notna().sum()
    print(f"  Match-Rate: {n_matched/len(joined)*100:.1f}%  "
          f"({n_matched:,}/{len(joined):,} Parzellen)")
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
        parcel_count                   =("blklot",             "count"),
        yrbuilt_count                  =("has_yrbuilt",        "sum"),
        pre1940_count                  =("is_pre1940",         "sum"),
        pre1960_count                  =("is_pre1960",         "sum"),
        total_resunits                 =("resunits",           "sum"),
        residential_count              =("is_residential",     "sum"),
        total_area_sqft                =("st_area_sh",         "sum"),
        high_risk_commercial_area_sqft =("high_risk_area_sqft","sum"),
    ).reset_index()

    for col in ["parcel_count", "yrbuilt_count", "pre1940_count",
                "pre1960_count", "residential_count"]:
        agg[col] = agg[col].astype(int)
    agg["total_resunits"] = agg["total_resunits"].round(0)

    total_area = agg["total_area_sqft"].sum()
    risk_area  = agg["high_risk_commercial_area_sqft"].sum()
    print(f"  {len(agg)} Neighborhoods aggregiert.")
    print(f"  Parzellen gesamt: {agg['parcel_count'].sum():,}  "
          f"| SF-weiter Hochrisiko-Anteil: {risk_area/total_area*100:.1f}% der Flaeche")
    return agg


# ══════════════════════════════════════════════════════════════════════════════
# I/O Helpers
# ══════════════════════════════════════════════════════════════════════════════

def load_neighborhoods_gdf():
    import geopandas as gpd
    path = RAW_DIR / "neighborhoods.geojson"
    if not path.exists():
        raise FileNotFoundError(f"{path} nicht gefunden. 01_fetch_data.py mit DOWNLOAD_NEIGHBORHOODS=True ausfuehren.")
    gdf = gpd.read_file(path)
    gdf["neighborhood"] = gdf["nhood"].str.strip().str.title()
    gdf = gdf[["neighborhood", "geometry"]].to_crs("EPSG:4326")
    print(f"  {len(gdf)} Neighborhoods aus {path.relative_to(ROOT)}")
    return gdf


def require(path: Path, hint: str):
    if not path.exists():
        raise FileNotFoundError(f"{path.relative_to(ROOT)} nicht gefunden. {hint}")


# ══════════════════════════════════════════════════════════════════════════════
# HAUPTPIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_join():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("  Schritt 2: Daten verarbeiten und joinen")
    print("=" * 80)

    # ── [1/5] SFFD laden + Zeit-Features ──────────────────────────────────────
    print("\n[1/5] SFFD: Rohdaten laden + Zeit-Features...")
    sffd_path = RAW_DIR / "fire_incidents.parquet"
    require(sffd_path, "01_fetch_data.py mit DOWNLOAD_SFFD=True ausfuehren.")
    sffd = prepare_sffd(pd.read_parquet(sffd_path))
    print(f"  {len(sffd):,} Einsaetze  |  Jahre: {int(sffd['year'].min())}-{int(sffd['year'].max())}")

    # ── [2/5] ACS aggregieren (pro Jahr) ──────────────────────────────────────
    print(f"\n[2/5] ACS: Aggregation Tract -> Neighborhood ({len(ACS_YEARS)} Jahrgaenge)...")
    cw_path = RAW_DIR / "crosswalk.csv"
    require(cw_path, "01_fetch_data.py mit DOWNLOAD_CROSSWALK=True ausfuehren.")
    crosswalk = pd.read_csv(cw_path, dtype={"geoid": str})

    nb_per_year = {}
    for year in ACS_YEARS:
        acs_path = RAW_DIR / f"acs_tracts_{year}.csv"
        require(acs_path, f"01_fetch_data.py mit DOWNLOAD_ACS=True ausfuehren.")
        acs_df = pd.read_csv(acs_path, dtype={"geoid": str})
        nb = aggregate_acs_to_neighborhood(acs_df, crosswalk)
        nb_per_year[year] = nb
        out = PROCESSED_DIR / f"acs_neighborhoods_{year}.csv"
        nb.to_csv(out, index=False)
        print(f"  ACS {year}: {len(nb)} Neighborhoods -> {out.relative_to(ROOT)}")

    # ── [3/5] Zeitbewusster Join SFFD <-> ACS ─────────────────────────────────
    print("\n[3/5] Zeitbewusster Join (Einsatz -> naechster ACS-Snapshot)...")
    base = year_aware_join(sffd, nb_per_year)

    # ── [4/5] Crime: aggregieren + joinen ─────────────────────────────────────
    print("\n[4/5] Crime: Aggregation + Join...")
    crime_raw_path = RAW_DIR / "crime_raw.parquet"
    require(crime_raw_path, "01_fetch_data.py mit DOWNLOAD_CRIME=True ausfuehren.")
    crime_raw = pd.read_parquet(crime_raw_path)
    crime_agg = aggregate_crime_to_neighborhood(crime_raw)
    crime_agg.to_csv(PROCESSED_DIR / "crime_neighborhoods.csv", index=False)
    base = base.merge(crime_agg, on="neighborhood", how="left")

    # ── [5/5] Land Use: Spatial Join + aggregieren + joinen ───────────────────
    print("\n[5/5] Land Use: Spatial Join + Aggregation + Join...")
    lu_raw_path = RAW_DIR / "land_use_2020_raw.parquet"
    require(lu_raw_path, "01_fetch_data.py mit DOWNLOAD_LAND_USE_2020=True ausfuehren.")
    parcels = pd.read_parquet(lu_raw_path)
    if "st_area_sh" not in parcels.columns:
        raise RuntimeError(
            "land_use_2020_raw.parquet enthaelt kein st_area_sh - "
            "01_fetch_data.py mit DOWNLOAD_LAND_USE_2020=True erneut ausfuehren."
        )
    neighborhoods = load_neighborhoods_gdf()
    lu_joined = spatial_join_land_use(parcels, neighborhoods)
    lu_agg = aggregate_land_use_to_neighborhood(lu_joined)
    lu_agg.to_csv(PROCESSED_DIR / "land_use_2020_neighborhoods.csv", index=False)
    base = base.merge(lu_agg, on="neighborhood", how="left")

    # ── Output ────────────────────────────────────────────────────────────────
    out_path = PROCESSED_DIR / "sf_fire_incidents_base.parquet"
    base.to_parquet(out_path, index=False)

    print("\n" + "=" * 80)
    print("  Schritt 2 abgeschlossen.")
    print("=" * 80)
    print(f"  => {out_path.relative_to(ROOT)}  "
          f"({len(base):,} Zeilen  |  {len(base.columns)} Spalten)")

    print("\n  Roh-Spalten und fehlende Werte:")
    raw_cols = [
        # Crime
        "total_crimes", "violent_crime_count", "property_crime_count",
        # Land Use
        "parcel_count", "yrbuilt_count", "pre1940_count", "pre1960_count",
        "total_resunits", "residential_count",
        "total_area_sqft", "high_risk_commercial_area_sqft",
        # ACS
        "total_population", "median_household_income", "median_gross_rent",
        "poverty_below", "poverty_universe_total",
        "bachelor_degree_count", "education_universe_total",
        "vacant_housing_units", "total_housing_units",
    ]
    for col in raw_cols:
        if col in base.columns:
            null_pct = base[col].isna().mean() * 100
            print(f"    {col:<40} {null_pct:5.1f}% fehlend")

    print("\n  Naechster Schritt: python scripts/03_compute_features.py")
    return base


if __name__ == "__main__":
    run_join()
