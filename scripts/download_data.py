"""
SFFD + ACS Datenpipeline  (Längsschnitt-Version)
Bachelorarbeit: Sozioökonomische Einflüsse auf Feuerwehreinsätze in SF

Lädt und verknüpft drei Datenquellen:
  1. SFFD Fire Incidents  → DataSF SODA API (data.sfgov.org)
  2. Neighborhood-Crosswalk → DataSF (Census Tract → Neighborhood)
  3. ACS 5-Year Estimates → US Census API – mehrere Jahrgänge

Join-Strategie: Jeder Einsatz wird mit dem zeitlich nächsten ACS-Snapshot
verknüpft. So werden Veränderungen sozioökonomischer Merkmale über die Zeit
korrekt berücksichtigt (keine Annahme zeitlicher Stabilität).

Ergebnis:
  data/raw/fire_incidents.parquet           – bereinigte SFFD-Rohdaten
  data/raw/crosswalk.csv                    – Census Tract ↔ Neighborhood
  data/raw/acs_tracts_{year}.csv            – ACS-Daten je Jahrgang
  data/processed/acs_neighborhoods_{year}.csv – ACS aggregiert je Jahrgang
  data/processed/sffd_acs_joined.parquet    – Analysedatei mit acs_year-Spalte

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

# ── ACS-Jahrgänge ─────────────────────────────────────────────────────────────
# Jeder Einsatz wird dem zeitlich nächsten Snapshot zugeordnet.
# ACS 5-Year auf Tract-Ebene ist frühestens ab 2009 verfügbar.
ACS_YEARS = [2009, 2014, 2019, 2021, 2023]

# ── Download-Steuerung ────────────────────────────────────────────────────────
DOWNLOAD_SFFD      = False   # data/raw/fire_incidents.parquet
DOWNLOAD_CROSSWALK = True    # data/raw/crosswalk.csv
DOWNLOAD_ACS       = True    # data/raw/acs_tracts_{year}.csv je Jahrgang

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
    print(f"  {len(df):,} Zeilen  |  Jahre: {df['year'].min()}–{df['year'].max()}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SCHRITT 2: Neighborhood → Census Tract Crosswalk (DataSF sevw-6tgi)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_neighborhood_crosswalk(app_token: str = None) -> pd.DataFrame:
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
    print(f"  {len(df)} Tract-Neighborhood-Paare, {df['neighborhood'].nunique()} Neighborhoods")
    return df


def load_crosswalk() -> pd.DataFrame:
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
# SCHRITT 3: ACS 5-Year Estimates – mehrere Jahrgänge
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

# Variablen in Gruppen aufgeteilt – falls der Gesamt-Request 400 zurückgibt,
# werden die Gruppen einzeln abgerufen. Nicht verfügbare Gruppen → NaN-Spalten.
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
    """Einzelner Census-API-Request. Gibt None zurück wenn 400/404."""
    var_str = ",".join(["NAME"] + var_codes)
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={var_str}"
        f"&for=tract:*"
        f"&in=state:06%20county:075"
        f"&key={api_key}"
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
    """
    Lädt ACS 5-Year für alle Census Tracts in San Francisco.
    Strategie: erst Vollversuch; bei 400 werden Variablengruppen einzeln
    abgerufen. Nicht verfügbare Gruppen werden als NaN-Spalten eingefügt.
    """
    # Versuch 1: alle Variablen in einem Request
    all_codes = list(ACS_VARIABLES.keys())
    df = _acs_request(year, api_key, all_codes)

    if df is None:
        # Versuch 2: gruppenweise laden und mergen
        print(f"  ACS {year}: Vollständiger Request fehlgeschlagen → lade gruppenweise...")
        df = None
        for group_name, codes in ACS_VAR_GROUPS.items():
            part = _acs_request(year, api_key, codes)
            if part is None:
                print(f"    Gruppe '{group_name}' nicht verfügbar → NaN")
                continue
            df = part if df is None else df.merge(part, on="geoid", how="outer")

        if df is None:
            raise RuntimeError(f"ACS {year}: Keine einzige Variablengruppe konnte geladen werden.")

    df.rename(columns=ACS_VARIABLES, inplace=True)

    for col in ACS_VARIABLES.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] < -999, col] = pd.NA
        else:
            df[col] = pd.NA  # Spalte existiert nicht in diesem Jahr

    print(f"  ACS {year}: {len(df)} Census Tracts | "
          f"fehlende Vars: {[c for c in ACS_VARIABLES.values() if df[c].isna().all()]or 'keine'}")
    return df[[c for c in OUTPUT_COLS if c in df.columns]]


def load_acs_multi(years: list[int]) -> dict[int, pd.DataFrame]:
    """
    Gibt ein dict {year: tract_df} zurück.
    Lädt frisch oder nutzt Cache je nach DOWNLOAD_ACS.
    """
    result = {}
    for year in years:
        path = RAW_DIR / f"acs_tracts_{year}.csv"
        if DOWNLOAD_ACS:
            df = fetch_acs_sf_tracts(year=year, api_key=CENSUS_API_KEY)
            df.to_csv(path, index=False)
            print(f"  Gespeichert: {path.relative_to(ROOT)}")
        else:
            if not path.exists():
                raise FileNotFoundError(
                    f"{path} nicht gefunden. DOWNLOAD_ACS=True setzen oder "
                    f"ACS_YEARS auf vorhandene Jahrgänge beschränken."
                )
            print(f"  Verwende Cache: {path.relative_to(ROOT)}")
            df = pd.read_csv(path, dtype={"geoid": str})
        result[year] = df
    return result


# ══════════════════════════════════════════════════════════════════════════════
# SCHRITT 4: Aggregation auf Neighborhood-Ebene
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_acs_to_neighborhood(acs_df: pd.DataFrame, crosswalk_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregiert ACS Tract-Daten auf Neighborhood-Ebene (bevölkerungsgewichtet)."""
    merged = acs_df.merge(crosswalk_df, on="geoid", how="left").dropna(subset=["neighborhood"])

    result_rows = []
    for hood, grp in merged.groupby("neighborhood"):
        pop_total = grp["total_population"].sum()
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

        poverty_universe = grp["poverty_universe_total"].sum()
        row["poverty_rate"] = (
            grp["poverty_below"].sum() / poverty_universe * 100
        ) if poverty_universe > 0 else pd.NA

        education_universe = grp["education_universe_total"].sum()
        row["bachelor_rate"] = (
            grp["bachelor_degree_count"].sum() / education_universe * 100
        ) if education_universe > 0 else pd.NA

        housing_total = grp["total_housing_units"].sum()
        row["vacancy_rate"] = (
            grp["vacant_housing_units"].sum() / housing_total * 100
        ) if housing_total > 0 else pd.NA

        result_rows.append(row)

    nb = pd.DataFrame(result_rows)

    for col in ["median_household_income", "median_gross_rent"]:
        nb[col] = pd.to_numeric(nb[col], errors="coerce").round(0).astype("Int64")
    for col in ["poverty_rate", "bachelor_rate", "vacancy_rate"]:
        nb[col] = pd.to_numeric(nb[col], errors="coerce").round(2)

    return nb


def aggregate_all_years(acs_per_year: dict[int, pd.DataFrame],
                        crosswalk: pd.DataFrame) -> dict[int, pd.DataFrame]:
    """Aggregiert alle ACS-Jahrgänge auf Neighborhood-Ebene und speichert sie."""
    nb_per_year = {}
    for year, acs_df in acs_per_year.items():
        nb = aggregate_acs_to_neighborhood(acs_df, crosswalk)
        nb_per_year[year] = nb
        out_path = PROCESSED_DIR / f"acs_neighborhoods_{year}.csv"
        nb.to_csv(out_path, index=False)
        print(f"  ACS {year}: {len(nb)} Neighborhoods → {out_path.relative_to(ROOT)}")
    return nb_per_year


# ══════════════════════════════════════════════════════════════════════════════
# SCHRITT 5: Zeitbewusster Join (jeder Einsatz → nächster ACS-Snapshot)
# ══════════════════════════════════════════════════════════════════════════════

def nearest_acs_year(incident_year: int, acs_years: list[int]) -> int:
    """Gibt den ACS-Jahrgang zurück, der dem Einsatzjahr am nächsten liegt."""
    return min(acs_years, key=lambda y: abs(y - incident_year))


def year_aware_join(sffd_df: pd.DataFrame,
                    nb_per_year: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """
    Verknüpft jeden SFFD-Einsatz mit dem zeitlich nächsten ACS-Neighborhood-
    Snapshot. Fügt Spalte 'acs_year' hinzu, die den verwendeten Snapshot markiert.
    """
    acs_years = sorted(nb_per_year.keys())
    sffd_df = sffd_df.copy()
    sffd_df["acs_year"] = sffd_df["year"].apply(
        lambda y: nearest_acs_year(int(y), acs_years)
    )

    print(f"\n  Einsätze nach zugeordnetem ACS-Snapshot:")
    mapping = sffd_df.groupby("acs_year")["year"].agg(
        lambda x: f"{x.min()}–{x.max()} ({len(x):,} Einsätze)"
    )
    for acs_y, info in mapping.items():
        print(f"    ACS {acs_y}  →  Einsatzjahre {info}")

    parts = []
    for acs_year, group in sffd_df.groupby("acs_year"):
        nb = nb_per_year[acs_year].copy()
        merged = group.merge(nb, on="neighborhood", how="left")
        parts.append(merged)

    final = pd.concat(parts).sort_index()

    acs_int_cols   = ["total_population", "median_household_income", "median_gross_rent"]
    acs_float_cols = ["poverty_rate", "bachelor_rate", "vacancy_rate"]
    for col in acs_int_cols:
        if col in final.columns:
            final[col] = pd.to_numeric(final[col], errors="coerce").round(0)
    for col in acs_float_cols:
        if col in final.columns:
            final[col] = pd.to_numeric(final[col], errors="coerce").round(2)

    return final


# ══════════════════════════════════════════════════════════════════════════════
# HAUPTPIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 65)
    print("  SFFD + ACS Datenpipeline  (Längsschnitt)")
    print("=" * 65)
    print(f"\n  ACS-Jahrgänge: {ACS_YEARS}")
    print(f"  Download-Flags: SFFD={DOWNLOAD_SFFD}  "
          f"CROSSWALK={DOWNLOAD_CROSSWALK}  ACS={DOWNLOAD_ACS}")

    # 1. SFFD
    print("\n[1/5] SFFD Fire Incidents...")
    sffd_raw = load_sffd()

    # 2. Crosswalk
    print("\n[2/5] Neighborhood-Crosswalk...")
    crosswalk = load_crosswalk()

    # 3. ACS – alle Jahrgänge
    print(f"\n[3/5] ACS 5-Year – {len(ACS_YEARS)} Jahrgänge ({ACS_YEARS[0]}–{ACS_YEARS[-1]})...")
    acs_per_year = load_acs_multi(ACS_YEARS)

    # 4. Aggregation auf Neighborhood-Ebene
    print("\n[4/5] Aggregation auf Neighborhood-Ebene...")
    nb_per_year = aggregate_all_years(acs_per_year, crosswalk)

    # 5. Zeitbewusster Join
    print("\n[5/5] Zeitbewusster Join (Einsatz → nächster ACS-Snapshot)...")
    final = year_aware_join(sffd_raw, nb_per_year)
    out_path = PROCESSED_DIR / "sffd_acs_joined.parquet"
    final.to_parquet(out_path, index=False)

    print("\n" + "=" * 65)
    print("  Pipeline abgeschlossen!")
    print("=" * 65)
    print(f"\n  data/raw/fire_incidents.parquet          ({len(sffd_raw):>7,} Zeilen)")
    print(f"  data/raw/crosswalk.csv                   ({len(crosswalk):>7,} Zeilen)")
    for y in ACS_YEARS:
        nb = nb_per_year[y]
        print(f"  data/processed/acs_neighborhoods_{y}.csv ({len(nb):>7,} Neighborhoods)")
    print(f"  data/processed/sffd_acs_joined.parquet   ({len(final):>7,} Zeilen)")

    print("\n  Neue Spalte 'acs_year' – Verteilung:")
    print(final["acs_year"].value_counts().sort_index().to_string())

    print("\n  Fehlende Werte (Analysedatei):")
    for col in final.columns:
        null_pct = final[col].isna().mean() * 100
        if null_pct > 0:
            print(f"    {col:<40} {null_pct:5.1f}%")

    return final


# ══════════════════════════════════════════════════════════════════════════════
# SCHNELLTEST
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
    print("SFFD API:", "OK" if resp.ok else f"Fehler {resp.status_code}")
    if resp.ok:
        print(pd.DataFrame(resp.json())[["incident_number", "neighborhood_district"]].to_string(index=False))

    print()

    resp2 = requests.get(
        "https://data.sfgov.org/resource/sevw-6tgi.json",
        params={"$select": "geoid,neighborhoods_analysis_boundaries", "$limit": 3},
        timeout=15,
    )
    print("Crosswalk API:", "OK" if resp2.ok else f"Fehler {resp2.status_code}")
    if resp2.ok:
        print(pd.DataFrame(resp2.json()).to_string(index=False))

    print()

    # ACS-Verfügbarkeit für alle konfigurierten Jahre testen
    for year in ACS_YEARS:
        resp3 = requests.get(
            f"https://api.census.gov/data/{year}/acs/acs5"
            "?get=NAME,B19013_001E"
            "&for=tract:*"
            "&in=state:06%20county:075",
            timeout=15,
        )
        if resp3.ok:
            n = len(resp3.json()) - 1
            print(f"ACS Census API {year}: OK ({n} Tracts)")
        else:
            print(f"ACS Census API {year}: Fehler {resp3.status_code}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        quick_test()
    else:
        run_pipeline()
