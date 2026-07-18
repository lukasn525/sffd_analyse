"""
Aggregation des Prep-Pipeline-Outputs (Einsatz-Ebene) auf Stadtteil x Monat.

Dieser Schritt fehlt in der bestehenden Prep-Pipeline (deren Output ist
Einsatz-Ebene) und wird hier ergaenzt, OHNE die Pipeline zu veraendern
(vgl. CLAUDE.md, Abschnitt Preprocessing / Decision Log).

Entwurfsentscheidungen (Bezug: Expose, Kap. 3):
- Zielgroesse Regression: anzahl_einsaetze pro Stadtteil und Monat.
- Stadtteil-Merkmale (ACS/Crime/Land Use) sind je Stadtteil bzw.
  Stadtteil x ACS-Jahr konstant -> "first" bei der Aggregation.
- Monate ohne Einsatz sind echte Nullen -> vollstaendiges Raster
  Stadtteil x Monat, fehlende Kombinationen mit 0 aufgefuellt.
- Der letzte (unvollstaendige) Kalendermonat wird abgeschnitten.

Alle drei Modelle (Ridge, RF, XGBoost) erhalten exakt diesen Datensatz.
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
FEATURES_PARQUET = ROOT / "data" / "processed" / "sf_fire_risk_features.parquet"

# Praediktoren gemaess Expose: soziooekonomisch, kriminalitaetsbezogen, baulich.
# Hinweis: sf_fire_risk_features_cleaned.parquet enthaelt keine Monatsspalte,
# daher wird die volle Feature-Tabelle als Input verwendet.
PRAEDIKTOREN = [
    "median_haushaltseinkommen", "armutsquote_pct", "akademikerquote_pct",
    "median_miete", "leerstandsquote_pct", "gesamtbevoelkerung",
    "anteil_gewaltdelikte_pct", "anteil_eigentumsdelikte_pct",
    "anteil_altbau_vor_1940_pct", "anteil_wohngebaeude_pct",
    "anteil_risikogewerbe_pct",
]


def lade_stadtteil_monat(pfad: Path = FEATURES_PARQUET) -> pd.DataFrame:
    """Laedt den Pipeline-Output und aggregiert auf Stadtteil x Monat."""
    df = pd.read_parquet(pfad)

    # Duplikate aus den DataSF-Quelldaten entfernen (208 mehrfach gemeldete
    # Einsatznummern, ~0,07% der Zeilen; Befund s. results/eignungspruefung/).
    # Bewusst hier und nicht in der Prep-Pipeline (bleibt unveraendert).
    df = df.drop_duplicates(subset=["einsatz_nummer"], keep="first")

    # Unvollstaendigen Randmonat abschneiden
    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]
    letzter = df["jahr_monat"].max()
    erster  = df["jahr_monat"].min()
    df = df[df["jahr_monat"] < letzter]

    agg = (df.groupby(["stadtteil", "jahr", "monat"])
             .agg(anzahl_einsaetze=("einsatz_nummer", "count"),
                  **{c: (c, "first") for c in PRAEDIKTOREN})
             .reset_index())

    # Vollstaendiges Raster: Monate ohne Einsatz = 0
    idx = pd.MultiIndex.from_product(
        [agg["stadtteil"].unique(),
         range(int(agg["jahr"].min()), int(agg["jahr"].max()) + 1),
         range(1, 13)],
        names=["stadtteil", "jahr", "monat"])
    raster = agg.set_index(["stadtteil", "jahr", "monat"]).reindex(idx).reset_index()
    raster["anzahl_einsaetze"] = raster["anzahl_einsaetze"].fillna(0).astype(int)
    raster[PRAEDIKTOREN] = (raster.groupby("stadtteil")[PRAEDIKTOREN]
                                  .transform(lambda s: s.ffill().bfill()))

    raster["jahr_monat"] = raster["jahr"] * 100 + raster["monat"]
    raster = raster[(raster["jahr_monat"] >= erster) & (raster["jahr_monat"] < letzter)]
    return (raster.drop(columns="jahr_monat")
                  .sort_values(["jahr", "monat", "stadtteil"])
                  .reset_index(drop=True))
