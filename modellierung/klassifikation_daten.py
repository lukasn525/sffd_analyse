"""
Datenaufbereitung fuer den Klassifikationsteil (Zielgroesse Einsatzart).

Gegenstueck zu `aggregation.py`: Dort entsteht der Regressionsdatensatz auf
Stadtteil-Monats-Ebene, hier der Klassifikationsdatensatz auf Einzeleinsatz-
Ebene. Beide teilen zwingend dieselbe Abgrenzung - Zeitraum und Stadtteile
werden aus dem Regressionspanel uebernommen, nicht neu definiert. Sonst
beziehen sich die beiden Teile der Arbeit auf unterschiedliche Datenbestaende.

Grundlage: docs/KLASSIFIKATION_DESIGN.md, CLAUDE.md Decision Log #6, #20, #21.

ZWEI ZIELGROESSEN werden bereitgestellt, damit die Entscheidung zwischen
mehrklassiger und binaerer Variante ohne erneute Aufbereitung getroffen werden
kann (die Merkmale und Zeilen sind identisch):

  `einsatzart_gruppe`  4 Klassen, zusammengefasste NFIRS-Serien
  `ist_brand`          binaer, Brand vs. Nicht-Brand

Ausfuehren (Selbsttests):
  python modellierung/klassifikation_daten.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

from aggregation import (CRIME_ROH, ENDE, EXPOSURE_ROH, FEATURES_PARQUET,
                         PRAEDIKTOREN, START, balanciertes_panel,
                         lade_stadtteil_monat)

ROOT = Path(__file__).parent.parent

# --------------------------------------------------------------------------
# Zielgroesse 1: NFIRS-Serien zu vier Klassen zusammengefasst (Decision Log #21)
# --------------------------------------------------------------------------
# Die NFIRS-Codes sind hierarchisch: Die fuehrende Ziffer bezeichnet die Serie.
# Zusammengefasst wird entlang der fachlichen Bedeutung, nicht nach Haeufigkeit.
#
#   100 Brand
#   200 Ueberdruck/Explosion OHNE Feuer  -> fachlich Gefahrenlage, nicht Brand
#   300 Rettungsdienst / EMS
#   400 Gefahrenlage (Gasleck, Stromausfall, Gefahrstoff)
#   500 Serviceeinsatz (Wasserschaden, Amtshilfe, Person in Aufzug)
#   600 Good Intent (in gutem Glauben gemeldet, nichts vorgefunden)
#   700 Fehlalarm (Anlagenstoerung, boeswillige Ausloesung)
#   800 Naturereignis
#   900 Sonstige
NFIRS_GRUPPEN = {
    "1": "Brand",
    "3": "Rettung/EMS",
    "2": "Technische Hilfe/Gefahr",
    "4": "Technische Hilfe/Gefahr",
    "5": "Technische Hilfe/Gefahr",
    "8": "Technische Hilfe/Gefahr",
    "9": "Technische Hilfe/Gefahr",
    "6": "Fehlalarm/Good Intent",
    "7": "Fehlalarm/Good Intent",
}
KLASSEN = ["Brand", "Rettung/EMS", "Technische Hilfe/Gefahr", "Fehlalarm/Good Intent"]
RESTKLASSE = "Technische Hilfe/Gefahr"   # fuer nicht zuordenbare Codes

# --------------------------------------------------------------------------
# Merkmalsbloecke
# --------------------------------------------------------------------------
# Block A: Stadtteilstruktur - exakt dieselben Merkmale wie in der Regression.
MERKMALE_STRUKTUR = list(PRAEDIKTOREN)

# Block B: Zeitpunkt des Alarms. Zyklische Kodierung fuer Stunde und Monat,
# weil der Zusammenhang periodisch und nicht monoton ist: Der Brandanteil
# schwankt ueber den Tag zwischen 8,5 % und 20,5 %, die lineare Korrelation
# mit `stunde` betraegt aber nur -0,006. Als Zahl lagen Stunde 23 und Stunde 0
# maximal weit auseinander, tatsaechlich sind sie benachbart.
MERKMALE_ZEIT = ["stunde_sin", "stunde_cos", "monat_sin", "monat_cos",
                 "ist_nacht", "ist_wochenende"]
# Wochentag bleibt kategorial und wird im ColumnTransformer One-Hot-kodiert -
# einheitlich fuer alle drei Verfahren (auch XGBoost), damit die Designmatrix
# identisch ist und Unterschiede rein algorithmisch bleiben.
MERKMALE_KATEGORIAL = ["wochentag"]

# Optionaler Robustheitslauf: Ortsidentitaet. NICHT im Hauptmodell, analog zur
# Entscheidung gegen die Stadtteil-ID in der Regression - das Hauptmodell soll
# zeigen, ob die INHALTLICHEN Merkmale die Einsatzart erklaeren.
MERKMALE_ORT = ["bataillon"]

# --------------------------------------------------------------------------
# Ergebnisvariablen - duerfen NIEMALS Merkmal sein
# --------------------------------------------------------------------------
# Diese Spalten stehen erst nach dem Einsatz fest oder sind eine Folge der
# Einsatzart. Ihre Verwendung waere Leakage im engeren Sinn: Das Modell wuerde
# die Antwort aus ihren eigenen Konsequenzen ableiten.
#   schaetzung_sachschaden_usd  Brand 9.962 $ vs. Nicht-Brand 28 $ - nach
#                               Loeschung geschaetzt
#   loeschfahrzeuge/-kraefte    Disposition richtet sich nach der Lage
#   alarmstufe                  wird im Einsatzverlauf hochgestuft
#   antwortzeit_min             Ergebnis des Einsatzes
#   zivile_tote/-verletzte      Einsatzfolgen
ERGEBNISVARIABLEN = [
    "schaetzung_sachschaden_usd", "loeschfahrzeuge", "loeschkraefte",
    "rettungsdienst_einheiten", "alarmstufe", "antwortzeit_min",
    "zivile_tote", "zivile_verletzte", "flammenausbreitung_eingedaemmt",
    "ankunft_zeitpunkt",
]


def _abgrenzung_aus_regression() -> list[str]:
    """Stadtteilliste des Regressionspanels - identische Abgrenzung erzwingen."""
    return sorted(balanciertes_panel(lade_stadtteil_monat())["stadtteil"].unique())


def lade_klassifikationsdaten(pfad: Path = FEATURES_PARQUET,
                              mit_ort: bool = False,
                              verbose: bool = False) -> pd.DataFrame:
    """Einzeleinsatz-Datensatz mit beiden Zielgroessen und beiden Merkmalsbloecken.

    Parameter:
      mit_ort - True nimmt `bataillon` zusaetzlich auf (Robustheitslauf).
      verbose - Diagnoseausgaben.

    Rueckgabe: DataFrame mit `jahr_monat` (Schluessel fuer cv.py), den
    Zielgroessen `einsatzart_gruppe` und `ist_brand` sowie den Merkmalen.
    Enthaelt garantiert KEINE Ergebnisvariablen.
    """
    df = pd.read_parquet(pfad)

    # --- Abgrenzung exakt wie die Regression -------------------------------
    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]
    df = df[df["jahr_monat"].between(START, ENDE)]
    stadtteile = _abgrenzung_aus_regression()
    df = df[df["stadtteil"].isin(stadtteile)].copy()
    # Sicherheits-Dedup (idempotent; Bereinigung erfolgt in 02_join.py)
    df = df.drop_duplicates(subset=["einsatz_nummer"], keep="first")

    # --- Zielgroessen ------------------------------------------------------
    serie = df["einsatzart"].astype(str).str.extract(r"^(\d)")[0]
    df["einsatzart_gruppe"] = serie.map(NFIRS_GRUPPEN).fillna(RESTKLASSE)
    df["ist_brand"] = (serie == "1").astype(int)

    # --- Block A: Stadtteilstruktur ----------------------------------------
    # Dieselben Transformationen wie in aggregation.py, hier auf Einsatz-Ebene.
    df["log_bevoelkerung"] = np.log1p(df[EXPOSURE_ROH].astype(float))
    idx = df[CRIME_ROH].astype(float)
    df["log_kriminalitaetsindex"] = np.log(idx.where(idx > 0))

    # --- Block B: Zeitpunkt des Alarms -------------------------------------
    df["stunde_sin"] = np.sin(2 * np.pi * df["stunde"] / 24)
    df["stunde_cos"] = np.cos(2 * np.pi * df["stunde"] / 24)
    df["monat_sin"]  = np.sin(2 * np.pi * df["monat"] / 12)
    df["monat_cos"]  = np.cos(2 * np.pi * df["monat"] / 12)

    spalten = (["einsatz_nummer", "stadtteil", "jahr", "monat", "jahr_monat",
                "einsatzart_gruppe", "ist_brand"]
               + MERKMALE_STRUKTUR + MERKMALE_ZEIT + MERKMALE_KATEGORIAL
               + (MERKMALE_ORT if mit_ort else []))
    ergebnis = df[spalten].dropna(subset=MERKMALE_STRUKTUR).reset_index(drop=True)

    if verbose:
        n_raus = len(df) - len(ergebnis)
        print(f"  Klassifikationsdaten: {len(ergebnis):,} Einsaetze | "
              f"{ergebnis['stadtteil'].nunique()} Stadtteile | {START}-{ENDE}")
        if n_raus:
            print(f"  {n_raus:,} Zeilen ohne vollstaendige Strukturmerkmale entfernt")
        print("\n  Zielgroesse A - einsatzart_gruppe (4 Klassen):")
        v = ergebnis["einsatzart_gruppe"].value_counts()
        for k, n in v.items():
            print(f"    {k:<26} {n:>7,}  ({n/len(ergebnis)*100:5.1f} %)")
        print(f"    Ungleichgewicht groesste/kleinste: {v.max()/v.min():.1f}:1")
        b = ergebnis["ist_brand"].mean()
        print(f"\n  Zielgroesse B - ist_brand (binaer): {b*100:.1f} % Brand, "
              f"Ungleichgewicht {(1-b)/b:.1f}:1")
        print(f"    -> scale_pos_weight = {(1-b)/b:.2f} bzw. class_weight='balanced'")
    return ergebnis


def merkmalslisten(mit_ort: bool = False) -> dict[str, list[str]]:
    """Merkmalssaetze der drei geplanten Laeufe (docs/KLASSIFIKATION_DESIGN.md)."""
    saetze = {
        "A+B": MERKMALE_STRUKTUR + MERKMALE_ZEIT + MERKMALE_KATEGORIAL,
        "B":   MERKMALE_ZEIT + MERKMALE_KATEGORIAL,
    }
    if mit_ort:
        saetze["A+B+Ort"] = saetze["A+B"] + MERKMALE_ORT
    return saetze


if __name__ == "__main__":
    lade_klassifikationsdaten(verbose=True)
    print(f"\n  Merkmalssaetze: "
          f"{ {k: len(v) for k, v in merkmalslisten().items()} }")
    print("\n  Pruefungen: python tests/test_aufbereitung.py")
