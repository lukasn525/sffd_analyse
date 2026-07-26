"""
Merkmalsbildung fuer den Regressionsteil: Saison und Vergangenheitswerte.

Baut auf `aggregation.py` auf (dort entstehen Panel, Zeitraum, Exposure und
Kriminalitaetsindex) und ergaenzt die beiden Merkmalsarten, die sich nicht aus
den Rohdaten, sondern aus der Zielgroesse selbst ergeben:

  1. SAISON  - Kalendermonat als Sinus/Kosinus
  2. LAGS    - Vergangenheitswerte der Einsatzzahl je Stadtteil

Ausserdem werden die beiden Merkmalssaetze definiert, auf denen ALLE drei
Verfahren laufen (Fairness-Regel, CLAUDE.md):

  Set S    Strukturmerkmale + Saison
           -> beantwortet Unterfrage 1 (Erklaerungsbeitrag der Merkmale)
  Set S+L  Set S + Lags
           -> realistische Prognoseaufgabe

Warum die Trennung? Sobald der Vormonatswert im Modell steckt, erklaert die
Vergangenheit der Zielgroesse fast alles, und Armut oder Altbauanteil
verschwinden in der Feature Importance - nicht weil sie irrelevant waeren,
sondern weil ihre Wirkung bereits im Vormonatswert enthalten ist. Zwei Saetze
trennen damit zwei verschiedene Fragen sauber voneinander.

Ausfuehren (Selbsttests):
  python modellierung/features.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

from aggregation import PRAEDIKTOREN, balanciertes_panel, lade_stadtteil_monat

ROOT = Path(__file__).parent.parent

# --------------------------------------------------------------------------
# Saison (zyklische Kodierung des Kalendermonats)
# --------------------------------------------------------------------------
# Die Einsatzzahl schwankt im Jahresverlauf deutlich: im Mittel 69,8 Einsätze
# je Stadtteil im April gegenueber 83,4 im Dezember (+19,5 %).
#
# Der Monat als ZAHL 1-12 waere dafuer eine schlechte Kodierung:
#   - Dezember (12) und Januar (1) haetten den Abstand 11, tatsaechlich liegt
#     ein einziger Monat dazwischen. Das Modell haelt sie fuer maximal
#     verschieden, obwohl sie benachbart sind.
#   - Fuer Ridge waere ein linearer Koeffizient auf "Monat" die Aussage "jeder
#     Monat addiert X Einsaetze" - eine Gerade. Ein U-foermiges Jahresmuster
#     laesst sich damit grundsaetzlich nicht abbilden.
#
# sin/cos legen die Monate auf ein Zifferblatt. Danach gilt (nachgerechnet):
#   Dez -> Jan  Abstand 0,52
#   Jan -> Feb  Abstand 0,52   (gleich weit - korrekt)
#   Jan -> Jul  Abstand 2,00   (Maximum - korrekt)
# Zwei Merkmale beschreiben zusammen genau eine Welle ueber das Jahr.
SAISON = ["monat_sin", "monat_cos"]

# --------------------------------------------------------------------------
# Lags (Vergangenheitswerte der Zielgroesse)
# --------------------------------------------------------------------------
# Die Lag-1-Autokorrelation der Einsatzzahl betraegt 0,96 - die Zielgroesse ist
# stark persistent. Ohne diese Merkmale schlaegt keines der Verfahren die naive
# Vormonats-Baseline (Decision Log #8).
#
# LEAKAGE-SICHERHEIT: Alle drei Merkmale sind strikt rueckwaertsgerichtet und
# werden je Stadtteil gebildet (`groupby`), nie ueber Stadtteilgrenzen hinweg.
# Beim gleitenden Mittel steht `shift(1)` VOR `rolling(3)` - der Wert fuer
# Monat t verwendet damit t-1, t-2, t-3, aber niemals t selbst.
LAGS = ["lag_1", "lag_12", "rolling_mean_3"]

# --------------------------------------------------------------------------
# Merkmalssaetze - identisch fuer Ridge, Random Forest und XGBoost
# --------------------------------------------------------------------------
# Bewusst NICHT enthalten: das rohe `jahr`. Baumverfahren koennen nicht
# extrapolieren - im Testfenster kommen ausschliesslich Jahreswerte vor, die im
# Training nie auftraten, und die Baeume ordnen sie stumpf dem letzten
# bekannten Blatt zu. Ridge extrapoliert dagegen linear weiter. Das wuerde
# genau den Verfahrensvergleich verzerren, um den es geht. Das Zeitniveau
# tragen die Lags (Leitfaden A2).
FEATURES_S  = PRAEDIKTOREN + SAISON
FEATURES_SL = PRAEDIKTOREN + SAISON + LAGS
FEATURE_SETS = {"S": FEATURES_S, "S+L": FEATURES_SL}


def baue_features(panel: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Ergaenzt Saison- und Lag-Merkmale und verwirft die Anlaufmonate.

    Erwartet das balancierte Stadtteil-Monats-Panel aus `aggregation.py`.

    Die ersten zwoelf Monate je Stadtteil fallen weg, weil `lag_12` dort nicht
    definiert ist. Das geschieht fuer ALLE Modelle und Merkmalssaetze
    gleichermassen - auch fuer Set S, das die Lags gar nicht verwendet. Sonst
    liefen die Verfahren auf unterschiedlichen Zeilen und die Guetemasse waeren
    nicht mehr vergleichbar (Fairness-Regel).
    """
    d = panel.copy()
    d["jahr_monat"] = d["jahr"] * 100 + d["monat"]

    d["monat_sin"] = np.sin(2 * np.pi * d["monat"] / 12)
    d["monat_cos"] = np.cos(2 * np.pi * d["monat"] / 12)

    d = d.sort_values(["stadtteil", "jahr_monat"]).reset_index(drop=True)
    g = d.groupby("stadtteil")["anzahl_einsaetze"]
    d["lag_1"]          = g.shift(1)
    d["lag_12"]         = g.shift(12)
    d["rolling_mean_3"] = g.transform(lambda s: s.shift(1).rolling(3).mean())

    vorher = len(d)
    d = d.dropna(subset=LAGS).reset_index(drop=True)
    # Feste, dokumentierte Zeilenreihenfolge (Zeit, dann Stadtteil).
    # REPRODUZIERBARKEIT: Random Forest und XGBoost ziehen ihre Bootstrap- bzw.
    # Subsample-Stichproben ueber Zeilenpositionen. Eine andere Sortierung
    # liefert daher trotz identischem `random_state` leicht andere Baeume -
    # empirisch 17,2587 statt 17,2974 RMSE in Fold 1, also gut innerhalb der
    # Fold-Streuung von +/-0,8, aber eben nicht bitgleich. Ridge ist dagegen
    # reihenfolgeinvariant (identisch auf vier Nachkommastellen).
    # Die Sortierung gehoert damit zum Reproduzierbarkeitsvertrag und darf
    # nicht mehr veraendert werden.
    d = d.sort_values(["jahr_monat", "stadtteil"]).reset_index(drop=True)

    if verbose:
        print(f"  Merkmalsbildung: {vorher:,} -> {len(d):,} Zeilen "
              f"({vorher - len(d):,} Anlaufmonate ohne lag_12 entfernt)")
        print(f"  Zeitraum nach Lag-Bildung: {d['jahr_monat'].min()}-"
              f"{d['jahr_monat'].max()} "
              f"({d.groupby(['jahr','monat']).ngroups} Monate, "
              f"{d['stadtteil'].nunique()} Stadtteile)")
        print(f"  Merkmalssaetze: S = {len(FEATURES_S)}, S+L = {len(FEATURES_SL)}")
    return d


def lade_modelldaten(verbose: bool = False) -> pd.DataFrame:
    """Bequemer Einstieg: Panel laden, balancieren, Merkmale bauen."""
    panel = balanciertes_panel(lade_stadtteil_monat(verbose=verbose),
                               verbose=verbose)
    return baue_features(panel, verbose=verbose)


def ridge_sicht(d: pd.DataFrame, spalten: list[str]) -> pd.DataFrame:
    """Modellspezifische Aufbereitung der Lags fuer Ridge.

    Ridge wird auf log(1+y) geschaetzt. Damit die Beziehung zwischen den Lags
    und der log-Zielgroesse linear ist, muessen auch die Lags logarithmiert
    werden (log-AR-Spezifikation). Rohe Lags in einem log-Modell sind
    fehlspezifiziert - empirisch ergab das R2 < 0.

    Das ist eine modellinterne Transformation wie die Standardisierung und
    KEINE Verletzung der Fairness-Regel: identische Zeilen, identische
    Information, nur eine andere Darstellung (Decision Log #9).
    """
    x = d[spalten].copy()
    for c in LAGS:
        if c in x.columns:
            x[c] = np.log1p(x[c])
    return x


if __name__ == "__main__":
    lade_modelldaten(verbose=True)
    print(f"\n  Set S   ({len(FEATURES_S)} Merkmale): {FEATURES_S}")
    print(f"  Set S+L ({len(FEATURES_SL)} Merkmale): + {LAGS}")
    print("\n  Pruefungen: python tests/test_aufbereitung.py")
