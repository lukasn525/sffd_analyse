"""
Trennschaerfe des gepaarten Wilcoxon bei zehn Wiederholungsmitteln.

    python modelle/trennschaerfe.py

Input:   results/regression/vergleich.csv, results/klassifikation/vergleich.csv
Output:  results/trennschaerfe/trennschaerfe.csv   je Vergleich: d, Trennschaerfe
         results/trennschaerfe/raster.csv          Trennschaerfe fuer d = 0,1 ... 1,6

  - welche Effekte n = 10 Wiederholungsmittel ueberhaupt aufloesen koennen
  - d = mittlere Differenz / SD der zehn Differenzen; SD aus dem
    Konfidenzintervall in vergleich.csv (Formel wie in m02/m03._gepaart):
    sd = (ci_oben - ci_unten) / 2 / t(1 - ALPHA/2; n - 1) * sqrt(n)
  - Monte Carlo: ZIEHUNGEN Stichproben aus N(d, 1), Test wie in m02/m03
    (wilcoxon, zero_method="wilcox", zweiseitig), Anteil mit p < ALPHA
  - gemeinsame Standardnormal-Ziehungen fuer alle d (glatte Kurve)

FALLSTRICKE
  1  nur Teststufe "wiederholung" ("lauf" mit n = 50 waere Pseudoreplikation)
  2  Normalverteilung ist Annahme der Simulation -> Zahlen als Groessenordnung
  3  zweiseitige Trennschaerfe haengt nur vom Betrag von d ab
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import t, wilcoxon

ROOT = Path(__file__).resolve().parents[1]
EINGANG = [ROOT / "results" / "regression" / "vergleich.csv",
           ROOT / "results" / "klassifikation" / "vergleich.csv"]
AUSGANG = ROOT / "results" / "trennschaerfe"

ALPHA = 0.05
N_PAARE = 10
ZIEHUNGEN = 4000
RANDOM_STATE = 42
RASTER = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6]


def effektstaerke(zeile: pd.Series) -> float:
    """Effektstaerke d einer Zeile aus vergleich.csv.

    Input:  Zeile mit n_paare, differenz_mittel, ci_unten, ci_oben
    Output: mittlere Differenz geteilt durch die Standardabweichung der Differenzen
    """
    n = int(zeile["n_paare"])
    halb = (zeile["ci_oben"] - zeile["ci_unten"]) / 2
    sd = halb / t.ppf(1 - ALPHA / 2, n - 1) * np.sqrt(n)
    return float(zeile["differenz_mittel"] / sd)


def trennschaerfe(d: float, basis: np.ndarray) -> float:
    """Anteil der Ziehungen, in denen der gepaarte Wilcoxon bei ALPHA verwirft.

    Input:  Effektstaerke d, Standardnormal-Ziehungen (ZIEHUNGEN x N_PAARE)
    Output: geschaetzte Trennschaerfe zwischen 0 und 1
    """
    p = wilcoxon(basis + abs(d), zero_method="wilcox", axis=1).pvalue
    return float((p < ALPHA).mean())


def run() -> pd.DataFrame:
    basis = np.random.default_rng(RANDOM_STATE).standard_normal((ZIEHUNGEN, N_PAARE))

    vergleiche = pd.concat([pd.read_csv(p) for p in EINGANG], ignore_index=True)
    vergleiche = vergleiche[vergleiche["teststufe"] == "wiederholung"].copy()
    assert (vergleiche["n_paare"] == N_PAARE).all(), "Simulation setzt n = 10 voraus"
    vergleiche["d"] = vergleiche.apply(effektstaerke, axis=1)
    vergleiche["trennschaerfe"] = [trennschaerfe(d, basis) for d in vergleiche["d"]]

    raster = pd.DataFrame({"d": RASTER})
    raster["trennschaerfe"] = [trennschaerfe(d, basis) for d in RASTER]

    AUSGANG.mkdir(parents=True, exist_ok=True)
    spalten = ["zielgroesse", "paarung", "rolle", "differenz_mittel", "d",
               "wilcoxon_p", "p_holm", "signifikant", "trennschaerfe"]
    vergleiche[spalten].round(4).to_csv(AUSGANG / "trennschaerfe.csv", index=False)
    raster.round(4).to_csv(AUSGANG / "raster.csv", index=False)

    print(vergleiche[spalten].round(3).to_string(index=False))
    print()
    print(raster.round(3).to_string(index=False))
    return vergleiche


if __name__ == "__main__":
    run()
