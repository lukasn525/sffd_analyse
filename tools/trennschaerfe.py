"""
Trennschaerfe des gepaarten Wilcoxon bei zehn Wiederholungsmitteln (B-51).

    python tools/trennschaerfe.py

Eingang: results/regression/vergleich.csv, results/klassifikation/vergleich.csv
Ausgang: results/trennschaerfe/trennschaerfe.csv   je Vergleich: d, Trennschaerfe
         results/trennschaerfe/raster.csv          Trennschaerfe fuer d = 0,1 ... 1,6

  - m02 und m03 pruefen auf n = 10 Wiederholungsmitteln. Ein nicht
    signifikanter Vergleich traegt erst dann eine Aussage, wenn feststeht,
    welche Effekte dieses Design ueberhaupt aufloesen kann. Diese Zahl
    liefert das Skript; bis zum 10.09.2026 war sie eine einmalige Rechnung
  - Effektstaerke d = mittlere Differenz / Standardabweichung der zehn
    Differenzen. Die Standardabweichung wird aus dem Konfidenzintervall in
    vergleich.csv zurueckgerechnet, mit derselben Formel, mit der
    m02._gepaart bzw. m03._gepaart das Intervall gebildet haben:
    sd = (ci_oben - ci_unten) / 2 / t(1 - ALPHA/2; n - 1) * sqrt(n)
  - Trennschaerfe per Monte Carlo: ZIEHUNGEN Stichproben zu n Differenzen aus
    N(d, 1); je Stichprobe derselbe Test wie in m02/m03 (wilcoxon,
    zero_method="wilcox", zweiseitig), Anteil der Ziehungen mit p < ALPHA
  - Alle Effektstaerken teilen sich dieselben Standardnormal-Ziehungen
    (gemeinsame Zufallszahlen): Die Kurve im Raster steigt dadurch glatt,
    und das Ergebnis haengt nicht von der Reihenfolge der Vergleiche ab

FALLSTRICKE
  1  Nur die Teststufe "wiederholung". Die Stufe "lauf" (n = 50) steht in
     vergleich.csv nur zur Gegenueberstellung - sie waere Pseudoreplikation
  2  Normalverteilte Differenzen sind eine Annahme der SIMULATION, nicht des
     Tests. Die Zahlen sind eine Groessenordnung, keine exakte Eigenschaft
  3  Die zweiseitige Trennschaerfe haengt nur vom Betrag von d ab
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

    Ein:  Zeile mit n_paare, differenz_mittel, ci_unten, ci_oben
    Aus:  mittlere Differenz geteilt durch die Standardabweichung der Differenzen
    """
    n = int(zeile["n_paare"])
    halb = (zeile["ci_oben"] - zeile["ci_unten"]) / 2
    sd = halb / t.ppf(1 - ALPHA / 2, n - 1) * np.sqrt(n)
    return float(zeile["differenz_mittel"] / sd)


def trennschaerfe(d: float, basis: np.ndarray) -> float:
    """Anteil der Ziehungen, in denen der gepaarte Wilcoxon bei ALPHA verwirft.

    Ein:  Effektstaerke d, Standardnormal-Ziehungen (ZIEHUNGEN x N_PAARE)
    Aus:  geschaetzte Trennschaerfe zwischen 0 und 1
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
