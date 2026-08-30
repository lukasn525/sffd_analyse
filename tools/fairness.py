"""
Fairnesspruefung - haengt die Prognoseguete am Sozialprofil der Teststadtteile?

    python tools/fairness.py

Ausgang: results/fairness/fairness.csv, results/fairness/fairness.md

NICHT TEIL DER ABGABE - das SKRIPT. Die erzeugten Dateien schon: Abschnitt 8.3
beruft sich auf sie.

--------------------------------------------------------------------------
WOZU
--------------------------------------------------------------------------
Der naheliegende Vorwurf gegen ein Modell, das Einsatzlast vorhersagt, lautet:
Es ist fuer arme Stadtteile ungenauer und benachteiligt sie damit. Der Vorwurf
laesst sich messen, und ein gemessener und verneinter Verdacht traegt mehr als
ein ungeprueftes Bedenken.

B-52 hat die Pruefung am 17.08.2026 einmal von Hand gerechnet und die Zahlen
NICHT in results/ abgelegt. Genau die Lage, gegen die tools/pruefe_zahlen.py
gebaut ist: eine Zahl lebt nur in einer Notiz. Dieses Skript schliesst die
Luecke - kein Modell laeuft neu, alles Noetige liegt bereits vor.

--------------------------------------------------------------------------
WIE
--------------------------------------------------------------------------
Je (Wiederholung, Fold) wird das Sozialprofil der TESTstadtteile gegen die
dort erreichte Guete gestellt, ueber alle 50 Laeufe:

  Sozialprofil  mittlere `armutsquote_pct` der Testzeilen des Folds
  Guete         RMSE aus menge_folds.csv (Ridge, Random Forest, XGBoost)
                und aus baselines_folds.csv (Poisson-GLM der Stufe 2)
  Niveau        mittlere Zielgroesse der Testzeilen desselben Folds

Zwei Rangkorrelationen je Verfahren und Zielgroesse:

  ABSOLUT   Armutsquote gegen RMSE
  RELATIV   Armutsquote gegen RMSE / Niveau

Die Fold-Zuteilung wird nicht aus einer Datei gelesen, sondern mit
`v0_aufteilung.wiederholte_aufteilung()` neu erzeugt - dieselbe Funktion, die
die Modelllaeufe benutzt haben. Sie ist deterministisch, es wird nichts
geschaetzt und nichts gezogen.

Nur der MENGENSTRANG wird geprueft. Im Strukturstrang gibt es kein Niveau, auf
das sich Macro-F1 sinnvoll normieren liesse; eine absolute Korrelation allein
traegt die Aussage nicht.

--------------------------------------------------------------------------
LESART
--------------------------------------------------------------------------
Ein positives rho_absolut heisst: In aermeren Stadtteilen ist der Fehler
groesser. Verschwindet rho_relativ, liegt das am Niveau - dort finden mehr
Einsaetze statt, und ein groesserer absoluter Fehler bei gleicher relativer
Genauigkeit ist keine Benachteiligung.

Geprueft wird damit die GLEICHHEIT DER FEHLER, nicht die Gerechtigkeit einer
Zuteilung, die aus den Prognosen abgeleitet wuerde. Das waere eine andere
Frage und wird hier nicht beantwortet.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE - nach jedem Lauf abzuarbeiten
--------------------------------------------------------------------------
  1  Liegen alle acht rho_absolut ueber null und alle p_absolut unter 0,05?
     Nur dann gilt der Satz in 8.3, dass der Zusammenhang absolut besteht.
  2  Ist KEIN p_relativ kleiner als 0,05? Ein einziger signifikanter Wert
     kippt die Aussage, und der Absatz waere neu zu schreiben.
  3  Stimmen die Spannen mit Abschnitt 8.3 der Arbeit ueberein? Dort stehen
     +0,35 bis +0,60 absolut und -0,08 bis +0,27 relativ.
  4  Zeigt `n` in jeder Zeile 50? Weniger heisst, dass ein Fold beim Join
     verloren gegangen ist.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from scipy.stats import spearmanr

WURZEL = Path(__file__).resolve().parents[1]
for _ordner in ("prep", "modelle", "vorpruefung"):
    sys.path.insert(0, str(WURZEL / _ordner))

from config import (N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, RESULTS_DIR)
from config_modelle import WIEDERHOLUNGEN  # noqa: E402
from s2_datensaetze import RATE, ZIELGROESSE, fold_masken  # noqa: E402
from v0_aufteilung import (selten_je_stadtteil,  # noqa: E402
                           wiederholte_aufteilung)

SOZIAL = "armutsquote_pct"
ZIELE = (ZIELGROESSE, RATE)
ZIEL = RESULTS_DIR / "fairness"


def z(wert: float, n: int = 1) -> str:
    """Deutsche Schreibweise: Punkt als Tausender-, Komma als Dezimaltrenner."""
    return f"{wert:,.{n}f}".replace(",", "#").replace(".", ",").replace("#", ".")


def foldprofil(reg: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """Sozialprofil und Niveau je (Wiederholung, Fold).

    Ein:  Regressionspanel, Zahl brand-dominierter Monate je Stadtteil
    Aus:  50 Zeilen mit Armutsquote, Stadtteilzahl und Niveau je Zielgroesse

    - die Fold-Spalte wird je Wiederholung neu belegt, nicht aus der Datei
      uebernommen; Wiederholung 0 reproduziert die Datei bitgenau
    """
    zeilen = []
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(reg, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            _, test = fold_masken(d, k)
            t = d.loc[test]
            zeile = {"wiederholung": w, "fold": k,
                     "armutsquote": t[SOZIAL].mean(),
                     "n_stadtteile": int(t["stadtteil"].nunique())}
            for ziel in ZIELE:
                zeile[f"niveau_{ziel}"] = t[ziel].mean()
            zeilen.append(zeile)
    return pd.DataFrame(zeilen)


def guete_je_fold() -> pd.DataFrame:
    """Die vier Verfahren des Mengenstrangs in einer Tabelle.

    Ein:  menge_folds.csv und baselines_folds.csv
    Aus:  Zeilen mit zielgroesse, verfahren, wiederholung, fold, RMSE

    - die Stufe-2-Referenz heisst in baselines_folds.csv `modell` und wird
      auf denselben Spaltennamen gebracht wie die drei Vergleichsverfahren
    """
    spalten = ["zielgroesse", "verfahren", "wiederholung", "fold", "RMSE"]
    menge = pd.read_csv(RESULTS_DIR / "regression" / "menge_folds.csv")
    basis = pd.read_csv(RESULTS_DIR / "regression" / "baselines_folds.csv")
    basis = (basis[basis["modell"] == "Poisson-GLM"]
             .rename(columns={"modell": "verfahren"}))
    return pd.concat([menge[spalten], basis[spalten]], ignore_index=True)


def main() -> int:
    ZIEL.mkdir(parents=True, exist_ok=True)

    reg = pd.read_parquet(PFAD_REGRESSION)
    selten = selten_je_stadtteil(pd.read_parquet(PFAD_KLASSIFIKATION))
    profil = foldprofil(reg, selten)

    d = guete_je_fold().merge(profil, on=["wiederholung", "fold"], how="left")
    if d["armutsquote"].isna().any():
        print("FEHLER: Fold ohne Sozialprofil - Join gescheitert.")
        return 1

    zeilen = []
    for (ziel, verfahren), g in d.groupby(["zielgroesse", "verfahren"]):
        relativ = g["RMSE"] / g[f"niveau_{ziel}"]
        rho_abs, p_abs = spearmanr(g["armutsquote"], g["RMSE"])
        rho_rel, p_rel = spearmanr(g["armutsquote"], relativ)
        zeilen.append({"zielgroesse": ziel, "verfahren": verfahren,
                       "n": len(g),
                       "rho_absolut": rho_abs, "p_absolut": p_abs,
                       "rho_relativ": rho_rel, "p_relativ": p_rel,
                       "rel_fehler_mittel": relativ.mean()})
    e = pd.DataFrame(zeilen).sort_values(["zielgroesse", "verfahren"])
    e.to_csv(ZIEL / "fairness.csv", index=False)

    t = ["# Fairness der Prognoseguete", "",
         "Erzeugt von `tools/fairness.py`. Je Verfahren und Zielgroesse "
         "Spearman-Rho ueber die 50 (Wiederholung, Fold)-Paare des "
         "Mengenstrangs.", "",
         "| Zielgroesse | Verfahren | n | rho absolut | p | rho relativ | p |"
         " rel. Fehler |",
         "|---|---|---:|---:|---:|---:|---:|---:|"]
    for _, r in e.iterrows():
        t.append(f"| {r.zielgroesse} | {r.verfahren} | {int(r.n)} | "
                 f"{z(r.rho_absolut, 3)} | {z(r.p_absolut, 4)} | "
                 f"{z(r.rho_relativ, 3)} | {z(r.p_relativ, 4)} | "
                 f"{z(r.rel_fehler_mittel, 3)} |")
    t += ["",
          f"**Absolut** liegt Rho zwischen {z(e.rho_absolut.min(), 3)} und "
          f"{z(e.rho_absolut.max(), 3)}; das groesste p betraegt "
          f"{z(e.p_absolut.max(), 4)}.",
          "",
          f"**Relativ zum Niveau** liegt Rho zwischen "
          f"{z(e.rho_relativ.min(), 3)} und {z(e.rho_relativ.max(), 3)}; "
          f"das kleinste p betraegt {z(e.p_relativ.min(), 4)}.", ""]
    (ZIEL / "fairness.md").write_text("\n".join(t), encoding="utf-8")

    print("Geschrieben nach results/fairness/:")
    print("  fairness.csv")
    print("  fairness.md")
    print()
    print(f"absolut  rho {z(e.rho_absolut.min(), 3)} bis "
          f"{z(e.rho_absolut.max(), 3)}, groesstes p "
          f"{z(e.p_absolut.max(), 4)}")
    print(f"relativ  rho {z(e.rho_relativ.min(), 3)} bis "
          f"{z(e.rho_relativ.max(), 3)}, kleinstes p "
          f"{z(e.p_relativ.min(), 4)}")
    print()
    print("Pruefauftraege im Docstring abarbeiten.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
