"""
Wiederholte Splits - die eine Stelle, an der die Fold-Zuteilung je Wiederholung
entsteht.

WOZU DIESE DATEI UEBERHAUPT EXISTIERT
--------------------------------------------------------------------------
Die Grundaufteilung steht als Spalten `fold` und `ist_holdout` in beiden
Parquet-Dateien; erzeugt hat sie `prep/s2_datensaetze.ergaenze_aufteilung()`.
Fuer die WIEDERHOLTEN Splits (docs/04_MODELLIERUNG.md, Abschnitt 2) reicht sie
nicht aus. Zwei nachgewiesene Gruende, beide am 05.08.2026 am Datensatz
gemessen (docs/07_BEFUNDE.md, B-1 und B-2):

  1  Der `versatz` verteilt die Stadtteile reihum auf N_FOLDS + 1 = 6 Gruppen.
     Wer i den Platz i belegt, landet in Gruppe (i + versatz) % 6. Zwei
     Stadtteile liegen also genau dann in derselben Gruppe, wenn ihre Plaetze
     modulo 6 uebereinstimmen - UNABHAENGIG vom Versatz. Der Versatz rotiert
     damit nur die Beschriftung der Gruppen, nicht ihre Zusammensetzung. Ueber
     versatz 0..9 entstehen 6 verschiedene Konstellationen, davon 4 Dubletten.

  2  Rotiert die Beschriftung, rotiert auch Gruppe 0 - und Gruppe 0 IST das
     Hold-out. Gemessen: bei versatz = 1 liegt kein einziger der sechs
     urspruenglichen Hold-out-Stadtteile mehr im Hold-out. Die Wiederholungen
     1 bis 9 wuerden auf genau den Stadtteilen trainieren und testen, die bis
     zur Schlussbewertung unberuehrt bleiben muessen.

Deshalb hier eine eigene Funktion mit drei Zusagen:

  Das Hold-out bleibt FEST      die sechs Stadtteile mit ist_holdout == 1 aus
                                der Datei, in jeder Wiederholung dieselben
  Die Stratifizierung bleibt    sortiert nach brand-dominierten Monaten, bei
                                Gleichstand nach Bevoelkerung (Decision Log #30)
  Wiederholung 0 = die Datei    bitgenau dieselbe fold-Spalte; das wird bei
                                jedem Aufruf per assert nachgeprueft

WIE DIE WIEDERHOLUNGEN ENTSTEHEN
--------------------------------------------------------------------------
Die 29 Entwicklungsstadtteile werden wie bisher nach (selten, bev) absteigend
sortiert und reihum ausgeteilt. Neu ist nur, dass vor dem Austeilen INNERHALB
der Rangbloecke gemischt wird - Block 0 sind die Plaetze 0-4, Block 1 die
Plaetze 5-9 und so fort:

    Rangblock 0   [Bayview, Bernal, Portola, Seacliff, Twin Peaks]
    Rangblock 1   [...]                        -> jeder Block liefert genau
    ...                                           EINEN Stadtteil je Fold

Damit bekommt jeder Fold weiterhin genau einen Stadtteil aus jedem Rangblock -
die doppelte Stratifizierung ueberlebt das Mischen unveraendert, und die
Foldgroessen bleiben 6/6/6/6/5. Anders als beim Versatz aendert sich aber die
ZUSAMMENSETZUNG der Folds, nicht nur ihre Nummer. Genau das brauchen die
wiederholten Splits.

Kein Leakage: Gemischt wird ausschliesslich die Frage, welche Stadtteile
gemeinsam getestet werden. Kein Modell sieht dadurch eine Zeile mehr.

Benutzt von `vorpruefung/v1_baselines.py`, `modelle/m02_menge.py` und
`modelle/m03_struktur.py` - alle drei muessen dieselbe Zuteilung sehen, sonst
vergleicht der gepaarte Wilcoxon-Test still auf verschiedenen Zeilen.

Selbsttest:
  python vorpruefung/v0_aufteilung.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "prep"))
sys.path.insert(0, str(_ROOT / "modelle"))

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION)
from config_modelle import RANDOM_STATE, WIEDERHOLUNGEN  # noqa: E402
from s2_datensaetze import ZIELKLASSE, fold_masken  # noqa: E402

# Die seltenste Klasse, nach der stratifiziert wird (Decision Log #30). Der
# Wert steht so in der Zielspalte der Klassifikation.
SELTENE_KLASSE = "brand"


def selten_je_stadtteil(klassifikation: pd.DataFrame) -> pd.Series:
    """Zahl der brand-dominierten Monate je Stadtteil - das Stratifizierungsmass.

    Wortgleich zu dem, was `prep/s2_datensaetze.run()` beim Bau der Dateien
    gerechnet hat. Es steht hier noch einmal, weil die Modellskripte den Wert
    brauchen, er aber nicht in den Dateien abgelegt ist: In `regression.parquet`
    gibt es keine Klassenspalte, und die fold-Spalte allein sagt nicht, WIE sie
    zustande kam.

    Fuer die Regression heisst das: `klassifikation.parquet` mitlesen, auch wenn
    nur die Menge modelliert wird. Das ist kein Leakage - die Zahl geht in kein
    Modell ein, sie bestimmt nur, welche Stadtteile gemeinsam getestet werden.
    """
    return (klassifikation[klassifikation[ZIELKLASSE] == SELTENE_KLASSE]
            .groupby("stadtteil").size())


def wiederholte_aufteilung(daten: pd.DataFrame, wiederholung: int = 0,
                           selten: pd.Series | None = None) -> pd.DataFrame:
    """Schreibt die fold-Spalte fuer eine Wiederholung. Hold-out bleibt fest.

    `wiederholung` 0 liefert exakt die Aufteilung aus der Datei - das wird per
    assert geprueft, nicht nur behauptet. 1 bis WIEDERHOLUNGEN-1 liefern
    verschiedene Zusammensetzungen bei gleicher Stratifizierung.

    `selten` ist die Reihe aus `selten_je_stadtteil()`. Fehlt sie, wird nur nach
    Bevoelkerung stratifiziert - das reproduziert die Dateien NICHT und ist nur
    fuer Sonderfaelle gedacht.

    Rueckgabe ist eine Kopie mit neu belegter Spalte `fold`; `ist_holdout` wird
    unveraendert uebernommen. Die Hold-out-Zeilen behalten fold = 0 und werden
    von `fold_masken()` in jeder Wiederholung ausgeschlossen.
    """
    if "ist_holdout" not in daten.columns or "fold" not in daten.columns:
        raise ValueError("Datensatz ohne fold/ist_holdout - erst prep/build.py.")
    if not 0 <= wiederholung < WIEDERHOLUNGEN:
        raise ValueError(f"wiederholung muss in 0..{WIEDERHOLUNGEN - 1} liegen.")

    entwicklung = daten[daten["ist_holdout"] == 0]
    bev = entwicklung.groupby("stadtteil")[EXPOSURE_ROH].mean()
    s = (pd.Series(0, index=bev.index) if selten is None
         else selten.reindex(bev.index).fillna(0))

    # Dieselbe Ordnung wie in prep/s2_datensaetze.ergaenze_aufteilung().
    ordnung = list(pd.DataFrame({"selten": s, "bev": bev})
                     .sort_values(["selten", "bev"], ascending=False).index)

    # Mischen INNERHALB der Rangbloecke. Wiederholung 0 bleibt ungemischt,
    # damit die Datei exakt reproduziert wird.
    if wiederholung:
        rng = np.random.default_rng(RANDOM_STATE + wiederholung)
        for anfang in range(0, len(ordnung), N_FOLDS):
            block = ordnung[anfang:anfang + N_FOLDS]
            ordnung[anfang:anfang + N_FOLDS] = [block[i] for i in
                                                rng.permutation(len(block))]

    gruppe = {st: i % N_FOLDS + 1 for i, st in enumerate(ordnung)}

    d = daten.copy()
    d["fold"] = d["stadtteil"].map(gruppe).fillna(0).astype("int64")
    d["ist_holdout"] = daten["ist_holdout"].astype("int64")

    if wiederholung == 0:
        abweichend = int((d["fold"] != daten["fold"]).sum())
        assert not abweichend, (
            f"Wiederholung 0 weicht in {abweichend} Zeilen von der fold-Spalte "
            f"der Datei ab - die Stratifizierung passt nicht. Wurde `selten` "
            f"uebergeben?")
    return d


def entwicklung_und_holdout(daten: pd.DataFrame
                            ) -> tuple[pd.Series, pd.Series]:
    """Masken fuer die Schlussbewertung: 29 Entwicklungs- gegen 6 Hold-out-Stadtteile.

    Das Gegenstueck zu `fold_masken()` fuer den einen Lauf, der das Hold-out
    ueberhaupt anfassen darf. Steht hier und nicht im Modellskript, damit es
    genau eine Stelle im Repo gibt, an der `ist_holdout == 1` gelesen wird.
    """
    return daten["ist_holdout"] == 0, daten["ist_holdout"] == 1


# ==========================================================================
# Selbsttest - beantwortet die vier Fragen, an denen diese Datei haengt
# ==========================================================================
def _selbsttest() -> int:
    r = pd.read_parquet(PFAD_REGRESSION)
    k = pd.read_parquet(PFAD_KLASSIFIKATION)
    selten = selten_je_stadtteil(k)

    ho_datei = frozenset(r.loc[r["ist_holdout"] == 1, "stadtteil"].unique())
    partitionen, fehler = {}, 0

    print(f"Datei: {len(r):,} Zeilen | {r['stadtteil'].nunique()} Stadtteile | "
          f"Hold-out {len(ho_datei)}")
    print(f"Stratifizierung: {SELTENE_KLASSE}-dominierte Monate, "
          f"Spitze {selten.sort_values(ascending=False).head(3).to_dict()}\n")

    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(r, wiederholung=w, selten=selten)
        groessen = [int(d.loc[d["fold"] == j, "stadtteil"].nunique())
                    for j in range(1, N_FOLDS + 1)]
        ho = frozenset(d.loc[d["ist_holdout"] == 1, "stadtteil"].unique())
        partitionen[w] = frozenset(
            frozenset(d.loc[d["fold"] == j, "stadtteil"].unique())
            for j in range(1, N_FOLDS + 1))

        kd = wiederholte_aufteilung(k, wiederholung=w, selten=selten)
        brand = [int(((kd["fold"] == j) & (kd[ZIELKLASSE] == SELTENE_KLASSE)).sum())
                 for j in range(1, N_FOLDS + 1)]

        ok = ho == ho_datei and sorted(groessen) == [5, 6, 6, 6, 6] and min(brand) > 0
        fehler += not ok
        print(f"  W{w}  Stadtteile je Fold {groessen} | Brand-Testfaelle {brand}"
              f" | Hold-out unveraendert {ho == ho_datei}  {'ok' if ok else 'FEHLER'}")

    verschieden = len(set(partitionen.values()))
    print(f"\n  Verschiedene Fold-Partitionen: {verschieden} von {WIEDERHOLUNGEN}")
    if verschieden < WIEDERHOLUNGEN:
        print("  FEHLER: Dubletten unter den Wiederholungen.")
        fehler += 1

    # Kein Stadtteil ist je zugleich Trainings- und Testfall.
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(r, wiederholung=w, selten=selten)
        for j in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, j)
            if set(d.loc[tr, "stadtteil"]) & set(d.loc[te, "stadtteil"]):
                print(f"  FEHLER: W{w} Fold {j} teilt Stadtteile.")
                fehler += 1
            if int(d.loc[te, "ist_holdout"].sum()) or int(d.loc[tr, "ist_holdout"].sum()):
                print(f"  FEHLER: W{w} Fold {j} enthaelt Hold-out-Zeilen.")
                fehler += 1

    print("\n  Alle Pruefungen bestanden." if not fehler
          else f"\n  {fehler} Pruefung(en) fehlgeschlagen.")
    return int(bool(fehler))


if __name__ == "__main__":
    raise SystemExit(_selbsttest())
