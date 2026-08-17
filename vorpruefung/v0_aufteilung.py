"""
Wiederholte Splits - die eine Stelle, an der die Fold-Zuteilung entsteht.

    python vorpruefung/v0_aufteilung.py    Selbsttest

Eingang: data/processed/{regression,klassifikation}.parquet mit den Spalten
         fold und ist_holdout aus prep/s2_datensaetze.ergaenze_aufteilung()
Ausgang: keine Datei - liefert Datenrahmen an v1_baselines.py, m02_menge.py
         und m03_struktur.py

  - Die Grundaufteilung aus der Datei reicht fuer die 10 Wiederholungen
    nicht: ein Versatz rotiert nur die Beschriftung der Gruppen, nicht ihre
    Zusammensetzung - und rotiert dabei das Hold-out mit (B-1, B-2)
  - Gemischt wird deshalb INNERHALB der Rangbloecke. Jeder Fold behaelt
    genau einen Stadtteil je Block, die Foldgroessen bleiben 6/6/6/6/5,
    aber die Zusammensetzung aendert sich wirklich
  - Drei Zusagen: das Hold-out bleibt fest, die doppelte Stratifizierung
    (#30) bleibt erhalten, und Wiederholung 0 reproduziert die fold-Spalte
    der Datei bitgenau - per assert geprueft, nicht behauptet
  - Kein Leakage: gemischt wird ausschliesslich, WELCHE Stadtteile
    gemeinsam getestet werden. Kein Modell sieht dadurch eine Zeile mehr
  - Alle drei Aufrufer muessen dieselbe Zuteilung sehen, sonst vergleicht
    der gepaarte Wilcoxon-Test still auf verschiedenen Zeilen

Ausfuehrliche Fassung: docs/08_FUNKTIONSDOKUMENTATION.md
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
    """Zahl der brand-dominierten Monate je Stadtteil.

    Ein:  klassifikation.parquet als Datenrahmen
    Aus:  Reihe stadtteil -> Anzahl

    - Stratifizierungsmass der Fold-Zuteilung (#30)
    - identisch zur Berechnung in prep/s2_datensaetze.run()
    - der Wert steht in keiner Datei; deshalb liest auch der Regressionsstrang
      klassifikation.parquet mit
    - kein Leakage: geht in kein Modell ein, bestimmt nur die Testgruppen
    """
    return (klassifikation[klassifikation[ZIELKLASSE] == SELTENE_KLASSE]
            .groupby("stadtteil").size())


def wiederholte_aufteilung(daten: pd.DataFrame, wiederholung: int = 0,
                           selten: pd.Series | None = None) -> pd.DataFrame:
    """Belegt die fold-Spalte fuer eine Wiederholung neu.

    Ein:  Datenrahmen mit fold/ist_holdout, Wiederholung 0..9, `selten`
    Aus:  Kopie mit neuer fold-Spalte; ist_holdout unveraendert

    - Wiederholung 0 reproduziert die Datei bitgenau, per assert geprueft
    - Wiederholungen 1..9 mischen innerhalb der Rangbloecke; Foldgroessen und
      Stratifizierung bleiben erhalten
    - Hold-out-Zeilen behalten fold = 0 und bleiben in jeder Wiederholung
      ausgeschlossen
    - ohne `selten` wird nur nach Bevoelkerung stratifiziert; das reproduziert
      die Datei NICHT und ist nur fuer Sonderfaelle gedacht
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
    """Masken der Schlussbewertung: 29 Entwicklungs- gegen 6 Hold-out-Stadtteile.

    Ein:  Datenrahmen mit Spalte ist_holdout
    Aus:  zwei boolesche Reihen (Entwicklung, Hold-out)

    - Gegenstueck zu fold_masken() fuer den einen Lauf, der das Hold-out liest
    - einzige Stelle im Repo, die ist_holdout == 1 auswertet
    """
    return daten["ist_holdout"] == 0, daten["ist_holdout"] == 1


# ==========================================================================
# Selbsttest - beantwortet die vier Fragen, an denen diese Datei haengt
# ==========================================================================
def _selbsttest() -> int:
    """Selbsttest ueber alle 10 Wiederholungen.

    Ein:  beide Parquet-Dateien
    Aus:  Exitcode 0 bei Erfolg, 1 bei mindestens einem Fehler

    Geprueft wird:
    - Foldgroessen 6/6/6/6/5
    - mindestens ein Brand-Testfall je Fold
    - Hold-out in jeder Wiederholung unveraendert
    - 10 verschiedene Partitionen, keine Dubletten
    - kein Stadtteil zugleich Trainings- und Testfall
    """
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
