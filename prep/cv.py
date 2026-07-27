"""
Validierungsschicht: Zeitschnitte, Folds, End-Hold-out, Guetemasse.

Warum liegt das in prep/ und nicht in modelle/? Drei Gruende:

  1. Es schaetzt nichts. Zeitschnitte, Foldgrenzen und Metrikformeln sind
     Festlegungen ueber die Daten, keine Ergebnisse.
  2. Die Eignungspruefung braucht es. VIF und Linearitaet duerfen nur auf
     Trainingsdaten gerechnet werden - dafuer sind die Foldgrenzen noetig.
     Laege cv.py in modelle/, muesste prep/ aus modelle/ importieren.
  3. "Alle drei Verfahren sehen identische Folds" ist eine Zusage ueber den
     DATENSATZ, nicht ueber die Algorithmen (Fairness-Regel, CLAUDE.md).

Aufbau der Zeitachse (Decision Log #14):

    |<---------- Entwicklungsdaten ---------->|<--- HOLD-OUT (12 M) --->|
    |  Fold 1: Train .......... | Test (12 M) |                         |
    |  Fold 2: Train ......................... | Test (12 M) |          |
                                                              ^
                                        wird beim Tuning NIE beruehrt

- Blockiertes Forward Chaining ueber GLOBALE Zeitschnitte: alle Stadtteile
  teilen dieselbe Trennlinie (kein Split je Stadtteil).
- Kein Gap zwischen Train und Test noetig: saemtliche Lag- und Rolling-Features
  sind strikt rueckwaertsgerichtet (shift vor rolling), ein Testmonat greift nie
  auf Werte nach seinem eigenen Zeitpunkt zu.
- Getunt wird auf dem INNEREN Fenster (letzte val_monate des jeweiligen
  Trainingsfensters), nie auf Testmonaten und nie auf dem Hold-out.

Zusaetzlich schreiben die Datensatz-Skripte die Aufteilung als SPALTEN in die
Parquet-Dateien (`fold`, `ist_holdout`). Damit ist sie nachzaehlbar und haengt
nicht davon ab, dass jedes Modellskript die richtige Funktion aufruft.

Ausfuehren (Selbsttest):
  python prep/cv.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import (average_precision_score, f1_score,
                             mean_absolute_error, mean_squared_error,
                             r2_score, roc_auc_score)

from config import N_FOLDS, N_HOLDOUT, N_TEST_MONATE, N_VAL_MONATE


# --------------------------------------------------------------------------
# Zeitschnitte
# --------------------------------------------------------------------------
def zeitachse(daten: pd.DataFrame, spalte: str = "jahr_monat") -> list[int]:
    """Sortierte, eindeutige Monatsschluessel des Datensatzes."""
    return sorted(int(m) for m in daten[spalte].unique())


def split_holdout(monate: list[int],
                  n_holdout: int = N_HOLDOUT) -> tuple[list[int], list[int]]:
    """Trennt die Zeitachse in Entwicklungsdaten und End-Hold-out.

    Das Hold-out umfasst die letzten `n_holdout` Monate und wird waehrend
    Modellauswahl und Hyperparameter-Tuning NICHT verwendet. Es dient
    ausschliesslich der abschliessenden, einmaligen Bewertung.
    """
    if len(monate) <= n_holdout:
        raise ValueError(f"Zeitachse zu kurz ({len(monate)} Monate) "
                         f"fuer ein Hold-out von {n_holdout} Monaten.")
    return monate[:-n_holdout], monate[-n_holdout:]


def zeit_folds(monate: list[int],
               n_folds: int = N_FOLDS,
               test_monate: int = N_TEST_MONATE) -> list[tuple[list[int], list[int]]]:
    """Expanding-Window-Folds ueber sortierte Jahr-Monats-Schluessel.

    Fold 1: Training bis t1, Test t1+1 .. t1+test_monate
    Fold 2: Training bis t1+test_monate, Test ... usw.

    `monate` sollte bereits das Hold-out ausschliessen (siehe split_holdout).
    """
    benoetigt = n_folds * test_monate + test_monate
    if len(monate) < benoetigt:
        raise ValueError(f"Zeitachse zu kurz: {len(monate)} Monate, "
                         f"mindestens {benoetigt} noetig fuer {n_folds} Folds "
                         f"a {test_monate} Testmonate.")
    folds = []
    for i in range(n_folds):
        ende_test  = len(monate) - (n_folds - 1 - i) * test_monate
        start_test = ende_test - test_monate
        folds.append((monate[:start_test], monate[start_test:ende_test]))
    return folds


def inneres_fenster(train_monate: list[int],
                    val_monate: int = N_VAL_MONATE) -> tuple[list[int], list[int]]:
    """Zerlegt ein Trainingsfenster in Sub-Training und Validierung.

    Fuer die Hyperparameter-Suche: Die letzten `val_monate` des Trainings
    dienen als Validierung, der Rest als Sub-Training. Damit wird nie auf
    Testmonaten getunt.
    """
    if len(train_monate) <= val_monate:
        raise ValueError(f"Trainingsfenster zu kurz ({len(train_monate)} Monate) "
                         f"fuer ein inneres Fenster von {val_monate} Monaten.")
    return train_monate[:-val_monate], train_monate[-val_monate:]


def maske(daten: pd.DataFrame, monate: list[int],
          spalte: str = "jahr_monat") -> pd.Series:
    """Boolesche Maske fuer eine Monatsliste."""
    return daten[spalte].isin(monate)


# --------------------------------------------------------------------------
# Aufteilung als Spalten im Datensatz
# --------------------------------------------------------------------------
def ergaenze_aufteilung(daten: pd.DataFrame) -> pd.DataFrame:
    """Schreibt die Spalten `fold` und `ist_holdout` in den Datensatz.

    `fold`         Nummer des Folds, in dessen TESTfenster der Monat liegt.
                   0 = der Monat dient ausschliesslich als Trainingsmaterial.
    `ist_holdout`  1 = Monat gehoert zum unberuehrten End-Hold-out.

    Die Trainingsfenster sind daraus vollstaendig ableitbar (siehe
    `fold_masken`), weil das Training bei Forward Chaining immer aus allen
    Monaten VOR dem Testfenster besteht. Die Aufteilung steht damit nachzaehlbar
    in der Datei und nicht nur in einem Funktionsaufruf.
    """
    d = daten.copy()
    monate = zeitachse(d)
    entwicklung, holdout = split_holdout(monate)

    d["fold"] = 0
    for i, (_, test) in enumerate(zeit_folds(entwicklung), start=1):
        d.loc[d["jahr_monat"].isin(test), "fold"] = i
    d["ist_holdout"] = d["jahr_monat"].isin(holdout).astype(int)
    d["fold"] = d["fold"].astype(int)
    return d


def fold_masken(daten: pd.DataFrame, k: int) -> tuple[pd.Series, pd.Series]:
    """Trainings- und Testmaske des Folds k - allein aus den Spalten der Datei.

    Training = alle Monate vor dem Testfenster, ohne das Hold-out.
    """
    test = daten["fold"] == k
    if not test.any():
        raise ValueError(f"Kein Fold {k} im Datensatz (vorhanden: "
                         f"{sorted(daten['fold'].unique())}).")
    start_test = daten.loc[test, "jahr_monat"].min()
    train = (daten["jahr_monat"] < start_test) & (daten["ist_holdout"] == 0)
    return train, test


def inneres_fenster_masken(daten: pd.DataFrame, k: int) -> tuple[pd.Series, pd.Series]:
    """Sub-Training und inneres Validierungsfenster des Folds k (fuer die Suche)."""
    train, _ = fold_masken(daten, k)
    sub, val = inneres_fenster(sorted(daten.loc[train, "jahr_monat"].unique()))
    return daten["jahr_monat"].isin(sub), daten["jahr_monat"].isin(val)


# --------------------------------------------------------------------------
# Guetemasse
# --------------------------------------------------------------------------
def bewerte_regression(y_true, y_pred) -> dict:
    """RMSE, MAE, R2 - immer auf der ORIGINALSKALA der Zaehlgroesse.

    Modelle, die auf log(1+y) trainiert werden (Ridge), muessen ihre
    Vorhersagen vorher per expm1 zuruecktransformieren.
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return {
        "RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "MAE":  float(mean_absolute_error(y_true, y_pred)),
        "R2":   float(r2_score(y_true, y_pred)),
    }


def bewerte_klassifikation(y_true, p_hat, schwelle: float = 0.5) -> dict:
    """Binaer: F1 (positive Klasse = Brand), AUROC, Average Precision.

    AUROC ist schwellenunabhaengig, F1 nicht - die Schwelle ist daher auf dem
    inneren Validierungsfenster zu waehlen, nicht blind auf 0,5.
    """
    y_true = np.asarray(y_true).astype(int)
    p_hat  = np.asarray(p_hat, dtype=float)
    return {
        "F1":    float(f1_score(y_true, (p_hat >= schwelle).astype(int),
                                zero_division=0)),
        "AUROC": float(roc_auc_score(y_true, p_hat)),
        "AP":    float(average_precision_score(y_true, p_hat)),
        "schwelle": float(schwelle),
    }


def bewerte_mehrklassig(y_true, p_hat, klassen: list[str]) -> dict:
    """Mehrklassig: Macro-F1 und Macro-AUROC (One-vs-Rest).

    Zuordnung ueber argmax statt Schwellenwert (Decision Log #21). Macro
    gewichtet alle vier Klassen gleich, unabhaengig von ihrer Haeufigkeit -
    sonst dominiert Fehlalarm/Good Intent mit 48 % das Ergebnis.
    """
    p_hat  = np.asarray(p_hat, dtype=float)
    y_true = np.asarray(y_true)
    y_pred = np.asarray(klassen)[p_hat.argmax(axis=1)]
    # roc_auc_score verlangt sortierte Labels; die Wahrscheinlichkeitsspalten
    # muessen in derselben Reihenfolge stehen wie `labels`.
    ordnung = sorted(range(len(klassen)), key=lambda i: klassen[i])
    return {
        "Macro-F1":    float(f1_score(y_true, y_pred, average="macro",
                                      labels=klassen, zero_division=0)),
        "Macro-AUROC": float(roc_auc_score(y_true, p_hat[:, ordnung],
                                           multi_class="ovr", average="macro",
                                           labels=[klassen[i] for i in ordnung])),
    }


def beste_schwelle(y_true, p_hat, raster: np.ndarray | None = None) -> float:
    """F1-optimale Schwelle auf dem inneren Validierungsfenster (nur binaer)."""
    y_true = np.asarray(y_true).astype(int)
    p_hat  = np.asarray(p_hat, dtype=float)
    raster = np.arange(0.05, 0.96, 0.01) if raster is None else raster
    werte = [f1_score(y_true, (p_hat >= s).astype(int), zero_division=0)
             for s in raster]
    return float(raster[int(np.argmax(werte))])


# --------------------------------------------------------------------------
# Beschreibung
# --------------------------------------------------------------------------
def beschreibe_splits(monate: list[int]) -> str:
    """Menschenlesbare Zusammenfassung der Zeitschnitte (fuer Kap. 5.2/5.4)."""
    entwicklung, holdout = split_holdout(monate)
    zeilen = [f"Zeitachse gesamt: {monate[0]}-{monate[-1]} ({len(monate)} Monate)",
              f"  Entwicklungsdaten: {entwicklung[0]}-{entwicklung[-1]} "
              f"({len(entwicklung)} Monate)",
              f"  End-Hold-out:      {holdout[0]}-{holdout[-1]} "
              f"({len(holdout)} Monate, beim Tuning unberuehrt)"]
    for i, (tr, te) in enumerate(zeit_folds(entwicklung), 1):
        _, val = inneres_fenster(tr)
        zeilen.append(f"  Fold {i}: Train {tr[0]}-{tr[-1]} ({len(tr)} M) "
                      f"[inneres Val {val[0]}-{val[-1]}] -> "
                      f"Test {te[0]}-{te[-1]} ({len(te)} M)")
    return "\n".join(zeilen)


if __name__ == "__main__":
    from config import PFAD_REGRESSION

    if not PFAD_REGRESSION.exists():
        raise SystemExit("regression.parquet fehlt - erst 'python prep/build.py'.")
    d = pd.read_parquet(PFAD_REGRESSION)
    print(beschreibe_splits(zeitachse(d)))
    print("\n  Aufteilung wie im Datensatz gespeichert:")
    for k in sorted(x for x in d["fold"].unique() if x > 0):
        tr, te = fold_masken(d, k)
        print(f"    Fold {k}: Train {tr.sum():>5,} Zeilen | Test {te.sum():>5,} Zeilen")
    print(f"    Hold-out: {int(d['ist_holdout'].sum()):,} Zeilen")
    print("\n  Pruefungen: python tests/test_aufbereitung.py")
