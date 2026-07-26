"""
Zentrale Validierungsschicht: Zeitschnitte, Folds, Hold-out, Guetemasse.

Alle Modelle (Ridge, Random Forest, XGBoost, Baselines, NegBin) beziehen ihre
Splits und Metriken ausschliesslich aus diesem Modul. Damit ist die
Fairness-Regel aus CLAUDE.md konstruktiv abgesichert: identische Folds fuer
alle Verfahren, keine versehentlich abweichenden Zeitschnitte.

Aufbau der Zeitachse (Decision Log #14, 2026-07-26):

    |<----------- Entwicklungsdaten ----------->|<--- HOLD-OUT (12 M) --->|
    |  Fold 1: Train ............ | Test (12 M) |                         |
    |  Fold 2: Train .......................... | Test (12 M) ... |        |
                                                                  ^
                                        wird beim Tuning NIE beruehrt

- Blockiertes Forward Chaining ueber GLOBALE Zeitschnitte: alle Stadtteile
  teilen dieselbe Trennlinie (kein Split je Stadtteil).
- Kein Gap zwischen Train- und Testfenster noetig: saemtliche Lag- und
  Rolling-Features sind strikt rueckwaertsgerichtet (shift vor rolling), ein
  Testmonat greift nie auf Werte nach seinem eigenen Zeitpunkt zu. Ein Gap
  waere nur bei zentrierten oder vorwaertsgerichteten Fenstern erforderlich.
- Tuning laeuft auf dem INNEREN Fenster (letzte val_monate des jeweiligen
  Trainingsfensters), nie auf den Testmonaten und nie auf dem Hold-out.

Ausfuehren (Selbsttest):
  python modellierung/cv.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import (average_precision_score, f1_score,
                             mean_absolute_error, mean_squared_error,
                             r2_score, roc_auc_score)

# --------------------------------------------------------------------------
# Konfiguration (zentral, damit alle Modelle identische Schnitte sehen)
# --------------------------------------------------------------------------
N_FOLDS        = 3    # Anzahl Forward-Chaining-Folds auf den Entwicklungsdaten
N_TEST_MONATE  = 12   # Laenge eines Testfensters
N_VAL_MONATE   = 12   # inneres Validierungsfenster fuer das Tuning
N_HOLDOUT      = 12   # unberuehrtes End-Hold-out


def zeitachse(daten: pd.DataFrame, spalte: str = "jahr_monat") -> list[int]:
    """Sortierte, eindeutige Monatsschluessel des Datensatzes."""
    return sorted(int(m) for m in daten[spalte].unique())


def split_holdout(monate: list[int],
                  n_holdout: int = N_HOLDOUT) -> tuple[list[int], list[int]]:
    """Trennt die Zeitachse in Entwicklungsdaten und End-Hold-out.

    Das Hold-out umfasst die letzten `n_holdout` Monate und wird waehrend
    Modellauswahl und Hyperparameter-Tuning NICHT verwendet. Es dient
    ausschliesslich der abschliessenden, einmaligen Bewertung der final
    gewaehlten Modelle (Kap. 5.4).
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
    Kein Blick in die Zukunft: Testmonate liegen immer nach dem Training.

    WICHTIG: `monate` sollte bereits das Hold-out ausschliessen
    (siehe split_holdout).
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
    Testmonaten getunt (Leitfaden A7).
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
# Guetemasse
# --------------------------------------------------------------------------
def bewerte_regression(y_true, y_pred) -> dict:
    """RMSE, MAE, R2 - immer auf der ORIGINALSKALA der Zaehlgroesse.

    Modelle, die auf log(1+y) trainiert werden (Ridge), muessen ihre
    Vorhersagen vorher per expm1 zuruecktransformieren (Leitfaden A5).
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return {
        "RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "MAE":  float(mean_absolute_error(y_true, y_pred)),
        "R2":   float(r2_score(y_true, y_pred)),
    }


def bewerte_klassifikation(y_true, p_hat, schwelle: float = 0.5) -> dict:
    """F1 (positive Klasse = Brand), AUROC, Average Precision.

    AUROC ist schwellenunabhaengig, F1 nicht - die Schwelle ist daher auf dem
    inneren Validierungsfenster zu waehlen, nicht blind auf 0,5 (Leitfaden A8).
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


def beste_schwelle(y_true, p_hat,
                   raster: np.ndarray | None = None) -> float:
    """Waehlt die F1-optimale Schwelle auf dem inneren Validierungsfenster."""
    y_true = np.asarray(y_true).astype(int)
    p_hat  = np.asarray(p_hat, dtype=float)
    raster = np.arange(0.05, 0.96, 0.01) if raster is None else raster
    werte = [f1_score(y_true, (p_hat >= s).astype(int), zero_division=0)
             for s in raster]
    return float(raster[int(np.argmax(werte))])


def beschreibe_splits(monate: list[int]) -> str:
    """Menschenlesbare Zusammenfassung der Zeitschnitte (fuer Kap. 5.2/5.4)."""
    entwicklung, holdout = split_holdout(monate)
    zeilen = [f"Zeitachse gesamt: {monate[0]}-{monate[-1]} ({len(monate)} Monate)",
              f"  Entwicklungsdaten: {entwicklung[0]}-{entwicklung[-1]} "
              f"({len(entwicklung)} Monate)",
              f"  End-Hold-out:      {holdout[0]}-{holdout[-1]} "
              f"({len(holdout)} Monate, beim Tuning unberuehrt)"]
    for i, (tr, te) in enumerate(zeit_folds(entwicklung), 1):
        sub, val = inneres_fenster(tr)
        zeilen.append(f"  Fold {i}: Train {tr[0]}-{tr[-1]} ({len(tr)} M) "
                      f"[inneres Val {val[0]}-{val[-1]}] -> "
                      f"Test {te[0]}-{te[-1]} ({len(te)} M)")
    return "\n".join(zeilen)


if __name__ == "__main__":
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent))
    from aggregation import balanciertes_panel, lade_stadtteil_monat

    d = balanciertes_panel(lade_stadtteil_monat())
    d["jahr_monat"] = d["jahr"] * 100 + d["monat"]
    monate = zeitachse(d)
    print(beschreibe_splits(monate))

    entwicklung, holdout = split_holdout(monate)
    assert set(entwicklung).isdisjoint(holdout), "Hold-out ueberlappt Entwicklung."
    for tr, te in zeit_folds(entwicklung):
        assert max(tr) < min(te), "Testfenster liegt nicht nach dem Training."
        assert set(te).isdisjoint(holdout), "Fold-Test greift ins Hold-out."
        sub, val = inneres_fenster(tr)
        assert max(sub) < min(val) < min(te), "Inneres Fenster falsch geordnet."
    print("\n  Selbsttests bestanden (Ordnung, Disjunktheit, Hold-out unberuehrt).")
