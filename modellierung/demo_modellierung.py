"""
Minimale, lauffaehige Demo-Modellierungspipeline (Aufgabe 3, vgl. CLAUDE.md).

Zeigt, dass der Gesamtansatz funktioniert, BEVOR das volle Setup
(alle 3 Verfahren + Randomized-Search-Tuning) umgesetzt wird:

  Stadtteil-x-Monat-Datensatz (aggregation.py, balanciertes Panel)
    -> zentrale Zeitschnitte inkl. End-Hold-out (cv.py)
    -> Vergleichsgroessen: naives Modell (Vormonat) + saisonaler Durchschnitt
    -> Beispielmodelle: Ridge auf log(1+y), Random Forest auf y
    -> Guetemasse: RMSE, MAE, R2 (Expose Kap. 3)

Stand 2026-07-26: Splits, Hold-out und Metriken kommen aus `cv.py`, damit alle
spaeteren Modelle zwangslaeufig dieselben Zeitschnitte sehen. Das End-Hold-out
(letzte 12 Monate) wird hier NICHT ausgewertet - es bleibt der finalen
Bewertung vorbehalten.

Ausfuehren:
  python modellierung/demo_modellierung.py
"""
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from cv import (bewerte_regression, beschreibe_splits, split_holdout,
                zeit_folds, zeitachse)
from features import FEATURE_SETS, lade_modelldaten, ridge_sicht

ROOT = Path(__file__).parent.parent
OUT  = ROOT / "results" / "demo_modellierung"

# Zeitraum, Merkmale und Merkmalssaetze sind zentral definiert:
#   aggregation.py -> Panel, Zeitraum, Exposure, Kriminalitaetsindex
#   features.py    -> Saison, Lags, Set S und Set S+L
#   cv.py          -> Folds, inneres Fenster, End-Hold-out, Guetemasse


def baseline_naiv(test: pd.DataFrame) -> np.ndarray:
    """Naives Modell: Wert des Vormonats desselben Stadtteils (= lag_1)."""
    return test["lag_1"].to_numpy()


def baseline_saisonal(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Saisonaler Durchschnitt: Mittelwert desselben Kalendermonats im Training."""
    saison = (train.groupby(["stadtteil", "monat"])["anzahl_einsaetze"]
                   .mean().rename("saison"))
    return (test.join(saison, on=["stadtteil", "monat"])["saison"]
                .fillna(train["anzahl_einsaetze"].mean()).to_numpy())


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    daten = lade_modelldaten(verbose=True)
    feature_sets = FEATURE_SETS

    monate = zeitachse(daten)
    entwicklung, holdout = split_holdout(monate)
    print()
    print(beschreibe_splits(monate))
    print(f"\nDatensatz: {len(daten):,} Zeilen | "
          f"{daten['stadtteil'].nunique()} Stadtteile\n"
          f"HINWEIS: Das End-Hold-out ({holdout[0]}-{holdout[-1]}) wird hier "
          f"bewusst NICHT ausgewertet.\n")

    ergebnisse = []
    for fold_nr, (train_m, test_m) in enumerate(zeit_folds(entwicklung), 1):
        train = daten[daten["jahr_monat"].isin(train_m)]
        test  = daten[daten["jahr_monat"].isin(test_m)]
        y_test = test["anzahl_einsaetze"].to_numpy()

        for name, y_hat in [("Naiv (Vormonat)",         baseline_naiv(test)),
                            ("Saisonaler Durchschnitt", baseline_saisonal(train, test))]:
            ergebnisse.append({"fold": fold_nr, "modell": name,
                               **bewerte_regression(y_test, y_hat)})

        for set_name, spalten in feature_sets.items():
            for modell_name, modell, log_ziel in [
                (f"Ridge ({set_name})",
                 make_pipeline(StandardScaler(), Ridge(alpha=1.0)), True),
                (f"RandomForest ({set_name})",
                 RandomForestRegressor(n_estimators=200, n_jobs=-1,
                                       random_state=42), False),
            ]:
                X_train = ridge_sicht(train, spalten) if log_ziel else train[spalten]
                X_test  = ridge_sicht(test,  spalten) if log_ziel else test[spalten]
                ziel = np.log1p(train["anzahl_einsaetze"]) if log_ziel \
                       else train["anzahl_einsaetze"]
                t0 = time.perf_counter()
                modell.fit(X_train, ziel)
                train_zeit = time.perf_counter() - t0
                y_hat = modell.predict(X_test)
                if log_ziel:
                    y_hat = np.expm1(y_hat)   # Metriken immer auf Originalskala
                ergebnisse.append({"fold": fold_nr, "modell": modell_name,
                                   **bewerte_regression(y_test, y_hat),
                                   "train_s": round(train_zeit, 3)})

    df = pd.DataFrame(ergebnisse)
    mittel = (df.groupby("modell")[["RMSE", "MAE", "R2"]]
                .agg(["mean", "std"]).round(2))
    mittel.columns = [f"{a}_{b}" for a, b in mittel.columns]
    mittel = mittel.sort_values("RMSE_mean")
    print("Ergebnisse je Fold:\n", df.round(2).to_string(index=False))
    print("\nMittelwert +/- Std ueber Folds:\n", mittel.to_string())

    df.to_csv(OUT / "demo_ergebnisse_folds.csv", index=False)
    mittel.to_csv(OUT / "demo_ergebnisse_mittel.csv")
    print(f"\n=> {OUT.relative_to(ROOT)}/demo_ergebnisse_*.csv")
    print("\nNaechster Ausbau: XGBoost + NegBin ergaenzen, dann "
          "Randomized-Search-Tuning auf dem inneren Fenster (cv.inneres_fenster), "
          "final einmalig auf dem End-Hold-out bewerten.")


if __name__ == "__main__":
    main()
