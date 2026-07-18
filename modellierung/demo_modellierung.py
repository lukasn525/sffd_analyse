"""
Minimale, lauffaehige Demo-Modellierungspipeline (Aufgabe 3, vgl. CLAUDE.md).

Zeigt, dass der Gesamtansatz funktioniert, BEVOR das volle Setup
(alle 3 Verfahren + Randomized-Search-Tuning) umgesetzt wird:

  Stadtteil-x-Monat-Datensatz (aggregation.py)
    -> zeitreihengerechte Cross-Validation (expanding window ueber Monate)
    -> Vergleichsgroessen: naives Modell (Vormonat) + saisonaler Durchschnitt
    -> Beispielmodell: Ridge Regression auf log(1+y)
       (Skalierung noetig fuer Ridge; log-Transformation gemaess
        Linearitaetspruefung, s. results/eignungspruefung/)
    -> Guetemasse: RMSE, MAE, R2 (Expose Kap. 3)

Die bestehende Prep-Pipeline bleibt unveraendert.

Ausfuehren:
  python modellierung/demo_modellierung.py
"""
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from aggregation import PRAEDIKTOREN, lade_stadtteil_monat

ROOT = Path(__file__).parent.parent
OUT  = ROOT / "results" / "demo_modellierung"

N_TEST_MONATE = 12   # Laenge des Testfensters je CV-Fold
N_FOLDS       = 3    # expanding window: Training waechst, Test rollt nach vorn


def zeit_folds(monate: list[int]) -> list[tuple[list[int], list[int]]]:
    """Expanding-Window-Folds ueber sortierte Jahr-Monats-Schluessel.

    Fold 1: Training bis t1, Test t1+1 .. t1+12
    Fold 2: Training bis t1+12, Test ... usw.
    Kein Blick in die Zukunft: Testmonate liegen immer nach dem Training.
    """
    folds = []
    for i in range(N_FOLDS):
        ende_test   = len(monate) - (N_FOLDS - 1 - i) * N_TEST_MONATE
        start_test  = ende_test - N_TEST_MONATE
        folds.append((monate[:start_test], monate[start_test:ende_test]))
    return folds


def bewerte(y_true, y_pred) -> dict:
    return {
        "RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "MAE":  float(mean_absolute_error(y_true, y_pred)),
        "R2":   float(r2_score(y_true, y_pred)),
    }


def baseline_naiv(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Naives Modell: Wert des Vormonats desselben Stadtteils."""
    historie = pd.concat([train, test]).sort_values(["stadtteil", "jahr", "monat"])
    historie["vormonat"] = historie.groupby("stadtteil")["anzahl_einsaetze"].shift(1)
    return historie.loc[test.index, "vormonat"].to_numpy()


def baseline_saisonal(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Saisonaler Durchschnitt: Mittelwert desselben Kalendermonats im Training."""
    saison = (train.groupby(["stadtteil", "monat"])["anzahl_einsaetze"]
                   .mean().rename("saison"))
    return (test.join(saison, on=["stadtteil", "monat"])["saison"]
                .fillna(train["anzahl_einsaetze"].mean()).to_numpy())


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    daten = lade_stadtteil_monat()
    daten = daten.dropna(subset=PRAEDIKTOREN).reset_index(drop=True)
    daten["jahr_monat"] = daten["jahr"] * 100 + daten["monat"]

    # Saisonalitaet als Feature fuer das Modell (zyklische Kodierung des Monats)
    daten["monat_sin"] = np.sin(2 * np.pi * daten["monat"] / 12)
    daten["monat_cos"] = np.cos(2 * np.pi * daten["monat"] / 12)

    # Zusatz-Attribute (Leitfaden A1): zeitliche Persistenz der Zielgroesse als
    # Lag-/Rolling-Features, je Stadtteil und streng vergangenheitsbezogen
    # (shift vor rolling -> kein Leakage). Loesen das Baseline-Problem
    # (Lag-1-Autokorrelation 0,96).
    daten = daten.sort_values(["stadtteil", "jahr_monat"]).reset_index(drop=True)
    g = daten.groupby("stadtteil")["anzahl_einsaetze"]
    daten["lag_1"]          = g.shift(1)
    daten["lag_12"]         = g.shift(12)
    daten["rolling_mean_3"] = g.transform(lambda s: s.shift(1).rolling(3).mean())
    lags = ["lag_1", "lag_12", "rolling_mean_3"]
    # Fairness: identische Zeilen fuer ALLE Modelle/Feature-Sets
    daten = daten.dropna(subset=lags).reset_index(drop=True)

    # Zwei Feature-Sets (vgl. Leitfaden A1): S = nur Struktur+Saison,
    # S+L = zusaetzlich Lags. Kein rohes `jahr` (Extrapolationsproblem, A2).
    feature_sets = {
        "S":   PRAEDIKTOREN + ["monat_sin", "monat_cos"],
        "S+L": PRAEDIKTOREN + ["monat_sin", "monat_cos"] + lags,
    }

    monate = sorted(daten["jahr_monat"].unique())
    print(f"Datensatz: {len(daten):,} Zeilen (Stadtteil x Monat), "
          f"{daten['stadtteil'].nunique()} Stadtteile, "
          f"{len(monate)} Monate ({monate[0]}-{monate[-1]})\n")

    ergebnisse = []
    for fold_nr, (train_m, test_m) in enumerate(zeit_folds(monate), 1):
        train = daten[daten["jahr_monat"].isin(train_m)]
        test  = daten[daten["jahr_monat"].isin(test_m)]
        y_test = test["anzahl_einsaetze"].to_numpy()

        # --- Vergleichsgroessen (Expose: naives Modell + saisonaler Durchschnitt)
        for name, y_hat in [("Naiv (Vormonat)",        baseline_naiv(train, test)),
                            ("Saisonaler Durchschnitt", baseline_saisonal(train, test))]:
            ergebnisse.append({"fold": fold_nr, "modell": name,
                               **bewerte(y_test, y_hat)})

        # --- Beispielmodelle je Feature-Set. Ridge auf log(1+y) (StandardScaler
        # NUR auf Train gefittet, Ruecktransformation expm1); RF direkt auf y.
        # Modellspezifische Aufbereitung fuer Ridge: Lag-Features werden
        # ebenfalls log(1+x)-transformiert, damit die Beziehung zur log-
        # Zielgroesse linear ist (log-AR-Spezifikation). Rohe Lags in einem
        # log-Modell sind fehlspezifiziert (empirisch: R2 < 0). Analog zur
        # Skalierung ist das eine modellinterne Transformation, keine
        # Verletzung der Fairness-Regel (identische Zeilen/Informationen).
        def ridge_sicht(d: pd.DataFrame, spalten: list[str]) -> pd.DataFrame:
            x = d[spalten].copy()
            for c in lags:
                if c in x.columns:
                    x[c] = np.log1p(x[c])
            return x

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
                t0 = time.time()
                modell.fit(X_train, ziel)
                train_zeit = time.time() - t0
                y_hat = modell.predict(X_test)
                if log_ziel:
                    y_hat = np.expm1(y_hat)
                ergebnisse.append({"fold": fold_nr, "modell": modell_name,
                                   **bewerte(y_test, y_hat),
                                   "train_s": round(train_zeit, 3)})

    df = pd.DataFrame(ergebnisse)
    mittel = (df.groupby("modell")[["RMSE", "MAE", "R2"]]
                .mean().round(2).sort_values("RMSE"))
    print("Ergebnisse je Fold:\n", df.round(2).to_string(index=False))
    print("\nMittelwert ueber Folds:\n", mittel.to_string())

    df.to_csv(OUT / "demo_ergebnisse_folds.csv", index=False)
    mittel.to_csv(OUT / "demo_ergebnisse_mittel.csv")
    print(f"\n=> {OUT.relative_to(ROOT)}/demo_ergebnisse_*.csv")
    print("\nNaechster Ausbau: Random Forest & XGBoost mit identischem Datensatz "
          "und identischen Folds ergaenzen, dann Randomized-Search-Tuning (CLAUDE.md).")


if __name__ == "__main__":
    main()
