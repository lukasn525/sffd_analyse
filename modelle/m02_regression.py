"""
Verfahrensvergleich Regression: Ridge, Random Forest, XGBoost.

Liest ausschliesslich data/processed/regression.parquet. Zeitschnitte, Merkmals-
saetze und Suchraeume kommen aus prep/ - dieses Skript legt nichts fest, es
rechnet nur.

Stand: lauffaehiger Vergleich OHNE Hyperparameter-Suche (Standardparameter).
Der naechste Ausbauschritt ist die RandomizedSearchCV auf dem inneren
Validierungsfenster; der Suchraum steht bereits in prep/config.py
(`SUCHRAEUME`, `TUNING_BUDGET`). Bewusst kein eigenes Tuning-Skript: Die besten
Parameter wuerden sonst in einer Datei liegen, die veralten kann, ohne dass es
auffaellt.

FAIRNESS-REGEL: Alle Verfahren sehen identische Zeilen, Merkmale und Folds. Die
Folds stehen als Spalten im Datensatz (`fold`, `ist_holdout`) und werden nicht
hier berechnet. Modellspezifisch ist nur, was innerhalb der sklearn-Pipeline je
Fold passiert:
  Ridge  StandardScaler + Zielgroesse log(1+y) + Lags log(1+x)
         (Decision Log #2 und #9; ohne die Log-Lags ergab sich R2 < 0)
  RF/XGB rohe Skala, keine Skalierung noetig

Das End-Hold-out (`ist_holdout == 1`) wird hier NICHT ausgewertet. Es bleibt der
einmaligen Schlussbewertung vorbehalten (Decision Log #14).

Ausfuehren:
  python modelle/m02_regression.py
"""
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep"))

from config import (FEATURE_SETS, LAGS, N_FOLDS,  # noqa: E402
                    PFAD_REGRESSION, RANDOM_STATE, RESULTS_DIR, ROOT)
from s2_datensaetze import beschreibe_splits, fold_masken, zeitachse  # noqa: E402
from s3_baselines import bewerte_regression  # noqa: E402

OUT = RESULTS_DIR / "regression"


def ridge_sicht(d: pd.DataFrame, spalten: list[str]) -> pd.DataFrame:
    """Modellspezifische Aufbereitung der Lags fuer Ridge.

    Ridge wird auf log(1+y) geschaetzt. Damit die Beziehung zwischen Lags und
    log-Zielgroesse linear ist, muessen auch die Lags logarithmiert werden
    (log-AR-Spezifikation). Rohe Lags in einem log-Modell sind fehlspezifiziert -
    empirisch ergab das R2 < 0.

    Steht bewusst HIER und nicht in prep/: Es ist eine modellinterne
    Transformation wie die Standardisierung, keine Eigenschaft des Datensatzes.
    Und KEINE Verletzung der Fairness-Regel - identische Zeilen, identische
    Information, nur eine andere Darstellung (Decision Log #9).
    """
    x = d[spalten].copy()
    for c in LAGS:
        if c in x.columns:
            x[c] = np.log1p(x[c])
    return x


def modelle():
    """Verfahren mit Angabe, ob auf log(1+y) geschaetzt wird.

    XGBoost wird nur aufgenommen, wenn das Paket installiert ist - so bleibt das
    Skript auch ohne xgboost lauffaehig.
    """
    liste = [
        ("Ridge", lambda: make_pipeline(StandardScaler(), Ridge(alpha=1.0)), True),
        ("Random Forest",
         lambda: RandomForestRegressor(n_estimators=500, n_jobs=-1,
                                       random_state=RANDOM_STATE), False),
    ]
    try:
        from xgboost import XGBRegressor
        liste.append(("XGBoost",
                      lambda: XGBRegressor(n_estimators=500, learning_rate=0.05,
                                           max_depth=6, subsample=0.8,
                                           colsample_bytree=0.8, n_jobs=-1,
                                           random_state=RANDOM_STATE,
                                           tree_method="hist"), False))
    except ImportError:
        print("  HINWEIS: xgboost nicht installiert - wird uebersprungen "
              "(`pip install xgboost`).\n")
    return liste


def main() -> pd.DataFrame:
    OUT.mkdir(parents=True, exist_ok=True)
    d = pd.read_parquet(PFAD_REGRESSION)

    print(beschreibe_splits(zeitachse(d)))
    print(f"\nDatensatz: {len(d):,} Zeilen | {d['stadtteil'].nunique()} Stadtteile "
          f"| {d['jahr_monat'].min()}-{d['jahr_monat'].max()}")
    print("Das End-Hold-out wird hier bewusst NICHT ausgewertet.\n")

    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(d, k)
        train, test = d[tr], d[te]
        y_train = train["anzahl_einsaetze"]
        y_test  = test["anzahl_einsaetze"].to_numpy()

        for set_name, spalten in FEATURE_SETS.items():
            for name, bauen, log_ziel in modelle():
                X_train = ridge_sicht(train, spalten) if log_ziel else train[spalten]
                X_test  = ridge_sicht(test,  spalten) if log_ziel else test[spalten]
                ziel    = np.log1p(y_train) if log_ziel else y_train

                modell = bauen()
                t0 = time.perf_counter()
                modell.fit(X_train, ziel)
                train_s = time.perf_counter() - t0
                t0 = time.perf_counter()
                y_hat = modell.predict(X_test)
                inferenz_s = time.perf_counter() - t0
                if log_ziel:
                    y_hat = np.expm1(y_hat)   # Guetemasse immer auf Originalskala

                zeilen.append({"fold": k, "modell": f"{name} ({set_name})",
                               **bewerte_regression(y_test, y_hat),
                               "train_s": round(train_s, 3),
                               "inferenz_s": round(inferenz_s, 4)})

    df = pd.DataFrame(zeilen)
    mittel = (df.groupby("modell")[["RMSE", "MAE", "R2", "train_s"]]
                .agg(["mean", "std"]).round(3))
    mittel.columns = [f"{a}_{b}" for a, b in mittel.columns]
    mittel = mittel.sort_values("RMSE_mean")

    print("Ergebnisse je Fold:\n", df.round(2).to_string(index=False))
    print("\nMittelwert +/- Std ueber die Folds:\n", mittel.to_string())

    df.to_csv(OUT / "regression_folds.csv", index=False)
    mittel.to_csv(OUT / "regression_mittel.csv")
    print(f"\n  => {OUT.relative_to(ROOT)}/regression_*.csv")
    print("\nNaechster Ausbau: RandomizedSearchCV auf dem inneren "
          "Validierungsfenster (prep/s2_datensaetze.inneres_fenster) mit den "
          "Suchraeumen aus prep/config.SUCHRAEUME, danach EINMALIG auf dem "
          "End-Hold-out bewerten.")
    return df


if __name__ == "__main__":
    main()
