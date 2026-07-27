"""
Schritt 3: die Vergleichsgroessen, an denen sich jedes Modell messen lassen muss.

Eingang:  data/processed/regression.parquet
Ausgang:  results/regression/baselines_folds.csv   je Fold
          results/regression/baselines_mittel.csv  Mittel +/- Std

Die Baselines gehoeren in die Data Preparation (Auflage Schroeter, 27.07.2026):
Sie legen den Referenzwert fest, BEVOR modelliert wird, und beantworten die
Leitfrage "bringt der zusaetzliche Aufwand der Modelle ueberhaupt etwas?".
Sie tunen nichts und waehlen nichts aus - deshalb kein Modellskript.

  Naiv       Vormonatswert desselben Stadtteils (steht als lag_1 im Datensatz).
             Die Zielgroesse ist mit Lag-1-Autokorrelation 0,96 stark
             persistent - wer das nicht schlaegt, hat nichts gelernt (#8).
             Nichtparametrisch: unterstellt keinerlei Funktionsform.
  Saisonal   Mittelwert desselben Kalendermonats im Training, je Stadtteil.
             Trennt das Saisonmuster von echtem Signal. Ebenfalls
             nichtparametrisch.
  NegBin     Interpretierbare Count-Baseline; Poisson scheidet aus
             (Dispersionsindex 62,8). log(Bevoelkerung) geht als echter OFFSET
             ein: Der Koeffizient ist auf 1 fixiert, das Modell schaetzt
             Einsaetze JE EINWOHNER statt der Stadtteilgroesse (#13). Der
             Dispersionsparameter alpha kommt aus einem Poisson-Vormodell
             (Momentenschaetzer auf den Pearson-Residuen).

Das End-Hold-out (`ist_holdout == 1`) wird bewusst NICHT ausgewertet - es bleibt
der einmaligen Schlussbewertung vorbehalten.

Ausfuehren:
  python prep/s3_baselines.py
"""
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_REGRESSION, PRAEDIKTOREN,
                    RESULTS_DIR, ROOT, SAISON)
from s2_datensaetze import fold_masken

OUT = RESULTS_DIR / "regression"


def bewerte_regression(y_true, y_pred) -> dict:
    """RMSE, MAE, R2 - immer auf der ORIGINALSKALA der Zaehlgroesse.

    Modelle auf log(1+y) (Ridge) muessen vorher per expm1 zuruecktransformieren.
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return {"RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "MAE":  float(mean_absolute_error(y_true, y_pred)),
            "R2":   float(r2_score(y_true, y_pred))}


def rechne_baselines(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Alle drei Vergleichsgroessen je Fold. Gibt (je Fold, Mittel) zurueck."""
    import statsmodels.api as sm

    OUT.mkdir(parents=True, exist_ok=True)
    spalten = PRAEDIKTOREN + SAISON
    zeilen = []

    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(panel, k)
        train, test = panel[tr], panel[te]
        y = test["anzahl_einsaetze"].to_numpy()

        saison = (train.groupby(["stadtteil", "monat"])["anzahl_einsaetze"]
                       .mean().rename("saison"))
        y_saisonal = (test.join(saison, on=["stadtteil", "monat"])["saison"]
                          .fillna(train["anzahl_einsaetze"].mean()).to_numpy())

        X_tr = sm.add_constant(train[spalten].astype(float), has_constant="add")
        X_te = sm.add_constant(test[spalten].astype(float),  has_constant="add")
        y_tr = train["anzahl_einsaetze"].astype(float)
        off_tr = np.log(train[EXPOSURE_ROH].astype(float))
        off_te = np.log(test[EXPOSURE_ROH].astype(float))
        mu = sm.GLM(y_tr, X_tr, family=sm.families.Poisson(), offset=off_tr).fit().mu
        alpha = max(float(np.sum((y_tr - mu) ** 2 / mu - 1) / np.sum(mu)), 1e-6)
        negbin = sm.GLM(y_tr, X_tr, offset=off_tr,
                        family=sm.families.NegativeBinomial(alpha=alpha)).fit()

        for name, y_hat in [
            ("Naiv (Vormonat)",         test["lag_1"].to_numpy()),
            ("Saisonaler Durchschnitt", y_saisonal),
            ("Negative Binomial",       np.asarray(negbin.predict(X_te, offset=off_te))),
        ]:
            zeilen.append({"fold": k, "modell": name, **bewerte_regression(y, y_hat)})

    df = pd.DataFrame(zeilen)
    mittel = (df.groupby("modell")[["RMSE", "MAE", "R2"]]
                .agg(["mean", "std"]).round(2))
    mittel.columns = [f"{a}_{b}" for a, b in mittel.columns]
    mittel = mittel.sort_values("RMSE_mean")

    df.to_csv(OUT / "baselines_folds.csv", index=False)
    mittel.to_csv(OUT / "baselines_mittel.csv")
    return df, mittel


def run() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not PFAD_REGRESSION.exists():
        raise SystemExit(f"{PFAD_REGRESSION.relative_to(ROOT)} fehlt - "
                         f"erst 'python prep/build.py' ausfuehren.")
    df, mittel = rechne_baselines(pd.read_parquet(PFAD_REGRESSION))
    print("\nVergleichsgroessen je Fold:\n", df.round(2).to_string(index=False))
    print("\nMittelwert +/- Std ueber die Folds:\n", mittel.to_string())
    print(f"\n  => {OUT.relative_to(ROOT)}/baselines_*.csv")
    print("\n  Das End-Hold-out bleibt der Schlussbewertung vorbehalten.")
    return df, mittel


if __name__ == "__main__":
    run()
