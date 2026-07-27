"""
Vergleichsgroessen der Regression: naiv, saisonal, Negative Binomial.

Ein Verfahrensvergleich ohne Referenz sagt wenig. Erst diese drei Groessen
machen die Ergebnisse der Modelle einordbar:

  Naiv (Vormonat)        Einsatzzahl des Vormonats desselben Stadtteils.
                         Die Zielgroesse ist mit Lag-1-Autokorrelation 0,96
                         stark persistent - wer diese Baseline nicht schlaegt,
                         hat nichts gelernt (Decision Log #8).
  Saisonaler Durchschnitt Mittelwert desselben Kalendermonats im Training,
                         je Stadtteil. Trennt Saisonmuster von echtem Signal.
  Negative Binomial      Interpretierbare Count-Baseline. Poisson scheidet aus:
                         Dispersionsindex 62,8 (Eignungspruefung, Abschnitt 2).
                         log(Bevoelkerung) geht als echter OFFSET ein, nicht als
                         Regressor - der Koeffizient ist damit auf 1 fixiert und
                         die Zielgroesse wird faktisch als Rate modelliert.

Liest ausschliesslich data/processed/regression.parquet.

Ausfuehren:
  python modelle/m01_baselines.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# prep/ enthaelt Konfiguration, Zeitschnitte und Guetemasse.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep"))

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_REGRESSION,  # noqa: E402
                    PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from cv import bewerte_regression, fold_masken  # noqa: E402

OUT = RESULTS_DIR / "regression"


def naiv(test: pd.DataFrame) -> np.ndarray:
    """Wert des Vormonats desselben Stadtteils - steht als lag_1 im Datensatz."""
    return test["lag_1"].to_numpy()


def saisonal(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Mittelwert desselben Kalendermonats im Training, je Stadtteil."""
    mittel = (train.groupby(["stadtteil", "monat"])["anzahl_einsaetze"]
                   .mean().rename("saison"))
    return (test.join(mittel, on=["stadtteil", "monat"])["saison"]
                .fillna(train["anzahl_einsaetze"].mean()).to_numpy())


def negative_binomial(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """NegBin-GLM mit log(Bevoelkerung) als Offset.

    Der Offset unterscheidet dieses Modell von einem gewoehnlichen Regressor
    auf log_bevoelkerung: Der Koeffizient ist auf 1 fixiert, das Modell schaetzt
    also Einsaetze JE EINWOHNER und nicht die Stadtteilgroesse mit. Genau das
    war die Begruendung der Exposure-Entscheidung (Decision Log #13).

    Der Dispersionsparameter alpha wird ueber ein Poisson-Vormodell geschaetzt
    (Momentenschaetzer auf den Pearson-Residuen) - das ist das uebliche
    zweistufige Vorgehen, wenn statsmodels alpha nicht selbst optimiert.
    """
    import statsmodels.api as sm

    spalten = PRAEDIKTOREN + SAISON
    X_tr = sm.add_constant(train[spalten].astype(float), has_constant="add")
    X_te = sm.add_constant(test[spalten].astype(float),  has_constant="add")
    y_tr = train["anzahl_einsaetze"].astype(float)
    off_tr = np.log(train[EXPOSURE_ROH].astype(float))
    off_te = np.log(test[EXPOSURE_ROH].astype(float))

    poisson = sm.GLM(y_tr, X_tr, family=sm.families.Poisson(),
                     offset=off_tr).fit()
    mu = poisson.mu
    alpha = float(np.sum((y_tr - mu) ** 2 / mu - 1) / np.sum(mu))
    alpha = max(alpha, 1e-6)

    negbin = sm.GLM(y_tr, X_tr,
                    family=sm.families.NegativeBinomial(alpha=alpha),
                    offset=off_tr).fit()
    return np.asarray(negbin.predict(X_te, offset=off_te))


def main() -> pd.DataFrame:
    OUT.mkdir(parents=True, exist_ok=True)
    d = pd.read_parquet(PFAD_REGRESSION)

    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(d, k)
        train, test = d[tr], d[te]
        y = test["anzahl_einsaetze"].to_numpy()
        for name, y_hat in [
            ("Naiv (Vormonat)",          naiv(test)),
            ("Saisonaler Durchschnitt",  saisonal(train, test)),
            ("Negative Binomial",        negative_binomial(train, test)),
        ]:
            zeilen.append({"fold": k, "modell": name,
                           **bewerte_regression(y, y_hat)})

    df = pd.DataFrame(zeilen)
    mittel = (df.groupby("modell")[["RMSE", "MAE", "R2"]]
                .agg(["mean", "std"]).round(2))
    mittel.columns = [f"{a}_{b}" for a, b in mittel.columns]
    mittel = mittel.sort_values("RMSE_mean")

    print("Vergleichsgroessen je Fold:\n", df.round(2).to_string(index=False))
    print("\nMittelwert +/- Std ueber die Folds:\n", mittel.to_string())
    print("\nDas End-Hold-out (ist_holdout == 1) wird hier bewusst NICHT "
          "ausgewertet - es bleibt der finalen Bewertung vorbehalten.")

    df.to_csv(OUT / "baselines_folds.csv", index=False)
    mittel.to_csv(OUT / "baselines_mittel.csv")
    print(f"\n  => {OUT.relative_to(ROOT)}/baselines_*.csv")
    return df


if __name__ == "__main__":
    main()
