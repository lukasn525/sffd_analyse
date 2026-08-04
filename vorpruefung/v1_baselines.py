"""
Stufe 1 und 2: die Messlatte.

Eine Baseline ist eine bewusst einfache Regel, die dieselbe Aufgabe loest und
dieselben Daten sieht. Sie legt fest, was die Vergleichsverfahren mindestens
schlagen muessen. Es gibt zwei Stufen:

  STUFE 1  Triviale Referenz - benutzt KEIN einziges Merkmal.
           Regression:    Gesamtmittelwert der Trainingsstadtteile
           Klassifikation: immer die haeufigste Klasse
           Beantwortet: Steckt in den Merkmalen ueberhaupt Information?

  STUFE 2  Einfachste Referenz, die zur DATENFORM passt - benutzt alle Merkmale,
           aber in der simpelsten Form.
           Regression:    Negative Binomial (Zaehldaten, ueberdispers)
           Klassifikation: multinomiale logistische Regression (nominale Klassen)
           Beantwortet: Wie weit kommt man mit der einfachen Form?

Stufe 3 sind die Vergleichsverfahren in modelle/. Ihre Aufgabe ist zu zeigen,
dass sie Stufe 2 schlagen - sonst hat sich der Mehraufwand nicht gelohnt.

Alle Baselines laufen ueber denselben STADTTEIL-SPLIT wie die Modelle: Der
Teststadtteil ist unbekannt. Das Hold-out bleibt unberuehrt.

Eingang:  data/processed/{regression,klassifikation}.parquet
Ausgang:  results/regression/baselines_{folds,mittel}.csv
          results/klassifikation/baselines_klasse.csv

Ausfuehren:
  python vorpruefung/v1_baselines.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep"))

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from s2_datensaetze import (RATE, ZIELGROESSE, ZIELKLASSE,  # noqa: E402
                            fold_masken)

OUT   = RESULTS_DIR / "regression"
OUT_K = RESULTS_DIR / "klassifikation"
MERKMALE = PRAEDIKTOREN + SAISON


def bewerte_regression(y_true, y_pred) -> dict:
    """RMSE, MAE, R2 - immer auf der ORIGINALSKALA der Zielgroesse."""
    from sklearn.metrics import (mean_absolute_error, mean_squared_error,
                                 r2_score)
    y_true, y_pred = np.asarray(y_true, float), np.asarray(y_pred, float)
    return {"RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "MAE":  float(mean_absolute_error(y_true, y_pred)),
            "R2":   float(r2_score(y_true, y_pred))}


# ---------------------------------------------------------------------------
def negative_binomial(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Stufe 2 der Regression: vorhergesagte Einsatzzahlen.

    log(Bevoelkerung) geht als OFFSET ein, nicht als normales Merkmal. Das
    Modell schaetzt damit Einsaetze JE EINWOHNER und multipliziert am Ende hoch -
    sonst wuerde es vor allem die Stadtteilgroesse vorhersagen (#13).
    alpha (die Ueberdispersion) kommt aus einem Poisson-Vormodell.
    """
    import statsmodels.api as sm

    X_tr = sm.add_constant(train[MERKMALE].astype(float), has_constant="add")
    X_te = sm.add_constant(test[MERKMALE].astype(float),  has_constant="add")
    y_tr = train[ZIELGROESSE].astype(float)
    off_tr = np.log(train[EXPOSURE_ROH].astype(float))
    off_te = np.log(test[EXPOSURE_ROH].astype(float))

    mu = sm.GLM(y_tr, X_tr, family=sm.families.Poisson(), offset=off_tr).fit().mu
    alpha = max(float(np.sum((y_tr - mu) ** 2 / mu - 1) / np.sum(mu)), 1e-6)
    negbin = sm.GLM(y_tr, X_tr, offset=off_tr,
                    family=sm.families.NegativeBinomial(alpha=alpha)).fit()
    return np.asarray(negbin.predict(X_te, offset=off_te))


def regression(panel: pd.DataFrame) -> pd.DataFrame:
    """Beide Mengen-Zielgroessen, Stufe 1 und 2, je Fold.

    Die Rate ergibt sich aus derselben NegBin-Vorhersage geteilt durch die
    Bevoelkerung - ein zweites Modell waere eine zweite Spezifikation und damit
    unfair gegenueber den Vergleichsverfahren.
    """
    OUT.mkdir(parents=True, exist_ok=True)
    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(panel, k)
        train, test = panel[tr], panel[te]
        anzahl  = negative_binomial(train, test)
        je_1000 = anzahl / test[EXPOSURE_ROH].to_numpy() * 1000

        for ziel, negbin in ((ZIELGROESSE, anzahl), (RATE, je_1000)):
            y = test[ziel].to_numpy()
            for stufe, name, y_hat in (
                    (1, "Gesamtmittelwert", np.full(len(test), train[ziel].mean())),
                    (2, "Negative Binomial", negbin)):
                zeilen.append({"fold": k, "stufe": stufe, "zielgroesse": ziel,
                               "modell": name, **bewerte_regression(y, y_hat)})

    df = pd.DataFrame(zeilen)
    mittel = (df.groupby(["zielgroesse", "stufe", "modell"])[["RMSE", "MAE", "R2"]]
                .agg(["mean", "std"]).round(3))
    mittel.columns = [f"{a}_{b}" for a, b in mittel.columns]
    df.to_csv(OUT / "baselines_folds.csv", index=False)
    mittel.to_csv(OUT / "baselines_mittel.csv")
    return mittel


# ---------------------------------------------------------------------------
def klassifikation(kl: pd.DataFrame) -> pd.DataFrame:
    """Beide Stufen der Klassifikation, je Fold.

    STUFE 1, Mehrheitsklasse: sagt immer die im Training haeufigste Einsatzart
    vorher. Accuracy faellt hoch aus, Macro-F1 niedrig - genau deshalb ist
    Macro-F1 das massgebliche Guetemass.

    STUFE 2, multinomiale logistische Regression: das Gegenstueck zur Negative
    Binomial. Sie ist die einfachste Form, die zu einer nominalen Zielgroesse
    passt - linear in den Log-Odds, L2-penalisiert. RF und XGBoost muessen SIE
    schlagen, nicht die Mehrheitsklasse (Decision Log #33).
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, f1_score
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    OUT_K.mkdir(parents=True, exist_ok=True)
    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(kl, k)
        X_tr, X_te = kl.loc[tr, MERKMALE].astype(float), kl.loc[te, MERKMALE].astype(float)
        y_tr, y_te = kl.loc[tr, ZIELKLASSE], kl.loc[te, ZIELKLASSE]

        haeufigste = y_tr.value_counts().idxmax()
        logreg = make_pipeline(
            StandardScaler(),
            LogisticRegression(max_iter=2000, class_weight="balanced")).fit(X_tr, y_tr)

        for stufe, name, y_hat in (
                (1, f"Mehrheitsklasse ({haeufigste})",
                 np.full(len(y_te), haeufigste)),
                (2, "Logistische Regression (L2)", logreg.predict(X_te))):
            zeilen.append({
                "fold": k, "stufe": stufe, "modell": name,
                "Accuracy": round(float(accuracy_score(y_te, y_hat)), 3),
                "Macro-F1": round(float(f1_score(y_te, y_hat, average="macro",
                                                 zero_division=0)), 3)})

    df = pd.DataFrame(zeilen)
    df.to_csv(OUT_K / "baselines_klasse.csv", index=False)
    return df


# ---------------------------------------------------------------------------
def run() -> None:
    for pfad in (PFAD_REGRESSION, PFAD_KLASSIFIKATION):
        if not pfad.exists():
            raise SystemExit(f"{pfad.relative_to(ROOT)} fehlt - "
                             f"erst 'python prep/build.py' ausfuehren.")

    mittel = regression(pd.read_parquet(PFAD_REGRESSION))
    print("Regression - Mittel +/- Std ueber die Folds:\n", mittel.to_string())

    df = klassifikation(pd.read_parquet(PFAD_KLASSIFIKATION))
    print("\nKlassifikation - Mittel ueber die Folds:")
    for (stufe, modell), g in df.groupby(["stufe", "modell"]):
        print(f"  Stufe {stufe}  {modell:<32} "
              f"Macro-F1 {g['Macro-F1'].mean():.3f} | "
              f"Accuracy {g['Accuracy'].mean():.3f}")

    print(f"\n  => {OUT.relative_to(ROOT)}/baselines_*.csv")
    print(f"  => {OUT_K.relative_to(ROOT)}/baselines_klasse.csv")
    print("\n  Das Hold-out bleibt der Schlussbewertung vorbehalten.")


if __name__ == "__main__":
    run()
