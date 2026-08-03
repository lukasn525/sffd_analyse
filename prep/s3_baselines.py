"""
Schritt 3: die Vergleichswerte, an denen sich jedes Modell messen lassen muss.

Eingang:  data/processed/regression.parquet · klassifikation.parquet
Ausgang:  results/regression/baselines_folds.csv · baselines_mittel.csv
          results/klassifikation/baselines_klasse.csv

Die Baselines gehoeren in die Data Preparation (Auflage Schroeter, 27.07.2026).
Sie sind kein Genehmigungspunkt, sondern ein Beweismittel: Sie legen fest, was
die drei Verfahren mindestens schlagen muessen, damit sich ihr Aufwand gelohnt
hat. Alle laufen ueber denselben STADTTEIL-SPLIT wie die Modelle - der
Teststadtteil ist unbekannt (Decision Log #29).

  REGRESSION: Negative Binomial (Decision Log #32)
      Ein vollwertiges Zaehldatenmodell mit denselben zwoelf Merkmalen,
      denselben Zeilen und denselben Folds wie die Verfahren. log(Bevoelkerung)
      geht als OFFSET ein: geschaetzt werden Einsaetze JE EINWOHNER, nicht die
      Stadtteilgroesse (Decision Log #13). alpha kommt aus einem
      Poisson-Vormodell (Momentenschaetzer auf den Pearson-Residuen).
      Sie kann Kruemmung, aber keine Wechselwirkungen zwischen Merkmalen -
      genau die Luecke, die Random Forest und XGBoost schliessen sollen.
      Die Rate ergibt sich aus derselben Vorhersage, geteilt durch die
      Bevoelkerung; ein zweites Modell waere eine zweite Spezifikation.

  KLASSIFIKATION: Mehrheitsklasse (Decision Log #32)
      Sagt immer die im Training haeufigste Einsatzart vorher. Die Negative
      Binomial ist hier nicht anwendbar - sie sagt eine Zahl vorher, die
      Zielgroesse ist eine von vier ungeordneten Kategorien. Die Referenz ist
      damit schwaecher als in der Regression; das ist in Kapitel 5 zu benennen.

  Gesamtmittelwert laeuft in beiden Faellen als NULLMARKE mit. Er ist kein
  Gegner, sondern der Bezugspunkt, der R2 lesbar macht: R2 = 0 heisst "so gut
  wie der Durchschnitt", negativ heisst "schlechter als der Durchschnitt".

Das Hold-out (`ist_holdout == 1`) wird NICHT ausgewertet - es bleibt der
einmaligen Schlussbewertung vorbehalten.

Ausfuehren:
  python prep/s3_baselines.py
"""
import numpy as np
import pandas as pd
from sklearn.metrics import (accuracy_score, f1_score, mean_absolute_error,
                             mean_squared_error, r2_score)

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION, PFAD_REGRESSION,
                    PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from s2_datensaetze import RATE, ZIELGROESSE, ZIELKLASSE, fold_masken

OUT   = RESULTS_DIR / "regression"
OUT_K = RESULTS_DIR / "klassifikation"


def bewerte_regression(y_true, y_pred) -> dict:
    """RMSE, MAE, R2 - immer auf der ORIGINALSKALA der Zielgroesse."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return {"RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "MAE":  float(mean_absolute_error(y_true, y_pred)),
            "R2":   float(r2_score(y_true, y_pred))}


def negative_binomial(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Vorhergesagte Einsatzzahlen fuer die Teststadtteile."""
    import statsmodels.api as sm

    spalten = PRAEDIKTOREN + SAISON
    X_tr = sm.add_constant(train[spalten].astype(float), has_constant="add")
    X_te = sm.add_constant(test[spalten].astype(float),  has_constant="add")
    y_tr = train[ZIELGROESSE].astype(float)
    off_tr = np.log(train[EXPOSURE_ROH].astype(float))
    off_te = np.log(test[EXPOSURE_ROH].astype(float))

    mu = sm.GLM(y_tr, X_tr, family=sm.families.Poisson(), offset=off_tr).fit().mu
    alpha = max(float(np.sum((y_tr - mu) ** 2 / mu - 1) / np.sum(mu)), 1e-6)
    negbin = sm.GLM(y_tr, X_tr, offset=off_tr,
                    family=sm.families.NegativeBinomial(alpha=alpha)).fit()
    return np.asarray(negbin.predict(X_te, offset=off_te))


def rechne_baselines(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Vergleichswerte der Regression, je Fold und Zielgroesse."""
    OUT.mkdir(parents=True, exist_ok=True)
    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(panel, k)
        train, test = panel[tr], panel[te]
        anzahl = negative_binomial(train, test)
        je_1000 = anzahl / test[EXPOSURE_ROH].to_numpy() * 1000

        for ziel, negbin_vorhersage in ((ZIELGROESSE, anzahl), (RATE, je_1000)):
            y = test[ziel].to_numpy()
            for name, y_hat in (
                ("Negative Binomial", negbin_vorhersage),
                ("Gesamtmittelwert", np.full(len(test), train[ziel].mean())),
            ):
                zeilen.append({"fold": k, "zielgroesse": ziel, "modell": name,
                               **bewerte_regression(y, y_hat)})

    df = pd.DataFrame(zeilen)
    mittel = (df.groupby(["zielgroesse", "modell"])[["RMSE", "MAE", "R2"]]
                .agg(["mean", "std"]).round(3))
    mittel.columns = [f"{a}_{b}" for a, b in mittel.columns]
    df.to_csv(OUT / "baselines_folds.csv", index=False)
    mittel.to_csv(OUT / "baselines_mittel.csv")
    return df, mittel


def mehrheitsklasse(kl: pd.DataFrame) -> pd.DataFrame:
    """Vergleichswert der Klassifikation, je Fold.

    Accuracy faellt hoch aus (79 % der Stadtteil-Monate werden von Fehlalarm
    dominiert), Macro-F1 niedrig, weil drei der vier Klassen leer ausgehen.
    Genau deshalb ist Macro-F1 das massgebliche Guetemass.
    """
    OUT_K.mkdir(parents=True, exist_ok=True)
    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(kl, k)
        haeufigste = kl.loc[tr, ZIELKLASSE].value_counts().idxmax()
        y_true = kl.loc[te, ZIELKLASSE]
        y_pred = np.full(len(y_true), haeufigste)
        zeilen.append({
            "fold": k, "modell": f"Mehrheitsklasse ({haeufigste})",
            "Accuracy": round(float(accuracy_score(y_true, y_pred)), 3),
            "Macro-F1": round(float(f1_score(y_true, y_pred, average="macro",
                                             zero_division=0)), 3)})
    df = pd.DataFrame(zeilen)
    df.to_csv(OUT_K / "baselines_klasse.csv", index=False)
    return df


def run() -> tuple[pd.DataFrame, pd.DataFrame]:
    for pfad in (PFAD_REGRESSION, PFAD_KLASSIFIKATION):
        if not pfad.exists():
            raise SystemExit(f"{pfad.relative_to(ROOT)} fehlt - "
                             f"erst 'python prep/build.py' ausfuehren.")

    df, mittel = rechne_baselines(pd.read_parquet(PFAD_REGRESSION))
    print("Regression - Mittel +/- Std ueber die 5 Folds:\n", mittel.to_string())

    mk = mehrheitsklasse(pd.read_parquet(PFAD_KLASSIFIKATION))
    print(f"\nKlassifikation - Mehrheitsklasse: "
          f"Macro-F1 {mk['Macro-F1'].mean():.3f} +/- {mk['Macro-F1'].std():.3f} | "
          f"Accuracy {mk['Accuracy'].mean():.3f} +/- {mk['Accuracy'].std():.3f}")

    print(f"\n  => {OUT.relative_to(ROOT)}/baselines_*.csv")
    print(f"  => {OUT_K.relative_to(ROOT)}/baselines_klasse.csv")
    print("\n  Das Hold-out bleibt der Schlussbewertung vorbehalten.")
    return df, mittel


if __name__ == "__main__":
    run()
