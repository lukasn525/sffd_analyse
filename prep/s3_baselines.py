"""
Schritt 3: die Vergleichsgroessen, an denen sich jedes Modell messen lassen muss.

Eingang:  data/processed/regression.parquet · klassifikation.parquet
Ausgang:  results/regression/baselines_*.csv
          results/klassifikation/baselines_*.csv  (Anteile und Mehrheitsklasse)

Die Baselines gehoeren in die Data Preparation (Auflage Schroeter, 27.07.2026).
Sie sind kein Genehmigungspunkt, sondern ein Beweismittel: Sie sollen belegen,
dass die komplexeren Verfahren ein besseres Ergebnis liefern als eine einfache
Regel. Genau deshalb muessen sie fair sein - eine durch ihre Funktionsform
benachteiligte Referenz macht den Beleg wertlos.

Alle Baselines laufen ueber denselben STADTTEIL-SPLIT wie die Modelle: Der
Teststadtteil ist unbekannt. Das schliesst die frueher verwendete naive
Vormonats-Baseline aus - sie wuerde die eigene Vergangenheit des Teststadtteils
nutzen und damit genau die Frage umgehen, um die es geht (Decision Log #29).

  Gesamtmittelwert   Mittelwert der Trainingsstadtteile. Die ehrlichste
                     Referenz fuer "ich weiss nichts ueber diesen Stadtteil".
                     Nichtparametrisch, unterstellt keine Funktionsform.
  Saisonal           Mittelwert desselben Kalendermonats ueber die
                     Trainingsstadtteile. Trennt das Jahresmuster vom
                     Strukturbeitrag. Ebenfalls nichtparametrisch.
  Negative Binomial  Interpretierbare Count-Baseline; Poisson scheidet aus
                     (Dispersionsindex 62,8). log(Bevoelkerung) geht als OFFSET
                     ein - geschaetzt werden Einsaetze JE EINWOHNER statt der
                     Stadtteilgroesse (Decision Log #13). alpha kommt aus einem
                     Poisson-Vormodell (Momentenschaetzer auf den
                     Pearson-Residuen).

Das Hold-out (`ist_holdout == 1`) wird bewusst NICHT ausgewertet - es bleibt der
einmaligen Schlussbewertung vorbehalten.

Ausfuehren:
  python prep/s3_baselines.py
"""
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from config import (ANTEILE, EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION,
                    PFAD_REGRESSION, PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from s2_datensaetze import RATE, ZIELGROESSE, ZIELKLASSE, fold_masken

OUT   = RESULTS_DIR / "regression"
OUT_K = RESULTS_DIR / "klassifikation"


def bewerte_regression(y_true, y_pred) -> dict:
    """RMSE, MAE, R2 - immer auf der ORIGINALSKALA der Zielgroesse.

    Modelle auf log(1+y) (Ridge) muessen vorher per expm1 zuruecktransformieren.
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return {"RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "MAE":  float(mean_absolute_error(y_true, y_pred)),
            "R2":   float(r2_score(y_true, y_pred))}


def _mittel(df: pd.DataFrame) -> pd.DataFrame:
    m = (df.groupby(["zielgroesse", "modell"])[["RMSE", "MAE", "R2"]]
           .agg(["mean", "std"]).round(3))
    m.columns = [f"{a}_{b}" for a, b in m.columns]
    return m.sort_values(["zielgroesse", "RMSE_mean"])


def negative_binomial(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
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
    """Vergleichsgroessen der Menge, je Fold und Zielgroesse."""
    OUT.mkdir(parents=True, exist_ok=True)
    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(panel, k)
        train, test = panel[tr], panel[te]
        for ziel in (ZIELGROESSE, RATE):
            y = test[ziel].to_numpy()
            saison = train.groupby("monat")[ziel].mean()
            vorhersagen = [
                ("Gesamtmittelwert", np.full(len(test), train[ziel].mean())),
                ("Saisonaler Durchschnitt", test["monat"].map(saison).to_numpy()),
            ]
            if ziel == ZIELGROESSE:
                vorhersagen.append(("Negative Binomial",
                                    negative_binomial(train, test)))
            for name, y_hat in vorhersagen:
                zeilen.append({"fold": k, "zielgroesse": ziel, "modell": name,
                               **bewerte_regression(y, y_hat)})

    df = pd.DataFrame(zeilen)
    mittel = _mittel(df)
    df.to_csv(OUT / "baselines_folds.csv", index=False)
    mittel.to_csv(OUT / "baselines_mittel.csv")
    return df, mittel


def strukturbaselines(kl: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Vergleichsgroessen der Zusammensetzung, je Fold und Anteil.

    Der Gesamtmittelwert ist hier die entscheidende Referenz: Er sagt fuer jeden
    unbekannten Stadtteil denselben Anteil vorher. Schlaegt ein Verfahren ihn
    nicht, erklaeren die Strukturmerkmale die Zusammensetzung nicht.
    """
    OUT_K.mkdir(parents=True, exist_ok=True)
    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(kl, k)
        train, test = kl[tr], kl[te]
        for ziel in ANTEILE:
            y = test[ziel].to_numpy()
            saison = train.groupby("monat")[ziel].mean()
            for name, y_hat in [
                ("Gesamtmittelwert", np.full(len(test), train[ziel].mean())),
                ("Saisonaler Durchschnitt", test["monat"].map(saison).to_numpy()),
            ]:
                zeilen.append({"fold": k, "zielgroesse": ziel, "modell": name,
                               **bewerte_regression(y, y_hat)})

    df = pd.DataFrame(zeilen)
    mittel = _mittel(df)
    df.to_csv(OUT_K / "baselines_folds.csv", index=False)
    mittel.to_csv(OUT_K / "baselines_mittel.csv")
    return df, mittel


def mehrheitsklasse(kl: pd.DataFrame) -> pd.DataFrame:
    """Mehrheitsklassen-Baseline der Klassifikation (Gutachten R6).

    Sagt in jedem Fold immer die im TRAINING haeufigste Einsatzart vorher.
    Accuracy faellt dadurch hoch aus (79 % der Stadtteil-Monate werden von
    Fehlalarm/Good Intent dominiert), Macro-F1 dagegen niedrig, weil drei der
    vier Klassen leer ausgehen. Genau deshalb ist Macro-F1 das massgebliche
    Guetemass: Accuracy waere hier wertlos.
    """
    from sklearn.metrics import accuracy_score, f1_score

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
    print("Menge - Mittel +/- Std ueber die Folds:\n", mittel.to_string())

    kl = pd.read_parquet(PFAD_KLASSIFIKATION)
    dfk, mittelk = strukturbaselines(kl)
    print("\nStruktur (Anteile) - Mittel +/- Std ueber die Folds:\n",
          mittelk.to_string())

    mk = mehrheitsklasse(kl)
    print(f"\nKlassifikation - Mehrheitsklasse: "
          f"Accuracy {mk['Accuracy'].mean():.3f} +/- {mk['Accuracy'].std():.3f} | "
          f"Macro-F1 {mk['Macro-F1'].mean():.3f} +/- {mk['Macro-F1'].std():.3f}")

    print(f"\n  => {OUT.relative_to(ROOT)}/baselines_*.csv")
    print(f"  => {OUT_K.relative_to(ROOT)}/baselines_*.csv")
    print("\n  Das Hold-out bleibt der Schlussbewertung vorbehalten.")
    return df, mittel


if __name__ == "__main__":
    run()
