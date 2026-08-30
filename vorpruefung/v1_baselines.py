"""
Stufe 1 und 2: die Messlatte.

    python vorpruefung/v1_baselines.py

Eingang: data/processed/{regression,klassifikation}.parquet
Ausgang: results/regression/baselines_{folds,mittel}.csv
         results/klassifikation/baselines_klasse.csv

  - STUFE 1, triviale Referenz ohne ein einziges Merkmal: Gesamtmittelwert
    der Trainingsstadtteile (Regression) bzw. immer die haeufigste Klasse
    (Klassifikation). Beantwortet, ob in den Merkmalen ueberhaupt
    Information steckt
  - STUFE 2, einfachste Form, die zur DATENFORM passt: Poisson-GLM mit
    Offset fuer Zaehldaten mit Exposition, multinomiales Logit fuer nominale
    Klassen. Beide mit kanonischem Link, unpenalisiert und ohne freien
    Hyperparameter - deshalb ohne Tuning (#45)
  - Die Vergleichsverfahren in modelle/ muessen STUFE 2 schlagen, nicht die
    triviale Referenz (#33)
  - Gerechnet wird ueber alle 10 Wiederholungen x 5 Folds. Der gepaarte Test
    (#34) braucht je Lauf einen Gegenwert auf DENSELBEN Testzeilen; die
    Baseline ist damit Mitbewerber unter identischem Protokoll (Auflage C),
    nicht bloss ein Referenzwert
  - Derselbe Stadtteil-Split wie die Modelle, das Hold-out bleibt unberuehrt

Ausfuehrliche Fassung: docs/08_FUNKTIONSDOKUMENTATION.md
"""
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "prep"))
sys.path.insert(0, str(_ROOT / "modelle"))   # nur config_modelle.WIEDERHOLUNGEN
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from config_modelle import WIEDERHOLUNGEN  # noqa: E402
from s2_datensaetze import (RATE, ZIELGROESSE, ZIELKLASSE,  # noqa: E402
                            fold_masken)
from v0_aufteilung import (selten_je_stadtteil,  # noqa: E402
                           wiederholte_aufteilung)

OUT   = RESULTS_DIR / "regression"
OUT_K = RESULTS_DIR / "klassifikation"
MERKMALE = PRAEDIKTOREN + SAISON

# Namen der Baseline-Modelle - einmal hier, damit m02/m03 sie ohne Tippfehler
# aus der CSV herausfiltern koennen.
POISSON        = "Poisson-GLM"
NULLMARKE      = "Gesamtmittelwert"
LOGREG         = "Multinomiale logistische Regression"


def bewerte_regression(y_true, y_pred) -> dict:
    """RMSE, MAE und R2 auf der Originalskala der Zielgroesse.

    Ein:  wahre und vorhergesagte Werte
    Aus:  dict mit rmse, mae, r2
    """
    from sklearn.metrics import (mean_absolute_error, mean_squared_error,
                                 r2_score)
    y_true, y_pred = np.asarray(y_true, float), np.asarray(y_pred, float)
    return {"RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "MAE":  float(mean_absolute_error(y_true, y_pred)),
            "R2":   float(r2_score(y_true, y_pred))}


# ---------------------------------------------------------------------------
def poisson_glm(train: pd.DataFrame, test: pd.DataFrame,
                merkmale: list[str] | None = None) -> np.ndarray:
    """Stufe 2 der Regression: Poisson-GLM mit Offset.

    Ein:  Trainings- und Testrahmen, optional ein reduzierter Merkmalssatz
    Aus:  Vorhersagen auf der Originalskala, eine Zahl je Stadtteil-Monat

    - kanonischer log-Link, unpenalisierte Maximum-Likelihood
    - log(Bevoelkerung) als OFFSET, Koeffizient fest auf 1: geschaetzt wird die
      Rate, hochgerechnet wird am Ende (#13)
    - kein freier Hyperparameter, deshalb kein Tuning
    - `merkmale` existiert allein fuer die Faktorgruppen-Ablation in m04_shap;
      der Offset bleibt in jeder Variante bestehen, er ist keine Merkmalsspalte
    - Poisson statt Negative Binomial (#45): Die Ueberdispersion (Index 62,8)
      beschaedigt die Standardfehler, nicht die Konsistenz des bedingten
      Mittelwerts (Gourieroux, Monfort & Trognon 1984). Diese Baseline liefert
      nur Punktvorhersagen und ist davon nicht betroffen
    - die Rate stammt aus derselben Anpassung, geteilt durch die Bevoelkerung
    """
    import statsmodels.api as sm

    spalten = MERKMALE if merkmale is None else list(merkmale)
    X_tr = sm.add_constant(train[spalten].astype(float), has_constant="add")
    X_te = sm.add_constant(test[spalten].astype(float),  has_constant="add")
    y_tr = train[ZIELGROESSE].astype(float)

    # log(Bevoelkerung) als OFFSET, Koeffizient fest auf 1: geschaetzt wird
    # die Rate, hochgerechnet wird erst in der Vorhersage (#13).
    off_tr = np.log(train[EXPOSURE_ROH].astype(float))
    off_te = np.log(test[EXPOSURE_ROH].astype(float))

    # kanonischer log-Link, unpenalisiert - kein freier Hyperparameter
    modell = sm.GLM(y_tr, X_tr, family=sm.families.Poisson(),
                    offset=off_tr).fit()
    return np.asarray(modell.predict(X_te, offset=off_te))


def logit_glm(train: pd.DataFrame, merkmale: list[str] | None = None):
    """Stufe 2 der Klassifikation: multinomiales Logit.

    Ein:  Trainingsrahmen, optional ein reduzierter Merkmalssatz
    Aus:  das angepasste Modell, nicht die Vorhersage

    - linear in den Log-Odds, unpenalisiert (C = inf), kein Tuning (#45)
    - class_weight="balanced" statt Resampling: kein SMOTE, keine duplizierte
      oder geloeschte Zeile
    - Rueckgabe ist das Modell, weil beide Aufrufer aus derselben Anpassung
      Klassenvorhersage, Wahrscheinlichkeiten und Klassenreihenfolge brauchen
    - Konvergenzwarnungen werden nicht abgefangen; der Aufrufer zaehlt sie
    - einzige Stelle, an der dieses Modell spezifiziert ist (seit 10.08.2026).
      Zuvor baute m03_struktur.hold_out() es ein zweites Mal nach; eine Aenderung
      an einem der beiden Orte blieb unbemerkt
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    spalten = MERKMALE if merkmale is None else list(merkmale)

    # unpenalisiert (C = inf); ausgeglichene Klassengewichte statt Resampling:
    # keine duplizierte und keine geloeschte Zeile
    return make_pipeline(
        StandardScaler(),
        LogisticRegression(max_iter=2000, C=np.inf, class_weight="balanced")
    ).fit(train[spalten].astype(float), train[ZIELKLASSE])


def regression(panel: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """Beide Mengen-Zielgroessen, Stufe 1 und 2, je Wiederholung und Fold.

    Ein:  regression.parquet, `selten` fuer die Stratifizierung
    Aus:  Datenrahmen mit 200 Zeilen (50 Laeufe x 2 Zielgroessen x 2 Modelle)

    - Stufe 1: Gesamtmittelwert der Trainingsstadtteile
    - Stufe 2: poisson_glm()
    - die Rate entsteht aus derselben Poisson-Vorhersage geteilt durch die
      Bevoelkerung; ein eigenes Ratenmodell waere eine zweite Spezifikation
    """
    OUT.mkdir(parents=True, exist_ok=True)
    zeilen = []
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(panel, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            bev = test[EXPOSURE_ROH].to_numpy()

            anzahl = poisson_glm(train, test)

            for ziel, referenz in ((ZIELGROESSE, anzahl),
                                   (RATE, anzahl / bev * 1000)):
                y = test[ziel].to_numpy()
                for stufe, name, y_hat in (
                        (1, NULLMARKE, np.full(len(test), train[ziel].mean())),
                        (2, POISSON, referenz)):
                    zeilen.append({"wiederholung": w, "fold": k, "stufe": stufe,
                                   "zielgroesse": ziel, "modell": name,
                                   **bewerte_regression(y, y_hat)})

    df = pd.DataFrame(zeilen)
    df.to_csv(OUT / "baselines_folds.csv", index=False)
    mittel = _zweistufig(df, ["zielgroesse", "stufe", "modell"],
                         ["RMSE", "MAE", "R2"])
    mittel.to_csv(OUT / "baselines_mittel.csv", index=False)
    return mittel


def _zweistufig(df: pd.DataFrame, schluessel: list[str],
                masse: list[str]) -> pd.DataFrame:
    """Zweistufige Aggregation ueber die Laeufe eines Durchgangs.

    Ein:  Datenrahmen der Einzellaeufe (Wiederholung x Fold)
    Aus:  je Modell und Zielgroesse eine Zeile mit Mittel und beiden Streuungen

    - Stufe 1: je Wiederholung ueber die 5 Folds mitteln
    - Stufe 2: Streuung dieser 10 Werte berichten -> `std_wiederholungen`
    - `std_folds` ueber alle 50 Laeufe ist zu optimistisch: dieselben 29
      Stadtteile in zehn Gruppierungen (R-5)
    - beide Spalten wandern mit, damit der Unterschied sichtbar bleibt
    - eine Datei beschreibt genau einen Durchgang; Einzellaeufe stehen in
      baselines_folds.csv
    """
    g = df.groupby(schluessel, sort=False)
    z = g[masse].mean().add_suffix("_mean")
    z = z.join(g[masse].std().add_suffix("_std_folds"))
    # Streuung der 10 Wiederholungsmittel, nicht der 50 Laeufe: dieselben 29
    # Stadtteile in zehn Gruppierungen waeren zu optimistisch (R-5).
    je_wdh = df.groupby(schluessel + ["wiederholung"], sort=False)[masse].mean()
    z = z.join(je_wdh.groupby(schluessel, sort=False).std()
                     .add_suffix("_std_wiederholungen"))
    spalten = schluessel + [f"{m}{s}" for m in masse for s in
                            ("_mean", "_std_folds", "_std_wiederholungen")]
    return z.reset_index()[spalten].round(3)


# ---------------------------------------------------------------------------
def klassifikation(kl: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """Beide Stufen der Klassifikation, je Wiederholung und Fold.

    Ein:  klassifikation.parquet, `selten` fuer die Stratifizierung
    Aus:  Datenrahmen der Einzellaeufe, Zahl der Konvergenzwarnungen

    - Stufe 1: haeufigste Klasse des Trainings. Accuracy hoch, Macro-F1 niedrig -
      deshalb ist Macro-F1 das massgebliche Guetemass
    - Stufe 2: logit_glm(). Anderes Modell als das Poisson-GLM des Mengenstrangs;
      RF und XGBoost muessen SIE schlagen (#33)
    - Macro-AUROC nur fuer Stufe 2. Fuer die Mehrheitsklasse nicht definiert -
      eine konstante Vorhersage hat keine Rangfolge - und bleibt leer statt 0,5
    - Konvergenzwarnungen werden gezaehlt und zurueckgegeben, nicht unterdrueckt
    """
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.metrics import accuracy_score, f1_score

    OUT_K.mkdir(parents=True, exist_ok=True)
    klassen_alle = sorted(kl[ZIELKLASSE].unique())
    zeilen, nicht_konvergiert = [], 0

    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(kl, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            X_te = d.loc[te, MERKMALE].astype(float)
            y_tr, y_te = d.loc[tr, ZIELKLASSE], d.loc[te, ZIELKLASSE]

            haeufigste = y_tr.value_counts().idxmax()
            with warnings.catch_warnings(record=True) as gefangen:
                warnings.simplefilter("always", ConvergenceWarning)
                logreg = logit_glm(d[tr])
            nicht_konvergiert += sum(issubclass(g.category, ConvergenceWarning)
                                     for g in gefangen)

            auroc = _macro_auroc(y_te, logreg.predict_proba(X_te),
                                 list(logreg.classes_), klassen_alle)

            for stufe, name, y_hat, au in (
                    (1, f"Mehrheitsklasse ({haeufigste})",
                     np.full(len(y_te), haeufigste), np.nan),
                    (2, LOGREG, logreg.predict(X_te), auroc)):
                zeilen.append({
                    "wiederholung": w, "fold": k, "stufe": stufe, "modell": name,
                    "Accuracy": round(float(accuracy_score(y_te, y_hat)), 3),
                    "Macro-F1": round(float(f1_score(y_te, y_hat, average="macro",
                                                     zero_division=0)), 3),
                    "Macro-AUROC": au if np.isnan(au) else round(au, 3)})

    df = pd.DataFrame(zeilen)
    df.to_csv(OUT_K / "baselines_klasse.csv", index=False)
    mittel = _zweistufig(df, ["stufe", "modell"],
                         ["Macro-F1", "Macro-AUROC", "Accuracy"])
    mittel.to_csv(OUT_K / "baselines_klasse_mittel.csv", index=False)
    df.attrs["nicht_konvergiert"] = nicht_konvergiert
    return df


def _macro_auroc(y_true, proba: np.ndarray, klassen_modell: list,
                 klassen_alle: list) -> float:
    """Macro-AUROC (One-vs-Rest), NaN wenn im Testfold eine Klasse fehlt.

    Ein:  wahre Klassen, Wahrscheinlichkeitsmatrix, Klassenreihenfolge des Modells
    Aus:  Zahl oder NaN

    - kein Ersatzwert 0,5 oder 0: ein erfundener Wert zieht den Mittelwert nach
      unten und sieht wie ein Messergebnis aus
    - durch die doppelte Stratifizierung (#30) sollte der Fall nicht eintreten
    """
    from sklearn.metrics import roc_auc_score

    if set(np.unique(y_true)) != set(klassen_alle):
        return float("nan")
    try:
        return float(roc_auc_score(y_true, proba, multi_class="ovr",
                                   average="macro", labels=klassen_modell))
    except ValueError:
        return float("nan")


# ---------------------------------------------------------------------------
def run() -> None:
    """Fuehrt beide Straenge aus und schreibt die drei Ergebnisdateien.

    Ein:  beide Parquet-Dateien
    Aus:  baselines_folds.csv, baselines_mittel.csv, baselines_klasse.csv

    - Schritt 1 von vorpruefung/run.py
    - das Stratifizierungsmass wird auch fuer die Regression aus der
      Klassifikation gelesen (siehe v0_aufteilung)
    - Konvergenzwarnungen werden am Ende zusammengefasst ausgegeben
    """
    for pfad in (PFAD_REGRESSION, PFAD_KLASSIFIKATION):
        if not pfad.exists():
            raise SystemExit(f"{pfad.relative_to(ROOT)} fehlt - "
                             f"erst 'python prep/build.py' ausfuehren.")

    r = pd.read_parquet(PFAD_REGRESSION)
    kl = pd.read_parquet(PFAD_KLASSIFIKATION)
    # Stratifizierungsmass aus der Klassifikation - siehe v0_aufteilung.
    selten = selten_je_stadtteil(kl)

    mittel = regression(r, selten)
    print(f"Regression - {WIEDERHOLUNGEN} Wiederholungen x {N_FOLDS} Folds")
    print(mittel.to_string(index=False))


    df = klassifikation(kl, selten)
    print(f"\nKlassifikation - Mittel ueber alle "
          f"{WIEDERHOLUNGEN * N_FOLDS} Laeufe:")
    for (stufe, modell), g in df.groupby(["stufe", "modell"]):
        print(f"  Stufe {stufe}  {modell:<32} "
              f"Macro-F1 {g['Macro-F1'].mean():.3f} | "
              f"Accuracy {g['Accuracy'].mean():.3f} | "
              f"Macro-AUROC {g['Macro-AUROC'].mean():.3f}")
    n = df.attrs.get("nicht_konvergiert", 0)
    print(f"  Konvergenzwarnungen der logistischen Regression: {n} von "
          f"{WIEDERHOLUNGEN * N_FOLDS} Laeufen")
    fehlend = int(df["Macro-AUROC"].isna().sum() - (df["stufe"] == 1).sum())
    if fehlend:
        print(f"  ACHTUNG: {fehlend} Lauf/Laeufe ohne definierte Macro-AUROC "
              f"(Klasse im Testfold nicht vertreten).")

    print(f"\n  => {OUT.relative_to(ROOT)}/baselines_*.csv")
    print(f"  => {OUT_K.relative_to(ROOT)}/baselines_klasse*.csv")
    print("\n  Das Hold-out bleibt der Schlussbewertung vorbehalten.")


if __name__ == "__main__":
    run()
