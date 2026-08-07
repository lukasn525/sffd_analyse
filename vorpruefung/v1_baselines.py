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
           Regression:    Poisson-GLM mit Offset (Zaehldaten mit Exposition)
           Klassifikation: multinomiale logistische Regression (nominale Klassen)
           Beide: kanonischer Link, unpenalisierte Maximum-Likelihood, KEIN
           freier Hyperparameter - deshalb kein Tuning (Decision Log #45).
           Beantwortet: Wie weit kommt man mit der einfachen Form?

Stufe 3 sind die Vergleichsverfahren in modelle/. Ihre Aufgabe ist zu zeigen,
dass sie Stufe 2 schlagen - sonst hat sich der Mehraufwand nicht gelohnt.

Alle Baselines laufen ueber denselben STADTTEIL-SPLIT wie die Modelle: Der
Teststadtteil ist unbekannt. Das Hold-out bleibt unberuehrt.

WARUM UEBER ALLE 10 WIEDERHOLUNGEN (Ergaenzung 05.08.2026)
--------------------------------------------------------------------------
Bis dahin lief hier nur die Aufteilung, die als `fold`-Spalte in den Dateien
steht - also fuenf Laeufe. Die Vergleichsverfahren erzeugen 50. Die
Primaeraussage nach Decision Log #34 ist aber ein GEPAARTER Test „Verfahren
gegen Stufe 2", und der braucht je Lauf einen Gegenwert auf DENSELBEN
Testzeilen. Fuer 45 der 50 Laeufe gab es keinen.

Die Baseline ist damit kein Referenzwert, sondern ein Mitbewerber unter
identischem Protokoll - so verlangt es auch Schroeters Auflage C („fuer alle
Vergleichsmodelle identische Merkmale und Splits"). Der Aufwand faellt nicht
ins Gewicht: 50 GLM-Anpassungen kosten zusammen wenige Sekunden.

Eingang:  data/processed/{regression,klassifikation}.parquet
Ausgang:  results/regression/baselines_{folds,mittel}.csv
          results/klassifikation/baselines_klasse.csv

Ausfuehren:
  python vorpruefung/v1_baselines.py
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
    """RMSE, MAE, R2 - immer auf der ORIGINALSKALA der Zielgroesse."""
    from sklearn.metrics import (mean_absolute_error, mean_squared_error,
                                 r2_score)
    y_true, y_pred = np.asarray(y_true, float), np.asarray(y_pred, float)
    return {"RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "MAE":  float(mean_absolute_error(y_true, y_pred)),
            "R2":   float(r2_score(y_true, y_pred))}


# ---------------------------------------------------------------------------
def poisson_glm(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Stufe 2 der Regression: vorhergesagte Einsatzzahlen.

    Poisson-GLM mit kanonischem log-Link, per unpenalisierter
    Maximum-Likelihood angepasst. `log(Bevoelkerung)` geht als OFFSET ein, also
    mit fest auf 1 gesetztem Koeffizienten: Das Modell schaetzt Einsaetze JE
    EINWOHNER und multipliziert am Ende hoch - sonst sagt es vor allem die
    Stadtteilgroesse vorher (#13). Kein freier Hyperparameter, also kein Tuning.

    WARUM POISSON UND NICHT NEGATIVE BINOMIAL (Decision Log #45). Die Zaehldaten
    sind ueberdispers (Dispersionsindex 62,8), die Poisson-Varianzannahme
    Var = mu ist also verletzt. Das ist folgenlos fuer den Zweck dieser
    Baseline: Der Poisson-Schaetzer bleibt konsistent, solange der BEDINGTE
    MITTELWERT richtig spezifiziert ist, unabhaengig von der Varianzstruktur
    (Gourieroux, Monfort & Trognon 1984, "Pseudo Maximum Likelihood Methods",
    Econometrica 52, 701-720). Was die Ueberdispersion beschaedigt, sind die
    STANDARDFEHLER - und die werden hier nicht verwendet, weil die Baseline
    ausschliesslich Punktvorhersagen liefert. Keine Koeffiziententests, keine
    Konfidenzintervalle.

    Die Negative Binomial waere die Erweiterung fuer korrekte Inferenz. Sie
    loest ein Problem, das wir nicht haben, und ist damit nicht mehr "die
    einfachste Form, die zur Datenform passt".

    Die Rate entsteht aus DERSELBEN Anpassung, geteilt durch die Bevoelkerung -
    ein zweites Modell waere eine zweite Spezifikation.
    """
    import statsmodels.api as sm

    X_tr = sm.add_constant(train[MERKMALE].astype(float), has_constant="add")
    X_te = sm.add_constant(test[MERKMALE].astype(float),  has_constant="add")
    y_tr = train[ZIELGROESSE].astype(float)
    off_tr = np.log(train[EXPOSURE_ROH].astype(float))
    off_te = np.log(test[EXPOSURE_ROH].astype(float))

    modell = sm.GLM(y_tr, X_tr, family=sm.families.Poisson(),
                    offset=off_tr).fit()
    return np.asarray(modell.predict(X_te, offset=off_te))


def regression(panel: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """Beide Mengen-Zielgroessen, Stufe 1 und 2, je Wiederholung und Fold.

    Die Rate ergibt sich aus derselben Poisson-Vorhersage geteilt durch die
    Bevoelkerung - ein zweites Modell waere eine zweite Spezifikation und damit
    unfair gegenueber den Vergleichsverfahren.

    50 Laeufe (10 Wiederholungen x 5 Folds) x 2 Zielgroessen x 2 Modelle
    (Nullmarke, Poisson-GLM) = 200 Zeilen.
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
    """Zweistufige Aggregation ueber alle Laeufe des Durchgangs.

    ZWEISTUFIG heisst: erst je Wiederholung ueber die 5 Folds mitteln, dann die
    Streuung DIESER Werte berichten.

      `std_folds`            ueber alle 50 Einzellaeufe. Zu optimistisch, weil
                             die Laeufe nicht unabhaengig sind - es sind
                             dieselben 29 Stadtteile in zehn Gruppierungen.
      `std_wiederholungen`   ueber die 10 Wiederholungsmittel. MASSGEBLICH
                             (docs/06_RISIKEN.md, R-5).

    Eine Datei beschreibt genau einen Durchgang. Frueher fuehrte sie zusaetzlich
    Zeilen fuer Wiederholung 0 allein - das war historischer Ballast und vor
    allem eine Falle: Wer den Filter vergisst, bekommt stillschweigend die
    falsche Baseline. Wer diese Werte braucht, filtert `baselines_folds.csv`
    auf `wiederholung == 0`; dort steht jeder Einzellauf.
    """
    g = df.groupby(schluessel, sort=False)
    z = g[masse].mean().add_suffix("_mean")
    z = z.join(g[masse].std().add_suffix("_std_folds"))
    je_wdh = df.groupby(schluessel + ["wiederholung"], sort=False)[masse].mean()
    z = z.join(je_wdh.groupby(schluessel, sort=False).std()
                     .add_suffix("_std_wiederholungen"))
    spalten = schluessel + [f"{m}{s}" for m in masse for s in
                            ("_mean", "_std_folds", "_std_wiederholungen")]
    return z.reset_index()[spalten].round(3)


# ---------------------------------------------------------------------------
def klassifikation(kl: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """Beide Stufen der Klassifikation, je Wiederholung und Fold.

    STUFE 1, Mehrheitsklasse: sagt immer die im Training haeufigste Einsatzart
    vorher. Accuracy faellt hoch aus, Macro-F1 niedrig - genau deshalb ist
    Macro-F1 das massgebliche Guetemass.

    STUFE 2, multinomiale logistische Regression: das Gegenstueck zur Negative
    Binomial. Sie ist die einfachste Form, die zu einer nominalen Zielgroesse
    passt - linear in den Log-Odds, unpenalisiert. RF und XGBoost muessen SIE
    schlagen, nicht die Mehrheitsklasse (Decision Log #33).

    Zwei Ergaenzungen vom 05.08.2026, beide additiv:
      - Schleife ueber die 10 Wiederholungen, damit m03 gepaart testen kann
      - Macro-AUROC wird fuer Stufe 2 mitgerechnet. Ohne sie gaebe es fuer das
        zweite Guetemass der Klassifikation keine Messlatte. Fuer die
        Mehrheitsklasse ist sie nicht definiert (eine konstante Vorhersage hat
        keine Rangfolge) und bleibt leer - NICHT 0,5, das waere eine erfundene
        Zahl.

    Konvergenzwarnungen werden GEZAEHLT und zurueckgegeben, nicht unterdrueckt
    (docs/04_MODELLIERUNG.md, Sonderfaelle).
    """
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    OUT_K.mkdir(parents=True, exist_ok=True)
    klassen_alle = sorted(kl[ZIELKLASSE].unique())
    zeilen, nicht_konvergiert = [], 0

    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(kl, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            X_tr, X_te = d.loc[tr, MERKMALE].astype(float), d.loc[te, MERKMALE].astype(float)
            y_tr, y_te = d.loc[tr, ZIELKLASSE], d.loc[te, ZIELKLASSE]

            haeufigste = y_tr.value_counts().idxmax()
            with warnings.catch_warnings(record=True) as gefangen:
                warnings.simplefilter("always", ConvergenceWarning)
                logreg = make_pipeline(
                    StandardScaler(),
                    LogisticRegression(max_iter=2000, penalty=None,
                                       class_weight="balanced")).fit(X_tr, y_tr)
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
    """Macro-AUROC (One-vs-Rest), oder NaN wenn im Testfold eine Klasse fehlt.

    NICHT durch 0,5 oder 0 ersetzen: Ein erfundener Wert zoege den Mittelwert
    nach unten und saehe wie ein Messergebnis aus (docs/04_MODELLIERUNG.md,
    Sonderfaelle). Durch die doppelte Stratifizierung (#30) sollte der Fall
    nicht eintreten - wenn doch, muss er sichtbar bleiben.
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
