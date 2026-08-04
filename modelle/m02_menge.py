"""
Verfahrensvergleich fuer die MENGE der Einsatzlast.

Zwei Zielgroessen (`anzahl_einsaetze`, `einsaetze_je_1000_ew`) x drei Verfahren
(Ridge, Random Forest, XGBoost) x 10 Wiederholungen x 5 Folds = 300 Laeufe.

    python modelle/m02_menge.py            Tuning, Bewertung, Aggregation, Vergleich
    python modelle/m02_menge.py holdout    zusaetzlich die einmalige Schlussbewertung

Ausgang: results/regression/menge_folds.csv · menge_mittel.csv
                            tuning.csv · vergleich.csv · holdout.csv

Spezifikation: docs/04_MODELLIERUNG.md. Die dort genannten Fallstricke sind hier
im Code markiert - wer eine der vier Stellen aendert, sollte den Abschnitt lesen.

STAND: Bausteine fertig, Orchestrierung offen (siehe TODO).
"""
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (N_FOLDS, PFAD_REGRESSION, PRAEDIKTOREN,  # noqa: E402
                    RESULTS_DIR, ROOT, SAISON)
from config_modelle import (RANDOM_STATE, SUCHRAEUME,  # noqa: E402
                            TUNING_BUDGET, WIEDERHOLUNGEN)
from s2_datensaetze import RATE, ZIELGROESSE, fold_masken  # noqa: E402

OUT = RESULTS_DIR / "regression"
MERKMALE = PRAEDIKTOREN + SAISON
ZIELE = (ZIELGROESSE, RATE)
VERFAHREN = ("ridge", "random_forest", "xgboost")


# ---------------------------------------------------------------------------
# BAUSTEIN 1  Die Pipeline
# ---------------------------------------------------------------------------
def verfahren(name: str):
    """Baut die ungetunte Pipeline fuer ein Verfahren.

    FALLSTRICK: Ridge braucht zweierlei, und beides gehoert IN die Pipeline,
    nicht davor. Der StandardScaler, weil der L2-Strafterm alle Koeffizienten
    gleich behandelt und Merkmale in verschiedenen Einheiten sonst
    unterschiedlich hart bestraft wuerden. Und die log-Transformation der
    ZIELGROESSE ueber TransformedTargetRegressor - der rechnet nach der
    Vorhersage automatisch mit expm1 zurueck, sodass die Guetemasse auf der
    Originalskala entstehen. Wer log(1+y) von Hand rechnet, vergisst die
    Ruecktransformation irgendwann.

    Random Forest und XGBoost bekommen nichts davon: Sie sind gegen Skalen
    unempfindlich, und eine transformierte Zielgroesse wuerde die Guetemasse
    zwischen den Verfahren unvergleichbar machen.
    """
    from sklearn.compose import TransformedTargetRegressor
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    if name == "ridge":
        return make_pipeline(
            StandardScaler(),
            TransformedTargetRegressor(regressor=Ridge(),
                                       func=np.log1p, inverse_func=np.expm1))
    if name == "random_forest":
        return RandomForestRegressor(random_state=RANDOM_STATE, n_jobs=-1)
    if name == "xgboost":
        from xgboost import XGBRegressor
        return XGBRegressor(random_state=RANDOM_STATE, n_jobs=-1,
                            objective="reg:squarederror")
    raise ValueError(f"Unbekanntes Verfahren: {name}")


def suchraum(name: str) -> dict:
    """Uebersetzt SUCHRAEUME aus der Config in scipy-Verteilungen.

    Die Config haelt die Raeume bewusst als einfache Tupel ("loguniform", a, b),
    damit sie ohne scipy lesbar bleibt. Hier werden daraus die Objekte, die
    RandomizedSearchCV erwartet.

    Der Praefix haengt am Pipeline-Aufbau: Bei Ridge liegt der Schaetzer zwei
    Ebenen tief (Pipeline -> TransformedTargetRegressor -> Ridge), bei den
    Baumverfahren direkt.
    """
    from scipy.stats import loguniform, randint, uniform

    praefix = ("transformedtargetregressor__regressor__"
               if name == "ridge" else "")
    raum = {}
    for parameter, spez in SUCHRAEUME[name].items():
        art, *werte = spez
        if art == "loguniform":
            verteilung = loguniform(werte[0], werte[1])
        elif art == "int":
            verteilung = randint(werte[0], werte[1] + 1)
        elif art == "uniform":
            verteilung = uniform(werte[0], werte[1] - werte[0])
        elif art == "choice":
            verteilung = werte[0]
        else:
            raise ValueError(f"Unbekannte Suchraum-Art: {art}")
        raum[praefix + parameter] = verteilung
    return raum


# ---------------------------------------------------------------------------
# BAUSTEIN 2  Das Tuning
# ---------------------------------------------------------------------------
def tune(name: str, train: pd.DataFrame, ziel: str) -> dict:
    """Sucht die besten Hyperparameter auf den Trainingsstadtteilen eines Folds.

    FALLSTRICK, der die ganze Arbeit entwerten kann: Der innere CV MUSS nach
    Stadtteil gruppieren. RandomizedSearchCV nimmt voreingestellt KFold und
    schneidet zufaellig nach Zeilen - ein Stadtteil hat aber 132 Zeilen, von
    denen dann etwa 100 im inneren Training und 32 in der inneren Validierung
    laegen. Da die Strukturmerkmale innerhalb eines Jahres konstant sind, waeren
    das faktisch dieselben Zeilen: Die Hyperparameter wuerden auf einen
    geleakten Schaetzwert optimiert, und der Vorteil des aeusseren
    Stadtteil-Splits waere verspielt. Man sieht es den Zahlen nicht an - sie
    waeren nur zu gut.

    Rueckgabe sind die PARAMETER, nicht das Modell. Wer `best_estimator_`
    weiterverwendet, hat auf dem inneren Trainingsanteil trainiert statt auf
    allen Trainingsstadtteilen des Folds - und ein Viertel der Daten verschenkt.
    """
    from sklearn.model_selection import GroupKFold, RandomizedSearchCV

    suche = RandomizedSearchCV(
        estimator=verfahren(name),
        param_distributions=suchraum(name),
        n_iter=TUNING_BUDGET,
        cv=GroupKFold(n_splits=4),
        scoring="neg_root_mean_squared_error",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    suche.fit(train[MERKMALE].astype(float), train[ziel].astype(float),
              groups=train["stadtteil"])
    return suche.best_params_


# ---------------------------------------------------------------------------
# BAUSTEIN 3  Ein einzelner Lauf
# ---------------------------------------------------------------------------
def ein_lauf(name: str, parameter: dict, train: pd.DataFrame,
             test: pd.DataFrame, ziel: str) -> dict:
    """Ein Fit, eine Vorhersage, mit Zeitmessung - eine Zeile fuer die CSV.

    FALLSTRICK: Die Zeit wird UM `fit` und `predict` herum gemessen, nicht um
    die ganze Funktion. Sonst steckt die Metrikberechnung mit in der Zahl, und
    Unterfrage 3 misst etwas anderes, als sie behauptet.
    """
    from sklearn.metrics import (mean_absolute_error, mean_squared_error,
                                 r2_score)

    X_tr, X_te = train[MERKMALE].astype(float), test[MERKMALE].astype(float)
    y_tr, y_te = train[ziel].astype(float), test[ziel].astype(float)

    modell = verfahren(name).set_params(**parameter)

    t = time.perf_counter()
    modell.fit(X_tr, y_tr)
    train_sek = time.perf_counter() - t

    t = time.perf_counter()
    y_hat = modell.predict(X_te)
    inferenz_sek = time.perf_counter() - t

    return {
        "verfahren": name, "zielgroesse": ziel,
        "RMSE": float(np.sqrt(mean_squared_error(y_te, y_hat))),
        "MAE": float(mean_absolute_error(y_te, y_hat)),
        "R2": float(r2_score(y_te, y_hat)),
        "train_sekunden": train_sek, "inferenz_sekunden": inferenz_sek,
        "n_train": len(train), "n_test": len(test),
        "extrapolationsanteil": extrapolationsanteil(train, test),
    }


def extrapolationsanteil(train: pd.DataFrame, test: pd.DataFrame) -> float:
    """Anteil der Testzeilen, die in mindestens einem Merkmal ausserhalb des
    Trainings-Wertebereichs liegen.

    Erklaert spaeter, warum ein Fold aus der Reihe faellt. Erfasst bewusst nur
    die Spanne je Merkmal, nicht unbekannte KOMBINATIONEN - das echte
    Extrapolationsproblem ist also eher groesser (docs/06_RISIKEN.md, R-3).
    """
    lo, hi = train[MERKMALE].min(), train[MERKMALE].max()
    aussen = ((test[MERKMALE] < lo) | (test[MERKMALE] > hi)).any(axis=1)
    return float(aussen.mean())


# ---------------------------------------------------------------------------
# ORCHESTRIERUNG  - noch zu schreiben
# ---------------------------------------------------------------------------
def phase_tuning(panel: pd.DataFrame) -> pd.DataFrame:
    """TODO: Fuer jede Zielgroesse, jedes Verfahren, jeden Fold auf
    Wiederholung 0 `tune()` aufrufen. 2 x 3 x 5 = 30 Zeilen -> tuning.csv.
    Die Parameter gelten danach fuer alle 10 Wiederholungen (#34).
    """
    raise NotImplementedError


def phase_bewertung(panel: pd.DataFrame, parameter: pd.DataFrame) -> pd.DataFrame:
    """TODO: versatz 0..9 -> `ergaenze_aufteilung(panel, versatz=versatz)`,
    darin fold 1..5 -> `fold_masken()`, darin je Verfahren und Zielgroesse
    `ein_lauf()`. 300 Zeilen -> menge_folds.csv.
    """
    raise NotImplementedError


def aggregiere(folds: pd.DataFrame) -> pd.DataFrame:
    """TODO: ZWEISTUFIG mitteln - erst je Wiederholung ueber die 5 Folds, dann
    ueber die 10 Wiederholungen. Ausgeben: mittelwert, std_folds (ueber alle
    50, optimistisch) und std_wiederholungen (ueber die 10, MASSGEBLICH).
    Grund: Die 50 Laeufe sind nicht unabhaengig (R-5).
    """
    raise NotImplementedError


def vergleiche(folds: pd.DataFrame, baselines: pd.DataFrame) -> pd.DataFrame:
    """TODO: Gepaarter Wilcoxon-Test.
    rolle="primaer"   je Verfahren gegen die Stufe-2-Baseline (6 Tests)
    rolle="sekundaer" je Verfahrenspaar (3 Paare x 2 Zielgroessen = 6 Tests)

    Innerhalb der SEKUNDAEREN Familie HOLM-BONFERRONI anwenden: p-Werte
    aufsteigend sortieren, den kleinsten gegen alpha/m pruefen, den naechsten
    gegen alpha/(m-1), bis zur ersten Nichtablehnung. Gleiche Fehlerkontrolle
    wie Bonferroni, aber uniform staerker. Die primaeren Tests bilden keine
    Familie - jede Frage ist vorab einzeln formuliert (#34).

    Spalten: zielgroesse, paarung, rolle, differenz_mittel, gewonnene_folds,
    wilcoxon_p, p_holm, n_tests_familie.
    """
    raise NotImplementedError


def hold_out(panel: pd.DataFrame, parameter: pd.DataFrame) -> pd.DataFrame:
    """TODO: EINMALIG. Auf allen 29 Entwicklungsstadtteilen trainieren, auf den
    6 Hold-out-Stadtteilen bewerten. Nur bei Aufruf mit Argument "holdout".
    Im Bericht als EINZELMESSUNG kennzeichnen, nicht als Mittelwert (R-4).
    """
    raise NotImplementedError


def main(argv: list[str]) -> int:
    if not PFAD_REGRESSION.exists():
        raise SystemExit(f"{PFAD_REGRESSION.relative_to(ROOT)} fehlt - "
                         f"erst 'python prep/build.py' ausfuehren.")
    OUT.mkdir(parents=True, exist_ok=True)
    panel = pd.read_parquet(PFAD_REGRESSION)

    parameter = phase_tuning(panel)
    folds = phase_bewertung(panel, parameter)
    aggregiere(folds)
    vergleiche(folds, pd.read_csv(OUT / "baselines_folds.csv"))

    if "holdout" in argv:
        hold_out(panel, parameter)
    else:
        print("\n  Hold-out unberuehrt. Fuer die Schlussbewertung:"
              "\n  python modelle/m02_menge.py holdout")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
