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

--------------------------------------------------------------------------
PRUEFAUFTRAEGE nach jedem Lauf
--------------------------------------------------------------------------
Nachgetragen am 05.08.2026 - dieser Block fehlte, obwohl CLAUDE.md ihn fuer
jedes Skript in modelle/ verlangt (docs/07_BEFUNDE.md, B-9). Abzuarbeiten nach
JEDEM Lauf, nicht nur beim ersten.

  1  Schlaegt jedes Verfahren die Stufe-2-Baseline - je Zielgroesse einzeln?
     Wenn nein, lautet das Ergebnis "der Mehraufwand lohnt sich hier nicht".
     Das ist ein Befund, kein Fehler (Gutachten R6).
  2  Ueberlappen sich die Streuungsbereiche zweier Verfahren? Dann ist "nicht
     unterscheidbar" zu berichten, keine Rangfolge (R-6, R-1).
  3  Wie oft liefert Ridge nach expm1 NEGATIVE Vorhersagen? Nicht kappen -
     die Haeufigkeit ist auszuweisen, sie ist ein Befund ueber das Verfahren.
  4  Passt die Zeilenzahl? 30 in tuning.csv, 300 in menge_folds.csv. Eine
     andere Zahl heisst, dass eine Schleife nicht durchgelaufen ist.
  5  Wurde das Hold-out beruehrt? Ohne Argument darf keine Zeile mit
     ist_holdout == 1 gelesen worden sein - main() filtert sie deshalb
     unwiderruflich heraus, bevor irgendetwas rechnet.
  6  Ist std_wiederholungen deutlich kleiner als std_folds? Erwartet ja. Waere
     es null, waeren die Wiederholungen Dubletten (B-3).
  7  Steht der Extrapolationsanteil je Fold noch bei rund 33,7 % im Mittel?
     Starke Abweichung heisst, dass die Aufteilung nicht die dokumentierte ist.
  8  Laufzeiten: Ridge laeuft einkernig, RF und XGBoost ueber alle Kerne. Die
     Zahlen sind an die Kernzahl gebunden und ohne sie nicht lesbar (B-10).

STAND: vollstaendig, 05.08.2026.
"""
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "prep"))
sys.path.insert(0, str(_ROOT / "vorpruefung"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from config_modelle import (RANDOM_STATE, SUCHRAEUME,  # noqa: E402
                            TUNING_BUDGET, WIEDERHOLUNGEN)
from s2_datensaetze import RATE, ZIELGROESSE, fold_masken  # noqa: E402
from v0_aufteilung import (entwicklung_und_holdout,  # noqa: E402
                           selten_je_stadtteil, wiederholte_aufteilung)

OUT = RESULTS_DIR / "regression"
MERKMALE = PRAEDIKTOREN + SAISON
ZIELE = (ZIELGROESSE, RATE)
VERFAHREN = ("ridge", "random_forest", "xgboost")

# Die Stufe-2-Baseline, gegen die die Primaeraussage laeuft (#34). Der Name
# muss zu vorpruefung/v1_baselines.NEGBIN passen.
BASELINE_STUFE2 = "Negative Binomial"

# Der gepaarte Test laeuft auf RMSE. Begruendung: Bei der Rate ist R2 kein
# tragfaehiges Mass (docs/03_STAND.md, Abschnitt 4) - der Mittelwert wird
# negativ, obwohl die Baseline in jedem Fold besser ist als die Nullmarke. Zwei
# verschiedene Testmetriken fuer zwei Zielgroessen waeren schwerer zu
# verteidigen als eine. MAE und R2 wandern als Spalten mit und werden
# nachrichtlich berichtet.
TESTMASS = "RMSE"
ALPHA = 0.05

# ==========================================================================
# PARALLELISIERUNG - eine Entscheidung mit zwei Gruenden
# ==========================================================================
# Die Modelle laufen EINKERNIG, parallelisiert wird nur die Hyperparametersuche.
#
# Grund 1, praktisch: `RandomizedSearchCV(n_jobs=-1)` um einen Schaetzer mit
# `n_jobs=-1` startet Prozesse ueber alle Kerne, von denen jeder seinerseits
# alle Kerne beansprucht. Die Prozesse blockieren sich gegenseitig; gemessen am
# 05.08.2026 auf zwei Kernen stand ein Probelauf mit Budget 2 nach 15 Minuten
# noch in Phase 1 (docs/07_BEFUNDE.md, B-16).
#
# Grund 2, inhaltlich und wichtiger: Unterfrage 3 fragt nach dem TRAININGS- UND
# INFERENZAUFWAND. Ridge ist einkernig, weil eine geschlossene Loesung nichts zu
# parallelisieren hat; RF und XGBoost skalieren ueber Kerne. Misst man sie in
# unterschiedlichen Betriebsarten, vergleicht man Rechenaufwand und
# Parallelisierungsgrad in einer Zahl - und die haengt dann an der Kernzahl der
# Maschine statt am Verfahren.
#
# Deshalb: Der berichtete Aufwand wird EINKERNIG gemessen, fuer alle Verfahren
# gleich. Der Parallelisierungsgewinn ist eine eigene, ebenfalls interessante
# Groesse und wird getrennt erhoben - in Wiederholung 0, wo 30 Messungen
# genuegen (siehe `ein_lauf(..., auch_parallel=True)`).
N_JOBS_MODELL = 1
N_JOBS_SUCHE = -1


# ---------------------------------------------------------------------------
# BAUSTEIN 1  Die Pipeline
# ---------------------------------------------------------------------------
def verfahren(name: str, n_jobs: int = N_JOBS_MODELL):
    """Baut die ungetunte Pipeline fuer ein Verfahren.

    `n_jobs` steuert nur die Parallelisierung, nicht das Ergebnis - siehe den
    Block PARALLELISIERUNG oben. Die Voreinstellung ist EINKERNIG, damit die
    gemessenen Laufzeiten zwischen den Verfahren vergleichbar bleiben.

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
        return RandomForestRegressor(random_state=RANDOM_STATE, n_jobs=n_jobs)
    if name == "xgboost":
        from xgboost import XGBRegressor
        return XGBRegressor(random_state=RANDOM_STATE, n_jobs=n_jobs,
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

    ZWEITER FALLSTRICK, seit 06.08.2026 behoben: Der Schaetzer laeuft hier
    EINKERNIG, parallelisiert wird allein die Suche. Zuvor stand `n_jobs=-1` an
    beiden Stellen, und die Prozesse haben sich gegenseitig blockiert (B-16).
    """
    from sklearn.model_selection import GroupKFold, RandomizedSearchCV

    suche = RandomizedSearchCV(
        estimator=verfahren(name, n_jobs=N_JOBS_MODELL),
        param_distributions=suchraum(name),
        n_iter=TUNING_BUDGET,
        cv=GroupKFold(n_splits=4),
        scoring="neg_root_mean_squared_error",
        random_state=RANDOM_STATE,
        n_jobs=N_JOBS_SUCHE,
    )
    suche.fit(train[MERKMALE].astype(float), train[ziel].astype(float),
              groups=train["stadtteil"])
    return suche.best_params_


# ---------------------------------------------------------------------------
# BAUSTEIN 3  Ein einzelner Lauf
# ---------------------------------------------------------------------------
def ein_lauf(name: str, parameter: dict, train: pd.DataFrame,
             test: pd.DataFrame, ziel: str,
             auch_parallel: bool = False) -> dict:
    """Ein Fit, eine Vorhersage, mit Zeitmessung - eine Zeile fuer die CSV.

    FALLSTRICK: Die Zeit wird UM `fit` und `predict` herum gemessen, nicht um
    die ganze Funktion. Sonst steckt die Metrikberechnung mit in der Zahl, und
    Unterfrage 3 misst etwas anderes, als sie behauptet.

    Gemessen wird EINKERNIG - fuer alle drei Verfahren gleich. Das ist der
    Aufwand, der in Unterfrage 3 berichtet wird, und er ist zwischen den
    Verfahren vergleichbar, weil keines einen Parallelisierungsvorteil
    mitbringt.

    Mit `auch_parallel=True` wird zusaetzlich ein zweiter Fit ueber alle Kerne
    gemessen. Die Differenz ist der Parallelisierungsgewinn - eine eigene
    Aussage fuer Unterfrage 4: Ridge hat als geschlossene Loesung nichts zu
    parallelisieren, die Ensembles skalieren. Wird nur in Wiederholung 0
    erhoben; fuer einen Faktor braucht es keine 50 Messungen.

    Ergaenzt am 05.08.2026 um `n_negativ` und `y_hat_min`: Ridge auf log(1+y)
    kann nach expm1 Werte unter null liefern. Die werden NICHT gekappt - das
    waere ein Eingriff -, aber ihre Haeufigkeit ist auszuweisen
    (docs/04_MODELLIERUNG.md, Sonderfaelle). Ohne diese zwei Felder muesste man
    dafuer jedes Modell ein zweites Mal fitten.
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

    train_par, inferenz_par = np.nan, np.nan
    if auch_parallel:
        parallel = verfahren(name, n_jobs=-1).set_params(**parameter)
        t = time.perf_counter()
        parallel.fit(X_tr, y_tr)
        train_par = time.perf_counter() - t
        t = time.perf_counter()
        y_par = parallel.predict(X_te)
        inferenz_par = time.perf_counter() - t
        # Die Kernzahl darf das ERGEBNIS nicht veraendern, nur die Dauer.
        # Wuerde sie es doch, waere der Vergleich nicht mehr fair.
        assert np.allclose(y_hat, y_par, rtol=1e-6, atol=1e-6), (
            f"{name}: Vorhersagen haengen von der Kernzahl ab "
            f"(max. Abweichung {np.max(np.abs(y_hat - y_par)):.3e}).")

    return {
        "train_sekunden_parallel": train_par,
        "inferenz_sekunden_parallel": inferenz_par,
        "verfahren": name, "zielgroesse": ziel,
        "RMSE": float(np.sqrt(mean_squared_error(y_te, y_hat))),
        "MAE": float(mean_absolute_error(y_te, y_hat)),
        "R2": float(r2_score(y_te, y_hat)),
        "train_sekunden": train_sek, "inferenz_sekunden": inferenz_sek,
        "n_train": len(train), "n_test": len(test),
        "extrapolationsanteil": extrapolationsanteil(train, test),
        "n_negativ": int((y_hat < 0).sum()),
        "y_hat_min": float(np.min(y_hat)),
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
# ORCHESTRIERUNG
# ---------------------------------------------------------------------------
def phase_tuning(panel: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """Je Zielgroesse, Verfahren und Fold einmal `tune()` - 30 Zeilen.

    Getunt wird ausschliesslich auf Wiederholung 0; die gefundenen Parameter
    gelten fuer alle zehn Wiederholungen (#34). Das ist eine bewusste
    Vereinfachung: Die Wiederholungen unterscheiden sich nur in der
    Fold-Zuteilung und dienen der Streuungsschaetzung, nicht der Modellwahl.
    Sie ist im Text zu benennen.

    Die Parameter landen sowohl als einzelne Spalten (lesbar fuer Kapitel 6.3)
    als auch als JSON (verlustfrei fuer den Wiedereinlesen-Weg).
    """
    d = wiederholte_aufteilung(panel, wiederholung=0, selten=selten)
    zeilen = []
    for ziel in ZIELE:
        for name in VERFAHREN:
            for k in range(1, N_FOLDS + 1):
                tr, _ = fold_masken(d, k)
                t = time.perf_counter()
                p = _rein_python(tune(name, d[tr], ziel))
                dauer = time.perf_counter() - t
                kurz = {schluessel.split("__")[-1]: wert
                        for schluessel, wert in p.items()}
                zeilen.append({"zielgroesse": ziel, "verfahren": name, "fold": k,
                               "tuning_sekunden": round(dauer, 2),
                               **kurz, "parameter_json": json.dumps(p)})
                print(f"    tune  {ziel:<21} {name:<14} Fold {k}  "
                      f"{dauer:6.1f}s  {kurz}")
    df = pd.DataFrame(zeilen)
    df.to_csv(OUT / "tuning.csv", index=False)
    return df


def _rein_python(p: dict) -> dict:
    """NumPy-Skalare in native Typen wandeln, BEVOR sie nach JSON gehen.

    Warum das noetig ist: `RandomizedSearchCV.best_params_` liefert je nach
    scipy- und numpy-Fassung `np.int64`/`np.float64` statt `int`/`float`.
    `np.float64` erbt von `float` und ueberlebt `json.dumps` zufaellig,
    `np.int64` erbt NICHT von `int`. Mit `default=str` als Notausgang wuerde
    daraus die Zeichenkette "287", und `set_params(n_estimators="287")` bricht
    ab - mitten im mehrstuendigen Lauf, nach dem Tuning.

    Ob es auftritt, haengt an der Paketversion; hier lief es durch, auf einer
    anderen Kombination nicht zwingend. Deshalb explizit wandeln statt hoffen -
    und ohne `default=`, damit ein unbekannter Typ laut auffaellt statt still
    zur Zeichenkette zu werden (docs/07_BEFUNDE.md, B-23).
    """
    return {schluessel: (wert.item() if isinstance(wert, np.generic) else wert)
            for schluessel, wert in p.items()}


def _parameter_je_fold(parameter: pd.DataFrame) -> dict:
    """tuning.csv -> {(zielgroesse, verfahren, fold): dict}."""
    return {(z["zielgroesse"], z["verfahren"], int(z["fold"])):
            json.loads(z["parameter_json"])
            for _, z in parameter.iterrows()}


def phase_bewertung(panel: pd.DataFrame, parameter: pd.DataFrame,
                    selten: pd.Series) -> pd.DataFrame:
    """10 Wiederholungen x 5 Folds x 3 Verfahren x 2 Zielgroessen = 300 Zeilen.

    Trainiert wird je Fold auf allen Trainingsstadtteilen - mit den Parametern
    aus Phase 1, aber einem FRISCHEN Modell. Der `best_estimator_` aus dem
    Tuning waere auf nur drei Vierteln der Trainingsstadtteile gefittet.
    """
    param = _parameter_je_fold(parameter)
    zeilen = []
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(panel, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            for ziel in ZIELE:
                for name in VERFAHREN:
                    # Der Parallelisierungsgewinn wird nur in Wiederholung 0
                    # erhoben - 30 Messungen genuegen fuer einen Faktor.
                    z = ein_lauf(name, param[(ziel, name, k)], train, test,
                                 ziel, auch_parallel=(w == 0))
                    zeilen.append({"wiederholung": w, "fold": k, **z})
        print(f"    Wiederholung {w}: {len(zeilen):>3} Laeufe")
    df = pd.DataFrame(zeilen)
    spalten = (["zielgroesse", "verfahren", "wiederholung", "fold",
                "RMSE", "MAE", "R2", "train_sekunden", "inferenz_sekunden",
                "train_sekunden_parallel", "inferenz_sekunden_parallel",
                "n_train", "n_test", "extrapolationsanteil",
                "n_negativ", "y_hat_min"])
    df = df[spalten]
    df.to_csv(OUT / "menge_folds.csv", index=False)
    return df


MASSE = ["RMSE", "MAE", "R2", "train_sekunden", "inferenz_sekunden"]
# Nur in Wiederholung 0 erhoben, deshalb getrennt gemittelt.
MASSE_PARALLEL = ["train_sekunden_parallel", "inferenz_sekunden_parallel"]


def aggregiere(folds: pd.DataFrame) -> pd.DataFrame:
    """Zweistufig mitteln - erst je Wiederholung, dann darueber.

    FALLSTRICK 1 (R-5): Die 50 Fold-Ergebnisse sind NICHT unabhaengig - es sind
    dieselben 29 Stadtteile in zehn Gruppierungen. Ein Konfidenzintervall aus
    std_folds/sqrt(50) waere deshalb zu eng. Massgeblich ist
    `std_wiederholungen`: erst je Wiederholung ueber die 5 Folds mitteln, das
    ergibt 10 Werte, und deren Standardabweichung wird berichtet.

    Beide Spalten wandern mit, damit der Unterschied sichtbar bleibt.
    """
    schluessel = ["zielgroesse", "verfahren"]
    g = folds.groupby(schluessel, sort=False)
    z = g[MASSE].mean().add_suffix("_mean")
    z = z.join(g[MASSE].std().add_suffix("_std_folds"))
    je_wdh = folds.groupby(schluessel + ["wiederholung"], sort=False)[MASSE].mean()
    z = z.join(je_wdh.groupby(schluessel, sort=False).std()
                     .add_suffix("_std_wiederholungen"))
    z = z.join(g[MASSE_PARALLEL].mean().add_suffix("_mean"))
    # Parallelisierungsgewinn: Faktor, um den der Fit ueber alle Kerne
    # schneller ist. Bei Ridge zu erwarten: rund 1 - eine geschlossene Loesung
    # hat nichts zu verteilen. Das ist selbst eine Aussage fuer UF4.
    #
    # ZAEHLER UND NENNER MUESSEN AUS DERSELBEN MENGE KOMMEN: Die parallele Zeit
    # wird nur in Wiederholung 0 erhoben. Teilte man durch den Mittelwert ueber
    # alle 50 Laeufe, verglichen sich 50 einkernige gegen 5 parallele Messungen,
    # und der "Gewinn" enthielte die Schwankung zwischen den Wiederholungen.
    w0 = folds[folds["wiederholung"] == 0].groupby(schluessel, sort=False)
    gewinn = (w0["train_sekunden"].mean() / w0["train_sekunden_parallel"].mean())
    z["parallel_gewinn"] = gewinn
    z = z.join(g[["extrapolationsanteil"]].mean())
    z = z.join(g[["n_negativ"]].sum().rename(columns={"n_negativ": "n_negativ_gesamt"}))
    spalten = [f"{m}{s}" for m in MASSE for s in
               ("_mean", "_std_folds", "_std_wiederholungen")]
    spalten += [f"{m}_mean" for m in MASSE_PARALLEL] + ["parallel_gewinn"]
    z = z[spalten + ["extrapolationsanteil", "n_negativ_gesamt"]].reset_index()
    z.round(4).to_csv(OUT / "menge_mittel.csv", index=False)
    return z


# ---------------------------------------------------------------------------
# FALLSTRICK 2  Mehrfachvergleiche (R-10)
# ---------------------------------------------------------------------------
def _holm(p: np.ndarray) -> np.ndarray:
    """Holm-Bonferroni: p-Werte aufsteigend, kleinster gegen alpha/m, dann
    alpha/(m-1), bis zur ersten Nichtablehnung.

    Zurueckgegeben werden angepasste p-Werte, die direkt gegen alpha geprueft
    werden koennen - das ist dieselbe Entscheidung wie der schrittweise
    Vergleich, nur bequemer. Uniform staerker als Bonferroni bei gleicher
    Fehlerkontrolle; es gibt keinen Grund, darauf zu verzichten.
    """
    m = len(p)
    ordnung = np.argsort(p)
    angepasst = np.empty(m, float)
    laufend = 0.0
    for rang, i in enumerate(ordnung):
        laufend = max(laufend, (m - rang) * p[i])
        angepasst[i] = min(laufend, 1.0)
    return angepasst


def _gepaart(a: np.ndarray, b: np.ndarray) -> dict:
    """Ein gepaarter Wilcoxon plus die Zahlen, die auch ohne p-Wert tragen.

    `a` ist das Verfahren, `b` der Gegner. Bei RMSE ist klein besser, die
    Differenz b - a ist also der VORTEIL von a.
    """
    from scipy.stats import t, wilcoxon

    diff = np.asarray(b, float) - np.asarray(a, float)
    n = len(diff)
    mittel = float(diff.mean())
    if n > 1 and diff.std(ddof=1) > 0:
        halb = float(t.ppf(1 - ALPHA / 2, n - 1) * diff.std(ddof=1) / np.sqrt(n))
    else:
        halb = 0.0
    try:
        p = float(wilcoxon(diff, zero_method="wilcox").pvalue)
    except ValueError:            # alle Differenzen null
        p = 1.0
    return {"n_paare": n, "differenz_mittel": mittel,
            "ci_unten": mittel - halb, "ci_oben": mittel + halb,
            "gewonnene": int((diff > 0).sum()), "wilcoxon_p": p}


def vergleiche(folds: pd.DataFrame, baselines: pd.DataFrame) -> pd.DataFrame:
    """Gepaarter Wilcoxon auf RMSE, zwei Rollen und zwei Teststufen.

    ROLLEN
      primaer     jedes Verfahren gegen die Stufe-2-Baseline (3 x 2 = 6 Tests).
                  KEINE Testfamilie - jede Frage ist nach #34 vorab einzeln
                  formuliert, deshalb keine Korrektur.
      sekundaer   jedes Verfahrenspaar (3 Paare x 2 Zielgroessen = 6 Tests).
                  Eine Familie, darauf Holm-Bonferroni.

    TESTSTUFEN (docs/07_BEFUNDE.md, B-5)
      wiederholung  n = 10, gemittelt je Wiederholung. DAS IST DER PRIMAERTEST.
                    Die 50 Einzellaeufe sind Pseudoreplikation - dieselben 29
                    Stadtteile, nur anders gruppiert. Ein Wilcoxon darueber
                    liefert zu kleine p-Werte.
      lauf          n = 50, alle Einzellaeufe. Ausdruecklich als Sensitivitaet
                    gefuehrt, nicht als Ergebnis.

    Auch die zehn Wiederholungsmittel sind nicht unabhaengig - es bleiben 29
    Einheiten. Das berichtete Konfidenzintervall ist daher enger als die wahre
    Unsicherheit (Nadeau & Bengio 2003). Deshalb stehen mittlere Differenz,
    Konfidenzintervall und gewonnene Laeufe IMMER daneben, unabhaengig vom p.
    """
    basis = baselines[baselines["modell"] == BASELINE_STUFE2]
    zeilen = []

    for stufe, schluessel in (("wiederholung", ["wiederholung"]),
                              ("lauf", ["wiederholung", "fold"])):
        # Auf der Stufe "wiederholung" wird je Wiederholung ueber die 5 Folds
        # gemittelt - dieselbe zweistufige Logik wie in aggregiere().
        v = (folds.groupby(["zielgroesse", "verfahren"] + schluessel,
                           sort=False)[TESTMASS].mean().rename("wert").reset_index())
        b = (basis.groupby(["zielgroesse"] + schluessel,
                           sort=False)[TESTMASS].mean().rename("wert").reset_index())

        for ziel in ZIELE:
            # GEPAART heisst: auf denselben Laeufen. Deshalb wird ueber die
            # Schluessel VERBUNDEN und nicht auf gleiche Reihenfolge vertraut -
            # sonst subtrahiert man stillschweigend verschiedene Testmengen
            # voneinander. Fehlt ein Gegenstueck, bricht der Lauf ab.
            reihen = {n: (v[(v["zielgroesse"] == ziel) & (v["verfahren"] == n)]
                          .set_index(schluessel)["wert"]) for n in VERFAHREN}
            gegner = b[b["zielgroesse"] == ziel].set_index(schluessel)["wert"]

            def paar(links: pd.Series, rechts: pd.Series) -> dict:
                zusammen = pd.concat([links.rename("a"), rechts.rename("b")],
                                     axis=1, join="inner")
                fehlend = max(len(links), len(rechts)) - len(zusammen)
                assert not fehlend, (
                    f"{fehlend} Laeufe ohne Gegenstueck bei {ziel}, Stufe "
                    f"{stufe} - Verfahren und Baseline liefen auf "
                    f"unterschiedlichen Aufteilungen.")
                return _gepaart(zusammen["a"].to_numpy(), zusammen["b"].to_numpy())

            for name in VERFAHREN:                       # primaer
                zeilen.append({"teststufe": stufe, "zielgroesse": ziel,
                               "paarung": f"{name} vs {BASELINE_STUFE2}",
                               "rolle": "primaer", "mass": TESTMASS,
                               **paar(reihen[name], gegner),
                               "n_tests_familie": 1})

            for i, a in enumerate(VERFAHREN):            # sekundaer
                for c in VERFAHREN[i + 1:]:
                    zeilen.append({"teststufe": stufe, "zielgroesse": ziel,
                                   "paarung": f"{a} vs {c}",
                                   "rolle": "sekundaer", "mass": TESTMASS,
                                   **paar(reihen[a], reihen[c]),
                                   "n_tests_familie": 3 * len(ZIELE)})

    df = pd.DataFrame(zeilen)

    # Holm je Teststufe getrennt, nur innerhalb der sekundaeren Familie.
    # ZWEI FAMILIEN, nicht sieben Tests: Regression und Klassifikation
    # beantworten verschiedene Teilfragen (Entscheidung 05.08.2026, B-6).
    # m03_struktur.py hat genau einen Test und wird nicht korrigiert.
    df["p_holm"] = np.nan
    for stufe in df["teststufe"].unique():
        maske = (df["rolle"] == "sekundaer") & (df["teststufe"] == stufe)
        df.loc[maske, "p_holm"] = _holm(df.loc[maske, "wilcoxon_p"].to_numpy())
    df["signifikant"] = np.where(
        df["rolle"] == "primaer", df["wilcoxon_p"] < ALPHA, df["p_holm"] < ALPHA)

    df.round(6).to_csv(OUT / "vergleich.csv", index=False)
    return df


def leakage_diagnose(folds: pd.DataFrame, baselines: pd.DataFrame) -> pd.DataFrame:
    """Beziffert, was das Tuning auf Wiederholung 0 kostet (B-21).

    Getunt wird einmal, auf Wiederholung 0. Dort stammen die Parameter aus dem
    Trainingssatz genau dieses Folds - der Vorsprung gegen die Baseline ist
    sauber gemessen. In den Wiederholungen 1 bis 9 werden dieselben Parameter
    auf andere Aufteilungen angewandt; im Mittel waren dort 78 % der
    Teststadtteile in der Menge, auf der die Parameter gesucht wurden.

    Waere der Effekt bedeutsam, muesste der Vorsprung in W1-9 SYSTEMATISCH
    groesser ausfallen als in W0. Diese Funktion misst genau das.

    Die Diagnose ist bewusst als schwach zu lesen: W0 ist auch eine andere
    Aufteilung als W1-9, der Unterschied ist also konfundiert. Ein deutlicher
    Effekt waere sichtbar, ein kleiner nicht von Fold-Schwankung zu trennen.
    Sie kostet dafuer keine zusaetzliche Rechenzeit - dieselbe Logik, mit der
    R-9 von einem Vorbehalt zu einer Zahl wurde.
    """
    basis = (baselines[baselines["modell"] == BASELINE_STUFE2]
             .set_index(["zielgroesse", "wiederholung", "fold"])[TESTMASS])
    zeilen = []
    for (ziel, name), g in folds.groupby(["zielgroesse", "verfahren"], sort=False):
        g = g.set_index(["zielgroesse", "wiederholung", "fold"])
        # Positiver Vorsprung = das Verfahren ist besser als die Baseline.
        vorsprung = basis.reindex(g.index) - g[TESTMASS]
        w0 = vorsprung.xs(0, level="wiederholung")
        rest = vorsprung[vorsprung.index.get_level_values("wiederholung") > 0]
        zeilen.append({
            "zielgroesse": ziel, "verfahren": name, "mass": TESTMASS,
            "vorsprung_w0": float(w0.mean()), "n_w0": int(len(w0)),
            "vorsprung_w1_9": float(rest.mean()), "n_w1_9": int(len(rest)),
            "differenz": float(rest.mean() - w0.mean()),
            "differenz_in_std_folds": float((rest.mean() - w0.mean())
                                            / g[TESTMASS].std())})
    df = pd.DataFrame(zeilen)
    df.round(4).to_csv(OUT / "leakage_diagnose.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# FALLSTRICK 4  Das Hold-out
# ---------------------------------------------------------------------------
def hold_out(panel: pd.DataFrame, parameter: pd.DataFrame,
             folds: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """EINMALIG: auf allen 29 Entwicklungsstadtteilen trainieren, auf den 6
    Hold-out-Stadtteilen bewerten.

    WELCHE PARAMETER? Das Tuning liefert fuenf Saetze je Zielgroesse und
    Verfahren, einen je Fold. Die Spezifikation sagt "die in der
    Kreuzvalidierung gewaehlten", legt aber nicht fest, welcher davon
    (docs/07_BEFUNDE.md, B-14). Gewaehlt ist der Satz des Folds mit dem
    niedrigsten RMSE in Wiederholung 0 - deterministisch, nachvollziehbar und
    ausschliesslich aus Entwicklungsdaten. Der gewaehlte Fold steht in der
    Ausgabespalte `fold_der_parameter`.

    ZU BERICHTEN ist, dass dies EINE Messung an SECHS Einheiten ist - kein
    Mittelwert, keine Streuung. Die Zahl ist deutlich unsicherer als die
    Kreuzvalidierungswerte und darf nicht als deren Bestaetigung gelesen
    werden (R-4).
    """
    param = _parameter_je_fold(parameter)
    dev, ho = entwicklung_und_holdout(panel)
    train, test = panel[dev], panel[ho]
    print(f"    Training auf {train['stadtteil'].nunique()} Stadtteilen "
          f"({len(train):,} Zeilen), Bewertung auf "
          f"{test['stadtteil'].nunique()} ({len(test):,} Zeilen)")

    w0 = folds[folds["wiederholung"] == 0]
    zeilen = []
    for ziel in ZIELE:
        for name in VERFAHREN:
            g = w0[(w0["zielgroesse"] == ziel) & (w0["verfahren"] == name)]
            bester = int(g.loc[g["RMSE"].idxmin(), "fold"])
            z = ein_lauf(name, param[(ziel, name, bester)], train, test, ziel)
            zeilen.append({**z, "fold_der_parameter": bester,
                           "n_stadtteile_test": int(test["stadtteil"].nunique())})
    df = pd.DataFrame(zeilen)
    df.round(6).to_csv(OUT / "holdout.csv", index=False)
    return df


def main(argv: list[str]) -> int:
    if not PFAD_REGRESSION.exists():
        raise SystemExit(f"{PFAD_REGRESSION.relative_to(ROOT)} fehlt - "
                         f"erst 'python prep/build.py' ausfuehren.")
    if not (OUT / "baselines_folds.csv").exists():
        raise SystemExit("results/regression/baselines_folds.csv fehlt - "
                         "erst 'python vorpruefung/v1_baselines.py' ausfuehren.")
    OUT.mkdir(parents=True, exist_ok=True)

    voll = pd.read_parquet(PFAD_REGRESSION)
    selten = selten_je_stadtteil(pd.read_parquet(PFAD_KLASSIFIKATION))

    # FALLSTRICK 4, konstruktiv: Ohne das Argument "holdout" wird der Datensatz
    # HIER auf die Entwicklungsstadtteile eingeschraenkt. Alles Folgende kann
    # die Hold-out-Zeilen nicht mehr sehen, auch nicht versehentlich.
    panel = voll[voll["ist_holdout"] == 0].reset_index(drop=True)
    print(f"  Entwicklung: {len(panel):,} Zeilen | "
          f"{panel['stadtteil'].nunique()} Stadtteile\n")

    print("  Phase 1  Tuning")
    parameter = phase_tuning(panel, selten)
    print("\n  Phase 2  Bewertung")
    folds = phase_bewertung(panel, parameter, selten)
    print("\n  Phase 3  Aggregation")
    mittel = aggregiere(folds)
    print(mittel.to_string(index=False))
    print("\n  Phase 4  Vergleich")
    basislinien = pd.read_csv(OUT / "baselines_folds.csv")
    v = vergleiche(folds, basislinien)
    print(v[v["teststufe"] == "wiederholung"]
          [["zielgroesse", "paarung", "rolle", "differenz_mittel",
            "gewonnene", "wilcoxon_p", "p_holm", "signifikant"]]
          .to_string(index=False))

    print("\n  Diagnose zum Tuning auf Wiederholung 0 (B-21):")
    print(leakage_diagnose(folds, basislinien).to_string(index=False))

    if "holdout" in argv:
        print("\n  Phase 5  Hold-out - EINMALIGE Schlussbewertung")
        print(hold_out(voll, parameter, folds, selten).to_string(index=False))
    else:
        print("\n  Hold-out unberuehrt. Fuer die Schlussbewertung:"
              "\n  python modelle/m02_menge.py holdout")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
