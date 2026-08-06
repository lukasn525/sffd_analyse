"""
Verfahrensvergleich fuer die STRUKTUR der Einsatzlast.

Eine Zielgroesse (`dominante_einsatzart`, vier Klassen) x zwei Verfahren
(Random Forest, XGBoost) x 10 Wiederholungen x 5 Folds = 100 Laeufe.

    python modelle/m03_struktur.py            Tuning, Bewertung, Aggregation, Vergleich
    python modelle/m03_struktur.py holdout    zusaetzlich die einmalige Schlussbewertung

Ausgang: results/klassifikation/struktur_folds.csv · struktur_mittel.csv
                                tuning.csv · vergleich.csv · holdout.csv

AUFBAU: Spiegelt m02_menge.py. Dieselben sieben Funktionen, dieselbe Reihenfolge,
dieselben Fallstricke. Wer m02 gelesen hat, kennt die Struktur - hier stehen nur
die Unterschiede.

STAND: vollstaendig, 05.08.2026.

--------------------------------------------------------------------------
WAS ANDERS IST ALS IN m02
--------------------------------------------------------------------------
  Zielgroesse   `dominante_einsatzart`, vier ungeordnete Klassen
  Verfahren     nur RandomForestClassifier und XGBClassifier - Ridge hat auf
                einer nominalen Zielgroesse keine Entsprechung (Decision Log #31)
  Guetemasse    Macro-F1 (Hauptmass) und Macro-AUROC; Accuracy nur nachrichtlich
  Baseline      Stufe 2 ist die multinomiale logistische Regression (#33),
                nicht die Mehrheitsklasse
  Scoring       beim Tuning "f1_macro" statt RMSE
  Holm          entfaellt. Es gibt genau EINEN sekundaeren Test (RF gegen
                XGBoost); eine Familie aus einem Test braucht keine Korrektur.
                Entscheidung vom 05.08.2026: Regression und Klassifikation sind
                getrennte Testfamilien (docs/07_BEFUNDE.md, B-6).

--------------------------------------------------------------------------
DREI FALLSTRICKE, die es in m02 nicht gibt
--------------------------------------------------------------------------
  1  KLASSENGEWICHTE statt Resampling. `class_weight="balanced"` beim Random
     Forest, `sample_weight` beim XGBClassifier. KEIN SMOTE, kein Over- oder
     Undersampling - das waere ein Eingriff in die Datenverteilung und wuerde
     die Vergleichbarkeit mit den Baselines brechen.

  2  LABEL-ENCODER EINMAL GLOBAL fitten, nicht je Fold. XGBClassifier erwartet
     Integer-Labels 0..3. Wird der Encoder je Fold neu gefittet, verschiebt sich
     das Mapping in Folds, in denen eine Klasse nicht auftritt - und die
     Wahrscheinlichkeitsspalten zeigen dann auf die falschen Klassen.
     Nach der Vorhersage die Spalten auf die Reihenfolge von KLASSEN
     zurueckbringen.

  3  MACRO-AUROC KANN UNDEFINIERT SEIN, wenn eine Klasse im Testfold fehlt.
     Durch die doppelte Stratifizierung (#30) sollte das nicht vorkommen -
     falls doch, den Wert als FEHLEND fuehren und nicht durch null ersetzen,
     sonst zieht er den Mittelwert nach unten. `zero_division=0` bei Macro-F1
     muss gesetzt bleiben, sonst bricht der Lauf ab.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE nach jedem Lauf
--------------------------------------------------------------------------
  - Schlaegt ueberhaupt ein Verfahren Stufe 2? Wenn nein, ist das ein
    berichtbares Ergebnis und kein Fehler (docs/06_RISIKEN.md, R-2).
  - Hat jeder Fold Brand-Testfaelle? In Wiederholung 0 erwartet: 13 · 9 · 6 · 3 · 2.
  - Liegt Accuracy deutlich ueber Macro-F1? Das ist normal und selbst ein
    Argument fuer die Metrikwahl - siehe docs/03_STAND.md.
  - Wie viele Laeufe haben keine definierte Macro-AUROC? Erwartet: keiner.
  - Passt die Zeilenzahl? 10 in tuning.csv, 100 in struktur_folds.csv.
  - Wurde das Hold-out beruehrt? Ohne Argument darf keine Zeile mit
    ist_holdout == 1 gelesen worden sein.
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

from config import (ANTEILE, N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from config_modelle import (RANDOM_STATE, SUCHRAEUME,  # noqa: E402
                            TUNING_BUDGET, WIEDERHOLUNGEN)
from s2_datensaetze import ZIELKLASSE, fold_masken  # noqa: E402
from v0_aufteilung import (entwicklung_und_holdout,  # noqa: E402
                           selten_je_stadtteil, wiederholte_aufteilung)

OUT = RESULTS_DIR / "klassifikation"
MERKMALE = PRAEDIKTOREN + SAISON
VERFAHREN = ("random_forest", "xgboost")

# Die vier Klassen in FESTER Reihenfolge - abgeleitet aus ANTEILE, also aus
# derselben Quelle, aus der die Zielgroesse per argmax entsteht. Diese Liste
# ist der globale Label-Encoder (Fallstrick 2): Index = Integer-Label.
KLASSEN = [s.replace("anteil_", "") for s in ANTEILE]
SELTENE_KLASSE = "brand"

BASELINE_STUFE2 = "Logistische Regression (L2)"
TESTMASS = "macro_f1"
ALPHA = 0.05

# Wie in m02: Modelle einkernig, nur die Suche parallel. Begruendung dort im
# Block PARALLELISIERUNG (docs/07_BEFUNDE.md, B-16). Der berichtete Aufwand
# muss zwischen den Verfahren vergleichbar sein, und der Parallelisierungs-
# gewinn ist eine eigene Groesse.
N_JOBS_MODELL = 1
N_JOBS_SUCHE = -1


# ---------------------------------------------------------------------------
# BAUSTEIN 1  Die Pipeline
# ---------------------------------------------------------------------------
def verfahren(name: str, n_jobs: int = N_JOBS_MODELL):
    """Baut die ungetunte Pipeline. Kein Scaler - beide Verfahren sind Baeume.

    `n_jobs` steuert nur die Parallelisierung, nicht das Ergebnis. Voreinstellung
    einkernig, damit die Laufzeiten vergleichbar bleiben.

    FALLSTRICK 1: Die Klassenverteilung ist stark schief (79 % Fehlalarm). Statt
    zu resampeln bekommen beide Verfahren GEWICHTE. Beim Random Forest geht das
    als Hyperparameter (`class_weight="balanced"`), beim XGBClassifier ueber
    `sample_weight` beim Fit - das Verfahren kennt keinen entsprechenden
    Parameter. Beides bewirkt dasselbe: seltene Klassen zaehlen mehr, ohne dass
    eine einzige Zeile dupliziert oder geloescht wird.
    """
    if name == "random_forest":
        from sklearn.ensemble import RandomForestClassifier
        return RandomForestClassifier(random_state=RANDOM_STATE, n_jobs=n_jobs,
                                      class_weight="balanced")
    if name == "xgboost":
        from xgboost import XGBClassifier
        return XGBClassifier(random_state=RANDOM_STATE, n_jobs=n_jobs,
                             objective="multi:softprob",
                             num_class=len(KLASSEN))
    raise ValueError(f"Unbekanntes Verfahren: {name}")


def suchraum(name: str) -> dict:
    """Uebersetzt SUCHRAEUME in scipy-Verteilungen - wie in m02, ohne Praefix.

    Beide Verfahren sind hier nackte Schaetzer statt Pipelines, weil keine
    Skalierung noetig ist. Die Suchraeume sind dieselben wie in der Regression;
    das ist Absicht: Es wechselt nur die Verlustfunktion, nicht der
    Ensemble-Mechanismus (docs/04_MODELLIERUNG.md, Abschnitt 3).

    EINE AUSNAHME: `tweedie_variance_power` steuert die Verlustfunktion der
    REGRESSION (Decision Log #42) und ist bei `multi:softprob` bedeutungslos.
    XGBoost wuerde ihn stillschweigend annehmen und ignorieren - er wuerde dann
    ein Sechstel des Tuning-Budgets auf eine wirkungslose Dimension verschwenden
    und in tuning.csv eine Zahl ausweisen, die nichts bedeutet.
    """
    from scipy.stats import loguniform, randint, uniform

    NUR_REGRESSION = {"tweedie_variance_power"}
    raum = {}
    for parameter, spez in SUCHRAEUME[name].items():
        if parameter in NUR_REGRESSION:
            continue
        art, *werte = spez
        if art == "loguniform":
            raum[parameter] = loguniform(werte[0], werte[1])
        elif art == "int":
            raum[parameter] = randint(werte[0], werte[1] + 1)
        elif art == "uniform":
            raum[parameter] = uniform(werte[0], werte[1] - werte[0])
        elif art == "choice":
            raum[parameter] = werte[0]
        else:
            raise ValueError(f"Unbekannte Suchraum-Art: {art}")
    return raum


def kodiere(y: pd.Series) -> np.ndarray:
    """Klassennamen -> Integer 0..3 nach der GLOBALEN Reihenfolge KLASSEN.

    FALLSTRICK 2: Das Mapping haengt bewusst NICHT von den Daten ab, die gerade
    vorliegen. Ein je Fold gefitteter LabelEncoder wuerde in einem Fold ohne
    Brand die Zahlen verschieben, und die Wahrscheinlichkeitsspalten zeigten
    danach auf die falschen Klassen - ohne Fehlermeldung.
    """
    index = {k: i for i, k in enumerate(KLASSEN)}
    unbekannt = set(y.unique()) - set(index)
    assert not unbekannt, f"Unbekannte Klassen im Datensatz: {unbekannt}"
    return y.map(index).to_numpy()


def _gewichte(y_int: np.ndarray) -> np.ndarray:
    """`class_weight='balanced'` von Hand - fuer XGBoost, das keinen hat."""
    from sklearn.utils.class_weight import compute_sample_weight
    return compute_sample_weight("balanced", y_int)


# ---------------------------------------------------------------------------
# BAUSTEIN 2  Das Tuning
# ---------------------------------------------------------------------------
def tune(name: str, train: pd.DataFrame) -> dict:
    """Wie m02.tune, aber mit `f1_macro` als Scoring.

    FALLSTRICK aus m02 gilt unveraendert: Der innere CV MUSS nach Stadtteil
    gruppieren, sonst stehen dieselben 132 Zeilen eines Stadtteils in innerem
    Training und innerer Validierung.

    Warum f1_macro und nicht Accuracy: Die Mehrheitsklasse allein erreicht ueber
    0,8 Accuracy. Ein darauf optimiertes Tuning wuerde Modelle waehlen, die die
    drei seltenen Klassen ignorieren - genau das, was die Fragestellung nicht
    will (docs/03_STAND.md, Abschnitt 4).
    """
    from sklearn.model_selection import GroupKFold, RandomizedSearchCV

    X = train[MERKMALE].astype(float)
    y = kodiere(train[ZIELKLASSE])
    suche = RandomizedSearchCV(
        estimator=verfahren(name, n_jobs=N_JOBS_MODELL),
        param_distributions=suchraum(name),
        n_iter=TUNING_BUDGET,
        cv=GroupKFold(n_splits=4),
        scoring="f1_macro",
        random_state=RANDOM_STATE,
        n_jobs=N_JOBS_SUCHE,
    )
    if name == "xgboost":
        suche.fit(X, y, groups=train["stadtteil"], sample_weight=_gewichte(y))
    else:
        suche.fit(X, y, groups=train["stadtteil"])
    return suche.best_params_


# ---------------------------------------------------------------------------
# BAUSTEIN 3  Ein einzelner Lauf
# ---------------------------------------------------------------------------
def ein_lauf(name: str, parameter: dict, train: pd.DataFrame,
             test: pd.DataFrame, auch_parallel: bool = False) -> dict:
    """Ein Fit, eine Vorhersage, mit Zeitmessung - eine Zeile fuer die CSV.

    Wie in m02 wird die Zeit UM `fit` und `predict` herum gemessen, und zwar
    EINKERNIG fuer beide Verfahren. Die Wahrscheinlichkeiten fuer die AUROC
    kommen aus einem zweiten Aufruf, damit `inferenz_sekunden` die reine
    Klassenvorhersage misst und zwischen den Verfahren vergleichbar bleibt.

    `auch_parallel=True` misst denselben Fit zusaetzlich ueber alle Kerne; nur
    in Wiederholung 0 erhoben.
    """
    from sklearn.metrics import accuracy_score, f1_score

    X_tr, X_te = train[MERKMALE].astype(float), test[MERKMALE].astype(float)
    y_tr, y_te = kodiere(train[ZIELKLASSE]), kodiere(test[ZIELKLASSE])

    def fitte(kerne: int):
        m = verfahren(name, n_jobs=kerne).set_params(**parameter)
        t = time.perf_counter()
        if name == "xgboost":
            m.fit(X_tr, y_tr, sample_weight=_gewichte(y_tr))
        else:
            m.fit(X_tr, y_tr)
        dauer_fit = time.perf_counter() - t
        t = time.perf_counter()
        vorhersage = m.predict(X_te)
        return m, vorhersage, dauer_fit, time.perf_counter() - t

    modell, y_hat, train_sek, inferenz_sek = fitte(N_JOBS_MODELL)

    train_par, inferenz_par, abweichung = np.nan, np.nan, np.nan
    if auch_parallel:
        _, y_par, train_par, inferenz_par = fitte(-1)
        # Anteil der Zeilen, die einkernig und parallel verschieden
        # klassifiziert werden. KEIN Abbruch - gemessen und berichtet: Bei
        # XGBoost ist die Vorhersage threadabhaengig (docs/07_BEFUNDE.md,
        # B-24). Die berichteten Guetemasse stammen aus dem einkernigen Fit.
        abweichung = float(np.mean(y_hat != y_par))

    return {
        "verfahren": name, "zielgroesse": ZIELKLASSE,
        "train_sekunden_parallel": train_par,
        "inferenz_sekunden_parallel": inferenz_par,
        "parallel_abweichung": abweichung,
        "macro_f1": float(f1_score(y_te, y_hat, average="macro", zero_division=0)),
        "macro_auroc": _macro_auroc(y_te, modell.predict_proba(X_te),
                                    list(modell.classes_)),
        "accuracy": float(accuracy_score(y_te, y_hat)),
        "train_sekunden": train_sek, "inferenz_sekunden": inferenz_sek,
        "n_train": len(train), "n_test": len(test),
        "n_brand_test": int((test[ZIELKLASSE] == SELTENE_KLASSE).sum()),
        "extrapolationsanteil": extrapolationsanteil(train, test),
    }


def extrapolationsanteil(train: pd.DataFrame, test: pd.DataFrame) -> float:
    """Anteil der Testzeilen ausserhalb des Trainings-Wertebereichs.

    Wortgleich zu m02_menge. Bewusst dupliziert statt importiert: Ein
    gemeinsames Hilfsmodul fuer zwei Aufrufer braechte mehr Indirektion als
    Ersparnis, und m03 soll unabhaengig von m02 lauffaehig bleiben
    (docs/04_MODELLIERUNG.md, Abschnitt 4).
    """
    lo, hi = train[MERKMALE].min(), train[MERKMALE].max()
    aussen = ((test[MERKMALE] < lo) | (test[MERKMALE] > hi)).any(axis=1)
    return float(aussen.mean())


def _gepaart(a: np.ndarray, b: np.ndarray) -> dict:
    """Ein gepaarter Wilcoxon plus die Zahlen, die auch ohne p-Wert tragen.

    Wortgleich zu m02_menge; siehe dort. `a` ist das Verfahren, `b` der Gegner,
    die Differenz b - a ist der Vorteil von a. Bei Macro-F1 ist gross besser,
    die Aufrufstelle dreht die Argumente entsprechend.
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
    except ValueError:
        p = 1.0
    return {"n_paare": n, "differenz_mittel": mittel,
            "ci_unten": mittel - halb, "ci_oben": mittel + halb,
            "gewonnene": int((diff > 0).sum()), "wilcoxon_p": p}


def _macro_auroc(y_true: np.ndarray, proba: np.ndarray,
                 klassen_modell: list) -> float:
    """Macro-AUROC (One-vs-Rest), oder NaN wenn eine Klasse im Test fehlt.

    FALLSTRICK 3: NICHT durch 0,5 ersetzen. Ein erfundener Wert saehe wie eine
    Messung aus und zoege den Mittelwert nach unten. Fehlend heisst fehlend.

    `labels=klassen_modell` bringt die Wahrscheinlichkeitsspalten in die
    Reihenfolge, die das Modell tatsaechlich benutzt hat - der zweite Teil von
    Fallstrick 2.
    """
    from sklearn.metrics import roc_auc_score

    if len(np.unique(y_true)) != len(KLASSEN):
        return float("nan")
    try:
        return float(roc_auc_score(y_true, proba, multi_class="ovr",
                                   average="macro", labels=klassen_modell))
    except ValueError:
        return float("nan")


# ---------------------------------------------------------------------------
# ORCHESTRIERUNG
# ---------------------------------------------------------------------------
def phase_tuning(panel: pd.DataFrame, selten: pd.Series,
                 neu: bool = False) -> pd.DataFrame:
    """Je Verfahren und Fold einmal `tune()` auf Wiederholung 0 - 10 Zeilen.

    Wie in m02: Eine vollstaendige `tuning.csv` wird wiederverwendet, damit ein
    Abbruch in einer spaeteren Phase die teuerste Phase nicht vernichtet.
    Neuberechnung erzwingen mit `python modelle/m03_struktur.py neutuning`.
    """
    pfad = OUT / "tuning.csv"
    erwartet = len(VERFAHREN) * N_FOLDS
    if pfad.exists() and not neu:
        vorhanden = pd.read_csv(pfad)
        grund = ""
        if len(vorhanden) != erwartet:
            grund = f"{len(vorhanden)} statt {erwartet} Zeilen"
        else:
            for v in VERFAHREN:
                teil = vorhanden[vorhanden["verfahren"] == v]
                if teil.empty or set(json.loads(teil.iloc[0]["parameter_json"])) != set(suchraum(v)):
                    grund = f"Suchraum von {v} hat sich geaendert"
                    break
        if not grund:
            print(f"    {erwartet} Parametersaetze aus {pfad.name} uebernommen "
                  f"- fuer eine Neuberechnung: 'm03_struktur.py neutuning'")
            return vorhanden
        print(f"    {pfad.name} wird verworfen und neu gerechnet: {grund}")

    d = wiederholte_aufteilung(panel, wiederholung=0, selten=selten)
    zeilen = []
    for name in VERFAHREN:
        for k in range(1, N_FOLDS + 1):
            tr, _ = fold_masken(d, k)
            t = time.perf_counter()
            p = _rein_python(tune(name, d[tr]))
            dauer = time.perf_counter() - t
            zeilen.append({"zielgroesse": ZIELKLASSE, "verfahren": name,
                           "fold": k, "tuning_sekunden": round(dauer, 2), **p,
                           "parameter_json": json.dumps(p)})
            print(f"    tune  {name:<14} Fold {k}  {dauer:6.1f}s  {p}")
    df = pd.DataFrame(zeilen)
    df.to_csv(OUT / "tuning.csv", index=False)
    return df


def _rein_python(p: dict) -> dict:
    """NumPy-Skalare in native Typen wandeln - wortgleich zu m02_menge.

    `np.int64` erbt nicht von `int` und ueberlebt `json.dumps` nicht. Ohne
    diese Wandlung wuerde aus 287 die Zeichenkette "287", und `set_params`
    braeche nach dem Tuning ab (docs/07_BEFUNDE.md, B-23).
    """
    return {schluessel: (wert.item() if isinstance(wert, np.generic) else wert)
            for schluessel, wert in p.items()}


def _parameter_je_fold(parameter: pd.DataFrame) -> dict:
    return {(z["verfahren"], int(z["fold"])): json.loads(z["parameter_json"])
            for _, z in parameter.iterrows()}


def phase_bewertung(panel: pd.DataFrame, parameter: pd.DataFrame,
                    selten: pd.Series) -> pd.DataFrame:
    """10 Wiederholungen x 5 Folds x 2 Verfahren = 100 Zeilen."""
    param = _parameter_je_fold(parameter)
    zeilen = []
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(panel, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            for name in VERFAHREN:
                zeilen.append({"wiederholung": w, "fold": k,
                               **ein_lauf(name, param[(name, k)], train, test,
                                          auch_parallel=(w == 0))})
        print(f"    Wiederholung {w}: {len(zeilen):>3} Laeufe")
    df = pd.DataFrame(zeilen)
    spalten = ["zielgroesse", "verfahren", "wiederholung", "fold",
               "macro_f1", "macro_auroc", "accuracy",
               "train_sekunden", "inferenz_sekunden",
               "train_sekunden_parallel", "inferenz_sekunden_parallel",
               "parallel_abweichung",
               "n_train", "n_test", "n_brand_test", "extrapolationsanteil"]
    df = df[spalten]
    df.to_csv(OUT / "struktur_folds.csv", index=False)
    return df


MASSE = ["macro_f1", "macro_auroc", "accuracy",
         "train_sekunden", "inferenz_sekunden"]
MASSE_PARALLEL = ["train_sekunden_parallel", "inferenz_sekunden_parallel"]


def aggregiere(folds: pd.DataFrame) -> pd.DataFrame:
    """Zweistufig - wie in m02. Massgeblich ist `std_wiederholungen` (R-5)."""
    schluessel = ["zielgroesse", "verfahren"]
    g = folds.groupby(schluessel, sort=False)
    z = g[MASSE].mean().add_suffix("_mean")
    z = z.join(g[MASSE].std().add_suffix("_std_folds"))
    je_wdh = folds.groupby(schluessel + ["wiederholung"], sort=False)[MASSE].mean()
    z = z.join(je_wdh.groupby(schluessel, sort=False).std()
                     .add_suffix("_std_wiederholungen"))
    z = z.join(g[MASSE_PARALLEL].mean().add_suffix("_mean"))
    # Zaehler und Nenner aus derselben Menge - die parallele Zeit gibt es nur
    # fuer Wiederholung 0 (Begruendung ausfuehrlich in m02_menge.aggregiere).
    w0 = folds[folds["wiederholung"] == 0].groupby(schluessel, sort=False)
    z["parallel_gewinn"] = (w0["train_sekunden"].mean()
                            / w0["train_sekunden_parallel"].mean())
    z["parallel_abweichung_max"] = w0["parallel_abweichung"].max()
    z = z.join(g[["n_brand_test", "extrapolationsanteil"]].mean())
    z = z.join(g[["macro_auroc"]].apply(lambda s: int(s["macro_auroc"].isna().sum()))
                .rename("n_auroc_fehlend"))
    spalten = [f"{m}{s}" for m in MASSE for s in
               ("_mean", "_std_folds", "_std_wiederholungen")]
    spalten += ([f"{m}_mean" for m in MASSE_PARALLEL]
                + ["parallel_gewinn", "parallel_abweichung_max"])
    z = z[spalten + ["n_brand_test", "extrapolationsanteil",
                     "n_auroc_fehlend"]].reset_index()
    z.round(4).to_csv(OUT / "struktur_mittel.csv", index=False)
    return z


def vergleiche(folds: pd.DataFrame, baselines: pd.DataFrame) -> pd.DataFrame:
    """Gepaarter Wilcoxon auf Macro-F1 - zwei primaere Tests, ein sekundaerer.

    KEIN HOLM. Die sekundaere Familie besteht aus einem einzigen Test (Random
    Forest gegen XGBoost); eine Korrektur ueber einen Test ist die Identitaet.
    Die Spalte `p_holm` bleibt deshalb leer, `n_tests_familie` steht auf 1.
    Entscheidung vom 05.08.2026: Regression und Klassifikation sind getrennte
    Testfamilien (docs/07_BEFUNDE.md, B-6). Das ist in Kapitel 7 zu benennen -
    dieser Vergleich laeuft ungekorrigiert gegen alpha = 0,05.

    Teststufen wie in m02: `wiederholung` (n = 10) ist der Primaertest, `lauf`
    (n = 50) die ausdruecklich gekennzeichnete Sensitivitaet (B-5).
    """
    basis = baselines[baselines["modell"] == BASELINE_STUFE2]
    zeilen = []

    for stufe, schluessel in (("wiederholung", ["wiederholung"]),
                              ("lauf", ["wiederholung", "fold"])):
        v = (folds.groupby(["verfahren"] + schluessel, sort=False)[TESTMASS]
                  .mean().rename("wert").reset_index())
        gegner = (basis.groupby(schluessel, sort=False)["Macro-F1"]
                       .mean().rename("wert"))
        reihen = {n: v[v["verfahren"] == n].set_index(schluessel)["wert"]
                  for n in VERFAHREN}

        def paar(links, rechts):
            zusammen = pd.concat([links.rename("a"), rechts.rename("b")],
                                 axis=1, join="inner")
            fehlend = max(len(links), len(rechts)) - len(zusammen)
            assert not fehlend, (f"{fehlend} Laeufe ohne Gegenstueck, Stufe "
                                 f"{stufe} - unterschiedliche Aufteilungen.")
            # Bei Macro-F1 ist GROSS besser: Vorteil von a ist a - b.
            return _gepaart(zusammen["b"].to_numpy(), zusammen["a"].to_numpy())

        for name in VERFAHREN:
            zeilen.append({"teststufe": stufe, "zielgroesse": ZIELKLASSE,
                           "paarung": f"{name} vs {BASELINE_STUFE2}",
                           "rolle": "primaer", "mass": TESTMASS,
                           **paar(reihen[name], gegner), "n_tests_familie": 1})
        zeilen.append({"teststufe": stufe, "zielgroesse": ZIELKLASSE,
                       "paarung": f"{VERFAHREN[0]} vs {VERFAHREN[1]}",
                       "rolle": "sekundaer", "mass": TESTMASS,
                       **paar(reihen[VERFAHREN[0]], reihen[VERFAHREN[1]]),
                       "n_tests_familie": 1})

    df = pd.DataFrame(zeilen)
    df["p_holm"] = np.nan          # Familie aus einem Test - siehe Docstring
    df["signifikant"] = df["wilcoxon_p"] < ALPHA
    df.round(6).to_csv(OUT / "vergleich.csv", index=False)
    return df


def leakage_diagnose(folds: pd.DataFrame, baselines: pd.DataFrame) -> pd.DataFrame:
    """Beziffert, was das Tuning auf Wiederholung 0 kostet - wie in m02.

    Bei Macro-F1 ist GROSS besser, der Vorsprung ist also Verfahren minus
    Baseline. Ausfuehrliche Begruendung in `m02_menge.leakage_diagnose`.
    """
    basis = (baselines[baselines["modell"] == BASELINE_STUFE2]
             .set_index(["wiederholung", "fold"])["Macro-F1"])
    zeilen = []
    for name, g in folds.groupby("verfahren", sort=False):
        g = g.set_index(["wiederholung", "fold"])
        vorsprung = g[TESTMASS] - basis.reindex(g.index)
        w0 = vorsprung.xs(0, level="wiederholung")
        rest = vorsprung[vorsprung.index.get_level_values("wiederholung") > 0]
        zeilen.append({
            "zielgroesse": ZIELKLASSE, "verfahren": name, "mass": TESTMASS,
            "vorsprung_w0": float(w0.mean()), "n_w0": int(len(w0)),
            "vorsprung_w1_9": float(rest.mean()), "n_w1_9": int(len(rest)),
            "differenz": float(rest.mean() - w0.mean()),
            "differenz_in_std_folds": float((rest.mean() - w0.mean())
                                            / g[TESTMASS].std())})
    df = pd.DataFrame(zeilen)
    df.round(4).to_csv(OUT / "leakage_diagnose.csv", index=False)
    return df


def hold_out(panel: pd.DataFrame, parameter: pd.DataFrame,
             folds: pd.DataFrame) -> pd.DataFrame:
    """EINMALIG - wie in m02, mit Macro-F1 statt RMSE als Auswahlkriterium.

    Gewaehlt wird der Parametersatz des Folds mit dem HOECHSTEN Macro-F1 in
    Wiederholung 0. Zu berichten ist, dass dies EINE Messung an SECHS Einheiten
    ist (R-4).
    """
    param = _parameter_je_fold(parameter)
    dev, ho = entwicklung_und_holdout(panel)
    train, test = panel[dev], panel[ho]
    print(f"    Training auf {train['stadtteil'].nunique()} Stadtteilen "
          f"({len(train):,} Zeilen), Bewertung auf "
          f"{test['stadtteil'].nunique()} ({len(test):,} Zeilen)")

    w0 = folds[folds["wiederholung"] == 0]
    zeilen = []
    for name in VERFAHREN:
        g = w0[w0["verfahren"] == name]
        bester = int(g.loc[g["macro_f1"].idxmax(), "fold"])
        zeilen.append({**ein_lauf(name, param[(name, bester)], train, test),
                       "fold_der_parameter": bester,
                       "n_stadtteile_test": int(test["stadtteil"].nunique())})
    df = pd.DataFrame(zeilen)
    df.round(6).to_csv(OUT / "holdout.csv", index=False)
    return df


def main(argv: list[str]) -> int:
    if not PFAD_KLASSIFIKATION.exists():
        raise SystemExit(f"{PFAD_KLASSIFIKATION.relative_to(ROOT)} fehlt - "
                         f"erst 'python prep/build.py' ausfuehren.")
    if not (OUT / "baselines_klasse.csv").exists():
        raise SystemExit("results/klassifikation/baselines_klasse.csv fehlt - "
                         "erst 'python vorpruefung/v1_baselines.py' ausfuehren.")
    OUT.mkdir(parents=True, exist_ok=True)

    voll = pd.read_parquet(PFAD_KLASSIFIKATION)
    selten = selten_je_stadtteil(voll)

    # Wie in m02: ohne das Argument "holdout" sind die Hold-out-Zeilen ab hier
    # nicht mehr erreichbar.
    panel = voll[voll["ist_holdout"] == 0].reset_index(drop=True)
    print(f"  Entwicklung: {len(panel):,} Zeilen | "
          f"{panel['stadtteil'].nunique()} Stadtteile | Klassen "
          f"{dict(panel[ZIELKLASSE].value_counts())}\n")

    print("  Phase 1  Tuning")
    parameter = phase_tuning(panel, selten, neu="neutuning" in argv)
    print("\n  Phase 2  Bewertung")
    folds = phase_bewertung(panel, parameter, selten)
    print("\n  Phase 3  Aggregation")
    mittel = aggregiere(folds)
    print(mittel.to_string(index=False))
    auffaellig = mittel[mittel["parallel_abweichung_max"] > 0]
    if len(auffaellig):
        print("\n  HINWEIS zur Reproduzierbarkeit: Bei folgenden Verfahren "
              "haengt die Klassenvorhersage von der Kernzahl ab "
              "(docs/07_BEFUNDE.md, B-24).")
        for _, z in auffaellig.iterrows():
            print(f"    {z['verfahren']:<14} "
                  f"{z['parallel_abweichung_max']:.1%} abweichende Zeilen")
    print("\n  Phase 4  Vergleich")
    basislinien = pd.read_csv(OUT / "baselines_klasse.csv")
    v = vergleiche(folds, basislinien)
    print(v[v["teststufe"] == "wiederholung"]
          [["paarung", "rolle", "differenz_mittel", "gewonnene",
            "wilcoxon_p", "signifikant"]].to_string(index=False))

    print("\n  Diagnose zum Tuning auf Wiederholung 0 (B-21):")
    print(leakage_diagnose(folds, basislinien).to_string(index=False))

    if "holdout" in argv:
        print("\n  Phase 5  Hold-out - EINMALIGE Schlussbewertung")
        print(hold_out(voll, parameter, folds).to_string(index=False))
    else:
        print("\n  Hold-out unberuehrt. Fuer die Schlussbewertung:"
              "\n  python modelle/m03_struktur.py holdout")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
