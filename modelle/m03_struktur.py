"""
Verfahrensvergleich fuer die STRUKTUR der Einsatzlast.

    python modelle/m03_struktur.py            Tuning, Bewertung, Aggregation, Vergleich
    python modelle/m03_struktur.py holdout    zusaetzlich die einmalige Schlussbewertung
    python modelle/m03_struktur.py holdout --weiter   Phase 1+2 aus results/ uebernehmen

Eingang: data/processed/klassifikation.parquet
Ausgang: results/klassifikation/struktur_folds.csv, struktur_mittel.csv,
         tuning.csv, vergleich.csv, holdout.csv

  - Eine Zielgroesse (dominante_einsatzart, vier ungeordnete Klassen) x zwei
    Verfahren (Random Forest, XGBoost) x 10 Wiederholungen x 5 Folds = 100
    Laeufe. Ridge hat auf einer nominalen Zielgroesse keine Entsprechung (#31)
  - AUFBAU: spiegelt m02_menge.py - dieselben Funktionen, dieselbe
    Reihenfolge, dieselben Fallstricke. Hier stehen nur die Unterschiede
  - Guetemasse Macro-F1 (Hauptmass) und Macro-AUROC, Accuracy nachrichtlich;
    getunt wird auf f1_macro. Stufe-2-Baseline ist die multinomiale
    logistische Regression (#33), nicht die Mehrheitsklasse
  - Holm entfaellt: Es gibt genau EINEN sekundaeren Test (RF gegen XGBoost),
    und Regression und Klassifikation sind getrennte Testfamilien (B-6)

DREI FALLSTRICKE, die es in m02 nicht gibt
  1  KLASSENGEWICHTE statt Resampling - class_weight="balanced" beim RF,
     sample_weight beim XGBClassifier. Kein SMOTE, kein Over- oder
     Undersampling; das waere ein Eingriff in die Datenverteilung
  2  LABEL-ENCODER EINMAL GLOBAL, nicht je Fold. Sonst verschiebt sich das
     Mapping in Folds ohne eine Klasse und die Wahrscheinlichkeitsspalten
     zeigen auf die falschen Klassen
  3  MACRO-AUROC KANN UNDEFINIERT SEIN. Dann als FEHLEND fuehren, nicht durch
     null ersetzen. zero_division=0 bei Macro-F1 muss gesetzt bleiben

PRUEFAUFTRAEGE nach jedem Lauf
  - Schlaegt ueberhaupt ein Verfahren Stufe 2? Wenn nein, ist das ein
    berichtbares Ergebnis und kein Fehler (R-2)
  - Hat jeder Fold Brand-Testfaelle? In Wiederholung 0 erwartet 13/9/6/3/2
  - Accuracy deutlich ueber Macro-F1? Normal, und selbst ein Argument fuer
    die Metrikwahl
  - Laeufe ohne definierte Macro-AUROC? Erwartet keiner
  - Zeilenzahl: 10 in tuning.csv, 100 in struktur_folds.csv
  - Hold-out unberuehrt, wenn ohne Argument gestartet?
  - ueberanpassung_macro_f1 (#51): Dieser Strang ist der, in dem
    Kreuzvalidierung und Hold-out sich widersprechen (R-2, B-42) - hier
    entscheidet sich, ob Ueberanpassung die Erklaerung ist
  - Gegenueber archiv/2026-08-14_budget50/ gesunken? Nur fuer 07_BEFUNDE.md;
    nach #52 wird kein Vorher-Nachher berichtet

Ausfuehrliche Fassung: docs/08_FUNKTIONSDOKUMENTATION.md
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

# Muss zu vorpruefung/v1_baselines.LOGREG passen - der Name filtert die Spalte
# `modell` in baselines_klasse.csv, ein Tippfehler liefert also stillschweigend
# eine leere Vergleichsmenge. `hold_out()` importiert die Konstante direkt.
BASELINE_STUFE2 = "Multinomiale logistische Regression"
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
    """Baut die ungetunte Pipeline. Kein Scaler, beide Verfahren sind Baeume.

    Ein:  Verfahrensname, optional n_jobs
    Aus:  Schaetzer ohne Hyperparameter

    - n_jobs steuert nur die Parallelisierung, nicht das Ergebnis; voreingestellt
      einkernig
    - Fallstrick 1: Die Klassenverteilung ist stark schief (79 % Fehlalarm).
      Statt zu resampeln bekommen beide Verfahren Gewichte
    - Random Forest ueber den Hyperparameter class_weight="balanced", XGBoost
      ueber sample_weight beim Fit - das Verfahren kennt keinen solchen Parameter
    - Wirkung in beiden Faellen gleich: seltene Klassen zaehlen mehr, ohne dass
      eine Zeile dupliziert oder geloescht wird
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
    """Uebersetzt SUCHRAEUME in scipy-Verteilungen, ohne Praefix.

    Ein:  Verfahrensname
    Aus:  dict Parametername -> Verteilung

    - beide Verfahren sind nackte Schaetzer statt Pipelines, weil keine
      Skalierung noetig ist
    - die Raeume sind dieselben wie in der Regression: es wechselt nur die
      Verlustfunktion, nicht der Ensemble-Mechanismus
    - Ausnahme: tweedie_variance_power steuert die Verlustfunktion der REGRESSION
      (#42) und ist bei multi:softprob bedeutungslos. XGBoost naehme ihn an und
      ignorierte ihn - ein Sechstel des Budgets auf einer wirkungslosen Dimension
      und eine bedeutungslose Zahl in tuning.csv
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
    """Klassennamen -> Integer 0..3 nach der globalen Reihenfolge KLASSEN.

    Ein:  Reihe mit Klassennamen
    Aus:  Integer-Array

    - Fallstrick 2: Das Mapping haengt nicht von den gerade vorliegenden Daten ab
    - ein je Fold gefitteter LabelEncoder verschoebe in einem Fold ohne Brand die
      Zahlen; die Wahrscheinlichkeitsspalten zeigten danach auf die falschen
      Klassen, ohne Fehlermeldung
    """
    index = {k: i for i, k in enumerate(KLASSEN)}
    unbekannt = set(y.unique()) - set(index)
    assert not unbekannt, f"Unbekannte Klassen im Datensatz: {unbekannt}"
    return y.map(index).to_numpy()


def _gewichte(y_int: np.ndarray) -> np.ndarray:
    """class_weight="balanced" von Hand, fuer XGBoost.

    Ein:  Integer-Labels des Trainings
    Aus:  Gewichtsvektor gleicher Laenge
    """
    from sklearn.utils.class_weight import compute_sample_weight
    return compute_sample_weight("balanced", y_int)


# ---------------------------------------------------------------------------
# BAUSTEIN 2  Das Tuning
# ---------------------------------------------------------------------------
def tune(name: str, train: pd.DataFrame) -> dict:
    """Wie m02.tune, aber mit f1_macro als Scoring.

    Ein:  Trainingsrahmen des Folds, Verfahren
    Aus:  die Parameter als dict

    - der Fallstrick aus m02 gilt unveraendert: Der innere CV muss nach Stadtteil
      gruppieren, sonst stehen dieselben 132 Zeilen in innerem Training und
      innerer Validierung
    - f1_macro statt Accuracy, weil die Mehrheitsklasse allein ueber 0,8 Accuracy
      erreicht; ein darauf optimiertes Tuning waehlte Modelle, die die drei
      seltenen Klassen ignorieren
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
             test: pd.DataFrame, auch_parallel: bool = False,
             mit_vorhersagen: bool = False) -> dict:
    """Ein Fit, eine Vorhersage, mit Zeitmessung - eine Zeile fuer die CSV.

    Ein:  Trainings- und Testrahmen, Verfahren, Parameter, auch_parallel
    Aus:  dict mit Macro-F1, Macro-AUROC, Accuracy, Laufzeiten,
          Extrapolationsanteil

    - die Zeit wird um fit und predict herum gemessen, einkernig fuer beide
      Verfahren
    - die Wahrscheinlichkeiten fuer die AUROC kommen aus einem zweiten Aufruf,
      damit inferenz_sekunden die reine Klassenvorhersage misst
    - auch_parallel=True misst denselben Fit zusaetzlich ueber alle Kerne; im Lauf
      steht das Argument in jedem Aufruf auf True
    """
    from sklearn.metrics import accuracy_score, f1_score

    X_tr, X_te = train[MERKMALE].astype(float), test[MERKMALE].astype(float)
    y_tr, y_te = kodiere(train[ZIELKLASSE]), kodiere(test[ZIELKLASSE])

    def fitte(kerne: int):
        """Fittet einen Schaetzer mit den Klassengewichten des Verfahrens.

        Ein:  Schaetzer, Merkmalsmatrix, Integer-Labels
        Aus:  der gefittete Schaetzer

        - XGBoost bekommt sample_weight beim Fit
        - der Random Forest hat die Gewichte bereits als Hyperparameter
          (Fallstrick 1)
        """
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

    # UEBERANPASSUNGSNACHWEIS, ergaenzt 14.08.2026 - wie in m02, siehe dort.
    # Eine zusaetzliche Vorhersage auf den Trainingsstadtteilen, kein zweiter
    # Fit, nach der Zeitmessung. Hier ist der Wert besonders wichtig: Der
    # Strukturstrang ist der, in dem Kreuzvalidierung und Hold-out sich
    # widersprechen (R-2, B-42), und die Baseline auf dem Hold-out BESSER wird,
    # waehrend beide Baumverfahren einbrechen.
    y_hat_tr = modell.predict(X_tr)

    ergebnis = {
        "verfahren": name, "zielgroesse": ZIELKLASSE,
        "train_sekunden_parallel": train_par,
        "inferenz_sekunden_parallel": inferenz_par,
        "parallel_abweichung": abweichung,
        "macro_f1": float(f1_score(y_te, y_hat, average="macro", zero_division=0)),
        "macro_f1_train": float(f1_score(y_tr, y_hat_tr, average="macro",
                                         zero_division=0)),
        "accuracy_train": float(accuracy_score(y_tr, y_hat_tr)),
        "macro_auroc": _macro_auroc(y_te, modell.predict_proba(X_te),
                                    list(modell.classes_)),
        "accuracy": float(accuracy_score(y_te, y_hat)),
        "train_sekunden": train_sek, "inferenz_sekunden": inferenz_sek,
        "n_train": len(train), "n_test": len(test),
        "n_brand_test": int((test[ZIELKLASSE] == SELTENE_KLASSE).sum()),
        "extrapolationsanteil": extrapolationsanteil(train, test),
    }

    # VORHERSAGEN JE ZEILE (nur von phase_bewertung angefordert): Traegt die
    # Konfusionsmatrix und die klassenweisen F1-Werte, ohne die ein Macro-F1
    # aus vier Klassen nicht lesbar ist. Kein neuer Modelllauf noetig.
    if mit_vorhersagen:
        ergebnis["_vorhersagen"] = pd.DataFrame({
            "stadtteil": test["stadtteil"].to_numpy(),
            "jahr_monat": test["jahr_monat"].to_numpy(),
            "verfahren": name,
            "y": np.asarray(y_te), "y_hat": np.asarray(y_hat),
        })
    return ergebnis


def extrapolationsanteil(train: pd.DataFrame, test: pd.DataFrame) -> float:
    """Anteil der Testzeilen ausserhalb des Trainings-Wertebereichs.

    Ein:  Trainings- und Testmatrix
    Aus:  Anteil zwischen 0 und 1

    - wortgleich zu m02_menge, bewusst dupliziert statt importiert
    - ein gemeinsames Hilfsmodul fuer zwei Aufrufer braechte mehr Indirektion als
      Ersparnis, und m03 soll unabhaengig von m02 lauffaehig bleiben
    """
    lo, hi = train[MERKMALE].min(), train[MERKMALE].max()
    aussen = ((test[MERKMALE] < lo) | (test[MERKMALE] > hi)).any(axis=1)
    return float(aussen.mean())


def _gepaart(a: np.ndarray, b: np.ndarray) -> dict:
    """Gepaarter Wilcoxon samt der Kennzahlen, die ohne p-Wert tragen.

    Ein:  zwei gepaarte Wertereihen (a = Verfahren, b = Gegner)
    Aus:  dict mit p-Wert, mittlerer Differenz, Konfidenzintervall, Siegen

    - wortgleich zu m02_menge
    - bei Macro-F1 ist gross besser; die Aufrufstelle dreht die Argumente
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
    """Macro-AUROC (One-vs-Rest), NaN wenn eine Klasse im Test fehlt.

    Ein:  wahre Labels, Wahrscheinlichkeitsmatrix, Klassenreihenfolge des Modells
    Aus:  Zahl oder NaN

    - Fallstrick 3: kein Ersatz durch 0,5 - ein erfundener Wert sieht wie eine
      Messung aus und zieht den Mittelwert nach unten
    - labels=klassen_modell bringt die Wahrscheinlichkeitsspalten in die
      Reihenfolge, die das Modell benutzt hat (zweiter Teil von Fallstrick 2)
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
def phase_tuning(panel: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """Phase 1: je Verfahren und Fold einmal tune() auf Wiederholung 0.

    Ein:  Panel der Entwicklungsstadtteile
    Aus:  tuning.csv mit 10 Zeilen

    - wie in m02 wird nichts STILL wiederverwendet: tuning.csv ist ein Ergebnis
      dieses Laufs, kein Eingang. Einzige Ausnahme ist der ausdrueckliche
      Schalter --weiter, siehe uebernehmen()
    """
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
    """Wandelt NumPy-Skalare in native Typen, wortgleich zu m02_menge.

    Ein:  Parameter-dict aus best_params_
    Aus:  dasselbe dict mit int/float

    - np.int64 erbt nicht von int; ohne die Wandlung wuerde aus 287 die
      Zeichenkette "287" und set_params braeche nach dem Tuning ab (B-23)
    """
    return {schluessel: (wert.item() if isinstance(wert, np.generic) else wert)
            for schluessel, wert in p.items()}


def _parameter_je_fold(parameter: pd.DataFrame) -> dict:
    """Liest tuning.csv als Nachschlagetabelle.

    Ein:  tuning.csv als Datenrahmen
    Aus:  {(verfahren, fold): Parameter-dict}
    """
    return {(z["verfahren"], int(z["fold"])): json.loads(z["parameter_json"])
            for _, z in parameter.iterrows()}


def phase_bewertung(panel: pd.DataFrame, parameter: pd.DataFrame,
                    selten: pd.Series) -> pd.DataFrame:
    """Phase 2: 10 Wiederholungen x 5 Folds x 2 Verfahren = 100 Zeilen.

    Ein:  Panel, Parametertabelle aus Phase 1
    Aus:  struktur_folds.csv und struktur_vorhersagen.parquet mit einer
          Zeile je Vorhersage

    - trainiert wird je Fold mit einem frischen Modell auf allen
      Trainingsstadtteilen, nicht mit best_estimator_ aus dem Tuning
    """
    param = _parameter_je_fold(parameter)
    zeilen, vorhersagen = [], []
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(panel, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            for name in VERFAHREN:
                z = ein_lauf(name, param[(name, k)], train, test,
                             auch_parallel=True, mit_vorhersagen=True)
                vorhersagen.append(z.pop("_vorhersagen")
                                    .assign(wiederholung=w, fold=k))
                zeilen.append({"wiederholung": w, "fold": k, **z})
        print(f"    Wiederholung {w}: {len(zeilen):>3} Laeufe")
    df = pd.DataFrame(zeilen)
    spalten = ["zielgroesse", "verfahren", "wiederholung", "fold",
               "macro_f1", "macro_auroc", "accuracy",
               "macro_f1_train", "accuracy_train",
               "train_sekunden", "inferenz_sekunden",
               "train_sekunden_parallel", "inferenz_sekunden_parallel",
               "parallel_abweichung",
               "n_train", "n_test", "n_brand_test", "extrapolationsanteil"]
    df = df[spalten]
    df.to_csv(OUT / "struktur_folds.csv", index=False)
    pd.concat(vorhersagen, ignore_index=True).to_parquet(
        OUT / "struktur_vorhersagen.parquet", index=False)
    return df


MASSE = ["macro_f1", "macro_auroc", "accuracy",
         "train_sekunden", "inferenz_sekunden"]
MASSE_PARALLEL = ["train_sekunden_parallel", "inferenz_sekunden_parallel"]


def aggregiere(folds: pd.DataFrame) -> pd.DataFrame:
    """Phase 3: zweistufig mitteln, wie in m02.

    Ein:  struktur_folds.csv als Datenrahmen
    Aus:  struktur_mittel.csv

    - massgeblich ist std_wiederholungen, nicht std_folds (R-5)
    - die 50 Fold-Ergebnisse sind dieselben 30 Stadtteile in zehn Gruppierungen
    """
    schluessel = ["zielgroesse", "verfahren"]
    g = folds.groupby(schluessel, sort=False)
    z = g[MASSE].mean().add_suffix("_mean")
    z = z.join(g[MASSE].std().add_suffix("_std_folds"))
    je_wdh = folds.groupby(schluessel + ["wiederholung"], sort=False)[MASSE].mean()
    z = z.join(je_wdh.groupby(schluessel, sort=False).std()
                     .add_suffix("_std_wiederholungen"))
    z = z.join(g[MASSE_PARALLEL].mean().add_suffix("_mean"))
    z["parallel_gewinn"] = (z["train_sekunden_mean"]
                            / z["train_sekunden_parallel_mean"])
    z["parallel_abweichung_max"] = g["parallel_abweichung"].max()
    # UEBERANPASSUNG - wie in m02. Positiv heisst: im Training deutlich besser
    # als auf unbekannten Stadtteilen.
    z = z.join(g[["macro_f1_train", "accuracy_train"]].mean())
    z["ueberanpassung_macro_f1"] = z["macro_f1_train"] - z["macro_f1_mean"]

    z = z.join(g[["n_brand_test", "extrapolationsanteil"]].mean())
    z = z.join(g[["macro_auroc"]].apply(lambda s: int(s["macro_auroc"].isna().sum()))
                .rename("n_auroc_fehlend"))
    spalten = [f"{m}{s}" for m in MASSE for s in
               ("_mean", "_std_folds", "_std_wiederholungen")]
    spalten += ([f"{m}_mean" for m in MASSE_PARALLEL]
                + ["parallel_gewinn", "parallel_abweichung_max"])
    z = z[spalten + ["macro_f1_train", "accuracy_train",
                     "ueberanpassung_macro_f1", "n_brand_test",
                     "extrapolationsanteil", "n_auroc_fehlend"]].reset_index()
    z.round(4).to_csv(OUT / "struktur_mittel.csv", index=False)
    return z


def vergleiche(folds: pd.DataFrame, baselines: pd.DataFrame) -> pd.DataFrame:
    """Phase 4: gepaarter Wilcoxon auf Macro-F1.

    Ein:  struktur_folds.csv, Baseline-Laeufe aus v1_baselines.py
    Aus:  vergleich.csv

    - zwei primaere Tests (je Verfahren gegen Stufe 2), ein sekundaerer (RF gegen
      XGBoost)
    - kein Holm: eine Korrektur ueber einen einzigen Test ist die Identitaet.
      p_holm bleibt leer, n_tests_familie steht auf 1
    - Regression und Klassifikation sind getrennte Testfamilien (B-6); in
      Kapitel 7 zu benennen, weil dieser Vergleich ungekorrigiert gegen
      alpha = 0,05 laeuft
    - Teststufen wie in m02: `wiederholung` (n = 10) primaer, `lauf` (n = 50) als
      gekennzeichnete Sensitivitaet (B-5)
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
            """Legt eine Vergleichszeile fuer vergleich.csv an.

            Ein:  Rolle, Teststufe, Verfahren, Gegner, Wertereihen
            Aus:  dict mit Testergebnis und Kennzahlen
            """
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
    """Beziffert, was das Tuning auf Wiederholung 0 kostet.

    Ein:  struktur_folds.csv, Baseline-Laeufe
    Aus:  Datenrahmen mit dem Vorsprung in W0 gegen W1-9

    - bei Macro-F1 ist gross besser; der Vorsprung ist Verfahren minus Baseline
    - ausfuehrliche Begruendung in m02_menge.leakage_diagnose
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
    """Einmalige Schlussbewertung, mit Macro-F1 als Auswahlkriterium.

    Ein:  vollstaendiges Panel, Parametertabelle aus Phase 1
    Aus:  holdout.csv

    - gewaehlt wird der Parametersatz des Folds mit dem hoechsten Macro-F1 in
      Wiederholung 0
    - das Baseline-Modell stammt aus v1_baselines.logit_glm(); seit 10.08.2026
      nicht mehr hier nachgebaut
    - ohne Bezugspunkt ist ein Macro-F1 von 0,33 keine Aussage (B-38)
    - zu berichten ist, dass dies EINE Messung an SECHS Einheiten ist: kein
      Mittelwert, keine Streuung (R-4)
    """
    param = _parameter_je_fold(parameter)
    dev, ho = entwicklung_und_holdout(panel)
    train, test = panel[dev], panel[ho]
    print(f"    Training auf {train['stadtteil'].nunique()} Stadtteilen "
          f"({len(train):,} Zeilen), Bewertung auf "
          f"{test['stadtteil'].nunique()} ({len(test):,} Zeilen)")

    # Wie in m02 gehoeren beide Baselines dazu - ohne Bezugspunkt ist ein
    # Macro-F1 von 0,33 keine Aussage (docs/07_BEFUNDE.md, B-38).
    #
    # EINE SPEZIFIKATION, ZWEI AUFRUFER (10.08.2026). Bis dahin baute diese
    # Funktion das Logit selbst nach - dieselben vier Argumente, an zwei Orten
    # aufgeschrieben. Aendert jemand eines davon, misst die Kreuzvalidierung
    # still gegen ein anderes Modell als die Schlussbewertung, und keine
    # Pruefung schlaegt an. m02 war immer richtig gebaut und holt `poisson_glm`
    # aus derselben Datei; hier fehlte genau das.
    from sklearn.metrics import accuracy_score, f1_score
    from v1_baselines import LOGREG, logit_glm

    X_te = test[MERKMALE].astype(float)
    y_tr, y_te = train[ZIELKLASSE], test[ZIELKLASSE]
    t = time.perf_counter()
    logreg = logit_glm(train)
    baseline_sek = time.perf_counter() - t
    haeufigste = y_tr.value_counts().idxmax()

    zeilen = []
    for stufe, modell, y_hat, proba in (
            (1, f"Mehrheitsklasse ({haeufigste})",
             np.full(len(y_te), haeufigste), None),
            (2, LOGREG, logreg.predict(X_te), logreg.predict_proba(X_te))):
        zeilen.append({
            "verfahren": modell, "zielgroesse": ZIELKLASSE, "stufe": stufe,
            "macro_f1": float(f1_score(y_te, y_hat, average="macro",
                                       zero_division=0)),
            # FALLSTRICK 2 auch hier: Die Wahrscheinlichkeitsspalten der
            # logistischen Regression stehen in alphabetischer Reihenfolge
            # ihrer Klassennamen, nicht in der von KLASSEN. Erst umsortieren,
            # dann bewerten - `roc_auc_score` verlangt aufsteigend sortierte
            # Labels und liefert sonst gar nichts (B-38).
            "macro_auroc": (np.nan if proba is None else _macro_auroc(
                kodiere(y_te),
                proba[:, [list(logreg.classes_).index(c) for c in KLASSEN]],
                list(range(len(KLASSEN))))),
            "accuracy": float(accuracy_score(y_te, y_hat)),
            "train_sekunden": round(baseline_sek, 4) if stufe == 2 else 0.0,
            "n_train": len(train), "n_test": len(test),
            "n_brand_test": int((y_te == SELTENE_KLASSE).sum()),
            "n_stadtteile_test": int(test["stadtteil"].nunique())})

    w0 = folds[folds["wiederholung"] == 0]
    for name in VERFAHREN:
        g = w0[w0["verfahren"] == name]
        bester = int(g.loc[g["macro_f1"].idxmax(), "fold"])
        zeilen.append({**ein_lauf(name, param[(name, bester)], train, test),
                       "stufe": 3, "fold_der_parameter": bester,
                       "n_stadtteile_test": int(test["stadtteil"].nunique())})
    df = pd.DataFrame(zeilen)
    df.round(6).to_csv(OUT / "holdout.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# Anschlusslauf: Phase 1 und 2 uebernehmen statt neu rechnen
# ---------------------------------------------------------------------------
def uebernehmen(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Liest Tuning und Bewertung aus results/, statt sie neu zu rechnen.

    Ein:  Entwicklungspanel; liest tuning.csv und struktur_folds.csv
    Aus:  dieselben zwei Datenrahmen, die Phase 1 und Phase 2 zurueckgeben

    - baugleich zu m02_menge.uebernehmen(); die Begruendung steht dort
    - NUR ueber den Schalter --weiter erreichbar; ohne ihn rechnet das Skript
      unveraendert alles neu
    """
    konfig = Path(__file__).resolve().parent / "config_modelle.py"
    fehlend = [d for d in ("tuning.csv", "struktur_folds.csv")
               if not (OUT / d).exists()]
    if fehlend:
        raise SystemExit(f"--weiter: {', '.join(fehlend)} fehlt in "
                         f"{OUT.relative_to(ROOT)}. Erst ohne --weiter laufen.")
    parameter = pd.read_csv(OUT / "tuning.csv")
    folds = pd.read_csv(OUT / "struktur_folds.csv")
    erwartet = len(VERFAHREN) * WIEDERHOLUNGEN * N_FOLDS
    n = int(folds["n_train"].iloc[0] + folds["n_test"].iloc[0])
    juenger = max(konfig.stat().st_mtime, PFAD_KLASSIFIKATION.stat().st_mtime)
    for erfuellt, meldung in (
            (juenger <= (OUT / "tuning.csv").stat().st_mtime,
             "klassifikation.parquet oder config_modelle.py ist neuer als "
             "tuning.csv - Daten, Suchraum oder Budget koennen andere sein"),
            (len(folds) == erwartet,
             f"struktur_folds.csv hat {len(folds)} statt {erwartet} Zeilen"),
            (n == len(panel),
             f"die uebernommenen Laeufe stammen aus einem Panel mit {n} "
             f"Zeilen, das aktuelle hat {len(panel)}"),
            (set(parameter["verfahren"]) == set(VERFAHREN),
             f"tuning.csv fuehrt {sorted(set(parameter['verfahren']))} "
             f"statt {sorted(VERFAHREN)}"),
            (sorted(int(f) for f in parameter["fold"].unique())
             == list(range(1, N_FOLDS + 1)),
             f"tuning.csv fuehrt die Folds "
             f"{sorted(int(f) for f in parameter['fold'].unique())}")):
        if not erfuellt:
            raise SystemExit(f"--weiter abgebrochen: {meldung}. "
                             f"Ohne --weiter neu rechnen.")
    print(f"  Phase 1+2 uebernommen aus {OUT.relative_to(ROOT)}: "
          f"{len(parameter)} Parametersaetze, {len(folds)} Laeufe, "
          f"Panel {n} Zeilen")
    return parameter, folds


def main(argv: list[str]) -> int:
    """Faehrt die vier Phasen und schreibt alle Ergebnisdateien.

    Ein:  klassifikation.parquet; Argument "holdout" haengt die
          Schlussbewertung an, "--weiter" uebernimmt Phase 1 und 2 aus
          results/klassifikation/
    Aus:  tuning.csv, struktur_folds.csv, struktur_mittel.csv, vergleich.csv,
          leakage_diagnose.csv, optional holdout.csv; Exitcode

    - ohne das Argument werden die Hold-out-Zeilen zu Beginn unwiderruflich
      herausgefiltert
    """
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

    if "--weiter" in argv:
        parameter, folds = uebernehmen(panel)
    else:
        print("  Phase 1  Tuning")
        parameter = phase_tuning(panel, selten)
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
