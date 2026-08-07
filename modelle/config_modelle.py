"""
Konfiguration der Modellierung. Gegenstueck zu prep/config.py.

DIE TRENNLINIE zwischen beiden Dateien ist nicht "Daten gegen Modelle", sondern:

    prep/config.py        was in die Parquet-Dateien GESCHRIEBEN wird
    modelle/config_modelle.py   was nur beim RECHNEN gilt

Deshalb stehen Praediktoren, Zielgroessen, Klassen und N_FOLDS weiterhin in
prep/config.py und nicht hier: Sie bestimmen, welche Spalten die Datensaetze
haben und wie die Spalten `fold` und `ist_holdout` belegt sind. Die
Modellskripte LESEN diese Festlegungen, sie treffen sie nicht. Zoege man sie
hierher, gaebe es zwei Dateien, die zwingend uebereinstimmen muessen - und
genau das ist die Fehlerquelle, die der Aufbau vermeiden soll.

Was hier steht, beruehrt keine einzige Datei auf der Platte.

Bezug: docs/04_MODELLIERUNG.md
"""

# ==========================================================================
# 1  HYPERPARAMETER-SUCHE
# ==========================================================================
# Nur die SUCHRAEUME stehen hier - die Suche selbst laeuft im jeweiligen
# Modellskript. Ein separates Tuning-Skript wuerde die besten Parameter in eine
# Datei auslagern, die veralten kann, ohne dass es auffaellt.
#
# Gleiches Budget fuer alle Verfahren -> fairer Vergleich (Bergstra & Bengio
# 2012 zur Randomized Search, Probst et al. 2019 zu den RF-Raeumen).
TUNING_BUDGET = 50
RANDOM_STATE  = 42

SUCHRAEUME = {
    "ridge": {
        "alpha": ("loguniform", 1e-3, 1e3),
    },
    "random_forest": {
        "n_estimators":     ("int", 200, 1000),
        "max_depth":        ("choice", [None, 8, 12, 16, 24]),
        "min_samples_leaf": ("int", 1, 20),
        "max_features":     ("choice", ["sqrt", "log2", 0.3, 0.5, 1.0]),
    },
    "xgboost": {
        "n_estimators":     ("int", 200, 1000),
        "learning_rate":    ("loguniform", 0.01, 0.3),
        "max_depth":        ("int", 3, 10),
        "subsample":        ("uniform", 0.6, 1.0),
        "colsample_bytree": ("uniform", 0.6, 1.0),
        "reg_lambda":       ("loguniform", 1e-2, 1e2),
        # Exponent der Tweedie-Varianzfunktion, Var = mu^p (Decision Log #42).
        # 1 waere Poisson, 2 waere Gamma; dazwischen liegt der ueberdisperse
        # Bereich, in dem dieser Datensatz liegt (Dispersionsindex 62,8). Ihn
        # fest auf 1,5 zu setzen waere eine Konvention ohne Grund - also wird
        # er getunt wie jeder andere Hyperparameter.
        #
        # UNTERGRENZE 1,01 statt 1,1 (#45): Die Baseline ist ein Poisson-GLM,
        # also der Grenzfall p = 1. Ein Suchraum, der diesen Grenzfall
        # ausschliesst, verbietet XGBoost genau die Loesung, die dem
        # Referenzmodell entspricht - das waere eine Ungleichbehandlung, wie sie
        # #42 und #43 gerade beseitigt haben. `reg:tweedie` verlangt 1 < p < 2,
        # deshalb 1,01 und nicht 1,0.
        # Gilt nur in der REGRESSION; m03 entfernt ihn (Klassifikation).
        "tweedie_variance_power": ("uniform", 1.01, 1.9),
    },
    # ----------------------------------------------------------------------
    # DIE BASELINES STEHEN HIER NICHT - und zwar aus einem Grund (#45)
    # ----------------------------------------------------------------------
    # Beide Messlatten sind verallgemeinerte lineare Modelle mit dem fuer die
    # Datenform kanonischen Link, per unpenalisierter Maximum-Likelihood
    # angepasst: Poisson mit Offset fuer die Zaehldaten, multinomiales Logit
    # fuer die nominalen Klassen. Sie haben KEINEN freien Hyperparameter -
    # es gibt nichts zu suchen.
    #
    # Das ist keine Sparsamkeit gegenueber der Baseline, sondern die Definition
    # von Stufe 2: die einfachste Form, die zur Datenform passt. Ein Strafterm
    # waere eine Erweiterung dieser Form und braechte einen Regler mit sich,
    # den man dann waehlen muesste.
    #
    # Regel, die daraus folgt und fuer ALLE Modelle gilt: Was einen freien
    # Parameter hat, wird mit demselben Budget getunt. Was keinen hat, wird
    # angepasst. Kein Modell laeuft mit einer unbegruendeten Voreinstellung.
}

# ==========================================================================
# 2  WIEDERHOLTE SPLITS
# ==========================================================================
# Bei 29 Entwicklungsstadtteilen schwankt ein einzelner Fold massiv. Deshalb
# wird die Fold-Zuteilung mehrfach mit unterschiedlichem Versatz gebildet und
# ueber alle Laeufe gemittelt (docs/04_MODELLIERUNG.md, Abschnitt 2).
WIEDERHOLUNGEN = 10
