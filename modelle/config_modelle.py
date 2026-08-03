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
    },
    # NegBin-Baseline: kein Tuning, sie ist die Referenz und keine Kandidatin.
}

# ==========================================================================
# 2  WIEDERHOLTE SPLITS
# ==========================================================================
# Bei 29 Entwicklungsstadtteilen schwankt ein einzelner Fold massiv. Deshalb
# wird die Fold-Zuteilung mehrfach mit unterschiedlichem Versatz gebildet und
# ueber alle Laeufe gemittelt (docs/04_MODELLIERUNG.md, Abschnitt 2).
WIEDERHOLUNGEN = 10
