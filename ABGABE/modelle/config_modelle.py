"""
Konfiguration der Modellierung. Gegenstueck zu prep/config.py.

Eingang: nichts - reine Konstanten
Ausgang: Suchraeume, WIEDERHOLUNGEN, RANDOM_STATE, Suchbudget, Ergebnispfade

  - Die Trennlinie zwischen beiden Config-Dateien ist nicht "Daten gegen
    Modelle", sondern: prep/config.py haelt fest, was in die Parquet-Dateien
    GESCHRIEBEN wird - diese Datei nur, was beim RECHNEN gilt
  - Praediktoren, Zielgroessen, Klassen und N_FOLDS stehen deshalb weiterhin
    in prep/config.py: Sie bestimmen die Spalten der Datensaetze und die
    Belegung von `fold` und `ist_holdout`. Die Modellskripte LESEN diese
    Festlegungen, sie treffen sie nicht
  - Zoege man sie hierher, gaebe es zwei Dateien, die zwingend
    uebereinstimmen muessen - genau die Fehlerquelle, die der Aufbau vermeidet
  - Was hier steht, beruehrt keine einzige Datei auf der Platte
"""







# ==========================================================================
# 1  HYPERPARAMETER-SUCHE
# ==========================================================================
# Nur die SUCHRAEUME stehen hier; gesucht wird im jeweiligen Modellskript.
# Gleiches Budget fuer alle Verfahren -> fairer Vergleich (Bergstra & Bengio
# 2012, Probst et al. 2019).
#
# BUDGET 100 - hergeleitet, nicht gewaehlt. Bergstra & Bengio
# (2012, S. 296): P = 1 - (1 - v/V)^T. Fuer v/V = 0,05 gibt T = 50 -> 92,3 %
# und T = 100 -> 99,4 %. Die Formel enthaelt die DIMENSION nicht - deshalb
# bekommen Ridge (1 Parameter) und XGBoost (7) dasselbe Budget. Die beiden
# Prozentzahlen sind unsere Anwendung ihrer Formel, nicht ihre Aussage; die
# verbreitete Angabe "60 Ziehungen" steht nicht in dem Papier.
# Ein weiterer Suchraum verduennt die gute Region.
TUNING_BUDGET = 100
RANDOM_STATE  = 42


# ==========================================================================
# SUCHRAEUME
# ==========================================================================
# Erweitert wurde, wo der beste Wert AN der Grenze lag.
# Das ist eine Aussage ueber die SUCHE, nicht ueber das Ergebnis:
# Lag das Optimum an der Grenze, war die Grenze falsch gesetzt.
#
# NICHT erweitert: max_features, min_samples_leaf, subsample, colsample_bytree
# - ihre Grenzen sind natuerlich (alle Merkmale, eine Beobachtung je Blatt,
# ganzer Datensatz), dahinter existiert nichts.
# Auch n_estimators bleibt: es ist der groesste Laufzeittreiber.
SUCHRAEUME = {
    "ridge": {

        "alpha": ("loguniform", 1e-5, 1e5),
    },
    "random_forest": {
        "n_estimators":     ("int", 200, 1000),

        # `None` steht am ENDE - unbegrenzte Tiefe ist faktisch der
        # TIEFSTE Wert und verdrehte an erster Stelle jede Auswertung, die die
        # Listenposition als Tiefe liest.
        "max_depth":        ("choice", [8, 12, 16, 24, 32, 48, None]),
        "min_samples_leaf": ("int", 1, 20),
        "max_features":     ("choice", ["sqrt", "log2", 0.3, 0.5, 1.0]),
    },
    "xgboost": {
        "n_estimators":     ("int", 200, 1000),
        "learning_rate":    ("loguniform", 0.01, 0.3),





        "max_depth":        ("int", 1, 14),
        "subsample":        ("uniform", 0.6, 1.0),
        "colsample_bytree": ("uniform", 0.6, 1.0),


        "reg_lambda":       ("loguniform", 1e-4, 1e4),
        # Exponent der Tweedie-Varianzfunktion, Var = mu^p. 1 = Poisson,
        # 2 = Gamma; dazwischen der ueberdisperse Bereich dieses Datensatzes.
        # Fest auf 1,5 waere eine Konvention ohne Grund, also getunt.
        # UNTERGRENZE 1,01: Die Baseline ist ein Poisson-GLM,
        # der Grenzfall p = 1. Ein Suchraum ohne ihn verboete XGBoost genau die
        # Loesung des Referenzmodells. reg:tweedie verlangt 1 < p < 2.
        # Gilt nur in der REGRESSION; m03 entfernt ihn.
        "tweedie_variance_power": ("uniform", 1.01, 1.9),
    },
    # ----------------------------------------------------------------------
    # DIE BASELINES STEHEN HIER NICHT: Beide sind GLM mit kanonischem
    # Link, unpenalisiert per Maximum-Likelihood angepasst - sie haben KEINEN
    # freien Hyperparameter, es gibt nichts zu suchen. Das ist keine
    # Sparsamkeit, sondern die Definition von Stufe 2: die einfachste Form,
    # die zur Datenform passt. Die Regel dahinter gilt fuer alle Modelle - was
    # einen freien Parameter hat, wird mit demselben Budget getunt; was keinen
    # hat, wird angepasst. Kein Modell laeuft mit einer unbegruendeten
    # Voreinstellung.
    # ----------------------------------------------------------------------
}

# ==========================================================================
# 2  WIEDERHOLTE SPLITS
# ==========================================================================
# Bei 30 Entwicklungsstadtteilen schwankt ein einzelner Fold massiv. Deshalb
# wird die Fold-Zuteilung mehrfach mit unterschiedlichem Versatz gebildet und
# ueber alle Laeufe gemittelt.
WIEDERHOLUNGEN = 10
