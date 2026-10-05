"""
Konfiguration der Modellierung. Gegenstueck zu prep/config.py.

Input:   nichts - reine Konstanten
Output:  Suchraeume, WIEDERHOLUNGEN, RANDOM_STATE, Suchbudget, Ergebnispfade

  - hier nur, was beim RECHNEN gilt; was in die Parquet-Dateien geschrieben
    wird (Praediktoren, Zielgroessen, Klassen, N_FOLDS), steht in
    prep/config.py - keine zweite Stelle fuer dieselbe Festlegung
"""


# ==========================================================================
# 1  HYPERPARAMETER-SUCHE
# ==========================================================================
# Nur die Suchraeume; gesucht wird im Modellskript. Gleiches Budget fuer alle
# Verfahren (Bergstra & Bengio 2012, Probst et al. 2019).
# Budget 100 hergeleitet: P = 1 - (1 - v/V)^T (Bergstra & Bengio 2012, S. 296);
# v/V = 0,05: T = 50 -> 92,3 %, T = 100 -> 99,4 % (eigene Anwendung der Formel).
# Unabhaengig von der Dimension -> Ridge (1 Parameter) wie XGBoost (7).
TUNING_BUDGET = 100
RANDOM_STATE  = 42


# ==========================================================================
# SUCHRAEUME
# ==========================================================================
# Erweitert, wo der beste Wert an der Grenze lag (Aussage ueber die Suche).
# Nicht erweitert: max_features, min_samples_leaf, subsample, colsample_bytree
# (natuerliche Grenzen) und n_estimators (Laufzeit).
SUCHRAEUME = {
    "ridge": {

        "alpha": ("loguniform", 1e-5, 1e5),
    },
    "random_forest": {
        "n_estimators":     ("int", 200, 1000),

        # `None` (unbegrenzt = tiefster Wert) am Ende, damit die Listenposition
        # als Tiefe lesbar bleibt.
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
        # Tweedie-Exponent p (Var = mu^p; 1 Poisson, 2 Gamma), getunt statt fest
        # 1,5. Untergrenze 1,01 schliesst die Loesung der Poisson-Baseline ein
        # (reg:tweedie verlangt 1 < p < 2). Nur Regression; m03 entfernt ihn.
        "tweedie_variance_power": ("uniform", 1.01, 1.9),
    },
    # Keine Baselines: GLM mit kanonischem Link, ohne freien Hyperparameter.
    # Regel fuer alle Modelle: freie Parameter mit gleichem Budget tunen,
    # sonst nur anpassen.
}

# ==========================================================================
# 2  WIEDERHOLTE SPLITS
# ==========================================================================
# Ein einzelner Fold schwankt bei 30 Stadtteilen stark -> mehrere
# Fold-Zuteilungen, gemittelt ueber alle Laeufe.
WIEDERHOLUNGEN = 10
