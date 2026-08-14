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
#
# WARUM 100 UND NICHT 50 (Decision Log #50, 13.08.2026) - hergeleitet, nicht
# gewaehlt. Bergstra und Bengio (2012, S. 296) geben die geschlossene Form
#
#     P = 1 - (1 - v/V)^T
#
# fuer die Wahrscheinlichkeit an, mit T Zufallsziehungen mindestens einmal in
# einen Zielbereich vom relativen Volumen v/V zu treffen. Fuer v/V = 0,05:
#
#     T =  50  ->  92,3 %
#     T = 100  ->  99,4 %
#
# Der Ausdruck enthaelt die DIMENSION des Suchraums nicht. Genau deshalb
# bekommen Ridge mit einem und XGBoost mit sieben Hyperparametern dasselbe
# Budget, ohne dass dem hoeherdimensionalen Verfahren ein Nachteil entsteht -
# das ist die Antwort auf den naheliegenden Einwand, XGBoost braeuchte mehr.
#
# Die beiden Prozentzahlen sind UNSERE Anwendung ihrer Formel, nicht ihre
# Aussage. Die verbreitete Angabe "60 Ziehungen" steht nicht in dem Papier
# (im Volltext geprueft, 13.08.2026); dessen eigene Simulation rechnet mit 1 %.
#
# Der eigentliche Anlass ist #49: Ein weiterer Suchraum verduennt die gute
# Region. Wer den Raum oeffnet, muss das Budget mitziehen.
#
# GEMESSEN (tools/suchdiagnose.py, 13.08.2026): Die Verdopplung allein ist bei
# VIER VON FUENF Verfahren wirkungslos - Random Forest im Mengenstrang gewinnt
# exakt null, Ridge +0,0065, beide Strukturverfahren +0,0001. Nur XGBoost in
# der Regression gewinnt spuerbar. Genau so gehoert es in Kapitel 6.
TUNING_BUDGET = 100
RANDOM_STATE  = 42

# ==========================================================================
# SUCHRAEUME - vier davon am 13.08.2026 erweitert (Decision Log #49)
# ==========================================================================
# Erweitert wurde dort, wo der beste gefundene Wert AN der Grenze lag: In sechs
# von sieben geprueften Parametern lag mindestens ein Fold-Sieger ausserhalb.
# Die Begruendung ist eine Aussage ueber die SUCHE, nicht ueber das Ergebnis -
# das Optimum lag an der Grenze, also war die Grenze falsch gesetzt.
#
# NICHT erweitert wurden max_features, min_samples_leaf, subsample und
# colsample_bytree: Ihre Grenzen sind natuerlich (alle Merkmale, eine
# Beobachtung je Blatt, der ganze Datensatz) - dahinter existiert nichts. Dass
# der Random Forest dort ans Limit geht, ist ein BEFUND ueber das Verfahren.
# Ebenfalls nicht erweitert: n_estimators, nur ein Fold lag nahe der Grenze und
# es ist der groesste Laufzeittreiber.
SUCHRAEUME = {
    "ridge": {
        # War 1e-3 bis 1e3; zwei von fuenf Folds lagen ausserhalb (1052 und
        # 1,1e-05). Ridge ist das einzige Verfahren, das die Baseline gesichert
        # NICHT schlaegt - dort will man die Grenze nicht binden lassen.
        "alpha": ("loguniform", 1e-5, 1e5),
    },
    "random_forest": {
        "n_estimators":     ("int", 200, 1000),
        # War [None, 8, 12, 16, 24]. Zwei Aenderungen: nach oben erweitert
        # (Sieger bei 32, 32 und 48), und `None` ans ENDE gestellt. `None`
        # heisst unbegrenzte Tiefe, ist also faktisch der TIEFSTE Wert - an
        # erster Stelle verdrehte es jede Auswertung, die die Listenposition
        # als Tiefe liest (betraf auch Abbildung A8).
        "max_depth":        ("choice", [8, 12, 16, 24, 32, 48, None]),
        "min_samples_leaf": ("int", 1, 20),
        "max_features":     ("choice", ["sqrt", "log2", 0.3, 0.5, 1.0]),
    },
    "xgboost": {
        "n_estimators":     ("int", 200, 1000),
        "learning_rate":    ("loguniform", 0.01, 0.3),
        # War 3 bis 10 - DER wichtigste Fund. Im Strukturstrang waehlten am
        # 07.08. vier von fuenf Folds den Wert 3, also die Untergrenze. Mit
        # geoeffneter Grenze waehlen sie 2, 2 und 1. Das Modell wollte flacher
        # sein, als es durfte. Zusammen mit reg_lambda ist das die plausibelste
        # Erklaerung fuer R-2: Im Strukturstrang schlagen beide Baumverfahren
        # die Baseline in der Kreuzvalidierung und verlieren auf dem Hold-out,
        # waehrend die Baseline dort BESSER wird - Ueberanpassung, die der
        # Suchraum zum Teil erzwungen hat.
        "max_depth":        ("int", 1, 14),
        "subsample":        ("uniform", 0.6, 1.0),
        "colsample_bytree": ("uniform", 0.6, 1.0),
        # War 1e-2 bis 1e2; Sieger bei 3900, 2586, 570 und 340. Dieselbe
        # Richtung wie max_depth: das Modell will staerker regularisieren.
        "reg_lambda":       ("loguniform", 1e-4, 1e4),
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
