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

STAND: noch zu implementieren.

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
PRUEFAUFTRAEGE nach dem ersten Lauf
--------------------------------------------------------------------------
  - Schlaegt ueberhaupt ein Verfahren Stufe 2 (Macro-F1 0,290)? Wenn nein, ist
    das ein berichtbares Ergebnis und kein Fehler (docs/06_RISIKEN.md, R-2).
  - Hat jeder Fold Brand-Testfaelle? Erwartet: 13 · 9 · 6 · 3 · 2.
  - Liegt Accuracy deutlich ueber Macro-F1? Das ist normal und selbst ein
    Argument fuer die Metrikwahl - siehe docs/03_STAND.md.
"""
raise SystemExit(__doc__)
