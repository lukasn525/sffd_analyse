# Ergebnisverzeichnis

Welche Datei hinter welcher Tabelle der Arbeit steht. Pfade relativ zu
`results/`.

| Tabelle in der Arbeit | Datei | erzeugt von |
|---|---|---|
| Lage und Streuung der beiden Mengenzielgrößen | `deskriptiv/verteilung.csv` | `prep/deskriptiv.py` |
| Zusammenhänge mit der Einsatzhäufigkeit | `deskriptiv/korrelation_zielgroesse.csv` | `prep/deskriptiv.py` |
| Merkmale und Zielgrößen des Analysedatensatzes | `codebook/merkmale.md` | `prep/codebook.py` |
| Referenzwerte beider Baseline-Stufen | `regression/baselines_mittel.csv`, `klassifikation/baselines_klasse_mittel.csv` | `vorpruefung/v1_baselines.py` |
| Prognosegüte der Einsatzhäufigkeit | Kreuzvalidierung: `regression/menge_mittel.csv`, `regression/baselines_mittel.csv`, Schlussbewertung: `regression/holdout.csv` | `modelle/m02_menge.py`, `vorpruefung/v1_baselines.py` |
| Gepaarter Wilcoxon-Test, RMSE | `regression/vergleich.csv` | `modelle/m02_menge.py` |
| Güte im Strukturstrang und erreichbare Obergrenzen | `klassifikation/struktur_mittel.csv`, `klassifikation/baselines_klasse_mittel.csv`, `klassifikation/holdout.csv`, `klassifikation/decke.csv`, `klassifikation/decke_holdout.csv` | `modelle/m03_struktur.py`, `vorpruefung/v1_baselines.py`, `vorpruefung/v4_decke.py` |
| Gepaarter Wilcoxon-Test, Macro-F1 | `klassifikation/vergleich.csv` | `modelle/m03_struktur.py` |
| Trainings- und Inferenzzeit je Fold | `regression/menge_mittel.csv`, `klassifikation/struktur_mittel.csv` | `modelle/m02_menge.py`, `modelle/m03_struktur.py` |
| Güteänderung beim Weglassen einer Faktorgruppe | `shap/ablation_faktorgruppen_mittel.csv`, Ausgangswerte aus `shap/ablation_faktorgruppen.csv` | `modelle/m04_shap.py` |
