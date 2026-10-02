# Datenfluss

Welches Skript welche Dateien liest und schreibt, in der Reihenfolge der
README. Dateien ohne Pfad liegen in `data/processed/`.

| Skript | liest | schreibt |
|---|---|---|
| `prep/s1_daten.py` | `data/raw/*` | `einsaetze.parquet`, `acs_neighborhoods_<Jahr>.csv`, zwei Zwischendateien |
| `prep/s2_datensaetze.py` | `einsaetze.parquet` | `regression.parquet`, `klassifikation.parquet` |
| `prep/build.py` | führt s1 und s2 aus | |
| `tests/test_aufbereitung.py` | beide Analysedateien | keine Datei, nur Prüfbericht |
| `prep/rohbefunde.py` | Rohdaten der Einsätze, Parzellen, ACS und Zuordnung, `acs_neighborhoods_<Jahr>.csv` | `results/deskriptiv/rohbefunde.md`, `rohbefunde_acs.csv` |
| `prep/deskriptiv.py` | beide Analysedateien | `results/deskriptiv/*` |
| `prep/codebook.py` | beide Analysedateien | `results/codebook/merkmale.csv`, `merkmale.md` |
| `vorpruefung/v0_aufteilung.py` | beide Analysedateien | keine Datei, Selbsttest |
| `vorpruefung/panelprofil.py` | beide Analysedateien, `data/raw/neighborhoods.geojson` | `results/panelprofil/*` |
| `vorpruefung/run.py` | führt v1 und v2 aus | |
| `vorpruefung/v1_baselines.py` | beide Analysedateien | `results/regression/baselines_*.csv`, `results/klassifikation/baselines_klasse*.csv` |
| `vorpruefung/v2_eignung.py` | beide Analysedateien, `baselines_klasse.csv` | `results/eignungspruefung/*` |
| `vorpruefung/v3_spezifikation.py` | `regression.parquet` | `results/spezifikation/*` |
| `modelle/m02_menge.py` | beide Analysedateien, `baselines_folds.csv` | `results/regression/`: tuning, menge_*, vergleich, holdout, leakage_diagnose |
| `modelle/m03_struktur.py` | `klassifikation.parquet`, `baselines_klasse.csv` | `results/klassifikation/`: tuning, struktur_*, vergleich, holdout, leakage_diagnose |
| `vorpruefung/v4_decke.py` | `klassifikation.parquet`, Ergebnisse von m03 | `results/klassifikation/decke*`, `klassen_f1.csv` |
| `modelle/suchdiagnose.py` | beide Analysedateien | `results/suchdiagnose/*` |
| `modelle/parametersensitivitaet.py` | beide Analysedateien, `tuning.csv` beider Stränge | `results/parametersensitivitaet/*` |
| `modelle/trennschaerfe.py` | `vergleich.csv` beider Stränge | `results/trennschaerfe/*` |
| `modelle/m04_shap.py` | beide Analysedateien, Ergebnisse von m02 und m03 | `results/shap/*` |
