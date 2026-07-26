# Abgabe – was ins Zip gehört

Vorgabe Schröter: **Zip ≤ 250 MB**, inklusive flüchtiger Quellen als PDF/A.
Der Code ist Beleg, nicht Hauptbestandteil (mind. 3/4 Fließtext).

---

## 1. Pflicht – Analysecode (13 Dateien, ~150 KB)

Das ist alles, was jemand braucht, um die Arbeit nachzurechnen.

```text
CLAUDE.md                              Projektrahmen + Decision Log
README.md                              Setup und Ausführungsreihenfolge
DATA_DICTIONARY.md                     Spaltenbeschreibung
requirements.txt                       eingefrorene Paketversionen

pipeline/01_fetch.py                   Rohdaten laden (DataSF, Census)
pipeline/02_join.py                    Joins, ACS-Versatz, Kriminalitätsindex
pipeline/03_features.py                Raten, deutsche Spaltennamen
pipeline/column_names.py               Mapping englisch → deutsch
pipeline/run_pipeline.py               Orchestrierung 01 → 02 → 03

modellierung/aggregation.py            Panel, Zeitraum, Exposure
modellierung/features.py               Saison, Lags, Merkmalssätze S und S+L
modellierung/cv.py                     Folds, inneres Fenster, End-Hold-out
modellierung/klassifikation_daten.py   Zielgrößen + Merkmale Einzeleinsatz

tests/test_aufbereitung.py             11 Prüfungen der Datenaufbereitung
```

Hinzu kommen die Modellierungsskripte, sobald sie existieren
(`baselines.py`, `train_regression.py`, `train_klassifikation.py`,
`shap_analyse.py`).

**Warum `tests/` mitgeht:** Die Datei belegt, dass die Aufbereitung geprüft ist –
kein Leakage, rechteckiges Panel, Lags gegen die Rohdaten verifiziert. Das ist
ein Qualitätsnachweis, der in einer Prüfungssituation mehr wert ist als
zusätzlicher Analysecode. Ein Satz im Methodenkapitel genügt als Verweis.

## 2. Pflicht – Ergebnisse (~2 MB)

```text
results/eignungspruefung/              Summary + 5 Grafiken
results/regression/                    Metriken je Fold und Modell (CSV)
results/klassifikation/                dito
results/shap/                          SHAP-Grafiken
```

## 3. Empfohlen – Nachvollziehbarkeit der Entscheidungen

```text
docs/KLASSIFIKATION_DESIGN.md          Aufbau und Begründung
docs/RISIKEN_MODELLIERUNG.md           Risikoanalyse R1–R10
docs/PREPROCESSING_AUDIT_2026-07-26.md Audit-Protokoll
```

Diese drei belegen, dass Entscheidungen begründet und nicht zufällig getroffen
wurden. Das Decision Log in `CLAUDE.md` ist ohnehin Pflicht.

## 4. Optional – Datenbelege

```text
data/sample/*.csv                      je 100 Zeilen der drei Haupttabellen (~90 KB)
```

Zeigt die Datenstruktur, ohne den Rahmen zu sprengen. Die vollen Parquets
(78 MB) passen zwar ins Limit, sind aber über `01_fetch.py` reproduzierbar –
im Zweifel nur `sf_fire_risk_features.parquet` (35 MB) beilegen.

---

## Nicht ins Zip

| Was | Warum |
|---|---|
| `venv/` | mehrere hundert MB, über `requirements.txt` reproduzierbar |
| `data/raw/` | 38 MB Rohdaten, über `01_fetch.py` reproduzierbar |
| `docs/archiv/` | veraltete Stände, würde nur verwirren |
| `analyse/dashboard.py`, `deskriptiv.py` | explorative Hilfsskripte ohne Ergebnisbezug |
| `.git/`, `__pycache__/`, `notebooks/` | Arbeitsstände |

---

## Vor der Abgabe

```powershell
pip freeze > requirements.txt          # Versionen einfrieren
python tests\test_aufbereitung.py      # muss 11/11 zeigen
python pipeline\run_pipeline.py        # einmal komplett durchlaufen lassen
```

Größenabschätzung: Code und Doku ~560 KB, Ergebnisse ~2 MB, Samples ~90 KB –
also **weit unter 250 MB**, selbst mit einem vollen Parquet.

Flüchtige Quellen (DataSF-Datensatzseiten, Census-API-Doku) als PDF/A sichern
und beilegen – die Datensätze werden aktualisiert, die zitierten Stände sind
später nicht mehr abrufbar.
