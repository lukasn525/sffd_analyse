# Abgabe – was ins Zip gehört

Vorgabe Schröter: **Zip ≤ 250 MB**, inklusive flüchtiger Quellen als PDF/A.
Der Code ist Beleg, nicht Hauptbestandteil (mind. 3/4 Fließtext).

---

## 1. Pflicht – Analysecode (18 Dateien, ~200 KB)

Das ist alles, was jemand braucht, um die Arbeit nachzurechnen.

```text
CLAUDE.md                              Projektrahmen + Decision Log
ORIENTIERUNG.md                        Datenfluss + Zuständigkeit je Datei
README.md                              Setup und Ausführung
DATA_DICTIONARY.md                     Spaltenbeschreibung
requirements.txt                       eingefrorene Paketversionen

prep/config.py                         alle Festlegungen an einer Stelle
prep/download.py                       Rohdaten laden (DataSF, Census)
prep/join.py                           Joins, ACS-Versatz, Kriminalitätsindex, Quoten
prep/regression_datensatz.py           Panel, Exposure, Saison, Lags
prep/klassifikation_datensatz.py       Zielgrößen + Merkmale Einzeleinsatz
prep/cv.py                             Folds, inneres Fenster, End-Hold-out, Gütemaße
prep/eignungspruefung.py               Eignungsurteil je Verfahren
prep/spaltennamen.py                   Mapping englisch → deutsch
prep/build.py                          der eine Befehl

modelle/baselines.py                   naiv, saisonal, Negative Binomial
modelle/train_regression.py            Ridge, Random Forest, XGBoost
modelle/train_klassifikation.py        dieselben drei, 4 Klassen

tests/test_aufbereitung.py             13 Prüfungen der Datenaufbereitung
```

Hinzu kommt `modelle/shap_analyse.py`, sobald es existiert.

**Warum `tests/` mitgeht:** Die Datei belegt, dass die Aufbereitung geprüft ist –
kein Leakage, rechteckiges Panel, Lags gegen die Rohdaten verifiziert,
Fold-Spalten konsistent. Das ist ein Qualitätsnachweis, der in einer
Prüfungssituation mehr wert ist als zusätzlicher Analysecode. Ein Satz im
Methodenkapitel genügt als Verweis.

## 2. Pflicht – Ergebnisse (~2 MB)

```text
results/eignungspruefung/              Summary + 5 Grafiken
results/regression/                    Metriken je Fold und Modell (CSV)
results/klassifikation/                dito
results/shap/                          SHAP-Grafiken
```

## 3. Empfohlen – Nachvollziehbarkeit der Entscheidungen

```text
docs/UMBAU_PREPROCESSING.md            Umbau-Plan + Nachweis der Bitgleichheit
docs/KLASSIFIKATION_DESIGN.md          Aufbau und Begründung
docs/RISIKEN_MODELLIERUNG.md           Risikoanalyse R1–R10
docs/PREPROCESSING_AUDIT_2026-07-26.md Audit-Protokoll
```

Diese vier belegen, dass Entscheidungen begründet und nicht zufällig getroffen
wurden. Das Decision Log in `CLAUDE.md` ist ohnehin Pflicht.

## 4. Optional – Datenbelege

```text
data/processed/regression.parquet      finaler Regressionsdatensatz (125 KB)
data/processed/klassifikation.parquet  finaler Klassifikationsdatensatz (2,8 MB)
```

Beide zusammen unter 3 MB – sie können bedenkenlos beiliegen und sind der
direkteste Beleg dafür, worauf die Modelle gerechnet haben. Der Zwischenstand
`einsaetze.parquet` (35 MB) ist über `prep/build.py` reproduzierbar.

---

## Nicht ins Zip

| Was | Warum |
|---|---|
| `venv/` | mehrere hundert MB, über `requirements.txt` reproduzierbar |
| `data/raw/` | 38 MB Rohdaten, über `prep/download.py` reproduzierbar |
| `data/processed/einsaetze.parquet` | 35 MB Zwischenstand, reproduzierbar |
| `docs/archiv/` | veraltete Stände, würde nur verwirren |
| `.git/`, `__pycache__/`, `notebooks/` | Arbeitsstände |

---

## Vor der Abgabe

```powershell
pip freeze > requirements.txt          # Versionen einfrieren
python prep\build.py                   # einmal komplett durchlaufen lassen
python tests\test_aufbereitung.py      # muss 13/13 zeigen
```

`prep/build.py` endet mit Exit-Code 1, falls ein Eignungskriterium nicht erfüllt
ist – die Aufbereitung gilt dann als nicht abgenommen.

Größenabschätzung: Code und Doku ~600 KB, Ergebnisse ~2 MB, Datensätze ~3 MB –
also **weit unter 250 MB**.

Flüchtige Quellen (DataSF-Datensatzseiten, Census-API-Doku) als PDF/A sichern
und beilegen – die Datensätze werden aktualisiert, die zitierten Stände sind
später nicht mehr abrufbar.
