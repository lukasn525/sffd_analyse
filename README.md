# sffd_analyse

Analysecode zur Bachelorarbeit *„Vorhersage von Feuerwehreinsätzen mittels
Machine Learning – ein Verfahrensvergleich am Beispiel der Stadtteile San
Franciscos"* (FOM, B.Sc. Wirtschaftsinformatik, Abgabe 07.10.2026).

Verglichen werden **Ridge Regression, Random Forest und XGBoost** auf zwei
Zielgrößen: Einsatzhäufigkeit (Regression, Stadtteil × Monat) und Einsatzart
(Klassifikation, Einzeleinsatz). Vorgehen nach CRISP-DM.

**Verbindlicher Projektrahmen: [`CLAUDE.md`](CLAUDE.md)** – Forschungsfrage,
Zielgrößen, Phasenstatus, Decision Log, Validierungs- und Tuning-Strategie.

---

## Analysedatensatz (festgesetzt, Stand 2026-07-26)

| | Regression | Klassifikation |
|---|---|---|
| Ebene | Stadtteil × Monat | Einzeleinsatz |
| Zeitraum | **2015-01 – 2025-12** (132 Monate) | identisch |
| Einheiten | **35 Stadtteile** | dieselben 35 |
| Beobachtungen | **4.620** (rechteckiges Panel) | 350.481 |
| Zielgröße | `anzahl_einsaetze` | `ist_brand` (13,6 %) |
| End-Hold-out | 2025-01 – 2025-12, beim Tuning unberührt | identisch |

---

## Projektstruktur

```text
sffd_analyse/
├── CLAUDE.md                  # Projektrahmen + Decision Log  ← zuerst lesen
├── DATA_DICTIONARY.md         # Spaltenbeschreibung
├── pipeline/                  # ETL (Prep-Pipeline)
│   ├── 01_fetch.py            #   Rohdaten von DataSF / Census laden
│   ├── 02_join.py             #   Joins, ACS-Versatz, Kriminalitätsindex
│   ├── 03_features.py         #   Raten, deutsche Spaltennamen
│   ├── column_names.py        #   Mapping englisch → deutsch
│   └── run_pipeline.py        #   Orchestrierung 01 → 02 → 03
├── ABGABE.md                  # was ins Abgabe-Zip gehört
├── modellierung/
│   ├── aggregation.py         # Stadtteil × Monat, Zeitraum, Exposure, Panel
│   ├── features.py            # Saison, Lags, Merkmalssätze S und S+L
│   ├── cv.py                  # Zeitschnitte, Folds, Hold-out, Gütemaße
│   ├── klassifikation_daten.py # Zielgrößen + Merkmale Einzeleinsatz
│   └── demo_modellierung.py   # lauffähige Demo (Baselines + Ridge + RF)
├── tests/
│   └── test_aufbereitung.py   # 11 Prüfungen der Datenaufbereitung
├── analyse/
│   ├── eignungspruefung.py    # Linearität, VIF, Overdispersion, Klassenbalance
│   ├── deskriptiv.py          # deskriptive Kennzahlen
│   └── dashboard.py           # Übersichtsgrafik
├── docs/
│   ├── NAECHSTE_SCHRITTE.md   # Roadmap in einfacher Sprache
│   ├── KLASSIFIKATION_DESIGN.md
│   ├── UMSETZUNGSLEITFADEN_MODELLIERUNG.md
│   ├── PREPROCESSING_AUDIT_2026-07-26.md
│   └── archiv/                # veraltet – nicht in die Arbeit übernehmen
├── data/                      # nicht im Repo (gitignored)
└── results/                   # nicht im Repo (gitignored)
```

---

## Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

VS Code Interpreter auf `.\venv\Scripts\python.exe` setzen
(Ctrl+Shift+P → „Python: Select Interpreter").

---

## Ausführen

```powershell
# ETL – Downloads über Argumente statt Flags im Code
python pipeline\01_fetch.py test          # Erreichbarkeit der Quellen prüfen
python pipeline\01_fetch.py crime         # z. B. nur Kriminalitätsdaten
python pipeline\02_join.py
python pipeline\03_features.py

# Selbsttests der Aufbereitung
python modellierung\aggregation.py        # Panel, Zeitraum, Exposure
python modellierung\cv.py                 # Folds, Hold-out

# Analyse
python analyse\eignungspruefung.py
python modellierung\demo_modellierung.py
```

`01_fetch.py` akzeptiert: `sffd`, `crosswalk`, `acs`, `crime`, `crime_neu`,
`crime_hist`, `landuse`, `neighborhoods`, `alle`, `test`. Ohne Argument gelten
die `DOWNLOAD_*`-Schalter im Kopf der Datei (Default: alle `False`).

Für die ACS-Daten wird ein kostenloser Census API Key benötigt
([Signup](https://api.census.gov/data/key_signup.html)), eingetragen in
`pipeline/01_fetch.py` als `CENSUS_API_KEY`. Optional: DataSF App Token für
höhere Rate-Limits (`DATASF_APP_TOKEN`).

---

## Datenquellen

| Quelle | ID | Inhalt |
|---|---|---|
| SFFD Fire Incidents | `wr8u-xric` | ~720.000 Einsätze, 2003–2026 |
| Census-Tract-Crosswalk | `sevw-6tgi` | Tract ↔ Neighborhood |
| ACS 5-Year Estimates | Census API | 5 Jahrgänge (2009, 2014, 2019, 2021, 2023) |
| SFPD Incidents (ab 2018) | `e3si-785i` | monatlich voraggregiert |
| SFPD Incidents (historisch) | `tmnf-yvry` | 2003 – 05/2018, mit Koordinaten |
| Land Use 2020 | `ygi5-84iq` | Parzellen |
| Neighborhood-Grenzen | `j2bu-swwd` | Geometrie für Spatial Joins |
