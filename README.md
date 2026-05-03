# sffd_analyse

Bachelorarbeit: Sozioökonomische Einflüsse auf Feuerwehreinsätze in San Francisco

---

## Projektstruktur

```text
sffd_analyse/
├── data/
│   ├── raw/                        # Rohdaten (von download_data.py befüllt)
│   │   ├── fire_incidents.parquet  # SFFD-Einsätze (bereinigt)
│   │   ├── crosswalk.csv           # Census Tract ↔ Neighborhood
│   │   └── acs_tracts_2019.csv     # ACS-Daten auf Tract-Ebene
│   └── processed/                  # Analysefertige Daten
│       ├── acs_neighborhoods.csv   # ACS aggregiert auf Neighborhood
│       └── sffd_acs_joined.parquet # Hauptanalysedatei (SFFD + ACS, je 1 Einsatz)
├── docs/
│   ├── DATA_DICTIONARY_ANALYSIS.md
│   └── FIR-0001_DataDictionary_fire-incidents.xlsx
├── results/
│   ├── basic_stats_summary.txt
│   └── sffd_fire_incidents_report.pdf
├── scripts/
│   ├── download_data.py    # Datenpipeline (SFFD + Crosswalk + ACS)
│   ├── basic_stats.py      # Deskriptive Statistiken
│   └── generate_report.py  # PDF-Report mit Visualisierungen
└── requirements.txt
```

---

## Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

VS Code Interpreter auf `.\venv\Scripts\python.exe` setzen (Ctrl+Shift+P → "Python: Select Interpreter").

---

## Daten laden

```powershell
# APIs kurz testen (kein Key nötig)
python scripts/download_data.py test

# Volle Pipeline starten
python scripts/download_data.py
```

Für die ACS-Daten (Einkommen, Armut, Bildung) wird ein kostenloser Census API Key benötigt:

1. Key beantragen: [api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html)
2. In `scripts/download_data.py` eintragen: `CENSUS_API_KEY = "..."`

Optional: DataSF App Token für höhere Rate-Limits (`DATASF_APP_TOKEN`).

---

## Analyse

```powershell
# Deskriptive Statistiken → results/basic_stats_summary.txt
python scripts/basic_stats.py

# PDF-Report → results/sffd_fire_incidents_report.pdf
python scripts/generate_report.py
```

---

## Datenquellen

| Datensatz                      | Quelle                        | Granularität                 |
| ------------------------------ | ----------------------------- | ---------------------------- |
| SFFD Fire Incidents (FIR-0001) | DataSF SODA API (`wr8u-xric`) | Einzelner Einsatz            |
| Neighborhood Crosswalk         | DataSF (`rqw6-h7c5`)          | Census Tract                 |
| ACS 5-Year 2019                | US Census Bureau              | Census Tract → Neighborhood  |

Die Hauptanalysedatei `data/processed/sffd_acs_joined.parquet` enthält einen Einsatz pro Zeile (~720.000 Zeilen), angereichert mit den ACS-Sozioökonomiedaten der jeweiligen Neighborhood: Medianeinkommen, Armutsquote, Bildungsgrad, Mietkosten und Leerstandsquote.
