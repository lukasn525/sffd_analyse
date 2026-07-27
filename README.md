# sffd_analyse

Analysecode zur Bachelorarbeit *„Vorhersage von Feuerwehreinsätzen mittels
Machine Learning – ein Verfahrensvergleich am Beispiel der Stadtteile San
Franciscos"* (FOM, B.Sc. Wirtschaftsinformatik, Abgabe 07.10.2026).

Verglichen werden **Ridge Regression, Random Forest und XGBoost** auf zwei
Zielgrößen: Einsatzhäufigkeit (Regression, Stadtteil × Monat) und Einsatzart
(Klassifikation, Einzeleinsatz). Vorgehen nach CRISP-DM.

**Verbindlicher Projektrahmen: [`CLAUDE.md`](CLAUDE.md)** – Forschungsfrage,
Zielgrößen, Phasenstatus, Decision Log, Validierungs- und Tuning-Strategie.

**Wo gehört was hin? [`ORIENTIERUNG.md`](ORIENTIERUNG.md)** – Datenfluss und
Zuständigkeit je Datei.

---

## Ein Befehl

```powershell
python prep\build.py
```

Erzeugt aus den Rohdaten die beiden finalen Datensätze und prüft anschließend,
ob die drei Verfahren zu ihnen passen. Läuft in etwa 30 Sekunden, ohne Internet
und ohne API-Key – die Downloads sind über die `DOWNLOAD_*`-Schalter in
`prep/config.py` gesteuert und stehen per Default auf `False`.

```text
prep/download.py  ─┐
prep/join.py       ├─→ data/processed/einsaetze.parquet        Zwischenstand
prep/regression_datensatz.py     → data/processed/regression.parquet      FINAL
prep/klassifikation_datensatz.py → data/processed/klassifikation.parquet  FINAL
prep/eignungspruefung.py         → results/eignungspruefung/
```

Die beiden **FINAL** markierten Dateien sind das Einzige, was die Modellskripte
unter `modelle/` lesen.

---

## Die beiden Datensätze

| | `regression.parquet` | `klassifikation.parquet` |
|---|---|---|
| Zeile ist | ein Stadtteil-Monat | ein Einsatz |
| Zeitraum | **2015-01 – 2025-12** (132 Monate) | identisch |
| Einheiten | **35 Stadtteile** | dieselben 35 |
| Beobachtungen | **4.620** (rechteckiges Panel) | **350.481** |
| Zielgröße | `anzahl_einsaetze` | `einsatzart_gruppe` (4 Klassen) + `ist_brand` |
| Aufteilung | `fold` (1–3) und `ist_holdout` als Spalten | identisch |
| End-Hold-out | 2025-01 – 2025-12, beim Tuning unberührt | identisch |

Beide teilen zwingend dieselbe Abgrenzung: `klassifikation_datensatz.py`
übernimmt Zeitraum und Stadtteilliste aus `regression.parquet`.

Die Fold-Zuordnung steht **als Spalte im Datensatz**, nicht nur in einem
Funktionsaufruf. Damit ist die Fairness-Regel – alle drei Verfahren sehen
identische Splits – nachzählbar.

---

## Projektstruktur

```text
sffd_analyse/
├── CLAUDE.md                  # Projektrahmen + Decision Log  ← zuerst lesen
├── ORIENTIERUNG.md            # Datenfluss + Zuständigkeit je Datei
├── DATA_DICTIONARY.md         # Spaltenbeschreibung
├── ABGABE.md                  # was ins Abgabe-Zip gehört
│
├── prep/                      # alles Festgelegte – erzeugt die Datensätze
│   ├── config.py              #   EINZIGE Wahrheit: Zeitraum, Merkmale,
│   │                          #   Ausschlüsse, Folds, Suchräume, API-Keys
│   ├── download.py            #   DataSF + Census → data/raw
│   ├── join.py                #   Joins, ACS-Versatz, Kriminalitätsindex, Quoten
│   ├── regression_datensatz.py     # Panel, Exposure, Saison, Lags
│   ├── klassifikation_datensatz.py # Zielgrößen + Merkmale, Einzeleinsatz
│   ├── cv.py                  #   Zeitschnitte, Folds, Hold-out, Gütemaße
│   ├── eignungspruefung.py    #   Eignungsurteil je Verfahren
│   ├── spaltennamen.py        #   englisch → deutsch
│   └── build.py               #   DER EINE BEFEHL
│
├── modelle/                   # nur was tatsächlich schätzt
│   ├── baselines.py           #   naiv, saisonal, Negative Binomial
│   ├── train_regression.py    #   Ridge, Random Forest, XGBoost
│   └── train_klassifikation.py#   dieselben drei, 4 Klassen
│
├── tests/test_aufbereitung.py # 13 Prüfungen der Aufbereitung
├── docs/                      # Design, Risiken, Audit, Umbau-Plan
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

Für die ACS-Daten wird ein kostenloser Census API Key benötigt
([Signup](https://api.census.gov/data/key_signup.html)), eingetragen in
`prep/config.py` als `CENSUS_API_KEY`. Optional: DataSF App Token für höhere
Rate-Limits (`DATASF_APP_TOKEN`).

---

## Ausführen

```powershell
# Aufbereitung
python prep\build.py                 # alles: Daten + Eignungsprüfung
python prep\build.py daten           # nur die Datensätze
python prep\build.py pruefung        # nur die Eignungsprüfung
python prep\download.py test         # Erreichbarkeit der Quellen prüfen

# Einzelschritte (falls nur ein Teil neu soll)
python prep\join.py
python prep\regression_datensatz.py
python prep\klassifikation_datensatz.py
python prep\cv.py                    # zeigt die Zeitschnitte

# Absicherung
python tests\test_aufbereitung.py    # muss 13/13 zeigen

# Modelle
python modelle\baselines.py
python modelle\train_regression.py
python modelle\train_klassifikation.py
```

Rohdaten werden nur geladen, wenn der jeweilige `DOWNLOAD_*`-Schalter in
`prep/config.py` auf `True` steht. Nach einem Crime- oder ACS-Neu-Download
verwirft `download.py` automatisch den Cache `crime_index_monatlich.csv`.

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
