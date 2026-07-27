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

**Schreibvorlage Kapitel 5: [`docs/KAPITEL_5_AUFBEREITUNG.md`](docs/KAPITEL_5_AUFBEREITUNG.md)**
– je Arbeitsschritt: was passiert, wo im Code, welche Zahlen, welche Entscheidung.

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
prep/s1_daten.py         →  data/raw/*
                         →  data/processed/einsaetze.parquet       Zwischenstand
prep/s2_datensaetze.py   →  data/processed/regression.parquet      FINAL
                         →  data/processed/klassifikation.parquet  FINAL
prep/s3_pruefung.py      →  results/eignungspruefung/
                         →  results/regression/baselines_*.csv
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

Beide entstehen in **einer** Datei (`prep/s2_datensaetze.py`) und teilen dadurch
zwingend dieselbe Abgrenzung – Zeitraum und Stadtteilliste werden einmal
bestimmt und an beide weitergereicht.

Alle Merkmale sind `float64`; die Designmatrix lässt sich ohne Umweg an Ridge,
Random Forest und XGBoost übergeben.

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
│   ├── s1_daten.py            #   1 laden, auswählen, joinen, Raten
│   ├── s2_datensaetze.py      #   2 aggregieren, Lags, Folds, Gütemaße
│   │                          #     → beide finalen Datensätze
│   ├── s3_pruefung.py         #   3 Eignungsurteil je Verfahren + Baselines
│   ├── build.py               #   DER EINE BEFEHL
│   └── _archiv/               #   ersetzte Vorgängerdateien (Nachvollziehbarkeit)
│
├── modelle/                   # nur was tatsächlich schätzt
│   ├── m02_regression.py      #   Ridge, Random Forest, XGBoost
│   └── m03_klassifikation.py  #   dieselben drei, 4 Klassen
│
├── tests/test_aufbereitung.py # 14 Prüfungen der Aufbereitung
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
python prep\s1_daten.py test         # Erreichbarkeit der Quellen prüfen

# Einzelschritte (falls nur ein Teil neu soll)
python prep\s1_daten.py join         # nur joinen, ohne Download
python prep\s2_datensaetze.py
python prep\s2_datensaetze.py splits # zeigt die Zeitschnitte
python prep\s3_pruefung.py baselines # nur die Vergleichsgrößen

# Absicherung
python tests\test_aufbereitung.py    # muss 14/14 zeigen

# Modelle
python modelle\m02_regression.py
python modelle\m03_klassifikation.py
```

Rohdaten werden nur geladen, wenn der jeweilige `DOWNLOAD_*`-Schalter in
`prep/config.py` auf `True` steht. Nach einem Crime- oder ACS-Neu-Download
verwirft `s1_daten.py` automatisch den Cache `crime_index_monatlich.csv`.

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
