# Orientierung – welche Datei macht was?

Stand 2026-07-27, nach dem Umbau auf `prep/` und `modelle/`.

**Zweck:** eine einzige Seite, die den Datenfluss vom Rohdownload bis zu den
finalen Datensätzen zeigt. Wenn unklar ist, wo etwas hingehört oder woher eine
Spalte kommt: hier anfangen, nicht im Code suchen.

Ergänzt `README.md` (Setup/Ausführung) und `CLAUDE.md` (Begründungen, Decision Log).

---

## 1. Die eine Regel

> **`prep/` legt fest. `modelle/` rechnet.**

`prep/` enthält alles, was den Datensatz und die Spielregeln bestimmt: Zeitraum,
Stadtteilauswahl, Merkmale, Fold-Grenzen, Gütemaß-Definitionen, Suchräume.
`modelle/` enthält die vier Skripte, die tatsächlich Modelle schätzen.

Ein Modellskript liest genau eine Parquet-Datei und legt selbst nichts fest:

```python
import pandas as pd
from config import FEATURE_SETS, PFAD_REGRESSION

d = pd.read_parquet(PFAD_REGRESSION)
X, y = d[FEATURE_SETS["S+L"]], d["anzahl_einsaetze"]
```

---

## 2. Der Datenfluss

```text
DataSF / Census API
   │  prep/download.py           nur was in config.py auf True steht
   ▼  data/raw/*
   │
   │  prep/join.py               Dedup · Antwortzeit · Zeit-Features
   │                             ACS-Join mit Publikationsversatz (+1 Jahr)
   │                             Kriminalitätsindex (Location Quotient)
   │                             Land Use (Spatial Join, gecacht)
   │                             Quoten · deutsche Spaltennamen
   ▼  data/processed/einsaetze.parquet        720k × 50, Einsatz-Ebene
   │
   ├──────────────────────────────┬────────────────────────────────────
   │                              │
   │ prep/regression_datensatz.py │ prep/klassifikation_datensatz.py
   │   Parkgebiete raus (#19)     │   übernimmt Zeitraum + Stadtteile
   │   Aggregation ab 2014-01     │   AUS dem Regressionsdatensatz
   │     (Lag-Vorlauf, #23)       │   Zielgrößen aus NFIRS-Serien
   │   Raster, ffill (kein bfill) │   Zeit zyklisch kodiert
   │   log_bevoelkerung (#13)     │   Ergebnisvariablen ausgeschlossen (#20)
   │   log_kriminalitaetsindex    │
   │   Saison + Lags              │
   │   Zuschnitt auf 2015-01      │
   │   balanciertes Panel (#15)   │
   │   fold / ist_holdout         │   fold / ist_holdout
   ▼                              ▼
 regression.parquet            klassifikation.parquet
 4.620 × 24                    350.481 × 26
   │                              │
   └──────────────┬───────────────┘
                  │  prep/eignungspruefung.py   Urteil je Verfahren
                  ▼
              modelle/*                          Ridge · RF · XGBoost
```

Alles davon läuft mit **einem** Befehl: `python prep\build.py`.

---

## 3. Zuständigkeit je Datei

### `prep/` – legt fest

| Datei | Zuständig für | Anfassen, wenn … |
|---|---|---|
| `config.py` | **Einzige Wahrheit:** Pfade, Zeitraum, Vorlauf, Stadtteil-Ausschlüsse, Prädiktoren, Merkmalssätze, ACS-Versatz, Download-Schalter, API-Keys, Fold-Konfiguration, Suchräume | irgendeine Festlegung sich ändert |
| `download.py` | Rohdownloads von DataSF und Census | eine neue Quelle dazukommt |
| `join.py` | Joins, Kriminalitätsindex, Quoten, Eindeutschung | eine Rohspalte fehlt oder falsch gejoint ist |
| `regression_datensatz.py` | Panel, Exposure, Saison, Lags, balanciertes Panel | die Analyseeinheit oder ein Merkmal sich ändert |
| `klassifikation_datensatz.py` | Zielgrößen und Merkmale auf Einsatz-Ebene | am Klassifikationsteil |
| `cv.py` | Zeitschnitte, Folds, Hold-out, alle Gütemaße | die Validierungsstrategie sich ändert |
| `eignungspruefung.py` | Prüft die Voraussetzungen der drei Verfahren, fällt ein Urteil | ein Eignungskriterium dazukommt |
| `spaltennamen.py` | Mapping englisch → deutsch | eine Spalte anders heißen soll |
| `build.py` | Orchestrierung, Kurzbericht | ein Schritt dazukommt |

### `modelle/` – rechnet

| Datei | Inhalt |
|---|---|
| `baselines.py` | naiv (Vormonat), saisonaler Durchschnitt, Negative Binomial mit Offset |
| `train_regression.py` | Ridge, Random Forest, XGBoost auf beiden Merkmalssätzen |
| `train_klassifikation.py` | Logistische Regression (L2), Random Forest, XGBoost, 4 Klassen |

Noch offen: `shap_analyse.py` und die RandomizedSearchCV in den beiden
Trainingsskripten (Suchräume liegen bereits in `config.py`).

### `tests/`

`test_aufbereitung.py` – 13 Prüfungen **gegen die fertigen Parquet-Dateien**,
nicht gegen den Code, der sie erzeugt. Fällt damit auch auf, wenn jemand eine
Datei von Hand ändert oder den Datensatz nach einer Konfigurationsänderung
vergisst neu zu bauen.

---

## 4. Wo gehört mein neues Merkmal hin?

- Ergibt es sich aus **Rohspalten desselben Einsatzes** (Quote, Anteil,
  Umbenennung) → `prep/join.py`
- Ergibt es sich aus **Zeit oder der Zielgröße** (Saison, Lag, gleitendes
  Mittel) → `prep/regression_datensatz.py`
- Ist es eine **Festlegung** (Zeitraum, Stadtteile, Prädiktorenliste,
  Merkmalssatz, Suchraum) → `prep/config.py`
- Ist es ein **Ergebnis** (Vorhersage, Gütemaß, SHAP-Wert) → `modelle/`

Faustregel für die Grenze: Erzeugt der Schritt **Daten**, gehört er nach
`prep/`. Erzeugt er **Zahlen über Daten**, nach `modelle/`.

Ausnahme mit Absicht: Die **Suchräume** des Hyperparameter-Tunings stehen in
`prep/config.py`, obwohl die Suche in `modelle/` läuft. Ein eigenes
Tuning-Skript würde die besten Parameter in eine Datei auslagern, die veralten
kann, ohne dass es auffällt.

---

## 5. Was NICHT mehr existiert

Der Umbau vom 2026-07-27 hat drei Ordner aufgelöst (Decision Log #22):

| Früher | Jetzt |
|---|---|
| `pipeline/01_fetch.py` | `prep/download.py` |
| `pipeline/02_join.py` + `03_features.py` | `prep/join.py` (zusammengelegt) |
| `pipeline/column_names.py` | `prep/spaltennamen.py` |
| `pipeline/run_pipeline.py` | `prep/build.py` |
| `modellierung/aggregation.py` + `features.py` | `prep/regression_datensatz.py` |
| `modellierung/klassifikation_daten.py` | `prep/klassifikation_datensatz.py` |
| `modellierung/cv.py` | `prep/cv.py` |
| `modellierung/demo_modellierung.py` | `modelle/baselines.py` + `train_regression.py` |
| `analyse/eignungspruefung.py` | `prep/eignungspruefung.py` |
| `analyse/deskriptiv.py`, `dashboard.py` | gestrichen (explorativ) |
| `sf_fire_incidents_base.parquet` | gestrichen |
| `sf_fire_risk_features.parquet` | `einsaetze.parquet` |
| `sf_fire_risk_features_cleaned.parquet` | gestrichen (war eine Sackgasse) |

Der Umbau war inhaltlich folgenlos: Der neue Datensatz war vor Aktivierung des
Lag-Vorlaufs **zellengleich** zum alten (Nachweis in
`docs/UMBAU_PREPROCESSING.md`, Schritt 4).

---

## 6. Dokumente – wozu welches?

| Datei | Beantwortet |
|---|---|
| `ORIENTIERUNG.md` (diese) | *Was gehört wozu? Woher kommt eine Spalte?* |
| `README.md` | *Wie richte ich das ein und führe es aus?* |
| `CLAUDE.md` | *Warum ist das so? Was ist mit Schröter zu klären?* (Decision Log) |
| `DATA_DICTIONARY.md` | *Was bedeutet diese Spalte?* |
| `ABGABE.md` | *Was kommt ins Zip?* |
| `docs/UMBAU_PREPROCESSING.md` | *Wie kam die heutige Struktur zustande?* |
| `docs/KLASSIFIKATION_DESIGN.md` | *Wie ist der Klassifikationsteil aufgebaut?* |
| `docs/RISIKEN_MODELLIERUNG.md` | *Was kann jetzt noch schiefgehen?* (R1–R10) |
| `docs/UMSETZUNGSLEITFADEN_MODELLIERUNG.md` | *Was ist der nächste Programmierschritt?* |
| `docs/PREPROCESSING_AUDIT_2026-07-26.md` | *Was wurde geprüft und behoben?* |
| `docs/NAECHSTE_SCHRITTE.md` | *Was muss Lukas selbst tun?* |
| `docs/archiv/` | **veraltet** – nicht in die Arbeit übernehmen |

---

## 7. Regel für die Zukunft

Neue Datei in `prep/` oder `modelle/`? **Eine Zeile in Abschnitt 3 ergänzen,
bevor Code geschrieben wird.** Ändert sich der Datenfluss, das Diagramm in
Abschnitt 2 nachziehen. Das kostet eine Minute und ersetzt die Suche im Code.
