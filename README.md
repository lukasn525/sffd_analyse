# Vorhersage von Feuerwehreinsätzen in San Francisco

Bachelorarbeit (FOM, B.Sc. Wirtschaftsinformatik): Verfahrensvergleich von
Ridge Regression, Random Forest und XGBoost auf Stadtteildaten.

## Setup

```bash
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

## Ausführen

```bash
python prep\build.py                   # Aufbereitung -> zwei Datensätze + Baselines
python tests\test_aufbereitung.py      # 14 Prüfungen an den fertigen Dateien

python modelle\m01_eignung.py          # Eignungsurteil je Verfahren
python modelle\m02_regression.py       # Ridge, Random Forest, XGBoost
python modelle\m03_klassifikation.py   # dieselben drei, 4 Klassen
```

`prep\build.py` läuft ohne Internet aus `data\raw` (~13 s). Rohdaten werden nur
geladen, wenn der jeweilige `DOWNLOAD_*`-Schalter in `prep\config.py` auf `True`
steht.

Einzelschritte:

```bash
python prep\s1_daten.py join           # nur joinen, ohne Download
python prep\s2_datensaetze.py splits   # Zeitschnitte anzeigen
python prep\s3_baselines.py            # nur die Vergleichsgrößen
```

## Aufbau

```
prep/       erzeugt Daten       config.py · s1_daten.py · s2_datensaetze.py
                                s3_baselines.py · build.py
modelle/    rechnet Zahlen      m01_eignung.py · m02_regression.py · m03_klassifikation.py
tests/      prüft die Dateien   test_aufbereitung.py
data/       raw · processed
results/    eignungspruefung · regression · klassifikation
docs/       01_PIPELINE · 02_ENTSCHEIDUNGEN · 03_VORGABEN
```

**Faustregel:** Erzeugt ein Schritt *Daten*, gehört er nach `prep/`. Erzeugt er
*Zahlen über Daten*, nach `modelle/`.

## Die zwei finalen Datensätze

| Datei | Zeilen × Spalten | Ebene | Zielgröße |
|---|---|---|---|
| `data/processed/regression.parquet` | 4.620 × 24 | Stadtteil × Monat | `anzahl_einsaetze` |
| `data/processed/klassifikation.parquet` | 350.481 × 26 | Einzeleinsatz | `einsatzart_gruppe`, `ist_brand` |

Beide decken 2015-01 bis 2025-12 ab, 35 Stadtteile, keine fehlenden Werte, alle
Merkmale `float64`. Die Spalten `fold` und `ist_holdout` enthalten die
CV-Aufteilung — dadurch sehen alle drei Verfahren zwangsläufig dieselben Folds.

Was genau in den Datensätzen steht und wie sie entstehen: **`docs/01_PIPELINE.md`**.

## Dokumentation

| Datei | Inhalt |
|---|---|
| `docs/01_PIPELINE.md` | was die Aufbereitung tut, was hinten rauskommt, Spaltenbeschreibung |
| `docs/02_ENTSCHEIDUNGEN.md` | Decision Log: jede Abweichung vom Exposé mit Begründung |
| `docs/03_VORGABEN.md` | Schröter-Auflagen, Gutachten-Regeln R1–R10, Formales, Abgabe |
| `CLAUDE.md` | Rahmenplan, Status, KI-Verzeichnis |
