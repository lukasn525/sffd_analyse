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
python tests\test_aufbereitung.py      # Prüfungen an den fertigen Dateien
```

`prep\build.py` läuft ohne Internet aus `data\raw`. Rohdaten werden nur geladen,
wenn der jeweilige `DOWNLOAD_*`-Schalter in `prep\config.py` auf `True` steht.

Einzelschritte:

```bash
python prep\s1_daten.py join           # nur joinen, ohne Download
python prep\s2_datensaetze.py splits   # Fold-Zuteilung anzeigen
python prep\s3_baselines.py            # nur die Vergleichswerte
```

Die Skripte unter `modelle/` werden gerade neu geschrieben — Spezifikation in
`docs/04_MODELLIERUNG.md`.

## Aufbau

```
prep/       erzeugt Daten       config.py · s1_daten.py · s2_datensaetze.py
                                s3_baselines.py · build.py
modelle/    rechnet Zahlen      m01_eignung · m02_menge · m03_struktur · m04_shap
tests/      prüft die Dateien   test_aufbereitung.py
data/       raw · processed
results/    eignungspruefung · regression · klassifikation
docs/       01_VORGABEN · 02_ENTSCHEIDUNGEN · 03_STAND · 04_MODELLIERUNG · 05_KAPITEL5
```

**Faustregel:** Erzeugt ein Schritt *Daten*, gehört er nach `prep/`. Erzeugt er
*Zahlen über Daten*, nach `modelle/`.

## Die zwei finalen Datensätze

`data/processed/regression.parquet` und `klassifikation.parquet`, beide auf der
Analyseeinheit **Stadtteil × Monat**, ohne fehlende Werte, Merkmale durchgehend
`float64`. Die Spalten `fold` und `ist_holdout` enthalten die Aufteilung —
dadurch sehen alle Verfahren zwangsläufig dieselben Folds.

**Steckbrief, Spaltenbeschreibung und Baseline-Werte: `docs/03_STAND.md`.**

## Dokumentation

Die Dateien sind nach ihrer Lebensdauer geschnitten — danach, wodurch sie
veralten. **Ergebniszahlen stehen ausschließlich in `03_STAND.md`**, alles
andere verweist darauf.

| Datei | Ändert sich | Inhalt |
|---|---|---|
| `docs/01_VORGABEN.md` | nur wenn Schröter etwas sagt | Auflagen, Gutachten-Regeln R1–R10, Formales, Abgabe |
| `docs/02_ENTSCHEIDUNGEN.md` | wächst, wird nie umgeschrieben | Decision Log mit Begründungen |
| `docs/03_STAND.md` | bei jedem `build.py` | Pipeline, Datensatz-Steckbrief, Baseline-Werte |
| `docs/04_MODELLIERUNG.md` | bei Änderung der Modellplanung | Spezifikation für `modelle/` |
| `docs/05_KAPITEL5.md` | beim Schreiben | Schreibvorlage Kapitel Data Preparation |
| `CLAUDE.md` | selten | Rahmenplan, Arbeitsregeln, KI-Verzeichnis |
