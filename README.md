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
python prep\build.py                   # 1  Aufbereitung -> zwei Datensätze
python tests\test_aufbereitung.py      #    Prüfungen an den fertigen Dateien
python vorpruefung\run.py              # 2  Messlatte + Verfahrenseignung
```

`prep\build.py` läuft ohne Internet aus `data\raw`. Rohdaten werden nur geladen,
wenn der jeweilige `DOWNLOAD_*`-Schalter in `prep\config.py` auf `True` steht.

Einzelschritte:

```bash
python prep\s1_daten.py join            # nur joinen, ohne Download
python prep\s2_datensaetze.py splits    # Fold-Zuteilung anzeigen
python vorpruefung\v1_baselines.py      # nur die Messlatte
python vorpruefung\v2_eignung.py        # nur die Eignungsprüfung
```

Die Skripte unter `modelle/` werden gerade neu geschrieben — Spezifikation in
`docs/04_MODELLIERUNG.md`.

## Aufbau

Drei Arbeitsschritte, drei Ordner:

```
prep/          die Daten        config · s1_daten · s2_datensaetze · build
vorpruefung/   die Messlatte    v1_baselines  Stufe 1 (ohne Merkmale)
               und die Eignung                Stufe 2 (einfachste passende Form)
                                v2_eignung    welche Verfahrensklasse passt?
modelle/       der Vergleich    m02_menge · m03_struktur · m04_shap
tests/                          test_aufbereitung
data/          raw · processed
results/       regression · klassifikation · eignungspruefung
docs/          01_VORGABEN · 02_ENTSCHEIDUNGEN · 03_STAND · 04_MODELLIERUNG · 06_RISIKEN
```

**Faustregel:** Erzeugt ein Schritt *Daten*, gehört er nach `prep/`. Legt er
fest, *was ein Modell mindestens leisten muss und warum diese Verfahren*, nach
`vorpruefung/`. Vergleicht er Verfahren, nach `modelle/`.

## Die zwei finalen Datensätze

`data/processed/regression.parquet` und `klassifikation.parquet`, beide auf der
Analyseeinheit **Stadtteil × Monat**, ohne fehlende Werte, Merkmale durchgehend
`float64`. Die Spalten `fold` und `ist_holdout` enthalten die Aufteilung —
dadurch sehen alle Verfahren zwangsläufig dieselben Folds.

**Steckbrief, Spaltenbeschreibung und Baseline-Werte: `docs/03_STAND.md`.**

## Dokumentation

Vier Dateien, geschnitten danach, **wodurch sie veralten**:

| Datei | Ändert sich durch |
|---|---|
| `docs/01_VORGABEN.md` | Ansagen von Schröter |
| `docs/02_ENTSCHEIDUNGEN.md` | neue Entscheidungen — wächst, wird nie umgeschrieben |
| `docs/03_STAND.md` | jeden Lauf von `build.py` |
| `docs/04_MODELLIERUNG.md` | Änderungen an der Modellplanung |
| `docs/06_RISIKEN.md` | eingetretene oder weggefallene Risiken |

**Ergebniszahlen stehen ausschließlich in `03_STAND.md`**, alles andere verweist
darauf. Rahmenplan, Arbeitsregeln und KI-Verzeichnis: `CLAUDE.md`.
