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
python tools\sichere_ergebnisse.py name #    ZUERST: results/ sichern         < 1 min
python prep\build.py                    # 1  Aufbereitung -> zwei Datensätze   ~2 min
python tests\test_aufbereitung.py       #    19 Prüfungen an den Dateien       ~1 min
python vorpruefung\v0_aufteilung.py     # 2  Selbsttest der Fold-Zuteilung    < 1 min
python vorpruefung\run.py               #    Messlatte + Eignung (v1 und v2)   ~2 min
python vorpruefung\v3_spezifikation.py  #    Gegenprobe zur Eignungsprüfung    ~2 min
python modelle\m02_menge.py holdout     # 3  Regression (der lange Teil)      ~55 min
python modelle\m03_struktur.py holdout  #    Klassifikation                   ~45 min
python modelle\m04_shap.py              #    Faktorgruppen, Ablation, VIF     ~10 min
python modelle\m05_abbildungen.py       #    zehn Abbildungen (liest nur CSV) < 1 min
python tools\codebook.py                # 4  Merkmalstabelle für Kapitel 4    < 1 min
python tools\pruefe_zahlen.py           #    Doku gegen results/ prüfen       < 1 min
```

Ein vollständiger Durchlauf dauert rund **zwei Stunden**. Das Argument
`holdout` gehört nur in einen bewusst als Schlussbewertung gefahrenen Lauf —
ohne es sind die sechs zurückgehaltenen Stadtteile für den Code unerreichbar.

Die Reihenfolge ist verbindlich: `m05` rechnet nichts, es liest nur die CSV der
vorherigen Schritte. `tools\pruefe_zahlen.py` gehört **nicht zur Abgabe** und
meldet mit Exit-Code 1, welche Stelle der Dokumentation nicht mehr zu
`results/` passt.

`prep\build.py` läuft ohne Internet aus `data\raw`. Rohdaten werden nur geladen,
wenn der jeweilige `DOWNLOAD_*`-Schalter in `prep\config.py` auf `True` steht.

Einzelschritte:

```bash
python prep\s1_daten.py join            # nur joinen, ohne Download
python prep\s2_datensaetze.py splits    # Fold-Zuteilung anzeigen
python vorpruefung\v1_baselines.py      # nur die Messlatte
python vorpruefung\v2_eignung.py        # nur die Eignungsprüfung
python tools\aufraeumen.py              # Vorschau, löscht nichts
python tools\aufraeumen.py --wirklich   # verwaiste Artefakte entfernen
```

Spezifikation der Modellierung in `docs/04_MODELLIERUNG.md`, Ergebniszahlen
ausschließlich in `docs/03_STAND.md`.

**Was sich bei einem Wiederholungslauf reproduzieren muss:** Gütemaße,
Hyperparameter, Baselines, SHAP-Beiträge — alles läuft auf `RANDOM_STATE = 42`.
Was sich zwangsläufig ändert: sämtliche Laufzeiten sowie `parallel_gewinn` und
`parallel_abweichung`, weil XGBoost nicht threaddeterministisch ist
(`docs/07_BEFUNDE.md`, B-24). Der Zahlenwächter meldet die Laufzeitzahlen
danach als Fehler; sie sind in `03_STAND.md` §5.4 nachzuziehen.

## Aufbau

Drei Arbeitsschritte, drei Ordner:

```
prep/          die Daten        config · s1_daten · s2_datensaetze · build
vorpruefung/   die Messlatte    v0_aufteilung   wiederholte Splits, Selbsttest
               und die Eignung  v1_baselines    Stufe 1 + Stufe 2
                                v2_eignung      welche Verfahrensklasse passt?
                                v3_spezifikation  haelt die Nichtlinearitaet?
modelle/       der Vergleich    m02_menge · m03_struktur · m04_shap · m05_abbildungen
tests/                          test_aufbereitung
tools/         NICHT ABGABE     pruefe_zahlen      Doku gegen results/
                                codebook           Merkmalstabelle für Kapitel 4
                                aufraeumen         verwaiste Artefakte, Vorschau
                                sichere_ergebnisse results/ nach archiv/ kopieren
                                suchdiagnose       war die Hyperparametersuche am Limit?
archiv/        NICHT ABGABE     gesicherte Ergebnisstände, je mit Manifest
entwuerfe/     NICHT ABGABE     E-Mails, Erklärungen
data/          raw · processed
results/       regression · klassifikation · eignungspruefung · shap ·
               spezifikation · abbildungen · codebook
main.tex                        Gliederung und Schreibanleitung als Kommentare
docs/          01_VORGABEN · 02_ENTSCHEIDUNGEN · 03_STAND · 04_MODELLIERUNG ·
               06_RISIKEN · 07_BEFUNDE
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
