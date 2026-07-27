# Die Aufbereitung in einfacher Sprache

Ein Befehl, zwei fertige Datensätze:

```
python prep/build.py
```

Danach liegen in `data/processed/` zwei Dateien, auf denen du direkt trainieren
kannst — plus die Baseline-Werte, an denen sich die Modelle messen lassen müssen.

---

## Die vier Bausteine

### `prep/config.py` — die Einstellungen

Kein Arbeitsschritt, sondern ein Zettel mit allen Festlegungen: Von wann bis
wann geht die Analyse (2015-01 bis 2025-12)? Welche Stadtteile fliegen raus?
Welche Merkmale gehen ins Modell? Wie viele Folds? Wenn du irgendetwas ändern
willst, änderst du es hier — und nur hier.

### `prep/s1_daten.py` — Rohdaten holen und zusammenkleben

Vier Quellen werden geladen und zu **einer Tabelle mit einer Zeile je Einsatz**
verbunden:

| Quelle | Was daraus kommt |
|---|---|
| SFFD Feuerwehreinsätze | die Einsätze selbst, ~720.000 seit 2003 |
| ACS (US-Zensus) | Einkommen, Armut, Bildung, Miete, Leerstand, Einwohner |
| SFPD Kriminalität | wie viel Kriminalität in diesem Stadtteil, in diesem Monat |
| Land Use 2020 | Gebäudealter, Wohnanteil, Anteil Risiko-Gewerbe |

Unterwegs passiert das Nötige: doppelte Einsatznummern raus, unplausible
Antwortzeiten raus, Stadtteilnamen vereinheitlichen, Quoten ausrechnen (z. B.
Armutsquote = arme Menschen ÷ alle Menschen), englische Spaltennamen eindeutschen.

**Der wichtigste Punkt hier:** Jeder Einsatz bekommt nur Daten, die es zum
Zeitpunkt des Einsatzes schon gab. Ein Einsatz aus 2023 bekommt die
Sozialdaten von 2021, weil die Zahlen für 2023 erst Ende 2024 veröffentlicht
wurden. Sonst würde das Modell in die Zukunft schauen.

**Ergebnis:** `data/processed/einsaetze.parquet` — 719.989 Zeilen × 50 Spalten.
Das ist ein Zwischenstand, kein Endprodukt.

### `prep/s2_datensaetze.py` — daraus die zwei fertigen Datensätze bauen

Nimmt die Einsatz-Tabelle und macht daraus zwei Dinge:

**Datensatz 1 — Regression.** Zählt die Einsätze je Stadtteil und Monat. Aus
720.000 einzelnen Einsätzen werden 4.620 Zeilen (35 Stadtteile × 132 Monate).
Dazu kommen die Vergangenheitswerte: Wie viele Einsätze waren im Vormonat
(`lag_1`), vor einem Jahr (`lag_12`), im Schnitt der letzten drei Monate
(`rolling_mean_3`)? Und die Jahreszeit als Sinus/Kosinus, damit Dezember und
Januar dicht beieinander liegen.

**Datensatz 2 — Klassifikation.** Bleibt auf Einsatz-Ebene, aber nur für
dieselben Stadtteile und denselben Zeitraum wie oben. Jeder Einsatz bekommt sein
Label: Brand, Rettung/EMS, Technische Hilfe oder Fehlalarm. Dazu die
Stadtteilmerkmale und die Uhrzeit.

Beide Datensätze bekommen zwei Spalten, die die Aufteilung festhalten: `fold`
(1, 2 oder 3 = in welchem Testfenster liegt dieser Monat, 0 = nur Training) und
`ist_holdout` (1 = gehört zu den letzten 12 Monaten, die beim Tunen nicht
angefasst werden). Dadurch sehen alle drei Algorithmen zwangsläufig dieselbe
Aufteilung — das kann kein Modellskript versehentlich anders machen.

**Ergebnis:** die zwei finalen Dateien.

### `prep/s3_baselines.py` — die Messlatte festlegen

Rechnet drei einfache Vergleichswerte, **bevor** modelliert wird: Was kommt
heraus, wenn man einfach den Vormonatswert nimmt (naiv)? Wenn man den
Durchschnitt desselben Kalendermonats nimmt (saisonal)? Wenn man ein
klassisches Zähldatenmodell rechnet (Negative Binomial)?

Das gehört hierher und nicht zu den Modellen, weil hier nichts getunt und nichts
ausgewählt wird — es wird nur die Latte festgelegt (Auflage Schröter,
27.07.2026). Die Leitfrage dahinter: *Bringt der zusätzliche Aufwand der Modelle
überhaupt etwas?*

**Ergebnis:** `results/regression/baselines_folds.csv` und `baselines_mittel.csv`.
Aktuell liegt die Latte bei **RMSE 17,78** (naiv, Vormonatswert).

---

## Was am Ende rauskommt

### `data/processed/regression.parquet` — 4.620 × 24

Eine Zeile = ein Stadtteil in einem Monat. Zielgröße ist `anzahl_einsaetze`.

| Spalte | Was drinsteht |
|---|---|
| `stadtteil`, `jahr`, `monat`, `jahr_monat` | wer und wann |
| **`anzahl_einsaetze`** | **Zielgröße:** Einsätze in diesem Stadtteil-Monat |
| `median_haushaltseinkommen`, `armutsquote_pct`, `akademikerquote_pct`, `median_miete`, `leerstandsquote_pct` | Sozioökonomie aus dem ACS |
| `log_bevoelkerung` | Einwohnerzahl, logarithmiert (Größenkontrolle) |
| `log_kriminalitaetsindex` | Kriminalität relativ zum Stadtdurchschnitt, logarithmiert |
| `anteil_altbau_vor_1940_pct`, `anteil_wohngebaeude_pct`, `anteil_risikogewerbe_pct` | Bebauung |
| `monat_sin`, `monat_cos` | Jahreszeit |
| `lag_1`, `lag_12`, `rolling_mean_3` | Vergangenheitswerte der Zielgröße |
| `fold`, `ist_holdout` | die Aufteilung |
| `gesamtbevoelkerung`, `kriminalitaetsindex` | Rohwerte, **keine Modellmerkmale** — nur für den NegBin-Offset und die Beschreibung |

Zwei Merkmalssätze stehen in `config.py`:
**S** = Struktur + Jahreszeit (12 Merkmale) · **S+L** = zusätzlich die Lags (15).

### `data/processed/klassifikation.parquet` — 350.481 × 26

Eine Zeile = ein Einsatz. Zwei Zielgrößen: `einsatzart_gruppe` (4 Klassen,
Hauptvariante) und `ist_brand` (0/1, Robustheitslauf).

| Spalte | Was drinsteht |
|---|---|
| `einsatz_nummer`, `stadtteil`, `jahr`, `monat`, `jahr_monat` | wer und wann |
| **`einsatzart_gruppe`** | **Zielgröße:** Brand · Rettung/EMS · Technische Hilfe/Gefahr · Fehlalarm/Good Intent |
| **`ist_brand`** | **Zielgröße 2:** 1 = Brand, 0 = alles andere |
| dieselben 10 Stadtteilmerkmale wie oben | Block A |
| `stunde_sin`, `stunde_cos`, `monat_sin`, `monat_cos`, `ist_nacht`, `ist_wochenende`, `wochentag` | Block B: Zeitpunkt des Alarms |
| `fold`, `ist_holdout` | die Aufteilung |

Alle Merkmale sind `float64` (`wochentag` bleibt `int64`, weil kategorial). Es
gibt keine fehlenden Werte. Du kannst die Spalten direkt an Ridge, Random Forest
und XGBoost übergeben, ohne vorher etwas umzuwandeln.

**Was bewusst NICHT drin ist:** Sachschaden, Anzahl Löschfahrzeuge, Alarmstufe,
Antwortzeit, Verletzte. Das steht alles erst nach dem Einsatz fest — wer damit
die Einsatzart vorhersagt, betrügt sich selbst.

---

## Was noch in die sklearn-Pipeline gehört, nicht hierher

Skalierung, Imputation und Resampling werden **aus Daten gelernt** und müssen
deshalb innerhalb jedes Folds passieren, nicht vorher. Konkret:

- Ridge braucht `StandardScaler`, Zielgröße `log(1+y)` und `log(1+x)` auf den Lags
- die Klassifikation braucht One-Hot für `wochentag` und `class_weight="balanced"`

Das steht in den Modellskripten unter `modelle/`, nicht in `prep/`.

---

## Ablauf im Ganzen

```
DataSF / Census API
   │  prep/s1_daten.py       laden, säubern, zusammenkleben
   ▼  data/processed/einsaetze.parquet      720k × 50   (Zwischenstand)
   │
   │  prep/s2_datensaetze.py  zählen, Lags, Labels, Aufteilung
   ├──────────────────┬──────────────────
   ▼                  ▼
regression.parquet   klassifikation.parquet      ← ab hier modellfertig
 4.620 × 24           350.481 × 26
   │                  │
   │  prep/s3_baselines.py    naiv · saisonal · NegBin  → die Messlatte
   │                  │
   └────────┬─────────┘
            ▼
      modelle/m01_eignung.py     Eignungsurteil je Verfahren
      modelle/m02_regression.py  Ridge · RF · XGBoost
      modelle/m03_klassifikation.py
```

Absicherung: `python tests/test_aufbereitung.py` prüft 14 Eigenschaften direkt
an den fertigen Dateien — nicht am Code, der sie erzeugt hat.

---

## Gliederungsvorschlag Kapitel 5 (Data Preparation)

Vier Unterkapitel, Schwerpunkt auf den Joins (5.2) und der Konstruktion der
Variablen und Lags (5.3). Die Baseline steht in 5.4, wie von Schröter am
27.07.2026 gefordert.

**5 Data Preparation** — die Einleitung trägt Inhalt, keinen Blindabsatz: CRISP-DM-Verortung, die Festlegung der Analyseeinheit *Stadtteil × Monat* und der Grundsatz, dem das ganze Kapitel folgt — *jeder Prädiktor muss zum Prognosezeitpunkt tatsächlich verfügbar gewesen sein*. Dazu die Übersicht: vier Quellen, ein Befehl, zwei Datensätze.

**5.1 Datenauswahl und Bereinigung**
Warum diese vier Quellen. Strikte Spaltenauswahl: von 23 Einsatzspalten bleiben 6 — Sachschaden, Löschfahrzeuge, Alarmstufe und Antwortzeit stehen erst *nach* dem Einsatz fest und wären Leakage im engeren Sinn. Dedup (269 Zeilen), Antwortzeitfenster 0–60 min (~1,7 %), eine einzige Normalisierungsfunktion für Stadtteilnamen. Ausschluss von Analyseeinheiten: Parkgebiete ohne Wohnbevölkerung und Stadtteile ohne durchgängige ACS-Abdeckung → 41 auf 35.

**5.2 Zusammenführung der vier Datenquellen** ← *Fokus*
Das Grundproblem: vier Quellen, drei verschiedene Raumbezüge, zwei verschiedene Zeitraster. Drei Joins im Detail — **ACS** (Tract → Stadtteil über Crosswalk, bevölkerungsgewichtete Mediane; dann zeitbewusst über die Regel `acs_jahr ≤ Einsatzjahr − 1`, weil ein Jahrgang erst rund ein Jahr später erscheint), **Kriminalität** (zwei SFPD-Quellen mit Systembruch 05/2018, die ältere ohne Stadtteilspalte → Spatial Join gegen dieselbe Geometrie wie Land Use), **Land Use** (Parzellen-Centroid → Polygon, Snapshot 2020). Dazu die Join-Hygiene, die kartesische Produkte verhindert: Schlüsseleindeutigkeit vor jedem Merge, `validate=`, Match-Quoten, Zeilenzahl vor und nach. Als Limitation die ACS-Trefferquoten je Jahrgang (2009: 63,1 % — 2021/2023: 99,2 %). Ergebnis: eine Zeile je Einsatz, 719.989 × 50.

**5.3 Konstruktion der Merkmale und Lags** ← *Schwerpunkt*
Quoten aus Zählvariablen, Nenner ≤ 0 ergibt NaN statt Division durch Null. Der **Kriminalitätsindex als Location Quotient** mit Formel: rollierendes 12-Monats-Fenster, das im Vormonat endet — relativ, weil sich ein stadtweiter Niveausprung im Quotienten herauskürzt, und mit der Grenze dieses Arguments. **Exposure** über `log_bevoelkerung`, belegt durch die Vorzeichenumkehr der Armutsquote (+0,20 auf absolute Zahlen, −0,13 auf die Rate). Log-Transformationen einheitlich für alle drei Verfahren, weil sie für Baumverfahren wirkungsneutral sind (Fairness). Aggregation auf das vollständige Raster, Nullmonate als echte Nullen, `ffill` ohne `bfill`. Die **Lags**: `lag_1`, `lag_12`, `rolling_mean_3`, `shift` vor `rolling`, 12 Monate Vorlauf vor dem Analysebeginn — und die Lag-1-Autokorrelation von 0,96, die erklärt, warum ohne Lags kein Verfahren die naive Baseline schlägt. Saison als sin/cos statt Monat 1–12. Zielgrößen und die zwei Merkmalssätze S und S+L.

**5.4 Analyserahmen und Baselines**
Die Designmatrix: alles `float64`, keine fehlenden Werte, direkt übergebbar. Die zeitliche Aufteilung als Spalten in der Datei — dadurch ist die Fairness-Regel konstruktiv abgesichert statt nur behauptet. Dann die **Baselines als Referenz**: naiv, saisonal, Negative Binomial, mit Begründung der Wahl und der Diskussion, ob eine nichtlineare Baseline nötig ist. Ergebnis: Die Latte liegt bei RMSE 17,78 — und die Antwort auf *„bringt der Aufwand etwas?"* fällt für zwei der drei Verfahren negativ aus.
