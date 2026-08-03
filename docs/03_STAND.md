# Stand der Aufbereitung — der einzige Ort für Zahlen

> **Regel für dieses Dokument:** Jede Zahl, die in der Arbeit auftaucht, steht
> hier — und **nur** hier. Andere Dateien verweisen hierher, statt Werte
> abzuschreiben. Nach jedem `python prep/build.py` wird diese Datei einmal
> überschrieben, dann stimmt der Rest von allein.
>
> Stand: 2026-08-03, nach dem Lauf vom selben Tag. Alle Werte unten sind an den
> erzeugten Dateien nachgerechnet, nicht aus älteren Fassungen übernommen.

Ein Befehl, zwei fertige Datensätze plus die Vergleichswerte:

```
python prep/build.py
python tests/test_aufbereitung.py     # 19 Prüfungen, zuletzt 19/19
```

---

## 1. Die vier Bausteine

### `prep/config.py` — die Einstellungen

Kein Arbeitsschritt, sondern die Sammelstelle aller Festlegungen: Analysezeitraum,
ausgeschlossene Stadtteile, Merkmalsliste, Anzahl Folds, Suchräume fürs Tuning.
Wenn etwas geändert wird, dann hier und nur hier.

### `prep/s1_daten.py` — Rohdaten holen und zusammenführen

Vier Quellen werden zu **einer Tabelle mit einer Zeile je Einsatz** verbunden:

| Quelle | Was daraus kommt |
|---|---|
| SFFD Feuerwehreinsätze | die Einsätze selbst |
| ACS (US-Zensus) | Einkommen, Armut, Bildung, Miete, Leerstand, Einwohner |
| SFPD Kriminalität | Kriminalitätsindex je Stadtteil und Monat |
| Land Use 2020 | Gebäudealter, Wohnanteil, Anteil Risikogewerbe |

Unterwegs: doppelte Einsatznummern raus, unplausible Antwortzeiten raus,
Stadtteilnamen vereinheitlichen, Quoten berechnen, Spalten eindeutschen.

**Der wichtigste Punkt:** Jeder Einsatz bekommt nur Daten, die es zum Zeitpunkt
des Einsatzes schon gab. Ein Einsatz aus 2023 bekommt den ACS-Jahrgang 2021,
weil die Zahlen für 2023 erst Ende 2024 erschienen (Decision Log #11).

**Ergebnis:** `data/processed/einsaetze.parquet` — **719.989 × 50**.
Zwischenstand, kein Endprodukt.

### `prep/s2_datensaetze.py` — die beiden finalen Datensätze

Beide liegen auf derselben Analyseeinheit: **ein Stadtteil in einem Monat**.

- **Regression** zählt die Einsätze je Stadtteil und Monat.
- **Klassifikation** misst die *Zusammensetzung* derselben Einsatzlast: vier
  NFIRS-Gruppen als Anteile, daraus per `argmax` die dominante Einsatzart.

Hier entsteht außerdem die Aufteilung, die als Spalten `fold` und `ist_holdout`
**in die Dateien geschrieben** wird. Dadurch sehen alle Verfahren zwangsläufig
dieselben Folds — die Fairness-Regel ist konstruktiv abgesichert, nicht bloß
behauptet.

### `prep/s3_baselines.py` — die Messlatte

Legt fest, was die Verfahren mindestens schlagen müssen, **bevor** modelliert
wird (Auflage Schröter, 27.07.2026). Hier wird nichts getunt und nichts
ausgewählt. Werte in Abschnitt 4.

---

## 2. Was am Ende herauskommt

Beide Dateien: **2015-01 bis 2025-12**, 132 Monate, **35 Stadtteile**,
**keine fehlenden Werte**, Merkmale durchgehend `float64`.

### `data/processed/regression.parquet` — 4.620 × 25

4.620 = 35 Stadtteile × 132 Monate, lückenlos.

| Spalte(n) | Rolle |
|---|---|
| `stadtteil`, `jahr`, `monat`, `jahr_monat` | Schlüssel |
| **`anzahl_einsaetze`** | **Zielgröße 1** — Zähldaten |
| **`einsaetze_je_1000_ew`** | **Zielgröße 2** — Rate |
| `median_haushaltseinkommen`, `armutsquote_pct`, `akademikerquote_pct`, `median_miete`, `leerstandsquote_pct` | Sozioökonomie (ACS) |
| `log_bevoelkerung` | Größenkontrolle |
| `log_kriminalitaetsindex` | Kriminalität relativ zum Stadtdurchschnitt |
| `anteil_altbau_vor_1940_pct`, `anteil_wohngebaeude_pct`, `anteil_risikogewerbe_pct` | Bebauung |
| `monat_sin`, `monat_cos` | Jahreszeit |
| `lag_1`, `lag_12`, `rolling_mean_3` | **kein Modellmerkmal** — siehe unten |
| `gesamtbevoelkerung`, `kriminalitaetsindex` | Rohwerte, **kein Modellmerkmal** — NegBin-Offset und Deskription |
| `fold`, `ist_holdout` | die Aufteilung |

**Die zwölf Modellmerkmale** sind die zehn Strukturmerkmale plus `monat_sin` und
`monat_cos`. In beiden Datensätzen identisch.

**Warum die Lags kein Merkmal sind:** Unter dem Stadtteil-Split wären sie die
eigene Vergangenheit des Teststadtteils — das Modell bekäme sein Niveau frei
Haus und die Strukturmerkmale müssten nichts erklären. Sie bleiben in der Datei
für eine klar gekennzeichnete Nebenbemerkung zur zeitlichen Prognose
(Decision Log #29).

### `data/processed/klassifikation.parquet` — 4.619 × 29

Eine Zeile fehlt gegenüber der Regression: ein Stadtteil-Monat ohne Einsatz,
dessen Anteile 0/0 wären.

| Spalte(n) | Rolle |
|---|---|
| `stadtteil`, `jahr`, `monat`, `jahr_monat`, `anzahl_einsaetze` | Schlüssel und Bezugsgröße |
| **`dominante_einsatzart`** | **Zielgröße** — 4 Klassen, per `argmax` über die Anteile |
| `anzahl_brand`, `anzahl_rettung_ems`, `anzahl_technische_hilfe`, `anzahl_fehlalarm` | Deskription |
| `anteil_brand`, `anteil_rettung_ems`, `anteil_technische_hilfe`, `anteil_fehlalarm` | Rechenbasis der Zielgröße, **keine eigene Zielgröße** |
| dieselben 12 Modellmerkmale wie oben | |
| `gesamtbevoelkerung` | Rohwert, kein Merkmal |
| `fold`, `ist_holdout` | die Aufteilung |

**Klassenverteilung** (stark schief, deshalb ist Accuracy hier wertlos):

| Klasse | Anteil der Stadtteil-Monate |
|---|---|
| Fehlalarm/Good Intent | 79,0 % |
| Technische Hilfe/Gefahr | 16,3 % |
| Rettung/EMS | 3,1 % |
| Brand | 1,5 % |

**Was bewusst nicht drin ist:** Sachschaden, Löschfahrzeuge, Löschkräfte,
Alarmstufe, Antwortzeit, Verletzte. Alles steht erst nach dem Einsatz fest.

---

## 3. Der Validierungsrahmen

**Stadtteil-Split, kein Zeitschnitt** (Decision Log #29). Getestet wird auf
Stadtteilen, von denen das Modell keinen einzigen Monat gesehen hat.

| | Stadtteile |
|---|---|
| Hold-out (`ist_holdout == 1`) | 6 |
| Fold 1–5 | 6 · 6 · 6 · 6 · 5 |
| **Entwicklung insgesamt** | **29** |

Kein Stadtteil liegt in mehr als einem Fold. Ein Teststadtteil wird mit allen
132 Monaten getestet.

**Warum kein Zeitschnitt:** **92,5 %** der Varianz von `anzahl_einsaetze` liegen
*zwischen* den Stadtteilen. Bei einem Zeitschnitt stünde jeder Stadtteil in
Training und Test, das Modell kennte sein Niveau bereits, und die Forschungsfrage
wäre nicht geprüft.

**Die Streuung ist groß, und das ist die Aussage.** Bei 29 Stadtteilen hängt das
Ergebnis erheblich davon ab, welche sechs gerade im Test liegen — die
Fold-Ergebnisse der Negative Binomial reichen von R² −0,17 bis 0,73. Berichtet
wird deshalb Mittelwert ± Standardabweichung, nie ein Punktwert (Gutachten R6).

Das Hold-out wird **genau einmal** ausgewertet, nach Abschluss von Modellwahl
und Tuning.

### Größenordnungen zur Einordnung

| | Wert |
|---|---|
| Einsätze je Stadtteil-Monat, Mittel | 75,9 |
| kleinster Stadtteil (Seacliff) | 6,4 |
| größter Stadtteil (Tenderloin) | 279,7 |
| Autokorrelation Lag 1, innerhalb Stadtteil | 0,368 |

---

## 4. Die Baselines

Festgelegt in Decision Log #32. Sie laufen über denselben Stadtteil-Split wie
die Modelle und sehen dieselben Merkmale.

### Regression — Negative Binomial

Ein vollwertiges Zähldatenmodell mit denselben zwölf Merkmalen, denselben
Zeilen, denselben Folds. `log(Bevölkerung)` geht als **Offset** ein: geschätzt
werden Einsätze je Einwohner, nicht die Stadtteilgröße (Decision Log #13).

| Zielgröße | Baseline | R² | RMSE | MAE |
|---|---|---|---|---|
| `anzahl_einsaetze` | **Negative Binomial** | **0,472 ± 0,368** | 37,44 | 25,71 |
| `anzahl_einsaetze` | Gesamtmittelwert (Nullmarke) | −0,832 | 71,19 | 53,27 |
| `einsaetze_je_1000_ew` | Negative Binomial | *steht nach dem nächsten Lauf* | | |
| `einsaetze_je_1000_ew` | Gesamtmittelwert (Nullmarke) | −2,122 | 7,45 | 5,01 |

Fold-Ergebnisse der Negative Binomial auf `anzahl_einsaetze`:

| Fold | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| R² | 0,70 | −0,17 | 0,60 | 0,50 | 0,73 |

**Was sie stützt:** Die Negative Binomial kann Krümmung, aber keine
Wechselwirkungen zwischen Merkmalen. Random Forest und XGBoost finden
Wechselwirkungen konstruktionsbedingt. Schlagen sie die Baseline, ist belegt,
dass solche Wechselwirkungen existieren und der Mehraufwand sich lohnt.
Schlagen sie sie nicht, reicht die einfachere Struktur — ebenfalls ein
Ergebnis (Gutachten R6).

Der Gesamtmittelwert ist kein Gegner, sondern der Bezugspunkt, der R² lesbar
macht: R² = 0 heißt „so gut wie der Durchschnitt der Testwerte", negativ heißt
schlechter. Dass er negativ ausfällt, ist unter einem Stadtteil-Split korrekt —
der Trainingsdurchschnitt ist nicht der Testdurchschnitt.

### Klassifikation — Mehrheitsklasse

| Baseline | Macro-F1 | Accuracy |
|---|---|---|
| Mehrheitsklasse (immer „Fehlalarm") | **0,223 ± 0,009** | 0,806 |

**Und das gehört in den Text:** Die Negative Binomial ist hier nicht anwendbar.
Sie sagt eine Zahl vorher, die Zielgröße ist eine von vier ungeordneten
Kategorien — es gibt weder eine Umrechnung noch einen Schwellenwert. Die
Referenz der Klassifikation ist damit **schwächer als die der Regression**. Das
ist vertretbar, solange es dasteht.

Der Abstand zwischen Macro-F1 0,223 und Accuracy 0,806 ist selbst ein Argument:
Accuracy sieht hervorragend aus, obwohl das Modell nichts kann.

---

## 5. Was bewusst nicht in `prep/` passiert

Skalierung, Imputation und Klassengewichtung werden **aus Daten gelernt** und
müssen deshalb innerhalb jedes Folds passieren, nicht vorher. Sie gehören in die
sklearn-Pipeline in `modelle/`:

- Ridge braucht `StandardScaler` und die Zielgröße als `log(1+y)`
- die Klassifikation braucht `class_weight="balanced"` bzw. `sample_weight`
- `XGBClassifier` braucht Integer-Labels (Encoder einmal global fitten)

Beide Klassifikationsverfahren sind baumbasiert und brauchen **keine
Skalierung** (Decision Log #31).

---

## 6. Ablauf im Ganzen

```
DataSF / Census API
   │  prep/s1_daten.py        laden, säubern, zusammenführen
   ▼  einsaetze.parquet                 719.989 × 50   (Zwischenstand)
   │
   │  prep/s2_datensaetze.py  aggregieren, Anteile, Lags, Fold-Zuteilung
   ├──────────────────────┬──────────────────────
   ▼                      ▼
regression.parquet     klassifikation.parquet     ← ab hier modellfertig
   4.620 × 25             4.619 × 29
   │                      │
   │  prep/s3_baselines.py    NegBin · Mehrheitsklasse  → die Messlatte
   └──────────┬───────────┘
              ▼
        modelle/m01_eignung.py    Eignungsurteil je Verfahren
        modelle/m02_menge.py      Ridge · RF · XGBoost
        modelle/m03_struktur.py   RF · XGBoost
        modelle/m04_shap.py       nur für Modelle mit Signal
```

**Absicherung:** `tests/test_aufbereitung.py` prüft **19 Eigenschaften** direkt
an den fertigen Dateien — nicht am Code, der sie erzeugt hat. Zuletzt 19/19.

---

## 7. Was noch aussteht

| Punkt | Stand |
|---|---|
| NegBin-Referenz für `einsaetze_je_1000_ew` | Code steht, Wert nach dem nächsten `build.py` eintragen |
| `results/eignungspruefung/` | vom 27.07., noch aus der Zeitschnitt-Welt — mit `m01_eignung.py` neu zu rechnen |
| Linearitätsprüfung vor Ridge (R7) | Teil der Neufassung von `m01_eignung.py` |
| `modelle/m01`–`m03` | beziehen sich auf die alte Datenstruktur, vollständig neu zu schreiben (siehe `04_MODELLIERUNG.md`) |
