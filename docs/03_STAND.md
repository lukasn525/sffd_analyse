# Stand der Aufbereitung — der einzige Ort für Zahlen

> **Regel:** Jede Ergebniszahl der Arbeit steht hier und nur hier. Andere
> Dateien und die Thesis verweisen darauf, statt Werte abzuschreiben. Nach jedem
> `python prep/build.py` wird diese Datei einmal überschrieben.
>
> Stand **2026-08-03**, alle Werte an den erzeugten Dateien nachgerechnet.

```
python prep/build.py                 # zwei Datensätze + Baselines
python tests/test_aufbereitung.py    # zuletzt 19/19 bestanden
```

---

## 1. Was die Aufbereitung tut

| Schritt | Aufgabe | Ergebnis |
|---|---|---|
| `config.py` | alle Festlegungen an einer Stelle | – |
| `s1_daten.py` | vier Quellen laden, säubern, zu einer Zeile je Einsatz verbinden | `einsaetze.parquet` 719.989 × 50 |
| `s2_datensaetze.py` | auf Stadtteil × Monat aggregieren, Zielgrößen, Fold-Zuteilung | die zwei finalen Datensätze |
| `vorpruefung/v1_baselines.py` | Messlatte in zwei Stufen festlegen | `results/*/baselines_*.csv` |

Quellen: SFFD Feuerwehreinsätze · ACS (US-Zensus) · SFPD Kriminalität · Land Use 2020.

**Der tragende Grundsatz:** Jeder Einsatz bekommt nur Daten, die es zum Zeitpunkt
des Einsatzes schon gab — ACS mit einem Jahr Publikationsversatz (#11), der
Kriminalitätsindex rückwärtsgerichtet als rollierendes Fenster endend im Vormonat
(#17).

Die Aufteilung wird als Spalten `fold` und `ist_holdout` **in die Dateien
geschrieben**. Dadurch sehen alle Verfahren zwangsläufig dieselben Folds — die
Fairness-Regel ist konstruktiv abgesichert, nicht behauptet.

---

## 2. Die beiden finalen Datensätze

Beide: **2015-01 bis 2025-12**, 132 Monate, **35 Stadtteile**, Analyseeinheit
Stadtteil × Monat, **keine fehlenden Werte**, Merkmale durchgehend `float64`.

| Datei | Zeilen × Spalten | Zielgrößen |
|---|---|---|
| `regression.parquet` | **4.620 × 25** | `anzahl_einsaetze` · `einsaetze_je_1000_ew` |
| `klassifikation.parquet` | **4.619 × 29** | `dominante_einsatzart` (4 Klassen) |

4.620 = 35 × 132, lückenlos. Dem Klassifikationsdatensatz fehlt ein
Stadtteil-Monat ohne Einsatz, dessen Anteile 0/0 wären.

**Zwölf Modellmerkmale**, in beiden identisch: zehn Strukturmerkmale plus
`monat_sin`/`monat_cos`. Spaltenbeschreibung im Data Dictionary; nicht als
Merkmal verwendet werden die Lags, `gesamtbevoelkerung` und
`kriminalitaetsindex` (Offset und Deskription).

**Klassenverteilung** von `dominante_einsatzart` — stark schief, deshalb ist
Accuracy hier wertlos:

| Fehlalarm | Techn. Hilfe | Rettung/EMS | Brand |
|---|---|---|---|
| 79,0 % | 16,3 % | 3,1 % | 1,5 % |

**Ausgeschlossen:** 3 Parkgebiete ohne nennenswerte Wohnbevölkerung (#19), 3
Stadtteile ohne durchgängige ACS-Abdeckung (#15). Nicht enthalten sind sämtliche
Ergebnisvariablen (Sachschaden, Löschfahrzeuge, Alarmstufe, Antwortzeit) — sie
stehen erst nach dem Einsatz fest.

### Größenordnungen

| | Wert |
|---|---|
| Einsätze je Stadtteil-Monat, Mittel | 75,9 |
| kleinster Stadtteil (Seacliff) | 6,4 |
| größter Stadtteil (Tenderloin) | 279,7 |
| Autokorrelation Lag 1, innerhalb Stadtteil | 0,368 |
| Dispersionsindex Var/Mean, voller Datensatz | 62,8 |
| Dispersionsindex Var/Mean, Trainingsstadtteile Fold 1 | 54,2 |

Beide Dispersionswerte sind korrekt und beziehen sich auf unterschiedliche
Mengen — der Wert aus der Eignungsprüfung (54,2) ist der auf den
Trainingsstadtteilen, weil dort keine Testinformation einfließen darf.

---

## 3. Der Validierungsrahmen

**Stadtteil-Split, kein Zeitschnitt** (#29). Getestet wird auf Stadtteilen, von
denen das Modell keinen einzigen Monat gesehen hat.

| | Stadtteile |
|---|---|
| Hold-out (`ist_holdout == 1`) | 6 |
| Fold 1–5 | 6 · 6 · 6 · 6 · 5 |
| **Entwicklung insgesamt** | **29** |

Kein Stadtteil liegt in mehr als einem Fold; ein Teststadtteil wird mit allen
132 Monaten getestet. Zuteilung stratifiziert nach Bevölkerung und zusätzlich
nach der seltensten Klasse (#30) — Brand-Testfälle je Fold: 13 · 9 · 6 · 3 · 2.

**Warum kein Zeitschnitt:** **92,5 %** der Varianz von `anzahl_einsaetze` liegen
*zwischen* den Stadtteilen. Bei einem Zeitschnitt stünde jeder Stadtteil in
Training und Test, das Modell kennte sein Niveau bereits — die Forschungsfrage
wäre nicht geprüft.

**Fold-Größen und Extrapolation:**

| Fold | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| Trainingszeilen | 3.036 | 3.036 | 3.036 | 3.036 | 3.168 |
| Testzeilen | 792 | 792 | 792 | 792 | 660 |
| Testzeilen außerhalb des Trainings-Wertebereichs | 40,9 % | 33,3 % | 57,4 % | 33,3 % | **3,6 %** |

Im Mittel liegen **33,7 %** der Testzeilen in mindestens einem Merkmal außerhalb
der Spanne, die das Modell im Training gesehen hat. Das trifft Ridge und die
Baumverfahren unterschiedlich: Ridge rechnet linear weiter, Bäume ordnen dem
letzten Blatt zu. Die Spanne von 3,6 % bis 57,4 % erklärt einen erheblichen Teil
der Fold-Streuung und ist ein Grund für die wiederholten Splits.

Das Hold-out wird **genau einmal** ausgewertet, nach Abschluss von Modellwahl
und Tuning.

---

## 4. Die Baselines

Festgelegt in Decision Log #32. Sie laufen über denselben Split und sehen
dieselben Merkmale wie die Modelle.

| Zielgröße | Baseline | R² | RMSE | MAE |
|---|---|---|---|---|
| `anzahl_einsaetze` | **Negative Binomial** | **0,472 ± 0,368** | 37,44 | 25,71 |
| `anzahl_einsaetze` | Gesamtmittelwert (Nullmarke) | −0,832 | 71,19 | 53,27 |
| `einsaetze_je_1000_ew` | **Negative Binomial** | **−0,237 ± 1,682** | 4,14 | 2,42 |
| `einsaetze_je_1000_ew` | Gesamtmittelwert (Nullmarke) | −2,122 | 7,45 | 5,01 |

Fold-Ergebnisse der Negative Binomial:

| Fold | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| R² `anzahl_einsaetze` | 0,70 | −0,17 | 0,60 | 0,50 | 0,73 |
| R² `einsaetze_je_1000_ew` | 0,75 | −0,02 | 0,62 | **−3,19** | 0,66 |

**Auf der Rate ist R² kein tragfähiges Hauptmaß.** Der Mittelwert ist negativ,
obwohl die Negative Binomial die Nullmarke in **jedem einzelnen Fold** bei RMSE
schlägt (4,71/10,34 · 1,54/4,09 · 8,78/15,01 · 3,75/4,03 · 1,94/3,78). Ursache:
R² misst gegen den Mittelwert der *Testdaten*. Die Rate streut zwischen den
Stadtteilen um den Faktor 32 (Excelsior 1,04 · Financial District 33,80), also
liegt der Testmittelwert je nach Fold weit vom Trainingsmittelwert entfernt. In
Fold 4 kippt R² dadurch auf −3,19, während RMSE weiter besser ist als die
Nullmarke.

**Konsequenz für Kapitel 7:** Bei der Rate ist RMSE bzw. MAE zu berichten und
R² nur nachrichtlich — mit dieser Begründung. Bei `anzahl_einsaetze` bleibt R²
aussagekräftig.

### Klassifikation — `dominante_einsatzart`

| Stufe | Baseline | Macro-F1 | Accuracy |
|---|---|---|---|
| 1 | Mehrheitsklasse („Fehlalarm") | 0,223 | **0,806** |
| 2 | **Multinomiale logistische Regression** | **0,290** | 0,578 |

Seit #33 hat auch die Klassifikation eine Stufe 2 — eine Referenz, die dieselben
zwölf Merkmale benutzt. **Stufe 2 ist die Latte, die Random Forest und XGBoost
schlagen müssen**, nicht die Mehrheitsklasse. Die Negative Binomial ist hier
nicht anwendbar (sie sagt eine Zahl vorher, die Zielgröße ist eine von vier
ungeordneten Kategorien); die logistische Regression ist ihr Gegenstück.

**Der Vergleich der beiden Zeilen ist selbst ein Argument.** Die logistische
Regression hat die deutlich *schlechtere* Trefferquote (0,578 gegen 0,806) und
zugleich das deutlich *bessere* Macro-F1. Das ist kein Widerspruch, sondern die
Wirkung von `class_weight="balanced"`: Das Modell gibt Treffer bei der
dominanten Klasse auf, um die drei seltenen überhaupt zu finden. Wer Accuracy
als Hauptmaß nähme, käme zu dem Schluss, das Modell sei schlechter geworden.
Genau deshalb ist Macro-F1 maßgeblich.

**Was Stufe 2 stützt.** In der Regression: Die Negative Binomial kann Krümmung,
aber keine Wechselwirkungen — genau die finden RF und XGBoost
konstruktionsbedingt. Schlagen sie die Latte, ist der Mehraufwand belegt;
schlagen sie sie nicht, reicht die einfachere Struktur. Beides ist ein Ergebnis.

**Was in der Klassifikation offen ist.** Dort schlägt ein flacher
Entscheidungsbaum (Macro-F1 0,270) die logistische Regression **nicht**. Der
Mehraufwand von RF und XGBoost ist im zweiten Strang vorab nicht begründet —
siehe `06_RISIKEN.md`, R-2.

Negative R² sind korrekt: Wer für einen unbekannten Stadtteil den
Gesamtdurchschnitt vorhersagt, liegt schlechter als dessen eigener Mittelwert.
Genau diese Lücke sollen die Strukturmerkmale schließen.

---

## 5. Was noch aussteht

| Punkt | Stand |
|---|---|
| Ertrag des Klassifikationsstrangs | Stufe 2 schöpft nur 0,067 von maximal 1,0 aus — der Strang trägt voraussichtlich weniger als erhofft (`06_RISIKEN.md`, R-2) |
| Linearitätsprüfung vor Ridge (R7) | ✅ gerechnet, `results/eignungspruefung/` |
| Abweichungen mit Schröter besprechen | offen — `06_RISIKEN.md`, R-7 |
| `modelle/m01`–`m04` | vollständig neu zu schreiben, Spezifikation in `04_MODELLIERUNG.md` |
