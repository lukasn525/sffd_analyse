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
| `s3_baselines.py` | Vergleichswerte festlegen, bevor modelliert wird | `results/*/baselines_*.csv` |

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

| Zielgröße | Baseline | Macro-F1 | Accuracy |
|---|---|---|---|
| `dominante_einsatzart` | Mehrheitsklasse („Fehlalarm") | **0,223 ± 0,009** | 0,806 |

**Was die Negative Binomial stützt:** Sie kann Krümmung, aber keine
Wechselwirkungen zwischen Merkmalen. Genau die finden Random Forest und XGBoost
konstruktionsbedingt. Schlagen sie die Baseline, ist belegt, dass solche
Wechselwirkungen existieren und der Mehraufwand sich lohnt — schlagen sie sie
nicht, reicht die einfachere Struktur. Beides ist ein Ergebnis.

**Was offen zu benennen ist:** In der Klassifikation gibt es kein Pendant. Eine
Zahl kann keine von vier ungeordneten Kategorien vorhersagen; dort bleibt die
Mehrheitsklasse die einzige Referenz und die Latte liegt niedriger. Der Abstand
zwischen Macro-F1 0,223 und Accuracy 0,806 ist selbst ein Argument: Accuracy
sieht hervorragend aus, obwohl das Modell nichts kann.

Negative R² sind korrekt: Wer für einen unbekannten Stadtteil den
Gesamtdurchschnitt vorhersagt, liegt schlechter als dessen eigener Mittelwert.
Genau diese Lücke sollen die Strukturmerkmale schließen.

---

## 5. Was noch aussteht

| Punkt | Stand |
|---|---|
| NegBin-Referenz für die Rate | Code steht, Wert nach dem nächsten `build.py` eintragen |
| `results/eignungspruefung/` | vom 27.07., noch aus der Zeitschnitt-Welt |
| Linearitätsprüfung vor Ridge (R7) | Teil der Neufassung von `m01_eignung.py` |
| `modelle/m01`–`m04` | vollständig neu zu schreiben, Spezifikation in `04_MODELLIERUNG.md` |
