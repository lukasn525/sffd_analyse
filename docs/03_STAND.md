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
der Spanne, die das Modell im Training gesehen hat.

**Korrigiert am 06.08.2026** (`07_BEFUNDE.md`, B-31 und B-32). Hier stand
zuvor, die Spanne erkläre „einen erheblichen Teil der Fold-Streuung" und treffe
Ridge und die Baumverfahren unterschiedlich. **Beides ist gemessen und nicht
haltbar:**

- Extrapolationsanteil gegen RMSE: Spearman ρ 0,14–0,31, für Ridge (0,184) und
  Random Forest (0,185) praktisch identisch — also rund 3 % der Rangvarianz.
- Rückstand der Baumverfahren gegenüber Ridge, korreliert mit dem
  Extrapolationsanteil: ρ **+0,020** und **+0,011** (p ≈ 0,9). Kein
  Zusammenhang. Der Rückstand beträgt konstant rund 20 RMSE, unabhängig davon,
  ob ein Fold 3,6 % oder 57,4 % Extrapolation aufweist.

Die Extrapolation bleibt eine Eigenschaft des Validierungsrahmens und begrenzt
die Generalisierbarkeit — sie erklärt aber **nicht** den Verfahrensunterschied.

**Sie ist zudem eine Eigenschaft von Stadtteilen, nicht von Zeilen.** Weil die
Strukturmerkmale innerhalb eines Stadtteils nahezu konstant sind, bricht ein
Stadtteil ganz aus oder gar nicht: **9 von 29** liegen mit 100 % ihrer Zeilen
außerhalb (Chinatown, South Of Market, Financial District/South Beach, Marina,
Haight Ashbury, Sunset/Parkside, Twin Peaks, Seacliff, Presidio), **16 von 29**
bei 0 %. Kein einzelnes Merkmal dominiert — die Anteile reichen von 10,0 %
(`anteil_risikogewerbe_pct`) bis 4,1 % (`akademikerquote_pct`), die
Saisonmerkmale brechen nie aus.

Das Hold-out wird **genau einmal** ausgewertet, nach Abschluss von Modellwahl
und Tuning.

---

## 4. Die Baselines

Festgelegt in Decision Log #32. Sie laufen über denselben Split und sehen
dieselben Merkmale wie die Modelle.

Seit **05.08.2026** laufen die Baselines über **alle 10 Wiederholungen**, also
über dieselben 50 Läufe wie die Vergleichsverfahren. Nur so lässt sich gepaart
testen (`07_BEFUNDE.md`, B-4). Beide Fassungen stehen in
`baselines_mittel.csv`, unterschieden durch die Spalte `basis`.

**Maßgeblich — 50 Läufe** (`basis = alle_wiederholungen`). Streuung ist
`std_wiederholungen` über die 10 Wiederholungsmittel, nicht `std_folds`:

| Zielgröße | Baseline | R² | RMSE | MAE |
|---|---|---|---|---|
| `anzahl_einsaetze` | **Negative Binomial** | **0,477 ± 0,086** | 37,27 ± 3,23 | 24,79 |
| `anzahl_einsaetze` | Gesamtmittelwert (Nullmarke) | −0,744 ± 0,325 | 69,93 | 52,33 |
| `einsaetze_je_1000_ew` | **Negative Binomial** | **0,024 ± 0,679** | 4,41 ± 0,59 | 2,43 |
| `einsaetze_je_1000_ew` | Gesamtmittelwert (Nullmarke) | −1,054 ± 0,875 | 7,54 | 4,92 |

**Warum über zehn Wiederholungen und nicht über eine Aufteilung.** Bei der Rate
steigt R² von **−0,237 auf +0,024**, sobald über zehn Fold-Konstellationen
gemittelt wird. Der negative Wert war kein Modellbefund, sondern Fold 4 einer
einzelnen Aufteilung (R² −3,19). Das zeigt, wie stark ein einzelner Fold bei 29
Einheiten durchschlägt — und ist der Grund für die wiederholten Splits (R-5).

**Der Offset-Vorteil der Negative Binomial ist gemessen und beträgt null.**
Eine zweite Variante ohne Offset (`log_bevoelkerung` als gewöhnlicher
Prädiktor) liegt bei `anzahl_einsaetze` um **0,0017 RMSE besser**, bei der Rate
um 0,0000. Grund: `log_bevoelkerung` ist auch in der Offset-Variante ein freies
Merkmal. Damit entfällt R-9 (`07_BEFUNDE.md`, B-19).

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

| Stufe | Baseline | Macro-F1 | Macro-AUROC | Accuracy |
|---|---|---|---|---|
| 1 | Mehrheitsklasse („Fehlalarm") | 0,223 | – | **0,806** |
| 2 | **Multinomiale logistische Regression** | **0,298** | 0,711 | 0,588 |

Über 50 Läufe gerechnet; auf den 5 Folds der Wiederholung 0 lauteten die Werte
0,223 und **0,290** / 0,578. Macro-AUROC wird seit 05.08.2026 mitgeführt — ohne
sie gäbe es für das zweite Gütemaß der Klassifikation keine Messlatte. Für die
Mehrheitsklasse ist sie nicht definiert und bleibt leer, nicht 0,5.
Konvergenzwarnungen der logistischen Regression: **0 von 50 Läufen**.

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

## 5. Die Modellergebnisse

> Stand **06.08.2026**, erster vollständiger Lauf auf der Zielmaschine.
> 10 Wiederholungen × 5 Folds, Tuning einmal auf Wiederholung 0, Budget 50.
> Alle Modelle einkernig gefittet. **Hold-out unberührt.**
> Streuung ist `std_wiederholungen` über die 10 Wiederholungsmittel.

### 5.0 Die Spezifikation — eine, für alle Modelle gleich

| | |
|---|---|
| Zielgrößen | `anzahl_einsaetze`, `einsaetze_je_1000_ew` |
| Exposition | **alle Verfahren modellieren die Rate**, für die absolute Zahl wird mit der Einwohnerzahl zurückmultipliziert (#43) |
| Verlustfunktion | Ridge `log(1+y)` · Random Forest `criterion="poisson"` · XGBoost `reg:tweedie` mit getuntem Varianzexponenten (#42) |
| Validierung | Stadtteil-Split, 10 Wiederholungen × 5 Folds, Tuning einmal auf Wiederholung 0 |
| Aufwandsmessung | einkernig für alle Verfahren, Parallelisierungsgewinn getrennt (#39/#40) |

Ein Lauf, ein Befehl, keine Ausnahmen. Die Gegenprobe ohne
Expositionsbehandlung ist kein zweiter Betriebsmodus, sondern eine Ablation
(Abschnitt 5.5).

### 5.1 Menge — Ergebnisse

> ⚠️ **Die Zahlen dieses Abschnitts stammen noch aus dem Lauf vom 06.08.2026
> ohne Expositionsbehandlung und werden nach dem nächsten `m02`-Lauf ersetzt.**
> Vorabmessung auf Wiederholung 0 mit der neuen Spezifikation:
> XGBoost 35,74 · Random Forest 36,43 · Ridge 37,25 gegen eine Baseline von
> 37,44 — alle drei Verfahren vor der Baseline, Rangfolge XGBoost > Random
> Forest > Ridge. Ob die Abstände den gepaarten Wilcoxon überstehen, ist offen.

| Zielgröße | Verfahren | RMSE | MAE | R² | Trainingszeit |
|---|---|---|---|---|---|
| `anzahl_einsaetze` | **Negative Binomial** | **37,27 ± 3,23** | 24,79 | 0,477 | – |
| | Ridge | 38,31 ± 3,10 | 25,16 | 0,522 | 0,006 s |
| | XGBoost | 54,95 ± 4,21 | 36,26 | 0,024 | 0,986 s |
| | Random Forest | 58,69 ± 4,02 | 41,94 | −0,484 | 2,726 s |
| `einsaetze_je_1000_ew` | **Negative Binomial** | **4,41 ± 0,59** | 2,43 | 0,024 | – |
| | Random Forest | 4,19 ± 0,63 | 2,43 | 0,251 | 5,447 s |
| | XGBoost | 4,37 ± 0,43 | 2,41 | 0,496 | 1,234 s |
| | Ridge | 4,68 ± 0,24 | 2,50 | 0,283 | 0,006 s |

**Primäraussage nach #34** — gepaarter Wilcoxon auf den 10 Wiederholungsmitteln,
positive Differenz heißt das Verfahren ist besser:

| Zielgröße | Verfahren gegen Baseline | Differenz | gewonnen | p | Befund |
|---|---|---|---|---|---|
| `anzahl_einsaetze` | Ridge | −1,04 | 2/10 | 0,084 | nicht unterscheidbar |
| | Random Forest | −21,42 | 0/10 | 0,002 | signifikant schlechter |
| | XGBoost | −17,68 | 0/10 | 0,002 | signifikant schlechter |
| `einsaetze_je_1000_ew` | Random Forest | +0,22 | 5/10 | 0,275 | nicht unterscheidbar |
| | XGBoost | +0,04 | 6/10 | 0,846 | nicht unterscheidbar |
| | Ridge | −0,27 | 1/10 | 0,049 | signifikant schlechter |

**Verfahrensvergleich** (sekundär, Holm über 6 Tests):

| Zielgröße | Paarung | Differenz | p_holm | Befund |
|---|---|---|---|---|
| `anzahl_einsaetze` | Ridge – Random Forest | 20,38 | 0,012 | Ridge besser |
| | Ridge – XGBoost | 16,64 | 0,012 | Ridge besser |
| | Random Forest – XGBoost | −3,74 | 0,016 | XGBoost besser |
| `einsaetze_je_1000_ew` | alle drei Paarungen | – | ≥ 0,111 | nicht unterscheidbar |

Rangfolge bei `anzahl_einsaetze`: **Ridge > XGBoost > Random Forest**. Bei der
Rate überlappen die Streuungsbereiche, dort ist keine Rangfolge zulässig (R-6).

**Nebeneffekt der Verlustfunktion (#42):** Es gibt **keine negativen
Vorhersagen** mehr. Tweedie und Poisson haben eine Log-Verknüpfung und können
nicht unter null fallen — die Zielgröße wird strukturell respektiert statt
nachträglich geprüft.

### 5.2 Struktur — beide Verfahren schlagen die Stufe-2-Baseline

| Verfahren | Macro-F1 | Macro-AUROC | Accuracy | Trainingszeit |
|---|---|---|---|---|
| Mehrheitsklasse (Stufe 1) | 0,223 | – | 0,806 | – |
| **Logistische Regression (Stufe 2)** | **0,298** | 0,711 | 0,588 | – |
| Random Forest | 0,3276 ± 0,0129 | 0,735 | 0,761 | 2,097 s |
| XGBoost | 0,3343 ± 0,0128 | 0,751 | 0,754 | 1,763 s |

| Paarung | Differenz | gewonnen | p | Befund |
|---|---|---|---|---|
| Random Forest gegen Stufe 2 | +0,0296 | 10/10 | 0,002 | **signifikant besser** |
| XGBoost gegen Stufe 2 | +0,0362 | 9/10 | 0,004 | **signifikant besser** |
| Random Forest – XGBoost | −0,0067 | 2/10 | 0,131 | nicht unterscheidbar |

Keine Korrektur beim Verfahrensvergleich — die Familie besteht aus einem Test
(#38). Kein Lauf ohne definierte Macro-AUROC.

### 5.3 Der Kernbefund

**Derselbe Datensatz, dieselben Merkmale, dieselben Folds — und die Antwort
kehrt sich um.** In der Menge lohnt sich der Mehraufwand nicht, in der Struktur
schon. Erklärung in `07_BEFUNDE.md`, B-30: In der Menge entscheidet
Extrapolation (33,7 % der Testzeilen außerhalb des Trainingsbereichs), und
dort sind parametrische Modelle im Vorteil. In der Struktur entscheidet die
Form der Klassengrenze, und dort sind flexible Verfahren im Vorteil.

### 5.4 Aufwand und Reproduzierbarkeit

Alle Zeiten je Fold, **einkernig gemessen** (#40):

| | Ridge | Random Forest | XGBoost |
|---|---|---|---|
| Training, Menge | 0,005–0,006 s | 1,89–4,49 s | 0,82–1,00 s |
| Training, Struktur | – | 2,10 s | 1,76 s |
| Parallelisierungsgewinn | 1,02–1,45 | 1,65–2,18 | **0,64–0,77** |

Ridge ist bei bester oder gleichwertiger Güte **300- bis 800-mal schneller** als
die Ensembles. Bei XGBoost liegt der Parallelisierungsgewinn **unter 1** — der
Fit über alle Kerne dauert länger als der einkernige (B-28).

**XGBoost ist nicht threaddeterministisch.** Bei anderer Kernzahl weichen die
Vorhersagen ab: bis 34,7 bei `anzahl_einsaetze` (Mittelwert 76) und 7,4 %
abweichende Klassen in der Struktur. Ridge und Random Forest sind unauffällig
(≤ 6·10⁻¹⁴). Die berichteten Werte stammen durchgehend aus dem einkernigen Fit
(B-24) — die Reproduzierbarkeitsangabe in Kapitel 6 muss die Kernzahl nennen.

**Negative Vorhersagen:** 7, alle bei XGBoost auf der Rate. Ridge keine — die
`log1p`/`expm1`-Transformation kann nicht unter −1 fallen (B-15). Nicht gekappt.

### 5.5 Ablation — was leistet die Expositionsbehandlung?

Aus der Hauptspezifikation wird **ein** Baustein entfernt: Die Baumverfahren
passen direkt auf `anzahl_einsaetze` an, statt die Rate zu modellieren und
zurückzurechnen. Alles andere bleibt identisch — dieselben Folds, Merkmale und
Hyperparameter. Damit ist der Effekt der Spezifikation isoliert.

| Modell | RMSE | R² |
|---|---|---|
| **Negative Binomial (Referenz)** | **37,44** | 0,472 |
| Random Forest, mit Exposition | **36,43** | 0,523 |
| Random Forest, ohne Exposition | 67,71 | −1,536 |
| XGBoost, mit Exposition | **35,74** | 0,607 |
| XGBoost, ohne Exposition | 61,70 | −0,637 |

*(Wiederholung 0; die vollständigen Werte liefert `m04_shap.py`.)*

**Der Effekt der Spezifikation übersteigt den Effekt der Verfahrenswahl um mehr
als eine Größenordnung:** zwischen den Verfahren liegen unter 2 RMSE, zwischen
den Spezifikationen eines Verfahrens 24 bis 31.

Der Grund ist die **multiplikative Größenstruktur**. Baumverfahren geben je
Blatt einen festen Wert aus und können „Einsätze = Bevölkerung × Risiko" nicht
abbilden; sie ziehen Extremwerte zur Blattmitte, und RMSE auf der Originalskala
wird von den großen Stadtteilen dominiert (Tenderloin 280, Seacliff 6,4).
Verfahren mit Log-Verknüpfung bekommen die Multiplikation geschenkt.

**Nicht** die Extrapolation — die wurde geprüft und ausgeschlossen (B-31).

**Das ist die Antwort auf UF4:** Bei tabellarischen Prognoseaufgaben mit einer
Größen- oder Expositionsgröße entscheidet weniger die Wahl des Verfahrens als
die Frage, ob die Größenbeziehung in der Modellspezifikation abgebildet ist.

### 5.6 Beitrag der Faktorgruppen im Mengenstrang (UF1)

Standardisierte Beiträge der Negative Binomial — dem besten Modell dieses
Strangs (`07_BEFUNDE.md`, B-35):

| Faktorgruppe | Anteil |
|---|---|
| baulich | **31,0 %** |
| kriminalitätsbezogen | 25,6 % |
| sozioökonomisch | 23,2 % |
| Größenkontrolle | 15,3 % |
| Saison | 4,9 % |

Alle drei Faktorgruppen des Exposés tragen bei, in vergleichbarer
Größenordnung. Stärkstes Einzelmerkmal ist `log_kriminalitaetsindex` mit 25,6 %
(p < 0,0001) — allein so viel wie die gesamte sozioökonomische Gruppe.

**`median_haushaltseinkommen` trägt 0,3 % bei p = 0,80** — praktisch nichts,
sobald Armuts- und Akademikerquote im Modell stehen. Es hat zugleich den
höchsten VIF (12,29). `anteil_wohngebaeude_pct` wirkt negativ (−0,338): Je
höher der Wohnanteil, desto weniger Einsätze je Einwohner.

### 5.7 Diagnose zum Tuning auf Wiederholung 0

Getunt wird einmal; in den Wiederholungen 1–9 waren im Mittel 78 % der
Teststadtteile in der Menge, auf der die Parameter gesucht wurden (B-21).
Wäre das wirksam, müsste der Vorsprung gegen die Baseline dort systematisch
größer ausfallen. Gemessen, in Einheiten von `std_folds`:

| Strang | Ridge | Random Forest | XGBoost |
|---|---|---|---|
| `anzahl_einsaetze` | +0,017 | +0,119 | −0,181 |
| `einsaetze_je_1000_ew` | −0,016 | −0,340 | −0,106 |
| `dominante_einsatzart` | – | −0,265 | −0,318 |

Sechs von acht Werten sind **negativ**, kein systematisches Muster. Der Effekt
ist nicht nachweisbar (B-27).

---

## 6. Was noch aussteht

| Punkt | Stand |
|---|---|
| Ertrag des Klassifikationsstrangs | Stufe 2 schöpft nur 0,067 von maximal 1,0 aus — der Strang trägt voraussichtlich weniger als erhofft (`06_RISIKEN.md`, R-2) |
| Linearitätsprüfung vor Ridge (R7) | ✅ gerechnet, `results/eignungspruefung/` |
| Abweichungen mit Schröter besprechen | offen — `06_RISIKEN.md`, R-7 |
| `modelle/m01`–`m04` | vollständig neu zu schreiben, Spezifikation in `04_MODELLIERUNG.md` |
