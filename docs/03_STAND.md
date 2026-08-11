# Stand der Aufbereitung — der einzige Ort für Zahlen

> **Regel:** Jede Ergebniszahl der Arbeit steht hier und nur hier. Andere
> Dateien und die Thesis verweisen darauf, statt Werte abzuschreiben. Nach jedem
> `python prep/build.py` wird diese Datei einmal überschrieben.
>
> Stand **2026-08-07**, alle Werte an den erzeugten Dateien nachgerechnet.
> Abschnitte 1–2 vom 03.08., Abschnitt 3 und 4 am 07.08. nachgezogen
> (Baselinewechsel #45, Extrapolationszahlen aus dem finalen Lauf),
> Abschnitt 5 aus dem finalen Modelllauf vom 07.08.

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

Die Zahl 33,7 % ist das Mittel der **Wiederholung 0**. Über alle 50 Läufe
gerechnet liegt der Anteil bei **34,6 %**; im Hold-out bei 7,6 %. Wo die drei
Werte auftauchen, ist die Bezugsmenge zu nennen.

**Korrigiert am 06.08.2026, Zahlen nachgezogen am 07.08.2026** nach dem finalen
Lauf (`07_BEFUNDE.md`, B-31 und B-32; `results/shap/extrapolation_*.csv`). Hier
stand zuvor, die Spanne erkläre „einen erheblichen Teil der Fold-Streuung" und
treffe Ridge und die Baumverfahren unterschiedlich. **Beides ist gemessen und
nicht haltbar — unter der finalen Spezifikation sogar mit umgekehrtem
Vorzeichen:**

- Extrapolationsanteil gegen RMSE, Spearman ρ über 50 Läufe: **Ridge +0,298 /
  +0,311** (`anzahl` / Rate), Random Forest +0,126 / +0,181, XGBoost +0,173 /
  +0,188. Der Zusammenhang ist ausgerechnet bei **Ridge am stärksten** — wäre
  Extrapolation der Hebel gegen die Baumverfahren, müsste es umgekehrt sein.
- Es gibt seit #43 ohnehin **keinen Rückstand mehr zu erklären**: Bei
  `anzahl_einsaetze` liegen Random Forest und XGBoost um 0,88 bzw. 0,64 RMSE
  **vor** Ridge, bei der Rate um 0,49 bzw. 0,47. Die Korrelation dieser
  Differenz mit dem Extrapolationsanteil ist durchgehend insignifikant
  (ρ −0,20 bis −0,26, p 0,07 bis 0,16).

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

> Stand **07.08.2026**, neu gefasst mit Decision Log **#45**.

Festgelegt in #32, neu gefasst mit **#45**: Beide Stufe-2-Baselines sind
**unpenalisierte verallgemeinerte lineare Modelle mit kanonischem Link** —
Poisson mit Offset für die Menge, multinomiales Logit für die Struktur. Keines
von beiden hat einen freien Hyperparameter, also wird keines getunt. Sie laufen
über denselben Split und sehen dieselben Merkmale wie die Modelle.

Seit **05.08.2026** laufen die Baselines über **alle 10 Wiederholungen**, also
über dieselben 50 Läufe wie die Vergleichsverfahren. Nur so lässt sich gepaart
testen (`07_BEFUNDE.md`, B-4).

> **Ersetzt die Negative Binomial.** Bis zum 06.08.2026 stand hier die Negative
> Binomial mit R² 0,477 und RMSE 37,27. Der Poisson-Schätzer bleibt bei richtig
> spezifiziertem bedingtem Mittelwert auch unter Überdispersion konsistent
> (Gourieroux, Monfort & Trognon 1984); beschädigt werden die Standardfehler,
> und die verwendet eine Baseline mit reinen Punktvorhersagen nicht. Gemessen
> ist der Poisson zugleich **besser**: 33,98 gegen 37,27 RMSE — einfacher und
> stärker. **Schröter hat die Negative Binomial namentlich freigegeben; der
> Wechsel ist ihm mitzuteilen** (`06_RISIKEN.md`, R-14).

**Maßgeblich — 50 Läufe.** Streuung ist `std_wiederholungen` über die 10
Wiederholungsmittel, nicht `std_folds`:

| Zielgröße | Stufe | Baseline | R² | RMSE | MAE |
|---|---|---|---|---|---|
| `anzahl_einsaetze` | **2** | **Poisson-GLM mit Offset** | **0,542 ± 0,082** | **33,98 ± 3,11** | 22,94 |
| `anzahl_einsaetze` | 1 | Gesamtmittelwert (Nullmarke) | −0,744 ± 0,325 | 69,93 ± 1,92 | 52,33 |
| `einsaetze_je_1000_ew` | **2** | **Poisson-GLM mit Offset** | **0,367 ± 0,261** | **4,08 ± 0,62** | 2,25 |
| `einsaetze_je_1000_ew` | 1 | Gesamtmittelwert (Nullmarke) | −1,054 ± 0,875 | 7,54 ± 0,13 | 4,92 |

Die Rate entsteht aus **derselben** Anpassung, geteilt durch die Bevölkerung —
ein zweites Modell wäre eine zweite Spezifikation und damit unfair gegenüber
den Vergleichsverfahren.

**Warum über zehn Wiederholungen und nicht über eine Aufteilung.** Die einzelne
Aufteilung (Wiederholung 0) weicht in **beide** Richtungen ab: RMSE 31,78 statt
33,98 bei `anzahl_einsaetze` — also scheinbar besser —, R² auf der Rate
dagegen 0,313 statt 0,367. Eine einzelne Fold-Konstellation kann bei 29
Einheiten also ebenso schmeicheln wie strafen. Genau deshalb die wiederholten
Splits (R-5).

Fold-Ergebnisse des Poisson-GLM, Wiederholung 0:

| Fold | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| R² `anzahl_einsaetze` | 0,795 | 0,091 | 0,654 | 0,588 | 0,847 |
| R² `einsaetze_je_1000_ew` | 0,865 | 0,148 | 0,676 | **−0,920** | 0,795 |
| RMSE Rate, Poisson / Nullmarke | 3,48 / 10,34 | 1,41 / 4,09 | 8,11 / 15,01 | 2,54 / 4,03 | 1,50 / 3,78 |

**Auf der Rate ist R² kein tragfähiges Hauptmaß.** In Fold 4 fällt es auf
−0,920, obwohl das Poisson-GLM die Nullmarke bei RMSE in **jedem einzelnen
Fold** schlägt (letzte Zeile). Ursache: R² misst gegen den Mittelwert der
*Testdaten*. Die Rate streut zwischen den Stadtteilen um den Faktor 32
(Excelsior 1,04 · Financial District 33,80), also liegt der Testmittelwert je
nach Fold weit vom Trainingsmittelwert entfernt.

**Konsequenz für Kapitel 7:** Bei der Rate ist RMSE bzw. MAE zu berichten und
R² nur nachrichtlich — mit dieser Begründung. Bei `anzahl_einsaetze` bleibt R²
aussagekräftig.

### Klassifikation — `dominante_einsatzart`

| Stufe | Baseline | Macro-F1 | Macro-AUROC | Accuracy |
|---|---|---|---|---|
| 1 | Mehrheitsklasse („fehlalarm") | 0,223 | – | **0,806** |
| 2 | **Multinomiales Logit, unpenalisiert** | **0,297 ± 0,014** | 0,705 | 0,584 |

Über 50 Läufe gerechnet; auf den 5 Folds der Wiederholung 0 lauteten die Werte
0,223 und **0,287** / 0,689 / 0,569, Macro-F1 je Fold 0,318 · 0,314 · 0,247 ·
0,319 · 0,235. Macro-AUROC wird seit 05.08.2026 mitgeführt — ohne sie gäbe es
für das zweite Gütemaß der Klassifikation keine Messlatte. Für die
Mehrheitsklasse ist sie nicht definiert und bleibt leer, nicht 0,5.
Konvergenzwarnungen: **0 von 50 Läufen**, am 07.08.2026 auf einer zweiten
Umgebung (scikit-learn 1.7.2 statt 1.8) mit identischen Werten nachgerechnet.

> **Zur Historie, gehört in Kapitel 6.** Bis #44 lief das Logit mit dem
> scikit-learn-Vorgabewert `C = 1,0` (Macro-F1 0,298) — eine Voreinstellung,
> keine Entscheidung. #44 tunte es (0,314), #45 machte es unpenalisiert
> (0,297) und strich das Tuning. Seither gilt einheitlich: Was einen freien
> Parameter hat, wird mit gleichem Budget getunt; was keinen hat, wird
> angepasst. **Die Klassifikationslatte sinkt dadurch von 0,314 auf 0,297,
> während die Regressionslatte von 37,27 auf 33,98 RMSE steigt** — die
> Änderung hilft im einen Strang und schadet im anderen. Das spricht gegen
> Rosinenpicken und sollte genau so gesagt werden.
> `results/klassifikation/tuning_baseline.csv` stammt aus der getunten
> Zwischenfassung und hat im aktuellen Lauf keinen Bezug mehr.

Seit #33 hat auch die Klassifikation eine Stufe 2 — eine Referenz, die dieselben
zwölf Merkmale benutzt. **Stufe 2 ist die Latte, die Random Forest und XGBoost
schlagen müssen**, nicht die Mehrheitsklasse. Das Poisson-GLM ist hier nicht
anwendbar (es sagt eine Zahl vorher, die Zielgröße ist eine von vier
ungeordneten Kategorien); das multinomiale Logit ist sein Gegenstück — dieselbe
Modellklasse, derselbe kanonische Link, derselbe Verzicht auf einen Strafterm.

**Der Vergleich der beiden Zeilen ist selbst ein Argument.** Das Logit hat die
deutlich *schlechtere* Trefferquote (0,584 gegen 0,806) und zugleich das
deutlich *bessere* Macro-F1. Das ist kein Widerspruch, sondern die Wirkung von
`class_weight="balanced"`: Das Modell gibt Treffer bei der dominanten Klasse
auf, um die drei seltenen überhaupt zu finden. Wer Accuracy als Hauptmaß nähme,
käme zu dem Schluss, das Modell sei schlechter geworden. Genau deshalb ist
Macro-F1 maßgeblich.

**Was Stufe 2 stützt.** In der Regression: Das Poisson-GLM bildet über die
Log-Verknüpfung die multiplikative Größenstruktur ab und kann Krümmung, aber
**keine Wechselwirkungen** — genau die finden RF und XGBoost
konstruktionsbedingt. Schlagen sie die Latte, ist der Mehraufwand belegt;
schlagen sie sie nicht, reicht die einfachere Struktur. Beides ist ein Ergebnis.

**Was in der Klassifikation offen ist.** Dort schlägt ein flacher
Entscheidungsbaum (Macro-F1 0,270) das Logit **nicht**. Der Mehraufwand von RF
und XGBoost ist im zweiten Strang vorab nicht begründet — siehe
`06_RISIKEN.md`, R-2.

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

### 5.1 Menge — kein Verfahren schlägt die Baseline

| Zielgröße | Verfahren | RMSE | MAE | R² | Trainingszeit |
|---|---|---|---|---|---|
| `anzahl_einsaetze` | **Poisson-GLM** | **33,98 ± 3,11** | 22,94 | 0,542 | – |
| | Random Forest | 35,63 ± 3,51 | 25,41 | 0,402 | 5,78 s |
| | XGBoost | 35,88 ± 2,55 | 24,49 | 0,532 | 1,43 s |
| | Ridge | 36,51 ± 2,76 | 24,40 | 0,511 | 0,011 s |
| `einsaetze_je_1000_ew` | **Poisson-GLM** | **4,08 ± 0,62** | 2,25 | 0,367 | – |
| | Random Forest | 4,19 ± 0,63 | 2,43 | 0,251 | 5,88 s |
| | XGBoost | 4,20 ± 0,44 | 2,31 | 0,508 | 1,43 s |
| | Ridge | 4,68 ± 0,24 | 2,50 | 0,283 | 0,011 s |

**Primäraussage nach #34** — gepaarter Wilcoxon auf den 10 Wiederholungsmitteln,
positive Differenz heißt das Verfahren ist besser:

| Zielgröße | Verfahren gegen Baseline | Differenz | gewonnen | p | Befund |
|---|---|---|---|---|---|
| `anzahl_einsaetze` | Random Forest | −1,65 | 3/10 | 0,160 | **nicht unterscheidbar** |
| | XGBoost | −1,90 | 3/10 | 0,232 | **nicht unterscheidbar** |
| | Ridge | −2,53 | 2/10 | 0,010 | signifikant schlechter |
| `einsaetze_je_1000_ew` | Random Forest | −0,11 | 4/10 | 0,557 | **nicht unterscheidbar** |
| | XGBoost | −0,13 | 3/10 | 0,375 | **nicht unterscheidbar** |
| | Ridge | −0,60 | 1/10 | 0,004 | signifikant schlechter |

**Kein Verfahren schlägt die Baseline.** Random Forest und XGBoost erreichen in
beiden Zielgrößen ihr Niveau, Ridge bleibt in beiden signifikant darunter.

**Verfahrensvergleich** (sekundär, Holm über 6 Tests):

| Zielgröße | Paarung | Differenz | p_holm | Befund |
|---|---|---|---|---|
| `anzahl_einsaetze` | alle drei Paarungen | ≤ 0,88 | 1,000 | nicht unterscheidbar |
| `einsaetze_je_1000_ew` | Ridge – XGBoost | −0,47 | 0,035 | **XGBoost besser** |
| | Ridge – Random Forest | −0,49 | 0,186 | nicht unterscheidbar |
| | Random Forest – XGBoost | +0,01 | 1,000 | nicht unterscheidbar |

Nur eine einzige Paarung ist trennbar. Bei `anzahl_einsaetze` überlappen die
Streuungsbereiche durchgehend — dort ist keine Rangfolge zulässig (R-1).

**Nebeneffekt der Verlustfunktion (#42):** **keine negativen Vorhersagen**,
in keinem der 300 Läufe. Tweedie und Poisson haben eine Log-Verknüpfung und
können nicht unter null fallen — die Zielgröße wird strukturell respektiert
statt nachträglich geprüft.

### 5.2 Struktur — beide Verfahren schlagen die Stufe-2-Baseline in der Kreuzvalidierung

> **Achtung, gilt nur für die Kreuzvalidierung.** Auf dem Hold-out kehrt sich
> das Ergebnis um: dort gewinnt das Logit mit 0,327 gegen XGBoost 0,274 und
> Random Forest 0,255 (Abschnitt 5.7). Beide Auswertungen sind zu berichten;
> Analyse in `07_BEFUNDE.md`, B-42. Eine Rangfolge zwischen Logit und
> Baumverfahren ist im Strukturstrang **nicht** zulässig.

| Verfahren | Macro-F1 | Macro-AUROC | Accuracy | Trainingszeit |
|---|---|---|---|---|
| Mehrheitsklasse (Stufe 1) | 0,223 | – | 0,806 | – |
| **Multinomiales Logit (Stufe 2)** | **0,297** | 0,705 | 0,584 | – |
| Random Forest | 0,3276 ± 0,0129 | 0,735 | 0,761 | 2,05 s |
| XGBoost | 0,3343 ± 0,0128 | 0,751 | 0,754 | 1,64 s |

| Paarung | Differenz | gewonnen | p | Befund |
|---|---|---|---|---|
| Random Forest gegen Stufe 2 | +0,0304 | **10/10** | 0,002 | **signifikant besser** |
| XGBoost gegen Stufe 2 | +0,0371 | **10/10** | 0,002 | **signifikant besser** |
| Random Forest – XGBoost | −0,0067 | 2/10 | 0,131 | nicht unterscheidbar |

Beide Verfahren gewinnen in **allen zehn** Wiederholungen — der kleinste bei
n = 10 erreichbare p-Wert. Die Richtung ist damit eindeutig, nicht knapp.

Keine Korrektur beim Verfahrensvergleich — die Familie besteht aus einem Test
(#38). Kein Lauf ohne definierte Macro-AUROC. Brand-Testfälle im Mittel 6,6
je Fold, in Wiederholung 0 wie dokumentiert 13 · 9 · 6 · 3 · 2.

### 5.3 Der Kernbefund

**In keinem der beiden Stränge ist der Mehraufwand belegt.** In der Menge
erreichen Random Forest und XGBoost das Niveau der Baseline, ohne es zu
übertreffen; Ridge bleibt darunter. In der Struktur schlagen beide die Baseline
in der Kreuzvalidierung, verlieren aber auf dem Hold-out gegen sie (B-42).

**Woran es liegt.** Nicht an der Extrapolation — diese Erklärung stand hier
bis zum 07.08.2026 und wurde durch die Messung widerlegt (B-31, Zahlen unter
der finalen Spezifikation neu erhoben: der Extrapolationsanteil sagt den Fehler
nicht vorher, und der Zusammenhang ist bei Ridge stärker als bei den
Baumverfahren — Abschnitt 3). Maßgeblich ist die **Spezifikation**: Die
Expositionsbehandlung ist
22 bis 29 RMSE wert (Abschnitt 5.5), und die von der Eignungsprüfung
diagnostizierte Nichtlinearität überträgt sich nicht auf unbekannte Stadtteile
(B-41). Bei 29 unabhängigen Einheiten zahlt sich zusätzliche Flexibilität
nicht aus — was die Ensembles trägt, ist ihre Regularisierung, nicht ihre
Ausdrucksfähigkeit.

### 5.4 Aufwand und Reproduzierbarkeit

**Messumgebung.** Alle Laufzeiten stammen aus einem Durchgang auf derselben
Maschine, ohne Nebenlast:

| | |
|---|---|
| Prozessor | Intel Core i5-7300U @ 2,60 GHz |
| Kerne | **2 physisch**, 4 logisch (Hyperthreading) |
| Arbeitsspeicher | 7,8 GB |
| Betriebssystem | Windows 10 Pro |
| Python | 3.14.0 · Pakete in `requirements_lauf.txt` |

**Trainingszeit je Fold, einkernig gemessen** (#40) — der Wert, der zwischen
den Verfahren vergleichbar ist:

| | Ridge | XGBoost | Random Forest |
|---|---|---|---|
| Menge, `anzahl_einsaetze` | **0,011 s** | 1,43 s | 5,78 s |
| Menge, Rate | **0,011 s** | 1,43 s | 5,88 s |
| Struktur | – | 1,78 s | 2,18 s |
| Inferenz, Menge | 0,003 s | 0,016 s | 0,083 s |

Ridge ist bei nicht unterscheidbarer Güte **130-mal schneller als XGBoost** und
**526-mal schneller als Random Forest**. Das ist die belastbarste Aussage des
Aufwandvergleichs, weil zwischen den Verfahren Größenordnungen liegen und
nicht Prozentpunkte.

**Parallelisierungsgewinn** — Faktor, um den der Fit über alle vier logischen
Kerne schneller ist:

| | Ridge | Random Forest | XGBoost |
|---|---|---|---|
| Menge | 1,08 / 1,05 | 2,19 / 2,25 | **0,68 / 0,71** |
| Struktur | – | 1,53 | **0,77** |

**Bei XGBoost liegt der Gewinn unter 1** — der Fit über alle Kerne dauert
*länger* als der einkernige (B-28). Random Forest profitiert, weil seine Bäume
unabhängig sind; Ridge hat als geschlossene Lösung nichts zu verteilen.

> **Einschränkung, die in Kapitel 7 gehört:** Der Parallelisierungsgewinn ist
> stärker an diese Maschine gebunden als die Einkern-Zeiten. Zwei physische
> Kerne mit Hyperthreading sind ein Grenzfall — die vier logischen Prozessoren
> teilen sich zwei Recheneinheiten. Auf einer Maschine mit acht oder mehr
> echten Kernen fielen die Faktoren anders aus. Bei einer U-Serie-CPU ist
> zudem thermische Drosselung über eine Laufzeit von rund einer Stunde nicht
> auszuschließen.

**XGBoost ist nicht threaddeterministisch.** Bei anderer Kernzahl weichen die
Vorhersagen ab: bis **323** bei `anzahl_einsaetze` (Mittelwert 76), 18,9 bei
der Rate, **7,4 %** abweichende Klassen in der Struktur. Ridge und Random
Forest sind unauffällig (≤ 2·10⁻¹³). Alle berichteten Werte stammen aus dem
einkernigen Fit (B-24) — die Reproduzierbarkeitsangabe in Kapitel 6 muss die
Kernzahl nennen, nicht nur den `random_state`.

**Negative Vorhersagen: keine**, in keinem der 300 Läufe. Tweedie und Poisson
haben eine Log-Verknüpfung (B-15).

### 5.5 Ablation — was leistet die Expositionsbehandlung?

Aus der Hauptspezifikation wird **ein** Baustein entfernt: Die Baumverfahren
passen direkt auf `anzahl_einsaetze` an, statt die Rate zu modellieren und
zurückzurechnen. Alles andere bleibt identisch — dieselben Folds, Merkmale und
Hyperparameter. Damit ist der Effekt der Spezifikation isoliert.

| Modell | RMSE | R² |
|---|---|---|
| **Poisson-GLM (Referenz)** | **33,98** | 0,542 |
| Random Forest, mit Exposition | **35,63** | 0,402 |
| Random Forest, ohne Exposition | 64,81 | −1,023 |
| XGBoost, mit Exposition | **35,88** | 0,532 |
| XGBoost, ohne Exposition | 57,86 | −0,042 |
| Ridge, mit Exposition | 36,51 | 0,511 |

**Der Effekt der Spezifikation übersteigt den Effekt der Verfahrenswahl um mehr
als eine Größenordnung:** zwischen den drei Verfahren liegen **0,9 RMSE**,
zwischen den Spezifikationen eines Verfahrens **22 bis 29**.

Der Grund ist die **multiplikative Größenstruktur**. Baumverfahren geben je
Blatt einen festen Wert aus und können „Einsätze = Bevölkerung × Risiko" nicht
abbilden; sie ziehen Extremwerte zur Blattmitte, und RMSE auf der Originalskala
wird von den großen Stadtteilen dominiert (Tenderloin 280, Seacliff 6,4).
Verfahren mit Log-Verknüpfung bekommen die Multiplikation geschenkt.

**Nicht** die Extrapolation — die wurde geprüft und ausgeschlossen (B-31).

**Das ist die Antwort auf UF4:** Bei tabellarischen Prognoseaufgaben mit einer
Größen- oder Expositionsgröße entscheidet weniger die Wahl des Verfahrens als
die Frage, ob die Größenbeziehung in der Modellspezifikation abgebildet ist.

#### Gegenprobe: hält die diagnostizierte Nichtlinearität out-of-sample?

Quelle: `vorpruefung/v3_spezifikation.py` → `results/spezifikation/`. Dasselbe
Poisson-GLM, derselbe Split, dieselben 50 Läufe — nur mit den Termen, deren
Fehlen die Eignungsprüfung nachweist.

| Spezifikation | Terme | RMSE | R² | konvergiert |
|---|---|---|---|---|
| **linear** | 12 | **33,98** | **0,542** | 50/50 |
| + quadratische Terme | 22 | 101,11 | −5,971 | 50/50 |
| + Interaktionen | 57 | 121,63 | −6,247 | 50/50 |
| + beides | 67 | 180,86 | −16,454 | 50/50 |

Die Zeile `linear` reproduziert die Stufe-2-Baseline exakt (33,978); das prüft
das Skript selbst und bricht sonst ab.

**Die Struktur, die in-sample nachweisbar ist, zerstört die Prognose
out-of-sample.** Der RESET-Test der Eignungsprüfung läuft auf 3.828 Zeilen, die
als unabhängig behandelt werden — tatsächlich liegen 29 unabhängige Stadtteile
vor. Ausführlich in `07_BEFUNDE.md`, B-41.

Damit ergibt sich die Spannweite, die A3 zeigt: Die **Wahl des Verfahrens**
bewegt bis zu **2,5 RMSE**, die **Wahl der Spezifikation** bis zu **146,9**.

### 5.6 Beitrag der Faktorgruppen (UF1)

Zwei Quellen, weil die beiden Stränge verschiedene beste Modelle haben: im
Mengenstrang die standardisierten Koeffizienten des Poisson-GLM (keines der
Verfahren schlägt es), in der Struktur die SHAP-Werte von Random Forest und
XGBoost. Beides auf Fold 5, dem Fold mit dem geringsten Extrapolationsanteil
(3,6 %).

| Faktorgruppe | **Menge** (Poisson-GLM) | **Struktur** (RF) | **Struktur** (XGBoost) |
|---|---|---|---|
| kriminalitätsbezogen | **36,2 %** | 16,7 % | 13,4 % |
| baulich | 25,6 % | 29,2 % | 21,1 % |
| sozioökonomisch | 23,2 % | **34,9 %** | **45,3 %** |
| Größenkontrolle | 11,2 % | 10,3 % | 10,1 % |
| Saison | 3,9 % | 8,8 % | 10,2 % |

**Alle drei Faktorgruppen des Exposés tragen in beiden Strängen bei** — UF1 ist
mit ja beantwortet, und keine Gruppe ist bedeutungslos.

**Die Treiber unterscheiden sich aber zwischen den Strängen.** Wie *viele*
Einsätze ein Stadtteil hat, hängt am stärksten vom Kriminalitätsindex ab
(36,2 %). *Welche Art* dort überwiegt, wird vor allem von den sozioökonomischen
Merkmalen bestimmt (34,9 bzw. 45,3 %), während der Kriminalitätsindex dort auf
den dritten Platz zurückfällt.

Beide Verfahren der Struktur stimmen in der Rangfolge der ersten drei Gruppen
überein — sozioökonomisch vor baulich vor kriminalitätsbezogen. Nur die beiden
letzten Plätze tauschen. Das erfüllt den Prüfauftrag „stimmen RF und XGBoost
überein" (`m04_shap.py`).

**Multikollinearität:** höchster VIF 12,29 bei `median_haushaltseinkommen`, auf
beiden Bezugsmengen identisch. Deshalb die blockweise Auswertung — einzelne
Merkmalsbeiträge wären Scheinpräzision.

### 5.7 Die Schlussbewertung auf dem Hold-out

Einmalig, auf sechs Stadtteilen, die in **kein** Tuning, **keine** Bewertung und
**keine** Spezifikationsentscheidung eingeflossen sind. Trainiert wird auf allen
29 Entwicklungsstadtteilen.

**Menge**

| Stufe | | RMSE `anzahl` | R² | RMSE Rate | R² |
|---|---|---|---|---|---|
| 1 | Gesamtmittelwert | 50,23 | −0,006 | 3,30 | −3,320 |
| **2** | **Poisson-GLM** | **22,35** | **0,801** | **0,98** | **0,621** |
| 3 | Ridge | 23,71 | 0,776 | 1,14 | 0,485 |
| 3 | XGBoost | 26,33 | 0,724 | 1,06 | 0,549 |
| 3 | Random Forest | 30,58 | 0,627 | 1,56 | 0,034 |

**Der Befund der Kreuzvalidierung bestätigt sich:** Auch auf unberührten Daten
erzielt die Baseline in beiden Zielgrößen den geringsten Fehler.

**Struktur**

| Stufe | | Macro-F1 | Macro-AUROC | Accuracy |
|---|---|---|---|---|
| 1 | Mehrheitsklasse | 0,208 | – | 0,711 |
| **2** | **Multinomiales Logit** | **0,327** | **0,756** | 0,472 |
| 3 | XGBoost | 0,274 | 0,637 | 0,574 |
| 3 | Random Forest | 0,255 | 0,507 | 0,563 |

**Hier weicht die Schlussbewertung von der Kreuzvalidierung ab** — dort schlugen
beide Verfahren die Baseline in 10 von 10 Wiederholungen.

#### Wie diese Zahlen zu lesen sind

**Nicht mit den Kreuzvalidierungswerten vergleichen.** Der Hold-out ist eine
andere, leichtere Aufgabe: Extrapolationsanteil 7,6 % gegenüber 34,6 % in den
CV-Folds, und das Training läuft auf 29 statt 23 Stadtteilen. Die absoluten
Werte fallen deshalb günstiger aus.

**Keine Rangfolge zwischen den Verfahren.** Eine Messung an sechs Einheiten,
ohne Streuung und ohne Test (R-4).

**Die Abweichung im Strukturstrang ist kein Widerspruch.** Der Hold-out-Wert
liegt innerhalb der Spannweite der 50 Einzelläufe — Random Forest 0,229 bis
0,404, XGBoost 0,230 bis 0,421. 14 % bzw. 16 % der Folds lagen darunter. Es ist
eine Ziehung aus einer breiten Verteilung, keine Anomalie.

**Was der Hold-out dagegen sichtbar macht** (`07_BEFUNDE.md`, B-40): Beide
Baumverfahren sagen die seltenste Klasse dort **kein einziges Mal** vorher —
F1 für `brand` ist 0,000, bei Random Forest liegt die AUROC mit 0,173 sogar
unter dem Zufall. Die logistische Regression erkennt dieselbe Klasse mit einer
AUROC von 0,895 zuverlässig und klassifiziert lediglich zurückhaltend. Der
Mittelwert der Kreuzvalidierung verdeckt diese Instabilität.

### 5.8 Diagnose zum Tuning auf Wiederholung 0

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

## 6. Was noch aussteht

| Punkt | Stand |
|---|---|
| Linearitätsprüfung vor Ridge (Gutachten R7) | ✅ gerechnet, `results/eignungspruefung/` — mit der Einschränkung aus B-41: Die diagnostizierte Nichtlinearität generalisiert nicht |
| Anforderungen je Verfahren (Auflage 10.08.) | ✅ gerechnet, **Abschnitt 7** — drei formale Tests, Tabelle und Abbildung A10 |
| Codebook mit Skalenniveau (Auflage 10.08.) | ✅ `results/codebook/merkmale.md`, 34 Merkmale, erzeugt von `tools/codebook.py` |
| `modelle/m02`–`m05` | ✅ geschrieben und gelaufen, Ergebnisse in Abschnitt 5 |
| Hold-out | ✅ einmalig ausgewertet, Abschnitt 5.7 |
| E-Mail an Schröter | ✅ **abgeschickt 08.08.2026** — Poisson statt Negative Binomial (#45), Expositionsbehandlung für alle Verfahren (#43), Primärtest auf den 10 Wiederholungsmitteln (#37), zwei Testfamilien (#38). Antwort ausstehend, `06_RISIKEN.md` R-14 |
| **„Generalisierung auf unbekannte Stadtteile" in den Text** | **offen, zugesagt** — der Begriff ist Schröter gegenüber als umgesetzt gemeldet, steht aber noch nicht in `main.tex`. Gehört wörtlich in die Zielsetzung und in Kapitel 5.4 (`06_RISIKEN.md`, R-17) |
| Ertrag des Klassifikationsstrangs | offen als **Befund**, nicht als Aufgabe: bestes Verfahren 0,334 von maximal 1,0, und CV und Hold-out widersprechen sich (`06_RISIKEN.md`, R-2) |
| Kapitel 6 bis 9 | zu schreiben |

---

## 7. Anforderungen je Verfahren — die formalen Tests

> Stand **11.08.2026**, Auflage Schröter vom 10.08.2026: „Prüfung ob die
> Algorithmen auf den Daten passen … Jeder Algorithmus sollte dargestellt
> werden … Test laufen lassen: in Tabelle Statistiken mit p-Werten anzeigen."
>
> Quelle: `results/eignungspruefung/annahmen.csv`, erzeugt von
> `vorpruefung/v2_eignung.annahmen()`. Gerechnet auf den 23
> Trainingsstadtteilen von Fold 1 — die Teststadtteile bleiben unberührt.
>
> Nachgestellt und nicht als Abschnitt 5 eingefügt, weil die Abschnittsnummern
> in `tools/pruefe_zahlen.py` als Anker dienen. Eine Umnummerierung hätte
> stillschweigend Prüfungen abgeschaltet.

**15 Anforderungen geprüft:** 7 verletzt, 3 bestehen für das jeweilige
Verfahren gar nicht, der Rest ist eingehalten.

### Die drei neuen Teststatistiken

| Prüfung | Was sie prüft | Statistik | p |
|---|---|---|---|
| **Cameron & Trivedi (1990)**, Hilfsregression | Equidispersion des Poisson-GLM, Var = μ | t = 17,2 | < 0,001 |
| **Breusch-Pagan** auf log(1+y) | Homoskedastizität der Residuen | LM = 722,7 | < 0,001 |
| **Jarque-Bera** auf log(1+y) | Normalverteilung der Residuen | JB = 98,2 | < 0,001 |

Schiefe −0,21, Wölbung 3,78. Die zugehörige Abbildung ist **A10**
(`results/abbildungen/a10_qq_residuen.pdf`): Der Kern liegt auf der Geraden,
nur die Ränder biegen ab — schwere Ränder, kein verbogener Kern.

Bereits an anderer Stelle berichtet und hier nur zugeordnet: RESET F = 215,2
(Abschnitt 3 der Eignungsprüfung), Dispersionsindex 54,2 auf den
Trainingsstadtteilen von Fold 1 gegenüber 62,8 auf dem vollen Datensatz
(Abschnitt 2 dieser Datei), Extrapolationsanteil 33,7 %.

### Was daraus folgt — je Verfahren

| Verfahren | Anforderung | Status | Konsequenz |
|---|---|---|---|
| alle | unabhängige Beobachtungen | **verletzt** | 132 Monate je Stadtteil; Antwort ist der Stadtteil-Split und die Streuung über die 10 Wiederholungsmittel (R-5) |
| alle | identische Merkmale, Zeilen, Folds | erfüllt | konstruktiv über die `fold`-Spalte, Auflage C vom 04.08. |
| Poisson-GLM | Equidispersion | **verletzt** | folgenlos: nur Punktvorhersagen, Schätzer bleibt konsistent (#45) |
| Poisson-GLM | Linearität im Log-Link | **verletzt** | in Kauf genommen; die Gegenprobe `v3` zeigt, dass die nichtlinearen Erweiterungen out-of-sample schlechter sind (B-41) |
| Ridge | Linearität | **verletzt** | Schätzung auf log(1+y), Rücktransformation mit expm1 |
| Ridge | Homoskedastizität | **verletzt** | betrifft die Standardfehler, nicht die Punktprognose |
| Ridge | Normalverteilung der Residuen | *nicht erforderlich* | Voraussetzung für Inferenz, nicht für die Punktprognose eines L2-penalisierten Modells |
| Random Forest, XGBoost | Verteilungsannahme | *nicht erforderlich* | verteilungsfrei — der Grund, warum beide im Vergleich stehen |
| Random Forest, XGBoost | Testpunkte im gelernten Wertebereich | **verletzt** | 33,7 % außerhalb; Limitation des Stadtteil-Splits, Kapitel 8.3 (R-3) |
| Random Forest, XGBoost | Verlustfunktion passend zur Datenform | erfüllt | `criterion="poisson"` bzw. `reg:tweedie` (#42) |
| Multinomiales Logit | Linearität in den Log-Odds | *angenommen* | genau die Trennlinie zu RF und XGBoost — fehlende Wechselwirkungen sind der zu messende Unterschied |
| Multinomiales Logit | jede Klasse im Testfold besetzt | erfüllt | doppelte Stratifizierung (#30), Selbsttest `v0_aufteilung` |

**Die Zeilen „nicht erforderlich" sind kein Füllmaterial.** Eine Tabelle, die
nur verletzte Annahmen zeigt, ließe die Baumverfahren voraussetzungslos
aussehen; eine, die sie ganz wegließe, beantwortete die Auflage nicht. Ihre
eigentliche Anforderung ist der Interpolationsbereich — und die ist verletzt.

Der VIF steht bewusst **nicht** hier, sondern in `results/shap/vif.csv`: Seine
einzige echte Konsequenz betrifft die Interpretation der Beiträge, und dieselbe
Zahl an zwei Orten ist die Fehlerquelle, gegen die Abschnitt 7 selbst
abgesichert ist.

---
