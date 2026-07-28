# Anforderungen an die Modellierung

Verbindliche Vorgabe für den Neuaufbau von `modelle/`. Die Data Preparation ist
abgeschlossen und wird **nicht mehr angefasst** — die Modellskripte lesen
ausschließlich die fertigen Parquet-Dateien.

Die bestehenden Skripte `m01_eignung.py`, `m02_regression.py` und
`m03_klassifikation.py` beziehen sich noch auf die alte Datenstruktur
(Einzeleinsatz-Ebene, Zeitschnitt) und sind **vollständig neu zu schreiben**.

---

## 1. Was geliefert wird

| Datei | Zeilen × Spalten | Analyseeinheit |
|---|---|---|
| `data/processed/regression.parquet` | 4.620 × 25 | Stadtteil × Monat |
| `data/processed/klassifikation.parquet` | 4.619 × 28 | Stadtteil × Monat |

Beide decken 2015-01 bis 2025-12 ab, 35 Stadtteile, **keine fehlenden Werte**,
alle Merkmale `float64`. Ein Stadtteil-Monat ohne Einsatz fehlt im
Klassifikationsdatensatz, weil sein Anteil 0/0 wäre.

### Merkmale (identisch in beiden Dateien, 12 Stück)

`FEATURE_SETS["S"]` aus `prep/config.py`: zehn Strukturmerkmale plus
`monat_sin`, `monat_cos`.

```
median_haushaltseinkommen   armutsquote_pct        akademikerquote_pct
median_miete                leerstandsquote_pct    log_bevoelkerung
log_kriminalitaetsindex     anteil_altbau_vor_1940_pct
anteil_wohngebaeude_pct     anteil_risikogewerbe_pct
monat_sin                   monat_cos
```

Die Lag-Spalten `lag_1`, `lag_12`, `rolling_mean_3` liegen im
Regressionsdatensatz, sind aber **kein Modellmerkmal**. Unter einem
Stadtteil-Split wären sie die eigene Vergangenheit des Teststadtteils und würden
die Forschungsfrage umgehen. Nur für eine Nebenbemerkung zur zeitlichen Prognose
verwendbar, klar als solche gekennzeichnet.

### Zielgrößen

| Strang | Zielgröße | Typ | Gütemaße |
|---|---|---|---|
| **Menge** | `anzahl_einsaetze` | Zähldaten | RMSE, MAE, R² |
| **Menge** | `einsaetze_je_1000_ew` | stetig | RMSE, MAE, R² |
| **Art** | `dominante_einsatzart` | 4 Klassen | **Macro-F1**, Macro-AUROC |

Die vier `anteil_*`-Spalten bleiben im Datensatz — aus ihnen wird die Klasse
per `argmax` gebildet, und sie dienen der Deskription in Kapitel 5. Sie sind
**keine eigene Zielgröße der Modellierung**: Ein Anteil ist eine Zahl, seine
Vorhersage wäre Regression, und der Klassifikationsstrang soll die *Art*
vorhersagen. Das hält Kapitel 7 fokussiert (Gutachten R8).

Die Klassenverteilung von `dominante_einsatzart` ist stark schief: Fehlalarm
79,0 %, Technische Hilfe 16,3 %, Rettung/EMS 3,1 %, Brand 1,5 %. **Accuracy ist
hier wertlos** — die Mehrheitsklasse allein erreicht 0,786. Maßgeblich ist
Macro-F1.

---

## 2. Der Validierungsrahmen — nicht neu erfinden

Die Aufteilung steht als Spalten `fold` (0–5) und `ist_holdout` (0/1) **in den
Dateien**. Sie ist nicht neu zu berechnen. Zu verwenden ist ausschließlich:

```python
from s2_datensaetze import fold_masken
train, test = fold_masken(daten, k)     # k = 1..5
```

Es ist ein **Stadtteil-Split**: 5 Folds à 6 Stadtteile, dazu 6
Hold-out-Stadtteile. Kein Stadtteil ist je zugleich Trainings- und Testfall; ein
Teststadtteil wird mit allen 132 Monaten getestet. Die Zuteilung ist nach
Bevölkerung stratifiziert, damit kein Fold nur aus Großstadtteilen besteht.

**Das Hold-out (`ist_holdout == 1`) bleibt bis zum Schluss unberührt.** Es wird
genau einmal ausgewertet, nach Abschluss von Modellwahl und Tuning.

### Wiederholte Splits — verbindlich

Bei nur 30 Entwicklungsstadtteilen schwankt das Ergebnis eines einzelnen Folds
massiv. Gemessene Streuungen im Vortest: bis ±0,89 R². Mit fünf Folds allein
lässt sich nicht sagen, welches Verfahren besser ist.

Deshalb: **10 Wiederholungen mit unterschiedlichem Versatz**, Mittelung über
alle 50 Fold-Ergebnisse.

```python
from s2_datensaetze import ergaenze_aufteilung
for versatz in range(10):
    d = ergaenze_aufteilung(daten, versatz=versatz)
    for k in range(1, N_FOLDS + 1):
        train, test = fold_masken(d, k)
        ...
```

Berichtet wird Mittelwert ± Standardabweichung über alle Läufe. Überlappen die
Bereiche zweier Verfahren, ist das **so zu schreiben** — nicht als Rangfolge zu
kaschieren (Gutachten R6).

### Auswertungsebene

Die Gütemaße werden **je Zeile (Stadtteil × Monat)** berechnet. Einheitlich für
Regression und Klassifikation, damit beide Stränge dieselbe Sichtweise haben.

Zu dokumentieren ist dabei: Die Strukturmerkmale sind innerhalb eines Jahres
nahezu konstant, das Modell sagt also für alle zwölf Monate eines Stadtteils
fast denselben Wert vorher. Die Monatsschwankung geht damit vollständig in das
Residuum ein. Ergänzend darf eine auf Stadtteilebene aggregierte Auswertung als
Zusatzangabe berichtet werden — sie fällt deutlich höher aus (bei
`anzahl_einsaetze` 0,580 statt 0,276) und ist getrennt zu kennzeichnen.

---

## 3. Die drei Verfahren

Identische Zeilen, identische Merkmale, identische Folds, identisches
Tuning-Budget. Modellspezifisch ist nur, was **innerhalb der sklearn-Pipeline je
Fold** passiert — sonst entsteht Leakage über die Skalierung.

| | Regression | Klassifikation |
|---|---|---|
| linear | `Ridge` | `LogisticRegression(penalty="l2")` |
| Bagging | `RandomForestRegressor` | `RandomForestClassifier` |
| Boosting | `XGBRegressor` | `XGBClassifier` |

**Wichtig für den Text:** In der Klassifikation ist es nicht Ridge, sondern
logistische Regression mit L2-Strafterm. Das ist das sachlich richtige
Gegenstück, aber ein anderes Verfahren mit anderer Link-Funktion. Das Exposé
verspricht dreimal dieselben drei — bleibt das unerwähnt, ist es genau der im
Gutachten kritisierte Punkt „heterogene Spezifikationen" (R1).

### Auflagen aus der Eignungsprüfung

- Ridge auf `log(1+y)` schätzen, Gütemaße nach `expm1`-Rücktransformation auf
  der Originalskala berechnen
- `StandardScaler` in die Pipeline, nicht vorher
- Klassifikation mit `class_weight="balanced"` bzw. `sample_weight`
- Für `XGBClassifier` numerische Klassenlabels, Wahrscheinlichkeitsspalten
  danach auf die Reihenfolge von `KLASSEN` zurückbringen

### Hyperparameter-Suche

`RandomizedSearchCV` mit den Suchräumen aus `prep/config.py` (`SUCHRAEUME`),
Budget `TUNING_BUDGET = 50` für **jedes** Verfahren. Getunt wird ausschließlich
auf den Trainingsstadtteilen des jeweiligen Folds — nie auf Testfolds, nie auf
dem Hold-out.

---

## 4. Baselines — bereits gerechnet, nicht neu bauen

Sie liegen in `results/` und stammen aus `prep/s3_baselines.py`. Jedes Modell
wird gegen sie gestellt.

| Zielgröße | Baseline | Wert |
|---|---|---|
| `anzahl_einsaetze` | **Negative Binomial** | R² **0,472 ± 0,368** |
| `anzahl_einsaetze` | Gesamtmittelwert | R² −0,832 |
| `einsaetze_je_1000_ew` | Gesamtmittelwert | R² −2,122 |
| `anteil_*` | Gesamtmittelwert | R² −0,06 bis −0,11 |
| `dominante_einsatzart` | Mehrheitsklasse | Macro-F1 0,220 · Accuracy 0,786 |

Negative R² sind korrekt und aussagekräftig: Wer für einen unbekannten Stadtteil
den Gesamtdurchschnitt vorhersagt, liegt schlechter als dessen eigener
Mittelwert. Genau diese Lücke sollen die Strukturmerkmale schließen.

---

## 5. Erwartungswerte aus dem Vortest

Gerechnet mit Standardparametern ohne Tuning, 5 Stadtteil-Folds, Auswertung je
Zeile, Zielgröße `anzahl_einsaetze`.

| Verfahren | R² | Std |
|---|---|---|
| Ridge, **rohe** Zielgröße | −0,237 | 0,730 |
| **Ridge auf `log(1+y)`** — so ist es vorgeschrieben | **0,472** | **0,253** |
| Poisson-GLM mit L2-Strafterm | **0,588** | **0,219** |
| Random Forest | −1,309 | 1,837 |
| *Negative Binomial (Baseline)* | *0,472* | *0,368* |

**Die Log-Transformation ist nicht optional.** Ohne sie liefert Ridge −0,237 mit
einer Streuung von 0,73; mit ihr 0,472 bei halber Streuung. Der Grund: Auf der
Rohskala extrapoliert Ridge linear und erzeugt für Teststadtteile außerhalb des
Trainingsbereichs — das sind 31 % der Zeilen — wilde Werte, teils negative
Einsatzzahlen. Die Log-Spezifikation macht das Modell multiplikativ und
begrenzt den Schaden. Wer diese Auflage übersieht, kommt zu dem falschen
Schluss, Ridge sei für diese Aufgabe untauglich.

Bei `einsaetze_je_1000_ew` liegt Random Forest mit 0,377 ± 0,29 vorn, Ridge auf
der Rohskala bei −3,601. Bei `dominante_einsatzart` erreichen logistische
Regression 0,282 und Random Forest 0,301 Macro-F1 gegen 0,220 der
Mehrheitsklasse.

---

## 6. Warum überhaupt komplexere Verfahren? — die Begründungskette

Die Arbeit muss belegen, dass der Schritt über ein einfaches Regressionsmodell
hinaus gerechtfertigt ist. Diese Begründung entsteht **aus der Regression
selbst**, nicht aus einer Behauptung. Vier Schritte, alle bereits gerechnet:

**Schritt 1 — Das einfache Modell funktioniert.** Ridge auf `log(1+y)` erreicht
R² 0,472 und liegt damit gleichauf mit der Negative-Binomial-Baseline; das
L2-regularisierte Poisson-GLM erreicht 0,588. Ein lineares Modell ist also nicht
etwa unbrauchbar — es ist ein ernstzunehmender Kandidat. Damit ist die Latte
hoch gelegt, und alles Weitere muss sich daran messen.

**Schritt 2 — Der formale Spezifikationstest verwirft es trotzdem.** Der
RESET-Test nach Ramsey (1969) prüft die Nullhypothese, dass die lineare
Spezifikation adäquat ist:

| Test | F | p | Urteil |
|---|---|---|---|
| RESET, Potenzen bis 2 | 215,23 | 4 · 10⁻⁴⁷ | **H0 verworfen** |
| RESET, Potenzen bis 3 | 125,59 | 4 · 10⁻⁵³ | **H0 verworfen** |

Das ist eine formale, zitierbare Aussage: Die lineare Spezifikation reicht
nicht aus. Damit ist der Schritt zu flexibleren Verfahren methodisch begründet
und nicht bloß behauptet.

**Schritt 3 — Die Ursache ist lokalisierbar.** Hier wird die Begründung
konkret, statt nur „es ist nichtlinear" zu sagen:

| Prüfung | Befund | Deutung |
|---|---|---|
| Pearson gegen Spearman, je Merkmal | **0 von 10** Merkmalen mit Abstand > 0,05 | Die *einzelnen* Effekte sind praktisch linear — Krümmung ist nicht die Ursache |
| Interaktionsterme ergänzt | adjustiertes R² **0,805 → 0,914** | Die Ursache sind **Wechselwirkungen zwischen Merkmalen** |

**Schritt 4 — Daraus folgt die Verfahrenswahl.** Ein lineares Modell kann
Interaktionen nur abbilden, wenn man sie von Hand spezifiziert — bei zehn
Merkmalen sind das 45 zusätzliche Terme, deren Auswahl willkürlich wäre und die
bei 30 Trainingsstadtteilen zu Überanpassung führt. Genau diese Lücke schließen
Baumverfahren konstruktionsbedingt: Jeder Split bedingt auf die vorherigen,
Interaktionen entstehen also automatisch und datengetrieben. Random Forest
(Bagging) und XGBoost (Boosting) sind damit **theoretisch** im Vorteil.

Ob sich dieser Vorteil in Prognosegüte übersetzt, ist die empirische Frage der
Arbeit. Der Vortest sagt: bisher nicht — Random Forest liegt bei −1,309. Auch
das ist ein Ergebnis, und es ist genau die Art ehrlicher Vergleichsaussage, die
das Gutachten unter R6 und R9 verlangt.

---

## 7. Aufbau der neuen Skripte

```
modelle/m01_eignung.py         Eignungsprüfung: Linearität, VIF, Verteilungen,
                               Urteil je Verfahren  -> results/eignungspruefung/
modelle/m02_menge.py           Anzahl und Rate, drei Verfahren
                               -> results/regression/
modelle/m03_struktur.py        dominante Einsatzart + vier Anteile
                               -> results/klassifikation/
modelle/m04_shap.py            Interpretation, nur für Modelle mit Signal
```

Jedes Skript liest ausschließlich die Parquet-Dateien und `prep/config.py`,
schreibt CSV nach `results/` und legt **nichts** fest, was in `prep/` gehört.

### Was `m01_eignung.py` neu prüfen muss

Die bestehende Fassung prüft gegen den Zeitschnitt und die Einzeleinsatz-Ebene.
Neu zu prüfen sind:

- Linearität und Residuen der Strukturmerkmale gegen beide Mengen-Zielgrößen,
  ausschließlich auf den Trainingsstadtteilen des ersten Folds (Auflage
  Schröter, Gutachten R7)
- VIF auf den eindeutigen Stadtteil-Merkmalskombinationen
- **Extrapolation neu bewerten:** Unter einem Stadtteil-Split kann ein
  Teststadtteil Merkmalswerte außerhalb des Trainingsbereichs haben. Ridge
  rechnet dann linear weiter — das erklärt vermutlich die Streuung von ±0,89 bei
  der Rate. Der Anteil solcher Fälle ist zu quantifizieren.
- Klassenbalance von `dominante_einsatzart` und die Frage, ob vier Klassen bei
  1,5 % Brand tragfähig sind

### Was `m04_shap.py` beachten muss

SHAP nur für Modelle, die ihre Baseline schlagen. Für `anteil_brand` und
`anteil_rettung_ems` wäre es die Erklärung von Rauschen. Die Lag-Merkmale sind
nicht enthalten, das frühere Blockproblem entfällt; die Strukturmerkmale sind
untereinander aber weiterhin korreliert (max. VIF 7,1), Beiträge verteilen sich
also — blockweise interpretieren.

---

## 8. Was nicht passieren darf

- Skalierung, Imputation oder Encoding **vor** dem Fold-Split
- Eigene Fold-Berechnung statt `fold_masken`
- Auswertung des Hold-outs vor Abschluss des Tunings
- Lag-Merkmale im Hauptvergleich
- Accuracy als Hauptmaß der Klassifikation
- Rangfolgen der drei Verfahren, wo sich die Streuungsbereiche überlappen
- Änderungen an `prep/` — die Aufbereitung ist abgeschlossen und getestet
