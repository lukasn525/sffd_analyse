# Anforderungen an die Modellierung

> **Lebensdauer dieses Dokuments:** Spezifikation, keine Ergebnisse. Zahlen zu
> Datensätzen und Baselines stehen in `03_STAND.md` und werden hier **nicht**
> wiederholt. Erwartungswerte aus Vortests sind als solche gekennzeichnet.

Verbindliche Vorgabe für den Neuaufbau von `modelle/`. Die Data Preparation ist
abgeschlossen und wird **nicht mehr angefasst** — die Modellskripte lesen
ausschließlich die fertigen Parquet-Dateien.

Die bestehenden Skripte `m01_eignung.py`, `m02_regression.py` und
`m03_klassifikation.py` beziehen sich noch auf die alte Datenstruktur
(Einzeleinsatz-Ebene, Zeitschnitt) und sind **vollständig neu zu schreiben**.

---

## 1. Was geliefert wird

Beide Datensätze liegen auf der Analyseeinheit **Stadtteil × Monat**, decken
denselben Zeitraum und dieselben Stadtteile ab, haben keine fehlenden Werte und
Merkmale durchgehend `float64`. Ein Stadtteil-Monat ohne Einsatz fehlt im
Klassifikationsdatensatz, weil sein Anteil 0/0 wäre.

**Steckbrief mit Zeilen, Spalten und Zeitraum: `03_STAND.md`, Abschnitt 2.**

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

Die Klassenverteilung von `dominante_einsatzart` ist stark schief (Werte in
`03_STAND.md`). **Accuracy ist hier wertlos** — die Mehrheitsklasse allein
erreicht über 0,8. Maßgeblich ist Macro-F1.

---

## 2. Der Validierungsrahmen — nicht neu erfinden

Die Aufteilung steht als Spalten `fold` (0–5) und `ist_holdout` (0/1) **in den
Dateien**. Sie ist nicht neu zu berechnen. Zu verwenden ist ausschließlich:

```python
from s2_datensaetze import fold_masken
train, test = fold_masken(daten, k)     # k = 1..5
```

Es ist ein **Stadtteil-Split** (Aufteilung siehe `03_STAND.md`, Abschnitt 3).
Kein Stadtteil ist je zugleich Trainings- und Testfall; ein Teststadtteil wird
mit allen Monaten des Zeitraums getestet. Die Zuteilung ist nach Bevölkerung
stratifiziert, damit kein Fold nur aus Großstadtteilen besteht, und zusätzlich
nach der seltensten Klasse, damit kein Fold ohne Brand-Testfälle bleibt (#30).

**Das Hold-out (`ist_holdout == 1`) bleibt bis zum Schluss unberührt.** Es wird
genau einmal ausgewertet, nach Abschluss von Modellwahl und Tuning.

### Wiederholte Splits — verbindlich

Bei 29 Entwicklungsstadtteilen schwankt das Ergebnis eines einzelnen Folds
massiv — die Baseline allein streut über die fünf Folds von R² −0,17 bis 0,73.
Mit fünf Folds lässt sich nicht sagen, welches Verfahren besser ist.

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
Zusatzangabe berichtet werden — sie fällt deutlich höher aus und ist getrennt zu
kennzeichnen.

---

## 3. Die Verfahren — drei für die Menge, zwei für die Struktur

Identische Zeilen, identische Merkmale, identische Folds, identisches
Tuning-Budget. Modellspezifisch ist nur, was **innerhalb der sklearn-Pipeline je
Fold** passiert — sonst entsteht Leakage über die Skalierung.

| | Regression | Klassifikation |
|---|---|---|
| linear | `Ridge` | — (entfällt, siehe unten) |
| Bagging | `RandomForestRegressor` | `RandomForestClassifier` |
| Boosting | `XGBRegressor` | `XGBClassifier` |

Die Klassifikationsmenge ist eine **echte Teilmenge** der Regressionsmenge: Es
kommt kein Verfahren hinzu, genau eines fällt weg. Freigegeben von Schröter per
E-Mail vom 03.08.2026, festgehalten als Decision Log #31.

**Warum RF und XGBoost sich übertragen:** Zwischen Regression und Klassifikation
wechselt bei ihnen ausschließlich die Verlustfunktion — Gini bzw. Entropie statt
Varianzreduktion beim Splitkriterium, `multi:softprob` statt
`reg:squarederror` als Objective. Der Ensemble-Mechanismus (Bagging über
Bootstrap-Stichproben, sequenzielles Gradient Boosting) bleibt identisch, beide
sind nativ mehrklassenfähig.

**Warum Ridge sich nicht überträgt:** Ridge minimiert den quadratischen Fehler
auf einer metrischen Zielgröße. `dominante_einsatzart` ist nominal skaliert mit
vier ungeordneten Klassen — ohne Ordnung und ohne Abstandsbegriff ist der
quadratische Fehler als Verlustfunktion nicht definiert. Der `RidgeClassifier`
aus scikit-learn wurde geprüft und verworfen: ±1-Kodierung, One-vs-Rest, keine
kalibrierten Klassenwahrscheinlichkeiten.

**Warum keine logistische Regression:** Sie wäre das sachlich korrekte lineare
Gegenstück (L2-penalisiert wie Ridge, andere Link-Funktion) und schlägt im
Vortest die Baseline. Sie wäre aber ein vierter Verfahrensstrang — bei zwei
Zielgrößen genau die Verbreiterung, die das Gutachten unter R4 als größtes
Notenrisiko benennt und die R8 untersagt. Entschieden am 03.08.2026, nicht
wieder aufzunehmen ohne Revision von Decision Log #31.

**Wichtig für den Text:** Das Exposé verspricht dreimal dieselben drei
Verfahren. Die Abweichung ist in 6.2 zu benennen und zu begründen (Auflage
Schröter) — bleibt sie unerwähnt, ist sie genau der im Gutachten kritisierte
Punkt „heterogene Spezifikationen" (R1). Die Begründung lautet, dass die
Verfahrensauswahl der Skalierung der jeweiligen Zielgröße folgt, nicht der
Bequemlichkeit: metrisch → drei, nominal → die zwei, die eine nominale
Zielgröße überhaupt verarbeiten können.

### Auflagen aus der Eignungsprüfung

- Ridge auf `log(1+y)` schätzen, Gütemaße nach `expm1`-Rücktransformation auf
  der Originalskala berechnen
- `StandardScaler` in die Pipeline, nicht vorher
- Klassifikation mit `class_weight="balanced"` bzw. `sample_weight` — als
  Modellhyperparameter, **nicht** als Preprocessing-Schritt (kein Resampling),
  konsistent zur Metrik Macro-F1
- Für `XGBClassifier` numerische Klassenlabels, Wahrscheinlichkeitsspalten
  danach auf die Reihenfolge von `KLASSEN` zurückbringen. Der Label-Encoder ist
  **einmal global auf allen vier Klassen** zu fitten, nicht je Fold — sonst
  verschiebt sich das Mapping in Folds, in denen eine Klasse nicht auftritt
- Da beide Klassifikationsverfahren baumbasiert sind, braucht der
  Klassifikationsstrang **keine Skalierung**. `StandardScaler` betrifft nur
  Ridge. Die Data Preparation bleibt davon unberührt und wird nicht angefasst

### Hyperparameter-Suche

`RandomizedSearchCV` mit den Suchräumen aus `prep/config.py` (`SUCHRAEUME`),
Budget `TUNING_BUDGET = 50` für **jedes** Verfahren. Getunt wird ausschließlich
auf den Trainingsstadtteilen des jeweiligen Folds — nie auf Testfolds, nie auf
dem Hold-out.

---

## 4. Baselines — bereits gerechnet, nicht neu bauen

Sie stammen aus `prep/s3_baselines.py` und liegen in `results/`. Jedes Modell
wird gegen sie gestellt. **Die Werte stehen in `03_STAND.md`, Abschnitt 4** —
hier nicht wiederholen, sonst laufen die beiden Stellen auseinander.

Festgelegt in Decision Log #32: **Negative Binomial** für die Regression,
**Mehrheitsklasse** für die Klassifikation, Gesamtmittelwert als Nullmarke.
Die Negative Binomial ist bewusst ein starker Gegner — sie bekommt dieselben
zwölf Merkmale und dieselben Folds. Was sie nicht kann, sind Wechselwirkungen
zwischen Merkmalen; genau daran hängt die Rechtfertigung der Baumverfahren.

Negative R² sind korrekt und aussagekräftig: Wer für einen unbekannten Stadtteil
den Gesamtdurchschnitt vorhersagt, liegt schlechter als dessen eigener
Mittelwert. Genau diese Lücke sollen die Strukturmerkmale schließen.

---

## 5. Erwartungswerte aus dem Vortest

> **Herkunft:** Vortest vom 28.07.2026, Standardparameter ohne Tuning, 5
> Stadtteil-Folds, Auswertung je Zeile. **Seit dem Lauf vom 03.08.2026 nicht
> reproduziert.** Die Werte dienen der Orientierung, welche Größenordnung zu
> erwarten ist — sie gehören **nicht** in die Arbeit, bevor `m02_menge.py` sie
> neu gerechnet hat.

Zielgröße `anzahl_einsaetze`.

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
der Rohskala bei −3,601. Bei `dominante_einsatzart` erreicht Random Forest 0,301
Macro-F1 gegen 0,220 der Mehrheitsklasse.

Für die logistische Regression lag der Vortestwert bei 0,282 — sie schlägt die
Baseline also ebenfalls und liegt nah am Forest. Der Verzicht auf sie (#31)
erfolgt aus Fokusgründen, nicht mangels Eignung; **das ist in 6.2 so zu
schreiben.** Eine verworfene Option als untauglich darzustellen, die es nicht
ist, wäre genau die Art unbelegter Behauptung, die das Gutachten unter R9
kritisiert.

---

## 6. Warum überhaupt komplexere Verfahren? — die Begründungskette

Die Arbeit muss belegen, dass der Schritt über ein einfaches Regressionsmodell
hinaus gerechtfertigt ist. Diese Begründung entsteht **aus der Regression
selbst**, nicht aus einer Behauptung. Vier Schritte — die Zahlen stammen alle
aus dem Vortest vom 28.07.2026 und sind mit `m01_eignung.py` **neu zu rechnen**,
bevor sie in die Arbeit gehen:

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
bei 29 Trainingsstadtteilen zu Überanpassung führt. Genau diese Lücke schließen
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
modelle/m03_struktur.py        dominante Einsatzart, zwei Verfahren
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
  rechnet dann linear weiter — das erklärt vermutlich die große Streuung bei der
  Rate. Der Anteil solcher Fälle ist zu quantifizieren.
- Klassenbalance von `dominante_einsatzart` und die Frage, ob vier Klassen bei
  so dünn besetzter Brandklasse tragfähig sind

### Was `m04_shap.py` beachten muss

SHAP nur für Modelle, die ihre Baseline schlagen — sonst erklärt man Rauschen.
Die Lag-Merkmale sind nicht enthalten, das frühere Blockproblem entfällt; die
Strukturmerkmale sind untereinander aber weiterhin korreliert, Beiträge
verteilen sich also — blockweise interpretieren. Der VIF-Wert ist mit
`m01_eignung.py` neu zu bestimmen.

---

## 8. Was nicht passieren darf

- Skalierung, Imputation oder Encoding **vor** dem Fold-Split
- Eigene Fold-Berechnung statt `fold_masken`
- Auswertung des Hold-outs vor Abschluss des Tunings
- Lag-Merkmale im Hauptvergleich
- Accuracy als Hauptmaß der Klassifikation
- Ridge oder `RidgeClassifier` auf die nominale Zielgröße anwenden
- Die logistische Regression als drittes Klassifikationsverfahren wieder
  aufnehmen, ohne Decision Log #31 zu revidieren
- Rangfolgen der drei Verfahren, wo sich die Streuungsbereiche überlappen
- Änderungen an `prep/` — die Aufbereitung ist abgeschlossen und getestet
