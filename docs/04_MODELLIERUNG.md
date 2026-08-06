# Spezifikation für `modelle/`

> Enthält **keine Ergebniszahlen** — die stehen in `03_STAND.md`. Enthält auch
> keine Argumentation für die Arbeit; die steht als Kommentarblock in `main.tex`
> bei Kapitel 6.2 und im Decision Log.

`prep/` ist abgeschlossen und wird nicht mehr angefasst. Die Modellskripte lesen
ausschließlich die fertigen Parquet-Dateien. Die bestehenden `m01`–`m03`
beziehen sich noch auf die alte Datenstruktur und sind **vollständig neu zu
schreiben**.

---

## 0. Checkliste vor der Implementierung

Stand 04.08.2026. Alles, was stehen muss, **bevor** die erste Modellzeile
geschrieben wird. ✅ erledigt · ⬜ offen · ⚠️ offen und blockierend.

### A · Datengrundlage

- [x] `prep/build.py` gelaufen, Artefakte aktuell
- [x] 19/19 Prüfungen bestanden
- [x] Beide Datensätze auf derselben Analyseeinheit (Stadtteil × Monat)
- [x] Identische Fold-Zuteilung in beiden Dateien
- [x] Keine fehlenden Werte, Merkmale durchgehend `float64`
- [x] Keine Ergebnisvariablen im Datensatz (per Test abgesichert)
- [x] Keine zeitgleichen Merkmale — ACS-Versatz, Crime-Fenster endet im Vormonat
- [x] Hold-out (6 Stadtteile) unberührt

### B · Zielgrößen und Gütemaße

- [x] Drei Zielgrößen festgelegt: `anzahl_einsaetze`, `einsaetze_je_1000_ew`, `dominante_einsatzart`
- [x] Gütemaße je Strang: RMSE/MAE/R² · Macro-F1 und Macro-AUROC
- [x] **Bei der Rate ist RMSE das Hauptmaß**, R² nur nachrichtlich — Begründung in `03_STAND.md`
- [x] Die vier `anteil_*` sind keine Modellzielgröße, nur Rechenbasis

### C · Verfahrensauswahl

- [x] Drei Verfahrensfamilien: linear (Ridge) · Bagging (RF) · Boosting (XGBoost)
- [x] Eignungsprüfung gerechnet (`vorpruefung/v2_eignung.py`)
- [x] Ridge auf `log(1+y)` — belegt durch Residuenbild
- [x] RF/XGBoost in der **Regression** — belegt durch RESET-Test und Interaktionen
- [x] RF/XGBoost in der **Klassifikation** — belegt durch Signaltest und geringe lineare Ausschöpfung
- [x] Drei Verfahren Regression / zwei Klassifikation — begründet (#31), freigegeben 03.08.

### D · Messlatte

- [x] Stufe 1 (ohne Merkmale) und Stufe 2 (einfachste passende Form) je Strang gerechnet
- [x] Werte in `03_STAND.md`, Abschnitt 4
- [x] Auflage „nichtlineare Baseline" beantwortet (`01_VORGABEN.md`, Abschnitt 0)
- [x] **Stufe 2 ist die Latte**, nicht Stufe 1

### E · Validierungsrahmen

- [x] Stadtteil-Split, `fold` und `ist_holdout` stehen in den Dateien
- [x] Wiederholungen festgelegt: `WIEDERHOLUNGEN = 10`
- [x] **Innerer CV gruppiert nach Stadtteil** (`GroupKFold(4)`, `groups=train["stadtteil"]`) — umgesetzt in `m02_menge.tune()`
- [x] Hold-out wird genau einmal ausgewertet, nach Abschluss des Tunings
- [x] Auswertungsebene: je Zeile (Stadtteil × Monat), einheitlich für beide Stränge

### F · Tuning

- [x] Suchräume und Budget in `modelle/config_modelle.py`
- [x] Gleiches Budget für jedes Verfahren (Fairness)
- [x] Getunt wird nur auf den Trainingsstadtteilen des jeweiligen Folds
- [x] Tuning einmal auf Wiederholung 0, Parameter für 1–9 wiederverwendet — bewusste Vereinfachung, im Text zu benennen
- [x] `RANDOM_STATE` fixiert

### G · Auswertung — vorab festgelegt (#34)

- [x] Primäraussage: **je Verfahren gegen die Stufe-2-Baseline** (UF2)
- [x] Rangfolge zwischen den Verfahren nur bei signifikantem gepaartem Wilcoxon-Test, α = 0,05, je Zielgröße getrennt
- [x] Bei Nichtsignifikanz: „nicht unterscheidbar" mit Konfidenzintervall der Differenz
- [x] **Trainings- und Inferenzzeiten** werden gemessen (UF3) — umgesetzt in `m02_menge.ein_lauf()`
- [x] Ausgabe: `*_folds.csv` · `*_mittel.csv` · `tuning.csv` · `vergleich.csv`
- [x] Keine nachträgliche Zuschneidung der Auswertung (kein Aufteilen nach Extrapolationsgrad)
- [x] **Streuung zweistufig aggregieren** — `std_wiederholungen` statt `std_folds` als maßgeblicher Wert (R-5)
- [x] **`vergleich.csv` trennt primär und sekundär**, Zahl der Tests wird mitgeführt (R-10)
- [x] **Hold-out nur mit Argument** `m02_menge.py holdout` — konstruktiv abgesichert: ohne das Argument filtert `main()` die Hold-out-Zeilen heraus, bevor irgendetwas rechnet
- [x] **NegBin ohne Offset als Zusatzvariante** in `v1_baselines.py` — gerechnet, Vorteil des Offsets **−0,0017 RMSE**, R-9 entfällt (`07_BEFUNDE.md`, B-19)
- [x] **Wiederholte Splits neu gebaut** — `vorpruefung/v0_aufteilung.py`, weil der `versatz` das Hold-out rotiert und keine 10 verschiedenen Aufteilungen liefert (B-1 bis B-3)
- [x] **Baselines über alle 10 Wiederholungen**, sonst gibt es für 45 von 50 Läufen keinen gepaarten Gegenwert (B-4)
- [x] **`xgboost` und `shap` in `requirements.txt`** — beide fehlten, beide blockierend (B-7, B-8)

### H · Modellspezifische Auflagen

- [x] Ridge: `StandardScaler` und `log(1+y)` **in der Pipeline**, Rücktransformation per `expm1` vor der Metrik
- [x] Skalierung betrifft nur Ridge — beide Klassifikationsverfahren sind baumbasiert
- [x] Klassifikation: `class_weight="balanced"` bzw. `sample_weight`, **kein Resampling**
- [x] `XGBClassifier`: Label-Encoder **einmal global** auf allen vier Klassen fitten
- [x] Keine Skalierung, Imputation oder Encoding vor dem Fold-Split

### I · Was nicht blockiert, aber benannt sein muss

- [x] **Stadtteil-Split und Baselines freigegeben** (Schröter, 04.08.2026, #35) — drei Auflagen daraus in `01_VORGABEN.md`, Abschnitt 0
- [x] Risikoregister aktuell (`06_RISIKEN.md`, Stand 04.08.)
- [x] Decision Log vollständig bis #34

---

## 1. Alle Stellschrauben auf einen Blick

Grundlage für Kapitel 6.3 („Modellkonfiguration und Hyperparameter-Suchräume").
Die Spalte **Art** entscheidet, *wie* der Wert zu begründen ist.

| Größe | Wert | Wo | Art |
|---|---|---|---|
| Analysezeitraum | 2015-01 bis 2025-12 | `config.START/ENDE` | zwingend |
| Lag-Vorlauf | 12 Monate | `config.VORLAUF_MONATE` | zwingend |
| ACS-Publikationsversatz | 1 Jahr | `config.ACS_PUBLIKATIONS_LAG` | zwingend |
| Kriminalitäts-Fenster | 12 Monate, endet im Vormonat | `config.CRIME_FENSTER_MONATE` | Abwägung |
| Antwortzeit-Filter | 0–60 Minuten | `config.ANTWORTZEIT_*` | Abwägung |
| Randmonat-Warnschwelle | 50 % des Medians | `config.VOLLSTAENDIGKEITS_SCHWELLE` | Konvention |
| **Anzahl Folds** | **5** | `config.N_FOLDS` | **Abwägung** |
| Hold-out-Größe | 6 Stadtteile | folgt aus `N_FOLDS + 1` | Folge |
| Stratifizierung | Bevölkerung + seltenste Klasse | `ergaenze_aufteilung` | zwingend |
| **Wiederholungen** | **10** | `config_modelle.WIEDERHOLUNGEN` | **Abwägung** |
| **Tuning-Budget** | **50 Iterationen** | `config_modelle.TUNING_BUDGET` | **Abwägung** |
| Suchverfahren | Randomized statt Grid | `SUCHRAEUME` | begründbar |
| Innere Folds beim Tuning | 4, gruppiert nach Stadtteil | `GroupKFold(4)` | Abwägung |
| Tuning-Zeitpunkt | einmal auf Wiederholung 0 | #34 | Abwägung |
| Signifikanzniveau | α = 0,05 | #34 | Konvention |
| Zufallszahl | 42 | `config_modelle.RANDOM_STATE` | Konvention |

### Vier Sorten von Zahlen — vier Sorten von Sätzen

Der häufigste Fehler ist, alle wie die erste Sorte zu behandeln. Das fällt auf,
sobald jemand nachfragt.

**Zwingend** — folgt aus den Daten oder einer Auflage. Der Satz benennt den Zwang:

> „Der ACS-Jahrgang für ein Jahr erscheint erst rund ein Jahr später. Ein
> Prädiktor, der zum Prognosezeitpunkt nicht publiziert war, wäre nicht
> verfügbar; der Versatz ist deshalb keine Einstellung, sondern eine Konsequenz."

**Abwägung** — man hätte auch anders gewählt, aber mit Grund. Der Satz nennt
**beide Seiten**. Diese Sorte macht eine Arbeit stark, weil sie zeigt, dass die
Alternative bedacht wurde:

> „Gewählt wurden fünf Folds. Bei zehn Folds enthielte jeder Test nur drei
> Stadtteile, das Ergebnis hinge dann noch stärker von einzelnen Einheiten ab;
> bei drei Folds stünden nur 19 statt 23 Stadtteile im Training."

**Konvention** — ein Standardwert ohne inhaltlichen Grund. Genau so benennen;
ein erfundener Grund ist schlechter als die ehrliche Auskunft:

> „Das Signifikanzniveau von 0,05 folgt der Konvention; ein inhaltlicher Grund
> für gerade diesen Wert besteht nicht."

**Folge** — ergibt sich aus einer anderen Entscheidung:

> „Die Stadtteile werden auf `N_FOLDS + 1` Gruppen verteilt. Die Hold-out-Größe
> ist damit eine Folge der Fold-Zahl und keine unabhängige Festlegung."

### Die drei Zahlen, nach denen am ehesten gefragt wird

**Fünf Folds.** Siehe oben. Zusatz: Bei sechs Teststadtteilen je Fold verschiebt
ein einzelner ungewöhnlicher Stadtteil das Ergebnis um höchstens ein Sechstel.

**Zehn Wiederholungen.** Abwägung Präzision gegen Rechenzeit — jeder Stadtteil
wird in zehn verschiedenen Fold-Konstellationen getestet. Unbedingt dazusagen:
Der Präzisionsgewinn ist **kleiner als √10**, weil die Läufe nicht unabhängig
sind (R-5).

**50 Tuning-Iterationen.** Bergstra & Bengio (2012) zeigen, dass Zufallssuche
mit wenigen Dutzend Iterationen eine Gittersuche meist schlägt, weil sie den
Suchraum in den relevanten Dimensionen dichter abtastet. Der eigentliche Punkt
ist aber die **Gleichheit**: Ridge hat nur einen Hyperparameter und bräuchte
weniger — ein ungleiches Budget wäre kein Algorithmenvergleich mehr, sondern
ein Budgetvergleich.

---

## 1b. Merkmale und Zielgrößen

**Zwölf Merkmale**, in beiden Datensätzen identisch (`PRAEDIKTOREN + SAISON` aus
`prep/config.py`): zehn Strukturmerkmale plus `monat_sin`, `monat_cos`.

```
median_haushaltseinkommen   armutsquote_pct        akademikerquote_pct
median_miete                leerstandsquote_pct    log_bevoelkerung
log_kriminalitaetsindex     anteil_altbau_vor_1940_pct
anteil_wohngebaeude_pct     anteil_risikogewerbe_pct
monat_sin                   monat_cos
```

| Strang | Zielgröße | Typ | Gütemaße |
|---|---|---|---|
| Menge | `anzahl_einsaetze` | Zähldaten | RMSE, MAE, R² |
| Menge | `einsaetze_je_1000_ew` | stetig | RMSE, MAE, R² |
| Struktur | `dominante_einsatzart` | 4 Klassen | **Macro-F1**, Macro-AUROC |

**Kein Modellmerkmal:** `lag_1`, `lag_12`, `rolling_mean_3` (unter dem
Stadtteil-Split die eigene Vergangenheit des Teststadtteils), `gesamtbevoelkerung`
und `kriminalitaetsindex` in Rohform (Offset und Deskription).

Die Lags werden **in keinem Analysestrang verwendet** — auch nicht in einem
Zusatzlauf mit Zeitschnitt. Ein zweiter Validierungsrahmen verstieße gegen R1
und R8; die zeitreihengerechte Variante wird in Kapitel 8 begründet verworfen
statt gerechnet (#29, präzisiert 04.08.2026). Im Datensatz bleiben sie zur
Deskription der zeitlichen Struktur in Kapitel 4.

**Keine eigene Zielgröße:** die vier `anteil_*`-Spalten. Aus ihnen entsteht die
Klasse per `argmax`; ihre Vorhersage wäre Regression, der Strang soll die *Art*
vorhersagen (R8). Accuracy ist als Hauptmaß ungeeignet — die Mehrheitsklasse
allein erreicht über 0,8.

---

## 2. Validierungsrahmen — nicht neu erfinden

Die Aufteilung steht als Spalten `fold` und `ist_holdout` **in den Dateien**.
Zu verwenden ist ausschließlich:

```python
from s2_datensaetze import fold_masken
train, test = fold_masken(daten, k)     # k = 1..5
```

**Wiederholte Splits sind verbindlich.** Bei 29 Entwicklungsstadtteilen schwankt
ein einzelner Fold massiv — schon die Baseline streut von R² −0,17 bis 0,73.
Deshalb 10 Wiederholungen mit unterschiedlichem Versatz, Mittelung über alle 50
Fold-Ergebnisse:

```python
from v0_aufteilung import selten_je_stadtteil, wiederholte_aufteilung
selten = selten_je_stadtteil(pd.read_parquet(PFAD_KLASSIFIKATION))
for w in range(WIEDERHOLUNGEN):
    d = wiederholte_aufteilung(daten, wiederholung=w, selten=selten)
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(d, k)      # MASKEN, nicht DataFrames
        train, test = d[tr], d[te]
```

**Korrigiert am 05.08.2026.** Hier stand zuvor
`ergaenze_aufteilung(daten, versatz=versatz)`. Das war aus drei Gründen nicht
durchführbar, alle am Datensatz nachgewiesen (`07_BEFUNDE.md`, B-1 bis B-3,
B-12):

1. Ohne `selten` entfällt die Stratifizierung nach der seltensten Klasse (#30);
   30 von 35 Stadtteilen landen in einem anderen Fold als in der Datei.
2. Der `versatz` rotiert nur die Gruppen*nummern*. Über 0–9 entstehen 6
   Partitionen statt 10 — und hielte man das Hold-out fest, sogar nur eine.
3. Gruppe 0 **ist** das Hold-out. Bei `versatz = 1` liegt keiner der sechs
   Hold-out-Stadtteile mehr im Hold-out.

`wiederholte_aufteilung()` hält das Hold-out fest, erhält die doppelte
Stratifizierung und mischt je Wiederholung innerhalb der Rangblöcke. Wiederholung 0
reproduziert die `fold`-Spalte der Datei bitgenau — geprüft per `assert` bei
jedem Aufruf.

Berichtet wird Mittelwert ± Standardabweichung. Überlappen die Bereiche zweier
Verfahren, ist das **so zu schreiben**, nicht als Rangfolge zu kaschieren (R6).

**Auswertungsebene:** je Zeile (Stadtteil × Monat), einheitlich für beide
Stränge. Zu dokumentieren: Die Strukturmerkmale sind innerhalb eines Jahres
nahezu konstant, die Monatsschwankung geht also vollständig ins Residuum. Eine
auf Stadtteilebene aggregierte Auswertung darf als getrennt gekennzeichnete
Zusatzangabe dazu.

Das Hold-out wird **genau einmal** ausgewertet, nach Abschluss des Tunings.

---

## 3. Die Verfahren — drei für die Menge, zwei für die Struktur

| | Regression | Klassifikation |
|---|---|---|
| linear | `Ridge` | — |
| Bagging | `RandomForestRegressor` | `RandomForestClassifier` |
| Boosting | `XGBRegressor` | `XGBClassifier` |

Echte Teilmenge, kein Verfahren kommt hinzu (Decision Log #31, freigegeben von
Schröter am 03.08.2026). Kurzfassung der Begründung — ausformuliert im
tex-Kommentar zu 6.2:

- **RF und XGBoost übertragen sich,** weil nur die Verlustfunktion wechselt
  (Gini/Entropie statt Varianzreduktion; `multi:softprob` statt
  `reg:squarederror`). Der Ensemble-Mechanismus bleibt identisch.
- **Ridge überträgt sich nicht:** quadratischer Fehler auf einer nominalen
  Zielgröße ist nicht definiert. `RidgeClassifier` geprüft und verworfen.
- **Logistische Regression entfällt** aus Fokusgründen (R4/R8), nicht mangels
  Eignung — das ist im Text so zu schreiben.

### Warum genau diese drei — Begründung für Kapitel 6.2

Die Auswahl ist keine Dreierliste aus dem Exposé, sondern eine **systematische
Abdeckung der Fehlerquellen**. Jedes Prognosemodell macht zwei Sorten Fehler:
zu starre Annahmen (Bias) und zu große Empfindlichkeit gegenüber den
Trainingsdaten (Varianz).

| Verfahren | Bias | Varianz | Strategie |
|---|---|---|---|
| Ridge | hoch (linear) | niedrig (Strafterm) | Struktur vorgeben |
| Random Forest | niedrig | durch Mittelung reduziert | Bagging |
| XGBoost | niedrig (sequenziell korrigiert) | durch Regularisierung kontrolliert | Boosting |

Drei Verfahren, drei verschiedene Antworten auf dasselbe Dilemma. Das ist der
Satz, der aus „standen im Exposé" eine begründete Auswahl macht.

**Ridge Regression.** Zwölf Merkmale bei 23 unabhängigen Trainingseinheiten,
maximaler VIF 11,5 — Einkommen und Miete messen weitgehend dasselbe. Genau für
diese Konstellation wurde der L2-Strafterm entwickelt: Bei korrelierten
Prädiktoren werden Kleinste-Quadrate-Schätzer instabil, kleine Datenänderungen
bewegen die Koeffizienten stark. Ridge verhindert das (`Hoerl1970`).

*Warum nicht Lasso oder Elastic Net:* Lasso setzt Koeffizienten auf exakt null,
betreibt also Merkmalsselektion. Die zwölf Merkmale sind aber vorab aus drei
Faktorgruppen begründet, und **Unterfrage 1 fragt nach dem Erklärungsbeitrag
aller Gruppen**. Ein Verfahren, das eine Gruppe eliminiert, umgeht die Frage.
Ridge behält alle Merkmale und schrumpft nur.

**Random Forest.** 33,7 % Extrapolation, große Fold-Streuung, Wechselwirkungen
zwischen Merkmalen (adjustiertes R² 0,805 → 0,919). Bäume finden Wechselwirkungen
datengetrieben, weil jeder Split auf den vorherigen bedingt. Und Splits nutzen
nur die **Ordnung** der Werte, nicht ihre Größe — das macht sie robust gegen
Ausreißer in den Prädiktoren (`Breiman2001`).

*Warum das Ensemble und nicht ein einzelner Baum:* gemessen in der eigenen
Vorprüfung. Ein Baum der Tiefe 3 schwankte in der Klassifikation zwischen
Macro-F1 0,197 und 0,353 — in zwei Folds der beste Wert überhaupt, in anderen
unter der Mehrheitsklasse. Genau diese Instabilität behebt Bagging.
Praktisch relevant: Random Forest ist vergleichsweise unempfindlich gegenüber
den Voreinstellungen (`Probst2019`), was bei begrenztem Tuning-Budget zählt.

**XGBoost.** 4.620 Zeilen, zwölf numerische Merkmale — ein mittelgroßer
tabellarischer Datensatz. `Grinsztajn2022` zeigen über 45 Datensätze, dass
Baumverfahren auf genau dieser Größenordnung führen, begründet durch ihre
Fähigkeit, **nicht-glatte Zielfunktionen** abzubilden. Das passt direkt auf den
Befund gekrümmter Zusammenhänge.

*Warum zusätzlich zu Random Forest:* Boosting greift die **andere** Fehlerquelle
an. Random Forest baut Bäume parallel und mittelt — das senkt Varianz. XGBoost
baut sie sequenziell, jeder korrigiert die Fehler des vorigen — das senkt Bias
(`Chen2016`). Beide zu vergleichen ist deshalb keine Doppelung, sondern der
eigentliche Erkenntnisgewinn: **Welche Fehlerquelle dominiert bei diesen Daten?**
Datenbezogen relevant ist außerdem die eingebaute Regularisierung
(`reg_lambda`, `subsample`, `colsample_bytree`) — bei 23 Trainingseinheiten ist
Überanpassung die Hauptgefahr, und die Gegenmittel stehen im Suchraum.

### Warum nicht die naheliegenden Alternativen

Dieser Absatz macht die Auswahl stark, weil er zeigt, dass auch das Verworfene
bedacht wurde:

| Alternative | Warum nicht |
|---|---|
| Neuronale Netze | Bei 35 Analyseeinheiten nicht sinnvoll trainierbar; auf tabellarischen Daten dieser Größe unterlegen (`Grinsztajn2022`) |
| LightGBM, CatBoost | Leistungsgleich zu XGBoost, aber eine vierte Variante derselben Familie ohne neue Frage (R8) |
| Support Vector Machines | Kernelwahl wäre selbst zu begründen; keine interpretierbaren Beiträge für die SHAP-Analyse |
| Einzelner Entscheidungsbaum | In der eigenen Vorprüfung als zu instabil gemessen |

**Literaturstatus:** Alle fünf Quellen stehen bereits in der Bibliografie
(`Hoerl1970`, `Breiman2001`, `Chen2016`, `Grinsztajn2022`, `Probst2019`).
`Grinsztajn2022` wurde am 03.08.2026 verifiziert; die übrigen sind Standardwerke
und vor der Verwendung im Text gegenzuprüfen.

### Auflagen je Verfahren

Modellspezifisch ist nur, was **innerhalb der Pipeline je Fold** passiert.

- Ridge auf `log(1+y)` schätzen, Gütemaße nach `expm1` auf der Originalskala
- `StandardScaler` in die Pipeline, nicht vorher. Betrifft **nur Ridge** — beide
  Klassifikationsverfahren sind baumbasiert und brauchen keine Skalierung
- Klassifikation mit `class_weight="balanced"` bzw. `sample_weight`, als
  Modellhyperparameter, **kein Resampling** im Preprocessing
- `XGBClassifier` braucht Integer-Labels; den Encoder **einmal global** auf allen
  vier Klassen fitten, nicht je Fold. Wahrscheinlichkeitsspalten danach auf die
  Reihenfolge von `KLASSEN` zurückbringen

### Hyperparameter-Suche

`RandomizedSearchCV`, Suchräume und `TUNING_BUDGET` aus
`modelle/config_modelle.py`, gleiches Budget für jedes Verfahren. Getunt wird
ausschließlich auf den Trainingsstadtteilen des jeweiligen Folds.

**Der innere CV muss nach Stadtteil gruppieren** — `GroupKFold`, nicht das
voreingestellte `KFold`. Sonst stünde derselbe Stadtteil im inneren Training und
in der inneren Validierung, die Hyperparameter würden auf einen geleakten
Schätzwert optimiert, und der Vorteil des äußeren Stadtteil-Splits wäre im
Tuning wieder verspielt. Das ist die häufigste Fehlerquelle an dieser Stelle und
von außen nicht sichtbar — die Zahlen sähen nur zu gut aus.

Getunt wird **einmal auf Wiederholung 0**; die gewählten Parameter gelten für
die Wiederholungen 1–9. Die Wiederholungen unterscheiden sich nur im Versatz der
Fold-Zuteilung und dienen der Streuungsschätzung, nicht der Modellwahl. Das ist
eine bewusste Vereinfachung und im Text zu benennen.

---

## 4. Aufbau der Skripte

```
m02_menge.py       Anzahl und Rate, drei Verfahren       -> results/regression/
m03_struktur.py    dominante Einsatzart, zwei Verfahren  -> results/klassifikation/
m04_shap.py        nur für Modelle, die Stufe 2 schlagen -> results/shap/
m05_abbildungen.py alle Abbildungen aus den CSV-Dateien  -> results/abbildungen/
```

`m05` rechnet **nichts** — es liest ausschließlich die CSV-Dateien und erzeugt
daraus die Abbildungen. Dadurch lassen sich Darstellungen ändern, ohne die
Modelle neu zu rechnen, und nach einem neuen Lauf ist ein Befehl genug.

### Struktur von `m02_menge.py` — die Vorlage

`m03_struktur.py` spiegelt sie, mit zwei statt drei Verfahren und Macro-F1
statt RMSE. Sieben Funktionen, jede mit einer Aufgabe:

```
verfahren(name, ziel)      -> Pipeline
    Baut die sklearn-Pipeline. Ridge bekommt StandardScaler und
    TransformedTargetRegressor(log1p / expm1), RF und XGBoost nur das nackte
    Modell. Gibt eine noch ungetunte Pipeline zurueck.

tune(pipeline, train, ziel) -> dict
    RandomizedSearchCV mit GroupKFold(4), groups = train["stadtteil"].
    Budget und Suchraeume aus config_modelle. Gibt die besten Parameter
    zurueck, NICHT das Modell - trainiert wird spaeter neu.

ein_lauf(pipeline, train, test, ziel) -> dict
    Ein Fit, eine Vorhersage, mit Zeitmessung um beides herum.
    Rueckgabe: RMSE, MAE, R2, train_sekunden, inferenz_sekunden,
    n_train, n_test, extrapolationsanteil.

phase_tuning(panel)        -> DataFrame
    Fuer jede Zielgroesse, jedes Verfahren, jeden Fold auf Wiederholung 0:
    tune() aufrufen. 30 Zeilen -> tuning.csv.

phase_bewertung(panel, parameter) -> DataFrame
    Versatz 0..9 x Fold 1..5 x Verfahren x Zielgroesse: ergaenze_aufteilung(),
    dann ein_lauf() mit den Parametern aus Phase 1. 300 Zeilen -> menge_folds.csv.

aggregiere(folds)          -> DataFrame
    Zweistufig: erst je Wiederholung ueber die 5 Folds mitteln, dann ueber die
    10 Wiederholungen. Liefert mittelwert, std_folds und std_wiederholungen.
    -> menge_mittel.csv

vergleiche(folds, baselines) -> DataFrame
    Gepaarter Wilcoxon. Primaer: jedes Verfahren gegen Stufe 2. Sekundaer:
    Verfahrenspaare. Spalten rolle und n_tests_familie. -> vergleich.csv

main(argv)
    Ohne Argument: Phase 1, Phase 2, Aggregation, Vergleich.
    Mit "holdout": zusaetzlich auf allen 29 Entwicklungsstadtteilen
    trainieren und auf den 6 Hold-out-Stadtteilen bewerten -> holdout.csv.
```

**Der Ablauf in einem Bild:**

```
regression.parquet
   │
   ├─ phase_tuning     30 Suchlaeufe a 200 Fits   ─→ tuning.csv
   │        │
   │        └─ beste Parameter je (Ziel, Verfahren, Fold)
   │                    │
   ├─ phase_bewertung   300 Laeufe mit Zeitmessung ─→ menge_folds.csv
   │                    │
   ├─ aggregiere        zweistufig                 ─→ menge_mittel.csv
   │                    │
   └─ vergleiche        Wilcoxon primaer/sekundaer ─→ vergleich.csv

   nur mit Argument "holdout":                     ─→ holdout.csv
```

**Zwei Stellen, an denen es schiefgeht, wenn man nicht aufpasst:**

`tune()` gibt **Parameter** zurueck, kein Modell. Wer das gefittete
`best_estimator_` weiterverwendet, trainiert auf dem inneren Trainingsanteil
statt auf allen Trainingsstadtteilen des Folds — und verschenkt Daten.

`ein_lauf()` misst die Zeit **um `fit` und `predict` herum**, nicht um die ganze
Funktion. Sonst steckt die Metrikberechnung mit in der Zahl.

Die Vorprüfung liegt in `vorpruefung/` und ist gerechnet. Jedes Modellskript
liest ausschließlich die Parquet-Dateien, die beiden Config-Dateien und die
Baseline-CSVs, schreibt nach `results/` und legt **nichts** fest, was in `prep/`
gehört.

**Kein gemeinsames Hilfsmodul.** `m02` und `m03` teilen die Struktur, aber nicht
die Metriken, Modelle und Zielgrößenbehandlung. Ein geteiltes Modul für zwei
Aufrufer bringt mehr Indirektion als Ersparnis — die etwa 30 doppelten Zeilen
sind der bessere Tausch. Jedes Skript bleibt von oben nach unten lesbar.

### Ausgabeformat — Spalten vorab festgelegt

Damit Kapitel 7 planbar ist und nichts nachträglich fehlt.

**`results/regression/menge_folds.csv`** — eine Zeile je Lauf, 300 Zeilen
(2 Zielgrößen × 3 Verfahren × 10 Wiederholungen × 5 Folds):

```
zielgroesse · verfahren · wiederholung · fold
RMSE · MAE · R2
train_sekunden · inferenz_sekunden
n_train · n_test · extrapolationsanteil
```

**`results/klassifikation/struktur_folds.csv`** — 100 Zeilen
(1 Zielgröße × 2 Verfahren × 10 × 5), statt der drei Regressionsmaße:

```
macro_f1 · macro_auroc · accuracy · n_brand_test
```

**`*_mittel.csv`** — je Zielgröße und Verfahren: Mittelwert, `std_folds`
(über alle 50 Läufe) und **`std_wiederholungen`** (über die 10
Wiederholungsmittel). Maßgeblich ist die zweite — siehe Fallstrick 1 unten.

**`tuning.csv`** — je Zielgröße, Verfahren und Fold die gewählten
Hyperparameter. Das ist Kapitel 6.3 und darf nicht rekonstruiert werden müssen.

**`vergleich.csv`** — je Zielgröße und Paarung: mittlere gepaarte Differenz,
Anzahl gewonnener Folds, Wilcoxon-p, dazu `rolle` (`primaer` = gegen die
Stufe-2-Baseline, `sekundaer` = Verfahren gegen Verfahren) und
`n_tests_familie`. Grundlage für #34.

**`holdout.csv`** — nur mit ausdrücklichem Argument, siehe Fallstrick 4.

`extrapolationsanteil` und `n_brand_test` wandern mit, weil sie erklären, warum
ein Fold aus der Reihe fällt — ohne sie steht man später vor Ausreißern ohne
Erklärung.

### Sonderfälle, die auftreten werden

- **Negative Vorhersagen nach `expm1`.** Ridge auf `log(1+y)` kann bei starker
  Extrapolation Werte unter −1 liefern; `expm1` ergibt dann etwas unter null.
  Auf null kappen wäre ein Eingriff — stattdessen unverändert lassen und die
  Häufigkeit im Bericht ausweisen. Es ist ein Befund über das Verfahren.
- **Fehlende Klasse im Testfold.** Macro-AUROC ist nicht definiert, wenn eine
  Klasse im Test fehlt. Durch die Stratifizierung (#30) sollte das nicht
  vorkommen; falls doch, wird der Wert als fehlend geführt und **nicht** durch
  null ersetzt — sonst zieht er den Mittelwert nach unten.
- **Konvergenzwarnungen der logistischen Regression** bei `max_iter`. Nicht
  unterdrücken, sondern zählen und berichten.
- **`zero_division=0`** bei Macro-F1 ist gesetzt und muss gesetzt bleiben, sonst
  bricht der Lauf bei einer nicht vorhergesagten Klasse ab.

### Hold-out — die Schlussbewertung

Genau **einmal**, nach Abschluss von Modellwahl und Tuning. Ablauf: Je Verfahren
und Zielgröße wird auf **allen 29 Entwicklungsstadtteilen** mit den in der
Kreuzvalidierung gewählten Hyperparametern neu trainiert und auf den 6
Hold-out-Stadtteilen bewertet.

**Nur mit ausdrücklichem Argument** — `python modelle/m02_menge.py holdout`.
Ohne das Argument rührt das Skript die Hold-out-Zeilen nicht an. Grund: Wer das
Ergebnis einmal gesehen hat, kann es nicht ungesehen machen, und jede spätere
Entscheidung ist davon berührt. Der Schalter macht Hinsehen zu einer bewussten
Handlung.

Zu berichten ist, dass es **eine einzige Messung an sechs Einheiten** ist — kein
Mittelwert, keine Streuung. Die Zahl ist deutlich unsicherer als die
Kreuzvalidierungswerte und darf nicht als deren Bestätigung gelesen werden
(`06_RISIKEN.md`, R-4).

### Abbildungen für Kapitel 7 — `m05_abbildungen.py`

Drei Abbildungen, alle aus den CSV-Dateien erzeugt, keine von Hand:

| | Inhalt | Beantwortet |
|---|---|---|
| **A1** | Boxplot je Verfahren über die 50 Läufe, je Zielgröße | zeigt die Streuung ehrlich statt sie zu mitteln |
| **A2** | Balkendiagramm Verfahren gegen Stufe-2-Baseline, mit Fehlerbalken aus `std_wiederholungen` | die Primäraussage nach #34 (UF2) |
| **A3** | Streudiagramm Trainingszeit (log) gegen Prognosegüte, ein Punkt je Verfahren | UF3 und UF4 in einem Bild |

**Anforderungen an die Darstellung** — sie landen im gedruckten Dokument:

- **Vektorformat.** PDF, nicht PNG. Rasterbilder werden im Druck unscharf, und
  Schröter hat Gestaltung als eigenes Kriterium bewertet.
- **Schriftgröße mindestens 9 pt** in der Abbildung, damit sie nach dem
  Verkleinern auf Textbreite lesbar bleibt. Faustregel: die Abbildung in der
  Endgröße erzeugen, nicht groß erzeugen und dann skalieren.
- **Keine Titel in der Abbildung.** Die Bildunterschrift in LaTeX ist der Titel;
  beides doppelt sich sonst.
- **Graustufentauglich.** Verfahren zusätzlich über Schraffur oder Marker
  unterscheiden, nicht allein über Farbe.
- **Achsenbeschriftung mit Einheit**, deutsches Dezimalkomma.
- **Bei A1 die Nulllinie einzeichnen**, wo R² dargestellt wird — negative Werte
  sind hier normal und müssen als solche erkennbar sein.
- **Fehlerbalken immer beschriften**: `std_wiederholungen`, nicht `std_folds`.
  Ohne Angabe ist ein Fehlerbalken bedeutungslos.

Ausgabe nach `results/abbildungen/`, Dateinamen `a1_boxplot_menge.pdf` usw. —
so lassen sie sich in der tex direkt einbinden.

### `m04_shap.py`

Nur für Modelle, die Stufe 2 schlagen — sonst erklärt man Rauschen. `TreeExplainer`
für RF und XGBoost, für Ridge genügen die Koeffizienten. Gerechnet wird auf einem
Fold, nicht auf allen; die Auswahl ist zu begründen.

**Blockweise interpretieren.** Die Strukturmerkmale sind untereinander korreliert
(max VIF 11,5 bei `median_haushaltseinkommen`, 7,1 bei `median_miete`) — SHAP
verteilt den Beitrag dann auf mehrere Merkmale, und einzelne Werte sind nicht
sinnvoll deutbar. Zusammenfassen zu den drei Faktorgruppen des Exposés:
sozioökonomisch, kriminalitätsbezogen, baulich.

### Reproduzierbarkeit

`RANDOM_STATE` ist gesetzt. Zusätzlich beim Lauf festhalten: Python-Version und
die Versionen von scikit-learn, XGBoost, statsmodels und pandas. Ohne sie ist
„reproduzierbar" eine Behauptung — Baumverfahren können zwischen Versionen
abweichen.

---

## 5. Wie das Ergebnis berichtet wird

Festgelegt in Decision Log #34, **vor** dem ersten Modelllauf. Drei Bausteine,
jeder einzeln messbar:

| | Aussage | Unterfrage |
|---|---|---|
| 1 | Schlägt Verfahren X die **Stufe-2-Baseline**? Um wie viel, mit welcher Streuung? Je Verfahren und Zielgröße einzeln | UF2 |
| 2 | Trainings- und Inferenzaufwand je Verfahren | UF3 |
| 3 | Daraus: Welches Verfahren eignet sich **für diesen Datensatz** | UF4 |

Eine Rangfolge *zwischen* den Verfahren wird nur berichtet, wenn der gepaarte
Wilcoxon-Test über alle Fold-Ergebnisse sie hergibt (α = 0,05, je Zielgröße
getrennt). Andernfalls steht dort „nicht unterscheidbar", mit dem
Konfidenzintervall der Differenz.

**Warum so:** Der Abstand Verfahren↔Baseline ist ein Vielfaches des Abstands
Verfahren↔Verfahren und damit messbar. Unterfrage 2 fragt ohnehin wörtlich „im
Vergleich zu Baselines". Unterfrage 3 ist gegen die Streuung immun — im Aufwand
trennen Größenordnungen. Damit steht eine belastbare Antwort auf „welches
Verfahren eignet sich hier" auch dann, wenn sich die Prognosegüten nicht trennen
lassen (`06_RISIKEN.md`, R-1).

**Nicht zulässig:** die Auswertung nachträglich so zuzuschneiden, dass ein
Unterschied sichtbar wird — etwa nach Extrapolationsgrad getrennt. Das weicht
die Fairness-Regel auf, weil die Verfahren dann nicht mehr auf identischen
Zeilen verglichen werden.

### Vier Fallstricke der Auswertung — und was der Code dagegen tut

Begründungen stehen in `06_RISIKEN.md`; hier nur die Umsetzung.

**1 · Die Streuung über 50 Läufe ist zu optimistisch (R-5).** Die 50
Fold-Ergebnisse sind nicht unabhängig — es sind dieselben 29 Stadtteile in zehn
verschiedenen Gruppierungen. Ein Konfidenzintervall aus σ/√50 wäre zu eng.

> **Umsetzung:** Zweistufig aggregieren. Erst je Wiederholung über die 5 Folds
> mitteln — das ergibt **10 Werte** je Verfahren und Zielgröße. Die
> Standardabweichung *dieser zehn* ist die zu berichtende Streuung. In
> `*_mittel.csv` beide Spalten führen: `std_folds` (über alle 50, optimistisch)
> und `std_wiederholungen` (über die 10, maßgeblich).

**2 · Sieben Tests, keine Korrektur (R-10).** Bei α = 0,05 liegt die
Wahrscheinlichkeit für mindestens einen Zufallstreffer bei rund 30 %.

> **Umsetzung, zweifach.** Erstens strukturell: `vergleich.csv` bekommt eine
> Spalte `rolle` mit `primaer` (Verfahren gegen Stufe-2-Baseline — keine
> Testfamilie, jede Frage ist vorab einzeln formuliert) und `sekundaer`
> (paarweise Verfahrensvergleiche), dazu `n_tests_familie`.
>
> Zweitens rechnerisch: **Holm-Bonferroni innerhalb der sekundären Familie.**
> p-Werte aufsteigend sortieren, den kleinsten gegen α/m prüfen, den nächsten
> gegen α/(m−1), und so fort bis zur ersten Nichtablehnung. Gleiche
> Fehlerkontrolle wie Bonferroni, aber uniform stärker. Zusätzliche Spalte
> `p_holm`; `wilcoxon_p` bleibt als Rohwert daneben stehen.

**Präzisiert am 05.08.2026: es sind ZWEI Familien, nicht eine mit sieben Tests**
(`07_BEFUNDE.md`, B-6). Regression und Klassifikation beantworten verschiedene
Teilfragen; ein Zufallstreffer im einen Strang macht den anderen nicht falsch.

| Strang | Skript | sekundäre Tests | m | kleinste Schwelle |
|---|---|---|---|---|
| Regression | `m02_menge.py` | 3 Paare × 2 Zielgrößen | 6 | α/6 = 0,0083 |
| Klassifikation | `m03_struktur.py` | 1 Paar (RF vs. XGBoost) | 1 | α = 0,05 |

Der Klassifikationsvergleich läuft damit **ungekorrigiert** — das ist in
Kapitel 7 ausdrücklich zu benennen, nicht zu verschweigen. Der praktische
Unterschied bei der Regression ist gering (0,0083 statt 0,0071).

**Fallstrick 2b · Der Test setzt Unabhängigkeit voraus, die es nicht gibt
(R-11).** Holm korrigiert Mehrfachvergleiche, nicht Pseudoreplikation. Die 50
Fold-Ergebnisse stammen von denselben 29 Stadtteilen.

> **Umsetzung:** Der **Primärtest läuft auf den 10 Wiederholungsmitteln**
> (Spalte `teststufe = wiederholung`), nicht auf den 50 Einzelläufen — dieselbe
> zweistufige Logik wie bei der Streuung. Kleinstes erreichbares zweiseitiges p
> bei n = 10: 0,00195, also auch nach Holm erreichbar. Bei n = 5 wäre es 0,0625
> und Signifikanz strukturell unmöglich. Der Test über alle 50 steht als
> `teststufe = lauf` daneben, ausdrücklich als Sensitivität.

**3 · Die Baseline hat einen strukturellen Vorteil (R-9).** Die Negative
Binomial bekommt `log(Bevölkerung)` als **Offset** — mit fest auf 1 gesetztem
Koeffizienten. Ridge, RF und XGBoost bekommen dieselbe Größe als gewöhnliches
Merkmal und müssen den Zusammenhang schätzen.

> **Umsetzung:** Den Vorteil nicht ausgleichen — das würde die Spezifikation der
> Vergleichsverfahren ändern. Stattdessen **beziffern**: In `v1_baselines.py`
> zusätzlich eine Negative Binomial **ohne** Offset rechnen, mit
> `log_bevoelkerung` als normalem Prädiktor. Die Differenz beider Varianten ist
> der Wert des Offsets — fünf Zeilen Code, und aus einem Vorbehalt wird eine
> Zahl. In Kapitel 8 als Fußnote zur Ergebnistabelle.

**4 · Das Hold-out darf man nicht versehentlich sehen.** Die Regel „genau
einmal, ganz am Schluss" ist eine Disziplinfrage — und Disziplin verliert man,
sobald das Ergebnis nebenbei mitläuft.

> **Umsetzung:** `holdout.csv` wird **nicht** bei jedem `m02`-Lauf erzeugt,
> sondern nur mit ausdrücklichem Argument:
> `python modelle/m02_menge.py holdout`. Ohne dieses Argument berührt das
> Skript die Hold-out-Zeilen nicht. Damit ist Hinsehen eine bewusste
> Handlung und kein Versehen.

---

## 6. Was nicht passieren darf

- Skalierung, Imputation oder Encoding **vor** dem Fold-Split
- Eigene Fold-Berechnung statt `fold_masken`
- Auswertung des Hold-outs vor Abschluss des Tunings
- Lag-Merkmale im Hauptvergleich
- Accuracy als Hauptmaß der Klassifikation
- Ridge oder `RidgeClassifier` auf die nominale Zielgröße
- Logistische Regression als drittes Klassifikationsverfahren ohne Revision von #31
- Rangfolgen, wo sich die Streuungsbereiche überlappen
- Änderungen an `prep/`
