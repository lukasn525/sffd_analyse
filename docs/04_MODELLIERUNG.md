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
- [ ] ⚠️ **Innerer CV muss nach Stadtteil gruppieren** (`GroupKFold`, nicht `KFold`) — spezifiziert, noch nicht implementiert
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
- [ ] ⬜ **Trainings- und Inferenzzeiten mitmessen** (UF3) — leicht zu vergessen, nachträglich teuer
- [x] Ausgabe: `*_folds.csv` (je Wiederholung und Fold) · `*_mittel.csv` · `tuning.csv`
- [x] Keine nachträgliche Zuschneidung der Auswertung (kein Aufteilen nach Extrapolationsgrad)

### H · Modellspezifische Auflagen

- [x] Ridge: `StandardScaler` und `log(1+y)` **in der Pipeline**, Rücktransformation per `expm1` vor der Metrik
- [x] Skalierung betrifft nur Ridge — beide Klassifikationsverfahren sind baumbasiert
- [x] Klassifikation: `class_weight="balanced"` bzw. `sample_weight`, **kein Resampling**
- [x] `XGBClassifier`: Label-Encoder **einmal global** auf allen vier Klassen fitten
- [x] Keine Skalierung, Imputation oder Encoding vor dem Fold-Split

### I · Was nicht blockiert, aber benannt sein muss

- [ ] ⬜ **Sprechstunde: der Stadtteil-Split.** Als einzige der vier Abweichungen leicht kritisch, weil er einem wörtlich genannten Element von Unterfrage 2 widerspricht (`06_RISIKEN.md`, R-7). Die übrigen drei sind unkritisch
- [x] Risikoregister aktuell (`06_RISIKEN.md`, Stand 04.08.)
- [x] Decision Log vollständig bis #34

---

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
from s2_datensaetze import ergaenze_aufteilung
for versatz in range(10):
    d = ergaenze_aufteilung(daten, versatz=versatz)
    for k in range(1, N_FOLDS + 1):
        train, test = fold_masken(d, k)
```

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
m01_eignung.py    Linearität + Residuen (nur Trainingsstadtteile, R7), VIF,
                  Extrapolationsanteil, Klassenbalance   -> results/eignungspruefung/
m02_menge.py      Anzahl und Rate, drei Verfahren        -> results/regression/
m03_struktur.py   dominante Einsatzart, zwei Verfahren   -> results/klassifikation/
m04_shap.py       nur für Modelle, die ihre Baseline schlagen
```

Jedes Skript liest ausschließlich die Parquet-Dateien und die beiden
Config-Dateien, schreibt CSV nach `results/` und legt **nichts** fest, was in
`prep/` gehört.

`m01_eignung.py` ist **gerechnet** (03.08.2026). Der Befund, auf dem Kapitel 6.2
ruht: Der RESET-Test verwirft die lineare Spezifikation, und sie scheitert an
**zwei** Dingen — an der Krümmung einzelner Effekte (am deutlichsten
`log_bevoelkerung`, Pearson +0,416 gegen Spearman +0,559) und an fehlenden
Wechselwirkungen (adjustiertes R² 0,805 → 0,919 mit 45 Interaktionstermen).
Baumverfahren fangen beides konstruktionsbedingt ab: Ein Split kann an
beliebiger Stelle schneiden, und jeder Split bedingt auf die vorherigen.

SHAP nur blockweise interpretieren — die Strukturmerkmale sind untereinander
korreliert (max VIF 11,5 bei `median_haushaltseinkommen`, 7,1 bei
`median_miete`), Beiträge verteilen sich.

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
