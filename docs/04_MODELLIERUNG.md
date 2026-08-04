# Spezifikation für `modelle/`

> Enthält **keine Ergebniszahlen** — die stehen in `03_STAND.md`. Enthält auch
> keine Argumentation für die Arbeit; die steht als Kommentarblock in `main.tex`
> bei Kapitel 6.2 und im Decision Log.

`prep/` ist abgeschlossen und wird nicht mehr angefasst. Die Modellskripte lesen
ausschließlich die fertigen Parquet-Dateien. Die bestehenden `m01`–`m03`
beziehen sich noch auf die alte Datenstruktur und sind **vollständig neu zu
schreiben**.

---

## 1. Merkmale und Zielgrößen

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

## 5. Was nicht passieren darf

- Skalierung, Imputation oder Encoding **vor** dem Fold-Split
- Eigene Fold-Berechnung statt `fold_masken`
- Auswertung des Hold-outs vor Abschluss des Tunings
- Lag-Merkmale im Hauptvergleich
- Accuracy als Hauptmaß der Klassifikation
- Ridge oder `RidgeClassifier` auf die nominale Zielgröße
- Logistische Regression als drittes Klassifikationsverfahren ohne Revision von #31
- Rangfolgen, wo sich die Streuungsbereiche überlappen
- Änderungen an `prep/`
