# Klassifikationsteil – Aufbau und Begründung

Stand 2026-07-26. Grundlage: `results/eignungspruefung/eignungspruefung_summary.md`.
Ergänzt `docs/UMSETZUNGSLEITFADEN_MODELLIERUNG.md`, Schritt 7.

---

## 1. Aufgabe und Analyseebene

**Zielgröße:** `ist_brand` = NFIRS-Code beginnt mit „1" (Brand-Serie 100er),
binär gegen alle übrigen Serien. Entspricht der im Exposé vorgesehenen
Vereinfachung (Decision Log #6).

**Ebene:** Einzeleinsatz. Die Einsatzart ist eine Eigenschaft des einzelnen
Einsatzes, nicht des Stadtteil-Monats. Die Alternative – Brandanteil je
Stadtteil-Monat – wäre eine Regression und würde F1/AUROC unmöglich machen,
also vom Exposé abweichen.

**Abgrenzung identisch zur Regression:** 2015-01 bis 2025-12, dieselben
35 Stadtteile. **350.481 Einsätze**, davon 13,6 % Brände (Verhältnis 1:6,4).
Beide Teile der Arbeit beziehen sich damit auf denselben Datenbestand – sonst
bricht der rote Faden.

---

## 2. Merkmale

### 2.1 Was NICHT verwendet werden darf (Ergebnisvariablen)

Diese Spalten stehen erst nach dem Einsatz fest oder sind eine Folge der
Einsatzart. Ihre Verwendung wäre Leakage im engeren Sinn – das Modell würde
die Antwort aus ihren eigenen Konsequenzen ableiten:

| Spalte | Brand | Nicht-Brand | Warum ausgeschlossen |
|---|---|---|---|
| `schaetzung_sachschaden_usd` | 9.962 $ | 28 $ | Schadenshöhe wird nach Löschung geschätzt |
| `loeschfahrzeuge` | 2,86 | 2,38 | Disposition richtet sich nach der gemeldeten Lage |
| `loeschkraefte` | 10,56 | 8,79 | dito |
| `alarmstufe` | 1,00 | 1,00 | Alarmstufe wird im Einsatz hochgestuft |
| `antwortzeit_min` | 4,55 | 4,92 | Ergebnis des Einsatzes, nicht Voraussetzung |
| `zivile_verletzte`, `zivile_tote`, `flammenausbreitung_eingedaemmt` | – | – | Einsatzfolgen |

Der Ausschluss ist methodisch zwingend, kostet aber praktisch nichts: Die
Korrelationen mit `ist_brand` liegen nur zwischen 0,04 und 0,09.
**Diese Tabelle gehört ins Methodenkapitel** – sie zeigt, dass der Ausschluss
bewusst und begründet erfolgt ist.

### 2.2 Merkmalssatz des Hauptmodells

**Block A – Stadtteilstruktur** (identisch mit `PRAEDIKTOREN` der Regression,
Stand Stadtteil × Monat des jeweiligen Einsatzes):
`median_haushaltseinkommen`, `armutsquote_pct`, `akademikerquote_pct`,
`median_miete`, `leerstandsquote_pct`, `log_bevoelkerung`,
`log_kriminalitaetsindex`, `anteil_altbau_vor_1940_pct`,
`anteil_wohngebaeude_pct`, `anteil_risikogewerbe_pct`

**Block B – Zeitpunkt des Alarms** (echte Variation je Einsatz):
`stunde` zyklisch als `stunde_sin`/`stunde_cos`, `monat` zyklisch als
`monat_sin`/`monat_cos`, `wochentag` als One-Hot, `ist_nacht`, `ist_wochenende`

Warum zyklisch und nicht als Zahl: Der Brandanteil schwankt über den Tag
zwischen 8,5 % und 20,3 %, die **lineare** Korrelation mit `stunde` beträgt
aber nur −0,006. Der Zusammenhang ist zyklisch, nicht monoton – Stunde 23 und
Stunde 0 liegen benachbart, als Zahl aber maximal weit auseinander.

**Nicht enthalten:** `bataillon` (11 Kategorien). Begründung analog zur
Entscheidung gegen die Stadtteil-ID in der Regression: Das Hauptmodell soll
zeigen, ob die **inhaltlichen** Merkmale die Einsatzart erklären, nicht ob
„Bataillon 3 viele Brände hat". → Robustheitslauf, s. Abschnitt 6.

### 2.3 Encoding – einheitlich für alle drei Verfahren

One-Hot für `wochentag` (und im Robustheitslauf `bataillon`), **auch für
XGBoost**, statt dessen nativen Categorical-Support. Damit sehen alle drei
Verfahren exakt dieselbe Designmatrix und beobachtete Unterschiede sind rein
algorithmisch. Umsetzung im `ColumnTransformer` **innerhalb** der
Modell-Pipeline, gefittet nur auf dem jeweiligen Trainingsfold.

---

## 3. Das Pseudo-Signal-Problem

**Befund:** 350.481 Einsätze verteilen sich auf nur **4.619 verschiedene
Stadtteil-Monats-Profile**. Im Mittel teilen sich 76 Einsätze (Median 53,
Maximum 451) dieselbe Ausprägung der Merkmale aus Block A.

**Konsequenz:** Die effektive Stichprobengröße für den Strukturblock ist um
Größenordnungen kleiner als die Zeilenzahl suggeriert. Standardfehler,
Signifikanzaussagen und Feature Importance wirken dadurch belastbarer, als sie
sind. Für Block B (Zeitmerkmale) gilt das nicht – die variieren je Einsatz.

**Umgang:**

1. **Benennen.** Ein Absatz im Methodenkapitel mit genau diesen Zahlen. Das ist
   der wichtigste Punkt – ein bekanntes und dokumentiertes Problem ist etwas
   anderes als ein übersehenes.
2. **Keine Signifikanztests** auf Einsatz-Ebene für Strukturmerkmale.
3. **SHAP nach Merkmalsblöcken aggregieren** (Struktur vs. Zeitpunkt), nicht
   einzelne Strukturmerkmale gegeneinander ranken.
4. **Diagnose-Vergleich:** zusätzlich ein Modell nur mit Block B rechnen. Die
   Differenz der AUROC zeigt, was der Strukturblock über die Zeitmerkmale
   hinaus tatsächlich beiträgt. Kostet einen Durchlauf, ist aber die ehrlichste
   Antwort auf Unterfrage 1 im Klassifikationsteil.

**Warum trotzdem nicht gruppiert validiert wird:** Eine Gruppierung nach
Stadtteil (GroupKFold) würde die Frage beantworten „wie gut generalisiert das
Modell auf **unbekannte** Stadtteile?". Deine Forschungsfrage ist aber die
Prognose für **bekannte** Stadtteile in der Zukunft. Der zeitliche Split ist
daher die richtige Wahl; eine Leave-one-neighborhood-out-Variante kann als
Robustheitscheck in den Anhang.

---

## 4. Validierung

**Identisch zur Regression, aus `modellierung/cv.py`:**

- Blockiertes Forward Chaining über globale Monatsgrenzen, 3 Folds à 12
  Testmonate
- End-Hold-out 2025-01 bis 2025-12, beim Tuning unberührt
- Inneres Validierungsfenster = letzte 12 Trainingsmonate, für
  Hyperparameter-Suche **und Schwellenwahl**

**Kein zufälliges Mischen, keine Stratifizierung über die Zeit hinweg.** Beides
würde Einsätze aus der Zukunft ins Training tragen.

### Basisratenverschiebung

| Fold | Training | Basisrate | inneres Val | Test | Basisrate Test |
|---|---|---|---|---|---|
| 1 | 2015-01–2021-12 | 12,0 % | 14,9 % | 2022 | 17,3 % |
| 2 | 2015-01–2022-12 | 12,7 % | 17,3 % | 2023 | 17,1 % |
| 3 | 2015-01–2023-12 | 13,3 % | 17,1 % | 2024 | 15,3 % |
| Hold-out | – | – | – | 2025 | 13,9 % |

Ursache ist ein realer Anstieg: Die absolute Zahl der Brände stieg von
Index 98 (2019) auf 185 (2023) und fiel auf 151 (2025), während die
Nicht-Brand-Einsätze nahezu konstant blieben. Es handelt sich also nicht um
einen Nenner-Effekt oder eine Codierungsänderung.

**Umgang (Decision Log #20):**

- Schwellenwert **je Fold auf dem inneren Fenster** wählen (14,9 % statt 12,0 %
  – näher am Testniveau), nicht blind 0,5.
- **AUROC als primäres Maß** berichten, weil schwellenunabhängig und damit von
  der Verschiebung unberührt; F1 zusätzlich, mit Hinweis auf die Kalibrierung.
- Den Anstieg in Kapitel 6 als **Befund** diskutieren, nicht als Störgröße.

---

## 5. Modelle und Tuning

| Rolle | Verfahren | Begründung |
|---|---|---|
| Lineares Verfahren | `LogisticRegression(penalty="l2", class_weight="balanced")` | Das Klassifikations-Pendant zu Ridge: identische L2-Regularisierung. `RidgeClassifier` hat kein `predict_proba` und wäre für AUROC unbrauchbar. **Als methodische Präzisierung ins Decision Log.** |
| Bagging | `RandomForestClassifier(class_weight="balanced_subsample", n_jobs=-1)` | – |
| Boosting | `XGBClassifier(scale_pos_weight=6.4, tree_method="hist")` | 6,4 = Verhältnis Nicht-Brand zu Brand |
| Baseline | Mehrheitsklasse + Basisrate-Zufallsmodell | Ohne Baseline ist ein AUROC-Wert nicht einzuordnen |

**Tuning:** Randomized Search, 50 Iterationen je Modell, gleiches Budget,
`random_state=42`, Bewertung ausschließlich auf dem inneren Fenster.
Bei 350.000 Zeilen ist für die Suche eine stratifizierte Teilstichprobe des
**Trainingsfensters** (z. B. 100.000 Zeilen) zulässig – das finale Modell wird
auf dem vollen Trainingsfenster gefittet. Die Teilstichprobe darf niemals das
Validierungs- oder Testfenster berühren.

**Kein Early Stopping auf Testdaten.** Falls Early Stopping bei XGBoost
verwendet wird, dann nur gegen das innere Fenster.

---

## 6. Läufe

| # | Merkmale | Zweck |
|---|---|---|
| 1 | Block A + B | **Hauptmodell**, Verfahrensvergleich |
| 2 | nur Block B (Zeit) | Wie viel trägt der Strukturblock wirklich bei? |
| 3 | Block A + B + `bataillon` | Robustheit: Wie viel trägt reine Ortsidentität bei? |

Alle drei Verfahren durchlaufen alle drei Merkmalssätze mit identischen Folds.

---

## 7. Erwartungshaltung

Der Merkmalssatz enthält **keine Information über den konkreten Vorfall** –
nur darüber, wo und wann er gemeldet wurde. Ein AUROC im Bereich 0,60–0,70 ist
das realistische Ergebnis, kein Misserfolg. Die Aussage der Arbeit lautet dann:
*Strukturelle und zeitliche Kontextmerkmale erlauben eine begrenzte, aber
systematisch besser-als-zufällige Differenzierung der Einsatzart; die
entscheidende Information liegt in der Meldung selbst, die in den offenen Daten
nicht enthalten ist.* Das ist ein sauberes, verteidigungsfähiges Ergebnis.

**Nicht** akzeptabel wäre ein AUROC über 0,90 – das wäre ein sicheres Zeichen
dafür, dass eine Ergebnisvariable aus Abschnitt 2.1 ins Modell gerutscht ist.
Dieser Plausibilitätscheck gehört fest in den Ablauf.

---

## 8. Umsetzungsreihenfolge

1. `modellierung/klassifikation_daten.py` – Zielgröße, Merkmalsblöcke,
   Zeitschlüssel, Selbsttests (keine Ergebnisvariablen enthalten, Zeitraum und
   Stadtteile identisch zur Regression, Klassenverhältnis wie erwartet)
2. `modellierung/train_klassifikation.py` – ColumnTransformer, drei Verfahren,
   Tuning auf innerem Fenster, Schwellenwahl, Metriken aus `cv.py`
3. Ergebnis-CSV nach `results/klassifikation/`
4. SHAP auf dem finalen Modell (TreeExplainer auf Margin-Output;
   für LogReg Koeffizienten × Standardabweichung als Quervalidierung)
