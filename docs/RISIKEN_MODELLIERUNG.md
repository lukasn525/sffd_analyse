# Risikoanalyse vor der Modellierung

Stand 2026-07-26, nach Abschluss der Data Preparation. Grundlage: Exposé
(05.06.2026), Schröter-Vorgaben, Prep-Pipeline, `results/eignungspruefung/`,
`results/demo_modellierung/`.

**Kurzantwort:** Die Daten sind für den festgelegten Zeitraum und die
festgelegten Kriterien sauber. Kein bekanntes Leakage, reproduzierbarer
Zeitraum, rechteckiges Panel, alle modellbegründenden Diagnosen nur auf
Trainingsdaten. Was jetzt noch zu Fehlern führen kann, liegt **nicht mehr in den
Daten**, sondern in der Modellierung und in der Argumentation. Die folgenden
Punkte sind nach Schwere sortiert.

---

## R1 – Das Exposé begründet die Aggregation mit dem Pseudo-Signal-Problem (HOCH)

**Der Konflikt.** Das Exposé schreibt auf S. 5 wörtlich:

> „Da die Feature-Werte auf Stadtteilebene und jährlicher Erhebung vorliegen,
> würden sich diese Werte bei einer Auswertung auf Einzeleinsatz-Ebene mehrfach
> wiederholen und potenziell zu **Pseudo-Signalen** führen. Die Daten werden
> daher zeitlich auf Stadtteil × Monat aggregiert."

Zugleich nennt es die Einsatzart als zweite Zielgröße mit F1 und AUROC – also
eine Klassifikation, die im aktuellen Design auf **Einzeleinsatz-Ebene** läuft.
Damit reproduziert der Klassifikationsteil genau das Problem, dessen Vermeidung
das Exposé als Begründung für die Aggregation anführt.

Die Zahlen dazu: 350.481 Einsätze verteilen sich auf **4.619 verschiedene
Stadtteil-Monats-Profile**. Im Mittel teilen sich 76 Einsätze dieselbe
Merkmalsausprägung.

**Warum das gefährlich ist.** Das ist kein technischer, sondern ein
argumentativer Widerspruch – und er steht schwarz auf weiß im eingereichten
Exposé. Ein aufmerksamer Zweitprüfer findet ihn.

**Handlungsoptionen:**

| Option | Beschreibung | Bewertung |
|---|---|---|
| **A** | Einzeleinsatz beibehalten, Widerspruch offen benennen und auflösen: Die Aggregation schützt die **Regression**; für die Klassifikation ist die Einsatz-Ebene sachlich zwingend, weil die Einsatzart eine Eigenschaft des Einzelereignisses ist. Zusätzlich der Diagnose-Lauf „nur Zeitmerkmale" als ehrliche Messung des Strukturbeitrags | **Empfohlen.** Exposé-treu bei den Gütemaßen, methodisch verteidigbar, geringer Aufwand. Setzt voraus, dass der Absatz wirklich geschrieben wird |
| **B** | Klassifikation auf Stadtteil × Monat verlagern, z. B. binäre Zielgröße „Brandanteil über dem Median" | Löst den Widerspruch vollständig und erhält F1/AUROC. Ändert aber, was „Vorhersage der Einsatzart" bedeutet – näher an einer Risikoklassifikation von Stadtteil-Monaten |
| **C** | Klassifikation als Anteilsregression | Verliert F1/AUROC → deutliche Exposé-Abweichung. Nicht empfohlen |

**In jedem Fall mit Schröter besprechen.** Das ist der Punkt, bei dem eine
Rückfrage vor der Umsetzung am meisten wert ist.

---

## R2 – Naive Baseline und Lag-Features dominieren den Verfahrensvergleich (HOCH)

**Befund.** Die naive Vormonats-Baseline erreicht R² 0,95. Ridge (S+L) 0,96,
RF (S+L) 0,95. Die Unterschiede zwischen den Verfahren liegen im Bereich weniger
RMSE-Punkte – und Ridge liegt derzeit **vorn**, obwohl das Exposé (S. 14)
erwartet, dass baumbasierte Verfahren am besten abschneiden.

**Warum das ein Risiko ist.** Wenn alle drei Verfahren praktisch gleichauf
liegen, wird der Verfahrensvergleich – der Kern der Forschungsfrage –
uninformativ. Ein Ergebnis „alle drei sind etwa gleich gut und kaum besser als
der Vormonatswert" beantwortet die Frage formal, trägt aber keine Arbeit.

**Gegenmaßnahmen:**

1. **Beide Feature-Sets konsequent berichten.** In Set S (reine Struktur)
   differenzieren die Verfahren sichtbar: Ridge 0,79 vs. RF 0,89. Das ist der
   informative Teil des Vergleichs.
2. **Standardabweichung über die Folds mitführen** und prüfen, ob Unterschiede
   überhaupt größer sind als die Fold-Streuung. Derzeit: Ridge (S+L)
   15,0 ± 0,9 vs. RF (S+L) 16,8 ± 0,9 – die Intervalle überlappen. Das ehrlich
   zu sagen ist besser, als einen Sieger zu behaupten.
3. **Trainings- und Inferenzzeiten** als eigenständiges Vergleichskriterium
   ernst nehmen (Unterfrage 3). Wenn die Güte gleich ist, entscheidet der
   Aufwand – Ridge braucht 0,02 s, RF 2–3 s. Das ist ein publikationsfähiges
   Ergebnis.
4. **Die abweichende Erwartung offen diskutieren.** Dass ein lineares Modell
   Bäume schlägt, ist bei einem stark autoregressiven, glatten Signal genau das,
   was Grinsztajn et al. erwarten lassen würden – Bäume glänzen bei
   Nichtlinearität und Interaktionen, nicht bei log-linearer Persistenz.

---

## R3 – Drei bauliche Merkmale haben keine Zeitvarianz (MITTEL)

`anteil_altbau_vor_1940_pct`, `anteil_wohngebaeude_pct` und
`anteil_risikogewerbe_pct` stammen aus dem Land-Use-Snapshot 2020 und sind über
alle 132 Monate konstant. Sie können Niveauunterschiede **zwischen** Stadtteilen
erklären, nicht deren Entwicklung **über die Zeit**.

**Risiko:** Formulierungen wie „bauliche Merkmale sagen die Einsatzentwicklung
vorher" sind nicht haltbar. Zudem ist `anteil_risikogewerbe_pct` mit r = +0,69
der stärkste Einzelprädiktor – ein rein querschnittliches Merkmal trägt damit
den größten Teil des Struktur-Signals.

**Gegenmaßnahme:** Konsequent „beschreiben Niveauunterschiede" statt „sagen
vorher" formulieren; die Tabelle zur Zeitvarianz aus der Eignungsprüfung in
Kap. 5.1 aufnehmen; Limitation in Kap. 6.3.

---

## R4 – Vier ACS-Stützstellen für elf Jahre (MITTEL)

Die sozioökonomischen Merkmale beruhen auf vier nutzbaren ACS-Jahrgängen
(2014, 2019, 2021, 2023) und sind zwischen den Stützstellen konstant. Über
132 Monate ergeben sich nur vier verschiedene Werte je Stadtteil, mit Sprüngen
an den Übergängen.

**Risiko:** SHAP und Feature Importance können diese Sprünge als Signal
aufgreifen. Bei einem zeitlichen Split kann ein Modell außerdem eine
Snapshot-Grenze indirekt als Zeitmarker verwenden.

**Gegenmaßnahme:** Diagnose vorsehen – Verläuft die Modellgüte an den
Snapshot-Grenzen (2019, 2021, 2023) auffällig? Falls ja, dokumentieren. Die
5-Jahres-Schätzungen sind ohnehin gleitende Mittel; ihre Trägheit ist eine
Eigenschaft der Quelle, keine Schwäche der Aufbereitung.

---

## R5 – Retransformationsverzerrung bei Ridge (MITTEL)

Ridge wird auf log(1+y) geschätzt. `expm1` liefert den Median, nicht den
Erwartungswert der Rückskala (Jensen-Ungleichung) – RMSE und MAE werden dadurch
systematisch leicht überschätzt.

**Gegenmaßnahme:** Gütemaße immer auf der Originalskala berechnen (umgesetzt);
Duan-Smearing als Robustheitscheck in einer Fußnote. Wichtig, weil genau dieses
Modell derzeit gewinnt – der Vorsprung darf kein Transformationsartefakt sein.

---

## R6 – NegBin-Konvergenz und Offset (MITTEL)

`statsmodels` kann bei der Negativen Binomialregression Konvergenzprobleme
haben. Zusätzlich muss `log_bevoelkerung` als **Offset** (Koeffizient auf 1
fixiert) übergeben werden, nicht als gewöhnlicher Regressor – sonst ist die
Exposure-Kontrolle nur halb umgesetzt.

**Gegenmaßnahme:** Reihenfolge GLM-NB mit geschätztem Alpha → `NegativeBinomial`
MLE → Poisson-QMLE mit robusten Standardfehlern; jeder Fallback wird
dokumentiert. Offset explizit über `offset=`, nicht über `exog`.

---

## R7 – Tuning-Budget und Laufzeit der Klassifikation (MITTEL)

350.481 Zeilen × 3 Folds × 50 Iterationen × 3 Verfahren ist ohne Subsampling
nicht in vertretbarer Zeit zu rechnen.

**Gegenmaßnahme:** Stratifizierte Teilstichprobe des **Trainingsfensters** für
die Suche (z. B. 100.000 Zeilen), finales Fit auf dem vollen Trainingsfenster.
Die Stichprobe darf Validierungs- und Testfenster nie berühren. Subsampling im
Methodenkapitel dokumentieren – es ist ein Eingriff, kein Detail.

---

## R8 – Plausibilitätsgrenze der Klassifikationsgüte (MITTEL)

Der Merkmalssatz enthält keine Information über den konkreten Vorfall, nur über
Ort und Zeitpunkt. Realistisch ist **Macro-AUROC 0,60–0,70**.

**Ein Macro-AUROC über 0,90 ist ein Alarmsignal**, kein Erfolg – dann ist mit
hoher Wahrscheinlichkeit eine Ergebnisvariable (Sachschaden, Löschfahrzeuge,
Alarmstufe, Antwortzeit) in den Merkmalssatz gerutscht.

**Abgesichert:** `modellierung/klassifikation_daten.py` enthält die Liste
`ERGEBNISVARIABLEN` und prüft bei jedem Lauf per `assert`, dass keine dieser
Spalten im Datensatz landet. Der Test wurde per Gegenprobe verifiziert (er
schlägt an, wenn `alarmstufe` künstlich eingefügt wird). Damit kann Leakage
dieser Art nicht mehr unbemerkt bis in die Ergebnisse durchrutschen.

---

## R9 – Titel- und Gliederungsinkonsistenz (NIEDRIG, aber formal relevant)

Es kursieren zwei Titelvarianten:

- Exposé-Deckblatt: *„Vorhersage von Feuerwehreinsätzen mittels Machine
  Learning, ein Verfahrensvergleich am Beispiel der Stadtteile San Franciscos"*
- Exposé, Abschnitt 6 „Arbeitstitel": *„… auf Basis sozioökonomischer,
  kriminalitätsbezogener und baulicher Merkmale – ein Vergleich von Ridge
  Regression, Random Forest und XGBoost am Beispiel der Stadtteile von San
  Francisco"*

Vor der Abgabe auf **eine** Fassung festlegen und mit der Anmeldung beim
Prüfungsamt abgleichen.

---

## R10 – Abweichungen vom Exposé, die in die Arbeit müssen (NIEDRIG)

Alle sachlich begründet, aber keine darf unerwähnt bleiben:

| Exposé sagt | Ist-Stand | Begründung |
|---|---|---|
| „auf 41 Stadtteile aggregiert" (S. 9) | **35 Stadtteile** | 3 ohne durchgängige ACS-Abdeckung, 3 Park-/Institutionsgebiete ohne Wohnbevölkerung (#15, #19) |
| Kriminalitätskennzahlen allgemein | relativer Index je Stadtteil × Monat | frühere Anteile waren statisch und enthielten Zukunftsinformation (#17) |
| kein Zeitraum genannt | 2015-01 – 2025-12 | ACS-Publikationsversatz + letztes vollständiges Kalenderjahr (#11, #18) |
| „Poisson- bzw. Negative-Binomial" | **NegBin**, nicht Poisson | Dispersionsindex 62,8 → Poisson-Annahme klar verletzt |
| Ridge Regression | Ridge auf log(1+y) | Linearitätsprüfung: 7,8 % negative Vorhersagen auf Rohskala (#2) |
| Klassifikation „kategorial" | binär Brand vs. Nicht-Brand | im Exposé S. 5 ausdrücklich vorgesehen (#6) |
| baumbasierte Verfahren erwartet stärker | Ridge derzeit vorn | als Befund diskutieren, nicht kaschieren |

---

## Was ausdrücklich **kein** Risiko mehr ist

| Früherer Punkt | Status |
|---|---|
| ACS-Leakage (Zukunfts-Snapshot) | behoben (#4, #11) – letzter *publizierter* Jahrgang |
| Kriminalitäts-Leakage (über Testzeitraum kumuliert) | behoben (#17) – rollierendes Fenster endend im Vormonat |
| `bfill`-Imputation mit Zukunftswerten | behoben (#10) |
| Phantom-Monat 2026-01 im letzten Fold | behoben (#12) – Folds stabil bei R² 0,95/0,95/0,96 |
| Unbalanciertes Panel durch Stadtteil-Zutritt | behoben (#15) – rechteckig, 35 × 132 |
| Größeneffekt (Modell sagt Stadtteilgröße vorher) | behoben (#13) – `log_bevoelkerung` als Exposure |
| Extremwerte durch Nenner nahe null | behoben (#19) – Parkgebiete ausgeschlossen |
| Kein unberührtes Hold-out | behoben (#14) – 2025 beim Tuning nie angefasst |
| Diagnosen auf dem Gesamtdatensatz | behoben – VIF und Linearität nur auf Fold-1-Training |
| Scaler vor dem Split gefittet | war nie der Fall – `StandardScaler` sitzt in der Fold-Pipeline |

---

## Empfohlene Reihenfolge

1. **R1 mit Schröter klären** – bestimmt den halben Klassifikationsteil.
2. `klassifikation_daten.py` bauen (Selbsttests: keine Ergebnisvariablen,
   Abgrenzung identisch zur Regression, Klassenverhältnis 1:6,4).
3. `features.py` und `baselines.py` aus der Demo herauslösen.
4. `train_regression.py` mit Tuning; dabei R5 und R6 beachten.
5. `train_klassifikation.py`; dabei R7 und R8 beachten.
6. SHAP, dann Ergebniskonsolidierung.
7. Erst ganz am Ende: **einmalige** Auswertung auf dem End-Hold-out 2025.
