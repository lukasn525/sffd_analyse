# Risiken der Modellierung

> **Lebensdauer:** ändert sich, wenn ein Risiko eintritt, entschärft wird oder
> wegfällt. Stand **04.08.2026**, nach `prep/build.py` und `vorpruefung/run.py`.
> Ersetzt den Risikobericht vom 28.07.2026 (`Risiken_Modellierung.pdf`), dessen
> Zahlen aus der Zeit vor dem Umbau stammen.

Die Data Preparation ist abgeschlossen: ein Befehl, zwei Datensätze, 19/19
Prüfungen, Baselines festgelegt. Die folgenden Risiken betreffen ausschließlich
die Modellierung. Es sind **keine Fehler in den Daten**, sondern Eigenschaften
der Fragestellung, die die Aussagekraft begrenzen.

## Register

| | Risiko | Beleg | Stufe |
|---|---|---|---|
| **R-1** | **Die Verfahren sind möglicherweise nicht unterscheidbar.** Das ist die zentrale Forschungsfrage. Im Vortest lag die gepaarte Differenz Ridge gegen Random Forest bei +0,042 ± 0,609; Ridge gewann 12 von 20 Folds — Münzwurf. Die Ursache (R-7) besteht unverändert fort. | Vortest 28.07., **neu zu rechnen** | hoch |
| **R-2** | **Im Klassifikationsstrang schlägt ein lineares Modell den Baum.** Die ungetunte Sonde ergibt Macro-F1 0,290 für die logistische Regression gegen 0,270 für einen Entscheidungsbaum der Tiefe 3, bei 0,223 der Mehrheitsklasse. Der Baum unterbietet die Mehrheitsklasse sogar in zwei von fünf Folds. Der Mehraufwand von RF und XGBoost ist damit im zweiten Strang **nicht vorab begründet**. | `m01`, Abschnitt 5b | hoch |
| **R-3** | **33,7 % der Testzeilen liegen außerhalb des Trainingsbereichs**, Spanne 3,6 bis 57,4 %. Unvermeidliche Folge des Stadtteil-Splits. Ridge rechnet dort linear weiter, Baumverfahren geben den Randwert des letzten Blatts — die Verfahren werden ungleich getroffen. | `m01`, Abschnitt 4 | hoch |
| **R-4** | **Das Hold-out umfasst 6 Stadtteile.** Die abschließende, einmalige Bewertung beruht auf sechs Einheiten; ihre Unsicherheit muss mitberichtet werden. | 6 von 35, 792 Zeilen | mittel |
| **R-5** | **Effektive Stichprobe: 35 Einheiten.** 4.620 Zeilen klingen komfortabel, es sind 35 Querschnittseinheiten × 132 Monate, davon 29 in der Entwicklung. Gemeinsame Ursache von R-1, R-3 und R-4. | `03_STAND.md` | mittel |
| **R-6** | **Merkmale sind innerhalb eines Jahres konstant.** ACS erscheint jährlich, Land Use ist ein Snapshot 2020. Das Modell sagt für alle zwölf Monate eines Stadtteils fast denselben Wert vorher; die Monatsschwankung geht vollständig ins Residuum. | 5 Jahrgänge auf 132 Monate | gering |
| **R-7** | **Abweichungen vom Exposé sind unbesprochen** (siehe unten). | #29, #31, #32 | gering |
| **R-8** | **ACS-Trefferquote 2009 nur 63,1 %.** Für die Hauptanalyse ab 2015 folgenlos, gehört in die Limitationen. | 2021/23: 99,2 % | gering |

**Entschärft und aus dem Register genommen:**

- *Brand in einzelnen Folds mit 1–2 Fällen* — durch die doppelte Stratifizierung (#30) auf 13/9/6/3/2 Testfälle je Fold gehoben.
- *Zwei Anteile nicht vorhersagbar* — hinfällig: Die `anteil_*`-Spalten sind seit #31 keine Modellzielgröße mehr, sondern Rechenbasis und Deskription.

---

## R-2 im Detail — der neue Befund

| Verfahren | Macro-F1 je Fold | Mittel |
|---|---|---|
| Mehrheitsklasse | 0,224 · 0,222 · 0,237 · 0,213 · 0,218 | 0,223 |
| Logistische Regression | 0,322 · 0,320 · 0,252 · 0,320 · 0,235 | **0,290** |
| Entscheidungsbaum, Tiefe 3 | 0,353 · 0,239 · 0,197 · 0,347 · 0,214 | 0,270 |

**Was der Befund sagt:** Signal ist vorhanden — beide Verfahren schlagen die
Mehrheitsklasse im Mittel, und der Kruskal-Wallis-Test weist 9 von 10 Merkmalen
als klassentrennend aus. Die Klassengrenze lässt sich aber offenbar gut linear
beschreiben; ein flacher Baum bringt keinen Vorteil und ist zudem instabil
(0,197 bis 0,353).

**Was der Befund NICHT sagt:** Dass Random Forest und XGBoost ungeeignet wären.
Ein einzelner Baum der Tiefe 3 ist ein bewusst schwacher Lerner. Seine
Instabilität ist genau das Problem, für dessen Behebung Bagging erfunden wurde —
insofern ist der Befund eher ein Argument *für* das Ensemble als gegen
Baumverfahren. Belegt ist er damit aber nicht.

**Konsequenz:** Im Regressionsstrang ist der Schritt über das lineare Modell
hinaus vorab begründet (RESET-Test), im Klassifikationsstrang **nicht**. Dort
entscheidet erst `m03`. Das gehört so in Kapitel 6.2 — und wenn RF und XGBoost
die logistische Regression am Ende nicht schlagen, ist das ein berichtbares
Ergebnis, kein Makel (Gutachten R6).

**Offene Frage an Decision Log #31:** Dort wurde die logistische Regression aus
Fokusgründen gestrichen, ausdrücklich „nicht mangels Eignung". Der Vortest sagt
jetzt, dass sie das beste der drei geprüften Verfahren ist. Das ist kein
Widerspruch zur Entscheidung, aber es erhöht die Begründungslast: Wer das beste
Verfahren weglässt, muss das benennen. Zwei Wege — sie als dokumentierten
Referenzlauf mitführen, oder die Streichung in 6.2 offen und mit dieser Zahl
begründen. Nicht möglich ist, es unerwähnt zu lassen.

---

## R-7 im Detail — was mit Schröter unbesprochen ist

Vier Festlegungen weichen vom Exposé ab oder gehen darüber hinaus. Alle sind im
Decision Log begründet, keine ist abgestimmt.

| Punkt | Was entschieden wurde | Warum es abweicht |
|---|---|---|
| **Stadtteil-Split** statt Zeitschnitt | 5 Folds, 6 Hold-out-Stadtteile | Unterfrage 2 nennt zeitreihengerechte Kreuzvalidierung. Die entfällt als Hauptvergleich, weil sie die Forschungsfrage nicht prüft: Bei einem Zeitschnitt steht jeder Stadtteil in Training *und* Test (#29) |
| **Einsatzart auf Stadtteilebene** | dominante Einsatzart je Stadtteil-Monat | Auf Einzeleinsatz-Ebene lag die Obergrenze bei 49,9 % gegenüber 48,2 % für bloßes Raten. Die Klasse bleibt echt, nur die Analyseeinheit wechselt (#29) |
| **Rate als zweite Zielgröße** | Einsätze je 1.000 Einwohner | Erweitert das Exposé, widerspricht ihm nicht (#29) |
| **Drei Verfahren für Regression, zwei für Klassifikation** | Ridge/RF/XGB gegen RF/XGB | Per E-Mail vom 03.08.2026 freigegeben, Begründung in 6.2 verlangt (#31) |

**Einstufung als gering** — mit einer Einschränkung, die ich einmal nennen
möchte: Die Eintrittswahrscheinlichkeit ist niedrig, die Schadenshöhe aber
maximal. Sollte Schröter beim Stadtteil-Split anderer Meinung sein, ist jede
Modellrechnung davor verloren, weil der Split als Spalte in den Datensätzen
steht. Der Punkt kostet in der Sprechstunde fünf Minuten. Das ist der Grund,
warum er trotz niedriger Stufe im Register bleibt.

---

## Was daraus für die Modellierung folgt

1. **R-1 vorab entscheiden.** Was passiert, wenn die Verfahren nicht
   unterscheidbar sind? Die Antwort muss *vor* dem Rechnen feststehen, sonst
   sucht man hinterher die Auswertung, die einen Unterschied zeigt.
   Vorgesehen: gepaarter Wilcoxon-Test je Zielgröße; wird er nicht signifikant,
   steht genau das als Ergebnis in Kapitel 7.
2. **10 Wiederholungen**, nicht 5 Folds allein — verengt das Konfidenzintervall
   des Mittelwerts, auch wenn die Einzelstreuung bleibt.
3. **Je Zielgröße getrennt entscheiden.** Die Verfahren tauschen zwischen
   `anzahl_einsaetze` und `einsaetze_je_1000_ew` die Plätze.
4. **R-3 dokumentieren, nicht wegrechnen.** Vorhersagen zu kappen wäre ein
   Eingriff, der den Verfahrensvergleich verwässert. Dass Baumverfahren bei
   unbekannten Stadtteilen strukturell im Nachteil sind, ist selbst ein
   Vergleichsergebnis.
