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
| **R-1** | **Die Verfahren sind bei `anzahl_einsaetze` möglicherweise nicht unterscheidbar.** Dort lag die gepaarte Differenz Ridge gegen Random Forest im Vortest bei +0,042 ± 0,609, Ridge gewann 12 von 20 Folds — Münzwurf. **Bei `einsaetze_je_1000_ew` sieht es umgekehrt aus:** RF 0,584 ± 0,19 gegen Ridge −0,087 ± 0,89, ein Abstand von 0,67. Das Risiko betrifft also **eine** Zielgröße, nicht den Vergleich als solchen. **Entschärft durch #34** (drei Bausteine statt Rangfolge). | Vortest 28.07., **neu zu rechnen** | mittel |
| **R-2** | **Der Klassifikationsstrang trägt weniger, als die Forschungsfrage verspricht.** Selbst die beste Sonde erreicht Macro-F1 0,290 gegen 0,223 der Mehrheitsklasse — bei einem Maximum von 1,0 wird also nur ein Bruchteil des vorhandenen Signals ausgeschöpft. Die Verfahrenswahl ist begründet (siehe unten), der Ertrag bleibt aber gering. | `v2_eignung`, Abschnitt 5 | mittel |
| **R-3** | **33,7 % der Testzeilen liegen außerhalb des Trainingsbereichs**, Spanne 3,6 bis 57,4 %. Unvermeidliche Folge des Stadtteil-Splits. Ridge rechnet dort linear weiter, Baumverfahren geben den Randwert des letzten Blatts — die Verfahren werden ungleich getroffen. | `m01`, Abschnitt 4 | hoch |
| **R-4** | **Das Hold-out umfasst 6 Stadtteile.** Die abschließende, einmalige Bewertung beruht auf sechs Einheiten; ihre Unsicherheit muss mitberichtet werden. | 6 von 35, 792 Zeilen | mittel |
| **R-5** | **Effektive Stichprobe: 35 Einheiten.** 4.620 Zeilen klingen komfortabel, es sind 35 Querschnittseinheiten × 132 Monate, davon 29 in der Entwicklung. Gemeinsame Ursache von R-1, R-3 und R-4. **Zwei Folgen für die Auswertung:** Die Gütemaße werden je Zeile gerechnet, aber die 132 Zeilen eines Stadtteils sind hochkorreliert — der effektive Stichprobenumfang der Metrik ist weit kleiner als *n*. Und die 50 Fold-Ergebnisse aus 10 Wiederholungen sind **nicht unabhängig**: Es sind dieselben 29 Stadtteile, nur anders gruppiert. Der Gewinn an Präzision ist kleiner als √10 (Nadeau & Bengio 2003). | `03_STAND.md` | mittel |
| **R-6** | **Merkmale sind innerhalb eines Jahres konstant.** ACS erscheint jährlich, Land Use ist ein Snapshot 2020. Das Modell sagt für alle zwölf Monate eines Stadtteils fast denselben Wert vorher; die Monatsschwankung geht vollständig ins Residuum. | 5 Jahrgänge auf 132 Monate | gering |
| ~~R-7~~ | ~~**Der Stadtteil-Split ist mit Schröter unbesprochen.**~~ **✅ ERLEDIGT am 04.08.2026.** Schröter per E-Mail: „Der Stadtteil-Split ist für die von Ihnen formulierte Forschungsfrage methodisch gut begründet. […] Insofern können Sie wie geplant vorgehen." Auch die beiden Baselines sind ausdrücklich bestätigt. Zwei Auflagen daraus — siehe unten. | E-Mail 04.08.2026 | entfallen |
| **R-8** | **ACS-Trefferquote 2009 nur 63,1 %.** Für die Hauptanalyse ab 2015 folgenlos, gehört in die Limitationen. | 2021/23: 99,2 % | gering |
| **R-9** | **Spezifikationsasymmetrie zwischen Baseline und Vergleichsverfahren.** Die Negative Binomial erhält `log(Bevölkerung)` als **Offset** — sie modelliert damit direkt Einsätze je Einwohner. Ridge, RF und XGBoost bekommen dieselbe Größe nur als gewöhnliches Merkmal und müssen den Zusammenhang erst lernen. Die Baseline hat damit einen strukturellen Vorteil; wer ihn nicht benennt, vergleicht nicht Verfahren, sondern Spezifikationen. | `v1_baselines.py`, `negative_binomial()` | mittel |
| **R-10** | **Mehrfachvergleiche ohne Korrektur.** Der gepaarte Wilcoxon-Test läuft je Zielgröße und Verfahrenspaar: 3 Paare × 2 Mengen-Zielgrößen plus 1 Paar in der Klassifikation = **7 Tests**. Bei α = 0,05 liegt die Wahrscheinlichkeit für mindestens einen falsch-positiven Befund bei rund 30 %. | #34 | mittel |

**Entschärft und aus dem Register genommen:**

- *Brand in einzelnen Folds mit 1–2 Fällen* — durch die doppelte Stratifizierung (#30) auf 13/9/6/3/2 Testfälle je Fold gehoben.
- *Zwei Anteile nicht vorhersagbar* — hinfällig: Die `anteil_*`-Spalten sind seit #31 keine Modellzielgröße mehr, sondern Rechenbasis und Deskription.

---

## R-2 im Detail — und der Beleg, der daraus folgt

| Stufe | Verfahren | Macro-F1 je Fold | Mittel |
|---|---|---|---|
| 1 | Mehrheitsklasse | 0,224 · 0,222 · 0,237 · 0,213 · 0,218 | 0,223 |
| 2 | Logistische Regression | 0,322 · 0,320 · 0,252 · 0,320 · 0,235 | **0,290** |

### Die Begründungskette für Kapitel 6.2

**Es ist viel zu holen, und das lineare Modell holt wenig davon.** Die
Kruskal-Wallis-Tests weisen neun der zehn Strukturmerkmale als hochsignifikant
klassentrennend aus, mit Teststatistiken bis H = 481 und p-Werten bis 10⁻¹⁰⁴. In
den Merkmalen steckt also erhebliche Information über die dominante Einsatzart.
Die multinomiale logistische Regression schöpft davon nur einen Bruchteil aus:
Macro-F1 0,290 gegenüber 0,223 der Mehrheitsklasse — ein Zugewinn von 0,067 bei
einem theoretischen Maximum von 1,0.

**Der Grund liegt in der Form der Klassengrenze.** Die Zielgröße entsteht als
Maximum über vier Anteile; die Grenze zwischen zwei Klassen liegt dort, wo die
zugehörigen Anteile einander schneiden. Im Merkmalsraum sind das Schnittflächen,
keine Hyperebenen. Ein Verfahren, das nur lineare Trennebenen ziehen kann, ist
hier konstruktionsbedingt im Nachteil — was die geringe Ausschöpfung erklärt.

**Daraus folgt die Verfahrenswahl.** Verfahren, die flexiblere Grenzen ziehen
können, sind damit begründet — Random Forest über die Kombination vieler Bäume,
XGBoost über sequenzielle Korrektur. Ob sie den Rückstand aufholen, ist die
empirische Frage von Kapitel 7. Holen sie ihn nicht auf, lautet der Befund „der
Mehraufwand lohnt sich hier nicht"; das ist ein berichtbares Ergebnis, kein
Makel (Gutachten R6).

**Was das Risiko bleibt:** nicht die Verfahrenswahl, sondern der **Ertrag**. Auch
das beste bisher gemessene Verfahren schöpft nur einen Bruchteil des Signals aus.
Der Klassifikationsstrang wird voraussichtlich weniger tragen, als die
Forschungsfrage verspricht — das gehört in die Limitationen.

### Offene Frage an Decision Log #31

Dort wurde die logistische Regression aus Fokusgründen gestrichen, ausdrücklich
„nicht mangels Eignung". Seit #33 ist sie **Stufe-2-Baseline** — damit ist die
Frage entschärft: Sie ist nicht mehr das weggelassene beste Verfahren, sondern
die Messlatte, gegen die RF und XGBoost antreten. Ihr Wert von 0,290 wird vom
Problem zum Beweismittel. In 6.2 ist das so zu benennen.

---

## R-7 — erledigt, mit zwei Auflagen

Schröter hat am **04.08.2026** zugestimmt: Stadtteil-Split und beide Baselines
sind freigegeben, „Insofern können Sie wie geplant vorgehen." Daraus folgen zwei
Auflagen, die in die Arbeit müssen:

**Auflage A — die Abweichung transparent erläutern.** Wörtlich: „Wichtig ist,
dass Sie die Abweichung von der ursprünglich angekündigten zeitreihengerechten
Kreuzvalidierung transparent erläutern." Damit ist das begründete Verwerfen in
Kapitel 8 nicht mehr optional, sondern verlangt (#29).

**Auflage B — die Zielsetzung als Generalisierung formulieren.** Wörtlich: „und
die Zielsetzung der Validierung klar als **Generalisierung auf unbekannte
Stadtteile** formulieren." Das ist eine Formulierungsvorgabe. Der Begriff gehört
in Kapitel 5.4 und in die Zielsetzung, nicht nur sinngemäß.

**Auflage C — identische Merkmale und Splits.** Wörtlich: „Achten Sie darauf,
für alle Vergleichsmodelle identische Merkmale und Splits zu verwenden." Das ist
erfüllt, muss aber **gezeigt** werden: Die Fairness ist konstruktiv abgesichert,
weil `fold` und `ist_holdout` als Spalten in den Dateien stehen und die
Merkmalsliste einmal in `prep/config.py` definiert ist. Kein Modellskript kann
davon abweichen. Dieser Mechanismus gehört in Kapitel 5.4 beschrieben — als
Beleg, nicht als Zusicherung.

### Die übrigen Abweichungen — unkritisch

Vier Festlegungen weichen vom Exposé ab oder gehen darüber hinaus. Alle sind im
Decision Log begründet, keine ist abgestimmt.

| Punkt | Was entschieden wurde | Warum es abweicht |
|---|---|---|
| **Stadtteil-Split** statt Zeitschnitt | 5 Folds, 6 Hold-out-Stadtteile | Unterfrage 2 nennt zeitreihengerechte Kreuzvalidierung. Die entfällt als Hauptvergleich, weil sie die Forschungsfrage nicht prüft: Bei einem Zeitschnitt steht jeder Stadtteil in Training *und* Test (#29) |
| **Einsatzart auf Stadtteilebene** | dominante Einsatzart je Stadtteil-Monat | Auf Einzeleinsatz-Ebene lag die Obergrenze bei 49,9 % gegenüber 48,2 % für bloßes Raten. Die Klasse bleibt echt, nur die Analyseeinheit wechselt (#29) |
| **Rate als zweite Zielgröße** | Einsätze je 1.000 Einwohner | Erweitert das Exposé, widerspricht ihm nicht (#29) |
| **Drei Verfahren für Regression, zwei für Klassifikation** | Ridge/RF/XGB gegen RF/XGB | Per E-Mail vom 03.08.2026 freigegeben, Begründung in 6.2 verlangt (#31) |

**Einstufung (Lukas, 04.08.2026): Nur der Stadtteil-Split ist leicht kritisch,
die übrigen drei sind unkritisch.**

Die Einsatzart auf Stadtteilebene, die Rate als zweite Zielgröße und die
3-gegen-2-Verfahrenswahl sind entweder empirisch zwingend, eine Erweiterung ohne
Widerspruch zum Exposé, oder bereits per E-Mail freigegeben. Sie werden in
Kapitel 6 benannt und brauchen keine gesonderte Abstimmung.

Beim **Stadtteil-Split** liegt der Fall anders: Er widerspricht einem wörtlich
im Exposé genannten Element von Unterfrage 2 (zeitreihengerechte
Kreuzvalidierung), und er steht als Spalte in beiden Datensätzen. Ein Veto
danach würde jede Modellrechnung entwerten. Eintrittswahrscheinlichkeit niedrig,
Schadenshöhe maximal — deshalb bleibt der Punkt im Register, obwohl er als
gering eingestuft ist. Er kostet in der Sprechstunde fünf Minuten.

---

## Was daraus für die Modellierung folgt

1. ~~**R-1 vorab entscheiden.**~~ ✅ erledigt mit Decision Log #34, festgelegt am
   04.08.2026 vor dem ersten Modelllauf: Die Forschungsfrage wird über drei
   einzeln messbare Bausteine beantwortet — Prognosegüte je Verfahren **gegen
   die Stufe-2-Baseline** (UF2), Trainings- und Inferenzaufwand (UF3), daraus
   die Eignungsaussage für diesen Datensatz (UF4). Eine Rangfolge zwischen den
   Verfahren nur, wenn der gepaarte Wilcoxon-Test sie hergibt.
2. **10 Wiederholungen**, nicht 5 Folds allein — verengt das Konfidenzintervall
   des Mittelwerts, auch wenn die Einzelstreuung bleibt.
3. **Je Zielgröße getrennt entscheiden.** Die Verfahren tauschen zwischen
   `anzahl_einsaetze` und `einsaetze_je_1000_ew` die Plätze — bei der Rate lag
   Random Forest im Vortest deutlich vorn. Sehr wahrscheinlich ist die Aussage
   dort belastbar und bei der absoluten Zahl nicht.

**Warum der Vortest eher zu pessimistisch als zu optimistisch ist:** Er lief
**ungetunt**, mit **20 statt 50** Fold-Ergebnissen und auf der Fold-Zuteilung
**vor** der doppelten Stratifizierung (#30). Alle drei Punkte verbessern sich im
geplanten Lauf. Und die Streuung von ± 0,609 sagt für sich genommen nichts über
die Signifikanz: Der gepaarte Wilcoxon-Test wertet die **Vorzeichen** der
Differenzen aus, nicht ihre Beträge. Gewinnt ein Verfahren 35 von 50 Folds, ist
das signifikant — auch wenn die Abstände wild streuen. Die 12 von 20 des
Vortests waren es nicht, aber das war ein Viertel der geplanten Messungen.
4. **R-3 dokumentieren, nicht wegrechnen.** Vorhersagen zu kappen wäre ein
   Eingriff, der den Verfahrensvergleich verwässert. Dass Baumverfahren bei
   unbekannten Stadtteilen strukturell im Nachteil sind, ist selbst ein
   Vergleichsergebnis.
5. **R-9 offen benennen statt ausgleichen.** Den Offset auch den
   Vergleichsverfahren zu geben wäre möglich, würde aber ihre Spezifikation
   ändern und den Vergleich mit dem Exposé brechen. Stattdessen in Kapitel 8
   benennen: Die Baseline ist an dieser Stelle strukturell im Vorteil, ein
   knapper Sieg über sie ist deshalb vorsichtig zu lesen.
6. **R-10 zweifach entschärfen — strukturell UND rechnerisch.** Erstens ist die
   Primäraussage nach #34 „Verfahren gegen Baseline", nicht der paarweise
   Vergleich; dort gibt es keine Testfamilie. Zweitens wird innerhalb der
   sekundären Familie (7 paarweise Tests) **Holm-Bonferroni** angewandt: p-Werte
   aufsteigend sortieren, den kleinsten gegen α/7 prüfen, den nächsten gegen
   α/6, und so fort bis zur ersten Nichtablehnung.

   *Korrektur einer früheren Einschätzung (04.08.2026):* Hier stand zunächst,
   eine Korrektur würde „jeden Befund kassieren". Das war überzogen — bei 50
   gepaarten Werten reicht α = 0,007 aus, wenn rund 35 Folds in dieselbe
   Richtung zeigen. Und Holm ist bei gleicher Fehlerkontrolle uniform stärker
   als Bonferroni. Es gibt keinen Grund, darauf zu verzichten.
