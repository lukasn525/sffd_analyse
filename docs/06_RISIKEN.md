# Risikoregister der Modellierung

> **Lebensdauer:** ändert sich, wenn ein Risiko eintritt, entschärft wird oder
> wegfällt. Neu gefasst am 07.08.2026, auf den finalen Lauf vom 16.08.2026
> nachgezogen (R-1 und R-2), **am 17.08.2026 um die Angriffsfläche außerhalb
> der Methodik erweitert** (R-18 bis R-24: Datenvalidität, inhaltlicher Nutzen,
> Ethik). Ursprünglich nach dem Lauf von
> `m02`, `m03`, `m04`, `m05` und nach den Entscheidungen #37 bis #46.
>
> **Diese Datei enthält keine eigenen Ergebniszahlen.** Sie zitiert
> `03_STAND.md` (Zahlen) und `07_BEFUNDE.md` (Herleitungen). Wo hier eine Zahl
> steht, steht sie als Beleg für die Einstufung eines Risikos, nicht als
> Ergebnis.

**Die Nummern R-1 bis R-25 sind Zitierschlüssel und werden nicht neu vergeben.**
Sie werden in `main.tex`, in den Docstrings von `m03`, `m04`, `m05`
und in `07_BEFUNDE.md` zitiert. Ein erledigtes Risiko behält seine Nummer und
wandert in Abschnitt 3, statt gelöscht zu werden — sonst zeigen die
Querverweise ins Leere.

---

## 1. Die Lage in fünf Sätzen

Gerechnet ist alles: zwei Zielgrößen-Stränge, ein Validierungsrahmen, 10
Wiederholungen × 5 Folds, Hold-out einmalig ausgewertet. **Kein offener
Rechenschritt, kein bekannter Fehler in Daten oder Code.** Die verbleibenden
Risiken sind Eigenschaften der Fragestellung (kleine Zahl unabhängiger
Einheiten) und Eigenschaften des Ergebnisses (die Verfahren trennen sich
nicht). Ein Punkt hat Außenwirkung und einen Termin: Schröter weiß vom
Baselinewechsel noch nichts. Alles Übrige ist Verschriftlichung.

**Was das Ergebnis in einem Satz ist:** In keinem der beiden Stränge ist der
Mehraufwand der Vergleichsverfahren belegt, und was die Prognose bewegt, ist
die Spezifikation, nicht die Verfahrensklasse — ein vollständiges,
verteidigbares Resultat, kein Negativbefund.

---

## 1b. Angriffsfläche — die kurze Übersicht

Wo die Arbeit heute am ehesten aufgemacht wird, nach Wucht sortiert. Jede Zeile
ist unten ausgeführt.

| # | Angriff | Wucht | Steht dem entgegen |
|---|---|---|---|
| **R-22** | „Wozu ist das gut? Sie sagen Stadtteile vorher, die alle bekannt sind." | **hoch** | Erklärungs- statt Prognoseabsicht offenlegen — die Arbeit sucht Struktur*faktoren*, keinen Einsatzplan |
| **R-18** | „Ihr wichtigstes Merkmal misst Polizeipräsenz, nicht Kriminalität." | **hoch** | Zugeben, in die Limitationen, Lum/Isaac zitieren |
| **R-20** | „Sie verteilen Ressourcen nach Armut — das ist algorithmisches Redlining." | **hoch** | Deployment ist abgegrenzt; dazu die Fairnessprüfung R-24 |
| **R-2** | Der Strukturstrang widerspricht sich zwischen CV und Hold-out | hoch, eingetreten | beide Auswertungen berichten, dazu die Decken (R-21b) |
| **R-19** | Kriminalitätsindex und Einsatzlast messen teils dasselbe | mittel–hoch | offen benennen, Partialkorrelation berichten |
| **R-21** | Ökologischer Fehlschluss und MAUP | mittel | Ergebnisse ausdrücklich auf Stadtteilebene begrenzen |
| **R-4/R-5** | Sechs bzw. 30 unabhängige Einheiten | eingetreten | bereits offen benannt, n_eff ≈ 39 |
| **R-1** | Die Verfahren trennen sich nicht | eingetreten | Trennschärfe beziffern, nicht als Gleichheit lesen |
| **R-23** | Rückkopplung bei hypothetischem Einsatz | gering | Deployment ausgeschlossen, Grenze benennen |
| **R-24** | „Ihr Modell ist für arme Stadtteile schlechter." | **entkräftet** | geprüft: relativ zum Niveau kein Zusammenhang |

**Die drei Sätze, die diese Übersicht zusammenfassen.** Methodisch ist die
Arbeit gut abgesichert — der Stadtteil-Split entspricht dem Standard für
geclusterte Daten (Roberts et al. 2017), die Fairnessfrage ist geprüft und
verneint, die Testkonstruktion ist offengelegt. Angreifbar ist sie an zwei
Stellen, die nicht in der Methodik liegen: an der **Validität des
Kriminalitätsindex**, der die Ablation dominiert und dennoch eine
Verwaltungsstatistik ist, und am **Zweck** — eine Prognose für unbekannte
Stadtteile hat in einer Stadt mit 35 bekannten Stadtteilen keinen unmittelbaren
Anwendungsfall. Beides ist mit einer ehrlichen Umdeutung zu halten: Die Arbeit
ist eine Erklärungsstudie mit Prognosewerkzeugen, keine Einsatzplanung.

---

## 2. Aktives Register

Sortiert nach Stufe. „Eingetreten" heißt: Das Risiko hat sich realisiert und
ist jetzt eine Eigenschaft des Ergebnisses, mit der die Arbeit umgehen muss.

| | Risiko | Stufe | Belegt in |
|---|---|---|---|
| **R-17** | Auflage B ist gegenüber Schröter als erledigt gemeldet, steht aber noch nicht im Text | **neu**, mittel | E-Mail 08.08. |
| **R-2** | Der Klassifikationsstrang trägt wenig, und seine beiden Auswertungen widersprechen sich | **hoch, eingetreten** | B-29, B-40, B-42 |
| **R-1** | Die Verfahren sind untereinander nicht unterscheidbar | **eingetreten, entschärft** | `vergleich.csv`, #34 |
| **R-4** | Die Schlussbewertung beruht auf sechs Einheiten | **eingetreten** | B-40, B-42 |
| **R-5** | Effektive Stichprobe: 29 unabhängige Einheiten in der Entwicklung | mittel | `03_STAND.md` §2/§3 |
| **R-3** | Ein Drittel der Testzeilen liegt außerhalb des Trainingsbereichs | mittel | B-31, B-32 |
| **R-11** | Die gepaarten Differenzen sind nicht unabhängig (Pseudoreplikation) | mittel, entschärft | B-5, B-41, #37 |
| **R-10** | Mehrfachvergleiche in zwei Familien, eine davon unkorrigiert | mittel | B-6, #38 |
| **R-16** | XGBoost ist nicht threaddeterministisch | **neu**, mittel | B-24 |
| **R-15** | UF3 beruht auf einer einzigen, nicht wiederholten Zeitmessung | **neu**, mittel | `03_STAND.md` §5.4 |
| **R-6** | Merkmale sind innerhalb eines Jahres konstant | gering | B-18 |
| **R-22** | Der praktische Nutzen des Prognoseziels ist nicht belegt | **neu**, hoch | §2.2 |
| **R-18** | Der Kriminalitätsindex misst auch Polizeipräsenz | **neu**, hoch | §2.2 |
| **R-20** | Proxy-Diskriminierung über sozioökonomische Merkmale | **neu**, hoch | §2.2 |
| **R-19** | Zirkularität zwischen Kriminalitätsindex und Einsatzlast | **neu**, mittel–hoch | §2.2 |
| **R-21** | Ökologischer Fehlschluss und MAUP | **neu**, mittel | §2.2 |
| **R-23** | Rückkopplung, falls das Modell je eingesetzt würde | **neu**, gering | §2.2 |
| **R-24** | Systematisch schlechtere Güte für arme Stadtteile | **neu**, geprüft und verneint | §2.2 |
| **R-25** | Berichtsumfang auf eine Mengen-Zielgröße verdichtet | **neu**, Entscheidung | §2.2 |
| **R-8** | ACS-Trefferquote 2009 nur 63,1 % | gering | `03_STAND.md` §1 |
| ~~R-13~~ | ~~Spezifikationsentscheidungen nach dem ersten Lauf~~ | **entfällt** — siehe unten | #42, #43, #45 |

### R-13 · entfällt — die Spezifikation ist iteriert, nicht abgeändert

**Festlegung Lukas, 08.08.2026: Dies ist kein Risiko und wird nicht als
Abweichung geführt.** Die Begründung gehört trotzdem aufgeschrieben, weil sie
in Kapitel 6 gebraucht wird.

**CRISP-DM ist ein Kreislauf, kein Wasserfall.** Die Rückkopplung zwischen
Modeling und Data Preparation ist die im Vorgehensmodell **vorgesehene**
Bewegung (Wirth & Hipp 2000), nicht ihre Verletzung. Wer sie als Abweichung
führt, unterstellt der Arbeit ein Präregistrierungsmodell, unter dem sie nie
angetreten ist.

**Und keine der drei Entscheidungen ist ergebnisgetrieben:**

| | Was war | Warum es geändert wurde |
|---|---|---|
| **#42** | Baseline mit Zähldaten-Likelihood, Baumverfahren mit quadratischem Fehler | **Ungleichbehandlung in der Spezifikation.** Der quadratische Fehler gewichtet einen Fehler von 20 bei Tenderloin (280 Einsätze) wie bei Seacliff (6,4). Das misst nicht Verfahren, sondern Verlustfunktionen |
| **#43** | Baseline mit Expositions-Offset, Baumverfahren ohne | **Dieselbe Ungleichbehandlung, zweite Form.** Die Forschungsfrage lautet, welches *Verfahren* die höchste Prognosegüte erzielt. Verlieren zwei davon, weil ihnen die Expositionsstruktur vorenthalten wurde, bleibt die gestellte Frage unbeantwortet |
| **#45** | NegBin mit Störparameter, Logit mit sklearn-Vorgabewert `C = 1,0` | **Vereinheitlichung einer Regel**, keine Modellwahl: Was einen freien Parameter hat, wird mit gleichem Budget getunt; was keinen hat, wird angepasst. `C = 1,0` war eine Voreinstellung, keine Entscheidung |

**Der stärkste Beleg gegen Ergebnisoptimierung ist die Wirkungsrichtung:** #45
machte die **Regressionslatte härter** (damals 37,27 → 33,98 RMSE) und die
Klassifikationslatte weicher (0,314 → 0,297); im finalen Lauf liegen sie bei
32,99 und 0,301. Wer sein Ergebnis sucht, ändert
nicht die Latte, über die er selbst springen muss.

**Zwei Belege bleiben im Text — nicht als Geständnis, sondern als Nachweis:**

1. **Das Hold-out war zu keinem Zeitpunkt berührt.** Keine der drei
   Entscheidungen konnte darauf schauen; die sechs Stadtteile sind erst nach
   Abschluss von Spezifikation und Tuning einmalig ausgewertet worden.
2. **Der Lauf unter der ersten Spezifikation ist vollständig berichtet** — als
   Ablation in `03_STAND.md` §5.5. Genau daraus entsteht die Antwort auf
   Unterfrage 4: Die Spezifikation bewegt bis zu 146,9 RMSE, die
   Verfahrenswahl 2,5. **Ohne die Iteration gäbe es diesen Befund nicht.**

**Formulierung für Kapitel 6:** iterative Verfeinerung der Spezifikation
innerhalb des CRISP-DM-Kreislaufs, mit Datum, Anlass und gemessener Wirkung —
nicht „nachträgliche Änderung". Das Gutachten verlangt, erkannte Probleme zu
**lösen** statt sie nur zu benennen (R9). Genau das ist hier passiert.

### ~~R-14~~ · ✅ ERLEDIGT am 08.08.2026 — Freigabe erteilt

Schröter hat noch am selben Tag geantwortet und **beides** freigegeben: die
einheitliche Spezifikation über die Einwohnerzahl (#43) als „plausibel" und die
unpenalisierten Varianten (#45) als „methodisch sauber, vermeidet willkürliche
Parameter und liefert zudem stärkere Vergleichswerte". Die Testkonstruktion
(#37, #38) hat er als „sauber umgesetzt" und den unkorrigierten Einzeltest der
Klassifikation als „folgerichtig" bestätigt.

**Daraus folgt eine neue Auflage, nicht nur eine Entlastung:** „Dokumentieren
Sie diese Begründung genau so." Der Wortlaut steht in `01_VORGABEN.md`,
Abschnitt 0, Auflage D. Zu beachten ist, dass sein Punkt „stärkere
Vergleichswerte" nur für den Mengenstrang zutrifft — im Strukturstrang sinkt die
Latte. Beide Zahlen standen in der Anfrage, die Freigabe erfolgte also in
Kenntnis; in Kapitel 5.4 ist der Unterschied dennoch zu benennen (R-2).

Der ursprüngliche Text dieses Risikos:

Schröter hat am 04.08.2026 die **Negative-Binomial-Regression** und die
**multinomiale logistische Regression** namentlich als geeignete Baselines
bestätigt (#35). Gerechnet wird seit #45 ein **Poisson-GLM mit Offset** und ein
**unpenalisiertes** Logit.

Sachlich trägt der Wechsel: Die Baseline liefert ausschließlich
Punktvorhersagen, und dafür bleibt der Poisson-Schätzer bei richtig
spezifiziertem bedingtem Mittelwert auch unter Überdispersion konsistent
(Gourieroux, Monfort & Trognon 1984). Die Modellklasse bleibt dieselbe, der
Link bleibt kanonisch, der Offset bleibt. **Die Freigabe deckt ihn dennoch
nicht.**

**Das stärkste Argument in der E-Mail ist die Asymmetrie der Folge:** Die
Regressionslatte **stieg** damals von 37,27 auf 33,98 RMSE, die
Klassifikationslatte **sank** von 0,314 auf 0,297 Macro-F1 (finaler Lauf:
32,99 und 0,301). Die Änderung hilft im einen Strang und
schadet im anderen — das ist das Gegenteil von Rosinenpicken und sollte genau
so formuliert werden.

**Mitgeteilt am 08.08.2026** (Entwurf in `entwuerfe/`). Die Mail nennt beide
Zahlenpaare und bietet an, mit den ursprünglich genannten Formen zu rechnen.
Antwort ausstehend; bis dahin bleibt der Punkt im Register. Widerspricht er,
sind es wenige Minuten Rechenzeit für die Regression — die getunte Logit wäre
aufwendiger.

### R-17 · Auflage B ist gemeldet, aber noch nicht umgesetzt — NEU

Die E-Mail vom 08.08.2026 meldet, die Zielsetzung der Validierung sei „als
Generalisierung auf unbekannte Stadtteile formuliert". **In
`main.tex` kommt der Begriff bislang nicht vor** (geprüft am 08.08. an der
damals `main_gliederung_2026-07-28.tex` benannten Datei, null Treffer für
„Generalisierung" und „unbekannte Stadtteile").

Damit ist aus einer Auflage eine Zusage geworden. Sie ist billig einzulösen —
der Begriff gehört wörtlich in die Zielsetzung und in Kapitel 5.4 —, aber sie
steht jetzt unter Beobachtung: Schröter hat die Formulierung selbst verlangt und
wird beim Lesen darauf achten. **Vor dem nächsten Kapitelversand erledigen.**

### R-2 · Der Klassifikationsstrang — eingetreten, in beide Richtungen

> **Ergänzt 17.08.2026, B-48:** Die erreichbare Obergrenze liegt bei Macro-F1
> 0,4147 (Stadtteilwissen), nicht bei 1,0. Der Random Forest schöpft davon
> 50,2 % aus, XGBoost 41,1 %.
> Das ordnet den Strang ein, statt ihn nur als schwach zu berichten.

| Auswertung | Mehrheitsklasse | Logit (Stufe 2) | Random Forest | XGBoost |
|---|---|---|---|---|
| Kreuzvalidierung, 50 Läufe | 0,221 | 0,301 | **0,3184 ± 0,0142** | 0,3008 ± 0,0149 |
| Hold-out, einmalig | 0,222 | **0,350** | 0,282 | 0,291 |
| Abstand Training zu Test | – | – | **+0,263** | **+0,179** |

**Neu am 16.08.2026: der Widerspruch ist erklärt.** Die dritte Zeile ist der
Überanpassungsnachweis nach #51. Wären die sechs Hold-out-Stadtteile schlicht
schwerer, müsste **jedes** Modell einbrechen. Die Baseline wird dort aber
**besser** (0,301 → 0,350) und die Mehrheitsklasse bleibt praktisch gleich
(0,221 → 0,222); nur die beiden Baumverfahren stürzen um 0,04 bzw. 0,01 ab —
und das, obwohl sie im Hold-out auf 30 statt 24 Stadtteilen trainieren, also
mit **mehr** Daten. Mehr Trainingsdaten und trotzdem schlechter: Das ist
Überanpassung, keine schwierige Testmenge (`07_BEFUNDE.md`, **B-46**).

**Drei Befunde, alle zu berichten:**

1. **In der Kreuzvalidierung schlägt nur der Random Forest die Stufe-2-Baseline**
   — +0,0173 Macro-F1 in 9 von 10 Wiederholungen, p = 0,004. XGBoost schlägt sie
   mit −0,0003 (6/10, p = 1,000) **nicht mehr**; in der Macro-AUROC liegt die
   Referenz mit 0,725 über beiden Baumverfahren.
2. **Auf dem Hold-out dreht das Vorzeichen.** Die Werte liegen innerhalb der
   jeweiligen CV-Spanne (Perzentil 14 für Random Forest, 16 für XGBoost, 64 für
   das Logit) — kein Wert ist für sich auffällig. Auffällig ist die Richtung:
   Auf denselben sechs Stadtteilen landen beide Ensembles im unteren Fünftel
   ihrer Verteilung und die Baseline im oberen Drittel.
3. **Beide Baumverfahren sagen die seltenste Klasse dort kein einziges Mal
   vorher.** F1 für `brand` ist 0,000; bei Random Forest liegt die AUROC mit
   0,173 unter dem Zufall, der gelernte Zusammenhang ist auf diesen Stadtteilen
   also invertiert. Das Logit erkennt dieselbe Klasse mit AUROC 0,895
   zuverlässig und klassifiziert nur zurückhaltend.

**Konsequenz: keine Rangfolge zwischen Logit und Baumverfahren.** Die Aussage
lautet: Der Mehraufwand von Random Forest und XGBoost ist im Strukturstrang
nicht belegt. Genau der Fall, den `CLAUDE.md` vorab als berichtbar vorgesehen
hat.

**Was zusätzlich bleibt, unabhängig vom Vorzeichen: der Ertrag.** Das beste
Verfahren erreicht 0,334 bei einem theoretischen Maximum von 1,0, während die
Kruskal-Wallis-Tests neun der zehn Strukturmerkmale als hochsignifikant
klassentrennend ausweisen (Teststatistiken bis H = 481). Es ist viel zu holen,
und alle Verfahren holen wenig davon. Gehört in die Limitationen.

**Und ein Vorbehalt, der mit einer Zahl in Kapitel 8 gehört** (#48): Der
Vorsprung in der Kreuzvalidierung ist gegen die **unpenalisierte** Baseline
gemessen (finaler Lauf 0,301). Gegen eine mit gleichem Budget getunte Fassung
(damals 0,314) fiele
er von +0,0304 und +0,0371 auf +0,0136 und +0,0203 — bei einer Streuung von
0,013 also womöglich nicht mehr signifikant. Die Sensitivität wird bewusst
**nicht** gerechnet: Schröter hat die unpenalisierte Form am 08.08. in Kenntnis
beider Zahlen freigegeben, und Stufe 2 ist definitorisch die Form ohne freien
Parameter. Der Vorbehalt wird stattdessen benannt — als Absatz, nicht als
Fußnote.

> **Die Begründungskette für Kapitel 6.2 trägt weiterhin.** Die Zielgröße
> entsteht als Maximum über vier Anteile; die Grenze zwischen zwei Klassen
> liegt dort, wo die zugehörigen Anteile einander schneiden. Im Merkmalsraum
> sind das Schnittflächen, keine Hyperebenen — ein Verfahren mit linearen
> Trennebenen ist hier konstruktionsbedingt im Nachteil, und Verfahren mit
> flexibleren Grenzen sind damit *ex ante* begründet. Dass sie den Vorsprung
> nicht durchhalten, widerlegt die Begründung nicht, sondern ist das Ergebnis.

### R-1 · Die Verfahren trennen sich nicht — eingetreten wie vorhergesagt

> **Ergänzt 17.08.2026, B-51:** Die Trennschärfe der Sekundärvergleiche liegt
> bei 10 bis 68 %. „Kein Unterschied" ist nicht gezeigt, sondern nicht
> gemessen — der Text muss das so sagen.

**Stand 16.08.2026: vollständig eingetreten.** Im finalen Lauf ist **keine
einzige** der sechs Paarungen trennbar — bei `anzahl_einsaetze` p_holm 0,773
bis 0,967, bei `einsaetze_je_1000_ew` 0,117 bis 0,967, in der Klassifikation
p 0,625. Zwölf von zwölf Paarungen sind „nicht unterscheidbar".

> Die Aussage „keine Rangfolge zulässig" gilt damit **ohne Ausnahme** — in
> beiden Zielgrößen der Menge und in der Struktur.

**Folgenlos für die Arbeit — weil #34 das vorab entschieden hat**, am
04.08.2026 und vor dem ersten Modelllauf: Die Forschungsfrage wird über drei
einzeln messbare Bausteine beantwortet, nicht über eine Rangfolge. Berichtet
werden mittlere Differenz, Konfidenzintervall und gewonnene Läufe — kein
Platz 1. Der Abstand Verfahren↔Baseline ist messbar, der Abstand
Verfahren↔Verfahren bei 29 Einheiten nicht.

**Der Vortest vom 28.07.2026 ist überholt und darf nicht mehr zitiert werden.**
Seine Zahlen (Ridge gegen Random Forest +0,042 ± 0,609; Rate RF 0,584 gegen
Ridge −0,087) stammen aus der Zeit vor der doppelten Stratifizierung (#30), vor
der Verlustfunktion (#42) und vor der Expositionsbehandlung (#43).

### R-4 · Sechs Einheiten in der Schlussbewertung

Die abschließende Bewertung läuft auf sechs Stadtteilen, 792 Zeilen, ohne
Streuung und ohne Test. **Im Strukturstrang weicht sie von der Kreuzvalidierung
ab** (R-2), liegt aber innerhalb der Spannweite der 50 Einzelläufe (Random
Forest 0,229–0,410, XGBoost 0,245–0,424; korrigiert am 25.08.2026, nachgerechnet
aus `results/klassifikation/struktur_folds.csv`). Als Einzelmessung zu kennzeichnen und
**nicht** als Widerlegung des Kreuzvalidierungsergebnisses zu lesen.

**Nicht mit den CV-Werten vergleichen.** Der Hold-out ist eine andere,
andere Aufgabe: Training auf 30 statt 24 Stadtteilen. Der Extrapolationsanteil
unterscheidet sich dagegen kaum noch (34,8 % gegen 36,6 %) — anders als im Lauf
vor der Bevölkerungskorrektur. Im Mengenstrang **kehrt sich die Rangfolge auf
dem Hold-out um**: dort schlagen beide Baumverfahren die Referenz, und Ridge
fällt hinter sie zurück.

**Zweite, eigenständige Schwäche des Verfahrens** (B-42): Die Baumverfahren
treten mit den Hyperparametern **eines einzigen** Folds an
(`fold_der_parameter`), obwohl diese über die Folds erheblich streuen — beim
Random Forest der Struktur `max_depth` 16/24/16/24/24, `n_estimators`
539/359/306/321/995. Das Logit hat keine Hyperparameter und ist von dieser
Asymmetrie nicht betroffen. **Wie viel das ausmacht, ist nicht gemessen.** Eine
saubere Gegenprobe wäre möglich, ohne das Hold-out anzufassen: innerhalb der
Kreuzvalidierung jedem Fold die Parameter eines anderen geben und den Abfall
messen. Nicht durchgeführt; Aufwand und Nutzen vor der Abgabe abzuwägen.

### R-5 · 29 unabhängige Einheiten

> **Ergänzt 17.08.2026:** Effektive Stichprobe ≈ 38 bei einem Designeffekt von
> 122, nur 140 verschiedene Merkmalsvektoren (B-49). Die Streuung der zehn
> Wiederholungsmittel misst die Abhängigkeit von der Fold-Zuteilung, nicht die
> Unsicherheit über die Grundgesamtheit — sie ist eine **Untergrenze** (B-50).

4.752 Zeilen klingen komfortabel, es sind **36 Querschnittseinheiten × 132
Monate**, davon 30 in der Entwicklung und 6 im Hold-out. Gemeinsame Ursache von
R-1, R-3, R-4 und R-11.

**Zwei Folgen für die Auswertung:**

- Die Gütemaße werden je Zeile gerechnet, aber die 132 Zeilen eines Stadtteils
  sind hochkorreliert — der effektive Stichprobenumfang der Metrik ist weit
  kleiner als *n*. Dass 92,5 % der Varianz von `anzahl_einsaetze` *zwischen*
  den Stadtteilen liegen, ist derselbe Sachverhalt von der anderen Seite.
- Die 50 Fold-Ergebnisse aus 10 Wiederholungen sind **nicht unabhängig**:
  dieselben 29 Stadtteile, nur anders gruppiert. Der Gewinn an Präzision ist
  kleiner als √10 (Nadeau & Bengio 2003). Deshalb ist `std_wiederholungen` über
  die 10 Wiederholungsmittel maßgeblich und nicht `std_folds` über die 50
  Läufe.

Gehört in die Limitationen und schützt vor dem Vorwurf der Überinterpretation
(Gutachten R2).

### R-3 · Extrapolation — Eigenschaft des Rahmens, keine Erklärung

**36,6 %** der Testzeilen über alle 50 Läufe liegen in mindestens einem Merkmal
außerhalb der Trainingsspanne; 38,2 % in Wiederholung 0 (Fold-Spanne 0,0 bis
66,7 %), 34,8 % im Hold-out. Unvermeidliche Folge des Stadtteil-Splits, begrenzt
die Generalisierbarkeit.

**Als Erklärung für Verfahrensunterschiede geprüft und widerlegt** (B-31) — und
unter der finalen Spezifikation erst recht gegenstandslos:

- Es gibt **keinen Rückstand mehr zu erklären**. Bei `anzahl_einsaetze` liegt
  Ridge 4,04 RMSE **vor** dem Random Forest und 6,46 vor XGBoost, beide
  signifikant; bei der Rate 0,16 bzw. 0,21 davor.
- Der Zusammenhang zwischen Extrapolationsanteil und RMSE ist bei **Ridge am
  stärksten** (ρ +0,335 / +0,461) und beim **Random Forest am schwächsten**
  (+0,249 / +0,287), bei XGBoost dazwischen (+0,353 / +0,375). Wäre
  Extrapolation der Hebel gegen die Bäume, müsste es umgekehrt sein — Ridge ist
  von ihr am stärksten betroffen und zugleich das beste Verfahren.

**Der Satz in Kapitel 7 und 8 muss lauten: geprüft und nicht bestätigt.** Das
ist ein stärkerer Beitrag als die ursprüngliche Vermutung, weil er eine
naheliegende Erklärung ausschließt statt sie weiterzureichen.

**Es ist zudem eine Eigenschaft von Stadtteilen, nicht von Zeilen** (B-32): 9
von 29 brechen zu 100 % ihrer Zeilen aus, 16 zu 0 %. Die tragfähige
Formulierung lautet nicht „33,7 % der Testzeilen", sondern: *Rund ein Drittel
der Stadtteile San Franciscos ist in mindestens einem Strukturmerkmal so
ungewöhnlich, dass kein anderer Stadtteil sie abdeckt.* Eine Aussage über die
Stadt, nicht über die Modelle.

### R-11 · Pseudoreplikation

> **Beziffert seit 17.08.2026 in `07_BEFUNDE.md`, B-50.** Die SD der zehn
> Wiederholungsmittel liegt bei 2,773 statt der 6,629, die bei unabhängigen
> Folds zu erwarten wären; im Strukturstrang unterscheiden sich die p-Werte
> beider Ebenen um den Faktor 2.000.

Der gepaarte Wilcoxon-Test setzt unabhängige Paare voraus; es sind dieselben 29
Stadtteile in zehn Gruppierungen. Über 50 Läufe gerechnet fiele sein p-Wert
**zu klein** aus, und Holm hilft dagegen nicht — es korrigiert
Mehrfachvergleiche, nicht Pseudoreplikation.

**Entschärft durch #37:** Der Primärtest läuft auf den **10
Wiederholungsmitteln** (n = 10, kleinstes erreichbares zweiseitiges p 0,00195 —
also auch nach Holm erreichbar). Der Test über alle 50 steht als ausdrücklich
gekennzeichnete Sensitivität in derselben Datei, Spalte `teststufe`.

**Rest bleibt und gehört in Kapitel 8:** Auch die zehn Mittel sind nicht
unabhängig. Der Test kontrolliert die Fold-Schwankung, nicht den kleinen Umfang
an Analyseeinheiten; das berichtete Konfidenzintervall ist enger als die wahre
Unsicherheit. Deshalb stehen mittlere Differenz, KI und gewonnene Läufe immer
neben dem p-Wert.

**Dieselbe Pseudoreplikation, an anderer Stelle und mit umgekehrter Wirkung**
(B-41): Der RESET-Test der Eignungsprüfung lief auf 3.168 Zeilen, als wären sie
unabhängig. Ein F-Test mit dieser Fallzahl findet praktisch jede Abweichung
signifikant — die daraus abgeleitete Nichtlinearität generalisiert nicht, sie
zerstört die Prognose out-of-sample um den Faktor drei bis fünf. Dort macht die
Pseudoreplikation p-Werte zu klein, hier lässt sie Modellstruktur erscheinen,
die es nicht gibt. **Ein übertragbarer methodischer Beitrag der Arbeit.**

### R-10 · Zwei Testfamilien, eine unkorrigiert

Der gepaarte Wilcoxon läuft je Zielgröße und Verfahrenspaar: **3 Paare × 2
Mengen-Zielgrößen** in der Regression, **1 Paar** in der Klassifikation. Seit
#38 sind das **zwei getrennte Familien (6 und 1)**, nicht eine Familie mit
sieben Tests — Regression und Klassifikation beantworten verschiedene
Teilfragen, ein Zufallstreffer im einen Strang macht den anderen nicht falsch.

- `m02` rechnet **Holm-Bonferroni über seine 6 sekundären Tests**: p-Werte
  aufsteigend sortieren, den kleinsten gegen α/6 prüfen, den nächsten gegen
  α/5, und so fort bis zur ersten Nichtablehnung.
- `m03` hat einen einzigen sekundären Test und wird **nicht** korrigiert. Preis,
  offen zu benennen: Der Vergleich Random Forest gegen XGBoost läuft gegen
  α = 0,05 statt gegen 0,0071.
- Die **Primäraussage** „Verfahren gegen Baseline" bildet nach #34 keine
  Testfamilie und wird nicht korrigiert.

Code und Dokumentation stimmen seit 07.08.2026 überein; bis dahin stand hier
„7 Tests, α/7" (B-6).

### R-16 · XGBoost ist nicht threaddeterministisch — NEU im Register

Bei anderer Kernzahl weichen die Vorhersagen ab: bis **322,8** bei
`anzahl_einsaetze` (Mittelwert der Zielgröße 75,9), 18,9 bei der Rate,
**7,4 %** abweichende Klassen im Strukturstrang. Ridge und Random Forest sind
unauffällig (≤ 2·10⁻¹³).

**Ergebnisse unberührt** — alle berichteten Werte stammen aus dem einkernigen
Fit, und die Abweichung ist gemessen statt vermutet (B-24). **Aber:** Die
Abgabe enthält Quellcode und muss reproduzierbar sein. Ein Prüfer, der das Repo
auf einer anderen Maschine startet, bekommt bei XGBoost andere Zahlen.

**Was daraus folgt:** Die Reproduzierbarkeitsangabe in Kapitel 6 muss die
**Kernzahl** nennen, nicht nur den `random_state`, und die Abweichung
beziffern. Bisher steht das nur in `03_STAND.md` §5.4 und in B-24, nicht im
Register — deshalb hier nachgetragen.

### R-15 · UF3 beruht auf einer einzigen Zeitmessung — NEU im Register

Trainings- und Inferenzaufwand sind der dritte tragende Baustein der
Forschungsfrage (#34). Die Datengrundlage dafür ist **ein** Durchgang auf
**einer** Maschine: Intel Core i5-7300U, 2 physische Kerne mit Hyperthreading,
7,8 GB, Windows 10, ohne Wiederholung der Messung.

**Belastbar ist der Kern der Aussage:** Zwischen Ridge und den Ensembles liegen
zwei Größenordnungen, nicht Prozentpunkte. Eine solche Differenz überlebt jedes
Messrauschen.

**Nicht belastbar ist der Parallelisierungsgewinn.** Zwei physische Kerne mit
Hyperthreading sind ein Grenzfall — vier logische Prozessoren teilen sich zwei
Recheneinheiten. Auf acht echten Kernen fielen die Faktoren anders aus, und bei
einer U-Serie-CPU ist thermische Drosselung über rund eine Stunde Laufzeit
nicht auszuschließen. Der Befund „bei XGBoost liegt der Gewinn unter 1"
(B-28) ist plausibel begründet, aber an diese Maschine gebunden.

**Was daraus folgt:** Einkern-Zeiten als Hauptaussage, Parallelisierungsgewinn
ausdrücklich als maschinengebunden kennzeichnen. Prozessor, Kernzahl,
Nebenlast und `requirements_lauf.txt` gehören in Kapitel 6.

### R-6 · Merkmale sind innerhalb eines Jahres konstant

ACS erscheint jährlich, Land Use ist ein Snapshot 2020. Das Modell sagt für
alle zwölf Monate eines Stadtteils fast denselben Wert vorher; die
Monatsschwankung geht vollständig ins Residuum. Sichtbar auch in der
VIF-Rechnung: Die Strukturmerkmale variieren nur auf **330 Stadtteil-Jahren**,
nicht auf 3.960 Zeilen (B-18) — höchster VIF 10,64 bei
`median_haushaltseinkommen`, weshalb blockweise statt einzelmerkmalsweise
interpretiert wird.

Das ist zugleich die Antwort auf eine der erwarteten Kolloquiumsfragen (siehe
Abschnitt 5): Das Modell sagt das **Niveau** eines unbekannten Stadtteils
vorher, nicht seine Monatsdynamik. Genau das ist die Forschungsfrage.

### R-8 · ACS-Trefferquote 2009

63,1 % im Jahrgang 2009, gegenüber 99,2 % in 2021/23. Für die Hauptanalyse ab
2015 folgenlos, gehört in die Limitationen.


---

## 2.2 Inhaltlicher Nutzen, Datenvalidität und Ethik

Ergänzt am 17.08.2026. Die Risiken R-1 bis R-17 betreffen Methodik, Anwendung
der Verfahren, Ergebnisse und Vergleichbarkeit. Sie decken nicht ab, ob die
Analyse **inhaltlich etwas wert** ist und ob sie **ethisch tragfähig** ist. Das
sind die beiden Fragen, auf die eine gut abgesicherte Arbeit am ehesten
unvorbereitet trifft.

### R-22 · Der praktische Nutzen des Prognoseziels ist nicht belegt — NEU, hoch

Der schärfste verfügbare Angriff, und er zielt nicht auf die Methode, sondern
auf den Zweck.

Der Stadtteil-Split misst, wie gut sich ein **unbekannter** Stadtteil aus
seinem Querschnittsprofil vorhersagen lässt. San Francisco hat aber keine
unbekannten Stadtteile — alle 35 liegen mit 132 Monaten Historie vor. Für den
operativen Einsatz wäre die eigene Vergangenheit eines Stadtteils die weitaus
stärkere Information, und genau die schließt Decision Log #29 als Modellmerkmal
aus: `lag_1`, `lag_12` und `rolling_mean_3` stehen in der Datei, aber nicht im
Modell.

Das ist **kein Fehler, sondern eine bewusste Entscheidung** — und sie muss als
solche im Text stehen, sonst wirkt sie wie ein Versehen. Die Arbeit fragt nach
dem Erklärungsbeitrag von Strukturfaktoren, nicht nach dem besten Einsatzplan.
Wer Lags aufnimmt, misst die Trägheit der Zeitreihe und nicht mehr den Beitrag
von Armut, Bausubstanz und Kriminalität.

**Konsequenz für die Verschriftlichung.** Das Wort „Prognose" trägt die Arbeit
nicht allein. Sie ist eine **Erklärungsstudie, die Prognosegüte als Maßstab
benutzt** — out-of-sample zu messen ist strenger als In-sample-Signifikanz und
genau deshalb gewählt. Diese Umdeutung gehört in Kapitel 3 (Zielsetzung), nicht
erst in die Limitationen. Der Übertragungsnutzen liegt bei Städten, die
**keine** Einsatzhistorie je Stadtteil haben — das ist der Anwendungsfall, den
das Modell tatsächlich bedient, und er ist zu benennen.

### R-18 · Der Kriminalitätsindex misst auch Polizeipräsenz — NEU, hoch

`log_kriminalitaetsindex` ist das einzige Merkmal, dessen Weglassen die
Prognose messbar verschlechtert (+24,3 RMSE, 10 von 10 Wiederholungen). Der
gesamte inhaltliche Befund zu UF1 hängt an dieser einen Spalte. Sie stammt aus
SFPD Incident Reports — also aus **polizeilich registrierten** Delikten.

Registrierte Kriminalität ist keine Kriminalität. Sie ist das Produkt aus
Tatgeschehen, Anzeigebereitschaft der Bevölkerung und Kontrollintensität der
Polizei. Wo mehr gestreift wird, wird mehr registriert. Der Location Quotient
misst damit teilweise, wo die Polizei präsent ist — und Polizeipräsenz ist in
US-Städten historisch ungleich über Stadtteile verteilt. Dass ein solches Maß
der stärkste Prädiktor ist, ist deshalb interpretationsbedürftig und nicht
einfach ein Befund über Brandrisiko.

**Was zu tun ist:** In die Limitationen, mit Beleg. Der kanonische Nachweis für
die Rückkopplung zwischen Polizeieinsatz und registrierter Kriminalität ist
Lum & Isaac (2016). Die Aussage lautet dann nicht mehr „Kriminalität erklärt
Feuerwehreinsätze", sondern „die **registrierte Kriminalitätsbelastung** eines
Stadtteils ist der stärkste verfügbare Indikator" — schwächer formuliert, aber
haltbar.

### R-19 · Zirkularität zwischen Prädiktor und Zielgröße — NEU, mittel–hoch

Beide Größen könnten dieselbe latente Eigenschaft messen: die allgemeine
Aktivitäts- und Belastungslage eines Stadtteils. Ein erheblicher Teil der
SFFD-Einsätze sind Fehlalarme und Rettungsdiensteinsätze — Kategorien, die mit
Bevölkerungsdichte, Notruffrequenz und sozialer Lage zusammenhängen, nicht mit
Brandlast.

Dazu kommt ein **mechanischer** Anteil bei der Rate: `einsaetze_je_1000_ew` hat
die Bevölkerung im Nenner, der Location Quotient ebenfalls (Delikte pro
Einwohner). Innerhalb eines Stadtteils korreliert `log_bevoelkerung` mit −0,734
mit der Rate und mit −0,732 mit dem Kriminalitätsindex. Kontrolliert man dafür,
fällt die Within-Korrelation zwischen Kriminalitätsindex und Rate von **+0,644
auf +0,230**.

Herleitung: `07_BEFUNDE.md`, B-53.

**Was zu tun ist:** Die Partialkorrelation berichten und `anzahl_einsaetze` mit
Bevölkerungs-Offset als Hauptzielgröße führen — dort tritt der
Nennerzusammenhang nicht auf.

### R-20 · Proxy-Diskriminierung über sozioökonomische Merkmale — NEU, hoch

Der Merkmalssatz enthält Armutsquote, Medianeinkommen, Akademikerquote,
Medianmiete und Leerstandsquote. Ethnische Zugehörigkeit ist **kein** Merkmal —
aber in einer segregierten Stadt sind Stadtteil und Sozialprofil starke Proxys
dafür. Ein Modell, das Ressourcen nach diesen Größen verteilte, wäre
Diskriminierung über Stellvertretermerkmale, auch ohne die geschützte
Eigenschaft je zu sehen. Das ist die Struktur, die als algorithmisches
Redlining beschrieben wird.

**Was der Arbeit hier hilft:** Die Deployment-Phase von CRISP-DM ist
ausdrücklich abgegrenzt. Es wird nichts eingesetzt und nichts zugeteilt. Diese
Abgrenzung ist bislang als **Umfangsentscheidung** formuliert — sie sollte
zusätzlich als **ethische Grenze** formuliert werden. Das kostet zwei Sätze und
verwandelt eine Lücke in eine bewusste Position.

Zweiter Punkt, der zu benennen ist: Ressourcenzuteilung nach vorhergesagtem
Bedarf ist nicht per se ungerecht — in der Notfallversorgung ist Zuteilung nach
Bedarf sogar das Ziel. Der Unterschied zur Polizeiarbeit liegt darin, dass ein
zusätzlicher Löschzug einem Stadtteil nützt, während zusätzliche Streifen ihn
belasten. Dieses Argument gehört in die Diskussion, denn es unterscheidet die
Arbeit von der Predictive-Policing-Kritik, statt ihr auszuweichen.

### R-21 · Ökologischer Fehlschluss und MAUP — NEU, mittel

Zwei klassische Einwände gegen jede Analyse auf Flächeneinheiten:

**Ökologischer Fehlschluss.** Ein Zusammenhang zwischen Armutsquote und
Einsatzlast auf Stadtteilebene sagt nichts über einzelne Haushalte. Der Satz
„arme Menschen verursachen mehr Feuerwehreinsätze" folgt aus den Ergebnissen
**nicht** und darf nirgends stehen (Robinson 1950).

**Modifiable Areal Unit Problem.** Die Ergebnisse hängen an der gewählten
Gebietseinteilung. Dieselben Rohdaten auf den 242 Census Tracts statt auf den
35 Analysis Neighborhoods könnten andere Koeffizienten und eine andere
Rangfolge liefern (Openshaw 1984). Der Zuschnitt ist eine Setzung, keine
Naturgegebenheit — die Wahl der Analysis Neighborhoods ist zu begründen
(offizielle Abgrenzung, Verfügbarkeit des Crosswalks) und ihre Wirkung als
Limitation zu benennen.

Beides ist billig zu entschärfen: je ein Absatz in Kapitel 8.3, keine
Rechnung nötig.

### R-23 · Rückkopplung bei hypothetischem Einsatz — NEU, gering

Würde das Modell zur Ressourcenverteilung genutzt, entstünde die aus dem
Predictive Policing bekannte Schleife: mehr Präsenz → mehr registrierte
Ereignisse → Bestätigung der Vorhersage. Beim Feuerwehreinsatz ist die Schleife
schwächer als bei der Polizei, weil ein Brand auch ohne Präsenz gemeldet wird —
bei **Fehlalarmen**, der mit 79 % dominanten Klasse, ist sie aber durchaus
denkbar.

Praktisch gering, weil nichts eingesetzt wird. Gehört trotzdem in einen Satz
des Ausblicks, weil es die Grenze markiert, an der aus einer Erklärungsstudie
ein Eingriff würde.

### R-24 · Fairness der Prognosegüte — NEU, geprüft und verneint

Der naheliegende Vorwurf lautet: Das Modell ist für arme Stadtteile ungenauer,
also benachteiligt es sie. Geprüft am 17.08.2026 über alle 50 Läufe, indem je
Fold das Sozialprofil der Teststadtteile gegen die Fold-Güte gestellt wurde.

**Absolut** besteht der Zusammenhang: Spearman-Rho zwischen Armutsquote und
RMSE liegt bei +0,35 bis +0,60 (p < 0,015) über alle Verfahren und beide
Zielgrößen, einschließlich der Poisson-Baseline.

**Relativ zum Niveau** verschwindet er vollständig. Setzt man den RMSE ins
Verhältnis zur mittleren Einsatzzahl des Folds, liegt Rho zwischen −0,006 und
+0,275, und **kein Wert ist auf 5 % signifikant** (kleinstes p = 0,053 bei
XGBoost auf der Rate).

**Lesart:** Der absolute Fehler ist in ärmeren Stadtteilen größer, weil dort
schlicht mehr Einsätze stattfinden. Die relative Genauigkeit ist über das
Sozialgefälle hinweg gleich. Es liegt kein Hinweis auf systematische
Benachteiligung vor.

**Zwei Ehrlichkeiten dazu.** Erstens ist der relative Fehler auf allen Stufen
hoch — im Mittel 0,48 bei `anzahl_einsaetze` und 0,66 bis 0,69 bei der Rate.
Das Modell ist nirgends präzise, nicht nur in armen Stadtteilen. Zweitens ist
das eine Prüfung auf Fehler*gleichheit*, nicht auf Verteilungsgerechtigkeit
eines hypothetischen Einsatzes — die wäre eine andere Frage und wird hier nicht
beantwortet.

Herleitung und Reproduktionsweg: `07_BEFUNDE.md`, B-52.

Diese Prüfung ist der Grund, warum das Ethikkapitel nicht bei „könnte
problematisch sein" stehen bleiben muss. Ein gemessener und verneinter Verdacht
ist stärker als ein ungeprüfter.

### R-25 · Berichtsumfang des Mengenstrangs verdichtet — NEU, Entscheidung

Am 17.08.2026 entschieden: Der Mengenstrang wird im **Bericht** auf
`anzahl_einsaetze` verdichtet. Gerechnet bleiben beide Zielgrößen, `results/`
und der Code sind unverändert, es wird kein Lauf wiederholt.

**Warum die Anzahl und nicht die Rate**

1. Kapitel 1.2 nennt wörtlich „die Einsatzhäufigkeit als Anzahl Einsätze pro
   Stadtteil und Monat". Die Rate kommt in der Forschungsfrage nicht vor —
   ihr Wegfall lässt die Fragestellung unangetastet.
2. Die Ratenmodellierung bleibt erhalten: Seit #43 modellieren alle Verfahren
   die Rate und multiplizieren zurück, das GLM trägt `log(Bevölkerung)` als
   Offset. Die Expositionsbehandlung ist über `ablation_exposition.csv` weiter
   belegt.
3. Bei der Rate ist die R²-Streuung über die Folds größer als der Mittelwert
   (random_forest 0,241 ± 1,247). Eine Rangfolge wäre dort nicht belastbar.
4. Nennerartefakt (R-19): Bevölkerung steht im Nenner der Rate und des
   Kriminalitätsindex; die Within-Korrelation fällt nach Kontrolle von +0,644
   auf +0,230. Mit der Rate verschwindet diese Flanke.

**Was der Schnitt kostet — geprüft: nichts.** Alle sechs Rate-Tests stimmen in
Richtung und Signifikanzmuster mit den Anzahl-Tests überein; kein sekundärer
Vergleich wird bei der Rate signifikant. Signifikanztests gesamt 15 → 9,
berichtete Einzelläufe 300 → 150.

**Zwei Stellen, die dabei korrekt bleiben müssen:**

- **Laufzeit (UF3).** Die Zeiten sind je Zielgröße gemessen. Berichtet wird die
  Zeile `anzahl_einsaetze` — kein Mittelwert über beide, keine Gesamtlaufzeit
  des Skripts. Die Rate liefert dieselben Zeiten mit unter 6 % Abweichung; das
  ist eine zweite Messung derselben Größenordnung und **schwächt R-15 ab**.
- **Abbildungen.** Alle zehn zeigen weiterhin beide Zielgrößen und werden nicht
  gefiltert. Abbildungen zählen nicht zu den 40 bis 60 Seiten (Schröter,
  10.08.), tragen die Robustheitsprüfung also kostenlos. Die Bildunterschriften
  müssen das benennen, sonst steht in der Abbildung mehr als im Text.

Damit ist Schröters Auflage „alle Analysen in die Arbeit" erfüllt — die Rate
erscheint als Robustheitsbefund statt als zweite gleichrangige Tabelle. Das
adressiert zugleich R8 des Gutachtens (Fokus statt Breite).

### Was die Literatur als Standard hergibt

| Einwand | Standard | Was er für diese Arbeit bedeutet |
|---|---|---|
| Validierung bei geclusterten Daten | Roberts et al. (2017), *Ecography* 40(8), 913–929 | **Entlastung.** Block- bzw. Leave-Group-Out-Kreuzvalidierung ist die empfohlene Praxis, wenn Abhängigkeitsstrukturen vorliegen; zufälliges k-Fold liefert zu optimistische Schätzungen. Der Stadtteil-Split ist damit kein Sonderweg, sondern der Standard — mit Zitat belegbar |
| Registrierte Kriminalität als Prädiktor | Lum & Isaac (2016), *Significance* 13(5), 14–19 | **Belastung.** Kanonischer Nachweis der Rückkopplung zwischen Polizeieinsatz und registrierten Delikten. Gehört zu R-18 |
| Schlüsse von Flächen auf Personen | Robinson (1950), *Am. Sociol. Rev.* 15(3), 351–357 | Formulierungsauflage: keine Aussage über Individuen |
| Abhängigkeit vom Gebietszuschnitt | Openshaw (1984), *Environment and Planning A* 16(1), 17–31 | Begründung der Analysis Neighborhoods, MAUP als Limitation |
| Vergleichbare Studien | Fire-/Emergency-Event-Prognose auf Stadtteilebene arbeitet regelmäßig mit **Negative-Binomial- und Poisson-Regression als Referenz** | **Entlastung.** Dass ein GLM die Vergleichsverfahren schlägt, ist in diesem Feld kein Ausreißer, sondern anschlussfähig |

---

## 3. Erledigt, entfallen, behoben

Die Nummern bleiben besetzt, weil sie zitiert werden.

| | Was es war | Auflösung |
|---|---|---|
| **R-7** | Der Stadtteil-Split ist mit Schröter unbesprochen | ✅ **04.08.2026, #35.** „Der Stadtteil-Split ist für die von Ihnen formulierte Forschungsfrage methodisch gut begründet. […] Insofern können Sie wie geplant vorgehen." Drei Auflagen daraus → Abschnitt 4 |
| **R-9** | Spezifikationsasymmetrie zulasten der Vergleichsverfahren | ✅ **06.08.2026, #43 — beseitigt statt beziffert.** Die Asymmetrie war real und beträgt über alle zehn Wiederholungen **22 bis 29 RMSE** (`03_STAND.md` §5.5; die 24 bis 30 in B-33 stammen aus Wiederholung 0): Baumverfahren, die direkt auf `anzahl_einsaetze` anpassen, können „Einsätze = Bevölkerung × Risiko" nicht abbilden. Seit #43 modellieren **alle** Verfahren die Rate und multiplizieren zurück; Schröter hat das am 08.08. als „plausibel" freigegeben (#47). Die frühere Festlegung „nicht ausgleichen, sondern berichten" gilt **nicht mehr**; die alte Spezifikation steht als Ablation in §5.5 und trägt dort die Antwort auf UF4 |
| **R-9** (erste Fassung) | Der Offset verschaffe der Baseline einen Vorteil | ✅ **05.08.2026 — geprüft, aber von der falschen Seite.** Gemessen wurde, ob die Baseline durch den Offset *gewinnt* (−0,0017 RMSE, also nichts). Die relevante Frage lautete, ob die Vergleichsverfahren *verlieren*, weil sie ihn nicht haben — Antwort: ja (B-34). Der Eintrag bleibt als Beleg für die Fehlerart stehen und gehört in die kritische Reflexion |
| **R-12** | Die wiederholten Splits waren wie spezifiziert nicht durchführbar | ✅ **behoben, #36.** Der `versatz` rotierte nur die Gruppen*nummern*: 6 statt 10 Partitionen — und **Gruppe 0 ist das Hold-out**, das damit in neun von zehn Wiederholungen mittrainiert worden wäre. `v0_aufteilung.py` hält das Hold-out fest, erhält die doppelte Stratifizierung und mischt je Wiederholung geseedet innerhalb der Rangblöcke. Wiederholung 0 reproduziert die Parquet-Dateien bitgenau, geprüft per `assert` bei jedem Aufruf (B-1 bis B-3) |
| — | Brand in einzelnen Folds mit 1–2 Fällen | ✅ durch die doppelte Stratifizierung (#30) auf 2 bis 13 Testfälle je Fold gehoben, im Mittel 6,6 |
| — | Zwei Anteile nicht vorhersagbar | ✅ hinfällig: Die `anteil_*`-Spalten sind seit #31 keine Modellzielgröße mehr |
| — | Laufzeiten zwischen den Verfahren nicht vergleichbar | ✅ durch #39/#40 gelöst: einkernige Messung für alle, Parallelisierungsgewinn als eigene Kennzahl. Rest lebt als R-15 |
| — | Negative Vorhersagen nach Rücktransformation | ✅ gegenstandslos: **null** negative Vorhersagen in allen 300 Läufen. Tweedie und Poisson haben eine Log-Verknüpfung, die Zielgröße wird strukturell respektiert statt nachträglich geprüft (B-15) |
| — | Tuning auf Wiederholung 0 könnte die übrigen Wiederholungen begünstigen | ✅ gemessen, kein Effekt: sechs von acht Diagnosewerten sind negativ, kein systematisches Muster (B-21, B-27) |

---

## 4. Die drei Auflagen aus Schröters Freigabe

Wörtlich aus der E-Mail vom 04.08.2026 (#35). Alle drei müssen in die Arbeit.

**Auflage A — die Abweichung transparent erläutern.** *„Wichtig ist, dass Sie
die Abweichung von der ursprünglich angekündigten zeitreihengerechten
Kreuzvalidierung transparent erläutern."* Das begründete Verwerfen in Kapitel 8
ist damit verlangt, nicht optional (#29). Der Beleg liegt vor: 92,5 % der
Varianz liegen zwischen den Stadtteilen, ein Zeitschnitt würde im Kern die
zeitliche Stabilität messen und die Forschungsfrage nicht prüfen.

**Auflage B — die Zielsetzung als Generalisierung formulieren.** *„und die
Zielsetzung der Validierung klar als Generalisierung auf unbekannte Stadtteile
formulieren."* Eine Formulierungsvorgabe: Der Begriff gehört **wörtlich** in
Kapitel 5.4 und in die Zielsetzung, nicht nur sinngemäß.

**Auflage C — identische Merkmale und Splits.** *„Achten Sie darauf, für alle
Vergleichsmodelle identische Merkmale und Splits zu verwenden."* Erfüllt, muss
aber **gezeigt** werden: `fold` und `ist_holdout` stehen als Spalten in beiden
Parquet-Dateien, die Merkmalsliste einmal in `prep/config.py`, die
Wiederholungen einmal in `v0_aufteilung.py`. Kein Modellskript kann davon
abweichen. Der Mechanismus gehört in Kapitel 5.4 beschrieben — als **Beleg**,
nicht als Zusicherung.

### Stand der Abstimmung mit Schröter

Diese Tabelle führt den **Abstimmungsstand**, nicht Abweichungen. Das Exposé
ist ein Vorhabensdokument und keine Präregistrierung; wo die Umsetzung von ihm
abweicht, ist das im Decision Log begründet und in Kapitel 6 zu erläutern — es
begründet keinen eigenen Registereintrag.

| Punkt | Festlegung | Stand |
|---|---|---|
| Stadtteil-Split statt Zeitschnitt | 5 Folds à 6 Stadtteile, 6 Hold-out | ✅ freigegeben 04.08. (#35), Auflage A verlangt die Erläuterung |
| Baselines in zwei Stufen | Stufe 1 ohne, Stufe 2 mit Merkmalen | ✅ freigegeben 04.08. (#35) |
| Drei Verfahren Regression, zwei Klassifikation | Ridge/RF/XGB gegen RF/XGB | ✅ freigegeben 03.08. (#31), Begründung in 6.2 verlangt |
| Einsatzart auf Stadtteil × Monat | dominante Einsatzart statt Einzeleinsatz | in Kapitel 6 zu begründen (#29) — auf Einzeleinsatz-Ebene lag die Obergrenze bei 49,9 % gegen 48,2 % für bloßes Raten |
| Rate als zweite Zielgröße | Einsätze je 1.000 Einwohner | in Kapitel 6 zu benennen (#29) |
| Verlustfunktion nach Datenform | Tweedie / `criterion="poisson"` | in Kapitel 6 zu begründen (#42) |
| Expositionsbehandlung für alle Verfahren | alle modellieren die Rate | ✅ freigegeben 08.08. als „plausibel" (#43) — trägt zugleich die Antwort auf UF4 |
| Poisson statt Negative Binomial | unpenalisierte GLM als Stufe 2 | ✅ freigegeben 08.08. (#45), **mit Auflage D** |
| Primärtest auf 10 Wiederholungsmitteln | statt über 50 Läufe | ✅ freigegeben 08.08. als „sauber umgesetzt" (#37) |
| Zwei Testfamilien statt einer | Holm über 6, ein Test unkorrigiert | ✅ freigegeben 08.08. als „folgerichtig" (#38) |

**Seit dem 08.08.2026 ist keine Festlegung mehr unabgestimmt.** Was bleibt, ist
Schreibarbeit: begründen, nicht rechtfertigen — und bei den Baselines die
Begründung wörtlich so übernehmen, wie Schröter sie formuliert hat
(`01_VORGABEN.md`, Auflage D).

---

## 5. Kolloquium — wo die Antwort steht

Die Fragen stammen aus dem Gutachten zum Anwendungsprojekt und aus der
Gruppensprechstunde vom 13.07.2026. Auf jede muss eine belastbare Antwort
vorliegen.

| Frage | Antwort liegt in |
|---|---|
| „Warum genau diese drei Algorithmen — und wieso überhaupt Machine Learning?" | Eignungsprüfung (`v2_eignung`) **plus** die Selbstkorrektur aus B-41: Die diagnostizierte Nichtlinearität hält out-of-sample nicht. Ex ante vertretbar, ex post relativiert — das ist die ehrlichere und stärkere Antwort |
| „Hätten Sie es nicht anders machen können? Ist das nicht Overkill?" | Genau das ist das Ergebnis (UF4): Die Spezifikation bewegt bis zu 146,9 RMSE, die Verfahrenswahl 2,5. Der Mehraufwand lohnt sich hier nicht — belegt, nicht behauptet |
| „Wie stellen Sie sicher, dass der Vergleich fair ist?" | Auflage C, konstruktiv abgesichert: Fold-Spalten in den Dateien, eine Merkmalsliste, eine Aufteilungsfunktion. Dazu #42 und #43: Verlustfunktion und Expositionsbehandlung sind für alle Verfahren gleich — die beiden Ungleichbehandlungen wurden gesucht, gemessen und beseitigt (B-33) |
| „Warum ist Ihre Validierung leakage-frei?" | Stadtteil-Split, Hold-out einmalig, alles Preprocessing in der Pipeline je Fold — und B-2: Der naheliegende Fehler, das Hold-out mitrotieren zu lassen, ist gefunden und behoben worden (R-12) |
| „Sie haben nur 35 Stadtteile — wie belastbar sind Ihre Ergebnisse?" | R-5, offen benannt. 29 unabhängige Entwicklungseinheiten, `std_wiederholungen` statt `std_folds`, Primärtest auf 10 Mitteln, Konfidenzintervall enger als die wahre Unsicherheit |
| „Ihre sozioökonomischen Merkmale ändern sich nur jährlich. Was sagt Ihr Modell dann eigentlich vorher?" | R-6: das **Niveau** eines unbekannten Stadtteils, nicht seine Monatsdynamik. Das ist die Forschungsfrage, nicht ein Mangel |
| „Ist Ridge bei Zähldaten überhaupt das richtige Modell?" | Ridge rechnet auf `log(1+y)` und modelliert seit #43 die Rate — es bekommt die multiplikative Struktur damit ebenso wie das Poisson-GLM. Und empirisch: Ridge ist bei nicht unterscheidbarer Güte 130-mal schneller als XGBoost |
| *(neu)* „Wozu ist das gut? Sie sagen Stadtteile vorher, die Sie alle kennen." | R-22. Die Arbeit ist eine Erklärungsstudie, die Prognosegüte als **Maßstab** benutzt, weil out-of-sample strenger ist als In-sample-Signifikanz. Die eigene Einsatzhistorie ist bewusst kein Merkmal (#29) — sonst misst man die Trägheit der Zeitreihe statt den Beitrag der Strukturfaktoren. Übertragungsnutzen: Städte ohne Einsatzhistorie je Stadtteil |
| *(neu)* „Ihr wichtigstes Merkmal misst doch nur, wo die Polizei hinfährt." | R-18. Zugestanden. Registrierte Kriminalität ist Tatgeschehen × Anzeigebereitschaft × Kontrollintensität (Lum & Isaac 2016). Die Aussage lautet deshalb „registrierte Kriminalitätsbelastung ist der stärkste verfügbare Indikator", nicht „Kriminalität erklärt Feuerwehreinsätze" |
| *(neu)* „Sie verteilen Ressourcen nach Armut. Ist das nicht Redlining?" | R-20. Deployment ist ausdrücklich abgegrenzt — als ethische Grenze, nicht nur als Umfangsentscheidung. Dazu der Unterschied zur Polizeiarbeit: Ein zusätzlicher Löschzug nützt einem Stadtteil, zusätzliche Streifen belasten ihn |
| *(neu)* „Ist Ihr Modell für arme Stadtteile schlechter?" | R-24, **geprüft und verneint.** Absolut ja (Rho +0,35 bis +0,60), relativ zum Einsatzniveau nein (kein Wert auf 5 % signifikant). Der absolute Fehler ist größer, weil dort mehr passiert |
| *(neu)* „Gilt Ihr Befund auch für einzelne Haushalte?" | R-21. Nein. Ökologischer Fehlschluss (Robinson 1950); die Ergebnisse gelten auf Stadtteilebene und nur dort. Zusätzlich hängen sie am Gebietszuschnitt (MAUP, Openshaw 1984) |
| *(neu)* „Warum nach Stadtteil aufteilen und nicht zufällig?" | Roberts et al. (2017): Block-Kreuzvalidierung ist bei Abhängigkeitsstrukturen die empfohlene Praxis, zufälliges k-Fold liefert zu optimistische Schätzungen. Der Split ist Standard, kein Sonderweg |
| *(zu erwarten)* „Sie haben die Spezifikation im Verlauf geändert." | Ja — das ist der CRISP-DM-Kreislauf, und ohne ihn gäbe es die Antwort auf UF4 nicht. Beide Spezifikationen sind berichtet (§5.5), das Hold-out war nie berührt, und die Änderung machte die Regressionslatte **härter** statt weicher (R-13) |

---

## 6. Was daraus für die Verschriftlichung folgt

1. **Keine Rangfolge zwischen den Verfahren**, außer bei der einen trennbaren
   Paarung (Ridge gegen XGBoost auf der Rate). Überall sonst „nicht
   unterscheidbar" mit mittlerer Differenz, Konfidenzintervall und gewonnenen
   Läufen (#34, R-1).
2. **Je Zielgröße getrennt berichten.** Bei der Rate ist **RMSE bzw. MAE das
   Hauptmaß und R² nur nachrichtlich** — R² misst gegen den Mittelwert der
   Testdaten, und die Rate streut zwischen den Stadtteilen um den Faktor 32.
   Bei `anzahl_einsaetze` bleibt R² aussagekräftig.
3. **Beide Auswertungen des Strukturstrangs berichten**, keine unterschlagen.
   Ein Bericht, der nur die Kreuzvalidierung zeigt, behauptet einen Vorteil,
   den die Schlussbewertung nicht trägt (R-2).
4. **Die Extrapolation dokumentieren, nicht als Erklärung verwenden.** Der Satz
   lautet: geprüft und nicht bestätigt (R-3).
5. **Den Hold-out als Einzelmessung kennzeichnen**, seine Absolutwerte nicht mit
   den CV-Werten vergleichen, die Parameter-Asymmetrie benennen (R-4).
6. **Die Testkonstruktion offenlegen:** zwei Familien, Holm über 6, ein Test
   unkorrigiert, Primärtest auf 10 Mitteln, Sensitivität über 50 gekennzeichnet
   (R-10, R-11).
7. **Die Kernzahl in die Reproduzierbarkeitsangabe**, nicht nur den
   `random_state` (R-15, R-16).
8. **Die Spezifikation als Iteration schreiben, nicht als Korrektur.** #42 und
   #43 haben zwei Ungleichbehandlungen beseitigt; die Ablation beziffert, was
   sie wert waren. Das ist der Beitrag, nicht das Eingeständnis (R-13).
9. **R-14 aktiv ansprechen, nicht abwarten.** Ein Kommunikations-, kein
   Rechenproblem. Ein im Kolloquium gestellter Punkt ist eine Frage; ein
   vorher genannter ist eine Angabe.

10. **Die Zweckfrage vorne klären, nicht hinten.** Die Umdeutung von
    „Prognose" zu „Erklärungsstudie mit Prognosegüte als Maßstab" gehört in
    Kapitel 3, nicht in die Limitationen. Wer sie erst hinten liest, hat
    vorne schon eine andere Arbeit erwartet (R-22).
11. **Den Kriminalitätsindex sprachlich zurücknehmen.** Überall
    „registrierte Kriminalitätsbelastung" statt „Kriminalität". Das ist keine
    Abschwächung, sondern die korrekte Bezeichnung der Messgröße (R-18).
12. **Die Deployment-Abgrenzung als ethische Grenze formulieren**, nicht nur
    als Umfangsentscheidung. Zwei Sätze, die aus einer Lücke eine Position
    machen (R-20).
13. **Die Fairnessprüfung berichten, auch weil sie negativ ausfällt.** Ein
    gemessener und verneinter Verdacht trägt mehr als ein ungeprüfter (R-24).
14. **Keine Aussage über Personen.** Der Satz „arme Menschen verursachen mehr
    Einsätze" folgt aus nichts in dieser Arbeit (R-21).

> **Der Leitsatz aus dem Gutachten, an dem dieses Register hängt:** Probleme zu
> *erkennen* reicht nicht — bewertet wird, ob sie methodisch *gelöst* wurden.
> Von den ursprünglich zwölf Risiken sind fünf gelöst, drei eingetreten und als
> Ergebnis berichtbar, vier offen benannt; die sieben am 17.08. ergänzten
> Risiken zu Nutzen, Validität und Ethik sind einer geprüft und verneint
> (R-24), sechs offen benannt. Zwei Ungleichbehandlungen zwischen
> Baseline und Vergleichsverfahren wurden gesucht, gemessen und beseitigt; drei
> plausible Erklärungen (B-31, B-34, B-39) wurden geprüft und verworfen, bevor
> sie in die Arbeit kamen. Das ist die Erzählung, die dieses Register trägt.
