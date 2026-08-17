# Risikoregister der Modellierung

> **Lebensdauer:** ändert sich, wenn ein Risiko eintritt, entschärft wird oder
> wegfällt. Neu gefasst am 07.08.2026, **auf den finalen Lauf vom 16.08.2026
> nachgezogen** (R-1 und R-2). Ursprünglich nach dem Lauf von
> `m02`, `m03`, `m04`, `m05` und nach den Entscheidungen #37 bis #46.
>
> **Diese Datei enthält keine eigenen Ergebniszahlen.** Sie zitiert
> `03_STAND.md` (Zahlen) und `07_BEFUNDE.md` (Herleitungen). Wo hier eine Zahl
> steht, steht sie als Beleg für die Einstufung eines Risikos, nicht als
> Ergebnis.

**Die Nummern R-1 bis R-16 sind Zitierschlüssel und werden nicht neu vergeben.**
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
macht die **Regressionslatte härter** (37,27 → 33,98 RMSE) und die
Klassifikationslatte weicher (0,314 → 0,297). Wer sein Ergebnis sucht, ändert
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
Regressionslatte **steigt** von 37,27 auf 33,98 RMSE, die Klassifikationslatte
**sinkt** von 0,314 auf 0,297 Macro-F1. Die Änderung hilft im einen Strang und
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

| Auswertung | Mehrheitsklasse | Logit (Stufe 2) | Random Forest | XGBoost |
|---|---|---|---|---|
| Kreuzvalidierung, 50 Läufe | 0,223 | 0,297 | **0,3278 ± 0,0121** | **0,3322 ± 0,0125** |
| Hold-out, einmalig | 0,208 | **0,327** | 0,257 | 0,260 |
| Abstand Training zu Test | – | – | **+0,244** | **+0,170** |

**Neu am 16.08.2026: der Widerspruch ist erklärt.** Die dritte Zeile ist der
Überanpassungsnachweis nach #51. Wären die sechs Hold-out-Stadtteile schlicht
schwerer, müsste **jedes** Modell einbrechen. Die Baseline wird dort aber
**besser** (0,297 → 0,327) und die Mehrheitsklasse bleibt etwa gleich
(0,223 → 0,208); nur die beiden Baumverfahren stürzen um 0,07 bzw. 0,07 ab —
und das, obwohl sie im Hold-out auf 29 statt 23 Stadtteilen trainieren, also
mit **mehr** Daten. Mehr Trainingsdaten und trotzdem schlechter: Das ist
Überanpassung, keine schwierige Testmenge (`07_BEFUNDE.md`, **B-46**).

**Drei Befunde, alle zu berichten:**

1. **In der Kreuzvalidierung schlagen beide Baumverfahren die Stufe-2-Baseline**
   — +0,0304 und +0,0371 Macro-F1, je **10 von 10** Wiederholungen, p = 0,002.
   Das ist der kleinste bei n = 10 erreichbare p-Wert; die Richtung ist
   eindeutig, nicht knapp.
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
gemessen (0,297). Gegen eine mit gleichem Budget getunte Fassung (0,314) fiele
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
Forest 0,229–0,404, XGBoost 0,230–0,421). Als Einzelmessung zu kennzeichnen und
**nicht** als Widerlegung des Kreuzvalidierungsergebnisses zu lesen.

**Nicht mit den CV-Werten vergleichen.** Der Hold-out ist eine andere,
leichtere Aufgabe: Extrapolationsanteil 7,6 % gegen 34,6 %, Training auf 29
statt 23 Stadtteilen. Die absoluten Werte fallen deshalb günstiger aus — im
Mengenstrang durchgehend.

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

4.620 Zeilen klingen komfortabel, es sind **35 Querschnittseinheiten × 132
Monate**, davon 29 in der Entwicklung und 6 im Hold-out. Gemeinsame Ursache von
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

**34,6 %** der Testzeilen über alle 50 Läufe liegen in mindestens einem Merkmal
außerhalb der Trainingsspanne; 33,7 % in Wiederholung 0 (Fold-Spanne 3,6 bis
57,4 %), 7,6 % im Hold-out. Unvermeidliche Folge des Stadtteil-Splits, begrenzt
die Generalisierbarkeit.

**Als Erklärung für Verfahrensunterschiede geprüft und widerlegt** (B-31) — und
unter der finalen Spezifikation erst recht gegenstandslos:

- Es gibt **keinen Rückstand mehr zu erklären**. Bei `anzahl_einsaetze` liegen
  Random Forest 0,90 RMSE **vor** Ridge, XGBoost 1,26 dahinter; bei der Rate
  Random Forest 0,49 davor, XGBoost 0,12 dahinter.
- Der Zusammenhang zwischen Extrapolationsanteil und RMSE ist bei **Ridge am
  stärksten** (ρ +0,302 / +0,309) und beim **Random Forest am schwächsten**
  (+0,108 / +0,162), bei XGBoost dazwischen (+0,247 / +0,316). Wäre
  Extrapolation der Hebel gegen die Bäume, müsste es umgekehrt sein. Beim
  Random Forest ist der Zusammenhang nicht einmal signifikant (p 0,456 / 0,261).

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
(B-41): Der RESET-Test der Eignungsprüfung lief auf 3.828 Zeilen, als wären sie
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
VIF-Rechnung: Die Strukturmerkmale variieren nur auf **319 Stadtteil-Jahren**,
nicht auf 3.828 Zeilen (B-18) — höchster VIF 12,29 bei
`median_haushaltseinkommen`, weshalb blockweise statt einzelmerkmalsweise
interpretiert wird.

Das ist zugleich die Antwort auf eine der erwarteten Kolloquiumsfragen (siehe
Abschnitt 5): Das Modell sagt das **Niveau** eines unbekannten Stadtteils
vorher, nicht seine Monatsdynamik. Genau das ist die Forschungsfrage.

### R-8 · ACS-Trefferquote 2009

63,1 % im Jahrgang 2009, gegenüber 99,2 % in 2021/23. Für die Hauptanalyse ab
2015 folgenlos, gehört in die Limitationen.

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

> **Der Leitsatz aus dem Gutachten, an dem dieses Register hängt:** Probleme zu
> *erkennen* reicht nicht — bewertet wird, ob sie methodisch *gelöst* wurden.
> Von den ursprünglich zwölf Risiken sind fünf gelöst, drei eingetreten und als
> Ergebnis berichtbar, vier offen benannt. Zwei Ungleichbehandlungen zwischen
> Baseline und Vergleichsverfahren wurden gesucht, gemessen und beseitigt; drei
> plausible Erklärungen (B-31, B-34, B-39) wurden geprüft und verworfen, bevor
> sie in die Arbeit kamen. Das ist die Erzählung, die dieses Register trägt.
