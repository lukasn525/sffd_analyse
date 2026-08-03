# Kapitel 5 „Data Preparation" — Gliederung und Inhalte

> **Lebensdauer dieses Dokuments:** Schreibvorlage. Sie sagt, *was* wohin gehört
> und *wie* es zu argumentieren ist — die konkreten Zahlen holst du beim
> Schreiben aus `03_STAND.md`. Hier stehen bewusst keine, damit die beiden
> Stellen nicht auseinanderlaufen.

Arbeitsvorlage für die Neufassung. Der bestehende Text in `main.tex` beschreibt
eine Pipeline, die es nicht mehr gibt — **er ist vollständig neu zu schreiben**,
nicht zu überarbeiten. Abschnitt 5 dieses Dokuments listet auf, welche Aussagen
konkret falsch geworden sind.

---

## 1. Die vier Unterkapitel

| | Titel | Fokus | Quelle im Code |
|---|---|---|---|
| **5.1** | Datenauswahl und Bereinigung | Was kommt rein, was fliegt raus | `prep/s1_daten.py`, Teile A–B |
| **5.2** | Zusammenführung der vier Datenquellen | Joins — **knapp halten** | `prep/s1_daten.py`, Teile C–G |
| **5.3** | Konstruktion der Merkmale und Zielgrößen | **Variablen und Lags** | `prep/s2_datensaetze.py`, Teile B–C |
| **5.4** | Analyserahmen und Baselines | Split, Datentypen, Referenzwerte | `prep/s2_datensaetze.py` Teil A, `prep/s3_baselines.py` |

Vier Unterkapitel, wie vorgegeben. Die Gliederung folgt exakt der Reihenfolge
der Pipeline — wer das Kapitel liest, kann `python prep/build.py` mitverfolgen.

**Gewichtung:** 5.3 und 5.4 tragen das Kapitel. 5.2 bleibt bewusst **kurz und
ohne Code** — die Joins sind Handwerk, das man beschreibt, nicht vorführt. Die
Code-Listings konzentrieren sich auf die beiden Stellen, an denen tatsächlich
etwas entschieden wird: die **Berechnung der Merkmale** und den **Split**.

**Kein Blindabsatz zwischen 5 und 5.1** (Auflage Schröter, 27.07.2026): Die
Kapiteleinleitung trägt eigenen Inhalt und wiederholt nicht, was in den
Unterkapiteln steht.

---

## 2. Inhalt je Unterkapitel

### Kapiteleinleitung (vor 5.1)

Drei Dinge, die nirgendwo sonst stehen:

1. **Verortung in CRISP-DM** und die Festlegung der Analyseeinheit
   **Stadtteil × Monat** — mit der Begründung, warum nicht Einzeleinsatz
   (Prädiktoren variieren nur auf Stadtteilebene) und nicht Census-Tract × Jahr
   (Einsatzdaten tragen keine Tract-Zuordnung, Jahresebene tötet die Saisonalität).
2. **Der Grundsatz, dem alle Schritte folgen:** Jeder Prädiktor muss zum
   Prognosezeitpunkt tatsächlich verfügbar gewesen sein. Beleg über
   Bergmeir & Benítez (2012).
3. **Der Überblick:** vier Quellen, ein Befehl, zwei Datensätze auf derselben
   Analyseeinheit — die eine misst die *Menge* der Einsatzlast, die andere ihre
   *Zusammensetzung*.

### 5.1 Datenauswahl und Bereinigung

**Strikte Spaltenauswahl.** Von 23 verfügbaren Einsatzspalten bleiben 6.
Ausgeschlossen werden Sachschaden, Löschfahrzeuge, Löschkräfte,
Rettungsdiensteinheiten, Alarmstufe, zivile Tote und Verletzte sowie die
Flammenausbreitung — sie stehen erst *nach* dem Einsatz fest und wären Leakage
im engeren Sinn. Das ist ein starkes Argument für die Arbeit: Was gar nicht erst
in den Datensatz kommt, kann auch nicht versehentlich in ein Modell geraten.

**Bereinigung in drei Schritten.** 269 mehrfach gemeldete Einsatznummern
(0,04 %) entfernt → 719.989 Einsätze. Ausrückzeit auf 0–60 Minuten begrenzt
(~1,7 % entfallen). Baujahre außerhalb 1800–2025 als fehlend markiert.

**Ausschluss von Analyseeinheiten** — hier muss der Text neu geschrieben werden,
er nennt bisher nur McLaren Park:

| Ausgeschlossen | Grund |
|---|---|
| Golden Gate Park, Lincoln Park, McLaren Park | Park-/Institutionsgebiete mit 45 bis 507 Einwohnern. Jede Pro-Kopf-Größe wird dort beliebig groß, weil der Nenner gegen null geht. Median der übrigen Stadtteile: 14.435 Einwohner |
| Treasure Island, Lakeshore | in **keinem** ACS-Jahrgang enthalten |
| Mission Bay | erst ab ACS 2021 — ein Zutritt mitten in der Zeitreihe würde ein unbalanciertes Panel erzeugen |

41 → **35 Stadtteile**. Zu betonen ist der Unterschied zwischen zeilenweisem
`dropna` und dem Ausschluss ganzer Einheiten: Ersteres hätte Mission Bay mitten
in der Zeitreihe auftauchen lassen, die Folds hätten unterschiedlich viele
Stadtteile enthalten.

**Analysezeitraum 2015-01 bis 2025-12**, hart in `config.py` fixiert und nicht
aus den Daten abgeleitet — sonst verschiebt sich die Analyse bei jedem neuen
Download und ein angebrochener Randmonat kann unbemerkt ins Testfenster geraten.

*Kein Code-Listing.* Deduplikation und Plausibilitätsfilter sind in zwei Sätzen
erklärt; ein Listing brächte keinen Erkenntnisgewinn.

### 5.2 Zusammenführung der vier Datenquellen — bewusst knapp

Das Grundproblem zuerst benennen: vier Quellen, drei verschiedene Raumbezüge,
zwei verschiedene Zeitraster. Dann die drei Joins einzeln.

**Join A — ACS (sozioökonomisch).** Zwei Stufen. Erst Census-Tract → Stadtteil
über den Crosswalk, wobei Mediane bevölkerungsgewichtet und Zähler summiert
werden. Dann der **zeitbewusste Join**: Jeder Einsatz erhält den letzten
Jahrgang, der zum Prognosezeitpunkt *tatsächlich veröffentlicht* war, also
`acs_jahr ≤ Einsatzjahr − 1`. Die Publikationsverzögerung von rund einem Jahr
ist der Punkt, den der bisherige Text unterschlägt: Ohne sie bekäme ein Einsatz
aus 2023 den Jahrgang 2023, der erst Ende 2024 erschien.

Als Limitation: **Trefferquoten je Jahrgang** — 2009 nur 63,1 %, 2014 und 2019
je 79,7 %, 2021 und 2023 je 99,2 %. Ursache sind sich ändernde
Census-Tract-Grenzen bei einem Crosswalk von 2020. Für die Hauptanalyse ab 2015
folgenlos, gehört aber benannt.

**Join B — Kriminalität.** Zwei SFPD-Quellen mit einem Systembruch: Im Mai 2018
wurde von CABLE auf das Crime Data Warehouse umgestellt. Die ältere Quelle
(2014–2017) hat **keine Stadtteilspalte**, dafür Koordinaten → Spatial Join
gegen dieselbe Neighborhood-Geometrie wie bei Land Use. Dass beide Spatial Joins
dieselbe Geometrie nutzen, ist kein Detail: Sonst bezögen sich Kriminalitäts-
und Baumerkmale desselben Stadtteils auf unterschiedliche Flächen.

**Join C — Land Use (baulich).** 154.544 Parzellen, Zuordnung über den
Parzellen-Mittelpunkt, Match-Rate 99,5 %. Snapshot 2020, einziger verfügbarer
Jahrgang → geht als zeitkonstantes Strukturmerkmal ein.

**Join-Hygiene.** Der Abschnitt, der die Arbeit von einer Projektarbeit
unterscheidet: Schlüsseleindeutigkeit vor jedem Merge, `validate=`-Parameter,
Match-Quoten geloggt, Zeilenzahl vor und nach jedem Join gegen den
Erwartungswert geprüft. Ein kartesisches Produkt bliebe sonst unbemerkt und
würde die Einsatzzählungen vervielfachen.

*Kein Code-Listing.* Die Join-Logik wird im Fließtext beschrieben; die
Snapshot-Regel `acs_jahr ≤ Einsatzjahr − 1` steht als Formel im Text, nicht als
Quellcode.

*Ergebnis:* `einsaetze.parquet`, 719.989 × 50, ein Einsatz je Zeile.

### 5.3 Konstruktion der Merkmale und Zielgrößen ← Schwerpunkt

**Quoten aus Zählvariablen.** Armuts-, Akademiker-, Leerstands-, Altbau-,
Wohnnutzungs- und Risikogewerbe-Anteil. Nenner ≤ 0 ergibt NaN statt Division
durch Null — fehlende Grundgesamtheiten führen zu einem explizit fehlenden Wert
statt zu einem verzerrten.

**Der Kriminalitätsindex als Location Quotient** — mit Formel:

```
rate(i,t)     = Delikte(i, Fenster endend in t−1) / Einwohner(i)
rate(Stadt,t) = Delikte(Stadt, gleiches Fenster) / Einwohner(Stadt)
index(i,t)    = rate(i,t) / rate(Stadt,t)
```

Lesart: 1,0 = Belastung wie im Stadtdurchschnitt desselben Monats. Zwei
Begründungen gehören dazu. Erstens **warum relativ**: Der Systembruch von 2018
verändert das stadtweite Niveau; ein multiplikativer Sprung wirkt auf Zähler und
Nenner gleich und kürzt sich heraus. Verbleibende Limitation: Eine Verschiebung
in der *Zusammensetzung* der erfassten Delikte, die einzelne Stadtteile stärker
trifft, kürzt sich **nicht** heraus. Zweitens **warum das Fenster im Vormonat
endet**: sonst erklärt Kriminalität im Monat t die Einsätze im Monat t — das
wäre Beschreibung, keine Prognose.

**Exposure.** `log_bevoelkerung` statt roher Einwohnerzahl. Die Begründung im
bisherigen Text ist **falsch** und muss ersetzt werden: Nicht die Armutsquote
wechselt das Vorzeichen (+0,49 absolut, +0,46 pro Kopf), sondern die
**Bevölkerung** (+0,20 absolut, −0,42 pro Kopf). Ohne diese Kontrolle sagt das
Modell im Kern die Stadtteilgröße vorher.

**Aggregation auf Stadtteil × Monat.** Vollständiges Raster, einsatzfreie Monate
als echte Nullen. Nur `ffill`, kein `bfill` — Rückwärtsfüllen würde fehlende
Werte mit Zukunftswerten imputieren.

**Saison** als sin/cos statt Monat 1–12: Dezember und Januar hätten sonst den
Abstand 11, obwohl sie benachbart sind, und ein linearer Koeffizient könnte ein
U-förmiges Jahresmuster grundsätzlich nicht abbilden.

**Lags** — und hier die wichtigste inhaltliche Änderung gegenüber dem alten
Text: `lag_1`, `lag_12`, `rolling_mean_3` werden gebildet, `shift(1)` steht vor
`rolling(3)`, und der Lag-Vorlauf von 12 Monaten sorgt dafür, dass `lag_12`
schon für Januar 2015 definiert ist. **Sie sind aber kein Modellmerkmal mehr.**
Unter dem Stadtteil-Split (5.4) wäre `lag_1` die eigene Vergangenheit des
Teststadtteils — dann erklärt wieder seine Historie das Ergebnis statt seiner
Struktur. Sie bleiben im Datensatz für eine Nebenbemerkung zur zeitlichen
Prognose.

**Zielgrößen.** Drei, die modelliert werden:

| Zielgröße | Typ | Strang |
|---|---|---|
| `anzahl_einsaetze` | Zähldaten | Menge |
| `einsaetze_je_1000_ew` | stetig | Menge |
| `dominante_einsatzart` | 4 Klassen | Struktur |

Die vier `anteil_*`-Spalten sind Rechenbasis der Zielgröße und Deskription,
**keine eigene Zielgröße** — ihre Vorhersage wäre Regression, der
Klassifikationsstrang soll die *Art* vorhersagen (R8).

Kennzahlen und Klassenverteilung: `03_STAND.md`, Abschnitt 2. Der hohe
**Dispersionsindex** begründet die Negative-Binomial-Regression als
Count-Baseline; Poisson mit Var = Mean ist deutlich verletzt.

Zur **Klassifikation** ist die Begründung neu zu schreiben — sie ist der stärkste
methodische Abschnitt des Kapitels. Auf Einzeleinsatz-Ebene tragen alle Einsätze
eines Stadtteil-Monats identische Strukturmerkmale: Die 350.481 Einzeleinsätze
enthielten nur 4.619 verschiedene Merkmalsprofile. Ein *perfektes* Modell auf den
Strukturmerkmalen hätte dort kaum mehr erreicht als bloßes Raten (Zahlen und
Herleitung in Decision Log #29). Auf Stadtteilebene ist dieselbe Frage
beantwortbar. Die Zielgröße bleibt dabei eine **echte Klasse** (`argmax` über
die vier NFIRS-Gruppen), keine künstliche Einteilung einer stetigen Größe — das
ist wichtig, weil Dichotomisierung stetiger Zielgrößen methodisch als Fehler
gilt (Altman & Royston 2006).

*Code-Listings 1 und 2:* `safe_ratio` mit der Quotenberechnung, und die
Lag-Bildung mit `shift(1)` vor `rolling(3)`. Beide zeigen eine Entscheidung, die
man im Ergebnis nicht mehr sieht: einen NaN statt einer Division durch Null, und
einen Monat Versatz, der über Leakage-Freiheit entscheidet.

### 5.4 Analyserahmen und Baselines

**Der Stadtteil-Split.** Er gehört in Kapitel 5 und nicht erst in Kapitel 6,
weil die Aufteilung als Spalten `fold` und `ist_holdout` **in den Dateien
steht** — sie ist eine Eigenschaft des Datensatzes, nicht der Algorithmen. Damit
ist die Fairness-Regel konstruktiv abgesichert statt nur behauptet.

Aufbau: fünf Folds plus Hold-out, jeder Stadtteil genau einmal Testfall, mit
allen Monaten des Zeitraums (genaue Aufteilung in `03_STAND.md`, Abschnitt 3).
Die Begründung ist die zentrale methodische Aussage der Arbeit: Ein Zeitschnitt
prüft die Forschungsfrage nicht, weil dort jeder Stadtteil in Training *und*
Test steht und das Modell sein Niveau bereits kennt.

**Doppelte Stratifizierung.** Sortiert wird nach der Anzahl brand-dominierter
Monate, bei Gleichstand nach Bevölkerung. Grund: Von 70 brand-dominierten
Monaten liegen 35 allein in Bayview Hunters Point; ohne diese Stratifizierung
hatte in drei von vier Aufteilungen ein Fold **null** Brand-Testfälle. Jetzt
sind es 13, 9, 6, 3 und 2. Kein Leakage — die Gruppenbildung gibt dem Modell
keine Information, wie bei `StratifiedGroupKFold`.

**Modelltauglichkeit.** Alle Merkmale `float64`, keine fehlenden Werte. Zu
erwähnen ist die `Int64`-Falle: Eine einzige nullable Spalte macht aus
`X.to_numpy()` ein object-Array; scikit-learn fängt das still ab, XGBoost lehnt
es ab — der Fehler träte erst beim dritten der drei Verfahren auf.

*Code-Listing 3:* die Fold-Zuteilung. Vier Zeilen Sortierlogik, an denen hängt,
ob der Verfahrensvergleich überhaupt aussagekräftig wird.

**Baselines** (Auflage Schröter, 27.07.2026: gehören in die Data Preparation).
Festlegung in Decision Log #32, **Werte in `03_STAND.md`, Abschnitt 4** — von
dort abschreiben, nicht aus älteren Fassungen.

Vier Punkte gehören unbedingt in den Text:

1. **Negative Binomial für die Regression, und warum sie ein starker Gegner
   ist:** dieselben zwölf Merkmale, dieselben Zeilen, dieselben Folds, über den
   Log-Link auf der Originalskala nichtlinear, verteilungsgerecht für Zähldaten.
   Kein Strohmann.
2. **Was sie nicht kann, ist die eigentliche Pointe:** Wechselwirkungen zwischen
   Merkmalen. Genau die finden Random Forest und XGBoost konstruktionsbedingt.
   Damit wird der Vergleich zum Beleg — schlagen die Baumverfahren sie, existieren
   solche Wechselwirkungen; schlagen sie sie nicht, reicht die einfachere Struktur.
   Das beantwortet zugleich die Auflage zur nichtlinearen Baseline.
3. **Negative R² sind korrekt** und aussagekräftig — wer für einen unbekannten
   Stadtteil den Gesamtdurchschnitt vorhersagt, liegt schlechter als dessen
   eigener Mittelwert. Genau diese Lücke sollen die Strukturmerkmale schließen.
   Der Gesamtmittelwert läuft deshalb als Nullmarke mit, nicht als Gegner.
4. **Offen zu benennen:** In der Klassifikation gibt es keine Entsprechung — eine
   Zahl kann keine von vier ungeordneten Kategorien vorhersagen. Dort bleibt die
   Mehrheitsklasse die einzige Referenz, die Latte liegt also niedriger als in
   der Regression. Ebenfalls hierher: Die naive Vormonats-Baseline aus dem alten
   Text entfällt, weil sie die eigene Vergangenheit des Teststadtteils nutzen würde.

**Steckbrief** am Kapitelende: Zahlen aus `03_STAND.md`, Abschnitt 2 und 3
übernehmen — Analyseeinheit, Zeitraum, Beobachtungen je Datensatz, Merkmalszahl,
Aufteilung, Ausschlüsse und die Zahl der automatisierten Prüfungen.

---

## 3. Code-Listings — Zuordnung alt → neu

Alle bisherigen Listings verweisen auf Dateien, die es nicht mehr gibt.

| Bisher | Neu |
|---|---|
| `pipeline/02_join.py` | `prep/s1_daten.py` |
| `pipeline/03_features.py` | `prep/s1_daten.py`, Teil G |
| `modellierung/aggregation.py` | `prep/s2_datensaetze.py`, `aggregiere` |
| `modellierung/demo_modellierung.py` | `prep/s2_datensaetze.py`, `baue_regression` |
| — | `prep/s3_baselines.py` (neu, für 5.4) |

Vorschlag: **drei Listings**, konzentriert auf Berechnungen und Split.

| Nr. | Abschnitt | Inhalt | Quelle |
|---|---|---|---|
| 1 | 5.3 | `safe_ratio` und die Quotenberechnung | `prep/s1_daten.py`, `berechne_quoten` |
| 2 | 5.3 | Lag-Bildung: `shift` **vor** `rolling` | `prep/s2_datensaetze.py`, `baue_regression` |
| 3 | 5.4 | Fold-Zuteilung mit doppelter Stratifizierung | `prep/s2_datensaetze.py`, `ergaenze_aufteilung` |

**5.1 und 5.2 bekommen keinen Code.** Deduplikation und Filter sind in zwei
Sätzen erklärt, die Joins ebenso — dafür lohnt kein Listing. Das hält die
Vorgabe „mindestens 3/4 Fließtext" ein und lenkt die Aufmerksamkeit auf die
Stellen, an denen wirklich etwas entschieden wird.

---

## 4. Was aus dem alten Text übernommen werden kann

Wenig, aber nicht nichts:

- Die **Abwägung der Aggregationsebene** (Einzeleinsatz vs. Tract × Jahr vs.
  Stadtteil × Monat) — inhaltlich weiterhin gültig, gehört in die
  Kapiteleinleitung
- Die Begründung der **zyklischen Saison-Kodierung**
- Die Beschreibung der **Deduplikation** und des Antwortzeitfilters
- Die Erklärung von **`safe_ratio`**
- Der Grundsatz **Leakage-Vermeidung** mit dem Beleg Bergmeir & Benítez

Die zugehörigen Exposé-Übernahmen mit KI-Verzeichnis-Fußnoten müssen mitwandern.

---

## 5. Was im alten Text falsch geworden ist

Diese Aussagen stehen aktuell in `main.tex` und sind sachlich nicht mehr
zutreffend. Sie dürfen nicht stehen bleiben.

| Zeile | Aussage im Text | Richtig |
|---|---|---|
| ~1032 | „39 der 41 Stadtteile verbleiben" | **35** |
| ~1258 | „5.103 Beobachtungen (39 × 133)" | **4.620 (35 × 132)** |
| ~1257 | „Hauptanalyse-Zeitraum 2014–2026" | **2015-01 bis 2025-12** |
| ~1113 | „Dispersionsindex 61", „Mittelwert 63,4; Median 45" | **62,8**; Mittel **75,9**, Median **53** |
| ~1038 | McLaren Park als einziger Ausschluss | **drei Parkgebiete + drei ohne ACS-Abdeckung** |
| ~995 | ACS-Join „Erhebungsjahr ≤ Einsatzjahr" | zusätzlich **Publikationsversatz**: `≤ Einsatzjahr − 1` |
| ~1005 | „Kriminalitätsmerkmale bis zum Vorjahr kumuliert" | **relativer Index**, rollierendes 12-Monats-Fenster endend im Vormonat |
| Tabelle | „Anteil Gewaltdelikte", „Anteil Eigentumsdelikte" | gibt es nicht mehr → **`log_kriminalitaetsindex`** |
| Tabelle | „Gesamtbevölkerung" als Prädiktor | **`log_bevoelkerung`** |
| ~1116 | Klassifikation „Einzeleinsatz, Brand vs. Nicht-Brand, 13,1/86,9 %" | **Stadtteil × Monat**, dominante Einsatzart (4 Klassen) + vier Anteile |
| ~1218 | „Set S und Set S+L" | nur noch **Set S**; Lags sind kein Modellmerkmal |
| ~1180 | Exposure-Begründung über die **Armutsquote** | über die **Bevölkerung** (+0,20 / −0,42) |
| ~1140 | „155.395 Parzellen" | **154.544** |
| — | Validierung fehlt in Kapitel 5 | **Stadtteil-Split gehört hierher** |
| — | Baselines fehlen in Kapitel 5 | **Auflage Schröter 27.07.** |

Ebenfalls zu streichen: der TODO-Kommentar „Zahlen nach finalem Trainingslauf
ggf. aktualisieren (Stand: Demo 2026-07-18)" und die Sensitivitätsanalyse „voller
Zeitraum ohne Akademikerquote" — der Zeitraum ist jetzt fest.
