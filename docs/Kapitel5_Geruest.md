# Kapitel 5 „Data Preparation" — Stichpunkt-Gerüst

**24.08.2026.** Umfang **7 Seiten**, davon rund 1,4 Seiten Code. Vier
Unterabschnitte, Nummerierung unverändert.

Grundlage: `docs/03_STAND.md` (alle Zahlen), `docs/01_VORGABEN.md` (Auflagen),
`docs/02_ENTSCHEIDUNGEN.md` (#7 bis #45), `prep/`, `vorpruefung/`, `tests/`,
`Quellen/Chapman2000.pdf`, das Kapitel-4-Gerüst und der Kapitel-3/4-Abgleich.

Der Abgrenzungsblock dazu liegt getrennt in `docs/kapitel5_abgrenzung.tex`.
Dieses Gerüst setzt ihn voraus und wiederholt ihn nicht.

---

## 0. Die eine Annahme, die ich getroffen habe

**Bestätigt am 24.08.: Variante C.** Vier Unterabschnitte, Nummerierung bleibt
5.1 bis 5.4. Die Analyseeinheit wird in Kapitel 5 **nicht erneut begründet** —
das tut 3.1. In 5.1 erscheint sie nur noch faktisch im Schlusssatz von
Absatz 1 („35 Stadtteile über 132 Monate"); 5.3 greift sie beim Aggregieren
ausdrücklich auf.

Grund: Seit feststeht, dass **3.1 die Entscheidungseinheit bereits begründet**,
trifft Kapitel 5 hier keine Wahl mehr — es setzt eine getroffene Wahl um. Ein
eigener Abschnitt für eine Umsetzung wäre Gliederungslärm, und die
Nummerierung bleibt stabil (keine fünf Codeverweise nachzuziehen, die
Listing-Zuordnung gilt unverändert).

Wenn du doch Variante A willst: rein mechanisch, ich ziehe alles in einem Zug
nach.

**Eine Überschrift geändert:** „Zusammenführung der **vier** Datenquellen" →
„Zusammenführung der Datenquellen". Kapitel 4 führt in Tabelle 4.1 sieben
Quellen; nebeneinander sieht das nach einem Fehler aus.

---

## 1. Der Aufbau im Überblick

| | Abschnitt | CRISP-DM (Chapman 2000, S. 23–26) | Seiten | Code |
|---|---|---|---:|---:|
| 5.1 | Datenauswahl und Bereinigung | 3.1 Select · 3.2 Clean | 1,5 | – |
| 5.2 | Zusammenführung der Datenquellen | 3.4 Integrate | 1,0 | L1 |
| 5.3 | Konstruktion der Merkmale und Zielgrößen | 3.3 Construct | 2,0 | L2, L3 |
| 5.4 | Analyserahmen und Baselines | 3.1 Select (Splitting) · 3.5 Format | 2,5 | L4, L5, L6 |

Der Satz, der die Reihenfolge erklärt und in 5.2 gehört: *Integriert wird vor
dem Konstruieren, weil die Aufbereitung die Quellen zunächst auf Einsatzebene
zusammenführt und erst danach verdichtet.* Ungesagt liest sich die Abweichung
von der CRISP-DM-Aufzählung als Schludrigkeit.

**Die Absatzform, die das ganze Kapitel trägt** — drei Sätze, immer gleich:

> 1. Was wurde gemacht. 2. Warum — Beleg, Quelle oder gemessene Zahl.
> 3. Was dadurch nicht gilt.

Satz 3 ist der, der in Bachelorarbeiten fehlt. Er ist zugleich die billigste
Antwort auf das Bewertungskriterium „kritische Reflexion im Verlauf" (im
Gutachten 2,7). Wo Satz 3 nicht existiert, ist der Absatz meist keine
Entscheidung, sondern Mechanik — und dann gehört er gekürzt.

---

## 2. Abschnitt 5.1 — Datenauswahl und Bereinigung

*Rund 1,5 Seiten, fünf Absätze, kein Code-Listing, keine Abbildung.*

### Absatz 1 — Einstieg

**Fassung 3, 24.08. — gebaut wie der Einstieg von 5.2.** Fünf Schritte:
Befund aus Kapitel 4 mit Zahl → was hier passiert → warum → wo es hinterlegt
ist → Ergebnis mit Zahl. Das Wort „dieser Abschnitt" kommt nicht vor.

- Ausgangspunkt: **720.258 Einsatzmeldungen aus 41 Analysis Neighborhoods**
  (zitiert aus 4.1)
- Die Zweiteilung trägt den Absatz: *Nicht alles davon darf und nicht alles
  kann in die Analyse eingehen* — **darf** = Zulässigkeit (Grundsatz aus 2.6),
  **kann** = Abdeckung (Qualitätsbefunde aus 4.2)
- Beide als benannte Konstanten in `prep/config.py`, nicht über den
  Verarbeitungscode verteilt: an einem Ort nachlesbar, an einem Ort änderbar
- Schluss mit Zahl: **35 Stadtteile über 132 Monate**

> **Der Schlusssatz nennt NUR den Rahmen.** Eine Zwischenfassung lautete „ein
> bereinigter Bestand von 719.989 Einsatzmeldungen und ein Analyserahmen von
> 35 Stadtteilen über 132 Monate" — das war falsch. 719.989 ist der
> entdoppelte Gesamtbestand ab 2003 über alle 41 Stadtteile; im Rahmen liegen
> 350.481. Zwei Zahlen mit verschiedener Bezugsmenge nebeneinander.

> **719.989 gehört nach 5.2**, wo sie als Ergebnis der Zusammenführung steht.
> In 5.1 kommt sie nicht mehr vor.

> **Die Analyseeinheit** wird in 5.1 nicht mehr eigens angesprochen. Der
> Schlusssatz nennt sie faktisch („35 Stadtteile über 132 Monate"), der
> ausdrückliche Rückverweis auf 3.1 ist entfallen. Falls der rote Faden zu
> Kapitel 3 sichtbar bleiben soll: ein Halbsatz am Schluss von Absatz 1
> genügt — sonst greift 5.3 ihn beim Aggregieren auf.

### Absatz 2 — Spaltenauswahl: Leakage konstruktiv ausschließen

Der wichtigste Absatz von 5.1. **Positiv formulieren, nicht aufzählen** —
geändert am 24.08. nach Rückmeldung.

- Der tragende Satz zuerst: *In die Analysedatensätze gelangen ausschließlich
  Größen, die zum Zeitpunkt des Alarms bereits feststanden.*
- Begründung: no-time-machine requirement, `\footcite[559]{Kaufman2011}`
- **Höchstens drei Beispiele** für das, was dadurch entfällt — Sachschaden,
  eingesetzte Kräfte, Antwortzeit. Keine Liste aller zehn Ergebnisgrößen
- Warum sie gefährlich sind, nicht nur unzulässig: Sie hängen eng mit der
  Zielgröße zusammen; ein Modell mit ihnen erreichte hohe Gütewerte, ohne eine
  Prognose zu leisten
- Der konstruktive Kern: *Was nicht in die Datei gelangt, kann später nicht
  versehentlich in ein Modell geraten.* — learn-predict separation,
  `\footcite[560]{Kaufman2011}`
- Eine quantifizierte Zeile: **63** Felder im Feldverzeichnis, **15**
  angefordert, **10** davon gesperrt. Nachgezählt am 24.08.; die Angabe „23
  Einsatzspalten, davon bleiben 6" aus der Schreibanleitung ist **nicht
  reproduzierbar** und wird nicht verwendet
- Nachweis statt Behauptung: `test_keine_ergebnisvariablen`

### Absatz 3 — Bereinigung

- **269** doppelte Einsatznummern entfernt *(zitiert aus 4.2)*. **Ohne die
  Ergebniszahl** — 719.989 steht in 5.2
- Antwortzeit auf **0–60 Minuten** geprüft. **Keine Entfallquote nennen** — es
  entfällt keine Zeile. Stattdessen der Satz, der bisher fehlt: *Die Grenzen
  sind eine gesetzte Plausibilitätsannahme und folgen nicht aus den Daten.*
- Baujahre außerhalb 1800–2025 werden als **fehlend markiert**, nicht gelöscht
- Der abgrenzende Satz: *An der Zielgröße selbst wird nichts gekappt,
  gewinsorisiert oder getrimmt.* Bereinigung betrifft ausschließlich
  Platzhalterwerte der Quellen

### Absatz 4 — Ausschluss ganzer Analyseeinheiten

- **Sechs Stadtteile scheiden aus.** Die 41 steht jetzt in Absatz 1, die sechs
  Namen in 4.2 — hier nur der Vollzug und die zwei Gründe: drei Parkgebiete
  ohne nennenswerte Wohnbevölkerung (#19), drei ohne durchgängige
  ACS-Abdeckung (#15)
- **Der methodische Punkt**, und er ist mehr wert als die Namen: Unterschied
  zwischen zeilenweisem `dropna` und dem Ausschluss ganzer Einheiten. Ein
  zeilenweises `dropna` erzeugte ein **unbalanciertes Panel** — ein Stadtteil
  tritt mitten in der Zeitreihe hinzu, und Summen im Testfenster springen
  allein dadurch
- Ein rechteckiges Panel ist Voraussetzung für den fairen Fold-Vergleich in 5.4

### Absatz 5 — Analysezeitraum

- **2015-01 bis 2025-12**, 132 Monate, hart in `config.py` fixiert (#18)
- **Der Beginn ist eine Konsequenz, keine Setzung** — so formulieren:
  Die Regel `acs_jahr ≤ Einsatzjahr − 1` trifft auf die verfügbaren Jahrgänge
  2009, 2014, 2019, 2021, 2023. Ein Einsatz aus 2014 bekäme ACS 2009 — und der
  führt die Tabelle B15003 nicht, aus der die Akademikerquote stammt
  (Rückverweis 4.2). Erst ab 2015 greift ACS 2014
- Das Ende ist das letzte vollständige Kalenderjahr; die Warnschwelle bei 50 %
  des Median-Aufkommens **filtert nicht**, sie meldet
- Warum hart fixiert und nicht aus den Daten abgeleitet: sonst wandert die
  Analyse mit jedem Download, und ein angebrochener Randmonat gerät unbemerkt
  ins Testfenster (#12)

> **Abbildung A0 entfällt** — entschieden am 24.08. Kapitel 5 hat damit genau
> eine Abbildung, A2 in 5.4.

---

## 3. Abschnitt 5.2 — Zusammenführung der Datenquellen

*Rund 1,0 Seiten, fünf Absätze, ein Listing. Bewusst knapp.*
**Geschrieben und eingetippt am 24.08. — Text in `docs/kapitel5-2_zusammenfuehrung.tex`.**
Ohne Fußnote, mit Absicht: Alle Quellen sind in 4.1 belegt, der
Publikationsversatz in 4.2. Der Kriminalitätsschnitt ist auf Januar 2018
datiert wie in 4.1; der Systemwechsel im Mai 2018 gehört nach 5.3.

### Absatz 1 — Aufgabe und Ergebnis

- Rückverweis 4.1: drei Raumbezüge, zwei Zeitraster, keine gemeinsame
  Beobachtungsebene
- Ergebnis in einem Satz: `einsaetze.parquet`, **719.989 × 50**, ein Einsatz
  je Zeile
- Der Reihenfolgesatz aus Abschnitt 1 dieses Gerüsts

### Absatz 2 — ACS: zweistufig und zeitbewusst

- Stufe 1, räumlich: Census Tract → Stadtteil über den Crosswalk. **Mediane
  bevölkerungsgewichtet, Zähler summiert** — ein Median lässt sich nicht
  addieren
- Stufe 2, zeitlich: `acs_jahr ≤ Einsatzjahr − 1`, **als Formel im Fließtext**
- Warum: Ohne den Versatz bekäme ein Einsatz aus 2023 den Jahrgang 2023, der
  erst Ende 2024 erschien (Rückverweis 4.2, `\footcite[13]{CensusBureau2020}`)
- Limitation, drei Zahlen: Trefferquoten je Jahrgang **63,1 %** (2009),
  **79,7 %** (2014/2019), **99,2 %** (2021/2023) — Census-Tract-Grenzen ändern
  sich, der Crosswalk stammt von 2020
- **→ Listing L1** `acs_snapshot`

### Absatz 3 — Kriminalität: zwei Quellen, ein Systembruch

- Zwei SFPD-Quellen; die ältere hat **keine Stadtteilspalte**, dafür
  Koordinaten → Spatial Join gegen **dieselbe** Neighborhood-Geometrie wie bei
  Land Use
- Warum dieselbe: Sonst bezögen sich Kriminalitäts- und Baumerkmale desselben
  Stadtteils auf unterschiedliche Flächen
- **Die Datumsklarstellung nicht vergessen:** Die Altquelle endet im Mai 2018,
  die Nachfolgequelle beginnt im Januar 2018; geschnitten wurde am Beginn der
  Nachfolgequelle (`CRIME_HISTORISCH_BIS = 2018-01-01`). Ohne diesen Satz liest
  sich der Unterschied zu „Systembruch 05/2018" wie ein Fehler
- Absicherung: Die Match-Rate des Spatial Join muss ≥ 90 % sein, sonst bricht
  der Lauf ab

### Absatz 4 — Land Use

- **154.544** zugeordnete Parzellen, Zuordnung über den Parzellen-Mittelpunkt,
  Match-Rate **99,5 %**
- Snapshot 2020, einziger Jahrgang → zeitkonstantes Strukturmerkmal
  (Rückverweis 4.2: Zwischenvarianz 1,000)
- Ein Halbsatz: Der Datensatz ist inzwischen archiviert und wird nicht
  fortgeschrieben — das ist der Grund, nicht nur ein Formaldetail (→ 8.3)

### Absatz 5 — Join-Hygiene

Kurz, aber nicht weglassen — hier steht der Nachweis, nicht die Behauptung.

- Schlüsseleindeutigkeit vor jedem Merge, `validate`-Parameter, Match-Quoten,
  Zeilenzahl vor und nach dem Join gegen den Erwartungswert
- `assert len(base) == vorher` nach dem Crime-Join — Schutz vor einem
  kartesischen Produkt, das sonst geräuschlos Zeilen vervielfacht

---

## 4. Abschnitt 5.3 — Konstruktion der Merkmale und Zielgrößen

*Rund 2,0 Seiten, acht Absätze, zwei Listings. Erster Schwerpunkt.*

### Absatz 1 — Quoten aus Zählvariablen

- Sechs Anteilsgrößen: Armuts-, Akademiker-, Leerstands-, Altbau-,
  Wohnnutzungs-, Risikogewerbeanteil
- **Nenner ≤ 0 ergibt einen fehlenden Wert**, nicht null und keine Division
  durch null
- Ein Nenner ist begründungspflichtig: `yrbuilt_count` statt `parcel_count`
  beim Altbauanteil, weil 10,9 % der Parzellen kein Baujahr tragen
  (Rückverweis 4.2). Mit `parcel_count` als Nenner wäre der Anteil systematisch
  zu klein
- **→ Listing L2** `berechne_quoten`

### Absatz 2 — Kriminalitätsindex als Location Quotient

Der Absatz mit der Formel. Sie gehört in den Fließtext, nicht als Quellcode.

$$\text{rate}(i,t) = \frac{\text{Delikte}(i,\ \text{Fenster endend in } t-1)}{\text{Einwohner}(i)}$$

$$\text{index}(i,t) = \frac{\text{rate}(i,t)}{\text{rate}(\text{Stadt},t)}$$

- Lesart: **1,0 = Belastung wie im Stadtdurchschnitt desselben Monats**
- **Warum relativ:** Der SFPD-Systemwechsel verändert das stadtweite Niveau
  multiplikativ; ein multiplikativer Sprung wirkt auf Zähler und Nenner gleich
  und kürzt sich heraus
- **Verbleibende Limitation** (Satz 3 der Absatzform): Eine Verschiebung in der
  *Zusammensetzung* der erfassten Delikte, die einzelne Stadtteile stärker
  trifft, kürzt sich **nicht** heraus
- **Warum das Fenster im Vormonat endet:** Sonst erklärte die Kriminalität in
  *t* die Einsätze in *t* — Beschreibung statt Prognose
- Logarithmiert, damit 0 den Stadtdurchschnitt bezeichnet und der Index
  symmetrisch wirkt
- **→ Listing L3** `kriminalitaetsindex`
- **Quelle: `\footcite[268-269]{Brantingham1997}`** — der Location Quotient
  stammt aus der Regionalwissenschaft (Gl. 1, S. 268) und wird dort auf
  Kriminalität übertragen (Gl. 2, S. 269). Das ist der Unterschied zwischen
  „ich habe mir eine Kennzahl gebaut" und „ich verwende ein etabliertes Maß"

> **Am Volltext geprüft — eine Formulierung muss sitzen.** Brantinghams LQC
> hat als Bezugsgröße **alle Straftaten** des Gebiets und misst damit
> *Spezialisierung*; er kommt bewusst ohne Einwohnerzahl aus. Dein Index hat
> als Bezugsgröße die **Einwohnerzahl** und misst die relative *Belastung*.
> Algebraisch ist er trotzdem ein Location Quotient — Anteil der
> Stadt-Delikte im Stadtteil geteilt durch dessen Bevölkerungsanteil.
> Schreib also „folgt der Form des Location Quotient, verwendet aber die
> Wohnbevölkerung als Bezugsgröße", **nicht** „ist ein LQC". Ein Satz, und
> die Flanke ist zu.

### Absatz 3 — Exposition

- `log_bevoelkerung` statt der rohen Einwohnerzahl
- **Achtung, die frühere Begründung war falsch:** Nicht die Armutsquote
  wechselt zwischen absoluter Zahl und Rate das Vorzeichen (+0,49 / +0,46),
  sondern die **Bevölkerung** (+0,20 / −0,42). Genau das ist das Argument für
  eine Größenkontrolle
- Die Bevölkerung geht im Poisson-GLM zusätzlich als **Offset** ein (→ 5.4)

### Absatz 4 — Aggregation auf Stadtteil × Monat

- Vollständiges Raster: **auch ein Monat ohne Einsätze bekommt eine Zeile mit
  null**. Ohne das Raster verrutschten die Lags, weil ein ruhiger Monat
  stillschweigend fehlte
- Fehlende Strukturmerkmale werden **nur vorwärts** gefüllt. Kein `bfill`:
  Rückwärtsfüllen imputierte mit Zukunftswerten — Leakage (#10)
- Ergebnis **4.620 = 35 × 132**, lückenlos

### Absatz 5 — Saison

- `monat_sin` / `monat_cos` statt Monat 1–12: Dezember und Januar hätten sonst
  den Abstand 11 statt 1
- Rückverweis 4.2: Die Saisonterme sind die einzigen Merkmale, die
  ausschließlich innerhalb der Stadtteile variieren

### Absatz 6 — Verlaufsmerkmale, und warum sie kein Merkmal sind

Ein starker Reflexionsabsatz — ein verworfener Weg, der gemessen wurde.

- `lag_1`, `lag_12`, `rolling_mean_3`; **`shift(1)` vor `rolling(3)`**, damit
  der Wert für Monat *t* nur *t−1, t−2, t−3* verwendet
- Lag-Vorlauf von zwölf Monaten, damit `lag_12` schon für Januar 2015
  definiert ist; die Vorlaufmonate gehen ausschließlich über `shift()` ein,
  nie als eigene Zeile
- **Der Punkt:** Sie sind **kein Modellmerkmal**. Unter dem Stadtteil-Split
  (5.4) wäre `lag_1` die eigene Vergangenheit des Teststadtteils — das Modell
  erklärte die Historie statt der Struktur. Sie bleiben im Datensatz, weil
  Kapitel 4 sie deskriptiv braucht
- Absicherung: `test_lags_nicht_gegenwartsbezogen` und
  `test_lags_gegen_rohdaten` schlagen 200 Stichproben direkt in den Rohdaten
  nach

> **Kein Listing hier.** Der Lag-Block zeigt dieselbe Regel wie L3
> (`shift` vor `rolling`). Zweimal dasselbe belegen ist die Dopplung, die der
> Abgrenzungsblock verbietet — der Fließtext genügt.

### Absatz 7 — Die Zielgrößen

Als kleine Tabelle, mit Bezugsmenge in der Überschrift („35 Stadtteile,
4.620 bzw. 4.619 Stadtteil-Monate"):

| Zielgröße | Typ | Kennwerte |
|---|---|---|
| `anzahl_einsaetze` | Zähldaten | Mittel 75,9 · Median 53 · Max 451 |
| `einsaetze_je_1000_ew` | stetig | Mittel 5,71 · Median 3,54 · Max 67,7 |
| `dominante_einsatzart` | 4 Klassen | Fehlalarm 79,0 · Techn. Hilfe 16,3 · Rettung/EMS 3,1 · Brand 1,5 % |

- Die vier Anteile in [0,1] sind **Zwischengröße**, keine eigenen Zielgrößen:
  Aus ihnen entsteht per argmax die `dominante_einsatzart`. Sie werden nicht
  modelliert und nicht berichtet — entsprechend haben sie keine Baselinewerte
- Rückverweis 3.2: Damit stehen drei Zielgrößen nebeneinander, zwei des
  Mengen- und eine des Strukturstrangs

> **Kennwerte oder nicht?** Streng nach der Merkmalsregel gehören
> Verteilungskennzahlen nach Kapitel 4. Hier sind sie vertretbar, weil die
> Tabelle die *Konstruktion* zusammenfasst — aber dann **ohne** Schiefe,
> Wölbung, Nullanteil und Dispersionsindex. Die stehen in 4.2.

### Absatz 8 — Warum die Klassifikation auf Stadtteilebene stattfindet

Der methodisch stärkste Absatz des Kapitels. Vier Sätze, alle mit Zahl.

- Innerhalb eines Stadtteil-Monats tragen **alle** Einsätze identische
  Strukturmerkmale: **350.481 Zeilen enthielten nur 4.619 verschiedene
  Merkmalsprofile**
- Ein Modell, das jedem Profil seine häufigste Klasse zuweist — also ein
  perfektes Modell auf dieser Ebene — erreichte **49,9 %** Treffer gegenüber
  **48,2 %** für bloßes Raten. Obergrenze: **1,7 Prozentpunkte**
- Auf Stadtteilebene ist die Frage beantwortbar
- Die Zielgröße bleibt eine **echte Klasse** — argmax über die vier
  NFIRS-Gruppen, kein gesetzter Schwellwert. Damit entfällt die
  Begründungslast, die eine künstliche Einteilung einer stetigen Größe mit
  sich brächte → **`\footcite{Altman2006}`**
- Zuordnung der neun NFIRS-Serien zu vier Gruppen →
  `\footcite[S.~3-21--3-28]{USFA2015}` (Locator mit `S.~` von Hand, sonst
  verschluckt biblatex den Rest)

---

## 5. Abschnitt 5.4 — Analyserahmen und Baselines

*Rund 2,5 Seiten, neun Absätze, drei Listings. Zweiter Schwerpunkt.*

### Absatz 1 — Warum der Validierungsrahmen in die Data Preparation gehört

- Die Aufteilung steht als Spalten `fold` und `ist_holdout` **in den Dateien**.
  Sie ist damit eine Eigenschaft des Datensatzes, nicht der Algorithmen — die
  Fairness-Regel ist konstruktiv abgesichert statt behauptet
- CRISP-DM führt Splitting selbst unter *Select data*
  (`\footcite[23-26]{Chapman2000}`)
- **Wörtlich zu formulieren (Auflage Schröter 04.08.2026, R-17):** Ziel der
  Validierung ist die **„Generalisierung auf unbekannte Stadtteile"**. Steht
  noch nirgends im Text

### Absatz 2 — Stadtteil-Split statt Zeitschnitt

- **29** Entwicklungsstadtteile auf **5 Folds** (6 · 6 · 6 · 6 · 5), dazu
  **6** Hold-out-Stadtteile
- Jeder Stadtteil genau einmal Testfall, **mit allen 132 Monaten**
- Trainingszeilen je Fold 3.036 bis 3.168, Testzeilen 660 bis 792
- **Die zentrale methodische Aussage der Arbeit:** Ein Zeitschnitt prüft die
  Forschungsfrage nicht, weil dort jeder Stadtteil in Training *und* Test
  steht und das Modell sein Niveau bereits kennt
- Beleg, zitiert aus 4.2: **92,5 %** der Varianz liegen zwischen den
  Stadtteilen; der Stadtteil-Mittelwert allein erreicht dort R² 0,888
- **Quelle: `\footcite[925]{Roberts2017}`** — „Step 3. Block according to
  objectives and structure: […] spatial blocks when predicting to new sites".
  Das ist dein Stadtteil-Split wörtlich. Die Empfehlung, geblockt zu
  validieren, sobald Abhängigkeitsstrukturen bestehen — auch wenn in den
  Residuen keine Korrelation sichtbar ist —, steht auf S. 913
- **→ Abbildung A2 `a2_foldstruktur`**

> **Ein Fund, der über Kapitel 5 hinausgeht.** Dieselbe Quelle benennt auf
> S. 913 die Kehrseite: Blocken *erzeugt* Extrapolation und überschätzt
> dadurch den Interpolationsfehler — ist Extrapolation aber das **Ziel**,
> verbessert bewusstes Blocken die Fehlerschätzung. Genau das ist deine
> Lage: Der Extrapolationsanteil von 33,7 % ist dann keine Schwäche, sondern
> eine dokumentierte Folge des gewählten Rahmens. Der Satz gehört nach 7.3
> und 8.3, hier nur der Verweis.

### Absatz 3 — Doppelte Stratifizierung

- Sortiert nach der Zahl brand-dominierter Monate, bei Gleichstand nach
  Bevölkerung
- Von **70** brand-dominierten Monaten liegen **35 allein in Bayview Hunters
  Point**
- Ohne Stratifizierung hatte in drei von vier Aufteilungen ein Fold **null**
  Brand-Testfälle — Macro-F1 mittelte dann über eine Klasse, die im Testfold
  gar nicht vorkam. Jetzt: **13 · 9 · 6 · 3 · 2**
- Kein Leakage: Festgelegt wird nur, welche Stadtteile *gemeinsam* getestet
  werden — dasselbe Prinzip wie `StratifiedGroupKFold`
- Bei Gleichstand nach Bevölkerung, sonst wäre die Fold-Streuung ein
  Größeneffekt
- **→ Listing L4** `ergaenze_aufteilung`

### Absatz 4 — Modelltauglichkeit (Serialisierung)

Auflage Schröter 10.08.2026: Serialisierung ausdrücklich zeigen. Hier ist die
Stelle.

- Alle Merkmale `float64`, keine fehlenden Werte
- Die Falle: Eine einzige nullable `Int64`-Spalte genügt, damit `X.to_numpy()`
  ein `object`-Array liefert. Scikit-learn fängt das still ab, XGBoost lehnt
  es ab — der Fehler träte also erst beim dritten der drei zu vergleichenden
  Verfahren auf und säße dann im Preprocessing
- Absicherung: `test_datentypen_modelltauglich`
- **→ Listing L5** `_setze_datentypen`

### Absatz 5 — Die Messlatte in zwei Stufen

- Rückverweis 3.2, wo die Referenzstufen als Erfolgsmaßstab eingeführt sind
- **Stufe 1** — ohne ein einziges Merkmal: Gesamtmittelwert der
  Trainingsstadtteile bzw. immer die häufigste Klasse. Beantwortet, ob in den
  Merkmalen überhaupt Information steckt
- **Stufe 2** — die einfachste Form, die zur Datenform passt: unpenalisiertes
  GLM mit kanonischem Link. Poisson mit Offset für die Menge, multinomiales
  Logit für die Struktur
- Beide über **dieselben** 50 Läufe, denselben Split, dieselben zwölf Merkmale
  wie die Vergleichsverfahren — die Baseline ist Mitbewerber unter identischem
  Protokoll, nicht bloß ein Referenzwert
- **Die Vergleichsverfahren müssen Stufe 2 schlagen**, nicht Stufe 1

Tabelle, Streuung ist `std_wiederholungen` über die zehn Wiederholungsmittel:

| Zielgröße | Stufe | Baseline | R² | RMSE |
|---|---|---|---|---|
| `anzahl_einsaetze` | 2 | Poisson-GLM mit Offset | 0,542 ± 0,082 | 33,98 ± 3,11 |
| `anzahl_einsaetze` | 1 | Gesamtmittelwert | −0,744 ± 0,325 | 69,93 ± 1,92 |
| `einsaetze_je_1000_ew` | 2 | Poisson-GLM mit Offset | 0,367 ± 0,261 | 4,08 ± 0,62 |
| `einsaetze_je_1000_ew` | 1 | Gesamtmittelwert | −1,054 ± 0,875 | 7,54 ± 0,13 |

| Zielgröße | Stufe | Baseline | Macro-F1 | Accuracy |
|---|---|---|---|---|
| `dominante_einsatzart` | 2 | Multinomiales Logit | 0,297 ± 0,014 | 0,584 |
| `dominante_einsatzart` | 1 | Mehrheitsklasse | 0,223 | 0,806 |

- **→ Listing L6** `poisson_glm`
- Die Rate entsteht aus **derselben** Anpassung, geteilt durch die Bevölkerung
  — ein zweites Modell wäre eine zweite Spezifikation und damit unfair
  gegenüber den Vergleichsverfahren (#43)

### Absatz 6 — Auflage D, wörtlich

Schröter hat die Begründung am 08.08.2026 selbst formuliert und angewiesen,
sie „genau so" zu dokumentieren. Seine drei Argumente, **in seiner
Reihenfolge**:

1. Die Reduktion auf die einfacheren Varianten ist **methodisch sauber**.
2. Sie **vermeidet willkürliche Parameter**.
3. Sie **liefert zudem stärkere Vergleichswerte**.

- Punkt 2 ist der tragende: Ein Strafterm auf dem Vorgabewert der Software
  wäre ein willkürlich gesetzter Parameter, ein getunter eine zu begründende
  Wahl. Die unpenalisierte Anpassung hat nichts zu wählen
- **Der Vorbehalt gehört dazu (R-2):** Punkt 3 trifft auf den Mengenstrang zu
  — die Latte **steigt** von 37,27 auf 33,98 RMSE. Auf den Strukturstrang
  trifft er **nicht** — dort **sinkt** sie von 0,314 auf 0,297 Macro-F1. Beide
  Zahlen standen in der Anfrage vom 08.08., die Freigabe erfolgte also in
  Kenntnis
- Das ist zugleich das Argument gegen Rosinenpicken: dieselbe Änderung hilft
  im einen Strang und schadet im anderen

### Absatz 7 — Warum Poisson und nicht Negative Binomial

- Die Negative Binomial ist die Erweiterung für korrekte **Inferenz**. Sie löst
  ein Problem, das diese Baseline nicht hat, und bringt mit dem
  Dispersionsparameter eine zusätzliche Größe mit — damit ist sie nicht mehr
  „die einfachste Form, die zur Datenform passt"
- Die Überdispersion (Dispersionsindex 62,8, zitiert aus 4.2) beschädigt beim
  Poisson-Schätzer nur die Standardfehler, nicht die Konsistenz des bedingten
  Mittelwerts → **`\footcite{Gourieroux1984}`**. Eine Baseline mit reinen
  Punktvorhersagen verwendet keine Standardfehler
- Gemessen ist das Poisson-GLM zugleich die **härtere** Latte: 33,98 gegen
  37,27 RMSE
- **Zu benennen:** Schröter hatte die Negative Binomial namentlich freigegeben;
  der Wechsel wurde ihm am 08.08.2026 mitgeteilt (R-14)

### Absatz 8 — Drei Klarstellungen, die sonst als Fehler gelesen werden

- **Negative R² sind korrekt und aussagekräftig.** Wer für einen unbekannten
  Stadtteil den Gesamtdurchschnitt vorhersagt, liegt schlechter als dessen
  eigener Mittelwert. Genau diese Lücke sollen die Strukturmerkmale schließen
- **Auf der Rate ist R² kein Hauptmaß.** In Fold 4 fällt es auf −0,920, obwohl
  das Poisson-GLM die Nullmarke bei RMSE in *jedem* Fold schlägt. Ursache: R²
  misst gegen den Mittelwert der Testdaten, und die Rate streut zwischen den
  Stadtteilen um den Faktor 32 (Excelsior 1,04 · Financial District 33,80).
  **Konsequenz für Kapitel 7:** bei der Rate RMSE und MAE berichten, R² nur
  nachrichtlich
- **Die naive Vormonats-Baseline entfällt.** Sie würde die eigene Vergangenheit
  des Teststadtteils nutzen — unter dem Stadtteil-Split gibt es sie nicht

> **Vorsicht bei der vierten Klarstellung.** Das Logit hat die schlechtere
> Trefferquote (0,584 gegen 0,806) und zugleich das bessere Macro-F1. In 5.4
> gehört nur der **Befund** hin, dass Accuracy hier in die Irre führt und
> deshalb Macro-F1 maßgeblich ist. Die **Ursache** — `class_weight="balanced"`
> — ist ein Modellparameter und gehört nach Kapitel 6. Sonst steht die
> Erklärung zweimal da.

### Absatz 9 — Steckbrief des finalen Datensatzes

Abschlusstabelle, zugleich die Brücke zu Kapitel 6:

| | |
|---|---|
| Analyseeinheit | Stadtteil × Monat, beide Datensätze |
| Zeitraum | 2015-01 bis 2025-12 (132 Monate) |
| Beobachtungen Menge | 4.620 (35 × 132) |
| Beobachtungen Struktur | 4.619 (ein Monat ohne Einsatz) |
| Merkmale | 10 Struktur + 2 Saison = 12 |
| Aufteilung | 5 Folds (6 · 6 · 6 · 6 · 5), 29 Entwicklungs- + 6 Hold-out-Stadtteile |
| Ausschlüsse | 3 Parkgebiete, 3 ohne durchgängige ACS-Abdeckung |
| Absicherung | **19 automatisierte Prüfungen an den erzeugten Dateien** |

Die letzte Zeile ist der Schlusssatz des Kapitels. Sie ist das, was Kapitel 5
von den übrigen unterscheidet — und die Antwort auf den Merksatz aus dem
Gutachten: Probleme zu erkennen reicht nicht, bewertet wird, ob sie
methodisch gelöst wurden.

---

## 6. Codeausschnitte — endgültige Zuordnung

Sechs statt der sieben aus der Notiz vom 22.08.; der Lag-Block entfällt (siehe
5.3 Absatz 6). Rund 80 Zeilen, etwa 1,4 Seiten. Fertig in
`docs/kapitel5_listings.tex`.

| | Label | Quelle | Abschnitt | Was er belegt |
|---|---|---|---|---|
| L1 | `lst:acs-versatz` | `s1_daten.acs_snapshot` | 5.2 | Verfügbarkeitsgrundsatz in vier Zeilen |
| L2 | `lst:quoten` | `s1_daten.berechne_quoten` | 5.3 | Nenner ≤ 0 ergibt NaN |
| L3 | `lst:krimindex` | `s1_daten.kriminalitaetsindex` | 5.3 | Location Quotient, Fenster im Vormonat |
| L4 | `lst:folds` | `s2_datensaetze.ergaenze_aufteilung` | 5.4 | doppelte Stratifizierung |
| L5 | `lst:dtypes` | `s2_datensaetze._setze_datentypen` | 5.4 | Serialisierung |
| L6 | `lst:poisson` | `v1_baselines.poisson_glm` | 5.4 | Offset, unpenalisiert, kein freier Parameter |

Die Test-Ausschnitte (`lst:tests`) bleiben Reserve. Wenn das Budget es
hergibt, gehören sie in 5.1 Absatz 2 — dort tragen sie den Leakage-Nachweis.

**Abbildungen:** nur A2 `a2_foldstruktur` in 5.4 Absatz 2. A0 `a0_pipeline`
ist am 24.08. gestrichen worden.

---

## 7. Quellen

### 7.1 Vorhanden und einsetzbar

| Key | Wofür in Kapitel 5 | Locator |
|---|---|---|
| `Chapman2000` | Gliederungsbegründung, Splitting unter *Select data* | S. 23–26 |
| `CensusBureau2020` | ACS-Publikationsversatz | S. 13 |
| `USFA2015` | NFIRS-Serien → vier Gruppen | `S.~3-21--3-28` |
| `SFPlanning2022`, `CensusACS2023`, `SFPD2018`, `SFPD2026`, `SFPlanning2020` | Quellenangaben der Joins | – |
| `Bergmeir2012` | Verfügbarkeitsgrundsatz — **nur falls 2.6 ihn nicht trägt** | S. 192–193 |

### 7.2 Neu, am Volltext geprüft — Einträge stehen in `literatur_kapitel5.bib`

| Key | Wofür in Kapitel 5 | Locator |
|---|---|---|
| `Kaufman2011` | Leakage: Spaltenauswahl (5.1), kein `bfill` (5.3), Lags kein Merkmal (5.3) | S. 559 „no-time-machine requirement", S. 560 „learn-predict separation" |
| `Roberts2017` | Stadtteil-Split (5.4), Extrapolation (7.3, 8.3) | S. 925 „Step 3", S. 913 Empfehlung |
| `Brantingham1997` | Location Quotient (5.3) | S. 268 Gl. 1, S. 269 Gl. 2 |

Drei Abweichungen von dem, was ich vorher geschrieben hatte:

- **Kaufman ist die Konferenzfassung** (KDD 2011, S. 556–563), nicht die
  Zeitschriftenfassung von 2012. Beide tragen denselben Titel. Zitiert wird,
  was gelesen wurde — der Key heißt `Kaufman2011`.
- **Brantingham ist 1997**, nicht 1998. Das Titelblatt des Bandes führt
  „Copyright 1997 by Willow Tree Press, Inc."; die verbreitete 1998 stammt vom
  Nachdruck. Die Datei heißt `brantingham1998.pdf` — bei Gelegenheit umbenennen.
- **Autorenschaft geklärt:** Patricia L. **Brantingham** und Paul J.
  **Brantingham**, beide Simon Fraser University. Das „Brantingham & Fraser"
  bei Semantic Scholar ist ein Parsing-Fehler aus der Affiliation.

### 7.3 Ehemals fehlend — am 24.08.2026 über freie Fassungen gelöst

Beide standen hinter einer Schranke. Beide sind jetzt frei zugänglich, geprüft
und in `literatur_kapitel5.bib` eingetragen.

**`Gourieroux1981`** — die Econometrica-Fassung von 1984 liegt hinter JSTOR.
Zitiert wird stattdessen die **frei zugängliche Arbeitspapierfassung derselben
Arbeit**: CEPREMAP Nr. 8203, Dezember 1981, 25 Seiten, gleicher Titel, gleiche
drei Autoren. Die Veröffentlichung in Econometrica 52 (3), S. 701–720 steht im
`note`-Feld. Das ist die saubere Lösung — zitiert wird, was gelesen wurde.
Locator: S. 5, Abschnitt 2.b.
<http://www.cepremap.fr/depot/couv_orange/co8203.pdf>

> **Eine Jahresangabe nachziehen.** Deine internen Notizen nennen durchgehend
> „Gourieroux, Monfort & Trognon 1984". Im Fließtext der Arbeit steht die
> Stelle noch nicht — dort also **1981** schreiben. Die Notizen können bleiben,
> sie meinen dasselbe Resultat.

**`Altman2006`** — frei über PubMed Central (PMCID PMC1458573) und als PDF.
Der Artikel ist einseitig, Locator ist 1080 oder gar keiner. Die zitierfähige
Zeile: *dichotomising a variable at the median reduces power by the same amount
as would discarding a third of the data.*
<https://doi.org/10.1136/bmj.332.7549.1080>

**Falls du für die Überdispersion doch lieber einen Lehrbuchbeleg willst:**
Cameron & Trivedi steht ohnehin auf deiner ausstehenden Quellenliste und wird
in 2.4.1 für den Dispersionsindex gebraucht. Dieselbe Quelle würde dort auch
die Konsistenz des Poisson-Schätzers tragen — ein Beleg statt zwei. Ich würde
trotzdem das Arbeitspapier nehmen: Es ist die Primärquelle, und Schröters Regel
lautet Paper für den Forschungsstand, Lehrbücher nur für Grundlagen.

---

## 8. Einfach und trotzdem wissenschaftlich — sechs Regeln

1. **Eine Entscheidung je Absatz.** Was gemacht wurde, warum, was dadurch
   nicht gilt. Wo der dritte Satz nicht existiert, ist es Mechanik und gehört
   gekürzt.
2. **Jede Zahl mit Bezugsmenge.** 35 Stadtteile (Kapitel 4 und 5), 29
   Entwicklungsstadtteile (5.4 und 7), 23 Trainingsstadtteile von Fold 1
   (6.2). Dieselbe Größe hat auf jeder Menge einen anderen Wert, und alle drei
   sind richtig.
3. **Formeln in den Fließtext, Code in die Listings.** Der Location Quotient
   ist eine Definition, keine Implementierung.
4. **Keine Chronologie** (Auflage 10.08.). Nicht „zunächst wurde die Negative
   Binomial verwendet, später…", sondern „die Stufe-2-Baseline ist ein
   unpenalisiertes Poisson-GLM". Die Reflexion steht konzentriert in Kapitel 8.
5. **Annahme als Annahme kennzeichnen.** Drei Stellen in Kapitel 5 sind
   gesetzt und nicht aus den Daten abgeleitet: das Antwortzeitfenster 0–60
   Minuten, das Indexfenster von zwölf Monaten, die Zahl der Folds. Wer sie
   als Ergebnis darstellt, verliert genau dort Punkte, wo Ehrlichkeit welche
   bringt.
6. **Kein Verweis auf einen Anhang.** Was im Text steht, muss für sich stehen
   (P2, entschieden 22.08.).

### Selbsttest, bevor du das Kapitel abgibst

- Kommt eine Verteilungskennzahl vor, die nicht in der Zielgrößentabelle
  steht? → gehört nach 4.2
- Kommt eine Rechenvorschrift in Kapitel 4 vor? → gehört hierher
- Beginnt jeder der vier Abschnitte mit dem Befund, den er beantwortet?
- Steht zwischen `\section` und `\subsection` nichts?
- Ist irgendwo eine Zahl, die nicht in `03_STAND.md` steht?
- Steht „Generalisierung auf unbekannte Stadtteile" wörtlich im Text?
- Sind die drei Auflage-D-Argumente in Schröters Reihenfolge und mit dem
  Vorbehalt zu Punkt 3?

---

## 9. Was noch offen ist

| | Frage | Stand |
|---|---|---|
| G1 | Variante C oder A | **entschieden 24.08.: Variante C.** Vier Unterabschnitte, Nummerierung 5.1 bis 5.4, Analyseeinheit als erster Absatz von 5.1 |
| G2 | Trägt 2.6 den Verfügbarkeitsgrundsatz? | **entschieden 24.08.: ja.** In 5.1 steht deshalb **nur der Verweis auf 2.6**, kein zweiter `\footcite{Bergmeir2012}` |
| G3 | Zielgrößen-Tabelle in 5.3 | **in Arbeit** — Tabelle wird gerade angefasst. Bis dahin schreibe ich 5.3 so, dass der Text ohne sie trägt |
| G5 | Test-Listing in 5.1 | **vertagt** — nach dem Seitenstand von 5.1 bis 5.4 |

Der laufende Arbeitsstand steht in `docs/Kapitel5_Arbeitsstand.md`. Diese
Datei hier bleibt die inhaltliche Vorlage; dort steht, was davon schon im Text
ist und was sich beim Eintippen geändert hat.

**Erledigt am 24.08.2026:** Alle fünf Quellen für Kapitel 5 sind am Volltext
geprüft, die `.bib`-Einträge stehen im Registerformat in
`docs/literatur_kapitel5.bib`, die Locatoren sind gesetzt. G4 ist damit
geschlossen.

**Ein Codekommentar ist nachzuziehen:** `prep/s2_datensaetze.py:376-377` sagt
„künstliche Einteilung einer **Zaehlgroesse**". Altman und Royston behandeln
**stetige** Größen, und dichotomisiert würde hier `anteil_brand` — ein Anteil
in [0,1], keine Zählgröße. Dasselbe in `docs/08_FUNKTIONSDOKUMENTATION.md`
Zeilen 738 und 4473.

Damit können wir mit 5.1 anfangen.
