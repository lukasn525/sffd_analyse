# Kontext für LLM: Anpassung des Exposés auf Basis der Regressionsanalyse

## Auftrag

Du hast ein Exposé für eine empirische Arbeit über die San Francisco Fire
Department (SFFD) und sozioökonomische Ungleichheit verfasst. Auf Basis einer
vollständigen Regressionsanalyse des SFFD-Datensatzes (708.242 Einsätze,
2018–2024, verknüpft mit ACS-Zensusdaten auf Neighborhood-Ebene) liegen jetzt
konkrete empirische Befunde vor. Passe das Exposé so an, dass Hypothesen,
Methodik und erwartete Befunde mit den tatsächlichen Ergebnissen übereinstimmen.
Behalte Argumentationsstruktur und Schreibstil des bestehenden Exposés bei.


## Datenbasis und Methodik der Analyse

### Datensatz

- **Quelle:** SFFD Fire Incidents (Kaggle/DataSF) + U.S. Census ACS 5-Year
  Estimates, gejoint auf Neighborhood-Ebene
- **Umfang:** 720.258 Einsätze, davon 708.242 nach Qualitätsfilter (response
  time 0–60 min), 701.193 nach Entfernung fehlender sozioökonomischer Werte
- **Neighborhoods:** 40 Stadtteile San Franciscos
- **Zeitraum:** 2018–2024
- **Sozioökonomische Merkmale (ACS):** `median_household_income`,
  `poverty_rate`, `median_gross_rent`, `bachelor_rate`, `vacancy_rate`

### Abhängige Variablen

1. **Einsatzdauer** (`response_time_min`): Zeit zwischen Alarmierung und
   Eintreffen der ersten Einheit. Median 4,37 min, Mittelwert 4,98 min.
2. **Feuerrate** (%): Anteil echter Brände (Codes 100–199) an allen Einsätzen
   je Neighborhood.

### Modellstrategie

Alle Sozioökonomie-Variablen wurden z-standardisiert (Mittelwert 0, SD 1),
sodass Beta-Koeffizienten direkt die Effektstärke in Minuten (Modell A–C) bzw.
Prozentpunkten (Modell D) ausdrücken und untereinander vergleichbar sind.
Robuste Standardfehler (HC3) wurden verwendet. Der Schlüsselschritt gegenüber
einer naiven Korrelation war das Einfügen von **Kontrollvariablen**, die
sozioökonomisch unabhängige Quellen der Einsatzzeit-Variation absorbieren.


## Zentrale empirische Befunde

### Befund 1 – Kontrollvariablen sind entscheidend (Modell A)

Ohne Kontrollvariablen erklären die fünf Sozioökonomie-Merkmale nur **R² =
1,4%** der Streuung in der Einsatzdauer. Mit Kontrollvariablen (Einsatzkategorie,
Tageszeit, Nacht, Wochenende) steigt R² auf **9,1%** – ein Zuwachs von +7,75
Prozentpunkten. Das zeigt: Ein erheblicher Teil der scheinbaren Nicht-Korrelation
zwischen Sozioökonomie und Einsatzdauer war durch Confounding verursacht
(Einsatzart und Tageszeit überdeckten den Signal). Die richtige Analyseeinheit
erfordert zwingend solche Controls.

**Wichtige Kontroll-Koeffizienten (Referenz: Feuereinsätze):**
- Service-Calls: +1,87 min länger ***
- Rettung/EMS: +0,81 min länger ***
- Fehlalarme: −0,50 min kürzer ***
- Nachteinsätze: +0,22 min länger ***
- Wochenende: −0,10 min kürzer ***

### Befund 2 – Armutsrate ist der robusteste Sozioökonomie-Prädiktor

Über alle drei Einsatztyp-Modelle hinweg ist `poverty_rate` der einzige
Sozioökonomie-Indikator, der **konsistent und signifikant** mit längerer
Einsatzdauer assoziiert ist:

| Modell | N | Beta poverty_rate | p-Wert |
|--------|---|-------------------|--------|
| A (alle Einsätze, mit Controls) | 701.193 | +0,143 min | < 0,001 |
| B (nur Feuereinsätze) | 92.776 | +0,123 min | < 0,001 |
| C (nur EMS-Einsätze) | 101.030 | +0,318 min | < 0,001 |

Interpretation: Pro 1 Standardabweichung höhere Armutsrate (~15 Prozentpunkte)
dauert ein EMS-Einsatz im Mittel 0,32 Minuten (≈ 19 Sekunden) länger, ein
Feuereinsatz 0,12 Minuten (≈ 7 Sekunden) länger. Diese Effekte sind statistisch
hochsignifikant, aber in absoluten Minuten moderat.

`median_household_income` ist dagegen in Modellen B und C **nicht signifikant**
(p = 0,64 bzw. p = 0,14), obwohl es mit poverty_rate r = −0,79 korreliert. Das
deutet auf Multikollinearität hin: Die Armutsrate trägt die eigenständige
Erklärungskraft.

### Befund 3 – Bildungsgrad (bachelor_rate) senkt die Einsatzdauer

`bachelor_rate` ist in allen Modellen negativ und hochsignifikant:

| Modell | Beta bachelor_rate | p-Wert |
|--------|--------------------|--------|
| A (alle) | −0,278 min | < 0,001 |
| B (Feuer) | −0,346 min | < 0,001 |
| C (EMS) | −0,369 min | < 0,001 |

Dieser Effekt ist stärker als der Armutsraten-Effekt. Eine plausible Erklärung:
Akademikerreiche Neighborhoods sind räumlich zentraler gelegen (kürzere
Fahrdistanzen), haben besseren Straßenzustand und höhere Gebäudezugänglichkeit.
**Wichtig:** bachelor_rate und poverty_rate sind nur r = −0,65 korreliert –
es handelt sich um eigenständige Dimensionen sozialer Ungleichheit.

### Befund 4 – Miete als Confounder für urbane Dichte

`median_gross_rent` hat über alle Modelle positive Beta-Koeffizienten (+0,49
bis +0,73 min). Das ist kontraintuitiv, da teure Neighborhoods wohlhabend sind.
Die Erklärung liegt in **urbaner Dichte als Confounder**: Hochpreisige
Innenstadtviertel (Financial District, SoMa) haben dichten Straßenverkehr,
Hochhäuser mit komplexem Gebäudezugang und häufig mehr Einsätze pro Fläche.
Dieses Muster sollte im Exposé explizit als Limitation/Confounder adressiert
werden und nicht als direkter Sozioökonomie-Effekt interpretiert werden.

### Befund 5 – Feuerrate: stärkste sozioökonomische Verbindung (Modell D)

Das stärkste Resultat der gesamten Analyse betrifft nicht die Einsatzdauer,
sondern die **Häufigkeit echter Brände je Neighborhood**:

- **R² = 0,41** – sozioökonomische Faktoren erklären 41% der Varianz der
  Feuerrate auf Neighborhood-Ebene
- `poverty_rate` β = +1,38 Prozentpunkte (p = 0,083, tendenziell signifikant)
- Das Gesamtmodell ist signifikant (F-p = 0,003)

**Top-5 Neighborhoods nach Feuerrate:**

| Neighborhood | Feuerrate | Armutsrate | Median Income |
|---|---|---|---|
| McLaren Park | 25,7% | 90,4% | 11.316 $ |
| Bayview Hunters Point | 23,7% | 16,9% | 80.943 $ |
| Potrero Hill | 18,0% | 6,3% | 203.465 $ |
| Bernal Heights | 16,3% | 7,3% | 152.701 $ |
| Mission | 16,1% | 12,8% | 128.489 $ |

McLaren Park ist hier ein starker Ausreißer mit einer Armutsrate von 90,4% –
er verzerrt das Modell erheblich und sollte in der Arbeit gesondert diskutiert
werden (möglicher Datenfehler in den ACS-Daten für diesen Park-Bereich).

### Befund 6 – Poverty-Quartil-Vergleich (deskriptiv)

**Feuereinsätze:**
| Quartil | Armutsrate | Ø Einsatzdauer | Median |
|---|---|---|---|
| Q1 (arm) | hoch | 4,87 min | 4,37 min |
| Q2 | | 4,65 min | 4,22 min |
| Q3 | | 4,81 min | 4,35 min |
| Q4 (reich) | niedrig | 4,28 min | 3,96 min |

Rohunterschied: 0,59 min (35 Sekunden) zwischen Q1 und Q4.

**EMS-Einsätze:**
| Quartil | Ø Einsatzdauer | Median |
|---|---|---|
| Q1 (arm) | 5,57 min | 4,83 min |
| Q2 | 5,46 min | 4,70 min |
| Q3 | 5,49 min | 4,70 min |
| Q4 (reich) | 5,45 min | 4,68 min |

Rohunterschied EMS: 0,12 min (7 Sekunden) – deutlich geringer und weitgehend
auf Q1 konzentriert. Kein monotoner Gradient, eher ein Schwellenwert-Effekt.


## Methodische Einschränkungen (für Exposé-Diskussion)

1. **Ecological Fallacy:** ACS-Daten messen Neighborhood-Durchschnitte, nicht
   individuelle Haushaltseinkommen. Effekte gelten auf Aggregatebene.
2. **Fehlende Distanzvariable:** Fahrdistanz zur nächsten Feuerwache ist der
   vermutlich stärkste Einzel-Prädiktor für Einsatzdauer, fehlt aber im
   Datensatz. Dadurch bleiben R²-Werte mit maximal 9% relativ niedrig.
3. **Multikollinearität:** Die fünf Sozioökonomie-Variablen sind teils stark
   korreliert (r bis 0,92 zwischen income und rent). Einzelne Koeffizienten
   sollten nicht isoliert interpretiert werden.
4. **McLaren Park Ausreißer:** Armutsrate 90,4% bei sehr kleiner Bevölkerung
   (Census-Artefakt für Park-Gebiete) – verzerrt Modell D erheblich.
5. **Kausale Identifikation:** OLS-Regression zeigt Assoziation, keine
   Kausalität. Reverse Causality ist unwahrscheinlich (Nachbarschaft bestimmt
   nicht Einsatzdauer direkt), aber Omitted Variable Bias durch fehlende
   Distanzvariable bleibt zentral.


## Empfehlungen für die Anpassung des Exposés

### Forschungsfragen: anpassen

**Alt (typisch für eine erste Version):**
"Gibt es einen Zusammenhang zwischen sozioökonomischem Status und
Einsatzdauer der SFFD?"

**Vorschlag neu (präziser, evidenzbasiert):**
"Inwiefern ist die Armutsrate auf Neighborhood-Ebene nach Kontrolle für
Einsatzart und Tageszeit mit der Einsatzdauer der SFFD assoziiert, und
unterscheidet sich dieser Zusammenhang zwischen Feuer- und EMS-Einsätzen?"

Und als zweite Forschungsfrage:
"Sagt der sozioökonomische Status eines Stadtteils die Rate echter Brände
(gemessen als Anteil an allen Einsätzen) vorher?"

### Hypothesen: konkretisieren

**H1 (Einsatzdauer):** Neighborhoods mit höherer Armutsrate weisen nach
Kontrolle für Einsatzart, Tageszeit und Wochentag längere Einsatzdauern auf.
Die Armut-Koeffizienten sind positiv und statistisch signifikant, während der
Effekt bei EMS-Einsätzen (β ≈ +0,32 min) stärker ausgeprägt ist als bei
Feuereinsätzen (β ≈ +0,12 min).

**H2 (Feuerrate):** Socioökonomisch benachteiligte Neighborhoods haben eine
höhere Rate echter Brände im Verhältnis zu allen Einsätzen. R² = 0,41 auf
Neighborhood-Ebene stützt diese Hypothese.

**H3 (Bildungseffekt):** Neighborhoods mit höherem Akademikeranteil weisen
kürzere Einsatzdauern auf (β ≈ −0,35 min bei Feuereinsätzen). Dieser Effekt
ist eigenständig von der Armutsrate.

### Methodik-Abschnitt: ergänzen

Füge hinzu, dass die Analyse in zwei Stufen erfolgt:

1. **Modell mit Controls (Ansatz 1):** Multiples OLS mit z-standardisierten
   Sozioökonomie-Variablen und Kontrollvariablen (Einsatzkategorie-Dummies,
   Tageszeit, Nacht-Dummy, Wochenend-Dummy). Robuste Standardfehler (HC3).

2. **Einsatztyp-Stratifizierung (Ansatz 3):** Separate Modelle für
   Feuereinsätze (N = 92.776) und EMS-Einsätze (N = 101.030) zur Vermeidung
   von Kompositionsverzerrung durch Einsatzmischung.

3. **Neighborhood-Level-Analyse (Modell D):** Aggregation auf 39 Neighborhoods
   für Feuerrate-Regression. Dieser Ansatz eliminiert Within-Neighborhood-Rauschen
   und zeigt den strukturellen Zusammenhang am deutlichsten.

### Erwartete Befunde: aktualisieren

Ersetze spekulative Erwartungen durch die tatsächlichen Ergebnisse:

- Der Socioökonomie-Armutsraten-Effekt auf Einsatzdauer ist **statistisch
  signifikant, aber in absoluten Zahlen moderat** (7–19 Sekunden pro SD).
  Das ist keine Überraschung: Die SFFD ist eine professionelle Behörde mit
  standardisierten Reaktionsprotokollen. Systematische Unterschiede existieren,
  sind aber nicht dramatisch.
- Die **Feuerrate** ist der sozioökonomisch relevantere Outcome: 41% erklärte
  Varianz auf Neighborhood-Ebene ist ein substanzieller Befund.
- **Bildungsgrad** ist ein unterschätzter, eigenständiger Prädiktor.
- Die naiven Korrelationen ohne Controls sind irreführend niedrig (r ≈ 0,03–0,05).
  Die Kontrollstrategie ist methodisch entscheidend und sollte als Kernargument
  der Methodik herausgestellt werden.


## Tonhinweis

Die Befunde stützen die Grundthese des Exposés (sozioökonomische Ungleichheit
korreliert mit Feuerwehr-Outcomes), aber sie qualifizieren sie: Der Effekt liegt
mehr in der Brandgefährdung (Feuerrate) als in der Reaktionszeit. Das ist ein
ehrlicheres und theoretisch interessanteres Ergebnis – es verweist auf
strukturelle Vulnerabilität (ältere Gebäude, dichtere Belegung, weniger
Brandschutz in armen Vierteln) statt auf operatives Versagen der Feuerwehr.
