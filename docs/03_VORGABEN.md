# Vorgaben und Richtlinien

Verbindlich für die gesamte Arbeit. Quellen: Gruppensprechstunden bei Prof. Dr.
Schröter (13.07.2026 und 27.07.2026, Mitschrift in
`sprechstunde_2026-07-27.txt`) sowie das Kurzgutachten zum Anwendungsprojekt,
das laut Schröter **ausdrücklich auch für die Bachelorarbeit gilt**. Zweitprüfer
Oliver Bach setzt dieselben Prioritäten.

**Merksatz aus dem Gutachten:** Probleme zu *erkennen* reicht nicht — bewertet
wird, ob sie methodisch *gelöst* wurden.

---

## 0. Sprechstunde 27.07.2026 — neue Auflagen

| Auflage | Konsequenz |
|---|---|
| **Baseline gehört in die Data Preparation** | Kapitel 5 bekommt einen eigenen Baseline-Abschnitt; die Referenzwerte stehen vor der Modellierung fest |
| **Die Baseline muss zum Anwendungsfall passen** — bei nichtlinearen Zusammenhängen braucht es eine **nichtlineare** Baseline | Zweck der Baseline ist der *Beleg*, dass die komplexeren Modelle ein besseres Ergebnis liefern. Keine Abstimmungspflicht, aber die Referenz muss fair sein — sonst ist der Beleg wertlos |
| **Keine Blindabsätze** — zwischen „1." und „1.1" darf kein leerer Übergang stehen | Jede Gliederungsebene braucht Inhalt; Redundanz zwischen Ebenen vermeiden |
| **Zitate einheitlich, mehrere Quellen pro Seite** | Quellendichte hochhalten, Zitierstil konsistent |
| **Alles mit dem Stand der Forschung belegen** | Jede methodische Entscheidung braucht eine Referenz |
| **KI-Verzeichnis: Engine, Jahr, Modell, Prompt, URL** | Kennung im Zitierstil, z. B. `anthropic2026a`, `openai2026b` |
| Kolloquium: 2 Termine pro Tag | organisatorisch |

### Wozu die Baseline dient

Die Baseline ist **kein Genehmigungspunkt**, sondern ein Beweismittel: Sie soll
belegen, dass die komplexeren Verfahren tatsächlich ein besseres Ergebnis
liefern als eine einfache Regel. Daraus folgt die Anforderung an ihre Form —
eine Referenz, die schon durch ihre Funktionsform benachteiligt ist, wäre ein
Strohmann und würde den Beleg wertlos machen. Genau das meint die Auflage
„bei nichtlinearen Zusammenhängen braucht es eine nichtlineare Baseline".

Für diesen Datensatz ist diese Anforderung erfüllt, und zwar aus vier Gründen:

1. **Zwei der drei Baselines unterstellen überhaupt keine Funktionsform.** Der
   Vormonatswert und der saisonale Durchschnitt sind nichtparametrisch — sie
   schätzen keine Koeffizienten und können deshalb weder linear noch nichtlinear
   danebenliegen.
2. **Die nichtparametrische Baseline ist zugleich die härteste.** Naiv erreicht
   RMSE 17,78 ± 1,29 (R² 0,950). Von sechs Modellläufen schlägt sie genau einer.
3. **Die Leitfrage ist damit beantwortet — unbequem.** Random Forest (16,96 ±
   0,73) liegt innerhalb der Streuung, XGBoost (18,47 ± 1,12) darunter. Nur
   Ridge (S+L) mit 15,01 ± 0,86 schlägt den Vormonatswert klar. Der zusätzliche
   Aufwand der Baumverfahren bringt gegenüber einer Regel, die man auf einem
   Bierdeckel notieren kann, nichts. Das ist ein Ergebnis, kein Makel (R6).
4. **Die einzige parametrisch-lineare Baseline ist die schwächste** (Negative
   Binomial, 37,26 ± 13,51). Sie dient der Interpretierbarkeit als
   verteilungsgerechtes Zähldatenmodell, nicht dem Leistungsvergleich.

### Was die Baseline derzeit tatsächlich belegt

Ihr Zweck ist der Nachweis, dass sich der Mehraufwand lohnt. Aktuell fällt
dieser Nachweis **nur für ein Verfahren von dreien** aus:

| Verfahren | RMSE | gegen naiv (17,78 ± 1,29) |
|---|---|---|
| Ridge (S+L) | 15,01 ± 0,86 | **schlägt die Baseline klar** |
| Random Forest (S+L) | 16,96 ± 0,73 | innerhalb der Streuung — nicht unterscheidbar |
| XGBoost (S+L) | 18,47 ± 1,12 | **schlechter als die Baseline** |

Bemerkenswert ist die Richtung: Ridge ist von den drei Verfahren das
*einfachste*. Die beiden aufwendigen Baumverfahren schlagen eine Regel nicht,
die man auf einem Bierdeckel notieren kann. Das ist ein belastbares Ergebnis und
genau die ehrliche Vergleichsaussage, die R6 verlangt — es muss nur als solches
ausformuliert werden, statt in einer Rangfolge unterzugehen.

Sobald der Stadtteil-Mittelwert als vierte Baseline dazukommt, verschärft sich
das Bild noch: Er erreicht R² 0,888, ohne ein einziges Merkmal zu benutzen.

---

## 1. Warum das Gutachten so schwer wiegt

Das bewertete Anwendungsprojekt (Note 2,7) war strukturell sehr ähnlich:
Vergleich mehrerer Algorithmen auf Panel-/Zeitreihendaten, mehrere Zielgrößen,
Risiko-Index-Features. Die Kritikpunkte sind fast 1:1 übertragbar.

| Block | Note | Befund |
|---|---|---|
| Formales | 2,3 | war nie das Problem |
| Methoden | 2,7 | **heterogene Spezifikationen, eingeschränkte Prognosevalidierung** |
| Inhalt | 2,7 | **Breite statt Tiefe, Argumentation der schwächste Punkt** |

Der Hebel für eine bessere Note liegt in Methodik, Argumentation und
Problemlösungstiefe — nicht in der Formatierung.

---

## 2. Die zehn Regeln

### R1 — Ein einziger, einheitlicher Forecasting-Rahmen
Der wörtlich benannte Hauptkritikpunkt. Es gibt **einen** Prognoserahmen, dem
sich alle Modelle unterordnen: feste Analyseeinheit, fester Horizont, identische
Splits, identische Feature-Matrix, identische Metriken, identische Baselines.
Jede Abweichung ist ein Rückfall in das kritisierte Muster.

*Umsetzung:* `fold` und `ist_holdout` stehen als Spalten in beiden Datensätzen —
kein Modellskript kann die Aufteilung anders berechnen.

### R2 — Panelabhängigkeiten explizit adressieren
Die Daten sind ein Panel, kein i.i.d.-Querschnitt. Stadtteil-Heterogenität
behandeln und die Wahl begründen. **Effektive Stichprobengröße offen ansprechen:**
4.620 Zeilen klingen viel, es sind aber nur 35 unabhängige Querschnittseinheiten.
Gehört in die Limitationen.

### R3 — Keine zeitgleichen Merkmale
Alle Prädiktoren müssen zum Prognosezeitpunkt tatsächlich verfügbar gewesen sein.
Kriminalität und Einsatzzahlen gehen ausschließlich gelagged ein. Die jährliche
Wiederholung der ACS-Werte muss benannt, quantifiziert und reflektiert werden.

### R4 — Zwei Zielgrößen: hierarchisieren statt parallelisieren
**Größtes Notenrisiko.** Zwei Zielgrößen × drei Algorithmen = sechs Modellstränge
— genau die parallele Behandlung, die im Gutachten die Tiefe gekostet hat.
Nicht akzeptabel: beide Stränge gleich breit, aber flach. Lieber eine Zielgröße
exzellent als zwei mittelmäßig.

*Stand:* Die Klassifikation erreicht im Vortest Macro-AUROC ≈ 0,59 — das spricht
dafür, die Regression als Hauptzielgröße zu führen und die Klassifikation
bewusst kompakt zu halten.

### R5 — Zeitlich blockierte Validierung, sauber und sichtbar
Kein zufälliges K-Fold. Forward Chaining, echtes End-Hold-out am Ende der
Zeitachse, Tuning ausschließlich innerhalb der Trainingsfolds. Skalierung,
Encoding und Imputation gehören in eine `sklearn.Pipeline` — Schröter hat
„Preprocessing Pipeline" ausdrücklich erwähnt. Der Split gehört **grafisch
dargestellt**.

### R6 — Baselines und ehrliche Vergleichsaussagen
Naive Baselines sind Pflicht: Vormonatswert, saisonaler Durchschnitt **und
Mehrheitsklasse** für die Klassifikation. Unterschiede nicht überinterpretieren —
Mittelwert ± Standardabweichung über die Folds angeben, nicht nur Punktwerte.
Überlappen die Bereiche, muss das so gesagt werden.

### R7 — Linearitätsprüfung vor Ridge (harte Auflage)
Wörtlich: *„erstmal plotten, falls keine lineare Baseline, KEIN lineares
Regressionsmodell."* Scatterplots und Residuenanalyse **vor** dem Einsatz von
Ridge, dokumentiert in der Arbeit. Bei Zähldaten ist Poisson/Negativ-Binomial
die naheliegende Ergänzung.

*Umsetzung:* `modelle/m01_eignung.py`, Abschnitt 5 → `results/eignungspruefung/`.

### R8 — Fokus statt Breite
Explizit abgestraft: „sehr umfangreich, aber nicht immer fokussiert". Jede
Analyse muss auf eine Forschungsfrage einzahlen — sonst raus oder in den Anhang.
Keine Doppelungen zwischen Grundlagen- und empirischem Teil. Mindestens 3/4
Fließtext, Code-Snippets sparsam.

### R9 — Wissenschaftlichkeit sichtbar machen
Kritische Reflexion ist ein eigenes Bewertungskriterium. Ein Limitationen-Kapitel
reicht nicht — Reflexion muss **im Verlauf** stattfinden: warum diese
Aggregation, warum dieser Split, was wurde verworfen und warum. Falsifizierende
Perspektive einnehmen. Behauptungen belegen oder als Annahme kennzeichnen.

### R10 — Formales absichern
War gut, muss gut bleiben: konsistente Zitierweise (biblatex/Chicago-notes),
vollständiges Literaturverzeichnis, Rechtschreib-Korrekturdurchgang einplanen,
**KI-Verzeichnis als Tabelle mit Datum, KI-System und Prompt**.

---

## 3. Was positiv bewertet wurde — beibehalten

1. Keine kausalen Überdeutungen, Ergebnisse vorsichtig und explorativ beschrieben
2. Detaillierte Beschreibung von Datenquellen, Lag-Variablen und Zielvariablen
3. Offene Reflexion methodischer Grenzen — mit der Auflage, erkannte Probleme
   nicht nur zu benennen, sondern zu **lösen**
4. Angemessener Umfang (bester Einzelwert: 1,7) und dokumentierte KI-Nutzung

---

## 4. Formale Vorgaben

- mindestens 3/4 Fließtext, Code nur als Beleg
- wissenschaftliche Arbeit, keine Projektarbeit
- 30–100 Quellen (Zotero); Lehrbücher für Grundlagen, Paper für den Forschungsstand
- KI-Verzeichnis als Tabelle: Datum, KI-System, Prompt
- Methodenkapitel so präzise, dass die Arbeit reproduzierbar ist
- Story und roter Faden
- Abgabe-Zip max. 250 MB inkl. flüchtiger Quellen als PDF/A

---

## 5. Was ins Abgabe-Zip gehört

```
prep/config.py            alle Festlegungen an einer Stelle
prep/s1_daten.py          1  laden, auswählen, joinen, Raten
prep/s2_datensaetze.py    2  aggregieren, Lags, Zielgrößen, Folds
prep/build.py             der eine Befehl
modelle/m01_eignung.py    Eignungsurteil + Vergleichsgrößen
modelle/m02_regression.py Ridge, Random Forest, XGBoost
modelle/m03_klassifikation.py
tests/test_aufbereitung.py
data/processed/regression.parquet         die zwei finalen Datensätze
data/processed/klassifikation.parquet
results/                  Eignungsprüfung, Fold-Ergebnisse, Abbildungen
docs/                     diese drei Dateien
README.md, requirements.txt
```

**Nicht ins Zip:** `venv/` (über `requirements.txt` reproduzierbar), `data/raw/`
(38 MB, über `prep/s1_daten.py` reproduzierbar), `data/processed/einsaetze.parquet`
(35 MB Zwischenstand), `prep/_archiv/`.

---

## 6. Kolloquiums-Fragen, auf die eine Antwort vorliegen muss

- „Warum genau diese drei Algorithmen — und wieso überhaupt Machine Learning?"
- „Hätten Sie es nicht anders machen können? Ist das nicht Overkill?"
- „Wie stellen Sie sicher, dass der Vergleich der drei Verfahren fair ist?"
- „Warum ist Ihre Validierung leakage-frei?"
- „Sie haben nur 35 Stadtteile — wie belastbar sind Ihre Ergebnisse?"
- „Ihre sozioökonomischen Merkmale ändern sich nur jährlich. Was sagt Ihr Modell
  dann eigentlich vorher?"
- „Ist Ridge bei Zähldaten überhaupt das richtige Modell?"

Storytelling ist ausdrücklich gewünscht: die Reise vom Problem über die
getroffenen Entscheidungen — **inklusive verworfener Wege** — zum Ergebnis, auch
für den Zweitprüfer nachvollziehbar.
