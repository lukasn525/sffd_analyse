# Vorgaben und Richtlinien

> **Lebensdauer dieses Dokuments:** ändert sich nur, wenn Schröter etwas Neues
> sagt. Enthält **keine** Ergebniszahlen — die stehen in `03_STAND.md`.

Verbindlich für die gesamte Arbeit. Quellen: Gruppensprechstunden bei Prof. Dr.
Schröter (13.07.2026 und 27.07.2026, Mitschrift in `sprechstunde_2026-07-27.txt`),
E-Mail vom 03.08.2026 sowie das Kurzgutachten zum Anwendungsprojekt, das laut
Schröter **ausdrücklich auch für die Bachelorarbeit gilt**. Zweitprüfer Oliver
Bach setzt dieselben Prioritäten.

**Merksatz aus dem Gutachten:** Probleme zu *erkennen* reicht nicht — bewertet
wird, ob sie methodisch *gelöst* wurden.

---

## 0. Auflagen aus Sprechstunde und E-Mail

| Auflage | Datum | Umsetzung |
|---|---|---|
| Baseline gehört in die Data Preparation | 27.07. | `prep/s3_baselines.py`, Kapitel 5.4 |
| Die Baseline muss zum Anwendungsfall passen — bei nichtlinearen Zusammenhängen braucht es eine nichtlineare Baseline | 27.07. | siehe unten |
| Keine Blindabsätze zwischen Gliederungsebenen | 27.07. | jede Ebene trägt Inhalt |
| Zitate einheitlich, mehrere Quellen pro Seite | 27.07. | biblatex/Chicago-notes |
| Jede methodische Entscheidung mit dem Stand der Forschung belegen | 27.07. | — |
| KI-Verzeichnis: Kennung, Engine, Jahr, Modell, Prompt, URL | 27.07. | `CLAUDE.md` |
| Regression und Klassifikation dürfen unterschiedlich viele Verfahren nutzen, wenn die Auswahl fachlich begründet wird | 03.08. | Decision Log #31, Kapitel 6.2 |

### Wozu die Baseline dient

Die Baseline ist **kein Genehmigungspunkt**, sondern ein Beweismittel: Sie soll
belegen, dass die komplexeren Verfahren tatsächlich ein besseres Ergebnis
liefern als eine einfache Regel. Daraus folgt die Anforderung an ihre Form —
eine Referenz, die schon durch ihre Funktionsform benachteiligt ist, wäre ein
Strohmann und würde den Beleg wertlos machen.

### Wie die Auflage „nichtlineare Baseline" beantwortet wird

Die frühere Begründung stützte sich auf den **Vormonatswert** und den saisonalen
Durchschnitt als nichtparametrische Referenzen. Sie ist hinfällig: Mit dem
Stadtteil-Split (Decision Log #29) entfällt der Vormonatswert, weil er die eigene
Vergangenheit des Teststadtteils nutzen würde. Der saisonale Durchschnitt fiel
faktisch mit dem Gesamtmittelwert zusammen und benutzte kein einziges Merkmal.

Gewählt wurde stattdessen die **Negative Binomial als alleinige Referenz der
Regression** (Decision Log #32). Die Begründung ist stärker als die alte:

1. Sie ist **kein Strohmann.** Sie bekommt dieselben zwölf Merkmale, dieselben
   Zeilen und dieselben Folds wie die drei Verfahren — ein vollwertiges
   statistisches Modell, kein konstanter Vergleichswert.
2. Sie ist **auf der Originalskala nichtlinear.** Über den Log-Link wirkt sie
   multiplikativ, bildet also überproportionale Effekte ab. Ein einfacher
   Durchschnitt kann das nicht, ein lineares Modell auf der Rohskala auch nicht.
3. Sie ist **verteilungsgerecht.** Poisson scheidet wegen Overdispersion aus;
   die Bevölkerung geht als Offset ein, das Modell schätzt also Einsätze je
   Einwohner statt der Stadtteilgröße.
4. Sie **zieht genau die relevante Trennlinie.** Was sie nicht kann, sind
   Wechselwirkungen zwischen Merkmalen — und genau die finden Random Forest und
   XGBoost konstruktionsbedingt. Damit ist der Vergleich aussagekräftig: Schlagen
   die Baumverfahren sie, existieren solche Wechselwirkungen und der Mehraufwand
   ist gerechtfertigt. Schlagen sie sie nicht, reicht die einfachere Struktur.

**Offen zu benennen:** In der Klassifikation gibt es keine Entsprechung. Die
Negative Binomial sagt eine Zahl vorher, die Zielgröße ist eine von vier
ungeordneten Kategorien. Dort bleibt die Mehrheitsklasse die einzige Referenz,
die Latte liegt also niedriger als in der Regression. Das gehört so in Kapitel 5.

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
sich alle Modelle unterordnen: feste Analyseeinheit, identische Splits,
identische Merkmalsmatrix, identische Metriken, identische Baselines.

*Umsetzung:* `fold` und `ist_holdout` stehen als Spalten in beiden Datensätzen —
kein Modellskript kann die Aufteilung anders berechnen. Beide Zielgrößen liegen
auf derselben Analyseeinheit (Stadtteil × Monat) und laufen durch dieselbe
Aufbereitung. Dass die Klassifikation nur zwei der drei Verfahren nutzt, ist
keine heterogene Spezifikation, sondern eine echte Teilmenge — begründet in
Decision Log #31.

### R2 — Panelabhängigkeiten explizit adressieren
Die Daten sind ein Panel, kein i.i.d.-Querschnitt. Stadtteil-Heterogenität
behandeln und die Wahl begründen. **Effektive Stichprobengröße offen ansprechen:**
Die Zeilenzahl klingt komfortabel, es sind aber nur 35 unabhängige
Querschnittseinheiten, davon 29 in der Entwicklung. Gehört in die Limitationen.

### R3 — Keine zeitgleichen Merkmale
Alle Prädiktoren müssen zum Prognosezeitpunkt tatsächlich verfügbar gewesen sein.
Der Kriminalitätsindex geht ausschließlich rückwärtsgerichtet ein, die ACS-Daten
mit einem Publikationsversatz von einem Jahr. Die jährliche Wiederholung der
ACS-Werte muss benannt, quantifiziert und reflektiert werden.

### R4 — Zwei Zielgrößenstränge: hierarchisieren statt parallelisieren
**Größtes Notenrisiko.** Die Menge hat zwei Zielgrößen × drei Verfahren, die
Struktur eine Zielgröße × zwei Verfahren. Nicht akzeptabel: beide Stränge gleich
breit, aber flach. Lieber eine Zielgröße exzellent als zwei mittelmäßig.

*Umsetzung:* Die Regression ist die Hauptzielgröße. Der Klassifikationsstrang ist
bewusst schmaler gehalten — zwei statt drei Verfahren (#31), die vier Anteile
sind keine eigene Zielgröße, sondern nur Rechenbasis und Deskription.

### R5 — Validierung sauber und sichtbar
Kein zufälliges K-Fold. Echtes Hold-out, das bis zum Schluss unberührt bleibt.
Tuning ausschließlich innerhalb der Trainingsfolds. Skalierung, Encoding und
Imputation gehören in eine `sklearn.Pipeline` — Schröter hat „Preprocessing
Pipeline" ausdrücklich erwähnt. Der Split gehört **grafisch dargestellt**.

**Dokumentierte Abweichung:** Schröter nannte am 13.07. eine *zeitlich* blockierte
Validierung. Die Arbeit verwendet stattdessen einen **Stadtteil-Split**, weil ein
Zeitschnitt die Forschungsfrage nicht prüft — dort steht jeder Stadtteil in
Training und Test und das Modell kennt sein Niveau bereits. Begründung und Beleg
in Decision Log #29; die zeitreihengerechte Variante wird als Nebenbemerkung
geführt. **Diese Abweichung ist mit Schröter zu besprechen.**

### R6 — Baselines und ehrliche Vergleichsaussagen
Baselines sind Pflicht, je Zielgröße. Aktuell: Negative Binomial für die
Regression, Mehrheitsklasse für die Klassifikation, Gesamtmittelwert als
Nullmarke (Decision Log #32). Unterschiede nicht überinterpretieren — Mittelwert
± Standardabweichung über die Folds angeben, nie nur Punktwerte. Überlappen die
Streuungsbereiche zweier Verfahren, **muss das so gesagt werden**.

### R7 — Linearitätsprüfung vor Ridge (harte Auflage)
Wörtlich: *„erstmal plotten, falls keine lineare Baseline, KEIN lineares
Regressionsmodell."* Scatterplots und Residuenanalyse **vor** dem Einsatz von
Ridge, dokumentiert in der Arbeit, gerechnet ausschließlich auf den
Trainingsstadtteilen. Bei Zähldaten ist Negativ-Binomial die naheliegende
Ergänzung — sie ist als Baseline gesetzt.

*Umsetzung:* `modelle/m01_eignung.py` → `results/eignungspruefung/`.
**Steht noch aus** — die vorhandenen Ergebnisse stammen aus der Zeitschnitt-Welt.

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
**KI-Verzeichnis als Tabelle** mit Kennung, Engine, Jahr, Modell, Prompt, URL.

---

## 3. Beibehalten, weil positiv bewertet

Keine kausalen Überdeutungen · detaillierte Beschreibung von Datenquellen und
Zielvariablen · offene Reflexion methodischer Grenzen (mit der Auflage, Probleme
nicht nur zu benennen, sondern zu **lösen**) · angemessener Umfang und
dokumentierte KI-Nutzung.

---

## 4. Formales und Abgabe

- mindestens 3/4 Fließtext, Code nur als Beleg; wissenschaftliche Arbeit, keine
  Projektarbeit; Story und roter Faden
- 30–100 Quellen (Zotero); Lehrbücher für Grundlagen, Paper für den Forschungsstand
- Methodenkapitel so präzise, dass die Arbeit reproduzierbar ist
- Abgabe-Zip max. 250 MB, flüchtige Quellen als PDF/A

**Ins Zip:** `prep/`, `modelle/`, `tests/`, die beiden finalen Parquet-Dateien,
`results/`, `docs/`, `README.md`, `requirements.txt`.
**Nicht ins Zip:** `venv/`, `data/raw/`, `einsaetze.parquet` (Zwischenstand),
`prep/_archiv/` — alles über `requirements.txt` bzw. `s1_daten.py` reproduzierbar.

---

## 5. Kolloquiums-Fragen, auf die eine Antwort vorliegen muss

- „Warum genau diese Algorithmen — und wieso überhaupt Machine Learning?"
- „Warum drei Verfahren für die Regression, aber nur zwei für die Klassifikation?"
- „Hätten Sie es nicht anders machen können? Ist das nicht Overkill?"
- „Wie stellen Sie sicher, dass der Vergleich der Verfahren fair ist?"
- „Warum ist Ihre Validierung leakage-frei?"
- „Warum teilen Sie nach Stadtteilen statt nach der Zeit?"
- „Sie haben nur 35 Stadtteile — wie belastbar sind Ihre Ergebnisse?"
- „Ihre sozioökonomischen Merkmale ändern sich nur jährlich. Was sagt Ihr Modell
  dann eigentlich vorher?"
- „Ist Ridge bei Zähldaten überhaupt das richtige Modell?"
- „Ihre Baseline ist ein Negativ-Binomial-Modell. Ist das nicht schon fast ein
  Modell — und was genau beweist der Vergleich dann?"

Storytelling ist ausdrücklich gewünscht: die Reise vom Problem über die
getroffenen Entscheidungen — **inklusive verworfener Wege** — zum Ergebnis, auch
für den Zweitprüfer nachvollziehbar.
