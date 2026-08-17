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
| Baseline gehört in die Data Preparation | 27.07. | `vorpruefung/v1_baselines.py`, Kapitel 5.4 |
| Die Baseline muss zum Anwendungsfall passen — bei nichtlinearen Zusammenhängen braucht es eine nichtlineare Baseline | 27.07. | siehe unten |
| Keine Blindabsätze zwischen Gliederungsebenen | 27.07. | jede Ebene trägt Inhalt |
| Zitate einheitlich, mehrere Quellen pro Seite | 27.07. | biblatex/Chicago-notes |
| Jede methodische Entscheidung mit dem Stand der Forschung belegen | 27.07. | — |
| KI-Verzeichnis: Kennung, Engine, Jahr, Modell, Prompt, URL | 27.07. | ✅ Form geklärt, Verzeichnis in `CLAUDE.md` |
| Regression und Klassifikation dürfen unterschiedlich viele Verfahren nutzen, wenn die Auswahl fachlich begründet wird | 03.08. | Decision Log #31, Kapitel 6.2 |
| **Stadtteil-Split freigegeben** — „methodisch gut begründet" | 04.08. | #29 |
| Die Abweichung von der zeitreihengerechten Kreuzvalidierung **transparent erläutern** | 04.08. | Kapitel 8 |
| Die Zielsetzung der Validierung als **„Generalisierung auf unbekannte Stadtteile"** formulieren | 04.08. | Kapitel 5.4 und Zielsetzung — wörtlich so |
| ~~Negative Binomial und multinomiale logistische Regression als Baselines freigegeben~~ | 04.08. | überholt durch die Freigabe vom 08.08. |
| Für alle Vergleichsmodelle **identische Merkmale und Splits** verwenden | 04.08. | erfüllt; Mechanismus in Kapitel 5.4 belegen |
| **Einheitliche Spezifikation über die Einwohnerzahl freigegeben** — „plausibel" | 08.08. | #43, Kapitel 6 |
| **Poisson und Logit ohne Strafterm freigegeben** — „methodisch sauber, vermeidet willkürliche Parameter und liefert zudem stärkere Vergleichswerte" | 08.08. | #45 |
| **Auflage D: „Dokumentieren Sie diese Begründung genau so."** | 08.08. | Kapitel 5.4 — siehe unten |
| **Primärtest auf zehn Werten und Holm-Bonferroni freigegeben** — „sauber umgesetzt"; unkorrigierter Einzeltest der Klassifikation „folgerichtig" | 08.08. | #37, #38, Kapitel 7 |
| **Anforderungen je Verfahren darstellen**, Tabelle oder Prosa, mit Teststatistik und p-Wert; QQ-Plot genannt | 10.08. | `03_STAND.md` §7, `v2_eignung` Abschnitt 6, Abbildung A10 |
| **„Keine lineare Baseline, wenn es keine Linearität gibt"** — Wiederholung von R7 | 10.08. | beantwortet über den Log-Link, siehe R7 unten |
| **Codebook und Variablenbuch mit Skalenniveau**, eine große Tabelle, Was/Wie/Wofür — *nicht* je Merkmal eine deskriptive Statistik | 10.08. | `results/codebook/merkmale.md`, erzeugt von `tools/codebook.py` |
| **Rund 20 nicht-triviale Codeausschnitte**, ausdrücklich Serialisierung und Multithreading | 10.08. | **offen** — Kandidaten stehen in `07_BEFUNDE.md` (B-16, B-23, B-24) |
| **Komplexität des „V" in E-V-A beziffern** | 10.08. | ✅ Kennzahl steht: **10.000 Modellanpassungen** (25 Suchläufe × 100 Ziehungen × 4 innere Folds), 139 Minuten Suchzeit; dazu 600 Bewertungsläufe, 200 Spezifikationsanpassungen, 1.200 Ablationsanpassungen. Schreibanleitung in `main.tex` §7.3 |
| **Alle Analysen in die Arbeit**, auch verworfene | 10.08. | Ablation, Spezifikationsgegenprobe, Leakage-Diagnose, Extrapolation — als **Sensitivitätsanalysen** gerahmt, nicht als Chronologie |
| **Keine große iterative Entwicklung im Text** — eine spätere Änderung wird als Plan A von vornherein dargestellt | 10.08. | **Darstellungsregel**: Kapitel 6 beschreibt die finale Spezifikation, die Reflexion steht konzentriert in Kapitel 8 |
| **Signifikanz ist nicht zwingend**, methodisch sauber muss es sein | 10.08. | entlastet R-2 — der Nichtbefund des Strukturstrangs ist berichtbar |
| **Abbildungen müssen nicht beschrieben werden**, aber begründet: warum diese Daten, diese Abbildung, diese Darstellung | 10.08. | Begründungen stehen in den Docstrings von `m05_abbildungen.py` |
| **Formales**: keine Kursivschrift, keine Anführungszeichen, keine Unterstreichungen · „o.A." bei unbekanntem Autor · p-Werte 95/99/99,5, Abweichung möglich | 10.08. | **offen** — beim Schreiben umzusetzen |
| **Seitenbudget**: zu den 40–60 Seiten zählen alle Inhaltsseiten inklusive Code und Tabellen, **Abbildungen nicht** | 10.08. | verschiebt die Rechnung: Abbildungen sind kostenlos, die 20 Codeausschnitte teuer |
| **Kolloquium**: kein KI-Text vorlesen, Live-Demo | 10.08. | **offen** — Demo-Modus nötig, ein voller Modelllauf dauert rund 90 Minuten |

### Drei Punkte der Sprechstunde vom 10.08., geklärt am 11.08.

| Punkt | Klärung |
|---|---|
| „Keine Zwischenfazits = **Blindabsätze nutzen**" gegen die Auflage vom 27.07. („keine Blindabsätze zwischen Gliederungsebenen") | **Blindabsätze nutzen.** Gemeint sind verbindende statt zusammenfassende Absätze; die Auflage vom 27.07. betraf leere Überleitungen zwischen zwei Überschriften |
| „RAG-Pipeline zeigen" | Diese Arbeit hat keine RAG-Komponente. Wird im Kolloquium **nicht gezeigt** |
| „Hyperparameteroptimierung bei jedem Algorithmus — keine Ausrede" | Bezieht sich auf die drei Vergleichsverfahren; die sind alle mit `RandomizedSearchCV`, **Budget 100** (#50, hergeleitet aus Bergstra & Bengio 2012), `GroupKFold(4)` getunt. Die **Stufe-2-Baselines haben keinen freien Hyperparameter** — das ist Auflage D vom 08.08., in Schröters eigenen Worten. Der Verzicht ist dort wörtlich zu belegen, sonst liest er sich wie die Ausrede, die diese Auflage ausschließt |

### Auflage D — die Begründung der Baselines, wörtlich zu übernehmen

Schröter hat am 08.08.2026 nicht nur zugestimmt, sondern die Begründung selbst
formuliert und angewiesen, sie **genau so** zu dokumentieren. Seine drei
Argumente, in seiner Reihenfolge:

1. Die Reduktion auf die einfacheren Varianten ist **methodisch sauber**.
2. Sie **vermeidet willkürliche Parameter**.
3. Sie **liefert zudem stärkere Vergleichswerte**.

Das gehört in Kapitel 5.4, wo die Stufe-2-Baselines eingeführt werden. Punkt 2
ist dabei der tragende: Ein Strafterm auf dem Vorgabewert der Software wäre ein
willkürlich gesetzter Parameter, und ein getunter wäre eine Wahl, die zu
begründen wäre. Die unpenalisierte Maximum-Likelihood-Anpassung hat nichts zu
wählen.

**Ein Vorbehalt, der dazugehört:** Punkt 3 trifft auf den Mengenstrang zu (die
Latte steigt von 37,27 auf 33,98 RMSE), auf den Strukturstrang jedoch nicht —
dort sinkt sie von 0,314 auf 0,297 Macro-F1. Beide Zahlen standen in der Anfrage
vom 08.08., die Freigabe erfolgte also in Kenntnis. In Kapitel 5.4 ist der
Unterschied trotzdem zu benennen, statt Punkt 3 pauschal zu übernehmen —
`06_RISIKEN.md`, R-2.

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

Gewählt wurde stattdessen ein **verallgemeinertes lineares Modell mit
kanonischem Link als Stufe-2-Referenz** — für die Menge ein **Poisson-GLM mit
Offset**, für die Struktur ein **multinomiales Logit ohne Strafterm**
(Decision Log #32/#33, neu gefasst mit #45, freigegeben 08.08.). Die Begründung
ist stärker als die alte:

1. Es ist **kein Strohmann.** Es bekommt dieselben zwölf Merkmale, dieselben
   Zeilen und dieselben Folds wie die Vergleichsverfahren — ein vollwertiges
   statistisches Modell, kein konstanter Vergleichswert.
2. Es ist **auf der Originalskala nichtlinear.** Über den Log-Link wirkt es
   multiplikativ, bildet also überproportionale Effekte ab. Ein einfacher
   Durchschnitt kann das nicht, ein lineares Modell auf der Rohskala auch nicht.
3. Es ist **verteilungsgerecht für Zähldaten mit Exposition.** Die Bevölkerung
   geht als Offset ein, das Modell schätzt also Einsätze je Einwohner statt der
   Stadtteilgröße. Die Überdispersion (Dispersionsindex 62,8) beschädigt beim
   Poisson-Schätzer nur die Standardfehler, nicht die Konsistenz des bedingten
   Mittelwerts (Gourieroux, Monfort & Trognon 1984) — und Standardfehler
   verwendet eine Baseline mit reinen Punktvorhersagen nicht.
4. Es **zieht genau die relevante Trennlinie.** Was es nicht kann, sind
   Wechselwirkungen zwischen Merkmalen — und genau die finden Random Forest und
   XGBoost konstruktionsbedingt. Damit ist der Vergleich aussagekräftig: Schlagen
   die Baumverfahren es, existieren solche Wechselwirkungen und der Mehraufwand
   ist gerechtfertigt. Schlagen sie es nicht, reicht die einfachere Struktur.
5. Es hat **keinen frei wählbaren Parameter.** Das ist Schröters eigenes Argument
   vom 08.08. und die Grundlage von Auflage D: „methodisch sauber, vermeidet
   willkürliche Parameter".

**Warum Poisson und nicht Negative Binomial** (#45): Die Negative Binomial wäre
die Erweiterung für korrekte **Inferenz**. Sie löst ein Problem, das diese
Baseline nicht hat, und bringt mit dem Dispersionsparameter eine zusätzliche
Größe mit — sie ist damit nicht mehr „die einfachste Form, die zur Datenform
passt". Gemessen ist das Poisson-GLM zudem die härtere Latte.

**Die Klassifikation hat seit #33 eine Entsprechung.** Frühere Fassungen dieses
Abschnitts hielten fest, dort bleibe nur die Mehrheitsklasse. Das gilt nicht
mehr: Das multinomiale Logit ist das Gegenstück zum Poisson-GLM — dieselbe
Modellklasse, derselbe kanonische Link, derselbe Verzicht auf einen Strafterm.
**Zu benennen bleibt**, dass die Latte dort trotzdem niedriger liegt als in der
Regression (`06_RISIKEN.md`, R-2).

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
in Decision Log #29. Die zeitreihengerechte Variante wird **nicht gerechnet**,
sondern in Kapitel 8 begründet verworfen — ein zweiter Validierungsrahmen
verstieße gegen R1 und R8, und der Beleg liegt vor (der Stadtteil-Mittelwert
allein erklärt R² 0,925). **Diese Abweichung ist mit Schröter zu besprechen.**

### R6 — Baselines und ehrliche Vergleichsaussagen
Baselines sind Pflicht, je Zielgröße, und sie stehen in **zwei Stufen**
(Decision Log #32/#33, neu gefasst mit #45, freigegeben 08.08.):

| Strang | Stufe 1 — ohne Merkmale | Stufe 2 — Messlatte |
|---|---|---|
| Menge | Gesamtmittelwert | **Poisson-GLM mit Offset** |
| Struktur | Mehrheitsklasse | **Multinomiales Logit ohne Strafterm** |

Die Primäraussage läuft gegen **Stufe 2**, nicht gegen Stufe 1 (#34).
Unterschiede nicht überinterpretieren — Mittelwert ± Standardabweichung über
die Folds angeben, nie nur Punktwerte. Überlappen die Streuungsbereiche zweier
Verfahren, **muss das so gesagt werden**.

### R7 — Linearitätsprüfung vor Ridge (harte Auflage)
Wörtlich: *„erstmal plotten, falls keine lineare Baseline, KEIN lineares
Regressionsmodell."* Scatterplots und Residuenanalyse **vor** dem Einsatz von
Ridge, dokumentiert in der Arbeit, gerechnet ausschließlich auf den
Trainingsstadtteilen. Am 10.08.2026 wiederholt: *„keine lineare Baseline, wenn
es keine Linearität gibt."*

Beantwortet ist die Auflage nicht durch eine Negativ-Binomial-Baseline — diese
frühere Fassung ist mit #45 hinfällig —, sondern durch die **Wahl des Links**:
Das Poisson-GLM ist über den Log-Link auf der Originalskala nichtlinear und
wirkt multiplikativ. Die ausführliche Begründung steht oben in Abschnitt 0
unter „Wie die Auflage ‚nichtlineare Baseline' beantwortet wird", Punkt 2.

*Umsetzung:* `vorpruefung/v2_eignung.py` → `results/eignungspruefung/`.
**Gerechnet**, auf den Trainingsstadtteilen von Fold 1. Abschnitt 1 des
Berichts ist am 10.08.2026 neu gefasst worden: Er schloss zuvor aus der
Überdispersion, Poisson scheide aus — also gegen die eigene Umsetzung nach #45.

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
