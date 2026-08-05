# CLAUDE.md — Rahmenplan und Wegweiser

> **Anweisung an Claude:** Diese Datei bei jeder neuen Session zuerst einlesen,
> danach `docs/03_STAND.md`. Diese Datei enthält **keine Ergebniszahlen** — sie
> sagt, wo was steht und nach welchen Regeln gearbeitet wird.

**Arbeit:** „Vorhersage von Feuerwehreinsätzen mittels Machine Learning, ein
Verfahrensvergleich am Beispiel der Stadtteile San Franciscos" (FOM, B.Sc.
Wirtschaftsinformatik) · Betreuer: Prof. Dr. Schröter · 2. Prüfer: Oliver Bach ·
**Abgabe: 07.10.2026** · Kolloquium bis 15.12.2026

Referenzen: Exposé (`expose.pdf`) und Sprechstunden-Mitschrift
(`vorgaben_schroeter.pdf`), beide im Claude-Projekt „Bachelorarbeit".

---

## 1. Die Dokumentation — geschnitten nach Lebensdauer

Der Zuschnitt folgt der Frage, **wodurch eine Datei veraltet**. Wer das
durchbricht, erzeugt genau das Problem, das dieser Aufbau lösen soll: dieselbe
Zahl an sechs Stellen, von denen fünf falsch werden, ohne dass es auffällt.

| Datei | Ändert sich | Inhalt |
|---|---|---|
| `docs/01_VORGABEN.md` | nur wenn Schröter etwas sagt | Auflagen, Gutachten-Regeln R1–R10, Formales, Abgabe, Kolloquiumsfragen |
| `docs/02_ENTSCHEIDUNGEN.md` | wächst, wird nie umgeschrieben | Decision Log: jede Abweichung vom Exposé mit Begründung |
| `docs/03_STAND.md` | **bei jedem `build.py`** | Was die Aufbereitung tut, Datensatz-Steckbrief, Baseline-Werte |
| `docs/04_MODELLIERUNG.md` | wenn sich die Modellplanung ändert | Spezifikation für `modelle/`: Verfahren, Validierungsrahmen, Verbote |
| `docs/06_RISIKEN.md` | wenn ein Risiko eintritt oder wegfällt | Risikoregister der Modellierung, Grundlage für die Sprechstunde |

Die Schreibanleitung für die Kapitel steht als Kommentarblöcke **in `main.tex`**,
nicht in `docs/` — sonst laufen zwei Fassungen derselben Anleitung auseinander.

### Die eine Regel, die den Aufbau trägt

**Jede Ergebniszahl steht in `docs/03_STAND.md` — und nur dort.** Alle anderen
Dateien und die Thesis verweisen darauf, statt Werte abzuschreiben. Nach jedem
`python prep/build.py` wird `03_STAND.md` einmal überschrieben, dann stimmt der
Rest von allein.

Ausnahmen, bewusst: Der Decision Log nennt die Zahlen, die eine Entscheidung
**begründet haben** — die gehören zur Begründung und altern mit ihr. Und
`04_MODELLIERUNG.md` nennt Erwartungswerte aus Vortests, ausdrücklich als solche
markiert und mit Datum.

---

## 2. Forschungsfrage und Unterfragen (wortgetreu aus dem Exposé)

> Inwiefern lassen sich die Häufigkeit und die Art von Feuerwehreinsätzen in den
> Stadtteilen San Franciscos durch sozioökonomische, kriminalitätsbezogene und
> bauliche Merkmale statistisch beschreiben und vorhersagen, und welches der drei
> Verfahren – Ridge Regression, Random Forest oder XGBoost – erzielt dabei die
> höchste Prognosegüte?

1. Lassen sich statistisch nachweisbare Zusammenhänge zwischen sozioökonomischen,
   kriminalitätsbezogenen und baulichen Merkmalen und der stadtteilbezogenen
   Einsatzhäufigkeit bzw. Einsatzart feststellen?
2. Wie unterscheiden sich Ridge Regression, Random Forest und XGBoost hinsichtlich
   ihrer Prognosegüte im Vergleich zu Baselines?
3. Wie verhalten sich die Modelle hinsichtlich Trainings- und Inferenzaufwand?
4. Welche Implikationen ergeben sich für die Modellauswahl bei vergleichbaren
   tabellarischen Prognoseaufgaben?

**Wie die Frage beantwortet wird** (#34, festgelegt vor dem ersten Modelllauf):
über drei einzeln messbare Bausteine — Prognosegüte je Verfahren **gegen die
Stufe-2-Baseline** (UF2), Trainings- und Inferenzaufwand (UF3), daraus die
Eignungsaussage für diesen Datensatz (UF4). Eine Rangfolge *zwischen* den
Verfahren nur, wenn der gepaarte Wilcoxon-Test sie hergibt. Grund: Der Abstand
Verfahren↔Baseline ist messbar, der Abstand Verfahren↔Verfahren bei 29
Entwicklungsstadtteilen nicht.

**Dokumentierte Abweichungen vom Exposé:** Stadtteil-Split statt
zeitreihengerechter Kreuzvalidierung (#29), Klassifikation auf Stadtteil × Monat
statt Einzeleinsatz (#29), zwei statt drei Verfahren in der Klassifikation (#31).
**Stadtteil-Split und beide Baselines sind seit 04.08.2026 freigegeben** (#35,
drei Auflagen daraus in `01_VORGABEN.md`); die übrigen sind begründet und werden
in Kapitel 6 benannt.

---

## 3. Zielgrößen und Verfahren

| Strang | Zielgröße | Typ | Verfahren | Gütemaße |
|---|---|---|---|---|
| Menge | `anzahl_einsaetze` | Zähldaten | Ridge · Random Forest · XGBoost | RMSE, MAE, R² |
| Menge | `einsaetze_je_1000_ew` | stetig | dieselben drei | RMSE, MAE, R² |
| Struktur | `dominante_einsatzart` | 4 Klassen | Random Forest · XGBoost | **Macro-F1**, Macro-AUROC |

Die Klassifikationsmenge ist eine **echte Teilmenge** der Regressionsmenge —
begründet in #31, freigegeben von Schröter am 03.08.2026.

**Baselines in zwei Stufen** (#32, #33). Werte in `03_STAND.md`:

| Strang | Stufe 1 — ohne Merkmale | Stufe 2 — einfachste passende Form |
|---|---|---|
| Menge | Gesamtmittelwert | Negative Binomial |
| Struktur | Mehrheitsklasse | Multinomiale logistische Regression |

Stufe 2 ist die eigentliche Messlatte: Sie benutzt dieselben Merkmale wie die
Vergleichsverfahren, nur in der simpelsten Form, die zur Datenform passt.

---

## 4. Struktur des Repos

Drei Arbeitsschritte, drei Ordner, drei Stufen:

```
prep/         die Daten          config.py · s1_daten.py · s2_datensaetze.py
              Stufe 0            build.py
vorpruefung/  die Messlatte      v1_baselines.py  Stufe 1 (trivial)
              und die Eignung                     Stufe 2 (einfachste passende Form)
              Stufe 1 + 2        v2_eignung.py    welche Verfahrensklasse passt?
                                 run.py
modelle/      der Vergleich      m02_menge.py · m03_struktur.py · m04_shap.py
              Stufe 3            (noch zu schreiben)
tests/                           test_aufbereitung.py
```

**Faustregel:** Erzeugt ein Schritt *Daten*, gehört er nach `prep/`. Legt er
fest, *was ein Modell mindestens leisten muss und warum diese Verfahren*, nach
`vorpruefung/`. Vergleicht er Verfahren, nach `modelle/`.

Schröters Auflage „Baseline gehört in die Data Preparation" (27.07.2026) bleibt
gewahrt — die Baselines stehen weiterhin vor der Modellierung und werden in
Kapitel 5 berichtet, nur in einem eigenen Ordner.

**Fairness-Regel:** Alle Verfahren erhalten exakt denselben Datensatz —
identische Zeilen, Merkmale und Folds. Konstruktiv abgesichert: Die
Fold-Zuordnung steht als Spalte in der Parquet-Datei. Modellspezifische
Transformationen laufen innerhalb der sklearn-Pipeline je Fold.

**`prep/` ist abgeschlossen** und wird nicht mehr angefasst. Die Modellskripte
lesen ausschließlich die fertigen Parquet-Dateien.

### Arbeitsregel für die Implementierung

Jedes Skript in `modelle/` trägt am Ende seines Docstrings einen Block
**Prüfaufträge**. Diese sind nach jedem Lauf abzuarbeiten, nicht nur beim ersten
Mal — sie prüfen, ob das Ergebnis zu den Entscheidungen passt, die es tragen
soll.

Zwei Regeln, die dabei gelten:

- **Ein Ergebnis, das einer Entscheidung widerspricht, ist ein Befund, kein
  Fehler.** Es gehört in `06_RISIKEN.md` und in die Arbeit, nicht wegoptimiert.
  Beispiel: Wenn RF und XGBoost die Stufe-2-Baseline der Klassifikation nicht
  schlagen, lautet das Ergebnis „der Mehraufwand lohnt sich hier nicht".
- **Jede Zahl, die in die Arbeit geht, steht in `03_STAND.md`** — und dort wird
  sie nach jedem Lauf aktualisiert. Zahlen aus älteren Läufen sind die
  häufigste Fehlerquelle dieses Projekts.

---

## 5. CRISP-DM-Status

| Phase | Status |
|---|---|
| Business Understanding | ✅ Exposé, Kap. 1/4 |
| Data Understanding | ✅ Eignungsprüfung gerechnet (`vorpruefung/`), fünf Belege |
| Data Preparation | ✅ **abgeschlossen** — ein Befehl, zwei Datensätze, 19/19 Prüfungen |
| Modeling | ⬜ `modelle/` vollständig neu zu schreiben, Spezifikation in `04_MODELLIERUNG.md` |
| Evaluation | ⬜ Hold-out unberührt |
| Deployment | ⬜ nicht Teil der Arbeit (Limitation, vgl. Schröer et al. 2021) |

**Nächster Schritt:** `modelle/m02_menge.py`. Offener Punkt vorher: Im
Klassifikationsstrang schlägt Stufe 2 die Baum-Sonde — der Mehraufwand von RF
und XGBoost ist dort vorab **nicht** belegt (`06_RISIKEN.md`, R-2).

---

## 6. Mapping Analyse → Gliederung

| Kapitel | Inhalt | Artefakt |
|---|---|---|
| 4 Anwendungsfall & Daten | Datenquellen, Variablen, Zielgrößen | `docs/03_STAND.md` |
| 5 Data Preparation | Aufbereitung Schritt für Schritt, Split, Baselines | `docs/03_STAND.md`, `prep/` |
| 6 Modelling | Eignungsprüfung, Verfahrenswahl, Tuning | `modelle/`, `docs/04_MODELLIERUNG.md` |
| 7 Evaluation | Fold-Ergebnisse, Baselines, Laufzeiten | `results/` |
| 8 Diskussion | Verfahrensvergleich, Limitationen | `docs/02_ENTSCHEIDUNGEN.md` |
| 9 Fazit | Beantwortung der Forschungsfrage | – |

---

## 7. KI-Verzeichnis

Format nach Auflage Schröter (27.07.2026): Kennung, Engine, Jahr, Modell,
Prompt, URL. Die Kennung wird im Text wie eine Quelle zitiert.

| Kennung | Engine | Jahr | Modell | Prompt (Kurzfassung) | URL |
|---|---|---|---|---|---|
| anthropic2026a | Anthropic Claude | 2026 | Claude Fable | „Lies Exposé und Betreuer-Vorgaben sowie die bestehende Data-Prep-Pipeline. Erstelle (1) einen persistenten Rahmenplan CLAUDE.md, (2) eine empirische Eignungsprüfung von Ridge/Random Forest/XGBoost auf dem Pipeline-Output, (3) eine minimale Demo-Modellierungspipeline, ohne die bestehende Prep-Pipeline zu verändern." | https://claude.ai |
| anthropic2026b | Anthropic Claude | 2026 | Claude Fable | „Analysiere mögliche Hürden bei der weiteren Bearbeitung und Lösungen, damit alle Algorithmen wertvolle und richtige Erkenntnisse liefern. Schreibe eine Markdown-Datei, die die Algorithmen einzeln in Teilschritten anleitet, inkl. korrekter SHAP-Verwendung." | https://claude.ai |
| anthropic2026c | Anthropic Claude | 2026 | Claude Fable | „Analysiere, wie wir die Probleme bereits in der Prep-Pipeline adressieren können, mache einfache Anpassungen, validiere per Demo-Test und schreibe eine Stichpunkt-Zusammenfassung für das Kapitel Data Preparation. Untersuche, ob zusätzliche Attribute die Probleme beheben und welche Baseline nötig ist." | https://claude.ai |
| anthropic2026d | Anthropic Claude | 2026 | Claude Opus | „Prüfe eine externe Kritikliste mit 11 Preprocessing-Prüfaufträgen gegen den tatsächlichen Repo-Stand und gib aus, was im Preprocessing noch zu erledigen ist und welche Entscheidungen zu treffen sind." | https://claude.ai |
| anthropic2026e | Anthropic Claude | 2026 | Claude Opus | „Baue einen relativen Kriminalitätsindex je Stadtteil und Monat aus beiden SFPD-Datensätzen, setze den Analysezeitraum dauerhaft fest und beschreibe, was für eine langfristig korrekte Data Preparation noch zu tun ist." → Decision Log #17–#19 | https://claude.ai |
| anthropic2026f | Anthropic Claude | 2026 | Claude Opus | „Rechne die Eignungsprüfung neu (nur auf Trainingsdaten) und entwirf den Klassifikationsteil." → Decision Log #20 | https://claude.ai |
| anthropic2026g | Anthropic Claude | 2026 | Claude Opus | „Räume Dokumentation und Hilfsskripte auf, setze alles auf den neuesten Stand und analysiere anhand von Exposé, Vorgaben, Pipeline und Ergebnissen, was bei der Modellierung noch zu Fehlern führen kann." | https://claude.ai |
| anthropic2026h | Anthropic Claude | 2026 | Claude Opus | „Setze die Priorität-1-Punkte des Audits um (Randmonat-Bugfix, Exposure-Kontrolle, ACS-Publikationsversatz, End-Hold-out) und beschreibe die Änderungen." → Decision Log #11–#16 | https://claude.ai |
| anthropic2026i | Anthropic Claude | 2026 | Claude Opus | „Erkläre das Verhältnis von `pipeline/03_features.py` und `modellierung/features.py` und gib einen Überblick, welche Dateien den finalen Datensatz erzeugen." | https://claude.ai |
| anthropic2026j | Anthropic Claude | 2026 | Claude Opus | „Entwickle eine Idee, wie sich der Code zu einer klaren Preprocessing-Pipeline aufräumen lässt: ein Ordner, ein Befehl, ein bis zwei finale Datensätze. Setze die Zielstruktur um und sorge dafür, dass die Eignungsprüfung validieren kann, ob die drei Algorithmen zum Datensatz passen." → Decision Log #22, #23 | https://claude.ai |
| anthropic2026k | Anthropic Claude | 2026 | Claude Fable | „Prüfe, ob noch Anpassungen in der Data Preparation nötig sind, und schreibe Kapitel 5 der Arbeit in LaTeX mit ausgewählten, erklärten Code-Snippets." → Audit-Fix #10, Decision Log #5 | https://claude.ai |
| anthropic2026l | Anthropic Claude | 2026 | Claude Opus | „Verdichte `prep/` auf drei Schritte, lege die Baselines dazu, verschiebe die Eignungsprüfung nach `modelle/` und reduziere die Dokumentation." → Decision Log #25, #26 | https://claude.ai |
| anthropic2026m | Anthropic Claude | 2026 | Claude Opus | „Lege Schröters E-Mail vom 03.08.2026 zur Algorithmenauswahl über den bisherigen Stand und analysiere, was das konkret für die Preprocessing-Pipeline heißt. Gibt es eine Zusammenstellung, bei der zwei der drei Regressionsverfahren auch die Klassifikation abdecken?" → Decision Log #31, Anpassung von Kapitel 6.2, 7.2 und den Limitationen in `main.tex` | https://claude.ai |
| anthropic2026n | Anthropic Claude | 2026 | Claude Opus | „Erkläre in einfachen Worten, was die Baselines berechnen und ob die Negative Binomial für Regression und Klassifikation getrennt stützt, dass weitere Modelle verwendet werden. Schreibe einen minimalen Baseline-Code mit diesen Entscheidungen und räume anschließend die gesamte Dokumentation auf den aktuellen Stand auf." → Decision Log #32, Neufassung von `prep/s3_baselines.py` (inzwischen `vorpruefung/v1_baselines.py`), Neuschnitt der Dokumentation nach Lebensdauer (`docs/01`–`05`) | https://claude.ai |
