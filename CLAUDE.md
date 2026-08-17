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
| `docs/07_BEFUNDE.md` | wächst während der Implementierung | Was beim Bauen aufgefallen ist: lückenhafte Spezifikation, Ergebnisse gegen Entscheidungen, Annahmen. Grundlage für Kapitel 8 und die kritische Reflexion |

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
| Menge | Gesamtmittelwert | **Poisson-GLM mit Offset** (#45) |
| Struktur | Mehrheitsklasse | Multinomiale logistische Regression |

Stufe 2 ist die eigentliche Messlatte: Sie benutzt dieselben Merkmale wie die
Vergleichsverfahren, nur in der simpelsten Form, die zur Datenform passt. Beide
sind verallgemeinerte lineare Modelle mit kanonischem Link, per unpenalisierter
Maximum-Likelihood angepasst, **ohne freien Hyperparameter** (#45).

---

## 4. Struktur des Repos

Drei Arbeitsschritte, drei Ordner, drei Stufen:

```
prep/         die Daten          config.py · s1_daten.py · s2_datensaetze.py
              Stufe 0            build.py
vorpruefung/  die Messlatte      v0_aufteilung.py  wiederholte Splits
              und die Eignung    v1_baselines.py   Stufe 1 + 2
              Stufe 1 + 2        v2_eignung.py     welche Verfahrensklasse passt?
                                 v3_spezifikation.py  haelt die Nichtlinearitaet
                                                   out-of-sample? (B-41)
                                 run.py
modelle/      der Vergleich      m02_menge.py · m03_struktur.py · m04_shap.py
              Stufe 3            m05_abbildungen.py · config_modelle.py
tests/                           test_aufbereitung.py
tools/        NICHT ABGABE       pruefe_zahlen.py       Doku gegen results/
                                 codebook.py            Merkmalstabelle Kap. 4
                                 aufraeumen.py          verwaiste Artefakte
                                 sichere_ergebnisse.py  results/ nach archiv/
                                 suchdiagnose.py        war die Suche am Limit?
archiv/       NICHT ABGABE       gesicherte Ergebnisstände mit Manifest
```

`tools/` wird vor dem Packen des Abgabe-ZIP gelöscht. **Eine Ausnahme:** Die
Ausgabe von `codebook.py` gehört zur Arbeit — `results/codebook/merkmale.md`
ist die große Merkmalstabelle aus Kapitel 4 und ist deshalb selbsttragend
geschrieben.

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
| Modeling | ✅ **abgeschlossen** — finaler Lauf **16.08.2026**, Budget 100, Suchräume nach #49 |
| Evaluation | ✅ **abgeschlossen** — Hold-out einmalig ausgewertet, beide Stränge |
| Deployment | ⬜ nicht Teil der Arbeit (Limitation, vgl. Schröer et al. 2021) |

**Nächster Schritt:** Kapitel 6 bis 9 schreiben. Gerechnet ist alles; seit dem
08.08.2026 ist zudem keine Festlegung mehr unabgestimmt (#47).

**Stand 16.08.2026 — finaler Lauf durch.** Budget 100 und vier erweiterte
Suchräume (#49, #50), Hauptanalyse vorab festgelegt (#52). Neue Ergebnisse:
Ridge **und XGBoost** liegen gesichert hinter der Stufe-2-Baseline, keine
Verfahrenspaarung ist trennbar (R-1 vollständig eingetreten), die Überanpassung
ist beziffert (#51, B-46) und die Faktorgruppen sind nicht nur attribuiert,
sondern abladiert (B-47). Zahlen ausschließlich in `03_STAND.md` §5 — der
Zahlenwächter deckt 116 Werte ab und läuft fehlerfrei.

**Stand 11.08.2026.** Aus der Sprechstunde vom 10.08. sind umgesetzt: die
Anforderungstabelle je Verfahren mit drei formalen Tests (`03_STAND.md` §7,
Abbildung A10) und das Codebook mit Skalenniveau
(`results/codebook/merkmale.md`). Offen bleiben daraus die rund 20
Codeausschnitte, die Komplexität des „V" in E-V-A, die Formalregeln beim
Schreiben und ein Demo-Modus fürs Kolloquium — alles in `01_VORGABEN.md`,
Abschnitt 0. Zwei Dokumentationsfehler sind behoben und als B-43 und B-44
verzeichnet.

Der vollständige Lauf, falls er wiederholt werden muss — Reihenfolge ist
verbindlich, `m05` liest alles Vorherige:

```
python tools/sichere_ergebnisse.py <name>   #    ZUERST: results/ sichern   < 1 min
python prep/build.py                        # 0  zwei Datensätze            ~2 min
python tests/test_aufbereitung.py           #    19 Prüfungen               ~1 min
python vorpruefung/v0_aufteilung.py         # 1  Selbsttest der Aufteilung  < 1 min
python vorpruefung/v1_baselines.py          #    Messlatte, 10 Wdh.         ~1 min
python vorpruefung/v2_eignung.py            #    Eignung + Annahmen (§6)    ~1 min
python vorpruefung/v3_spezifikation.py      #    Gegenprobe                 ~2 min
python modelle/m02_menge.py holdout         # 2  der lange Teil            ~55 min
python modelle/m03_struktur.py holdout      #    Klassifikation            ~45 min
python modelle/m04_shap.py                  #    SHAP, Ablation, VIF       ~10 min
python modelle/m05_abbildungen.py           #    zehn Abbildungen          < 1 min
python tools/codebook.py                    # 3  Merkmalstabelle Kap. 4    < 1 min
python tools/pruefe_zahlen.py               #    Doku gegen results/       < 1 min
```

**Sichern ist kein Ritual.** `results/` ist die einzige Stelle, an der die
Ergebnisse liegen, sie steht in `.gitignore`, und jeder Lauf überschreibt sie.
Ohne Kopie ist ein Lauf mit geänderter Konfiguration unumkehrbar. Wichtig
dabei: **erst sichern, dann die Konfiguration ändern** — das Manifest liest
`config_modelle.py` live und kann nicht wissen, womit die Dateien entstanden
sind.

Rund **zwei Stunden**, seit #49/#50 eher **drei** — die Tuningphase steigt von
66 auf rund 139 Minuten. `v1` und `v2` lassen sich auch als `python
vorpruefung/run.py` in einem Zug starten — die Reihenfolge ist dort zwingend,
weil `v2` die Baseline-Werte liest.

**Das Argument `holdout` ist Absicht.** Ohne es bleiben die
Hold-out-Stadtteile unerreichbar; `main()` filtert sie heraus, bevor
irgendetwas rechnet. Es gehört nur in einen bewusst als Schlussbewertung
gefahrenen Lauf.

**Was sich reproduzieren MUSS** (alles auf `RANDOM_STATE = 42`): Gütemaße,
Hyperparameter, Baselines, SHAP-Beiträge, Spezifikationsgegenprobe. Weicht
etwas ab, ist das ein Befund für `07_BEFUNDE.md` und kein Grund, den Lauf zu
wiederholen, bis er passt.

**Was sich zwangsläufig ändert:** sämtliche Laufzeiten, `parallel_gewinn` und
`parallel_abweichung` (B-24, XGBoost ist nicht threaddeterministisch). Betroffen
sind `03_STAND.md` §5.4, die Abbildungen A4 und A9 sowie die
Verhältnisprüfung im Zahlenwächter. Dass er danach Fehler meldet, ist sein
Zweck — die Laufzeitzahlen sind nachzuziehen.

Danach je Skript den Block **Prüfaufträge** am Ende des Docstrings abarbeiten
und `03_STAND.md` überschreiben. `tools/pruefe_zahlen.py` meldet mit Exit-Code 1,
welche Stelle der Dokumentation nicht mehr zu `results/` passt — der Ordner
`tools/` gehört nicht zur Abgabe.

**Ergebnis des Klassifikationsstrangs** (stand hier zuvor als offener Punkt):
In der Kreuzvalidierung schlagen Random Forest und XGBoost die Stufe-2-Baseline
in 10 von 10 Wiederholungen, auf dem Hold-out verlieren beide gegen sie
(`06_RISIKEN.md`, R-2). Eine Rangfolge zwischen Logit und Baumverfahren ist
damit nicht zulässig; die berichtbare Aussage lautet „der Mehraufwand ist im
Strukturstrang nicht belegt" — genau der Fall, der vorab vorgesehen war.

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
| anthropic2026o | Anthropic Claude | 2026 | Claude Opus | „Implementiere die Modellierung: die fünf offenen Funktionen in `m02_menge.py`, dann `m03_struktur.py`, eine Negative-Binomial-Variante ohne Offset in `v1_baselines.py`, danach `m04_shap.py` und `m05_abbildungen.py`. Frage nach, wenn etwas unklar ist, und dokumentiere alle Befunde." → Decision Log #36–#41, `docs/07_BEFUNDE.md` mit 22 Einträgen, `vorpruefung/v0_aufteilung.py` | https://claude.ai |
| anthropic2026p | Anthropic Claude | 2026 | Claude Opus | „Schau dir das Projekt an: Wie lange dauerte eine Umwandlung von `modelle/` in Jupyter Notebooks? Welche weiteren Abbildungen sind möglich? Wie ließe sich der Code kürzen? Gibt es alte Dateien, die nicht mehr gebraucht werden?" → Abbildungen A6–A10 in `m05_abbildungen.py`, `tools/aufraeumen.py`, Fund der beiden Dokumentationsfehler | https://claude.ai |
| anthropic2026q | Anthropic Claude | 2026 | Claude Opus | „Analysiere die Sprechstunden-Mitschrift vom 10.08.2026 und ihre Auswirkungen auf das Projekt. Behebe die gefundenen Fehler nachhaltig und setze die Auflagen um." → `tools/codebook.py`, Abschnitt 6 der Eignungsprüfung mit Cameron & Trivedi, Breusch-Pagan und Jarque-Bera, `03_STAND.md` §7, fünfte Strukturprüfung im Zahlenwächter, Decision-Log-Befunde B-43 und B-44 | https://claude.ai |
| anthropic2026r | Anthropic Claude | 2026 | Claude Opus | „Schaetze den Aufwand einer Umwandlung von `modelle/` in Jupyter Notebooks, analysiere moegliche weitere Abbildungen, pruefe Kuerzungspotenzial im Code und suche verwaiste Dateien.“ → Abbildungen A6-A9, `tools/aufraeumen.py`, Fund zweier Dokumentationsfehler (B-43, B-44) | https://claude.ai |
| anthropic2026s | Anthropic Claude | 2026 | Claude Opus | „Behebe die beiden gefundenen Fehler nachhaltig, setze die offenen Auflagen der Sprechstunde vom 10.08. um und ziehe die Dokumentation glatt.“ → `tools/codebook.py`, Abschnitt 6 der Eignungspruefung mit drei formalen Tests, Abbildung A10, fuenfte Strukturpruefung im Zahlenwaechter | https://claude.ai |
| anthropic2026t | Anthropic Claude | 2026 | Claude Opus | „Pruefe, ob Tuning-Budget und Suchraeume belegbar sind; baue eine Diagnose, die misst, ob mehr Ziehungen oder weitere Raeume etwas bringen.“ → `tools/suchdiagnose.py`, Verifikation der Formel bei Bergstra und Bengio im Volltext, Decision Log #49 und #50 | https://claude.ai |
| anthropic2026u | Anthropic Claude | 2026 | Claude Opus | „Belege, ob Ueberanpassung vorliegt, sichere die vorherigen Ergebnisse ab und bringe den Code auf einen finalen Stand.“ → Trainingsguete je Lauf (#51), Faktorgruppen-Ablation in `m04_shap.py`, `tools/sichere_ergebnisse.py`, Decision Log #52 | https://claude.ai |
| anthropic2026v | Anthropic Claude | 2026 | Claude Opus | „Werte den finalen Lauf aus und ziehe die gesamte Dokumentation nach; arbeite die Befunde in die LaTeX-Datei ein.“ → `03_STAND.md` Abschnitte 5 und 7, Befunde B-45 bis B-47, Nachzug von R-1 und R-2, Schreibanleitungen zu Kapitel 6.4, 7.3 und 7.4 in `main.tex` | https://claude.ai |
