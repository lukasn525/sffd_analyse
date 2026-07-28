# CLAUDE.md – Rahmenplan Bachelorarbeit (verbindlicher Projekt-Kontext)

> **Anweisung an Claude:** Diese Datei bei jeder neuen Session zuerst einlesen,
> danach `docs/01_PIPELINE.md`. Bei Änderungen im Projektverlauf eigenständig
> aktualisieren – insbesondere Status und `docs/02_ENTSCHEIDUNGEN.md`.

**Arbeit:** „Vorhersage von Feuerwehreinsätzen mittels Machine Learning, ein
Verfahrensvergleich am Beispiel der Stadtteile San Franciscos" (FOM, B.Sc.
Wirtschaftsinformatik) · Betreuer: Prof. Dr. Schröter · 2. Prüfer: Oliver Bach ·
**Abgabe: 07.10.2026** · Kolloquium bis 15.12.2026

Referenzen: Exposé (`expose.pdf`) und Sprechstunden-Mitschrift
(`vorgaben_schroeter.pdf`), beide im Claude-Projekt „Bachelorarbeit".

## Dokumente

| Datei | Inhalt |
|---|---|
| `CLAUDE.md` (diese Datei) | Rahmen, Status, KI-Verzeichnis |
| `docs/01_PIPELINE.md` | **was die Aufbereitung tut und was hinten rauskommt – zuerst lesen** |
| `docs/02_ENTSCHEIDUNGEN.md` | Decision Log: jede Abweichung vom Exposé mit Begründung |
| `docs/03_VORGABEN.md` | Schröter-Auflagen, Gutachten-Regeln R1–R10, Formales, Abgabe |
| `README.md` | Setup und Ausführung |

---

## 1. Forschungsfrage & Unterfragen (wortgetreu aus dem Exposé)

**Zentrale Forschungsfrage:**
> Inwiefern lassen sich die Häufigkeit und die Art von Feuerwehreinsätzen in den
> Stadtteilen San Franciscos durch sozioökonomische, kriminalitätsbezogene und
> bauliche Merkmale statistisch beschreiben und vorhersagen, und welches der drei
> Verfahren – Ridge Regression, Random Forest oder XGBoost – erzielt dabei die
> höchste Prognosegüte?

**Unterfragen:**
1. Lassen sich statistisch nachweisbare Zusammenhänge zwischen sozioökonomischen,
   kriminalitätsbezogenen und baulichen Merkmalen und der stadtteilbezogenen
   Einsatzhäufigkeit bzw. Einsatzart feststellen?
2. Wie unterscheiden sich Ridge Regression, Random Forest und XGBoost hinsichtlich
   ihrer Prognosegüte (RMSE/MAE/R² bzw. F1/AUROC) im Vergleich zu naiven Baselines,
   bewertet mittels zeitreihengerechter Kreuzvalidierung?
3. Wie verhalten sich die Modelle hinsichtlich Trainings- und Inferenzaufwand?
4. Welche Implikationen ergeben sich für die Modellauswahl bei vergleichbaren
   tabellarischen Prognoseaufgaben?

## 2. Zielgrößen

| Zielgröße | Typ | Ebene | Gütemaße |
|---|---|---|---|
| Einsatzhäufigkeit (`anzahl_einsaetze`) | Regression (Zähldaten) | Stadtteil × Monat | RMSE, MAE, R² |
| Einsatzart (`einsatzart_gruppe`, 4 NFIRS-Serien) | Klassifikation; binär `ist_brand` als Robustheitslauf | Einzeleinsatz | Macro-F1, Macro-AUROC |

Ergänzend: Negative-Binomial-Regression als interpretierbare Count-Baseline,
Trainings-/Inferenzzeiten, SHAP zur Interpretation.

## 3. Struktur des Repos

```
prep/     erzeugt Daten      config.py · s1_daten.py · s2_datensaetze.py
                             s3_baselines.py · build.py
modelle/  rechnet Zahlen     m01_eignung.py · m02_regression.py · m03_klassifikation.py
tests/    prüft die Dateien  test_aufbereitung.py  (14 Prüfungen)
```

**Faustregel:** Erzeugt ein Schritt *Daten*, gehört er nach `prep/`. Erzeugt er
*Zahlen über Daten*, nach `modelle/`. Ausnahme mit Begründung: die Baselines
liegen in `prep/s3_baselines.py`, weil Schröter sie in der Data Preparation
verlangt (27.07.2026). `python prep/build.py` endet mit den zwei finalen
Datensätzen und den Referenzwerten.

**Fairness-Regel:** Alle drei Modelle erhalten exakt denselben Datensatz –
identische Zeilen, Merkmale und Folds. Konstruktiv abgesichert: Die Fold-Zuordnung
steht als Spalte in der Parquet-Datei. Modellspezifische Transformationen
(Skalierung, log-Lags, One-Hot) laufen innerhalb der sklearn-Pipeline je Fold.

## 4. CRISP-DM-Status

| Phase | Status |
|---|---|
| Business Understanding | ✅ Exposé, Kap. 1/4 |
| Data Understanding | ✅ Eignungsprüfung mit Urteil je Verfahren, `results/eignungspruefung/` |
| Data Preparation – Baselines | ✅ naiv, saisonal, NegBin in `prep/s3_baselines.py` (Auflage Schröter 27.07.) |
| Data Preparation | ✅ **abgeschlossen** – ein Befehl, zwei Datensätze, 14/14 Prüfungen |
| Modeling | 🟡 Ridge, RF, XGBoost und alle Baselines laufen; offen: Randomized Search, SHAP, binärer Robustheitslauf |
| Evaluation | 🟡 Fold-Ergebnisse liegen vor, Hold-out unberührt |
| Deployment | ⬜ nicht Teil der Arbeit (Limitation, vgl. Schröer et al. 2021) |

**Eignungsurteil (nur Trainingsfenster 2015-01 – 2021-12, 2.940 Beobachtungen):**
alle harten Kriterien erfüllt. OLS R² 0,75 → lineare Baseline vorhanden,
Schröter-Kriterium erfüllt · max. VIF 7,1 → klassischer Ridge-Fall ·
Dispersionsindex 62,8 → NegBin statt Poisson · Nullanteil 0,02 % → keine
Zero-Inflation · Extrapolationsbedarf 2,1 % → für RF/XGBoost unkritisch ·
Klassenbalance 3,6:1 · Basisratendrift 5,3 pp.
Drei Auflagen: Ridge auf log(1+y), Lags log(1+x), Schwelle je Fold kalibrieren.

**Verfahrensvergleich** (3-Fold-TS-CV, Mittel ± Std, Standardparameter,
Hold-out unberührt):

| Modell | RMSE | R² |
|---|---|---|
| Ridge (S+L) | 15,01 ± 0,86 | 0,963 |
| Random Forest (S+L) | 16,96 ± 0,73 | 0,953 |
| Naiv (Vormonat) | 17,78 ± 1,29 | 0,950 |
| XGBoost (S+L) | 18,47 ± 1,12 | 0,944 |
| XGBoost (S) | 24,58 ± 3,83 | 0,898 |
| Random Forest (S) | 25,05 ± 4,03 | 0,895 |
| Saisonaler Durchschnitt | 25,77 ± 4,55 | 0,890 |
| Ridge (S) | 35,20 ± 11,12 | 0,772 |
| Negative Binomial | 37,26 ± 13,51 | 0,740 |

Klassifikation: Macro-AUROC ≈ 0,59 im Vortest – nahe am Zufall. Ursache ist das
Pseudo-Signal-Problem (350.481 Zeilen, nur 4.619 verschiedene
Stadtteil-Monats-Profile). Spricht für Priorisierung der Regression (R4).

## 5. Validierung und Tuning

- **Time-Series-CV**, expanding window, Testfenster 12 Monate, kein Blick in die
  Zukunft (Bergmeir & Benítez 2012). Zentral in `prep/s2_datensaetze.py`,
  als Spalten `fold`/`ist_holdout` in beiden Datensätzen.
- **End-Hold-out** der letzten 12 Monate, beim Tuning unberührt.
- **Inneres Validierungsfenster** (letzte 12 Trainingsmonate) für die Suche.
- Kein Gap zwischen Train und Test nötig: alle Lag-Features sind strikt
  rückwärtsgerichtet (`shift` vor `rolling`).
- **Baselines:** naiv (Vormonat), saisonaler Durchschnitt, Negative Binomial für
  die Regression, Mehrheitsklasse für die Klassifikation – alle in
  `prep/s3_baselines.py`, also Teil der Data Preparation (Auflage Schröter).
- **Randomized Search** (Bergstra & Bengio 2012), gleiches Budget je Verfahren
  (50 Iterationen). Suchräume in `prep/config.py`.

## 6. Mapping Analyse → Gliederung (Kap. 4–7)

| Kapitel | Inhalt | Artefakt |
|---|---|---|
| 4.1–4.3 Anwendungsfall & Daten | Datenquellen, Variablen, Zielgrößen | `docs/01_PIPELINE.md`, `prep/s1_daten.py` |
| 5.1 Data Understanding | EDA, Overdispersion, Klassenbalance, **Eignungsprüfung** | `modelle/m01_eignung.py`, `results/eignungspruefung/` |
| 5.2 Data Preparation | Aufbereitung Schritt für Schritt | `docs/01_PIPELINE.md`, `prep/` |
| 5.3 Modellierung & Tuning | 3 Verfahren + NegBin, Randomized Search | `modelle/`, Suchräume in `prep/config.py` |
| 5.4 Evaluation | Time-Series-CV, Baselines, Gütemaße, Laufzeiten | `results/` |
| 5.5 SHAP | Interpretierbarkeit | geplant |
| 6 Diskussion | Algorithmenvergleich, Limitationen | `docs/02_ENTSCHEIDUNGEN.md` |
| 7 Fazit | Beantwortung der Forschungsfrage | – |

---

## 7. KI-Verzeichnis

Format nach Auflage Schröter (27.07.2026): Kennung, Engine, Jahr, Modell,
Prompt, URL. Die Kennung wird im Text wie eine Quelle zitiert.

| Kennung | Engine | Jahr | Modell | Prompt (Kurzfassung) | URL |
|---|---|---|---|---|---|
| anthropic2026a | Anthropic Claude | 2026 | Claude Fable | „Lies Exposé und Betreuer-Vorgaben sowie die bestehende Data-Prep-Pipeline. Erstelle (1) einen persistenten Rahmenplan CLAUDE.md (Forschungsfrage, Zielgrößen, CRISP-DM-Status, Pipeline-Doku, Validierungs- und Tuning-Strategie, Decision Log, Gliederungs-Mapping), (2) eine empirische Eignungsprüfung von Ridge/Random Forest/XGBoost auf dem Pipeline-Output (EDA, Overdispersion, Klassenbalance, Linearitätsprüfung, VIF, Datenqualität/Leakage), (3) eine minimale Demo-Modellierungspipeline (Time-Series-CV, naive + saisonale Baseline, ein Beispielmodell), ohne die bestehende Prep-Pipeline zu verändern." | https://claude.ai |
| anthropic2026b | Anthropic Claude | 2026 | Claude Fable | „Analysiere mögliche Hürden/Probleme bei der weiteren Bearbeitung und Lösungen, damit alle Algorithmen wertvolle und richtige Erkenntnisse liefern. Schreibe eine Markdown-Datei, die die richtigen Algorithmen einzeln in Teilschritten nach allen Vorgaben zum Programmieren anleitet, inkl. korrekter SHAP-Verwendung." → `docs/UMSETZUNGSLEITFADEN_MODELLIERUNG.md` | https://claude.ai |
| anthropic2026c | Anthropic Claude | 2026 | Claude Fable | „Analysiere, wie wir die Probleme bereits in der Prep-Pipeline adressieren können, mache einfache Anpassungen, validiere per Demo-Test und schreibe eine Stichpunkt-Zusammenfassung für das Kapitel Data Preparation. Untersuche, ob zusätzliche Attribute die Probleme beheben und welche Baseline nötig ist." → Pipeline-Anpassungen (Dedup, ACS-/Crime-Join), Lag-Feature-Test, `docs/kapitel_5_2_data_preparation_stichpunkte.md` | https://claude.ai |
| anthropic2026d | Anthropic Claude | 2026 | Claude Opus | „Prüfe eine externe Kritikliste mit 11 Preprocessing-Prüfaufträgen gegen den tatsächlichen Repo-Stand und gib aus, was im Preprocessing noch zu erledigen ist und welche Entscheidungen mit welchen Auswirkungen zu treffen sind." → `docs/PREPROCESSING_AUDIT_2026-07-26.md` | https://claude.ai |
| anthropic2026e | Anthropic Claude | 2026 | Claude Opus | „Baue einen relativen Kriminalitätsindex je Stadtteil und Monat aus beiden SFPD-Datensätzen (vor und nach 2018), setze den Analysezeitraum dauerhaft fest und beschreibe in einfachen Worten, was für eine langfristig korrekte Data Preparation noch zu tun ist." → Decision Log #17–#19, `docs/NAECHSTE_SCHRITTE.md` | https://claude.ai |
| anthropic2026f | Anthropic Claude | 2026 | Claude Opus | „Rechne die Eignungsprüfung neu (nur auf Trainingsdaten) und entwirf den Klassifikationsteil." → `analyse/eignungspruefung.py` (Neufassung), `docs/KLASSIFIKATION_DESIGN.md`, Decision Log #20 | https://claude.ai |
| anthropic2026g | Anthropic Claude | 2026 | Claude Opus | „Räume Dokumentation und Hilfsskripte auf, setze alles auf den neuesten Stand und analysiere anhand von Exposé, Vorgaben, Pipeline und Ergebnissen, was bei der Modellierung noch zu Fehlern führen kann." → `docs/RISIKEN_MODELLIERUNG.md`, `docs/archiv/`, Aktualisierung von README, DATA_DICTIONARY und Umsetzungsleitfaden | https://claude.ai |
| anthropic2026h | Anthropic Claude | 2026 | Claude Opus | „Setze die Priorität-1-Punkte des Audits um (Randmonat-Bugfix, Exposure-Kontrolle, ACS-Publikationsversatz, End-Hold-out) und beschreibe die Änderungen." → Decision Log #11–#16, `modellierung/cv.py`, Anpassungen in `02_join.py`, `aggregation.py`, `demo_modellierung.py` | https://claude.ai |
| anthropic2026i | Anthropic Claude | 2026 | Claude Opus | „Erkläre das Verhältnis von `pipeline/03_features.py` und `modellierung/features.py` und gib einen Überblick, welche Dateien den finalen Datensatz erzeugen." → `ORIENTIERUNG.md` | https://claude.ai |
| anthropic2026j | Anthropic Claude | 2026 | Claude Opus | „Entwickle eine Idee, wie sich der Code zu einer klaren, einfachen Preprocessing-Pipeline aufräumen lässt: ein Ordner, ein Befehl, ein bis zwei finale Datensätze unter data/processed. Setze die Zielstruktur anschließend um, ergänze den Lag-Vorlauf und sorge dafür, dass die Eignungsprüfung validieren kann, ob die drei Algorithmen zum Datensatz passen." → `docs/UMBAU_PREPROCESSING.md`, `prep/`, `modelle/`, Decision Log #22 und #23 | https://claude.ai |
| anthropic2026k | Anthropic Claude | 2026 | Claude Fable | „Prüfe, ob noch Anpassungen in der Data Preparation nötig sind (Safe-to-Train, wissenschaftliche Vergleichbarkeit der drei Algorithmen), und schreibe Kapitel 5 der Arbeit in LaTeX mit ausgewählten, erklärten Code-Snippets inkl. Highlighting-Setup." → Audit-Fix #10 (bfill-Leakage), Hauptanalyse ab 2014 (#5), `docs/kapitel_5_empirische_analyse.tex` | https://claude.ai |
| anthropic2026l | Anthropic Claude | 2026 | Claude Opus | „Verdichte `prep/` auf drei Schritte, lege die Baselines dazu, räume Kommentare und Ausnahmebehandlung aus, verschiebe die Eignungsprüfung aus der Aufbereitung nach `modelle/` und reduziere die Dokumentation auf drei Dateien." → `prep/s1_daten.py`, `prep/s2_datensaetze.py`, `modelle/m01_eignung.py`, `docs/01–03`, Decision Log #25 und #26 | https://claude.ai |
