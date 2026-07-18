# CLAUDE.md – Rahmenplan Bachelorarbeit (verbindlicher Projekt-Kontext)

> **Anweisung an Claude:** Diese Datei bei JEDER neuen Session zuerst vollständig
> einlesen. Sie ist der verbindliche Rahmen bis zur Abgabe. Bei Änderungen im
> Projektverlauf eigenständig aktualisieren (insbesondere Status-Tracking und
> Decision Log). Die bestehende Prep-Pipeline (`pipeline/`) wird NICHT verändert,
> außer Lukas stimmt einer Änderung ausdrücklich zu.

**Arbeit:** „Vorhersage von Feuerwehreinsätzen mittels Machine Learning, ein
Verfahrensvergleich am Beispiel der Stadtteile San Franciscos" (FOM, B.Sc.
Wirtschaftsinformatik) · Betreuer: Prof. Dr. Schröter · 2. Prüfer: Oliver Bach ·
**Abgabe: 07.10.2026** · Kolloquium bis 15.12.2026 (kein Unterfrist-Kolloquium)

Referenzen: Exposé (`expose.pdf`), Sprechstunden-Mitschrift (`vorgaben_schroeter.pdf`)
– beide im Claude-Projekt „Bachelorarbeit" hinterlegt.
**Umsetzungs-Fahrplan:** `docs/UMSETZUNGSLEITFADEN_MODELLIERUNG.md`
(Hürden/Risiken A1–A10, Programmier-Teilschritte 1–9, SHAP-Vorgehen, Checkliste).

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
4. Welche Implikationen ergeben sich aus den Ergebnissen für die Modellauswahl bei
   vergleichbaren tabellarischen Prognoseaufgaben?

## 2. Zielgrößen

| Zielgröße | Typ | Ebene | Gütemaße |
|---|---|---|---|
| Einsatzhäufigkeit (`anzahl_einsaetze`) | Regression (Zähldaten) | Stadtteil × Monat | RMSE, MAE, R² |
| Einsatzart (`einsatzart`, NFIRS) | Klassifikation (bei starkem Ungleichgewicht: binär Brand vs. Nicht-Brand) | Einzeleinsatz | F1, AUROC |

Ergänzend: Poisson-/Negative-Binomial-Regression als interpretierbare
Count-Baseline (Exposé); Trainings-/Inferenzzeiten werden dokumentiert;
SHAP-Analyse zur Interpretation.

## 3. CRISP-DM-Phasenplan mit Status

| Phase | Status | Artefakte |
|---|---|---|
| Business Understanding | ✅ abgeschlossen | Exposé, Kap. 1/4 |
| Data Understanding | ✅ Eignungsprüfung durchgeführt (2026-07-18) | `analyse/eignungspruefung.py`, `results/eignungspruefung/` |
| Data Preparation | 🔄 **aktiv** (Prep-Pipeline fertig; Aggregation Stadtteil×Monat + Dedup ergänzt; offen: Encoding Klassifikation, NaN-Strategie akademikerquote) | `pipeline/`, `modellierung/aggregation.py` |
| Modeling | 🟡 Demo lauffähig (Baselines + Ridge); offen: RF, XGBoost, NegBin, Tuning | `modellierung/demo_modellierung.py` |
| Evaluation | 🟡 Demo-Ergebnisse liegen vor (s. u.) | `results/demo_modellierung/` |
| Deployment | ⬜ nicht Teil der Arbeit (Limitation, vgl. Schröer et al. 2021) | – |

**Zentrale empirische Befunde der Eignungsprüfung (2026-07-18):**
- Regression: Dispersionsindex 61 → starke Overdispersion → **NegBin statt Poisson** als Count-Baseline.
- Linearität (Schröter-Prüfpunkt): **lineare Baseline vorhanden** (OLS R²≈0,71 auf Stadtteil×Monat; stärkster Prädiktor `anteil_risikogewerbe_pct` r=0,69) → Ridge zulässig; wegen Trichter-Residuen und negativer Vorhersagen auf Rohskala: **Ridge auf log(1+y)**.
- VIF: max. 8,8 (Einkommen), 7,7 (Miete) → erhöhte, aber nicht extreme Multikollinearität → klassischer Ridge-Anwendungsfall; für RF/XGB unkritisch.
- Klassifikation: Fehlalarme 44,7 %, Brand 13,1 % → binär **Brand vs. Nicht-Brand (13/87)**, class_weight + F1/AUROC.
- Datenqualität: 269 doppelte Einsatznummern aus Quelldaten (0,04 %, Dedup in Modellierungsschicht); McLaren-Park-ACS-Artefakt (Armutsquote 0,90 bei 850 Ew.); Treasure Island & Lakeshore ohne ACS-Werte (entfallen); `akademikerquote_pct` 37 % NaN vor 2012.
- Demo-Modellierung (3-Fold-TS-CV, Test je 12 Monate): Naiv RMSE 24,5 / R² 0,88 · Saisonal RMSE 33,9 / R² 0,81 · Ridge (nur Strukturmerkmale + Saison) RMSE 81,3 / R² −0,10. **Lag-1-Autokorrelation der Zielgröße: 0,96.** → Rein strukturelle Features schlagen die zeitliche Persistenz nicht; s. Decision Log #8.

## 4. Preprocessing-Pipeline (Prüfpunkt Schröter!) – Bestandsdokumentation

Bestehende Pipeline `pipeline/01_fetch.py → 02_join.py → 03_features.py`
(Orchestrierung: `run_pipeline.py`). **Für Kapitel 5.2 der Arbeit.**

**Datenquellen (01_fetch):**
- SFFD Fire Incidents, DataSF `wr8u-xric` (~720.000 Einsätze, 2003–2026); nur
  Zeilen mit `neighborhood_district` und `arrival_dttm`
- Census-Tract↔Neighborhood-Crosswalk, DataSF `sevw-6tgi`
- ACS 5-Year Estimates, 5 Jahrgänge (2009, 2014, 2019, 2021, 2023), 9 Variablen
  (Einkommen, Armut, Bildung, Miete, Leerstand, Bevölkerung)
- SFPD Crime (monatlich voraggregiert), DataSF `e3si-785i`
- Land Use 2020 (Parzellen), DataSF `ygi5-84iq` + Neighborhood-Boundaries `j2bu-swwd`

**Join-Logik (02_join):**
- SFFD: `response_time_min` = Ankunft−Alarm, Filter 0–60 min (~1,7 % entfernt);
  Zeit-Features (jahr, monat, stunde, wochentag, ist_wochenende, ist_nacht);
  Neighborhood-Namen normalisiert (Title Case, 41 Stadtteile)
- ACS: Tract→Neighborhood via Crosswalk; Mediane populationsgewichtet, Zähler/Nenner
  summiert; **zeitbewusster Join**: jeder Einsatz erhält den zeitlich *nächsten*
  ACS-Snapshot (`acs_jahr`)
- Crime: Summe aller Monats-Snapshots 2003–2026 je Neighborhood → **statisch**
- Land Use: Spatial Join Parzellen-Centroid→Neighborhood-Polygon (Match 99,5 %),
  Aggregation je Neighborhood → **statisch** (Snapshot 2020)

**Feature-Berechnung (03_features):** Raten via `safe_ratio` (Zähler/Nenner, [0,1]):
`armutsquote_pct`, `akademikerquote_pct`, `leerstandsquote_pct`,
`anteil_gewaltdelikte_pct`, `anteil_eigentumsdelikte_pct`,
`anteil_altbau_vor_1940_pct` (= Altbau-Anteil aus Exposé), `anteil_altbau_vor_1960_pct`,
`anteil_wohngebaeude_pct`, `anteil_risikogewerbe_pct` (= Risiko-Gewerbe-Index aus
Exposé: RETAIL/ENT+PDR-Fläche / Gesamtfläche). Umbenennung auf Deutsch
(`column_names.py`). Output: `sf_fire_risk_features.parquet` (53 Sp.) und
`_cleaned.parquet` (23 Sp.).

**Aggregationsebene:** Output der Prep-Pipeline ist **Einsatz-Ebene**. Die im
Exposé festgelegte Aggregation auf **Stadtteil × Monat** fehlt dort und ist in
`modellierung/aggregation.py` ergänzt (Prep-Pipeline unangetastet): Zählung je
Stadtteil-Monat, vollständiges Raster (Monate ohne Einsatz = 0), unvollständiger
Randmonat abgeschnitten, Stadtteil-Merkmale konstant übernommen.

**Für die Modellierung noch fehlende/ergänzte Schritte** (nur außerhalb der
Prep-Pipeline umgesetzt):
- [x] Aggregation Stadtteil × Monat (`modellierung/aggregation.py`)
- [x] Skalierung für Ridge (StandardScaler in sklearn-Pipeline, nur auf Train gefittet)
- [x] Zielgrößen-Transformation log(1+y) für Ridge (Ergebnis Linearitätsprüfung)
- [ ] Encoding kategorialer Variablen (nur Klassifikationsteil: `bataillon`,
      Zeit-Features; One-Hot bzw. nativ bei XGBoost – einheitlich für alle Modelle!)
- [ ] Umgang mit Klassenungleichgewicht Einsatzart (binär Brand vs. Nicht-Brand
      gemäß Exposé; class_weight/scale_pos_weight; F1/AUROC statt Accuracy)
- [ ] Fehlende Werte: `akademikerquote_pct` ~37 % NaN (ACS 2009 ohne B15003) →
      Entscheidung: Zeitraum einschränken vs. Feature streichen vs. Imputation
      (→ Decision Log, mit Schröter klären)

**Fairness-Regel:** Alle drei Modelle erhalten exakt denselben aufbereiteten
Datensatz (identische Zeilen, Features, CV-Folds). Modellspezifische
Transformationen (Skalierung) laufen innerhalb der sklearn-Pipeline je Fold.

## 5. Validierungsstrategie

- **Time-Series-Cross-Validation** (expanding window über Monate, Testfenster
  12 Monate, kein Blick in die Zukunft; vgl. Bergmeir & Benítez 2012).
  Implementiert in `modellierung/demo_modellierung.py::zeit_folds`.
- **Vergleichsgrößen:** naives Modell (Vormonatswert je Stadtteil) + saisonaler
  Durchschnitt (Mittelwert desselben Kalendermonats im Training je Stadtteil).
- **Gütemaße:** RMSE, MAE, R² (Regression) · F1, AUROC (Klassifikation) ·
  Trainings-/Inferenzzeit.
- Schröter-Vorgabe beachtet: „erst plotten, falls keine lineare Baseline
  vorliegt, kein lineares Regressionsmodell verwenden" → Ergebnis in
  `results/eignungspruefung/` dokumentiert (Abschnitt 4 der Summary).

## 6. Hyperparameter-Tuning-Strategie

Randomized Search (Bergstra & Bengio 2012) je Modell, Bewertung ausschließlich
über die Time-Series-CV-Folds (identisch für alle Modelle):

| Modell | Suchraum (geplant) |
|---|---|
| Ridge | alpha ∈ log-uniform [1e-3, 1e3] |
| Random Forest | n_estimators 200–1000, max_depth, min_samples_leaf, max_features (Probst et al. 2019) |
| XGBoost | n_estimators, learning_rate, max_depth, subsample, colsample_bytree, reg_lambda |
| NegBin-Baseline | kein Tuning (interpretierbare Referenz) |

Budget je Modell gleich (z. B. 50 Iterationen) → fairer Vergleich.

## 7. Decision Log (Abweichungen vom Exposé – mit Schröter besprechen)

| # | Datum | Entscheidung / offener Punkt | Begründung | Status |
|---|---|---|---|---|
| 1 | 2026-07-18 | Aggregation Stadtteil×Monat als eigener Schritt in `modellierung/`, nicht in der Prep-Pipeline | Prep-Pipeline bleibt unverändert (Absprache); Exposé verlangt diese Ebene | umgesetzt |
| 2 | 2026-07-18 | Ridge auf log(1+y) statt roher Zählung | Linearitätsprüfung: y stark rechtsschief/heteroskedastisch; log-Baseline annähernd linear → Schröter-Vorgabe erfüllt, Ridge bleibt als interpretierbare Baseline im Vergleich | mit Schröter bestätigen |
| 3 | 2026-07-18 | Crime- und Land-Use-Features sind statisch (über Gesamtzeitraum bzw. Snapshot 2020) → milde Form von Zukunftsinformation | In Prep-Pipeline so angelegt; als quasi-stabile Strukturmerkmale interpretierbar; Alternative (zeitbewusste Crime-Aggregation) wäre Pipeline-Änderung | als Limitation dokumentieren, mit Schröter besprechen |
| 4 | 2026-07-18 | ACS-Join nutzt *nächsten* statt *letzten verfügbaren* Snapshot | Für frühe Jahre (2003–2011 → ACS 2009) leichte Zukunftsinformation; strikt prognostisch wäre "letzter verfügbarer" korrekt | mit Schröter besprechen |
| 5 | 2026-07-18 | `akademikerquote_pct` (37 % NaN vor 2012): Option A Zeitraum ab 2012, B Feature streichen, C Imputation | ACS 2009 enthält B15003 nicht | offen |
| 6 | 2026-07-18 | Klassifikation binär Brand vs. Nicht-Brand (statt Multiklasse) | Klassenverteilung stark unbalanciert (Brand 13,1 %); Exposé sieht Vereinfachung explizit vor | vorgesehen |
| 7 | 2026-07-18 | Dedup nach `einsatz_nummer` in `modellierung/aggregation.py` (269 Zeilen, 0,04 %) | Mehrfach gemeldete Einsatznummern in DataSF-Quelldaten; Prep-Pipeline bleibt unverändert | umgesetzt |
| 8 | 2026-07-18 | **Wichtigster offener Punkt:** naive Baseline (Vormonat) schlägt Strukturmodell deutlich (R² 0,88 vs. −0,10), weil Lag-1-Autokorrelation 0,96. Optionen: (A) Lag-/Rolling-Features der Einsatzzahl für ALLE drei Modelle ergänzen (fairer, praxisüblich; Forschungsfrage bleibt beantwortbar über SHAP-Beitrag der Strukturmerkmale), (B) bewusst nur Strukturmerkmale („können Stadtteil-Merkmale die Einsatzlast erklären?") und Baseline-Überlegenheit als ehrliches Ergebnis diskutieren | Beides wissenschaftlich vertretbar; Framing der Story betroffen → **mit Schröter besprechen** | offen |

## 8. Mapping Analyse → Gliederung der Arbeit (Kap. 4–7)

| Kapitel | Inhalt | Artefakt im Repo |
|---|---|---|
| 4.1–4.3 Anwendungsfall & Daten | Datenquellen, Variablen, Zielgrößen | `DATA_DICTIONARY.md`, `pipeline/01_fetch.py` |
| 5.1 Business & Data Understanding | EDA, Verteilungen, Klassenbalance, Overdispersion, **Eignungsprüfung** | `analyse/eignungspruefung.py`, `results/eignungspruefung/` |
| 5.2 Data Preparation | Prep-Pipeline (Abschnitt 4 dieser Datei), Aggregation, fehlende Werte, Encoding, Skalierung | `pipeline/`, `modellierung/aggregation.py` |
| 5.3 Modellierung & Tuning | 3 Verfahren + NegBin-Baseline, Randomized Search | `modellierung/` (Ausbau der Demo) |
| 5.4 Evaluation & Vergleich | Time-Series-CV, Baselines, Gütemaße, Laufzeiten | `results/demo_modellierung/` (später voll) |
| 5.5 SHAP | Interpretierbarkeit | geplant |
| 6 Diskussion | Algorithmenvergleich, Erklärungsbeitrag, Limitationen (Decision Log #3/#4!) | diese Datei, Abschnitt 7 |
| 7 Fazit | Beantwortung Forschungsfrage | – |

**Formale Vorgaben (Schröter):** mind. 3/4 Fließtext, Code nur als Beleg ·
wissenschaftliche Arbeit, keine Projektarbeit · min. 30–100 Quellen (Zotero) ·
Lehrbücher für Grundlagen, Papers für Forschungsstand · KI-Verzeichnis (Tabelle
Datum/KI-System/Prompt) · Zip max. 250 MB inkl. flüchtiger Quellen als PDF/A ·
Methodenkapitel so präzise, dass die Arbeit reproduzierbar ist · Story/roter Faden.

## 9. KI-Verzeichnis – Einträge aus dieser Session

| Datum | KI-System | Prompt (Kurzfassung) |
|---|---|---|
| 2026-07-18 | Anthropic Claude (Fable) | „Lies Exposé und Betreuer-Vorgaben sowie die bestehende Data-Prep-Pipeline. Erstelle (1) einen persistenten Rahmenplan CLAUDE.md (Forschungsfrage, Zielgrößen, CRISP-DM-Status, Pipeline-Doku, Validierungs- und Tuning-Strategie, Decision Log, Gliederungs-Mapping), (2) eine empirische Eignungsprüfung von Ridge/Random Forest/XGBoost auf dem Pipeline-Output (EDA, Overdispersion, Klassenbalance, Linearitätsprüfung, VIF, Datenqualität/Leakage), (3) eine minimale Demo-Modellierungspipeline (Time-Series-CV, naive + saisonale Baseline, ein Beispielmodell), ohne die bestehende Prep-Pipeline zu verändern." |
| 2026-07-18 | Anthropic Claude (Fable) | „Analysiere mögliche Hürden/Probleme bei der weiteren Bearbeitung und Lösungen, damit alle Algorithmen wertvolle und richtige Erkenntnisse liefern. Schreibe eine Markdown-Datei, die die richtigen Algorithmen einzeln in Teilschritten nach allen Vorgaben zum Programmieren anleitet, inkl. korrekter SHAP-Verwendung." → `docs/UMSETZUNGSLEITFADEN_MODELLIERUNG.md` |
