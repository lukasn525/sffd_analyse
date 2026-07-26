# CLAUDE.md – Rahmenplan Bachelorarbeit (verbindlicher Projekt-Kontext)

> **Anweisung an Claude:** Diese Datei bei JEDER neuen Session zuerst vollständig
> einlesen. Sie ist der verbindliche Rahmen bis zur Abgabe. Bei Änderungen im
> Projektverlauf eigenständig aktualisieren (insbesondere Status-Tracking und
> Decision Log). Die bestehende Prep-Pipeline (`pipeline/`) wird NICHT verändert,
> außer Lukas stimmt einer Änderung ausdrücklich zu.
> **2026-07-18: Lukas hat gezielten Pipeline-Anpassungen zugestimmt** (Dedup,
> strikt prognostischer ACS-Join, zeitbewusster Crime-Join mit Fallback,
> monat/wochentag im Cleaned-Datensatz) – umgesetzt und validiert, s. Abschnitt 4.

**Arbeit:** „Vorhersage von Feuerwehreinsätzen mittels Machine Learning, ein
Verfahrensvergleich am Beispiel der Stadtteile San Franciscos" (FOM, B.Sc.
Wirtschaftsinformatik) · Betreuer: Prof. Dr. Schröter · 2. Prüfer: Oliver Bach ·
**Abgabe: 07.10.2026** · Kolloquium bis 15.12.2026 (kein Unterfrist-Kolloquium)

Referenzen: Exposé (`expose.pdf`), Sprechstunden-Mitschrift (`vorgaben_schroeter.pdf`)
– beide im Claude-Projekt „Bachelorarbeit" hinterlegt.

**Dokumentenlandkarte (Stand 2026-07-26):**

| Datei | Inhalt |
|---|---|
| `CLAUDE.md` (diese Datei) | verbindlicher Rahmen, Decision Log, Status |
| `docs/NAECHSTE_SCHRITTE.md` | Roadmap in einfacher Sprache |
| `docs/RISIKEN_MODELLIERUNG.md` | **Risikoanalyse R1–R10 vor der Modellierung** |
| `docs/KLASSIFIKATION_DESIGN.md` | Aufbau des Klassifikationsteils |
| `docs/UMSETZUNGSLEITFADEN_MODELLIERUNG.md` | Programmierplan (A2–A4 revidiert) |
| `docs/PREPROCESSING_AUDIT_2026-07-26.md` | Audit-Protokoll (abgearbeitet) |
| `DATA_DICTIONARY.md` | Spaltenbeschreibung des Analysedatensatzes |
| `results/eignungspruefung/` | Linearität, VIF, Overdispersion, Klassenbalance |
| `ABGABE.md` | welche Dateien ins Abgabe-Zip gehören |
| `tests/test_aufbereitung.py` | 11 Prüfungen der Datenaufbereitung |
| `docs/archiv/` | **veraltet – nichts davon in die Arbeit übernehmen** |

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
| Einsatzart (`einsatzart_gruppe`, NFIRS) | Klassifikation, **4 zusammengefasste Serien** (Decision Log #21); binär Brand vs. Nicht-Brand als Robustheitslauf | Einzeleinsatz | Macro-F1, Macro-AUROC |

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
- Datenqualität: 269 doppelte Einsatznummern aus Quelldaten (0,04 %, Dedup in der Prep-Pipeline); McLaren-Park-ACS-Artefakt (Armutsquote 0,90 bei 850 Ew.); Treasure Island, Lakeshore und Mission Bay ohne durchgängige ACS-Abdeckung (entfallen, Decision Log #15); `akademikerquote_pct` erst ab ACS-2014-Snapshot.

**Stand nach dem Preprocessing-Audit vom 2026-07-26** (Decision Log #11–#16, Details:
`docs/PREPROCESSING_AUDIT_2026-07-26.md`):

- **Analysedatensatz Regression:** Stadtteil × Monat, **2015-01 – 2025-12**, **35 Stadtteile**,
  **4.620 Zeilen** (rechteckiges Panel, keine NaN); nach Lag-Bildung 120 Monate
  (2016-01 – 2025-12). Dispersionsindex 62,8 → NegBin bestätigt;
  Nullanteil 0,02 % → keine Zero-Inflation.
  Ausgeschlossen: Treasure Island, Lakeshore, Mission Bay (keine durchgängige
  ACS-Abdeckung, #15) sowie Golden Gate Park, Lincoln Park, McLaren Park
  (Park-/Institutionsgebiete ohne Wohnbevölkerung, #19).
- **Kriminalitätsindex (#17) wirkt:** 128 verschiedene Werte je Stadtteil über
  132 Monate (vorher: 1 Wert, 0 % Zeitvarianz). Median 0,77, Spanne 0,05–13,55;
  logarithmiert Schiefe 0,66. Korrelation mit der Einsatzzahl +0,59.
- **Zeitschnitte (zentral in `modellierung/cv.py`):** Entwicklungsdaten 2016-01 – 2024-12,
  **End-Hold-out 2025-01 – 2025-12 (beim Tuning unberührt)**; 3 Folds à 12 Testmonate,
  inneres Validierungsfenster = letzte 12 Trainingsmonate.
- **Demo-Modellierung nach allen Fixes** (3-Fold-TS-CV, Mittel ± Std, Hold-out nicht ausgewertet):
  **Ridge (S+L) RMSE 15,0 ± 0,9 / R² 0,96 · RF (S+L) RMSE 16,8 ± 0,9 / R² 0,95 ·
  Naiv RMSE 17,8 ± 1,3 / R² 0,95 · RF (S) RMSE 25,3 / R² 0,89 ·
  Saisonal RMSE 25,6 / R² 0,89 · Ridge (S) RMSE 34,0 / R² 0,79.**
  Set S (reine Strukturmerkmale) ist damit erstmals belastbar: R² 0,17 (Ausgangsstand)
  → 0,52 (Exposure #13) → **0,79** (Kriminalitätsindex #17 + Analyseeinheiten #19).
- **Korrektur gegenüber dem Stand 2026-07-18:** Der Einbruch von Fold 3 (Naiv R² 0,74)
  war **kein Nichtstationaritäts-Befund**, sondern der Phantom-Monat 2026-01
  (Decision Log #12). Nach dem Fix sind die Folds stabil (R² 0,95/0,95/0,96).
  **Hürde A2 im Umsetzungsleitfaden ist entsprechend zu korrigieren.**
- **Baseline-Problem (Decision Log #8) bleibt gelöst:** mit `lag_1`, `lag_12`,
  `rolling_mean_3` schlagen beide Modellklassen die naive Baseline; reine
  Strukturmerkmale (Set S) erklären das Querschnittsniveau, nicht die Dynamik.
  Für Ridge müssen Lags log(1+x)-transformiert werden. Die Exposure-Kontrolle
  (#13) hebt Set S deutlich an (Ridge (S) R² 0,17 → 0,52).
- **Weiterhin offen:** VIF/Linearitätsprüfung wurden auf dem Gesamtdatensatz statt
  nur auf Trainingsdaten gerechnet; `results/eignungspruefung/` ist nach den Fixes
  **veraltet** und neu zu rechnen. Klassifikationsteil existiert noch nicht.
  Details und Reihenfolge: `docs/NAECHSTE_SCHRITTE.md`.

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

**Join-Logik (02_join, Stand nach Anpassungen 2026-07-18):**
- SFFD: **Dedup nach `incident_number`** (269 mehrfach gemeldete Einsatznummern
  aus DataSF entfernt, 0,04 %) → 719.989 Einsätze; `response_time_min` =
  Ankunft−Alarm, Filter 0–60 min (~1,7 % entfernt); Zeit-Features (jahr, monat,
  stunde, wochentag, ist_wochenende, ist_nacht); Neighborhood-Namen normalisiert
  (Title Case, 41 Stadtteile)
- ACS: Tract→Neighborhood via Crosswalk; Mediane populationsgewichtet, Zähler/Nenner
  summiert; **strikt prognostischer Join**: jeder Einsatz erhält den *letzten
  verfügbaren* ACS-Snapshot (`acs_jahr` ≤ Einsatzjahr); Ausnahme 2003–2008 →
  Rückgriff auf ACS 2009 (kein älterer Jahrgang; Hauptanalyse ab 2012)
- Crime: **relativer Kriminalitätsindex je Stadtteil × Monat** (Decision Log #17).
  Zwei Quellen, weil `e3si-785i` erst 2018-01 beginnt: `tmnf-yvry` (2014–2017,
  Zuordnung per Spatial Join der Koordinaten gegen dieselbe Neighborhood-Geometrie
  wie bei Land Use) + `e3si-785i` (ab 2018-01). Alle Straftaten werden gezählt,
  daher **keine Kategorien-Harmonisierung nötig**.
  Definition (Location Quotient): `Index(i,t) = [Delikte(i, 12-Monats-Fenster
  endend in t−1) / Einwohner(i)] ÷ [dasselbe stadtweit]`. Lesart: 1,0 =
  Kriminalitätsbelastung wie im Stadtdurchschnitt desselben Monats.
  Der SFPD-Systemwechsel 05/2018 (CABLE → Crime Data Warehouse) verschiebt das
  stadtweite Niveau; ein solcher multiplikativer Sprung kürzt sich im Quotienten
  heraus. **Kein statischer Fallback mehr** – fehlen die Rohdaten, bricht die
  Pipeline mit Anleitung ab.
- Land Use: Spatial Join Parzellen-Centroid→Neighborhood-Polygon (Match 99,5 %),
  Aggregation je Neighborhood → **statisch** (Snapshot 2020; einziger Jahrgang);
  aggregierte Tabelle wird gecacht (CSV löschen zum Neuberechnen)

**Feature-Berechnung (03_features):** Raten via `safe_ratio` (Zähler/Nenner, [0,1]):
`armutsquote_pct`, `akademikerquote_pct`, `leerstandsquote_pct`,
`anteil_gewaltdelikte_pct`, `anteil_eigentumsdelikte_pct`,
`anteil_altbau_vor_1940_pct` (= Altbau-Anteil aus Exposé), `anteil_altbau_vor_1960_pct`,
`anteil_wohngebaeude_pct`, `anteil_risikogewerbe_pct` (= Risiko-Gewerbe-Index aus
Exposé: RETAIL/ENT+PDR-Fläche / Gesamtfläche). Umbenennung auf Deutsch
(`column_names.py`). Output: `sf_fire_risk_features.parquet` (53 Sp.) und
`_cleaned.parquet` (25 Sp., seit 2026-07-18 inkl. `monat`/`wochentag`).

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
- [x] Randmonat-Konstante `ENDE` + Vollständigkeitswarnung (#12)
- [x] Exposure `log_bevoelkerung`, Rohwert für NegBin-Offset erhalten (#13)
- [x] Balanciertes Panel, 38 Stadtteile (#15)
- [x] Zentrale Zeitschnitte + End-Hold-out (`modellierung/cv.py`, #14)
- [x] Klassifikationsdatensatz (`modellierung/klassifikation_daten.py`):
      beide Zielgrößen, Merkmalsblöcke, Ausschluss der Ergebnisvariablen,
      Selbsttests (Abgrenzung identisch zur Regression, keine Leakage-Spalten)
- [x] Crime-Merkmale zeitbewusst (#17) – ersetzt #16
- [ ] Encoding kategorialer Variablen im ColumnTransformer (`wochentag`,
      optional `bataillon`; One-Hot **einheitlich für alle Modelle**)
- [ ] Umgang mit Klassenungleichgewicht (3,6:1 mehrklassig):
      `class_weight="balanced"` bzw. `sample_weight`; Macro-F1/Macro-AUROC
      statt Accuracy
- [ ] Pseudo-Signal-Problem der Klassifikation im Methodenkapitel ausformulieren
      (350.481 Einsätze → nur 4.619 verschiedene Stadtteil-Monats-Profile)
- [ ] VIF/Linearität nur auf Trainingsdaten neu rechnen; `results/eignungspruefung/`
      nach den Fixes vom 2026-07-26 neu erzeugen
- [ ] Sensitivitätsanalyse Zielgröße „Einsätze je 1.000 Einwohner" (#13)
- [ ] Robustheitsvariante mit Stadtteil-ID (Maß für unbeobachtete Heterogenität)

**Fairness-Regel:** Alle drei Modelle erhalten exakt denselben aufbereiteten
Datensatz (identische Zeilen, Features, CV-Folds). Modellspezifische
Transformationen (Skalierung) laufen innerhalb der sklearn-Pipeline je Fold.

## 5. Validierungsstrategie

- **Time-Series-Cross-Validation** (expanding window über Monate, Testfenster
  12 Monate, kein Blick in die Zukunft; vgl. Bergmeir & Benítez 2012).
  **Zentral implementiert in `modellierung/cv.py`** – alle Verfahren beziehen
  Splits und Gütemaße ausschließlich von dort (konstruktive Absicherung der
  Fairness-Regel). Zusätzlich: **End-Hold-out der letzten 12 Monate**
  (`split_holdout`, beim Tuning unberührt) und **inneres Validierungsfenster**
  (`inneres_fenster`) für die Hyperparameter-Suche.
  *Kein Gap zwischen Train- und Testfenster nötig:* alle Lag-/Rolling-Features
  sind strikt rückwärtsgerichtet (`shift` vor `rolling`), ein Testmonat greift
  nie auf Werte nach seinem eigenen Zeitpunkt zu.
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
| 3 | 2026-07-18 | Zeitbewusste Crime-Aggregation (Kumulation bis Vorjahr) in 02_join implementiert; aktiv nach Crime-Neu-Download (Rohdatei ohne Datumsspalte); Land Use bleibt statisch (nur ein Snapshot) | Leakage-Behebung mit Zustimmung von Lukas; Fallback statisch dokumentiert | **Code umgesetzt; Lukas: `01_fetch.py` mit `DOWNLOAD_CRIME=True` neu laufen lassen** |
| 4 | 2026-07-18 | ACS-Join auf *letzten verfügbaren* Snapshot umgestellt (statt *nächsten*); 2003–2008 Rückgriff auf ACS 2009 | Strikte Prognose-Logik, kein Zukunfts-Leakage mehr; Linearitätsbefund bleibt stabil (R² 0,71) | **umgesetzt & validiert (2026-07-18)** |
| 5 | 2026-07-18 | **Hauptanalyse ab 2014** (nicht 2012): mit strikt prognostischem ACS-Join hängen 2012/13 am ACS 2009 (ohne B15003) → `akademikerquote_pct` erst ab 2014 verfügbar; Sensitivitätsanalyse voller Zeitraum ohne dieses Feature | Ehrliche NaN-Behandlung statt versteckter Imputation; Parameter `ab_jahr`/`ohne_ausreisser` in `aggregation.py` | entschieden; mit Schröter bestätigen |
| 6 | 2026-07-18 | Klassifikation binär Brand vs. Nicht-Brand (statt Multiklasse) | Klassenverteilung stark unbalanciert (Brand 13,1 %); Exposé sieht Vereinfachung explizit vor | vorgesehen |
| 7 | 2026-07-18 | Dedup nach `einsatz_nummer` **in die Prep-Pipeline verlagert** (02_join, mit Zustimmung); Sicherheits-Dedup in `modellierung/aggregation.py` bleibt (idempotent) | Bereinigung gehört fachlich in die Data Preparation; validiert: 0 Duplikate im Output | umgesetzt & validiert |
| 8 | 2026-07-18 | Baseline-Problem **empirisch gelöst durch Option A**: Lag-Features (`lag_1`, `lag_12`, `rolling_mean_3`) für alle Modelle; Demo: Ridge (S+L) R² 0,91 und RF (S+L) 0,90 schlagen Naiv 0,88; Set S bleibt für Unterfrage 1 im Vergleich | Fair (identische Zeilen/Folds), praxisüblich; Erklärungsbeitrag der Strukturmerkmale wird über SHAP quantifiziert | umgesetzt in Demo; **Framing mit Schröter bestätigen** |
| 9 | 2026-07-18 | Lag-Features werden für Ridge log(1+x)-transformiert (modellinterne Aufbereitung analog Skalierung) | Rohe Lags in log-Zielgrößen-Modell fehlspezifiziert (empirisch R² −5,9); log-AR-Spezifikation korrekt | umgesetzt |
| 10 | 2026-07-18 | **Audit-Fix:** `bfill` aus der Stadtteil-Monat-Aggregation entfernt (nur noch `ffill`) | `bfill` imputierte fehlende Werte (v. a. `akademikerquote_pct` vor 2014) stillschweigend mit **Zukunftswerten** (Leakage); nach Fix ehrliche NaN, Behandlung über Zeitraumfilter (#5); Demo-Ergebnisse bleiben robust (Ridge S+L 0,91 · RF S+L 0,90 · Naiv 0,88; Linearität R² 0,74) | umgesetzt & validiert |
| 11 | 2026-07-26 | **ACS-Publikationsversatz +1 Jahr** (`ACS_PUBLIKATIONS_LAG` in `02_join.py`): Snapshot-Bedingung jetzt `acs_jahr ≤ Einsatzjahr − 1` | Die ACS-5-Jahres-Schätzung für Jahr y erscheint erst ca. Dez. y+1. Ohne Versatz nutzte ein Einsatz aus 2023 den ACS-Jahrgang 2023 – zum Prognosezeitpunkt nicht publiziert, Modell nicht implementierbar. Kostet den Jahrgang 2014; **Hauptanalyse startet nun 2015** | **umgesetzt & validiert (Pipeline neu gerechnet)**; mit Schröter bestätigen |
| 12 | 2026-07-26 | **Audit-Fix: Randmonat-Konstante `ENDE = 202512`** in `aggregation.py` (statt „größter vorhandener `jahr_monat` minus 1") | Alte Logik schnitt nur 2026-02 (1 Einsatz) ab; **2026-01 blieb mit 258 statt ~3.300 Einsätzen als scheinbar vollständiger Monat im Panel** und lag im Testfenster des letzten Folds. Wirkung: naive Baseline dort R² 0,740 statt 0,955. Zusätzlich warnt `pruefe_randmonate()` bei künftigen Downloads | **umgesetzt & validiert** |
| 13 | 2026-07-26 | **Exposure: `log_bevoelkerung` statt roher `gesamtbevoelkerung`** als Prädiktor; Rohwert bleibt für NegBin-Offset und Raten-Sensitivität im Datensatz | Ohne Exposure-Kontrolle sagt das Modell im Kern die Stadtteilgröße vorher: `armutsquote_pct` r=+0,20 auf absolute Counts, aber **−0,13** auf Einsätze je 1.000 Ew.; `anteil_risikogewerbe_pct` +0,70 vs. −0,12. Zielgröße bleibt Zähldaten → NegBin/Overdispersion-Argumentation intakt. Empirischer Effekt: Ridge (S) R² 0,17 → **0,52** | umgesetzt; **Raten-Sensitivität steht noch aus**; mit Schröter bestätigen |
| 14 | 2026-07-26 | **End-Hold-out der letzten 12 Monate** (`modellierung/cv.py`, `split_holdout`): 2025-01–2025-12 wird bei Modellwahl und Tuning nie berührt | Fold 3 war zugleich CV-Fold und letzter Zeitraum → beim Tuning wird zwangsläufig darauf geschaut. Zeitschnitte, inneres Validierungsfenster und Gütemaße jetzt zentral in `cv.py`, damit alle Verfahren konstruktiv identische Splits sehen (Fairness-Regel) | umgesetzt & validiert |
| 15 | 2026-07-26 | **Balanciertes Panel: 38 statt 39 Stadtteile** (`balanciertes_panel()`); Mission Bay zusätzlich zu Treasure Island und Lakeshore ausgeschlossen | Mission Bay ist erst ab ACS 2021 als eigene Analyseeinheit enthalten. Zeilenweises `dropna` hätte ein **unbalanciertes** Panel erzeugt (Fold 1 mit 37, spätere Folds mit 38 Stadtteilen) → Testfenster-Summen springen allein durch den Zutritt eines Stadtteils. Rechteckiges Panel ist Voraussetzung für den fairen Fold-Vergleich | umgesetzt & validiert |
| 16 | 2026-07-26 | ~~Crime-Merkmale vorerst statisch belassen~~ | – | **überholt durch #17** |
| 17 | 2026-07-26 | **Relativer Kriminalitätsindex je Stadtteil × Monat** ersetzt `anteil_gewaltdelikte_pct` und `anteil_eigentumsdelikte_pct`. Maß: **alle Straftaten je Einwohner, relativ zum Stadtdurchschnitt desselben Monats** (Location Quotient), rollierendes 12-Monats-Fenster endend im Vormonat. Quellen: `tmnf-yvry` (2014–2017, Spatial Join) + `e3si-785i` (ab 2018) | Die alten Merkmale waren (a) **statisch** (0 % Zeitvarianz), (b) über den gesamten Zeitraum inkl. Testfenster kumuliert (**Leakage**) und (c) Anteile statt Intensitäten – zwei Stadtteile mit sehr unterschiedlicher Deliktdichte konnten identische Werte haben. Der relative Index löst alle drei Punkte und ist zugleich robust gegen den SFPD-Systemwechsel 05/2018, weil sich ein stadtweiter Niveausprung im Quotienten kürzt. Konsistent mit der Exposure-Entscheidung #13 | **Code umgesetzt & Logik getestet; Lukas muss `01_fetch.py` mit `DOWNLOAD_CRIME=True` und `DOWNLOAD_CRIME_HISTORISCH=True` laufen lassen** |
| 21 | 2026-07-26 | **Klassifikation mehrklassig statt binär** (revidiert #6): Zielgröße `einsatzart_gruppe` mit **4 zusammengefassten NFIRS-Serien** – Fehlalarm/Good Intent 48,2 % · Technische Hilfe/Gefahr 24,0 % · Rettung/EMS 14,2 % · Brand 13,6 %. Metriken **Macro-F1 und Macro-AUROC (One-vs-Rest)**, Zuordnung über `argmax` statt Schwellenwert. Die binäre Zielgröße `ist_brand` bleibt als Robustheitslauf im selben Datensatz erhalten | (a) **Bessere Balance:** 3,6:1 statt 6,4:1 – die binäre Vereinfachung erzeugte das Ungleichgewicht, das sie vermeiden sollte. (b) **Binär verwirft das stärkste Struktursignal:** Fehlalarm-Anteil streut je Stadtteil 28,8–61,0 %, Technische Hilfe 11,6–44,9 % (Spannen > 30 pp, Ursache: automatische Brandmeldeanlagen in Hochhäusern/Gewerbe) – beide liegen binär in „Nicht-Brand" und sind unsichtbar; Brand selbst streut nur 20,7 pp. (c) **Exposé-treuer:** dort ist die Einsatzart „kategoriale Zielgröße", und „seltene Kategorien zusammenfassen" ist die zuerst genannte Option (S. 5). (d) Schwellenwert-Kalibrierung entfällt → Basisraten-Problem entschärft | umgesetzt in `modellierung/klassifikation_daten.py`; **mit Schröter bestätigen** |
| 20 | 2026-07-26 | **Klassifikation: Design festgelegt** (`docs/KLASSIFIKATION_DESIGN.md`). Ebene Einzeleinsatz, gleiche Abgrenzung wie die Regression (2015-01–2025-12, 35 Stadtteile, 350.481 Einsätze, 13,6 % Brand). **Ergebnisvariablen ausgeschlossen** (Sachschaden, Löschfahrzeuge/-kräfte, Alarmstufe, Antwortzeit – erst nach dem Einsatz bekannt). Ridge-Pendant = `LogisticRegression(penalty="l2")`. Schwellenwert je Fold auf dem inneren Fenster, **AUROC primär** | Die Basisrate verschiebt sich über den Zeitraum (Training 12,0 % → Test 17,3 % in Fold 1), weil die absolute Zahl der Brände 2019→2023 um 85 % stieg, bei nahezu konstanten Nicht-Brand-Einsätzen. AUROC ist davon unberührt, F1 nicht → Schwelle zeitnah kalibrieren, Anstieg als Befund in Kap. 6. Pseudo-Signal: 350.481 Zeilen enthalten nur **4.619 verschiedene Stadtteil-Monats-Profile** → SHAP nur blockweise, keine Signifikanztests auf Einsatz-Ebene | Design festgelegt; Umsetzung offen |
| 19 | 2026-07-26 | **Park-/Institutionsgebiete ausgeschlossen:** Golden Gate Park, Lincoln Park, McLaren Park (`PARKGEBIETE` in `aggregation.py`) → **35 Stadtteile, 4.620 Beobachtungen**. Zusätzlich geht der Kriminalitätsindex **logarithmiert** ins Modell (`log_kriminalitaetsindex`) | Gebiete ohne nennenswerte Wohnbevölkerung sind für ein bevölkerungsbezogenes Risikomodell keine sinnvolle Analyseeinheit: Golden Gate Park hat **45 Einwohner**, einen Kriminalitätsindex von 186 im Median und 7.394 Einsätze je 1.000 Ew./Jahr (Median aller übrigen: 14.435 Ew.). Dieser eine Stadtteil erzeugte eine Schiefe von 9,0 im Index und **Ridge (S) R² −3,9**. Nach Ausschluss + Log-Transformation: Schiefe 0,66, **Ridge (S) R² 0,79**. Entscheidung über die Analyseeinheit, keine Ausreißerbereinigung nach Zielgröße | umgesetzt & validiert; **mit Schröter bestätigen** |
| 18 | 2026-07-26 | **Analysezeitraum dauerhaft festgesetzt: 2015-01 bis 2025-12** (`START`/`ENDE` in `aggregation.py`, nicht mehr aus den Daten abgeleitet) | Reproduzierbarkeit: Jeder Lauf liefert denselben Zeitraum, unabhängig davon, wie weit der letzte DataSF-Download reicht. 132 Monate × 38 Stadtteile = 5.016 Beobachtungen; nach Lag-Bildung 120 Monate | umgesetzt & validiert |

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
| 2026-07-18 | Anthropic Claude (Fable) | „Analysiere, wie wir die Probleme bereits in der Prep-Pipeline adressieren können, mache einfache Anpassungen, validiere per Demo-Test und schreibe eine Stichpunkt-Zusammenfassung für das Kapitel Data Preparation. Untersuche, ob zusätzliche Attribute die Probleme beheben und welche Baseline nötig ist." → Pipeline-Anpassungen (Dedup, ACS-/Crime-Join), Lag-Feature-Test, `docs/kapitel_5_2_data_preparation_stichpunkte.md` |
| 2026-07-26 | Anthropic Claude (Opus) | „Prüfe eine externe Kritikliste mit 11 Preprocessing-Prüfaufträgen gegen den tatsächlichen Repo-Stand und gib aus, was im Preprocessing noch zu erledigen ist und welche Entscheidungen mit welchen Auswirkungen zu treffen sind." → `docs/PREPROCESSING_AUDIT_2026-07-26.md` |
| 2026-07-26 | Anthropic Claude (Opus) | „Baue einen relativen Kriminalitätsindex je Stadtteil und Monat aus beiden SFPD-Datensätzen (vor und nach 2018), setze den Analysezeitraum dauerhaft fest und beschreibe in einfachen Worten, was für eine langfristig korrekte Data Preparation noch zu tun ist." → Decision Log #17–#19, `docs/NAECHSTE_SCHRITTE.md` |
| 2026-07-26 | Anthropic Claude (Opus) | „Rechne die Eignungsprüfung neu (nur auf Trainingsdaten) und entwirf den Klassifikationsteil." → `analyse/eignungspruefung.py` (Neufassung), `docs/KLASSIFIKATION_DESIGN.md`, Decision Log #20 |
| 2026-07-26 | Anthropic Claude (Opus) | „Räume Dokumentation und Hilfsskripte auf, setze alles auf den neuesten Stand und analysiere anhand von Exposé, Vorgaben, Pipeline und Ergebnissen, was bei der Modellierung noch zu Fehlern führen kann." → `docs/RISIKEN_MODELLIERUNG.md`, `docs/archiv/`, Aktualisierung von README, DATA_DICTIONARY und Umsetzungsleitfaden |
| 2026-07-26 | Anthropic Claude (Opus) | „Setze die Priorität-1-Punkte des Audits um (Randmonat-Bugfix, Exposure-Kontrolle, ACS-Publikationsversatz, End-Hold-out) und beschreibe die Änderungen." → Decision Log #11–#16, `modellierung/cv.py`, Anpassungen in `02_join.py`, `aggregation.py`, `demo_modellierung.py` |
| 2026-07-18 | Anthropic Claude (Fable) | „Prüfe, ob noch Anpassungen in der Data Preparation nötig sind (Safe-to-Train, wissenschaftliche Vergleichbarkeit der drei Algorithmen), und schreibe Kapitel 5 der Arbeit in LaTeX mit ausgewählten, erklärten Code-Snippets inkl. Highlighting-Setup." → Audit-Fix #10 (bfill-Leakage), Hauptanalyse ab 2014 (#5), `docs/kapitel_5_empirische_analyse.tex` |
