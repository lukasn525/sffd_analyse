# Umsetzungsleitfaden Modellierung – Ridge · Random Forest · XGBoost

> **Hinweis (2026-07-27):** Dieses Dokument nennt teilweise die alten Dateinamen
> (`pipeline/`, `modellierung/`, `analyse/`). Seit dem Struktur-Umbau liegt die
> gesamte Aufbereitung in `prep/`, die Modellskripte in `modelle/`. Die Zuordnung
> alt → neu steht in `ORIENTIERUNG.md`, Abschnitt 5 (Decision Log #22). Inhaltlich
> gilt alles Folgende unverändert weiter.

> Verbindlicher Fahrplan für den Ausbau von `modellierung/` bis zur fertigen
> Vergleichsstudie. Ergänzt `CLAUDE.md` (Rahmenplan) um die konkrete
> Programmier-Reihenfolge. Grundlage: Exposé, Schröter-Vorgaben,
> Eignungsprüfung (`results/eignungspruefung/`), Demo-Ergebnisse
> (`results/demo_modellierung/`).
>
> **Revision 2026-07-26** nach dem Preprocessing-Audit: A2 war eine
> Fehldiagnose und ist gestrichen, A3 und A4 sind erledigt. Die Prep-Pipeline
> wurde mit Zustimmung an drei Stellen angepasst (ACS-Publikationsversatz,
> Kriminalitätsindex, Dedup) – Details im Decision Log von `CLAUDE.md`.
> Den Klassifikationsteil beschreibt jetzt `docs/KLASSIFIKATION_DESIGN.md`
> ausführlicher als Schritt 7 hier.

---

## Teil A – Hürden, Risiken und ihre Lösungen

### A1. Die naive Baseline schlägt das Strukturmodell (KRITISCH – Decision Log #8)

**Problem:** Lag-1-Autokorrelation der Zielgröße = 0,96. Demo: Naiv R² 0,88 vs.
Ridge (nur Strukturmerkmale) R² −0,10. Ohne Gegenmaßnahme lautet das Ergebnis
der Arbeit „alle drei Verfahren sind schlechter als der Vormonatswert" – der
Verfahrensvergleich (Kern der Forschungsfrage) wird dann uninformativ, weil
alle Modelle an derselben strukturellen Grenze scheitern.

**Lösung (empfohlen, mit Schröter abstimmen):** Zwei Feature-Sets definieren
und BEIDE berichten:
- **Set S (Struktur):** nur sozioökonomische, kriminalitätsbezogene, bauliche
  Merkmale + Saison → beantwortet Unterfrage 1 („Erklärungsbeitrag").
- **Set S+L (Struktur + Lags):** zusätzlich `lag_1`, `lag_12`,
  `rolling_mean_3` der Einsatzzahl je Stadtteil → faire Prognoseaufgabe, auf
  der sich die drei Verfahren sinnvoll differenzieren können.

Alle drei Modelle bekommen je Set exakt dieselben Daten. Story der Arbeit:
„Strukturmerkmale allein erklären das Niveau (Querschnitt), nicht die Dynamik;
erst mit zeitlichen Features schlagen ML-Modelle die naiven Baselines – der
Beitrag der Strukturmerkmale wird über SHAP quantifiziert." Das ist ehrlich,
methodisch sauber und liefert die Story/roten Faden, den Schröter verlangt.

**Achtung Leakage:** Lags ausschließlich per `groupby("stadtteil").shift(k)`
berechnen (nie über Stadtteilgrenzen, nie mit Zukunftswerten); erste k Monate
je Stadtteil fallen weg (dropna) – identisch für alle Modelle.

### A2. ~~Fold-Instabilität / Nichtstationarität~~ → **KORRIGIERT (2026-07-26)**

> **Diese Hürde existiert nicht.** Der beschriebene Einbruch von Fold 3
> (Naiv R² 0,74 statt 0,96) war **kein Nichtstationaritäts-Befund**, sondern ein
> Datenartefakt: Die Randmonat-Logik in `aggregation.py` schnitt nur den
> maximalen Monat ab, sodass **Januar 2026 mit 258 statt ~3.300 Einsätzen** als
> scheinbar vollständiger Monat im Panel blieb – genau im Testfenster des letzten
> Folds. Nach dem Fix (Konstante `ENDE = 202512`, Decision Log #12) sind die
> Folds stabil: naive Baseline R² 0,95 / 0,95 / 0,96.
>
> Die COVID-Erklärung war eine nachträgliche Rationalisierung eines Bugs. Sie
> darf **nicht** in die Arbeit übernommen werden.

**Was aus A2 gültig bleibt:**

- `jahr` nicht als rohes Feature verwenden – Bäume können nicht extrapolieren,
  Ridge schon; das verzerrt genau den Verfahrensvergleich. Das Zeitniveau tragen
  die Lag-Features. (umgesetzt)
- Ergebnisse immer je Fold **und** als Mittelwert ± Standardabweichung berichten.
  (umgesetzt in `demo_modellierung.py`)
- Randmonats-Warnung `pruefe_randmonate()` bei jedem Neu-Download beachten.

### A3. ~~Fehlende Werte `akademikerquote_pct`~~ → **ERLEDIGT**

Der Analysezeitraum ist auf **2015-01 bis 2025-12** festgesetzt
(`START`/`ENDE` in `aggregation.py`, Decision Log #18). `akademikerquote_pct`
ist ab dem ACS-Jahrgang 2014 verfügbar, der mit dem Publikationsversatz von
einem Jahr (Decision Log #11) ab 2015 nutzbar ist. Im Panel gibt es dadurch
**keine fehlenden Werte mehr**; eine Imputation ist nicht erforderlich.
Offen bleibt nur die Sensitivitätsanalyse „voller Zeitraum ohne dieses Feature".

### A4. ~~Ausreißer / Datenartefakte~~ → **ERLEDIGT**

Sechs Stadtteile sind ausgeschlossen, aus zwei verschiedenen Gründen
(Decision Log #15 und #19) – **41 → 35 Stadtteile**:

| Stadtteil | Grund |
|---|---|
| Treasure Island, Lakeshore | in keinem ACS-Jahrgang enthalten |
| Mission Bay | erst ab ACS 2021 → würde ein unbalanciertes Panel erzeugen |
| Golden Gate Park (45 Ew.), Lincoln Park (299), McLaren Park (507) | Park-/Institutionsgebiete ohne nennenswerte Wohnbevölkerung – jede Pro-Kopf-Größe wird dort sinnlos |

Wichtig für die Formulierung in der Arbeit: Das ist eine **Entscheidung über die
Analyseeinheit**, keine Ausreißerbereinigung nach der Zielgröße. Letzteres wäre
methodisch angreifbar, Ersteres ist begründbar. Der Robustheitslauf mit allen
38 Einheiten ist über `lade_stadtteil_monat(mit_parkgebieten=True)` möglich.

### A5. Zähldaten & Retransformation (Ridge)

**Problem:** Ridge auf log(1+y) liefert Vorhersagen auf Log-Skala; naives
`expm1` unterschätzt systematisch den Erwartungswert (Jensen-Ungleichung).

**Lösung:** Gütemaße IMMER auf der Originalskala berechnen (nach `expm1`),
damit alle Modelle vergleichbar sind. Optional Duan-Smearing-Korrektur als
Fußnote/Robustheitscheck. RF/XGB direkt auf y (roh) trainieren – Bäume brauchen
keine Transformation; das ist KEIN Fairness-Bruch, sondern modellspezifische
Aufbereitung (wie Skalierung nur für Ridge) – im Methodenkapitel begründen.
XGBoost-Alternative: `objective="count:poisson"` als Robustheitscheck.

### A6. Overdispersion & NegBin-Konvergenz

`statsmodels` NegBin kann Konvergenzprobleme haben. Reihenfolge: (1) GLM-NB mit
via Hilfsregression geschätztem alpha, (2) fallback `NegativeBinomial` MLE mit
`method="nm"`-Start, (3) notfalls Poisson-QMLE mit robusten Standardfehlern und
Overdispersion explizit diskutieren. NegBin bleibt untunierte, interpretierbare
Referenz – kein Wettbewerber.

### A7. Faires Tuning & Vergleich

- **Identische Folds** für alle Modelle (ein einziges `zeit_folds`-Objekt,
  zentral definiert).
- **Tuning nur auf Trainingsfenstern:** je Fold inneres Validierungsfenster
  (letzte 12 Trainingsmonate) für Randomized Search; NIE auf Testmonaten
  tunen. Gleiches Budget: 50 Iterationen, gleicher `random_state`.
- **Kein Early Stopping** bei XGBoost auf Testdaten (Leakage) – nur auf dem
  inneren Validierungsfenster.
- **Einheitliches Encoding:** für die Klassifikation One-Hot für ALLE Modelle
  (auch XGBoost, statt nativem Categorical-Support) → identische Designmatrix,
  Unterschiede sind rein algorithmisch. Als Entscheidung dokumentieren.
- **Skalierung** nur für Ridge, innerhalb der sklearn-Pipeline (fit nur auf
  Train) – bereits so in der Demo.

### A8. Klassifikation: Ridge-Äquivalent & Imbalance

- Exposé verlangt alle drei Verfahren auch für die Einsatzart. Für „Ridge" in
  der Klassifikation **L2-regularisierte logistische Regression** verwenden
  (liefert Wahrscheinlichkeiten für AUROC; `RidgeClassifier` hat kein
  `predict_proba`). Als methodische Präzisierung ins Decision Log.
- Imbalance 13/87: `class_weight="balanced"` (LogReg, RF) bzw.
  `scale_pos_weight≈6,6` (XGB); Bewertung mit F1 (positiv = Brand) + AUROC;
  Schwellenwert auf innerem Validierungsfenster wählen, nicht 0,5 blind.
- Zeitbewusst auch hier: Splits nach Monaten (dieselben Foldgrenzen), keine
  zufällige Durchmischung. 720k Zeilen: für Tuning auf z. B. 150k
  Trainingszeilen subsampeln (stratifiziert, nur Train), final auf vollem
  Trainingsfenster fitten.

### A9. Laufzeiten (Unterfrage 3)

`time.perf_counter()` um `fit` und `predict`; je Modell/Fold 3 Wiederholungen,
Median berichten; Hardware im Methodenkapitel nennen; `n_jobs`/Threads
festhalten (RF `n_jobs=-1`, XGB `tree_method="hist"`). Nicht Tuning-Zeit mit
Fit-Zeit vermischen – getrennt ausweisen.

### A10. Reproduzierbarkeit & Formalia (Schröter)

- `random_state=42` überall; `requirements.txt` mit exakten Versionen
  einfrieren (`pip freeze`); Ergebnisse als CSV nach `results/`.
- **Zeilenreihenfolge gehört zum Reproduzierbarkeitsvertrag.** Random Forest und
  XGBoost ziehen Bootstrap- bzw. Subsample-Stichproben über Zeilenpositionen –
  eine andere Sortierung liefert trotz identischem `random_state` leicht andere
  Bäume (empirisch 17,2587 statt 17,2974 RMSE in Fold 1; innerhalb der
  Fold-Streuung von ±0,8, aber nicht bitgleich). Ridge ist reihenfolgeinvariant.
  Die Sortierung ist in `features.py` auf `["jahr_monat", "stadtteil"]`
  festgelegt und darf nicht mehr geändert werden.
- Methodenkapitel so schreiben, dass Schröter die Arbeit nachbauen könnte
  (Datenquellen-IDs, Filter, Folds, Suchräume, Seeds – vieles steht schon in
  CLAUDE.md Abschnitt 4–6).
- Abgabe-Zip ≤ 250 MB: `venv/` und `data/raw/` ausschließen (Rohdaten über
  `01_fetch.py` reproduzierbar; `data/processed/` ~80 MB passt, zur Not nur
  `sf_fire_risk_features.parquet` beilegen). Flüchtige Quellen als PDF/A.
- Jede KI-Session in CLAUDE.md Abschnitt 9 nachtragen (KI-Verzeichnis).
- Mind. 3/4 Fließtext: Code-Snippets kurz halten, Skripte referenzieren.

---

## Teil B – Programmierplan in Teilschritten

Zielstruktur (Ausbau von `modellierung/`, Demo bleibt als Vorstufe erhalten):

```text
modellierung/
├── aggregation.py        # [x] Stadtteil×Monat, Zeitraum, Exposure, Panel
├── cv.py                 # [x] Folds, inneres Fenster, End-Hold-out, Gütemaße
├── features.py           # [ ] Feature-Sets S und S+L (aktuell noch in der Demo)
├── baselines.py          # [ ] naiv, saisonal, NegBin
├── train_regression.py   # [ ] Ridge/RF/XGB + Tuning, Regression
├── klassifikation_daten.py # [ ] Zielgröße + Merkmalsblöcke Einzeleinsatz
├── train_klassifikation.py # [ ] LogReg-L2/RF/XGB, Brand vs. Nicht-Brand
├── shap_analyse.py       # [ ] SHAP für finale Modelle
└── demo_modellierung.py  # [x] Demo (wird von train_regression abgelöst)
results/
├── regression/           # Metriken je Fold/Modell/Feature-Set (CSV)
├── klassifikation/
└── shap/
```

### Schritt 1 – `features.py`: Datensatz finalisieren

> **Teilweise erledigt.** Zeitraum, Ausschlüsse und Exposure sitzen bereits in
> `aggregation.py` (`START`/`ENDE`, `PARKGEBIETE`, `balanciertes_panel()`,
> `log_bevoelkerung`, `log_kriminalitaetsindex`) mit eigenen Selbsttests.
> Offen ist nur, die Lag-/Saison-Features aus `demo_modellierung.py` in ein
> eigenes Modul zu ziehen.

1. ~~Zeitraumfilter~~ → festgesetzt in `aggregation.py` (2015-01 – 2025-12, #18).
2. ~~Ausreißer ausschließen~~ → `PARKGEBIETE` + `balanciertes_panel()` (#15, #19).
3. Lag-Features je Stadtteil: `lag_1`, `lag_12`, `rolling_mean_3`
   (`shift(1)` vor `rolling` → kein Leakage); erste 12 Monate fallen weg.
4. Saison: `monat_sin`, `monat_cos` (bereits in der Demo).
5. Zwei Spaltenlisten exportieren: `FEATURES_S` (Struktur + Saison),
   `FEATURES_SL` (S + Lags). KEIN rohes `jahr` (A2).
6. **Done-Kriterium:** DataFrame ohne NaN; Zeilenzahl und Zeitraum werden
   geloggt; Assert-Block prüft: keine NaN, Lags korrekt verschoben (Stichprobe),
   **35 Stadtteile, 4.620 Zeilen vor bzw. 4.200 nach Lag-Bildung**.

### Schritt 2 – `cv.py`: Validierung zentralisieren

1. `zeit_folds(monate, n_folds=3, test_monate=12)` aus Demo hierher ziehen.
2. Zusätzlich `inneres_fenster(train_monate, val_monate=12)` → für Tuning.
3. `bewerte_regression(y, y_hat)` (RMSE/MAE/R²) und
   `bewerte_klassifikation(y, p_hat, schwelle)` (F1, AUROC) hierher.
4. **Done:** Demo läuft nach Umbau unverändert (Import aus `cv.py`),
   identische Zahlen wie `results/demo_modellierung/` (Regressionstest!).

### Schritt 3 – `baselines.py`

1. Naiv + saisonal aus Demo übernehmen.
2. NegBin: `statsmodels` GLM auf Set S (Struktur), Konvergenz-Fallbacks
   gemäß A6; Vorhersagen auf Testfenster, gleiche Metriken.
3. **Done:** Baseline-CSV je Fold in `results/regression/baselines.csv`.

### Schritt 4 – Ridge Regression (`train_regression.py`, Modell 1)

1. Pipeline: `StandardScaler() → Ridge()`; Ziel `log1p(y)`, Rücktransformation
   `expm1`, Metriken auf Originalskala (A5).
2. Tuning: RandomizedSearch von Hand über das innere Fenster (A7):
   50× `alpha ~ loguniform(1e-3, 1e3)`; bestes alpha je Fold neu fitten auf
   vollem Trainingsfenster.
3. Beide Feature-Sets (S, S+L) durchlaufen.
4. Laufzeitmessung gemäß A9.
5. **Done:** `results/regression/ridge.csv` mit Spalten
   `fold, feature_set, alpha_best, rmse, mae, r2, fit_s, predict_s`.

### Schritt 5 – Random Forest (Modell 2)

1. `RandomForestRegressor(n_jobs=-1, random_state=42)` direkt auf y (roh).
2. Suchraum (Probst et al. 2019): `n_estimators` 200–1000,
   `max_depth` {None, 10–40}, `min_samples_leaf` 1–20,
   `max_features` {sqrt, 0.3–1.0}; 50 Iterationen, gleiches inneres Fenster.
3. Gleiche Outputs/Messungen wie Schritt 4 → `rf.csv`.
4. **Done:** RF (S+L) schlägt saisonale Baseline; falls nicht → Befund
   dokumentieren, nicht verschleiern.

### Schritt 6 – XGBoost (Modell 3)

1. `XGBRegressor(tree_method="hist", random_state=42)`; Suchraum:
   `n_estimators` 200–2000, `learning_rate` loguniform(0.005, 0.3),
   `max_depth` 3–10, `subsample` 0.5–1.0, `colsample_bytree` 0.5–1.0,
   `reg_lambda` loguniform(1e-2, 10).
2. Kein Early Stopping auf Test; optional auf innerem Fenster (A7).
3. Robustheitscheck `objective="count:poisson"` (eine Zeile im Bericht).
4. **Done:** `xgb.csv`; Vergleichstabelle aller Modelle+Baselines je
   Feature-Set wird von `train_regression.py` am Ende gedruckt und als
   `results/regression/vergleich.csv` gespeichert.

### Schritt 7 – Klassifikation (`train_klassifikation.py`)

1. Ziel: `ist_brand = einsatzart beginnt mit "1"` (NFIRS 100er), Ebene
   Einzeleinsatz; Zeitraum wie Regression; Dedup wie in `aggregation.py`.
2. Features: Stadtteil-Strukturmerkmale + Zeitfeatures (`stunde`, `wochentag`,
   `ist_nacht`, `ist_wochenende`, `monat_sin/cos`) + `bataillon` One-Hot
   (einheitlich für alle drei Modelle, A7).
3. Modelle: `LogisticRegression(penalty="l2", class_weight="balanced")` ·
   `RandomForestClassifier(class_weight="balanced")` ·
   `XGBClassifier(scale_pos_weight≈6.6)`; Tuning wie Regression
   (50 Iterationen, inneres Fenster, Subsampling fürs Tuning gemäß A8).
4. Bewertung: F1 (Brand) + AUROC je Fold; Schwellenwahl auf innerem Fenster.
5. **Done:** `results/klassifikation/vergleich.csv` + Laufzeiten.

### Schritt 8 – SHAP (`shap_analyse.py`) → Details Teil C

### Schritt 9 – Ergebnis-Konsolidierung

1. Eine Tabelle Regression (Modelle × Feature-Sets × Metriken ± Std),
   eine Tabelle Klassifikation, eine Tabelle Laufzeiten → direkt in
   Kap. 5.4 übernehmbar.
2. CLAUDE.md aktualisieren: CRISP-DM-Status, Decision Log, Befunde.
3. `pip freeze > requirements.txt` (im venv) einfrieren.

---

## Teil C – SHAP richtig einsetzen (Kap. 5.5)

### C1. Wofür SHAP hier dient

SHAP beantwortet Unterfrage 1 quantitativ: *Wie groß ist der Beitrag der
sozioökonomischen, kriminalitätsbezogenen und baulichen Merkmale zur
Prognose?* – insbesondere im Set S+L, wo Lags dominieren dürften. SHAP erklärt
das MODELL, nicht die Welt: keine Kausalaussagen (Shmueli 2010, Molnar 2022) –
diesen Satz wörtlich ins Methodenkapitel.

### C2. Korrektes Vorgehen (Regression)

1. **Ein finales Modell je Verfahren:** bestes Modell aus dem letzten Fold
   (größtes Trainingsfenster), getunte Hyperparameter.
2. **Explainer-Wahl:**
   - RF/XGB: `shap.TreeExplainer(model, data=background,
     feature_perturbation="interventional")` – exakt und schnell.
   - Ridge: `shap.LinearExplainer(ridge, background)` auf den
     **skalierten** Features (Explainer auf die sklearn-Pipeline-Stufen
     aufteilen: erst `scaler.transform`, dann Explainer auf `Ridge`).
3. **Background-Daten:** Zufallsstichprobe (z. B. 500 Zeilen) NUR aus dem
   Trainingsfenster – nie aus Testdaten.
4. **Erklärt werden Testdaten** des letzten Folds (out-of-sample, konsistent
   zur Evaluation).
5. **Skalen-Hinweis:** Ridge-SHAP-Werte liegen auf der log(1+y)-Skala,
   Baum-SHAP auf der Originalskala → SHAP-Werte NICHT zwischen Modellen
   absolut vergleichen, sondern **Rangfolgen und Anteile** vergleichen
   (mean|SHAP| normiert auf Summe = 100 %).

### C3. Auswertungen (genau diese vier, mehr braucht die Arbeit nicht)

1. **Beeswarm/Summary-Plot** je Modell (globale Wichtigkeit + Wirkrichtung).
2. **Bar-Plot mean|SHAP|**, aggregiert nach **Feature-Gruppen**:
   sozioökonomisch / Kriminalität / baulich / Saison / Lags. Die Gruppierung
   (mean|SHAP| der Gruppenmitglieder summieren) ist die zentrale Grafik der
   Arbeit: Sie zeigt den Erklärungsbeitrag der Exposé-Merkmalsgruppen.
3. **Dependence-Plots** für die 2–3 stärksten Strukturmerkmale (erwartet:
   `anteil_risikogewerbe_pct`, `armutsquote_pct`) → Nichtlinearitäten sichtbar
   (Mehrwert der Baummodelle gegenüber Ridge diskutieren).
4. **Klassifikation:** TreeExplainer auf Margin-Output (`raw`), im Text als
   Log-Odds interpretieren; alternativ `model_output="probability"` nur, wenn
   die Interventional-Voraussetzungen erfüllt sind – Margin ist der sichere
   Standard. Für LogReg: Koeffizienten × Std als Quervalidierung der
   SHAP-Rangfolge (Konsistenz-Check, stärkt die Wissenschaftlichkeit).

### C4. Typische SHAP-Fehler, die du vermeiden musst

- **Korrelierte Features** (Einkommen/Miete, VIF 8,8/7,7): SHAP verteilt
  Beiträge auf korrelierte Features → einzelne Rangplätze nicht überbewerten,
  Gruppenaggregation (C3.2) berichten; im Text explizit erwähnen.
- **KernelExplainer vermeiden** (langsam, approximativ) – hier unnötig, alle
  Modelle haben exakte Explainer.
- **Background aus Testdaten** oder ganzem Datensatz → subtiles Leakage.
- **SHAP auf untuniertem Modell** → erst tunen, dann erklären.
- **Kausalsprache** („Armut verursacht Einsätze") → immer „trägt zur Prognose
  bei / ist assoziiert".

---

## Teil D – Kurz-Checkliste vor jedem Sprechstunden-Termin

- [ ] Decision Log in CLAUDE.md aktuell? (offen: #3, #4, #5, #8, LogReg-Präzisierung)
- [ ] Ergebnisse je Fold + Mittelwert±Std, immer gegen beide Baselines?
- [ ] KI-Verzeichnis (CLAUDE.md §9) um neue Sessions ergänzt?
- [ ] Läuft `python modellierung/train_regression.py` durch (Reproduzierbarkeit)?
- [ ] Story konsistent: Struktur erklärt Niveau, Lags erklären Dynamik, SHAP quantifiziert den Beitrag?
