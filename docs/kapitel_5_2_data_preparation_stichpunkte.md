# Stichpunkte Kapitel 5.2 „Data Preparation" (CRISP-DM Phase 3)

> Rohmaterial zum Ausformulieren; gegliedert exakt nach deiner LaTeX-Struktur.
> Alle Zahlen sind validiert (Pipeline-Lauf + Demo-Test vom 2026-07-18,
> `results/eignungspruefung/`, `results/demo_modellierung/`).
> Hinweis KI-Verzeichnis: Abschnitt 5.2.2 trägt bereits Fußnote (Prompt 2);
> die Sessions vom 2026-07-18 sind in CLAUDE.md §9 erfasst und müssen ins
> KI-Verzeichnis der Thesis übernommen werden.

---

## 5.2.1 Bereinigung und Behandlung fehlender Werte

**Bereinigungsschritte (mit Regel, Anteil, Begründung):**
- Ausgangsbasis: 720.258 SFFD-Einsatzdatensätze (2003–2026, DataSF `wr8u-xric`),
  nur Zeilen mit vorhandenem Stadtteil und Ankunftszeit abgerufen.
- Duplikatentfernung: 269 mehrfach gemeldete Einsatznummern (0,04 %, davon 50
  vollständig identische Zeilen) → Deduplikation nach `incident_number`,
  Ergebnis 719.989 Einsätze. Begründung: Artefakt der Quelldaten (mehrfache
  Meldung), nicht der Join-Logik; ohne Bereinigung würden Einsatzzählungen
  systematisch leicht überschätzt.
- Plausibilitätsfilter Ausrückzeit: `response_time_min` = Ankunft − Alarm,
  zulässig 0–60 min; entfernt ~1,7 % (negative Zeiten = Uhren-/Erfassungsfehler,
  >60 min = Protokollartefakte). Konsequenz: Einsatzzählungen beziehen sich auf
  qualitätsgefilterte Einsätze (Limitation erwähnen).
- Baujahr-Plausibilisierung Land Use: `yrbuilt` außerhalb 1800–2025 → fehlend.

**Fehlende Werte (Regel + Anteil + Entscheidung):**
- ACS-Merkmale: ~5,7 % der Einsätze ohne Zuordnung (kleine/neue Neighborhoods
  ohne Tract-Mapping im jeweiligen Jahrgang); Treasure Island und Mission Bay
  erhalten erst ab ACS 2021 Werte, Lakeshore ohne Baujahr-Angaben im Land-Use →
  betroffene Stadtteil-Monate entfallen über den NaN-Ausschluss (im
  Hauptzeitraum verbleiben 38–39 von 41 Stadtteilen).
- `akademikerquote_pct`: fehlt für alle Jahre mit ACS-2009-Zuordnung, da die
  ACS-Tabelle B15003 im Jahrgang 2009 nicht existiert; beim strikt
  prognostischen Join betrifft das alle Einsätze bis einschließlich 2013.
  Entscheidung: **Hauptanalyse ab 2014** (Feature vollständig verfügbar);
  Sensitivitätsanalyse über den vollen Zeitraum ohne dieses Feature; keine
  Imputation über Jahre hinweg (nicht verteidigbar). Wichtig: Rückwärts-Füllen
  fehlender Werte wurde bewusst unterbunden, da es Zukunftsinformation
  einschleusen würde (Audit-Befund, Decision Log #10).
- McLaren Park: Armutsquote 0,90 bei 850 Einwohnern → Census-Artefakt für
  Parkflächen; als Ausreißer aus der Modellierung ausgeschlossen,
  Robustheits-Check mit/ohne.

**Leakage-Vermeidung als Bereinigungsprinzip (eigene Leistung, betonen):**
- ACS-Join strikt prognostisch: jeder Einsatz erhält den *letzten verfügbaren*
  ACS-Snapshot (Jahrgang ≤ Einsatzjahr) – ein „nächster"-Join würde für frühe
  Jahre Zukunftsinformation einschleusen. Ausnahme 2003–2008 (Rückgriff auf
  ACS 2009, kein älterer Jahrgang; für Hauptanalyse ab 2012 irrelevant).
- Kriminalitätsmerkmale zeitbewusst: Deliktzahlen je Stadtteil werden nur bis
  zum Vorjahr des Einsatzes kumuliert (implementiert; aktiv nach Neu-Download
  der datierten Rohdaten, sonst statischer Fallback als Limitation).
- Land Use bleibt statischer Snapshot 2020 (einziger verfügbarer Jahrgang) →
  als quasi-stabiles bauliches Strukturmerkmal interpretiert, Limitation Kap. 6.3.

## 5.2.2 Räumlich-zeitliche Aggregation

**Begründung Stadtteil × Monat (Exposé-Absatz ausbauen, Alternativen abwägen):**
- Alternative 1 – Einzeleinsatz-Ebene: Stadtteil-Merkmale wiederholen sich
  identisch über hunderttausende Zeilen → künstlich aufgeblähte Stichprobe,
  Pseudo-Signifikanzen, Pseudo-Signale (im Exposé bereits angelegt).
- Alternative 2 – Census-Tract × Jahr: feinere räumliche, aber gröbere
  zeitliche Auflösung; SFFD-Einsätze tragen keine Tract-Zuordnung (nur
  Neighborhood), Crosswalk nur als Aggregationshilfe → nicht umsetzbar ohne
  fehleranfällige Geokodierung; Jahresebene ließe Saisonalität verschwinden.
- Gewählt – Stadtteil × Monat: kleinste Ebene, auf der Zielgröße (Zähldaten)
  und Prädiktoren konsistent vorliegen; erhält Saisonalität; 39 Stadtteile ×
  ~265 Monate ≈ 10.300 Beobachtungen.
- Technik: vollständiges Raster Stadtteil × Monat (Monate ohne Einsatz = echte
  Null); unvollständiger Randmonat abgeschnitten; Stadtteil-Merkmale je
  Stadtteil/ACS-Jahrgang konstant übernommen.
- Zielgrößen-Charakteristik nach Aggregation: Mittelwert 63,4 Einsätze/Monat,
  Median 45, Maximum 451, Dispersionsindex 61 (Var/Mean) → starke
  Overdispersion, begründet NegBin- statt Poisson-Baseline (Verweis auf 5.1).
- Klassifikationszielgröße (Einsatzart) verbleibt bewusst auf
  Einzeleinsatz-Ebene (Brand vs. Nicht-Brand, 13,1 %/86,9 %).

## 5.2.3 Feature Engineering und finaler Analysedatensatz

**Konstruierte Merkmale (Exposé-Absatz + Ergänzungen):**
- Bauliche Merkmale: Altbau-Anteil vor 1940 (`pre1940_count/yrbuilt_count`),
  Risiko-Gewerbe-Index (Fläche RETAIL/ENT + PDR / Gesamtfläche) – aus 155.395
  Parzellen, Spatial Join Centroid→Neighborhood-Polygon, Match-Rate 99,5 %.
- Sozioökonomische Raten: Armuts-, Akademiker-, Leerstandsquote als
  Zähler/Nenner-Ratios in [0,1]; Mediane (Einkommen, Miete)
  populationsgewichtet von Tract auf Stadtteil aggregiert.
- Kriminalitätsanteile: Gewalt- bzw. Eigentumsdelikte / Gesamtdelikte.
- Saisonmerkmale: zyklische Kodierung des Monats (sin/cos) – vermeidet den
  künstlichen Sprung Dezember→Januar einer ordinalen Kodierung.
- **Lag-Merkmale (zentrale Ergänzung ggü. Exposé, empirisch begründet):**
  `lag_1`, `lag_12`, `rolling_mean_3` der Einsatzzahl je Stadtteil, streng
  vergangenheitsbezogen (shift vor rolling → kein Leakage). Begründung:
  Lag-1-Autokorrelation der Zielgröße 0,96; ohne zeitliche Merkmale schlägt
  bereits die naive Vormonats-Baseline (R² 0,88) jedes reine Strukturmodell
  (Ridge R² 0,17). Mit Lags: Ridge R² 0,91, Random Forest 0,90 → Modelle
  übertreffen die Baseline (validierter Demo-Test, 3-Fold-TS-CV, Datensatz
  2015–2026). Kein rohes `jahr` als Feature (Extrapolationsproblem).
- Zwei Feature-Sets für die Auswertung: Set S (nur Struktur + Saison,
  beantwortet Unterfrage 1) und Set S+L (zusätzlich Lags, fairer
  Prognosevergleich); Erklärungsbeitrag der Strukturmerkmale via SHAP.

**Encoding und Skalierung (modellspezifisch, innerhalb der CV-Pipeline):**
- Ridge: z-Standardisierung aller Prädiktoren (StandardScaler, nur auf
  Trainingsfenster gefittet); Zielgröße log(1+y) wegen Rechtsschiefe/
  Heteroskedastizität (Linearitätsprüfung 5.1: lineare Baseline R² 0,71
  vorhanden, aber Trichter-Residuen und negative Vorhersagen auf Rohskala);
  Lag-Features für Ridge ebenfalls log(1+x)-transformiert (log-AR-Form).
- Random Forest / XGBoost: keine Skalierung/Transformation nötig, Training
  direkt auf Zählwerten.
- Klassifikation: One-Hot-Encoding (`bataillon`, Zeitmerkmale) einheitlich für
  alle drei Verfahren → identische Designmatrix, Unterschiede rein algorithmisch.
- Fairness-Regel: alle Modelle erhalten identische Zeilen, Features und
  CV-Folds; modellspezifische Transformationen laufen ausschließlich innerhalb
  der sklearn-Pipeline je Trainingsfenster.

**Steckbrief finaler Analysedatensatz (als Tabelle setzen):**

| Merkmal | Wert |
|---|---|
| Regressionsdatensatz | Stadtteil × Monat, vollständiges Raster |
| Beobachtungen (Demo-Stand, 2015–2026) | 5.103 (39 Stadtteile × 133 Monate) |
| Hauptanalyse-Zeitraum | ab 2014 (Vollständigkeit `akademikerquote_pct` beim strikt prognostischen ACS-Join; Lags verbrauchen 12 Anlaufmonate) |
| Zielgröße Regression | `anzahl_einsaetze` (Ø 63,4; Median 45; Max 451; Dispersionsindex 61) |
| Prädiktoren | 11 Strukturmerkmale + 2 Saison + 3 Lags |
| Klassifikationsdatensatz | 719.989 Einzeleinsätze; Ziel Brand vs. Nicht-Brand (13,1 / 86,9 %) |
| Ausgeschlossen | Treasure Island, Lakeshore (keine ACS-Werte); McLaren Park (Census-Artefakt); Duplikate; Ausrückzeit ∉ [0, 60] min |
| Validierung | Time-Series-CV, expanding window, Testfenster 12 Monate |
| Baselines | naives Modell (Vormonat) – maßgebliche Hürde; saisonaler Monatsdurchschnitt; NegBin (interpretierbare Count-Referenz) |
