# Data Dictionary – einsaetze.parquet

*Stand 2026-07-27 (nach dem Struktur-Umbau, Decision Log #22).*

**Tabelle:** `data/processed/einsaetze.parquet`
(hieß bis 2026-07-27 `sf_fire_risk_features.parquet`)
**Zeilen:** 719.989 (ein Datensatz pro SFFD-Einsatz, 2003–2026, nach Dedup und
Antwortzeit-Filter)
**Spalten:** 50
**Trennzeichen (CSV-Export):** `;`

Jeder Einsatz wird mit Neighborhood-Merkmalen angereichert, die zum
Prognosezeitpunkt **tatsächlich verfügbar** waren:

| Quelle | Zeitliche Auflösung | Verfügbarkeitsregel |
|---|---|---|
| ACS (sozioökonomisch) | Snapshot je Jahrgang | letzter *publizierter* Jahrgang, `acs_jahr ≤ Einsatzjahr − 1` (Publikationsversatz, Decision Log #11) |
| Kriminalität | **Stadtteil × Monat** | rollierendes 12-Monats-Fenster, endend im **Vormonat** (Decision Log #17) |
| Land Use (baulich) | Snapshot 2020 | über den gesamten Zeitraum konstant (einziger verfügbarer Jahrgang) – dokumentierte Limitation |

## Analysedatensatz für die Modellierung

Diese Tabelle ist der Pipeline-Output auf **Einsatz-Ebene**. Die Modellierung
verwendet zwei davon abgeleitete Datensätze:

| | Regression | Klassifikation |
|---|---|---|
| Ebene | Stadtteil × Monat | Einzeleinsatz |
| Zeitraum | 2015-01 – 2025-12 (132 Monate, Lag-Vorlauf #23) | identisch |
| Einheiten | 35 Stadtteile | dieselben 35 Stadtteile |
| Beobachtungen | 4.620 (rechteckiges Panel) | 350.481 |
| Zielgröße | `anzahl_einsaetze` | `einsatzart_gruppe` (4 Klassen) + `ist_brand` (13,6 %) |
| Erzeugt in | `prep/regression_datensatz.py` | `prep/klassifikation_datensatz.py` |

Ausgeschlossene Stadtteile: Treasure Island, Lakeshore, Mission Bay (keine
durchgängige ACS-Abdeckung) sowie Golden Gate Park, Lincoln Park, McLaren Park
(Park-/Institutionsgebiete ohne nennenswerte Wohnbevölkerung).

---

## 1. SFFD-Einsatz­felder (Quelldaten)

> Quelle: DataSF – SFFD Fire Incidents (`wr8u-xric`)

| Spalte | Typ | Beschreibung | Wertebereich / Hinweis |
|---|---|---|---|
| `incident_number` | str | Eindeutige SFFD-Einsatznummer | z.B. `"14129561"` |
| `incident_date` | datetime | Datum des Einsatzes | 2003-01-01 – 2026 |
| `alarm_dttm` | datetime | Zeitpunkt Alarmeingang | – |
| `arrival_dttm` | datetime | Zeitpunkt Ankunft am Einsatzort | – |
| `neighborhood_district` | str | Neighborhood-Bezeichnung laut SFFD (Rohwert) | 41 Ausprägungen |
| `battalion` | str | Feuerwehrbataillon | z.B. `"B02"` |
| `primary_situation` | str | Primäre Einsatzkategorie (NFIRS-Code + Text) | z.B. `"111 - Building fire"` |
| `suppression_units` | int | Anzahl eingesetzter Löschfahrzeuge | ≥ 0 |
| `suppression_personnel` | int | Anzahl Löschkräfte | ≥ 0 |
| `ems_units` | int | Anzahl Rettungsdiensteinheiten | ≥ 0 |
| `number_of_alarms` | int | Alarmstufe | 1–5 |
| `civilian_fatalities` | int | Zivile Todesopfer | ≥ 0 |
| `civilian_injuries` | int | Zivile Verletzte | ≥ 0 |
| `no_flame_spread` | str | Flammenausbreitung eingedämmt | Heterogene Codierung: `"NA"`, `"NO"`, `"Y"`, `"YES"`, `"1"`–`"5"` – **nicht direkt modellierbar, ggf. rekodieren** |
| `estimated_property_loss` | float | Geschätzter Sachschaden (USD) | ≥ 0; 0 wenn kein Schaden oder unbekannt |

---

## 2. Abgeleitete Einsatz­felder (Zeitvariablen)

> Berechnet aus `alarm_dttm` / `incident_date` in `prep/join.py`

| Spalte | Typ | Beschreibung | Wertebereich |
|---|---|---|---|
| `response_time_min` | float | Ausrückzeit in Minuten (alarm→arrival) | 0 – 60 (Ausreißer gefiltert) |
| `year` | int | Einsatzjahr | 2003 – 2026 |
| `month` | int | Einsatzmonat | 1 – 12 |
| `hour` | int | Stunde des Alarmeingangs | 0 – 23 |
| `weekday` | int | Wochentag (ISO: 0 = Montag) | 0 – 6 |
| `is_weekend` | int | Wochenende-Dummy | 0 = Werktag, 1 = Samstag/Sonntag |
| `is_night` | int | Nacht-Dummy (22:00 – 05:59 Uhr) | 0 = Tag, 1 = Nacht |
| `neighborhood` | str | Normalisierter Neighborhood-Name (Title Case) | 41 Ausprägungen |
| `acs_year` | int | Zugeordneter ACS-Jahrgang (nächster verfügbarer Snapshot) | 2009, 2014, 2019, 2021, 2023 |

---

## 3. ACS-Rohdaten (Neighborhood-Ebene)

> Quelle: US Census Bureau – ACS 5-Year Estimates (`acs5`)
> Aggregation: Census Tracts → Neighborhood (populationsgewichteter Mittelwert für Mediane;
> Summe für Zähler/Nenner). Zeitbewusst: jeder Einsatz bekommt den ACS-Snapshot, der
> seinem Einsatzjahr am nächsten liegt.
>
> **NaN-Rate ~5,7 %:** Einsätze in Neighborhoods, für die im jeweiligen ACS-Jahrgang
> kein Census-Tract-Mapping vorlag (v.a. kleine/neue Neighborhoods in 2009).
>
> **bachelor_degree_count / education_universe_total NaN-Rate ~37 %:** ACS-Tabelle B15003
> war im Jahrgang 2009 nicht verfügbar; betrifft alle Einsätze 2003–2011.

| Spalte | Typ | Beschreibung | Einheit |
|---|---|---|---|
| `total_population` | Int64 | Wohnbevölkerung im Neighborhood | Personen |
| `median_household_income` | Int64 | Medianes Haushaltseinkommen (populationsgewichtet) | USD / Jahr |
| `median_gross_rent` | Int64 | Mediane Bruttomiete (populationsgewichtet) | USD / Monat |
| `poverty_below` | Int64 | Personen unterhalb der Armutsgrenze | Personen |
| `poverty_universe_total` | Int64 | Grundgesamtheit für Armutsberechnung | Personen |
| `bachelor_degree_count` | Int64 | Personen mit Bachelor-Abschluss (25+) | Personen |
| `education_universe_total` | Int64 | Grundgesamtheit für Bildungsberechnung (25+) | Personen |
| `vacant_housing_units` | Int64 | Leer stehende Wohneinheiten | Einheiten |
| `total_housing_units` | Int64 | Gesamtzahl Wohneinheiten | Einheiten |

---

## 4. Kriminalität (Neighborhood × Monat)

> **Seit 2026-07-26 vollständig überarbeitet (Decision Log #17).** Die früheren
> Merkmale `total_crimes`, `violent_crime_count`, `property_crime_count` und die
> daraus gebildeten Anteile existieren nicht mehr: Sie waren über den gesamten
> Zeitraum kumuliert (Zukunftsinformation), zeitlich konstant und maßen die
> Zusammensetzung statt der Intensität der Kriminalität.
>
> **Quellen** (zwei, weil der aktuelle Datensatz erst 2018 beginnt):
> - `tmnf-yvry` – SFPD Incident Reports historisch, 2014-01 bis 2017-12.
>   Ohne Stadtteilspalte → Spatial Join der Koordinaten gegen dieselbe
>   Neighborhood-Geometrie wie bei Land Use. Match-Rate 100,0 % (572.814 Delikte).
> - `e3si-785i` – SFPD Incident Reports monatlich voraggregiert, ab 2018-01
>   (992.441 Delikte). Enthält `analysis_neighborhood` direkt.
>
> Gezählt werden **alle** Straftaten; eine Harmonisierung der beiden
> Kategorienschemata ist deshalb nicht erforderlich.

**Berechnung** (Location Quotient der Kriminalitätsbelastung):

```
rate(i,t)     = Delikte(i, 12 Monate bis t−1) / Einwohner(i)
rate(Stadt,t) = Delikte(Stadt, gleiches Fenster) / Einwohner(Stadt)
index(i,t)    = rate(i,t) / rate(Stadt,t)
```

| Spalte | Typ | Beschreibung | Wertebereich |
|---|---|---|---|
| `crime_index` → `kriminalitaetsindex` | float | Relative Kriminalitätsbelastung. **1,0 = Stadtdurchschnitt desselben Monats**, 2,0 = doppelt so hoch | Panel: 0,05 – 13,55, Median 0,77 |
| `crime_rate_raw` → `kriminalitaetsrate_pro_1000_ew_roh` | float | Absolute Rate je 1.000 Einwohner, gleiches Fenster | **nur deskriptiv** – enthält den Strukturbruch 2018 |

**Warum relativ?** Im Mai 2018 stellte SFPD von CABLE auf das Crime Data
Warehouse um. Ein solcher stadtweiter Niveausprung wirkt auf Zähler und Nenner
des Quotienten gleich und kürzt sich heraus. Empirisch bestätigt: Rangkorrelation
der Stadtteile 2017 vs. 2019 = **0,975**, Median-Verhältnis 1,01
(`results/eignungspruefung/`). **Nicht** heraus kürzt sich eine Verschiebung in
der Zusammensetzung, die einzelne Stadtteile unterschiedlich trifft → Limitation
Kap. 6.3.

**Kein Leakage:** Das Fenster endet strikt im Vormonat. Für den ersten
Analysemonat 2015-01 werden die Delikte aus 2014-01 bis 2014-12 verwendet.

---

## 5. Land-Use-Rohdaten (Neighborhood-Ebene)

> Quelle: DataSF – Land Use 2020 (`ygi5-84iq`), Spatial Join via Parzel-Centroid gegen
> Analysis Neighborhood Boundaries (`j2bu-swwd`). Match-Rate: 99,5 % (154.544 / 155.395 Parzellen).
> **Statisch** – Snapshot 2020, kein zeitbewusster Join.

| Spalte | Typ | Beschreibung | Einheit |
|---|---|---|---|
| `parcel_count` | int | Anzahl Parzellen im Neighborhood | Parzellen |
| `yrbuilt_count` | int | Parzellen mit bekanntem Baujahr | Parzellen |
| `pre1940_count` | int | Parzellen gebaut vor 1940 | Parzellen |
| `pre1960_count` | int | Parzellen gebaut vor 1960 | Parzellen |
| `total_resunits` | Int64 | Wohneinheiten laut Parzelldaten (Summe `resunits`) | Einheiten |
| `residential_count` | int | Wohnparzellen (Landuse: RESIDENT, MIXRES) | Parzellen |
| `total_area_sqft` | float | Gesamtfläche aller Parzellen (Parzellebene, `st_area_sh`) | ft² |
| `high_risk_commercial_area_sqft` | float | Fläche brandrelevanter Gewerbeparzellen (RETAIL/ENT, PDR) | ft² |

---

## 6. Abgeleitete Variablen – ACS

> Berechnet in `prep/join.py` (`berechne_quoten`). Formel: Zähler / Nenner. **Wertebereich: [0, 1]**

| Spalte | Typ | Formel | Einheit | NaN% |
|---|---|---|---|---|
| `poverty_rate` | float | `poverty_below / poverty_universe_total` | [0, 1] | ~5,7 % |
| `bachelor_rate` | float | `bachelor_degree_count / education_universe_total` | [0, 1] | ~37,3 % (ACS 2009 fehlt) |
| `vacancy_rate` | float | `vacant_housing_units / total_housing_units` | [0, 1] | ~5,7 % |

---

## 7. Abgeleitete Variablen – Crime

> **Entfallen.** `pct_violent_crime` und `pct_property_crime` wurden am
> 2026-07-26 durch den Kriminalitätsindex ersetzt (s. Abschnitt 4). Die
> Berechnet in `prep/join.py` (`berechne_kriminalitaetsindex`)
> statt, weil sie eine Zeitdimension und die Einwohnerzahl benötigt.

---

## 8. Abgeleitete Variablen – Land Use

> Berechnet in `prep/join.py` (`berechne_quoten`). **Wertebereich: [0, 1]**

| Spalte | Typ | Formel | Einheit | NaN% |
|---|---|---|---|---|
| `pct_pre1940` | float | `pre1940_count / yrbuilt_count` | [0, 1] | ~2,2 % |
| `pct_pre1960` | float | `pre1960_count / yrbuilt_count` | [0, 1] | ~2,2 % |
| `pct_residential` | float | `residential_count / parcel_count` | [0, 1] | 0,0 % |
| `pct_high_risk_commercial_area` | float | `high_risk_commercial_area_sqft / total_area_sqft` | [0, 1] | 0,0 % |

---

## Werteübersicht abgeleitete Variablen (gewichtet nach Einsatzhäufigkeit)

| Variable | Mean | Median | Min | Max |
|---|---|---|---|---|
| `poverty_rate` | 0,1295 | 0,1126 | 0,0000 | 0,9038 |
| `bachelor_rate` | 0,3335 | 0,3487 | 0,0000 | 0,8000 |
| `vacancy_rate` | 0,1072 | 0,0937 | 0,0000 | 0,2371 |
| `pct_pre1940` | 0,7221 | 0,7689 | 0,0220 | 1,0000 |
| `pct_pre1960` | 0,8466 | 0,8923 | 0,6056 | 1,0000 |
| `pct_residential` | 0,6722 | 0,7952 | 0,0000 | 0,9708 |
| `pct_high_risk_commercial_area` | 0,0744 | 0,0525 | 0,0000 | 0,2218 |

---

## 9. Modellmerkmale (Analysepanel Stadtteil × Monat)

Die zehn Prädiktoren, die tatsächlich in die Modelle gehen
(`PRAEDIKTOREN` in `prep/config.py`). Werte bezogen auf das
Analysepanel 2015-01 – 2025-12, 35 Stadtteile, 4.620 Beobachtungen, keine NaN.

| Merkmal | Gruppe | Min | Median | Max | Zeitvarianz |
|---|---|---|---|---|---|
| `median_haushaltseinkommen` | sozioökonomisch | 20.562 | 112.328 | 246.635 | 4 Werte (ACS-Jahrgänge) |
| `armutsquote_pct` | sozioökonomisch | 0,007 | 0,096 | 0,361 | 4 Werte |
| `akademikerquote_pct` | sozioökonomisch | 0,147 | 0,360 | 0,553 | 4 Werte |
| `median_miete` | sozioökonomisch | 722 | 1.883 | 3.501 | 4 Werte |
| `leerstandsquote_pct` | sozioökonomisch | 0,005 | 0,082 | 0,237 | 4 Werte |
| `log_bevoelkerung` | Exposure | 7,77 | 9,73 | 11,31 | 4 Werte |
| `log_kriminalitaetsindex` | Kriminalität | −2,98 | −0,26 | 2,61 | **128 Werte (monatlich)** |
| `anteil_altbau_vor_1940_pct` | baulich | 0,128 | 0,769 | 0,908 | **konstant** |
| `anteil_wohngebaeude_pct` | baulich | 0,133 | 0,886 | 0,971 | **konstant** |
| `anteil_risikogewerbe_pct` | baulich | 0,000 | 0,034 | 0,222 | **konstant** |

Zusätzlich im Panel enthalten, aber **kein** Modellmerkmal:
`gesamtbevoelkerung` (roh, für NegBin-Offset und Raten-Sensitivität) und
`kriminalitaetsindex` (roh, für die Interpretation in Kap. 5.1).

---

## Hinweise für die Modellierung

- **Zwei Analyseebenen:** Regression auf Stadtteil × Monat, Klassifikation auf
  Einzeleinsatz. Beide mit identischer Abgrenzung (2015-01 – 2025-12,
  35 Stadtteile).
- **Pseudo-Signal auf Einsatz-Ebene:** Die Stadtteilmerkmale sind je
  Stadtteil-Monat konstant. 350.481 Einsätze enthalten nur 4.619 verschiedene
  Merkmalsprofile. Keine Signifikanztests auf Einsatz-Ebene; SHAP nur nach
  Merkmalsblöcken aggregiert (s. `docs/KLASSIFIKATION_DESIGN.md`).
- **Ergebnisvariablen nicht als Prädiktoren:** `schaetzung_sachschaden_usd`,
  `loeschfahrzeuge`, `loeschkraefte`, `alarmstufe`, `antwortzeit_min`,
  `zivile_verletzte`, `zivile_tote`, `flammenausbreitung_eingedaemmt` stehen erst
  nach dem Einsatz fest und sind vom Merkmalssatz ausgeschlossen.
- **`no_flame_spread`** enthält gemischte Codierungen (`"NA"`, `"NO"`, `"Y"`,
  `"1"`–`"5"`) – ohnehin ausgeschlossen (Ergebnisvariable).
- **Drei bauliche Merkmale ohne Zeitvarianz** (Land-Use-Snapshot 2020): Sie
  erklären Niveauunterschiede zwischen Stadtteilen, nicht deren zeitliche
  Entwicklung. In der Interpretation entsprechend formulieren.
- **`akademikerquote_pct`** ist erst ab dem ACS-Jahrgang 2014 verfügbar; mit dem
  Publikationsversatz von einem Jahr beginnt der Analysezeitraum daher 2015.
- **Antwortzeit-Filter** (0–60 min) entfernt ~1,7 % der Einsätze bereits in der
  Prep-Pipeline. Alle Zählungen beziehen sich auf den gefilterten Bestand.

---

## Die beiden finalen Datensätze

Aus dieser Tabelle entstehen in `prep/` die zwei Dateien, die die Modelle lesen.
Alle inhaltlichen Spalten sind oben beschrieben; hinzu kommen abgeleitete
Merkmale und die CV-Aufteilung.

### `data/processed/regression.parquet` – 4.620 × 24

| Spalte | Bedeutung |
|---|---|
| `stadtteil`, `jahr`, `monat`, `jahr_monat` | Schlüssel (`jahr_monat` = `jahr*100 + monat`) |
| `anzahl_einsaetze` | **Zielgröße**: Einsätze im Stadtteil-Monat (Monate ohne Einsatz = echte 0) |
| 10 Prädiktoren | `PRAEDIKTOREN` aus `prep/config.py`, s. Abschnitte oben |
| `log_bevoelkerung` | `log1p(gesamtbevoelkerung)`, Exposure-Kontrolle (#13) |
| `log_kriminalitaetsindex` | `log(kriminalitaetsindex)`, 0 = Stadtdurchschnitt (#17, #19) |
| `monat_sin`, `monat_cos` | Kalendermonat zyklisch kodiert |
| `lag_1`, `lag_12` | Einsatzzahl im Vormonat bzw. Vorjahresmonat, je Stadtteil |
| `rolling_mean_3` | Mittel der drei Vormonate (`shift(1)` vor `rolling(3)`) |
| `fold` | 1–3 = Monat liegt im **Testfenster** dieses Folds; 0 = nur Trainingsmaterial |
| `ist_holdout` | 1 = End-Hold-out 2025-01 – 2025-12, beim Tuning unberührt |
| `gesamtbevoelkerung`, `kriminalitaetsindex` | Rohwerte, kein Modellmerkmal – für NegBin-Offset, Raten-Sensitivität und Kap. 5.1 |

Das Trainingsfenster eines Folds ist aus `fold` und `ist_holdout` vollständig
ableitbar: alle Monate vor dem Testfenster, ohne Hold-out
(`prep/cv.fold_masken`).

### `data/processed/klassifikation.parquet` – 350.481 × 26

| Spalte | Bedeutung |
|---|---|
| `einsatz_nummer`, `stadtteil`, `jahr`, `monat`, `jahr_monat` | Schlüssel |
| `einsatzart_gruppe` | **Zielgröße A**: 4 zusammengefasste NFIRS-Serien (#21) |
| `ist_brand` | **Zielgröße B**: 1 = NFIRS 100er, binärer Robustheitslauf |
| Block A (10) | dieselben Strukturmerkmale wie in der Regression |
| `stunde_sin`, `stunde_cos` | Alarmzeitpunkt zyklisch über 24 Stunden |
| `monat_sin`, `monat_cos`, `ist_nacht`, `ist_wochenende` | weitere Zeitmerkmale |
| `wochentag` | kategorial, One-Hot erst im ColumnTransformer |
| `fold`, `ist_holdout` | wie oben, identische Zeitschnitte |

Ergebnisvariablen sind in beiden Dateien garantiert nicht enthalten; das prüft
`tests/test_aufbereitung.py`.
