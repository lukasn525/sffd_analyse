# Data Dictionary – sf_fire_risk_features.parquet

**Tabelle:** `data/processed/sf_fire_risk_features.parquet`
**Zeilen:** 720.258 (ein Datensatz pro SFFD-Feuerwehreinsatz, 2003–2026)
**Spalten:** 53
**Trennzeichen (CSV-Export):** `;`

Jeder Einsatz wird mit Neighborhood-Aggregaten angereichert, die zur Zeit des Einsatzes
gültig waren (zeitbewusster Join für ACS; Crime und Land Use sind statisch über alle Jahre).

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

> Berechnet aus `alarm_dttm` / `incident_date` in `02_join_data.py`

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

## 4. Crime-Rohdaten (Neighborhood-Ebene)

> Quelle: DataSF – SFPD Incident Reports, monatlich voraggregiert (`e3si-785i`)
> Aggregation: Summe aller monatlichen Snapshots über alle Jahre (2003–2026).
> **Statisch** – kein zeitbewusster Join, gleicher Wert für alle Einsätze im selben Neighborhood.

| Spalte | Typ | Beschreibung | Hinweis |
|---|---|---|---|
| `total_crimes` | int | Gesamtzahl gemeldeter Delikte (alle Kategorien) | Summe über gesamten Beobachtungszeitraum |
| `violent_crime_count` | int | Gewaltdelikte | Assault, Homicide, Robbery, Rape, Kidnapping, Weapons Offenses |
| `property_crime_count` | int | Eigentumsdelikte | Burglary, Theft, Motor Vehicle Theft, Arson, Vandalism |

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

> Berechnet in `03_compute_features.py`. Formel: Zähler / Nenner. **Wertebereich: [0, 1]**

| Spalte | Typ | Formel | Einheit | NaN% |
|---|---|---|---|---|
| `poverty_rate` | float | `poverty_below / poverty_universe_total` | [0, 1] | ~5,7 % |
| `bachelor_rate` | float | `bachelor_degree_count / education_universe_total` | [0, 1] | ~37,3 % (ACS 2009 fehlt) |
| `vacancy_rate` | float | `vacant_housing_units / total_housing_units` | [0, 1] | ~5,7 % |

---

## 7. Abgeleitete Variablen – Crime

> Berechnet in `03_compute_features.py`. Anteil an `total_crimes`. **Wertebereich: [0, 1]**

| Spalte | Typ | Formel | Einheit | NaN% |
|---|---|---|---|---|
| `pct_violent_crime` | float | `violent_crime_count / total_crimes` | [0, 1] | 0,0 % |
| `pct_property_crime` | float | `property_crime_count / total_crimes` | [0, 1] | 0,0 % |

---

## 8. Abgeleitete Variablen – Land Use

> Berechnet in `03_compute_features.py`. **Wertebereich: [0, 1]**

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
| `poverty_rate` | 0,1321 | 0,1126 | 0,0000 | 0,9038 |
| `bachelor_rate` | 0,3350 | 0,3523 | 0,0000 | 0,8000 |
| `vacancy_rate` | 0,1073 | 0,0971 | 0,0000 | 0,2371 |
| `pct_violent_crime` | 0,0896 | 0,0917 | 0,0168 | 0,1356 |
| `pct_property_crime` | 0,1252 | 0,1330 | 0,0323 | 0,2454 |
| `pct_pre1940` | 0,7221 | 0,7689 | 0,0220 | 1,0000 |
| `pct_pre1960` | 0,8466 | 0,8923 | 0,6056 | 1,0000 |
| `pct_residential` | 0,6722 | 0,7952 | 0,0000 | 0,9708 |
| `pct_high_risk_commercial_area` | 0,0745 | 0,0525 | 0,0000 | 0,2218 |

---

## Hinweise für die Modellierung

- **Einheit der Analyse:** Einzelner Feuerwehreinsatz (nicht Neighborhood).
- **Neighborhood-Variablen** (ACS, Crime, Land Use) sind pro Neighborhood konstant – d.h. alle Einsätze im selben Neighborhood / ACS-Jahrgang teilen identische Werte. Bei Regressionen Clustered Standard Errors auf `neighborhood` erwägen.
- **`bachelor_rate` (37 % NaN):** Für Einsätze 2003–2011 nicht verfügbar (ACS 2009 enthält B15003 nicht). Entweder Zeitraum einschränken, Imputation oder alternative Bildungsvariable verwenden.
- **`no_flame_spread`** enthält gemischte Codierungen (`"NA"`, `"NO"`, `"Y"`, `"1"`–`"5"`) und ist vor der Nutzung zu bereinigen.
- **Crime-Aggregate** sind kumulativ (alle Jahre), keine Jahresdurchschnitte. Für zeitvergleichende Analysen müssten sie durch die Beobachtungsjahre geteilt werden.
- **Land Use Snapshot 2020** – historische Nutzungsänderungen vor 2020 sind nicht abgebildet.
