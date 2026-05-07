"""
Mapping englischer -> deutscher Spaltennamen fuer alle finalen Tabellen.
Wird in 03_compute_features.py (Parquet-Export) und export_sample.py (CSV-Export) importiert.
"""

spalten_deutsch = {
    # ── SFFD Einsatzfelder (Quelldaten) ──────────────────────────────────────
    "incident_number":               "einsatz_nummer",
    "incident_date":                 "einsatz_datum",
    "alarm_dttm":                    "alarm_zeitpunkt",
    "arrival_dttm":                  "ankunft_zeitpunkt",
    "neighborhood_district":         "stadtteil_bezirk",
    "battalion":                     "bataillon",
    "primary_situation":             "einsatzart",
    "suppression_units":             "loeschfahrzeuge",
    "suppression_personnel":         "loeschkraefte",
    "ems_units":                     "rettungsdienst_einheiten",
    "number_of_alarms":              "alarmstufe",
    "civilian_fatalities":           "zivile_tote",
    "civilian_injuries":             "zivile_verletzte",
    "no_flame_spread":               "flammenausbreitung_eingedaemmt",
    "estimated_property_loss":       "schaetzung_sachschaden_usd",
    # ── Abgeleitete Einsatzfelder (Zeitvariablen) ─────────────────────────────
    "response_time_min":             "antwortzeit_min",
    "year":                          "jahr",
    "month":                         "monat",
    "hour":                          "stunde",
    "weekday":                       "wochentag",
    "is_weekend":                    "ist_wochenende",
    "is_night":                      "ist_nacht",
    "neighborhood":                  "stadtteil",
    "acs_year":                      "acs_jahr",
    # ── ACS Soziooekonomie (Rohdaten) ─────────────────────────────────────────
    "total_population":              "gesamtbevoelkerung",
    "median_household_income":       "median_haushaltseinkommen",
    "median_gross_rent":             "median_miete",
    "poverty_below":                 "armutsbevoelkerung",
    "poverty_universe_total":        "armuts_grundgesamtheit",
    "bachelor_degree_count":         "akademiker_anzahl",
    "education_universe_total":      "bildungs_grundgesamtheit",
    "vacant_housing_units":          "leerstehende_wohneinheiten",
    "total_housing_units":           "gesamtzahl_wohnungen",
    # ── SFPD Kriminalitaet (Rohdaten) ─────────────────────────────────────────
    "total_crimes":                  "gesamtzahl_straftaten",
    "violent_crime_count":           "gewaltdelikte_anzahl",
    "property_crime_count":          "eigentumsdelikte_anzahl",
    # ── Land Use (Rohdaten) ───────────────────────────────────────────────────
    "parcel_count":                  "parzellen_anzahl",
    "yrbuilt_count":                 "parzellen_mit_baujahr",
    "pre1940_count":                 "parzellen_vor_1940",
    "pre1960_count":                 "parzellen_vor_1960",
    "total_resunits":                "gesamtzahl_wohneinheiten",
    "residential_count":             "wohnparzellen_anzahl",
    "total_area_sqft":               "gesamtflaeche_sqft",
    "high_risk_commercial_area_sqft": "risikogewerbeflaeche_sqft",
    # ── Abgeleitete Variablen – ACS ───────────────────────────────────────────
    "poverty_rate":                  "armutsquote_pct",
    "bachelor_rate":                 "akademikerquote_pct",
    "vacancy_rate":                  "leerstandsquote_pct",
    # ── Abgeleitete Variablen – Crime ─────────────────────────────────────────
    "pct_violent_crime":             "anteil_gewaltdelikte_pct",
    "pct_property_crime":            "anteil_eigentumsdelikte_pct",
    # ── Abgeleitete Variablen – Land Use ──────────────────────────────────────
    "pct_pre1940":                   "anteil_altbau_vor_1940_pct",
    "pct_pre1960":                   "anteil_altbau_vor_1960_pct",
    "pct_residential":               "anteil_wohngebaeude_pct",
    "pct_high_risk_commercial_area": "anteil_risikogewerbe_pct",
    # ── Aggregierte Zielvariablen (fuer spaetere Neighborhood-Tabellen) ────────
    "incident_count":                "anzahl_einsaetze",
    "mean_response_time":            "mittlere_antwortzeit_min",
    "median_response_time":          "median_antwortzeit_min",
    "p90_response_time":             "p90_antwortzeit_min",
    "pct_over_5min":                 "anteil_ueber_5min_pct",
    "fire_rate":                     "feuerrate_pct",
    "incidents_per_1k_pop":          "einsaetze_pro_1000_ew",
    "crimes_per_1k_pop":             "kriminalitaetsrate_pro_1000_ew",
}
