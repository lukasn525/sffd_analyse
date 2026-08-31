"""
Konfiguration der Datenaufbereitung.

Ausgang: Konstanten fuer prep/, vorpruefung/ und modelle/.

- Hier steht, was in die Parquet-Dateien GESCHRIEBEN wird. Was nur beim
  RECHNEN gilt, steht in modelle/config_modelle.py.
- N_FOLDS steht deshalb hier: es belegt die Spalte `fold`.

Ausfuehrlich: docs/08_FUNKTIONSDOKUMENTATION.md
"""
from pathlib import Path

# ==========================================================================
# 1  PFADE
# ==========================================================================
ROOT          = Path(__file__).resolve().parent.parent
RAW_DIR       = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
RESULTS_DIR   = ROOT / "results"

# Zwischenstand: ein Einsatz je Zeile, alle Merkmale angejoint.
PFAD_EINSAETZE = PROCESSED_DIR / "einsaetze.parquet"

# Die beiden FINALEN Datensaetze. Nur diese beiden Dateien lesen die Modelle.
PFAD_REGRESSION     = PROCESSED_DIR / "regression.parquet"
PFAD_KLASSIFIKATION = PROCESSED_DIR / "klassifikation.parquet"

# ==========================================================================
# 2  DOWNLOADS  (Schritt: prep/s1_daten.py)
# ==========================================================================
# Default False: build.py laeuft aus data/raw, ohne Internet und API-Key.
DOWNLOAD_SFFD             = False
DOWNLOAD_CROSSWALK        = False
DOWNLOAD_ACS              = False
DOWNLOAD_CRIME            = False   # SFPD ab 2018 (e3si-785i), mit Datumsspalte
DOWNLOAD_CRIME_HISTORISCH = False   # SFPD 2014-2017 (tmnf-yvry), fuer den Index
DOWNLOAD_LAND_USE         = False
DOWNLOAD_NEIGHBORHOODS    = False

CENSUS_API_KEY   = "f5cb8b553da8a01e351b3804e56e7fe664e12c98"
DATASF_APP_TOKEN = None   # optional, nur fuer hoehere Rate-Limits

# ACS-Jahrgaenge. Frueher doppelt gepflegt - jetzt nur noch hier.
ACS_YEARS = [2009, 2014, 2019, 2021, 2023]

ACS_VARIABLES = {
    "B19013_001E": "median_household_income",
    "B17001_001E": "poverty_universe_total",
    "B17001_002E": "poverty_below",
    "B01003_001E": "total_population",
    "B15003_022E": "bachelor_degree_count",
    "B15003_001E": "education_universe_total",
    "B25064_001E": "median_gross_rent",
    "B25002_003E": "vacant_housing_units",
    "B25002_001E": "total_housing_units",
}

# Das Indexfenster endet im Vormonat - fuer 2015-01 braucht es 2014.
CRIME_HISTORISCH_AB  = "2014-01-01"
CRIME_HISTORISCH_BIS = "2018-01-01"   # exklusiv; ab hier greift e3si-785i

# ==========================================================================
# 3  JOINS  (Schritt: prep/s1_daten.py)
# ==========================================================================
# ACS-Jahrgang y erscheint erst um Dezember y+1 (Decision Log #11).
ACS_PUBLIKATIONS_LAG = 1

# Indexfenster, endend im Vormonat - strikt rueckwaerts, kein Leakage (#17).
CRIME_FENSTER_MONATE = 12

# Land-Use-Kategorien (Snapshot 2020, einziger verfuegbarer Jahrgang).
HIGH_RISK_COMMERCIAL = {"RETAIL/ENT", "PDR"}
RESIDENTIAL          = {"RESIDENT", "MIXRES"}

# Antwortzeit-Plausibilitaetsfenster in Minuten (Ankunft minus Alarm).
ANTWORTZEIT_MIN, ANTWORTZEIT_MAX = 0, 60

# ==========================================================================
# 4  ANALYSEZEITRAUM UND ANALYSEEINHEITEN
# ==========================================================================
# Hart fixiert, nicht aus den Daten abgeleitet (#18).
#   START  frueheste Periode mit vollstaendigen ACS-Merkmalen (#5, #11)
#   ENDE   letztes vollstaendiges Kalenderjahr (#12)
START = 201501
ENDE  = 202512

# Lag-Vorlauf (#23): ab START-VORLAUF aggregieren, Lags bilden, auf START
# zuschneiden. Ohne ihn beginnt die Regression erst 2016-01. 4.752 statt 4.320.
VORLAUF_MONATE = 12

# Parks ohne Wohnbevoelkerung (#19): 45 bis 507 Einwohner gegen 14.435 im
# Median. Jede Pro-Kopf-Groesse wird dort beliebig gross.
PARKGEBIETE = ["Golden Gate Park", "Lincoln Park", "Mclaren Park"]

# Erwarteter Analysezuschnitt. Ausgeschlossen sind die drei Parkgebiete ohne
# Wohnbevoelkerung sowie Lakeshore und Treasure Island, fuer die das
# Parzellenverzeichnis kein einziges Baujahr fuehrt - der Altbauanteil ist dort
# nicht bildbar. Bleiben 36 der 41 Analysis Neighborhoods.
N_STADTTEILE_ERWARTET = 36

# Plausibilitaetsspanne der stadtweiten Wohnbevoelkerung ueber die enthaltenen
# Stadtteile. San Francisco liegt im Analysezeitraum bei rund 810.000 bis
# 875.000 Einwohnern; die Spanne faengt ab, wenn ein Verbund Tracts verwirft und
# die Exposition dadurch stillschweigend zu klein wird.
BEV_PLAUSIBEL = (750_000, 950_000)

# Warnschwelle fuer unvollstaendige Monate. Kein Filter - massgeblich ist ENDE.
VOLLSTAENDIGKEITS_SCHWELLE = 0.5

# ==========================================================================
# 5  MERKMALE DER REGRESSION
# ==========================================================================
# Praediktoren gemaess Expose: soziooekonomisch, kriminalitaetsbezogen, baulich.
# log_bevoelkerung als Groessenkontrolle (#13), log_kriminalitaetsindex weil der
# Index multiplikativ ist (#17/#19). Beide Logarithmen gelten fuer ALLE Modelle
# gleich (Fairness-Regel).
PRAEDIKTOREN = [
    "median_haushaltseinkommen", "armutsquote_pct", "akademikerquote_pct",
    "median_miete", "leerstandsquote_pct", "log_bevoelkerung",
    "log_kriminalitaetsindex",
    "anteil_altbau_vor_1940_pct", "anteil_wohngebaeude_pct",
    "anteil_risikogewerbe_pct",
]

# Rohwerte: keine Merkmale, aber Offset des Poisson-GLM und Deskription 4.1.
EXPOSURE_ROH = "gesamtbevoelkerung"
CRIME_ROH    = "kriminalitaetsindex"

# Saison als sin/cos - der Monat als Zahl gaebe Dezember und Januar Abstand 11.
SAISON = ["monat_sin", "monat_cos"]

# Lags bleiben im Datensatz, sind aber KEIN Modellmerkmal (#29): sonst
# erklaerte die Historie das Ergebnis statt der Struktur. Nur fuer die
# Deskription in Kapitel 4. Strikt rueckwaerts, shift() VOR rolling().
LAGS = ["lag_1", "lag_12", "rolling_mean_3"]

# Ein Merkmalssatz fuer alle drei Verfahren. Ohne rohes `jahr` und
# Stadtteil-ID: Baeume koennen nicht extrapolieren, Ridge schon - das
# verzerrte den Vergleich.
FEATURE_SETS = {
    "S": PRAEDIKTOREN + SAISON,
}

# ==========================================================================
# 6  MERKMALE UND ZIELGROESSEN DER KLASSIFIKATION
# ==========================================================================
# NFIRS: die fuehrende Ziffer bezeichnet die Serie. Zusammengefasst nach
# fachlicher Bedeutung, nicht nach Haeufigkeit (#21).
#   100 Brand · 300 Rettungsdienst · 600/700 Fehlalarm
#   200/400/500/800/900 technische Hilfe und Gefahrenlagen
NFIRS_GRUPPEN = {
    "1": "Brand",
    "3": "Rettung/EMS",
    "2": "Technische Hilfe/Gefahr",
    "4": "Technische Hilfe/Gefahr",
    "5": "Technische Hilfe/Gefahr",
    "8": "Technische Hilfe/Gefahr",
    "9": "Technische Hilfe/Gefahr",
    "6": "Fehlalarm/Good Intent",
    "7": "Fehlalarm/Good Intent",
}
KLASSEN    = ["Brand", "Rettung/EMS", "Technische Hilfe/Gefahr", "Fehlalarm/Good Intent"]
RESTKLASSE = "Technische Hilfe/Gefahr"

# Zielgroesse ist die ZUSAMMENSETZUNG der Einsatzlast je Stadtteil-Monat (#29).
# Auf Einzeleinsatz-Ebene waeren es nur 4.751 Profile fuer 357.553 Einsaetze.
ANTEILE = [f"anteil_{k}" for k in
           ["brand", "rettung_ems", "technische_hilfe", "fehlalarm"]]

# Zaehlungen je Gruppe - im Datensatz mitgefuehrt fuer die Deskription und als
# Nenner-Kontrolle, keine Modellmerkmale.
ANZAHLEN = [f"anzahl_{k}" for k in
            ["brand", "rettung_ems", "technische_hilfe", "fehlalarm"]]

# Die Merkmale sind identisch mit denen der Regression - dieselbe Analyseeinheit,
# dieselben Folds, dieselben Verfahren (Fairness-Regel, Gutachten R1).
MERKMALE_STRUKTUR = list(PRAEDIKTOREN)

# Ergebnisvariablen - NIEMALS Merkmal. Stehen erst nach dem Einsatz fest (#20).
ERGEBNISVARIABLEN = [
    "schaetzung_sachschaden_usd", "loeschfahrzeuge", "loeschkraefte",
    "rettungsdienst_einheiten", "alarmstufe", "antwortzeit_min",
    "zivile_tote", "zivile_verletzte", "flammenausbreitung_eingedaemmt",
    "ankunft_zeitpunkt",
]


# ==========================================================================
# 7  VALIDIERUNG  (Schritt: prep/s2_datensaetze.py, Teil A)
# ==========================================================================
# STADTTEIL-SPLIT (#29): ein Stadtteil wird komplett zurueckgehalten. Bei einem
# Zeitschnitt stuende jeder Stadtteil in Training UND Test und das Modell
# kennte sein Niveau bereits.
#     30 Stadtteile -> 5 Folds (6/6/6/6/6)   6 -> Hold-out
# Stratifiziert nach einem PRAEDIKTOR, nicht nach der Zielgroesse.
N_FOLDS = 5

# ==========================================================================
# 8  SPALTENNAMEN  englisch -> deutsch
# ==========================================================================
# Englisch -> deutsch. Einzige Stelle des Wechsels, deshalb ganz unten.
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
    # ── SFPD Kriminalitaet ────────────────────────────────────────────────────
    # Relativer Index je Stadtteil x Monat (Location Quotient gegen den
    # Stadtdurchschnitt desselben Monats, rollierendes 12-Monats-Fenster endend
    # im Vormonat). Ersetzt die frueheren statischen Anteile.
    "crime_index":                   "kriminalitaetsindex",
    "crime_rate_raw":                "kriminalitaetsrate_pro_1000_ew_roh",
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
    # ── Abgeleitete Variablen – Land Use ──────────────────────────────────────
    "pct_pre1940":                   "anteil_altbau_vor_1940_pct",
    "pct_pre1960":                   "anteil_altbau_vor_1960_pct",
    "pct_residential":               "anteil_wohngebaeude_pct",
    "pct_high_risk_commercial_area": "anteil_risikogewerbe_pct",
}
