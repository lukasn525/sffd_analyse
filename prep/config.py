"""
Konfiguration der Datenaufbereitung.

HIER STEHT, WAS IN DIE PARQUET-DATEIEN GESCHRIEBEN WIRD: Analysezeitraum,
ausgeschlossene Stadtteile, Praediktoren, Zielgroessen, Klassen und die Zahl der
Folds. Jede dieser Festlegungen bestimmt, welche Spalten die Datensaetze haben
oder wie sie belegt sind.

Was nur beim Rechnen gilt - Suchraeume, Tuning-Budget, Random State, Zahl der
Wiederholungen - steht in modelle/config_modelle.py und beruehrt keine Datei auf
der Platte.

Die Trennlinie ist also nicht "Daten gegen Modelle". N_FOLDS zum Beispiel steht
hier, obwohl es nach Modellierung klingt: Es bestimmt die Spalte `fold` in
beiden Datensaetzen. Die Modellskripte lesen diese Festlegung, sie treffen sie
nicht.

Bezug: docs/03_STAND.md, docs/02_ENTSCHEIDUNGEN.md
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
# Alle Schalter stehen per Default auf False. `python prep/build.py` laeuft
# dann allein aus data/raw und braucht weder Internet noch API-Key.
# Zum Neuladen einer Quelle den jeweiligen Schalter auf True setzen.
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

# Historische SFPD-Daten: Startdatum. Der Kriminalitaetsindex nutzt ein
# rollierendes 12-Monats-Fenster, das im VORMONAT endet; fuer den ersten
# Analysemonat 2015-01 werden daher die Delikte aus 2014 benoetigt.
# Die Lag-Vorlaufmonate (2014) brauchen KEINEN Kriminalitaetsindex - sie liefern
# ausschliesslich Einsatzzaehlungen fuer die Lags und werden nach der
# Lag-Bildung wieder entfernt.
CRIME_HISTORISCH_AB  = "2014-01-01"
CRIME_HISTORISCH_BIS = "2018-01-01"   # exklusiv; ab hier greift e3si-785i

# ==========================================================================
# 3  JOINS  (Schritt: prep/s1_daten.py)
# ==========================================================================
# Publikationsverzoegerung der ACS-5-Jahres-Schaetzungen: Jahrgang y erscheint
# erst ca. Dezember y+1. Ein Modell, das im Jahr y schon den Jahrgang y nutzt,
# waere zum Prognosezeitpunkt nicht implementierbar (Decision Log #11).
ACS_PUBLIKATIONS_LAG = 1

# Laenge des rollierenden Fensters des Kriminalitaetsindex, endend im Vormonat
# -> strikt rueckwaertsgerichtet, kein Leakage (Decision Log #17).
CRIME_FENSTER_MONATE = 12

# Land-Use-Kategorien (Snapshot 2020, einziger verfuegbarer Jahrgang).
HIGH_RISK_COMMERCIAL = {"RETAIL/ENT", "PDR"}
RESIDENTIAL          = {"RESIDENT", "MIXRES"}

# Antwortzeit-Plausibilitaetsfenster in Minuten (Ankunft minus Alarm).
ANTWORTZEIT_MIN, ANTWORTZEIT_MAX = 0, 60

# ==========================================================================
# 4  ANALYSEZEITRAUM UND ANALYSEEINHEITEN
# ==========================================================================
# Hart fixiert, NICHT aus den Daten abgeleitet: Jeder Lauf liefert denselben
# Zeitraum, egal wie weit der letzte DataSF-Download reicht (Decision Log #18).
#
# START 2015-01: frueheste Periode mit vollstaendigen ACS-Merkmalen unter
#   Beruecksichtigung des Publikationsversatzes (Decision Log #5, #11).
# ENDE  2025-12: letztes vollstaendiges Kalenderjahr (Decision Log #12).
START = 201501
ENDE  = 202512

# Lag-Vorlauf (Decision Log #23, 2026-07-27)
# --------------------------------------------------------------------------
# lag_12 fuer Januar 2015 braucht Januar 2014. Frueher fehlte dieser Monat im
# Panel, weshalb das erste Jahr je Stadtteil per dropna verlorenging und die
# Regression erst 2016-01 begann - waehrend die Klassifikation ab 2015-01 lief.
#
# Die Lags brauchen ausschliesslich `anzahl_einsaetze` aus der Vergangenheit,
# KEINE ACS-Merkmale. Der Grund fuer START = 2015 (Akademikerquote) betrifft nur
# die Praediktoren der Zielzeile, nicht deren Vergangenheitswerte. Einsatzzahlen
# liegen bis 2003 vor.
#
# Ablauf: ab START-VORLAUF aggregieren -> Lags bilden -> auf START zuschneiden.
# Die Vorlaufmonate gehen ausschliesslich ueber shift() ein, nie als eigene
# Zeile. Ergebnis: 4.620 statt 4.200 Modellzeilen, beide Datensaetze decken
# denselben Zeitraum ab.
VORLAUF_MONATE = 12

# Stadtteile ohne nennenswerte Wohnbevoelkerung (Decision Log #19). Fuer ein
# bevoelkerungsbezogenes Risikomodell keine sinnvolle Analyseeinheit: jede
# Pro-Kopf-Groesse wird dort beliebig gross, weil der Nenner gegen null geht.
#   Golden Gate Park  45 Einwohner, Kriminalitaetsindex im Median 186
#   Lincoln Park     299 Einwohner
#   McLaren Park     507 Einwohner, zusaetzlich Census-Artefakt (Armutsquote 0,90)
# Median aller uebrigen Stadtteile: 14.435 Einwohner.
PARKGEBIETE = ["Golden Gate Park", "Lincoln Park", "Mclaren Park"]

# Ein Monat gilt als verdaechtig unvollstaendig, wenn seine stadtweite
# Einsatzzahl unter diesem Anteil des Median-Monats liegt. Nur Warnung, kein
# automatischer Filter - massgeblich bleibt ENDE.
VOLLSTAENDIGKEITS_SCHWELLE = 0.5

# ==========================================================================
# 5  MERKMALE DER REGRESSION
# ==========================================================================
# Praediktoren gemaess Expose: soziooekonomisch, kriminalitaetsbezogen, baulich.
#
# log_bevoelkerung statt roher Einwohnerzahl (Exposure, Decision Log #13): ohne
# diese Kontrolle sagt das Modell im Kern die Stadtteilgroesse vorher. Die
# Bevoelkerung ist die Groesse mit dem Vorzeichenwechsel - sie korreliert +0,20
# mit der absoluten Einsatzzahl, aber -0,42 mit Einsaetzen je 1.000 Einwohner.
# (Die frueher hier genannte Armutsquote wechselt das Vorzeichen NICHT: +0,49
# absolut, +0,46 pro Kopf. Korrektur vom 28.07.2026, docs/02_ENTSCHEIDUNGEN.md)
#
# log_kriminalitaetsindex (Decision Log #17/#19): der Index ist ein Quotient,
# also multiplikativ und rechtsschief. Logarithmiert ist er symmetrisch um 0
# (0 = Stadtdurchschnitt, +0,69 = doppelt so hoch). Fuer Baumverfahren ist die
# monotone Transformation wirkungsneutral, deshalb einheitlich fuer ALLE
# Modelle (Fairness-Regel).
PRAEDIKTOREN = [
    "median_haushaltseinkommen", "armutsquote_pct", "akademikerquote_pct",
    "median_miete", "leerstandsquote_pct", "log_bevoelkerung",
    "log_kriminalitaetsindex",
    "anteil_altbau_vor_1940_pct", "anteil_wohngebaeude_pct",
    "anteil_risikogewerbe_pct",
]

# Rohwerte: keine Modellmerkmale, aber im Datensatz mitgefuehrt fuer den
# NegBin-Offset, die Raten-Sensitivitaet und die Deskription in Kap. 5.1.
EXPOSURE_ROH = "gesamtbevoelkerung"
CRIME_ROH    = "kriminalitaetsindex"

# Saison: Kalendermonat als Sinus/Kosinus.
# Der Monat als ZAHL 1-12 waere eine schlechte Kodierung - Dezember und Januar
# haetten den Abstand 11, obwohl sie benachbart sind, und ein linearer
# Koeffizient koennte ein U-foermiges Jahresmuster grundsaetzlich nicht
# abbilden. sin/cos legen die Monate auf ein Zifferblatt.
SAISON = ["monat_sin", "monat_cos"]

# Lags: Vergangenheitswerte der Zielgroesse, je Stadtteil. Sie bleiben im
# Datensatz, sind aber KEIN Modellmerkmal mehr (Decision Log #29).
#
# Grund: Der Verfahrensvergleich laeuft seit dem 28.07.2026 ueber einen
# STADTTEIL-Split - trainiert wird auf 23 Stadtteilen je Fold, getestet auf
# unbekannten. Der Vormonatswert eines Teststadtteils waere dabei technisch
# verfuegbar, denn es ist seine eigene Vergangenheit. Genau dann erklaert aber
# wieder seine Historie das Ergebnis statt seiner Struktur - und die
# Forschungsfrage bliebe unbeantwortet.
#
# Wozu sie dann noch da sind: zur DESKRIPTION der zeitlichen Struktur in
# Kapitel 4 (Autokorrelation Lag 1 innerhalb eines Stadtteils). Es ist KEIN
# zweiter Analysestrang mit Zeitschnitt geplant - der waere ein zweiter
# Validierungsrahmen und verstiesse gegen R1 und R8 (praezisiert 04.08.2026,
# Decision Log #29). Sie formen den Datensatz auch nicht: Das dropna auf die
# Lags entfernt dank Vorlauf null Zeilen.
# Leakage-sicher gebildet: strikt rueckwaertsgerichtet, shift() VOR rolling().
LAGS = ["lag_1", "lag_12", "rolling_mean_3"]

# Ein Merkmalssatz - identisch fuer Ridge, Random Forest und XGBoost.
#
# Bewusst NICHT enthalten: das rohe `jahr` und die Stadtteil-ID. Baumverfahren
# koennen nicht extrapolieren und ordnen unbekannte Werte dem letzten Blatt zu,
# waehrend Ridge linear weiterrechnet - das wuerde den Verfahrensvergleich
# verzerren. Eine Stadtteil-ID waere unter einem Stadtteil-Split ohnehin
# sinnlos: Der Teststadtteil ist im Training nie vorgekommen.
FEATURE_SETS = {
    "S": PRAEDIKTOREN + SAISON,
}

# ==========================================================================
# 6  MERKMALE UND ZIELGROESSEN DER KLASSIFIKATION
# ==========================================================================
# NFIRS-Codes sind hierarchisch; die fuehrende Ziffer bezeichnet die Serie.
# Zusammengefasst wird entlang der fachlichen Bedeutung, nicht nach Haeufigkeit
# (Decision Log #21).
#   100 Brand · 200 Ueberdruck/Explosion ohne Feuer · 300 Rettungsdienst
#   400 Gefahrenlage · 500 Serviceeinsatz · 600 Good Intent · 700 Fehlalarm
#   800 Naturereignis · 900 Sonstige
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

# Zielgroessen der Klassifikation: die ZUSAMMENSETZUNG der Einsatzlast je
# Stadtteil und Monat, nicht die Art des einzelnen Einsatzes (Decision Log #29).
#
# Warum der Wechsel: Innerhalb eines Stadtteil-Monats tragen ALLE Einsaetze
# identische Strukturmerkmale. 350.481 Einzeleinsaetze enthielten nur 4.619
# verschiedene Profile; ein perfektes Modell auf den Strukturmerkmalen haette
# 49,9 % Treffer erreicht gegenueber 48,2 % fuer blosses Raten. Auf Stadtteil-
# ebene ist dieselbe Frage dagegen beantwortbar: Der Fehlalarm-Anteil laesst
# sich fuer einen unbekannten Stadtteil mit R2 0,66 vorhersagen.
ANTEILE = [f"anteil_{k}" for k in
           ["brand", "rettung_ems", "technische_hilfe", "fehlalarm"]]

# Zaehlungen je Gruppe - im Datensatz mitgefuehrt fuer die Deskription und als
# Nenner-Kontrolle, keine Modellmerkmale.
ANZAHLEN = [f"anzahl_{k}" for k in
            ["brand", "rettung_ems", "technische_hilfe", "fehlalarm"]]

# Die Merkmale sind identisch mit denen der Regression - dieselbe Analyseeinheit,
# dieselben Folds, dieselben Verfahren (Fairness-Regel, Gutachten R1).
MERKMALE_STRUKTUR = list(PRAEDIKTOREN)

# Ergebnisvariablen - duerfen NIEMALS Merkmal sein. Sie stehen erst nach dem
# Einsatz fest oder sind eine Folge der Einsatzart; ihre Verwendung waere
# Leakage im engeren Sinn (Decision Log #20).
ERGEBNISVARIABLEN = [
    "schaetzung_sachschaden_usd", "loeschfahrzeuge", "loeschkraefte",
    "rettungsdienst_einheiten", "alarmstufe", "antwortzeit_min",
    "zivile_tote", "zivile_verletzte", "flammenausbreitung_eingedaemmt",
    "ankunft_zeitpunkt",
]


# ==========================================================================
# 7  VALIDIERUNG  (Schritt: prep/s2_datensaetze.py, Teil A)
# ==========================================================================
# STADTTEIL-SPLIT (Decision Log #29). Die Forschungsfrage lautet: Laesst sich
# aus Strukturmerkmalen vorhersagen, wie viele und welche Einsaetze ein
# Stadtteil hat? Diese Frage prueft man, indem man einen Stadtteil komplett
# zurueckhaelt - nicht, indem man die Zeitachse schneidet. Bei einem Zeitschnitt
# steht jeder Stadtteil in Training UND Test; das Modell kennt sein Niveau
# bereits und die Strukturmerkmale muessen nichts erklaeren.
#
#     29 Stadtteile -> 5 Folds (6/6/6/6/5)   6 Stadtteile -> Hold-out
#     jeder Stadtteil ist genau einmal Testfall, nie zugleich Trainingsfall
#
# Zuteilung stratifiziert nach Bevoelkerung: Die Stadtteile werden nach
# Einwohnerzahl sortiert und reihum auf die Gruppen verteilt. Damit deckt jede
# Gruppe die gesamte Groessenspanne ab - sonst laege im Test zufaellig nur
# Downtown oder nur Seacliff. Stratifiziert wird nach einem PRAEDIKTOR, nicht
# nach der Zielgroesse; sonst floesse Testinformation in die Gruppenbildung ein.
# Die Stadtteile werden reihum auf N_FOLDS + 1 Gruppen verteilt. Gruppe 0 ist
# das Hold-out, die Gruppen 1..N_FOLDS sind die Folds. Bei 35 Stadtteilen
# ergibt das 6 Hold-out-Stadtteile und Folds der Groesse 6, 6, 6, 6, 5.
N_FOLDS = 5

# ==========================================================================
# 8  SPALTENNAMEN  englisch -> deutsch
# ==========================================================================
# Die Rohquellen (DataSF, Census) liefern englische Namen; ab dem Ende von
# prep/s1_daten.py heisst im Projekt alles deutsch. Dieses Mapping ist die
# einzige Stelle, an der der Wechsel passiert. Steht hier unten, weil man es
# beim Arbeiten praktisch nie anfasst - im Gegensatz zu allem darueber.
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
