"""
Zentrale Konfiguration der gesamten Datenaufbereitung.

DIESE DATEI IST DIE EINZIGE WAHRHEIT. Jede Festlegung - Zeitraum, ausgeschlossene
Stadtteile, Praediktoren, Merkmalssaetze, Zeitschnitte, Suchraeume - steht hier
und nirgendwo sonst. Wer wissen will, ab wann die Analyse laeuft oder welche
Merkmale ins Modell gehen, oeffnet ausschliesslich diese Datei.

Bezug: CLAUDE.md (Decision Log), docs/UMBAU_PREPROCESSING.md
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
# diese Kontrolle sagt das Modell im Kern die Stadtteilgroesse vorher -
# armutsquote_pct korreliert +0,20 mit der absoluten Einsatzzahl, aber -0,13 mit
# Einsaetzen je 1.000 Einwohner. Das Vorzeichen des zentralen Struktur-Befundes
# haengt an dieser Entscheidung.
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

# Lags: Vergangenheitswerte der Zielgroesse, je Stadtteil.
# Die Lag-1-Autokorrelation betraegt 0,96. Ohne diese Merkmale schlaegt keines
# der Verfahren die naive Vormonats-Baseline (Decision Log #8).
# Leakage-sicher: strikt rueckwaertsgerichtet, shift() VOR rolling().
LAGS = ["lag_1", "lag_12", "rolling_mean_3"]

# Die beiden Merkmalssaetze - identisch fuer Ridge, Random Forest und XGBoost.
#   S    Strukturmerkmale + Saison  -> Unterfrage 1 (Erklaerungsbeitrag)
#   S+L  zusaetzlich Lags           -> realistische Prognoseaufgabe
# Zwei Saetze, weil der Vormonatswert sonst fast alles erklaert und Armut oder
# Altbauanteil in der Feature Importance verschwinden - nicht weil sie
# irrelevant waeren, sondern weil ihre Wirkung im Vormonatswert steckt.
#
# Bewusst NICHT enthalten: das rohe `jahr`. Baumverfahren koennen nicht
# extrapolieren und ordnen unbekannte Jahreswerte dem letzten Blatt zu, waehrend
# Ridge linear weiterrechnet - das wuerde genau den Verfahrensvergleich
# verzerren, um den es geht. Das Zeitniveau tragen die Lags.
FEATURE_SETS = {
    "S":   PRAEDIKTOREN + SAISON,
    "S+L": PRAEDIKTOREN + SAISON + LAGS,
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

# Block A: Stadtteilstruktur - exakt dieselben Merkmale wie in der Regression.
MERKMALE_STRUKTUR = list(PRAEDIKTOREN)

# Block B: Zeitpunkt des Alarms. Zyklisch kodiert, weil der Zusammenhang
# periodisch und nicht monoton ist: Der Brandanteil schwankt ueber den Tag
# zwischen 8,5 % und 20,5 %, die lineare Korrelation mit `stunde` betraegt aber
# nur -0,006.
MERKMALE_ZEIT = ["stunde_sin", "stunde_cos", "monat_sin", "monat_cos",
                 "ist_nacht", "ist_wochenende"]

# Kategorial: One-Hot erst im ColumnTransformer, einheitlich fuer alle drei
# Verfahren (auch XGBoost), damit die Designmatrix identisch ist und
# Unterschiede rein algorithmisch bleiben.
MERKMALE_KATEGORIAL = ["wochentag"]

# Optionaler Robustheitslauf: Ortsidentitaet. NICHT im Hauptmodell, analog zur
# Entscheidung gegen die Stadtteil-ID in der Regression.
MERKMALE_ORT  = ["bataillon"]
MIT_BATAILLON = False

# Ergebnisvariablen - duerfen NIEMALS Merkmal sein. Sie stehen erst nach dem
# Einsatz fest oder sind eine Folge der Einsatzart; ihre Verwendung waere
# Leakage im engeren Sinn (Decision Log #20).
ERGEBNISVARIABLEN = [
    "schaetzung_sachschaden_usd", "loeschfahrzeuge", "loeschkraefte",
    "rettungsdienst_einheiten", "alarmstufe", "antwortzeit_min",
    "zivile_tote", "zivile_verletzte", "flammenausbreitung_eingedaemmt",
    "ankunft_zeitpunkt",
]

def merkmalslisten(mit_ort: bool = MIT_BATAILLON) -> dict[str, list[str]]:
    """Merkmalssaetze der geplanten Laeufe (docs/KLASSIFIKATION_DESIGN.md).

      A+B  Stadtteilstruktur + Zeitpunkt -> Hauptmodell
      B    nur Zeitpunkt                 -> zeigt den Beitrag der Struktur
      A    nur Stadtteilstruktur         -> Gegenprobe
    """
    saetze = {
        "A+B": MERKMALE_STRUKTUR + MERKMALE_ZEIT + MERKMALE_KATEGORIAL,
        "B":   MERKMALE_ZEIT + MERKMALE_KATEGORIAL,
        "A":   list(MERKMALE_STRUKTUR),
    }
    if mit_ort:
        saetze["A+B+Ort"] = saetze["A+B"] + MERKMALE_ORT
    return saetze


# ==========================================================================
# 7  VALIDIERUNG  (Schritt: prep/cv.py)
# ==========================================================================
# Blockiertes Forward Chaining ueber globale Zeitschnitte; alle Stadtteile
# teilen dieselbe Trennlinie. Kein Gap zwischen Train und Test noetig, weil
# saemtliche Lag-Features strikt rueckwaertsgerichtet sind.
N_FOLDS       = 3    # Folds auf den Entwicklungsdaten
N_TEST_MONATE = 12   # Laenge eines Testfensters
N_VAL_MONATE  = 12   # inneres Validierungsfenster fuer die Suche
N_HOLDOUT     = 12   # End-Hold-out, bei Modellwahl und Tuning nie beruehrt

# ==========================================================================
# 8  HYPERPARAMETER-SUCHE
# ==========================================================================
# Nur die SUCHRAEUME stehen hier - die Suche selbst laeuft im jeweiligen
# Modellskript unter modelle/. Ein separates Tuning-Skript wuerde die besten
# Parameter in eine Datei auslagern, die veralten kann, ohne dass es auffaellt.
#
# Gleiches Budget fuer alle Verfahren -> fairer Vergleich (Bergstra & Bengio
# 2012 zur Randomized Search, Probst et al. 2019 zu den RF-Raeumen).
TUNING_BUDGET = 50
RANDOM_STATE  = 42

SUCHRAEUME = {
    "ridge": {
        "alpha": ("loguniform", 1e-3, 1e3),
    },
    "random_forest": {
        "n_estimators":     ("int", 200, 1000),
        "max_depth":        ("choice", [None, 8, 12, 16, 24]),
        "min_samples_leaf": ("int", 1, 20),
        "max_features":     ("choice", ["sqrt", "log2", 0.3, 0.5, 1.0]),
    },
    "xgboost": {
        "n_estimators":     ("int", 200, 1000),
        "learning_rate":    ("loguniform", 0.01, 0.3),
        "max_depth":        ("int", 3, 10),
        "subsample":        ("uniform", 0.6, 1.0),
        "colsample_bytree": ("uniform", 0.6, 1.0),
        "reg_lambda":       ("loguniform", 1e-2, 1e2),
    },
    # NegBin-Baseline: kein Tuning, interpretierbare Referenz.
}


# ==========================================================================
# 9  SPALTENNAMEN  englisch -> deutsch
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
