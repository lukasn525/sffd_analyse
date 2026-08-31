"""Foliensatz 01 - prep/ (Datenaufbereitung)."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from renderer import (Deck, absatz, block, einaus, fluss, kasten, kpi, punkte,
                      reihe, schreibe, stapel, tabelle)

D = Deck(
    "prep",
    "prep/ — von der Rohquelle zum Analysedatensatz",
    "Vier Dateien führen sieben offene Datenquellen zu zwei Panels zusammen. "
    "Alles, was danach kommt, liest nur noch — es rechnet nichts neu.",
    [("Bachelorarbeit", "SFFD-Einsatzprognose"),
     ("Phase", "CRISP-DM 2 und 3"),
     ("Ergebnis", "regression.parquet · klassifikation.parquet")])

P = "prep"

# ------------------------------------------------------------------ Ordner
D.folie(P, "Was dieser Ordner leistet",
        "Er beantwortet eine einzige Frage: Welche Zahl darf zu welchem "
        "Zeitpunkt im Modell stehen?",
        stapel(
            fluss([
                {"nr": "Quellen", "titel": "7 offene Datensätze",
                 "text": "SFFD-Einsätze, ACS, SFPD (2 Quellen), Land Use, "
                         "Stadtteilgeometrien, Crosswalk"},
                {"nr": "Schritt 1", "titel": "s1_daten.py",
                 "text": "Laden, bereinigen, räumlich und zeitlich verknüpfen"},
                {"nr": "Zwischenstand", "titel": "einsaetze.parquet",
                 "text": "eine Zeile je Einsatz, mit allen Stadtteilmerkmalen"},
                {"nr": "Schritt 2", "titel": "s2_datensaetze.py", "akt": True,
                 "text": "verdichten auf Stadtteil × Monat, Ziele und Folds"},
            ]),
            reihe(
                kasten("Die Kernidee", punkte([
                    "Modelliert wird nicht der einzelne Einsatz, sondern der "
                    "<b>Stadtteil-Monat</b> — 36 Stadtteile × 132 Monate.",
                    "Jedes Merkmal muss zum Prognosezeitpunkt bereits "
                    "veröffentlicht gewesen sein.",
                    "Die Aufteilung in Trainings- und Testgruppen wird hier "
                    "festgeschrieben, nicht erst im Modellskript.",
                ])),
                block("Warum das die heikelste Stufe ist", punkte([
                    "Ein Fehler hier ist in keinem Ergebnis mehr sichtbar — er "
                    "macht die Zahlen nur <b>besser</b>, als sie sein dürften.",
                    "Deshalb liegen alle drei Leakage-Sperren in diesem Ordner "
                    "und nicht in den Modellen.",
                ])),
                gewichte=[1.15, 1])),
        label="Ordner")

D.folie(P, "Die vier Dateien", None,
        stapel(
            tabelle(["Datei", "Rolle", "Was sie erzeugt", "Zeilen"], [
                [("config.py", "fn"), "Alle Festlegungen an einem Ort",
                 "nichts — sie wird nur gelesen", ("252", "z")],
                [("s1_daten.py", "fn"), "Laden und Verknüpfen",
                 "data/processed/einsaetze.parquet", ("674", "z")],
                [("s2_datensaetze.py", "fn"), "Verdichten und Zuschneiden",
                 "regression.parquet · klassifikation.parquet", ("459", "z")],
                [("build.py", "fn"), "Der eine Startbefehl",
                 "Konsolenausgabe und Prüfübersicht", ("106", "z")],
            ]),
            kasten("Aufrufreihenfolge", absatz(
                "<code>python prep/build.py</code> fährt s1 und dann s2. Die "
                "Reihenfolge ist zwingend, weil s2 liest, was s1 schreibt. "
                "Einzeln startbar sind beide trotzdem — das ist der Zweck der "
                "<code>run_*</code>-Funktionen.")),
            abstand=20),
        label="Ordner")

D.folie(P, "Drei Sperren gegen Informationsvorgriff",
        "Jede sitzt an genau einer Stelle im Code. Wer sie sucht, findet sie "
        "an der Funktion, die daneben steht.",
        reihe(
            block("1 · Publikationsverzug", stapel(
                absatz("Ein Einsatz aus 2023 bekommt <b>nicht</b> den "
                       "ACS-Jahrgang 2023 — der erschien erst Ende 2024."),
                punkte(["gelöst in <code>acs_snapshot()</code>",
                        "Konstante <code>ACS_PUBLIKATIONS_LAG = 1</code>"], eng=True),
                abstand=10)),
            block("2 · Rückwärtsfenster", stapel(
                absatz("Der Kriminalitätsindex eines Monats zählt nur Delikte "
                       "der zwölf Monate <b>bis zum Vormonat</b>."),
                punkte(["gelöst in <code>kriminalitaetsindex()</code>",
                        "Konstante <code>CRIME_FENSTER_MONATE = 12</code>"], eng=True),
                abstand=10)),
            block("3 · Stadtteil-Trennung", stapel(
                absatz("Kein Stadtteil steht je zugleich im Training und im "
                       "Test. Getestet wird auf <b>unbekannten Stadtteilen</b>."),
                punkte(["gelöst in <code>ergaenze_aufteilung()</code>",
                        "Spalten <code>fold</code> und <code>ist_holdout</code>"],
                       eng=True),
                abstand=10))),
        label="Ordner")

# ------------------------------------------------------------------ config
D.kapitel("config.py",
          "Alle Festlegungen an einem Ort. Enthält keine einzige Funktion — "
          "und genau das ist die Absicht.",
          [("252", "Zeilen"), ("~40", "Konstanten"), ("0", "Funktionen")])

D.folie("prep/config.py", "Was hier steht — und warum nichts davon rechnet",
        "Die Datei ist ein Vertrag: Wer eine Zahl ändern will, ändert sie hier "
        "und nirgendwo sonst.",
        stapel(
            tabelle(["Gruppe", "Beispiele", "Wirkung"], [
                ["Pfade", ("PFAD_REGRESSION, RAW_DIR", "fn"),
                 "wo Roh- und Ergebnisdateien liegen"],
                ["Quellen", ("ACS_YEARS, DOWNLOAD_*", "fn"),
                 "welche Jahrgänge geladen werden; Schalter gegen "
                 "unnötige API-Aufrufe"],
                ["Zeitfenster", ("START = 201501, ENDE = 202512", "fn"),
                 "der Analysezeitraum: 132 Monate"],
                ["Sperren", ("ACS_PUBLIKATIONS_LAG, CRIME_FENSTER_MONATE", "fn"),
                 "die beiden zeitlichen Leakage-Sperren"],
                ["Merkmale", ("PRAEDIKTOREN, SAISON, LAGS", "fn"),
                 "die zehn Prädiktoren; Saison als sin/cos; Lags bleiben "
                 "deskriptiv"],
                ["Zielgrößen", ("NFIRS_GRUPPEN, KLASSEN", "fn"),
                 "die Abbildung der NFIRS-Codes auf vier Klassen"],
                ["Validierung", ("N_FOLDS = 5", "fn"),
                 "fünf Folds plus ein Hold-out"],
            ], eng=True),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Warum steht <code>WIEDERHOLUNGEN</code> nicht hier?“</b> — "
                "Weil <code>config.py</code> festlegt, was in die Dateien "
                "<i>geschrieben</i> wird. Die Zahl der Wiederholungen betrifft "
                "nur das Rechnen und steht deshalb in "
                "<code>modelle/config_modelle.py</code>. Dass die Vorprüfung "
                "sie von dort holt, ist eine bekannte strukturelle Schwäche."),
                "frage"),
            abstand=16),
        label="Datei")

# ------------------------------------------------------------------ s1
D.kapitel("s1_daten.py",
          "Laden und Verknüpfen. Aus sieben Quellen wird eine Tabelle mit "
          "einer Zeile je Einsatz.",
          [("674", "Zeilen"), ("14", "Funktionen"),
           ("2", "Einstiegspunkte")])

D.folie("prep/s1_daten.py", "Zwei Phasen, zwei Einstiegspunkte",
        None,
        stapel(
            reihe(
                block("Phase A — Herunterladen", stapel(
                    absatz("<code>run_download()</code> holt die Rohdaten in "
                           "<code>data/raw/</code>. Über die "
                           "<code>DOWNLOAD_*</code>-Schalter abschaltbar, damit "
                           "ein Wiederholungslauf nicht erneut über die APIs geht."),
                    punkte(["<code>_get()</code> — HTTP mit Wiederholversuchen",
                            "<code>lade_datasf()</code> — die DataSF-Quellen",
                            "<code>lade_acs()</code> — die Census-Jahrgänge"],
                           eng=True),
                    abstand=10)),
                block("Phase B — Zusammenführen", stapel(
                    absatz("<code>run_join()</code> baut daraus "
                           "<code>einsaetze.parquet</code>. Hier liegen die "
                           "inhaltlichen Entscheidungen."),
                    punkte(["Bereinigen: <code>prepare_sffd()</code>",
                            "Sozialstruktur: <code>acs_*</code>, <code>join_acs()</code>",
                            "Kriminalität: <code>crime_monatlich()</code>, "
                            "<code>kriminalitaetsindex()</code>",
                            "Bausubstanz: <code>land_use_je_neighborhood()</code>"],
                           eng=True),
                    abstand=10))),
            kasten("Die räumliche Klammer", absatz(
                "Drei Quellen liefern Koordinaten statt Stadtteilnamen. Sie "
                "werden über <b>dieselben</b> Stadtteilpolygone zugeordnet "
                "(<code>neighborhoods_gdf()</code>) — sonst bezögen sich "
                "Kriminalitäts- und Baumerkmale auf leicht verschiedene Flächen.")),
            abstand=18),
        label="Datei")

D.folie("prep/s1_daten.py", "Die drei Ladefunktionen",
        "Reine Beschaffung. Sie treffen keine inhaltliche Entscheidung.",
        stapel(
            reihe(
                block("_get(url, params)", stapel(
                    einaus("URL und Parameter", "Antwort der Anfrage"),
                    punkte(["kapselt Wiederholversuche bei Zeitüberschreitung",
                            "damit ein Netzaussetzer keinen Ladelauf beendet"],
                           eng=True), abstand=10)),
                block("lade_datasf(name, limit)", stapel(
                    einaus("Datensatz-ID der Socrata-API, Zielpfad",
                           "Parquet-Datei in data/raw, Zeilenzahl"),
                    punkte(["paginiert, weil die API je Anfrage deckelt",
                            "setzt die Spaltentypen ausdrücklich — sonst rät "
                            "pandas bei GEOIDs auf Ganzzahl und die führende "
                            "Null verschwindet"], eng=True), abstand=10)),
                block("lade_acs(year)", stapel(
                    einaus("Jahrgang und Variablenliste",
                           "eine CSV je Jahrgang"),
                    punkte(["eigene Funktion, weil die Census-API ein anderes "
                            "Format liefert: Kopfzeile plus verschachtelte "
                            "Datenliste"], eng=True), abstand=10))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Warum stehen die Typen im Code statt automatisch erkannt?“</b> "
                "— Weil ein Census-Tract-Schlüssel wie <code>06075010100</code> "
                "als Zahl gelesen die führende Null verliert und dann auf keinen "
                "Crosswalk-Eintrag mehr passt. Der Join liefe still ins Leere."),
                "frage"),
            abstand=16), label="Funktionen")

D.folie("prep/s1_daten.py", "prepare_sffd(df)",
        "Bereinigt die rohe Einsatztabelle des SFFD.",
        stapel(
            einaus("die rohe SFFD-Tabelle",
                   "dieselbe Tabelle, bereinigt und um Zeitspalten ergänzt"),
            reihe(
                block("Was passiert", punkte([
                    "Doppelte Meldungen desselben Einsatzes werden über die "
                    "Einsatznummer entfernt",
                    "Die Antwortzeit wird aus Melde- und Ankunftszeitstempel "
                    "berechnet",
                    "Zeitmerkmale werden abgeleitet: Jahr, Monat, "
                    "<code>jahr_monat</code> als Schlüssel",
                    "Stadtteilnamen werden vereinheitlicht",
                ])),
                kasten("Worauf es ankommt", punkte([
                    "Die Zahl der entfernten Dubletten wird <b>ausgegeben</b>, "
                    "nicht stillschweigend geschluckt.",
                    "Ohne den Dedup zählte derselbe Einsatz zweimal — und die "
                    "Zielgröße wäre systematisch zu hoch.",
                ]))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Woran erkennen Sie eine Dublette?“</b> — An der "
                "Einsatznummer. Ein Einsatz kann mehrfach gemeldet werden, "
                "behält aber dieselbe Nummer. Verschiedene Fahrzeuge zum selben "
                "Einsatz sind <i>keine</i> Dubletten in dieser Tabelle, weil "
                "sie bereits auf Einsatzebene geliefert wird."), "frage"),
            abstand=15))

D.folie("prep/s1_daten.py", "acs_je_neighborhood(acs, crosswalk)",
        "Hebt die Sozialdaten von der Tract- auf die Stadtteilebene.",
        stapel(
            einaus("ACS-Tabelle auf Census-Tract-Ebene und der Crosswalk",
                   "eine Zeile je Stadtteil und Jahrgang"),
            reihe(
                block("Das Problem", absatz(
                    "Der American Community Survey liefert Census Tracts — "
                    "kleinere Einheiten als die 36 Stadtteile. Mehrere Tracts "
                    "gehören zu einem Stadtteil und müssen zusammengefasst "
                    "werden.")),
                kasten("Die Lösung, und warum sie zweigeteilt ist", punkte([
                    "<b>Zählgrößen</b> (Einwohner, Haushalte) werden "
                    "<b>summiert</b>.",
                    "<b>Mediane</b> (Einkommen, Miete) werden "
                    "<b>bevölkerungsgewichtet gemittelt</b> — ein Median lässt "
                    "sich nicht addieren.",
                ]))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Ist der gewichtete Mittelwert von Medianen ein Median?“</b> "
                "— Nein, und das ist eine bewusst in Kauf genommene Näherung. "
                "Der echte Stadtteil-Median ließe sich nur aus Mikrodaten "
                "berechnen, die der ACS auf dieser Ebene nicht veröffentlicht. "
                "Die Näherung gilt für alle Stadtteile gleich und verzerrt den "
                "Verfahrensvergleich deshalb nicht."), "frage"),
            abstand=15))

D.folie("prep/s1_daten.py", "acs_snapshot(jahr, acs_years)",
        "Die erste Leakage-Sperre: Welcher Jahrgang war damals überhaupt "
        "veröffentlicht?",
        stapel(
            einaus("Einsatzjahr und die Liste verfügbarer ACS-Jahrgänge",
                   "der Jahrgang, der verwendet werden darf"),
            reihe(
                kasten("Die Regel", stapel(
                    absatz("Zulässig ist nur ein Jahrgang mit"),
                    absatz("<code>acs_jahr ≤ Einsatzjahr − "
                           "ACS_PUBLIKATIONS_LAG</code>"),
                    absatz("Davon der <b>jüngste</b>."), abstand=8)),
                block("Zwei Stufen der Absicherung", punkte([
                    "<b>Stufe 1:</b> der letzte verfügbare Jahrgang, nicht der "
                    "zeitlich nächstgelegene. Der nächstgelegene könnte in der "
                    "Zukunft liegen.",
                    "<b>Stufe 2:</b> zusätzlich die reale "
                    "Publikationsverzögerung von rund einem Jahr.",
                ]))),
            kasten("Warum beides nötig ist", absatz(
                "Ohne Stufe 2 bekäme ein Einsatz aus 2023 den ACS-Jahrgang "
                "2023 — der aber erst Ende 2024 erschienen ist. Das Modell "
                "hätte damit Information benutzt, die zum Prognosezeitpunkt "
                "nicht existierte. Vor dem ersten Snapshot gibt es keinen "
                "vergangenen Jahrgang; dort wird auf den ältesten "
                "zurückgegriffen — als dokumentierte Limitation, die die "
                "Hauptanalyse ab 2015 nicht mehr betrifft.")),
            abstand=15))

D.folie("prep/s1_daten.py", "join_acs(sffd, nb_per_year)",
        "Hängt jedem Einsatz die Sozialstruktur seines Stadtteils an.",
        stapel(
            einaus("Einsatztabelle und die ACS-Jahrgänge je Stadtteil",
                   "Einsatztabelle mit den fünf sozioökonomischen Merkmalen"),
            reihe(
                block("Was verknüpft wird", punkte([
                    "Medianes Haushaltseinkommen",
                    "Armutsquote und Akademikerquote",
                    "Mediane Miete und Leerstandsquote",
                    "Gesamtbevölkerung — später Offset, kein Merkmal",
                ])),
                kasten("Der Schlüssel", stapel(
                    absatz("Verknüpft wird über <b>Stadtteil × zulässigem "
                           "ACS-Jahrgang</b>, den <code>acs_snapshot()</code> "
                           "bestimmt hat."),
                    absatz("Damit trägt jede Einsatzzeile genau die "
                           "Sozialdaten, die zu ihrem Zeitpunkt öffentlich "
                           "waren."), abstand=8))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Wie oft wechselt der Jahrgang?“</b> — Die ACS-Jahrgänge "
                "sind 2009, 2014, 2019, 2021 und 2023. Innerhalb eines Blocks "
                "sind die Sozialmerkmale also über mehrere Jahre konstant. "
                "Genau das ist später der Grund, warum die effektive "
                "Stichprobe so klein ausfällt."), "frage"),
            abstand=15))

D.folie("prep/s1_daten.py", "Die räumliche Zuordnung",
        "Zwei Funktionen, eine gemeinsame Geometrie.",
        stapel(
            reihe(
                block("neighborhoods_gdf()", stapel(
                    einaus("nichts", "GeoDataFrame der 41 Stadtteilpolygone"),
                    punkte([
                        "Wird von beiden räumlichen Verknüpfungen benutzt.",
                        "Absicht: Kriminalitäts- und Baumerkmale beziehen sich "
                        "auf <b>identische</b> Flächen.",
                    ], eng=True), abstand=10)),
                block("land_use_je_neighborhood()", stapel(
                    einaus("Parzellentabelle und Stadtteilgeometrien",
                           "eine Zeile je Stadtteil mit den baulichen Merkmalen"),
                    punkte([
                        "Jede Parzelle wird über ihren Mittelpunkt einem "
                        "Stadtteil zugeordnet, dann aggregiert.",
                        "Ergebnis: Altbauanteil vor 1940, Wohngebäudeanteil, "
                        "Anteil Risikogewerbe.",
                    ], eng=True), abstand=10))),
            kasten("Die wichtigste Einschränkung dieser Datei", absatz(
                "Die Baudaten sind ein <b>Snapshot aus 2020</b> — der einzige "
                "verfügbare Jahrgang. Sie sind damit über den gesamten "
                "Analysezeitraum konstant. Zusammen mit den nur alle paar Jahre "
                "wechselnden ACS-Daten heißt das: Von zehn Prädiktoren variiert "
                "nur einer monatlich."), "warn"),
            abstand=15), label="Funktionen")

D.folie("prep/s1_daten.py", "crime_monatlich()",
        "Zählt Delikte je Stadtteil und Monat aus zwei Polizeiquellen.",
        stapel(
            einaus("die historische SFPD-Tabelle (bis 2017), die moderne "
                   "(ab 2018-01) und der Crosswalk",
                   "eine Zeile je Stadtteil und Monat mit der Deliktzahl"),
            reihe(
                block("Zwei Quellen, ein Schnitt", punkte([
                    "Das SFPD hat im Mai 2018 sein Meldesystem gewechselt.",
                    "Die <b>moderne</b> Quelle ist voraggregiert und bringt "
                    "eine Stadtteilspalte mit.",
                    "Die <b>historische</b> hat nur Koordinaten — dort ein "
                    "räumlicher Join ins Polygon.",
                ])),
                kasten("Warum die Kategorien summiert werden", absatz(
                    "Der spätere Index zählt <b>alle</b> Straftaten. Damit "
                    "erübrigt sich eine Harmonisierung der beiden "
                    "Kategorienschemata — die ohnehin nicht deckungsgleich "
                    "abbildbar wären und eine eigene Fehlerquelle darstellten."))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Verzerrt der Systemwechsel Ihre Daten?“</b> — Auf der "
                "Ebene der Rohzahlen ja, deutlich. Deshalb wird daraus kein "
                "absoluter Wert, sondern ein <i>relativer</i> Index gebildet. "
                "Wie das den Bruch neutralisiert, steht auf der nächsten Folie."),
                "frage"),
            abstand=15))

D.folie("prep/s1_daten.py", "kriminalitaetsindex(nb_per_year)",
        "Das einzige monatlich variierende Merkmal — und die zweite "
        "Leakage-Sperre.",
        stapel(
            einaus("monatliche Deliktzahlen, Einwohnerzahlen, Fensterlänge",
                   "Indexspalte je Stadtteil-Monat, dazu crime_rate_raw"),
            reihe(
                kasten("Definition — Location Quotient", stapel(
                    absatz("<code>rate(i,t) = Delikte(i, Fenster bis t−1) / "
                           "Einwohner(i)</code>"),
                    absatz("<code>index(i,t) = rate(i,t) / rate(Stadt,t)</code>"),
                    absatz("<b>1,0</b> heißt: Belastung wie im "
                           "Stadtdurchschnitt <i>desselben</i> Monats."),
                    abstand=8)),
                block("Warum relativ statt absolut", punkte([
                    "Der Systemwechsel 2018 verschiebt das stadtweite Niveau.",
                    "Ein multiplikativer Sprung wirkt auf Zähler <b>und</b> "
                    "Nenner und kürzt sich heraus.",
                    "Verbleibende Limitation: Verschiebt sich die "
                    "<b>Zusammensetzung</b> der erfassten Delikte ungleich über "
                    "die Stadtteile, kürzt sie sich nicht heraus.",
                ]))),
            reihe(
                kasten("Leakage-Sperre", absatz(
                    "Das Zwölfmonatsfenster endet strikt im <b>Vormonat</b>. "
                    "Der Index eines Monats enthält keine einzige Straftat "
                    "dieses Monats.")),
                kasten("Nicht verwechseln", absatz(
                    "<code>crime_rate_raw</code> steht daneben, ist aber "
                    "<b>kein Modellmerkmal</b> — nur Deskription für Kapitel 5.1. "
                    "Sie enthält den Bruch von 2018."), "warn")),
            abstand=13))

D.folie("prep/s1_daten.py", "berechne_quoten(df) und run_join()",
        "Der Abschluss von Schritt 1.",
        stapel(
            reihe(
                block("berechne_quoten(df)", stapel(
                    einaus("Zähler- und Nennerspalten",
                           "Anteilswerte in [0,1]"),
                    punkte([
                        "Nenner ≤ 0 ergibt <code>NaN</code> statt einer "
                        "Division durch Null.",
                        "Kriminalität taucht hier <b>nicht</b> auf — sie geht "
                        "als relativer Index ein, nicht als Anteil.",
                    ], eng=True), abstand=10)),
                block("run_join()", stapel(
                    einaus("die Dateien aus run_download()",
                           "data/processed/einsaetze.parquet"),
                    punkte([
                        "Fährt die Reihenfolge: SFFD bereinigen → ACS anfügen → "
                        "Kriminalitätsindex → Bausubstanz → Quoten.",
                        "Prüft am Ende die Vollständigkeit je Spalte gegen "
                        "<code>VOLLSTAENDIGKEITS_SCHWELLE</code>.",
                    ], eng=True), abstand=10))),
            kasten("Zwischenstand nach Schritt 1", absatz(
                "<code>einsaetze.parquet</code> enthält eine Zeile je Einsatz "
                "mit allen zehn Stadtteilmerkmalen. Auf dieser Ebene wird "
                "<b>nicht</b> modelliert — sie ist die Vorstufe für die "
                "Verdichtung in s2.")),
            abstand=16), label="Funktionen")

# ------------------------------------------------------------------ s2
D.kapitel("s2_datensaetze.py",
          "Verdichten und Zuschneiden. Hier entstehen die beiden Panels, die "
          "alle späteren Skripte lesen.",
          [("459", "Zeilen"), ("9", "Funktionen"),
           ("4 752", "Zeilen im Panel")])

D.folie("prep/s2_datensaetze.py", "Der Weg zu den beiden Panels", None,
        stapel(
            fluss([
                {"nr": "1", "titel": "aggregiere()",
                 "text": "Einsatzebene → Stadtteil × Monat, lückenloses Raster"},
                {"nr": "2", "titel": "baue_regression()",
                 "text": "Merkmale, Zielgrößen, Saison, Lags — 4 752 Zeilen"},
                {"nr": "3", "titel": "baue_klassifikation()",
                 "text": "vier Anteilsspalten und deren argmax — 4 751 Zeilen"},
                {"nr": "4", "titel": "ergaenze_aufteilung()", "akt": True,
                 "text": "fold und ist_holdout, einmal für beide Panels"},
            ]),
            reihe(
                kpi([("36", "Stadtteile"), ("132", "Monate"),
                     ("4 752", "Zeilen Regression"), ("30 / 6", "Entwicklung / Hold-out")]),
            ),
            kasten("Die Fairness-Regel", absatz(
                "Die Fold-Zuteilung wird <b>einmal</b> berechnet und auf "
                "<b>beide</b> Panels angewandt. Nur so sehen Mengen- und "
                "Strukturstrang dieselben Stadtteile im Test — sonst wären die "
                "beiden Stränge nicht vergleichbar.")),
            abstand=17),
        label="Datei")

D.folie("prep/s2_datensaetze.py", "aggregiere(von, bis, ...)",
        "Verdichtet die Einsatzebene auf Stadtteil × Monat.",
        stapel(
            einaus("Zeitgrenzen als jahr_monat-Schlüssel, inklusive Lag-Vorlauf",
                   "ein Panel mit einer Zeile je Stadtteil und Monat"),
            reihe(
                kasten("Was „vollständiges Raster“ heißt", absatz(
                    "Auch ein Stadtteil-Monat <b>ohne</b> Einsätze bekommt eine "
                    "Zeile — mit dem Wert Null. Ohne dieses Raster fehlte ein "
                    "ruhiger Monat stillschweigend.")),
                block("Warum das entscheidend ist", punkte([
                    "Die Lags arbeiten mit <code>shift()</code> über die "
                    "Zeilenfolge.",
                    "Fehlt eine Zeile, verrutscht <code>lag_1</code> auf den "
                    "vorletzten statt den letzten Monat — <b>ohne "
                    "Fehlermeldung</b>.",
                    "Parkgebiete ohne Wohnbevölkerung werden gesondert "
                    "behandelt.",
                ]))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Wie viele Nullmonate gibt es?“</b> — In der Praxis sehr "
                "wenige; die Einsatzzahlen liegen zwischen 6 und 280 je "
                "Stadtteil-Monat. Das Raster ist trotzdem konstruktiv nötig, "
                "weil man sich auf „kommt nicht vor“ nicht verlassen darf."),
                "frage"),
            abstand=15))

D.folie("prep/s2_datensaetze.py", "baue_regression(vorlauf, verbose)",
        "Der Mengenstrang: wie viele Einsätze fallen an?",
        stapel(
            einaus("einsaetze.parquet und die Zahl der Vorlaufmonate",
                   "4 752 Zeilen × 25 Spalten"),
            reihe(
                block("Was in den Datensatz kommt", punkte([
                    "<b>10 Prädiktoren</b> — sozioökonomisch, kriminalitäts"
                    "bezogen, baulich",
                    "<b>2 Zielgrößen</b> — <code>anzahl_einsaetze</code> und "
                    "<code>einsaetze_je_1000_ew</code>",
                    "<b>Exposition</b> — <code>gesamtbevoelkerung</code>, "
                    "Offset des Poisson-GLM",
                    "<b>Saison</b> — <code>monat_sin</code>, "
                    "<code>monat_cos</code>",
                    "<b>Lags</b> — bleiben im Datensatz, sind aber kein "
                    "Modellmerkmal",
                ])),
                stapel(
                    kasten("Der Lag-Vorlauf", absatz(
                        "Aggregiert wird ab <code>START</code> minus zwölf "
                        "Monaten, damit <code>lag_12</code> schon für den "
                        "ersten Analysemonat definiert ist. Danach wird auf "
                        "<code>START</code> zugeschnitten.")),
                    kasten("Warum Saison als sin/cos", absatz(
                        "Der Monat als Zahl gäbe Dezember und Januar den "
                        "Abstand 11 statt 1. Über Sinus und Kosinus wird das "
                        "Jahr zum Kreis.")), abstand=13)),
            abstand=15))

D.folie("prep/s2_datensaetze.py", "baue_klassifikation(regression, verbose)",
        "Der Strukturstrang: welche Art von Einsätzen dominiert?",
        stapel(
            einaus("der fertige Regressionsdatensatz",
                   "4 751 Zeilen × 29 Spalten mit dominante_einsatzart"),
            reihe(
                block("Wie die Zielgröße entsteht", punkte([
                    "Die NFIRS-Codes werden auf <b>vier Klassen</b> abgebildet: "
                    "Brand, Rettung/EMS, Technische Hilfe, Fehlalarm.",
                    "Je Stadtteil-Monat wird der <b>Anteil</b> jeder Klasse "
                    "berechnet.",
                    "<code>dominante_einsatzart</code> ist der <b>argmax</b> "
                    "über diese vier Anteile.",
                ])),
                kasten("Warum nicht der einzelne Einsatz?", absatz(
                    "Innerhalb eines Stadtteil-Monats tragen alle Einsätze "
                    "<b>identische</b> Strukturmerkmale. Auf Einzelfallebene "
                    "war deshalb nichts zu holen: 49,9 % Treffer gegen 48,2 % "
                    "für bloßes Raten. Zielgröße ist die "
                    "<b>Zusammensetzung</b> der Einsatzlast."))),
            kasten("Die Folge, die später zurückkommt", absatz(
                "Ein argmax über vier Anteile ist <b>kein beobachtetes "
                "Merkmal</b>. Liegen zwei Anteile dicht beieinander, entscheidet "
                "der Zufall der Monatsziehung. Genau das beziffert später "
                "<code>vorpruefung/v4_decke.py</code> als Decke A."), "warn"),
            abstand=14))

D.folie("prep/s2_datensaetze.py", "ergaenze_aufteilung(daten, versatz, selten)",
        "Die dritte Leakage-Sperre — und die wichtigste Funktion des Ordners.",
        stapel(
            einaus("Datensatz, ein Versatz für wiederholte Splits, "
                   "die Zahl brand-dominierter Monate je Stadtteil",
                   "derselbe Datensatz mit den Spalten fold und ist_holdout"),
            reihe(
                block("Das Verfahren", punkte([
                    "Die 36 Stadtteile werden sortiert und reihum auf "
                    "<b>sechs</b> Gruppen verteilt.",
                    "<b>Gruppe 0 ist das Hold-out</b> — sechs Stadtteile, die "
                    "bis zur Schlussbewertung unberührt bleiben.",
                    "Gruppen 1 bis 5 sind die Folds der Kreuzvalidierung.",
                    "Ein Stadtteil steht mit <b>allen</b> 132 Monaten in genau "
                    "einer Gruppe.",
                ])),
                kasten("Doppelt stratifiziert", punkte([
                    "<b>Erst</b> nach der Zahl brand-dominierter Monate — sonst "
                    "hat ein Fold keinen Brand-Testfall und Macro-F1 mittelt "
                    "über eine fehlende Klasse.",
                    "<b>Dann</b> nach Bevölkerung — sonst wäre die "
                    "Fold-Streuung nur ein Größeneffekt.",
                ]))),
            kasten("Warum das kein Leakage ist", absatz(
                "Festgelegt wird ausschließlich, <i>welche Stadtteile gemeinsam "
                "getestet werden</i> — genau wie bei "
                "<code>StratifiedGroupKFold</code>. Kein Modell sieht dadurch "
                "eine Zeile mehr. Der Unterschied zum Zeitschnitt: Getestet "
                "wird auf <b>unbekannten Stadtteilen</b>, nicht auf einer "
                "unbekannten Zukunft.")),
            abstand=13))

D.folie("prep/s2_datensaetze.py", "Die vier kleinen Funktionen",
        "Zwei Helfer und zwei Auskunftsfunktionen.",
        stapel(
            reihe(
                block("fold_masken(daten, k)", stapel(
                    einaus("Datensatz mit fold-Spalte, Foldnummer k",
                           "zwei boolesche Masken (Training, Test)"),
                    punkte([
                        "Test = die Stadtteile dieses Folds mit allen Monaten.",
                        "Training = alle übrigen Entwicklungsstadtteile, "
                        "<b>ohne</b> Hold-out.",
                        "Liest nur die Spalten der Datei — die Aufteilung wird "
                        "nirgends neu erfunden.",
                    ], eng=True), abstand=9)),
                block("beschreibe_splits(daten)", stapel(
                    einaus("Datensatz mit Fold-Spalten",
                           "nichts, reine Konsolenausgabe"),
                    punkte([
                        "Zeigt, welcher Stadtteil in welchem Fold getestet wird.",
                        "Belegt, dass jeder Fold den <b>vollen Zeitraum</b> "
                        "abdeckt — der sichtbare Unterschied zum Zeitschnitt.",
                        "Liefert die Zahlen für Kapitel 5.2 und 5.4.",
                    ], eng=True), abstand=9))),
            reihe(
                block("_setze_datentypen(d, merkmale)", punkte([
                    "Vereinheitlicht auf NumPy-Typen: Merkmale "
                    "<code>float64</code>, Schlüssel <code>int64</code>.",
                    "Nötig, weil <b>eine einzige</b> nullable "
                    "<code>Int64</code>-Spalte genügt, damit "
                    "<code>X.to_numpy()</code> ein Objekt-Array liefert. "
                    "sklearn fängt das still ab, XGBoost lehnt es ab.",
                ], eng=True)),
                block("_monat_minus(jahr_monat, monate)", punkte([
                    "Verschiebt einen Schlüssel wie <code>202403</code> um n "
                    "Monate zurück.",
                    "Nötig, weil <code>jahr_monat</code> eine Ganzzahl ist und "
                    "<code>202401 − 1</code> nicht <code>202312</code> ergibt.",
                ], eng=True))),
            abstand=15), label="Funktionen")

D.folie("prep/s2_datensaetze.py", "run(verbose)",
        "Der Einstiegspunkt: baut beide Panels und schreibt sie.",
        stapel(
            einaus("einsaetze.parquet",
                   "regression.parquet und klassifikation.parquet"),
            reihe(
                block("Die Reihenfolge", punkte([
                    "<code>baue_regression()</code>",
                    "<code>baue_klassifikation()</code> — nimmt Zeilen, "
                    "Zeitraum, Merkmale und Folds aus dem Regressionsdatensatz",
                    "<code>ergaenze_aufteilung()</code> <b>einmal</b>, "
                    "angewandt auf beide",
                    "<code>beschreibe_splits()</code> zur Kontrolle",
                ])),
                kasten("Warum die Ableitung so herum läuft", absatz(
                    "Der Klassifikationsdatensatz wird <b>aus</b> dem "
                    "Regressionsdatensatz gebaut, nicht unabhängig. Damit ist "
                    "konstruktiv ausgeschlossen, dass die beiden Stränge "
                    "verschiedene Zeilen, Merkmale oder Folds sehen — es gibt "
                    "keine zweite Stelle, an der das auseinanderlaufen könnte."))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Warum hat die Klassifikation eine Zeile weniger?“</b> — "
                "Ein Stadtteil-Monat ohne Einsätze hat keine Anteile und damit "
                "keinen argmax. Diese Zeile wird ausgeschlossen statt einer "
                "Klasse zugewiesen; eine erfundene Klasse wäre eine erfundene "
                "Beobachtung."), "frage"),
            abstand=14))

# ------------------------------------------------------------------ build
D.kapitel("build.py",
          "Der eine Startbefehl. Enthält keine Analyselogik — nur "
          "Ablaufsteuerung und Kontrollausgabe.",
          [("106", "Zeilen"), ("3", "Funktionen"), ("1", "Befehl")])

D.folie("prep/build.py", "Ablaufsteuerung und Selbstauskunft", None,
        stapel(
            reihe(
                block("schritt(nummer, titel)", stapel(
                    einaus("Nummer wie „1/2“ und der Titel",
                           "Startzeitpunkt für die Dauermessung"),
                    punkte(["Gibt die Überschrift aus und startet die Uhr."],
                           eng=True), abstand=9)),
                block("uebersicht()", stapel(
                    einaus("die Konstante DATEIEN",
                           "Konsolenausgabe: Zeilen, Spalten, Größe, Zeitraum"),
                    punkte([
                        "Steckbrief jeder erzeugten Datei.",
                        "Eine fehlende Datei wird als <b>FEHLT</b> gemeldet, "
                        "statt den Lauf abzubrechen — beim Teillauf ist so "
                        "sofort sichtbar, was aussteht.",
                    ], eng=True), abstand=9)),
                block("main()", stapel(
                    einaus("optional das Argument „tests“",
                           "Exitcode 0, bei „tests“ der Code des Testlaufs"),
                    punkte([
                        "Fährt s1, dann s2, dann die Übersicht.",
                        "Die Reihenfolge ist zwingend.",
                    ], eng=True), abstand=9))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Was passiert, wenn ich <code>build.py</code> zweimal "
                "starte?“</b> — Die Downloads sind über die "
                "<code>DOWNLOAD_*</code>-Schalter abgeschaltet, es wird also "
                "nichts erneut geladen. Die Panels werden neu gebaut und "
                "überschrieben — bitgenau identisch, weil keine Zufallsgröße "
                "ohne festen Startwert beteiligt ist."), "frage"),
            abstand=18),
        label="Datei")

D.folie(P, "Was man über diesen Ordner wissen muss",
        "Drei Sätze, die jede Rückfrage zur Datenbasis abdecken.",
        stapel(
            reihe(
                kasten("1 · Die Analyseeinheit", absatz(
                    "Modelliert wird der <b>Stadtteil-Monat</b>, nicht der "
                    "Einsatz. 36 Stadtteile × 132 Monate = 4 752 Zeilen. "
                    "Die Verdichtung ist eine bewusste Entscheidung, keine "
                    "Bequemlichkeit — auf Einzelfallebene tragen alle Einsätze "
                    "eines Monats dieselben Merkmale.")),
                kasten("2 · Die drei Sperren", absatz(
                    "Publikationsverzug beim ACS, Rückwärtsfenster beim "
                    "Kriminalitätsindex, Stadtteiltrennung bei den Folds. "
                    "Jede sitzt an genau einer Stelle und ist dort benannt.")),
                kasten("3 · Die Struktur der Merkmale", absatz(
                    "Von zehn Prädiktoren variiert <b>einer</b> monatlich. "
                    "Bausubstanz ist ein Snapshot 2020, Sozialdaten wechseln "
                    "alle paar Jahre. Diese Eigenschaft der Datenbasis erklärt "
                    "später den größten Teil der Ergebnisse."))),
            kasten("Der Satz, mit dem man den Ordner verteidigt", absatz(
                "<b>„Die Aufbereitung legt fest, welche Zahl zu welchem "
                "Zeitpunkt sichtbar sein darf — und schreibt das Ergebnis in "
                "die Datei, statt es jedem Skript erneut zu überlassen.“</b> "
                "Deshalb stehen <code>fold</code> und <code>ist_holdout</code> "
                "als Spalten im Panel und nicht als Code im Modell.")),
            abstand=20),
        label="Zusammenfassung")

if __name__ == "__main__":
    schreibe(D, "prep/ · Datenaufbereitung",
             pathlib.Path("/home/claude/out/folien/01_prep.pdf"))
