"""Foliensatz 03 - modelle/ (Verfahrensvergleich, Interpretation, Abbildungen)."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from renderer import (Deck, absatz, block, einaus, fluss, kasten, kpi, punkte,
                      reihe, schreibe, stapel, tabelle)

D = Deck(
    "modelle",
    "modelle/ — der Verfahrensvergleich und seine Deutung",
    "Fünf Dateien beantworten die Forschungsfrage: Welches Verfahren erzielt "
    "die höchste Prognosegüte, welche Merkmale tragen sie, und was kostet sie.",
    [("Bachelorarbeit", "SFFD-Einsatzprognose"),
     ("Phase", "CRISP-DM 4 und 5"),
     ("Ergebnis", "Verfahrensvergleich · SHAP · 10 Abbildungen")])

P = "modelle"

# ------------------------------------------------------------------ Ordner
D.folie(P, "Was dieser Ordner leistet", None,
        stapel(
            fluss([
                {"nr": "Strang 1", "titel": "m02_menge.py",
                 "text": "Wie viele Einsätze? Ridge, RF, XGBoost gegen das "
                         "Poisson-GLM"},
                {"nr": "Strang 2", "titel": "m03_struktur.py",
                 "text": "Welche Art dominiert? RF und XGBoost gegen das Logit"},
                {"nr": "Deutung", "titel": "m04_shap.py", "akt": True,
                 "text": "Welche Merkmale tragen — Attribution und Ablation"},
                {"nr": "Darstellung", "titel": "m05_abbildungen.py",
                 "text": "Zehn Abbildungen, ausschließlich aus CSV gelesen"},
            ]),
            reihe(
                block("Die vier Unterfragen der Arbeit", punkte([
                    "<b>UF1</b> — Welche Faktorgruppe trägt wie viel? "
                    "(m04)",
                    "<b>UF2</b> — Welches Verfahren erzielt die höchste Güte? "
                    "(m02, m03)",
                    "<b>UF3</b> — Was kostet Training und Inferenz? "
                    "(m02, m03 → A4, A9)",
                    "<b>UF4</b> — Was folgt daraus für die Modellauswahl? "
                    "(m04, v3 → A3)",
                ])),
                kasten("Die Regel, die den Ordner zusammenhält", absatz(
                    "Ein Lauf, <b>eine</b> Spezifikation. Nichts wird aus einem "
                    "früheren Lauf wiederverwendet, kein Zwischenstand "
                    "eingelesen, kein Schalter ändert das Modell. Wer die "
                    "Spezifikation ändert, rechnet neu — sonst wären still "
                    "Parameter aus einer anderen Welt im Ergebnis."))),
            abstand=18),
        label="Ordner")

D.folie(P, "Die fünf Dateien", None,
        stapel(
            tabelle(["Datei", "Rolle", "Läufe", "Zeilen"], [
                [("config_modelle.py", "fn"), "Suchräume, Budget, Startwert",
                 ("—", "z"), ("110", "z")],
                [("m02_menge.py", "fn"),
                 "Mengenstrang: 2 Zielgrößen × 3 Verfahren",
                 ("300", "z"), ("840", "z")],
                [("m03_struktur.py", "fn"),
                 "Strukturstrang: 1 Zielgröße × 2 Verfahren",
                 ("100", "z"), ("732", "z")],
                [("m04_shap.py", "fn"), "Interpretation: Beiträge und Ablation",
                 ("—", "z"), ("690", "z")],
                [("m05_abbildungen.py", "fn"), "Zehn Abbildungen für Kapitel 7",
                 ("—", "z"), ("1 103", "z")],
            ]),
            reihe(
                kasten("Reihenfolge", absatz(
                    "m02 und m03 sind unabhängig voneinander. m04 setzt beide "
                    "voraus, m05 setzt m02, m03, m04 sowie v1 und v3 voraus.")),
                kasten("Zwei Betriebsarten", absatz(
                    "m02 und m03 kennen das Argument <code>holdout</code>. Ohne "
                    "es werden die sechs Hold-out-Stadtteile <b>zu Beginn und "
                    "unwiderruflich</b> herausgefiltert — alles Folgende kann "
                    "sie nicht mehr sehen, auch nicht versehentlich."))),
            abstand=18),
        label="Ordner")

D.folie(P, "Das Vier-Phasen-Muster",
        "m02 und m03 sind identisch aufgebaut. Wer eine der beiden Dateien "
        "verstanden hat, kennt die andere.",
        stapel(
            fluss([
                {"nr": "Phase 1", "titel": "phase_tuning()",
                 "text": "Hyperparameter suchen — nur auf Wiederholung 0, "
                         "einmal je Verfahren und Fold"},
                {"nr": "Phase 2", "titel": "phase_bewertung()",
                 "text": "10 Wiederholungen × 5 Folds mit einem FRISCHEN "
                         "Modell je Lauf"},
                {"nr": "Phase 3", "titel": "aggregiere()",
                 "text": "zweistufig mitteln: erst je Wiederholung, dann "
                         "darüber"},
                {"nr": "Phase 4", "titel": "vergleiche()", "akt": True,
                 "text": "gepaarter Wilcoxon gegen die Stufe-2-Baseline"},
            ]),
            reihe(
                kasten("Warum in Phase 2 neu gefittet wird", absatz(
                    "Der <code>best_estimator_</code> aus dem Tuning ist auf "
                    "nur <b>drei Vierteln</b> der Trainingsstadtteile gefittet "
                    "— der innere CV hat ein Viertel für die Validierung "
                    "zurückgehalten. Übernähme man ihn, verschenkte man ein "
                    "Viertel der Daten.")),
                kasten("Warum nur auf Wiederholung 0 getunt wird", absatz(
                    "Die Wiederholungen dienen der <b>Streuungsschätzung</b>, "
                    "nicht der Modellwahl. Eine bewusste Vereinfachung — und "
                    "im Text zu benennen. Was sie kostet, beziffert "
                    "<code>leakage_diagnose()</code>."))),
            abstand=18),
        label="Konzept")

D.folie(P, "Drei Fallstricke, die in beiden Strängen gelten",
        "Sie sind der Grund, warum die Zahlen glaubwürdig sind — und die "
        "wahrscheinlichsten Prüfungsfragen.",
        reihe(
            block("1 · Der innere CV", stapel(
                absatz("<code>RandomizedSearchCV</code> nimmt voreingestellt "
                       "<code>KFold</code> und schneidet <b>nach Zeilen</b>."),
                punkte([
                    "Ein Stadtteil hat 132 Zeilen; die Strukturmerkmale sind "
                    "innerhalb eines Jahres konstant.",
                    "Ohne Gruppierung stünden faktisch dieselben Zeilen in "
                    "innerem Training und innerer Validierung.",
                    "<b>Man sieht es den Zahlen nicht an — sie wären nur zu "
                    "gut.</b>",
                ], eng=True), abstand=10)),
            block("2 · Zweistufig mitteln", stapel(
                absatz("Die 50 Fold-Ergebnisse sind <b>nicht unabhängig</b>."),
                punkte([
                    "Es sind dieselben 29 Stadtteile in zehn Gruppierungen.",
                    "Ein Intervall aus <code>std_folds/√50</code> wäre zu eng.",
                    "Maßgeblich ist <code>std_wiederholungen</code> — die "
                    "Streuung der zehn Wiederholungsmittel.",
                    "Beide Spalten wandern mit, damit der Unterschied sichtbar "
                    "bleibt.",
                ], eng=True), abstand=10)),
            block("3 · Die Hold-out-Sperre", stapel(
                absatz("Sechs Stadtteile bleiben bis zur Schlussbewertung "
                       "unberührt."),
                punkte([
                    "Der Filter steht in <code>main()</code>, <b>bevor</b> "
                    "irgendetwas rechnet.",
                    "Er ist konstruktiv, nicht disziplinarisch: Was gefiltert "
                    "ist, kann kein späterer Codeteil versehentlich lesen.",
                    "Die Schlussbewertung ist <b>eine</b> Messung an "
                    "<b>sechs</b> Einheiten — kein Mittelwert, keine Streuung.",
                ], eng=True), abstand=10))),
        label="Konzept")

# ------------------------------------------------------------------ config
D.kapitel("config_modelle.py",
          "Suchräume, Budget und Startwert. Berührt keine einzige Datei auf "
          "der Platte.",
          [("110", "Zeilen"), ("4", "Konstanten"), ("100", "Tuning-Budget")])

D.folie("modelle/config_modelle.py", "Die Trennlinie zu prep/config.py",
        "Nicht „Daten gegen Modelle“ — sondern: was geschrieben wird gegen "
        "was gerechnet wird.",
        stapel(
            reihe(
                block("prep/config.py hält fest …", punkte([
                    "welche Spalten die Panels haben",
                    "wie <code>fold</code> und <code>ist_holdout</code> belegt "
                    "sind",
                    "welche Prädiktoren und Klassen es gibt",
                    "— also alles, was <b>in die Datei geschrieben</b> wird",
                ])),
                block("config_modelle.py hält fest …", punkte([
                    "die Suchräume je Verfahren",
                    "<code>TUNING_BUDGET</code> und <code>RANDOM_STATE</code>",
                    "<code>WIEDERHOLUNGEN = 10</code>",
                    "— also alles, was nur <b>beim Rechnen</b> gilt",
                ]))),
            reihe(
                kasten("Warum die Prädiktoren nicht hierher wandern", absatz(
                    "Die Modellskripte <b>lesen</b> diese Festlegungen, sie "
                    "treffen sie nicht. Zöge man sie hierher, gäbe es zwei "
                    "Dateien, die zwingend übereinstimmen müssen — genau die "
                    "Fehlerquelle, die der Aufbau vermeiden soll.")),
                kasten("Die bekannte Schwäche", absatz(
                    "Vier Dateien der Vorprüfung holen <code>WIEDERHOLUNGEN</code> "
                    "bzw. <code>RANDOM_STATE</code> von hier — der Pfeil zeigt "
                    "<b>rückwärts</b>. Fachlich richtig ist, dass es nur "
                    "<i>eine</i> Definition gibt; strukturell säße sie besser "
                    "in <code>prep/config.py</code>."), "warn")),
            abstand=18),
        label="Datei")

D.folie("modelle/config_modelle.py", "Budget und Suchräume",
        "Zwei Zahlen, die beide hergeleitet und nicht gewählt sind.",
        stapel(
            reihe(
                kasten("Warum Budget 100", stapel(
                    absatz("Bergstra & Bengio (2012) geben die geschlossene Form"),
                    absatz("<code>P = 1 − (1 − v/V)^T</code>"),
                    punkte([
                        "für <code>v/V = 0,05</code>: T = 50 → 92,3 %, "
                        "T = 100 → 99,4 %",
                        "Die Formel enthält die <b>Dimension nicht</b> — "
                        "deshalb bekommen Ridge (1 Parameter) und XGBoost "
                        "(7 Parameter) dasselbe Budget.",
                        "Die verbreitete Angabe „60 Ziehungen“ steht nicht in "
                        "dem Papier.",
                    ], eng=True), abstand=9)),
                block("Warum vier Suchräume erweitert wurden", punkte([
                    "Erweitert wurde, wo der beste Wert <b>an der Grenze</b> "
                    "lag — bei sechs von sieben geprüften Parametern.",
                    "Das ist eine Aussage über die <b>Suche</b>, nicht über das "
                    "Ergebnis: Lag das Optimum an der Grenze, war die Grenze "
                    "falsch gesetzt.",
                    "<b>Nicht</b> erweitert: <code>max_features</code>, "
                    "<code>min_samples_leaf</code>, <code>subsample</code>, "
                    "<code>colsample_bytree</code> — ihre Grenzen sind "
                    "natürlich, dahinter existiert nichts.",
                ]))),
            kasten("Der wichtigste Fund", absatz(
                "<code>max_depth</code> lief bei XGBoost von 3 bis 10. Im "
                "Strukturstrang wählten <b>vier von fünf Folds die Untergrenze "
                "3</b>. Mit geöffneter Grenze wählen sie 2, 2 und 1 — das "
                "Modell wollte flacher sein, als es durfte. Zusammen mit "
                "<code>reg_lambda</code> ist das die plausibelste Erklärung "
                "dafür, dass die Baumverfahren in der Kreuzvalidierung gewinnen "
                "und auf dem Hold-out verlieren.")),
            abstand=15), label="Konstanten")

# ------------------------------------------------------------------ m02
D.kapitel("m02_menge.py",
          "Der Mengenstrang. Zwei Zielgrößen × drei Verfahren × 10 "
          "Wiederholungen × 5 Folds.",
          [("840", "Zeilen"), ("16", "Funktionen"), ("300", "Läufe")])

D.folie("modelle/m02_menge.py", "verfahren(name, n_jobs) und suchraum(name)",
        "Wie die drei Pipelines gebaut werden — und warum sie unterschiedlich "
        "aussehen dürfen.",
        stapel(
            reihe(
                block("verfahren(name, n_jobs)", stapel(
                    einaus("Verfahrensname, optional n_jobs",
                           "Pipeline ohne Hyperparameter"),
                    punkte([
                        "<b>Ridge</b> bekommt StandardScaler und "
                        "log-Zieltransformation <i>in</i> der Pipeline — der "
                        "L2-Strafterm behandelt alle Koeffizienten gleich.",
                        "Die Baumverfahren bekommen <b>keine</b> "
                        "Zieltransformation: sie sind skalenunempfindlich, und "
                        "eine transformierte Zielgröße machte die Gütemaße "
                        "unvergleichbar.",
                    ], eng=True), abstand=9)),
                block("suchraum(name)", stapel(
                    einaus("Verfahrensname",
                           "dict Parametername → scipy-Verteilung"),
                    punkte([
                        "Die Config hält die Räume als Tupel "
                        "<code>(\"loguniform\", a, b)</code>, damit sie ohne "
                        "scipy lesbar bleiben.",
                        "Der <b>Präfix</b> hängt am Pipeline-Aufbau: bei Ridge "
                        "liegt der Schätzer zwei Ebenen tief, bei den "
                        "Baumverfahren direkt.",
                    ], eng=True), abstand=9))),
            kasten("Die Verlustfunktion — eine Korrektur, kein Feintuning",
                   absatz(
                "Bis 06.08.2026 rechneten beide Baumverfahren mit dem "
                "<b>quadratischen Fehler auf rohen Zähldaten</b>, während "
                "Baseline und Ridge multiplikativ arbeiteten. Bei Einsatzzahlen "
                "von 6 bis 280 gewichtet der quadratische Fehler einen Fehler "
                "von 20 in Tenderloin genauso wie in Seacliff, wo er das "
                "Dreifache des Gesamtwerts ausmacht. Seither: "
                "<code>reg:tweedie</code> für XGBoost, "
                "<code>criterion=\"poisson\"</code> für den Random Forest. "
                "Dass scikit-learn kein Tweedie für Wälder kennt, ist selbst "
                "ein berichtbarer Befund über das Verfahren."), "warn"),
            abstand=14), label="Funktionen")

D.folie("modelle/m02_menge.py", "tune(name, train, ziel)",
        "Der Fallstrick, der die ganze Arbeit entwerten kann.",
        stapel(
            einaus("Trainingsrahmen des Folds, Verfahren, Zielgröße",
                   "die PARAMETER als dict — nicht das Modell"),
            reihe(
                kasten("Der innere CV muss gruppieren", punkte([
                    "<code>RandomizedSearchCV</code> nimmt voreingestellt "
                    "<code>KFold</code>.",
                    "Ein Stadtteil hätte dann etwa 100 Zeilen im inneren "
                    "Training und 32 in der inneren Validierung.",
                    "Da die Strukturmerkmale innerhalb eines Jahres konstant "
                    "sind, wären das faktisch <b>dieselben Zeilen</b>.",
                    "Die Hyperparameter würden auf einen geleakten Schätzwert "
                    "optimiert — der Vorteil des äußeren Stadtteil-Splits wäre "
                    "verspielt.",
                ]), "warn"),
                stapel(
                    block("Warum die Parameter zurückkommen", absatz(
                        "Nicht <code>best_estimator_</code>: der ist auf dem "
                        "inneren Trainingsanteil gefittet und verschenkte ein "
                        "Viertel der Daten.")),
                    block("Der zweite Fallstrick", absatz(
                        "Der Schätzer läuft <b>einkernig</b>, parallelisiert "
                        "wird allein die Suche. Zuvor stand "
                        "<code>n_jobs=-1</code> an beiden Stellen — die "
                        "Prozesse blockierten sich gegenseitig; ein Probelauf "
                        "stand nach 15 Minuten noch in Phase 1.")),
                    abstand=13)),
            abstand=15))

D.folie("modelle/m02_menge.py", "ein_lauf(...)",
        "Ein Fit, eine Vorhersage — und vier Messgrößen, die daneben "
        "mitlaufen.",
        stapel(
            einaus("Trainings- und Testrahmen, Verfahren, Parameter, "
                   "Zielgröße, auch_parallel",
                   "dict mit Gütemaßen, Laufzeiten, n_negativ, y_hat_min, "
                   "Extrapolationsanteil"),
            reihe(
                block("Die Zeitmessung", punkte([
                    "Gemessen wird <b>um <code>fit</code> und "
                    "<code>predict</code> herum</b>, nicht um die ganze "
                    "Funktion — sonst steckte die Metrikberechnung mit in der "
                    "Zahl.",
                    "<b>Einkernig</b> für alle drei Verfahren, damit Unterfrage "
                    "3 vergleichbar bleibt.",
                    "<code>auch_parallel=True</code> misst denselben Fit "
                    "zusätzlich über alle Kerne — die Differenz ist der "
                    "Parallelisierungsgewinn.",
                ])),
                block("Die mitlaufenden Diagnosen", punkte([
                    "<code>n_negativ</code> und <code>y_hat_min</code>: Ridge "
                    "auf <code>log(1+y)</code> kann nach <code>expm1</code> "
                    "Werte unter null liefern. <b>Nicht gekappt</b> — das wäre "
                    "ein Eingriff —, aber ausgewiesen.",
                    "<code>parallel_abweichung_max</code>: Ändert die Kernzahl "
                    "das <i>Ergebnis</i>? Bei XGBoost ja, erheblich; bei Ridge "
                    "und RF null.",
                    "<code>ueberanpassung_*</code>: dieselbe Güte auf den "
                    "<b>Trainings</b>stadtteilen.",
                ]))),
            kasten("Wie die Überanpassungszahl NICHT zu lesen ist", absatz(
                "Ein Random Forest mit <code>min_samples_leaf = 1</code> "
                "interpoliert seine Trainingsdaten <b>konstruktionsbedingt</b> "
                "— ein Trainings-R² von 0,98 ist dort erwartbar und kein Beweis "
                "krankhafter Überanpassung. Der Abstand ist zwischen "
                "<i>Konfigurationen desselben Verfahrens</i> aussagekräftig, "
                "nicht als Verhältnis zwischen Verfahren."), "warn"),
            abstand=13))

D.folie("modelle/m02_menge.py", "Die vier Phasenfunktionen", None,
        stapel(
            reihe(
                block("phase_tuning(panel, selten)", stapel(
                    einaus("Panel der Entwicklungsstadtteile",
                           "tuning.csv mit 30 Zeilen"),
                    punkte([
                        "<b>Keine Wiederverwendung:</b> tuning.csv ist ein "
                        "Ergebnis dieses Laufs, kein Eingang.",
                        "Gesucht wird über (Verfahren × Fold) = <b>15</b> "
                        "Durchgänge; beide Zielgrößen erhalten denselben Satz.",
                        "<code>tuning_sekunden</code> steht deshalb bei beiden "
                        "Zielgrößen gleich — eine Summe über alle 30 Zeilen "
                        "zählt doppelt.",
                    ], eng=True), abstand=9)),
                block("phase_bewertung(panel, parameter, selten)", stapel(
                    einaus("Panel, Parametertabelle aus Phase 1",
                           "menge_folds.csv mit 300 Zeilen"),
                    punkte([
                        "10 Wiederholungen × 5 Folds × 3 Verfahren × 2 "
                        "Zielgrößen.",
                        "Trainiert wird je Fold auf <b>allen</b> "
                        "Trainingsstadtteilen — mit einem frischen Modell.",
                    ], eng=True), abstand=9))),
            reihe(
                block("aggregiere(folds)", punkte([
                    "Zweistufig mitteln: erst je Wiederholung über die 5 Folds, "
                    "dann die Streuung dieser zehn Werte.",
                    "Liefert <code>std_folds</code> <b>und</b> "
                    "<code>std_wiederholungen</code>.",
                ], eng=True)),
                block("Die beiden Statistikhelfer", punkte([
                    "<code>_gepaart(a, b)</code> — Wilcoxon plus mittlere "
                    "Differenz, Konfidenzintervall und gewonnene Läufe. Bei "
                    "RMSE ist klein besser, <code>b − a</code> ist der Vorteil "
                    "von <code>a</code>.",
                    "<code>_holm(p)</code> — Holm-Bonferroni: p-Werte "
                    "aufsteigend, kleinster gegen α/m. Uniform stärker als "
                    "Bonferroni bei gleicher Fehlerkontrolle.",
                ], eng=True))),
            abstand=15), label="Funktionen")

D.folie("modelle/m02_menge.py", "vergleiche(folds, baselines)",
        "Phase 4 — hier entsteht die Primäraussage der Arbeit.",
        stapel(
            einaus("menge_folds.csv und die Baseline-Läufe aus v1_baselines.py",
                   "vergleich.csv"),
            reihe(
                block("Zwei Rollen", punkte([
                    "<b>primär</b> — jedes Verfahren gegen die "
                    "Stufe-2-Baseline: 3 × 2 = 6 Tests. <b>Keine Familie</b>, "
                    "weil jede Frage vorab einzeln formuliert ist; keine "
                    "Korrektur.",
                    "<b>sekundär</b> — jedes Verfahrenspaar: 3 Paare × 2 "
                    "Zielgrößen = 6 Tests. <b>Eine Familie</b>, darauf "
                    "Holm-Bonferroni.",
                ])),
                block("Zwei Teststufen", punkte([
                    "<b>wiederholung</b>, n = 10 — <b>der Primärtest</b>. Die "
                    "50 Einzelläufe sind Pseudoreplikation und liefern zu "
                    "kleine p-Werte.",
                    "<b>lauf</b>, n = 50 — ausdrücklich als "
                    "<b>Sensitivität</b> geführt, nicht als Ergebnis.",
                ]))),
            kasten("Warum immer drei Zahlen daneben stehen", absatz(
                "Auch die zehn Wiederholungsmittel sind nicht unabhängig — es "
                "bleiben 29 Einheiten. Das berichtete Konfidenzintervall ist "
                "daher <b>enger als die wahre Unsicherheit</b> (Nadeau & Bengio "
                "2003). Deshalb stehen mittlere Differenz, Intervall und "
                "gewonnene Läufe <b>immer</b> neben dem p-Wert — unabhängig "
                "davon, wie er ausfällt. Der Sinn: Ein Ergebnis darf nicht an "
                "einer einzigen Schwelle hängen.")),
            abstand=14))

D.folie("modelle/m02_menge.py", "hold_out() und leakage_diagnose()",
        "Die beiden Funktionen, die das eigene Vorgehen prüfen.",
        stapel(
            reihe(
                block("hold_out(panel, parameter, folds, selten)", stapel(
                    einaus("vollständiges Panel, Parametertabelle aus Phase 1",
                           "holdout.csv mit Spalte fold_der_parameter"),
                    punkte([
                        "Auf 29 Stadtteilen trainieren, auf 6 bewerten — "
                        "<b>einmalig</b>.",
                        "Welche Parameter? Das Tuning liefert fünf Sätze. "
                        "Gewählt ist der Satz des Folds mit dem niedrigsten "
                        "RMSE in Wiederholung 0 — deterministisch und "
                        "ausschließlich aus Entwicklungsdaten.",
                        "<b>Beide Baselines laufen mit:</b> Ein RMSE von 23,7 "
                        "ist ohne Bezugspunkt keine Aussage.",
                    ], eng=True), abstand=9)),
                block("leakage_diagnose(folds, baselines)", stapel(
                    einaus("menge_folds.csv und die Baseline-Läufe",
                           "Vorsprung in W0 gegen W1–9"),
                    punkte([
                        "Getunt wird einmal, auf Wiederholung 0. Dort ist der "
                        "Vorsprung sauber gemessen.",
                        "In W1–9 waren im Mittel <b>78 %</b> der Teststadtteile "
                        "in der Suchmenge. Wäre der Effekt bedeutsam, müsste "
                        "der Vorsprung dort systematisch größer ausfallen.",
                        "Bewusst als <b>schwache</b> Diagnose zu lesen: W0 ist "
                        "auch eine andere Aufteilung, der Unterschied ist "
                        "konfundiert.",
                    ], eng=True), abstand=9))),
            kasten("Wie das Hold-out zu berichten ist", absatz(
                "Als <b>eine</b> Messung an <b>sechs</b> Einheiten — kein "
                "Mittelwert, keine Streuung. Die Zahl ist deutlich unsicherer "
                "als die Kreuzvalidierungswerte und darf nicht als deren "
                "Bestätigung gelesen werden."), "warn"),
            abstand=14), label="Funktionen")

D.folie("modelle/m02_menge.py", "Die Hilfsfunktionen und main(argv)", None,
        stapel(
            reihe(
                block("_rein_python(p)", punkte([
                    "Wandelt NumPy-Skalare in native Typen, <b>bevor</b> sie "
                    "nach JSON gehen.",
                    "<code>np.float64</code> erbt von <code>float</code> und "
                    "überlebt <code>json.dumps</code> zufällig — "
                    "<code>np.int64</code> erbt <b>nicht</b> von "
                    "<code>int</code>.",
                    "Mit <code>default=str</code> würde aus 287 die "
                    "Zeichenkette „287“, und <code>set_params</code> bräche ab "
                    "— mitten im mehrstündigen Lauf, nach dem Tuning.",
                ], eng=True)),
                block("extrapolationsanteil(train, test)", punkte([
                    "Anteil der Testzeilen außerhalb des "
                    "Trainings-Wertebereichs.",
                    "Erfasst nur die <b>Spanne je Merkmal</b>, nicht unbekannte "
                    "Kombinationen — das echte Extrapolationsproblem ist also "
                    "eher größer.",
                    "Erklärt später, warum ein Fold aus der Reihe fällt.",
                ], eng=True))),
            reihe(
                block("_parameter_je_fold(parameter) · paar(...)", punkte([
                    "Zwei reine Umformer: Nachschlagetabelle aus tuning.csv, "
                    "und eine Vergleichszeile für vergleich.csv.",
                ], eng=True)),
                block("main(argv)", punkte([
                    "Fährt die vier Phasen und schreibt fünf CSV-Dateien.",
                    "Ohne das Argument <code>holdout</code> werden die "
                    "Hold-out-Zeilen <b>zu Beginn</b> herausgefiltert.",
                ], eng=True))),
            abstand=15), label="Funktionen")

# ------------------------------------------------------------------ m03
D.kapitel("m03_struktur.py",
          "Der Strukturstrang. Spiegelt m02 — dieselben Funktionen, dieselbe "
          "Reihenfolge. Hier zählen nur die Unterschiede.",
          [("732", "Zeilen"), ("20", "Funktionen"), ("100", "Läufe")])

D.folie("modelle/m03_struktur.py", "Was anders ist als in m02", None,
        stapel(
            tabelle(["Aspekt", "m02_menge.py", "m03_struktur.py"], [
                ["Zielgröße", "anzahl_einsaetze, einsaetze_je_1000_ew",
                 "dominante_einsatzart — vier ungeordnete Klassen"],
                ["Verfahren", "Ridge, Random Forest, XGBoost",
                 "nur Random Forest und XGBoost"],
                ["Stufe-2-Baseline", "Poisson-GLM mit Offset",
                 "multinomiales Logit"],
                ["Gütemaß", "RMSE (getestet), MAE und R² nachrichtlich",
                 "Macro-F1 (getestet), Macro-AUROC, Accuracy nachrichtlich"],
                ["Tuning-Scoring", "RMSE", "f1_macro"],
                ["Mehrfachvergleich", "Holm über 6 sekundäre Tests",
                 "kein Holm — nur ein sekundärer Test"],
                ["Läufe", "300", "100"],
            ], eng=True),
            reihe(
                kasten("Warum Ridge fehlt", absatz(
                    "Eine lineare Regression hat auf einer <b>nominalen</b> "
                    "Zielgröße keine Entsprechung. Das Gegenstück wäre das "
                    "multinomiale Logit — und das ist bereits die Baseline.")),
                kasten("Warum kein Holm", absatz(
                    "Die sekundäre Familie besteht aus einem <b>einzigen</b> "
                    "Test (RF gegen XGBoost); eine Korrektur über einen Test ist "
                    "die Identität. Regression und Klassifikation sind getrennte "
                    "Testfamilien — das ist in Kapitel 7 zu benennen, weil "
                    "dieser Vergleich unkorrigiert gegen α = 0,05 läuft."))),
            abstand=17),
        label="Datei")

D.folie("modelle/m03_struktur.py", "Drei Fallstricke, die es in m02 nicht gibt",
        None,
        reihe(
            block("1 · Klassengewichte statt Resampling", stapel(
                absatz("Die Klassenverteilung ist stark schief — <b>79 % "
                       "Fehlalarm</b>."),
                punkte([
                    "Random Forest: <code>class_weight=\"balanced\"</code> als "
                    "Hyperparameter.",
                    "XGBoost: <code>sample_weight</code> beim Fit — das "
                    "Verfahren kennt keinen solchen Parameter. Dafür "
                    "<code>_gewichte()</code>.",
                    "<b>Kein SMOTE</b>, kein Over- oder Undersampling: Das wäre "
                    "ein Eingriff in die Datenverteilung und bräche die "
                    "Vergleichbarkeit mit den Baselines.",
                ], eng=True), abstand=10)),
            block("2 · Label-Encoder einmal global", stapel(
                absatz("<code>XGBClassifier</code> erwartet Integer-Labels "
                       "0…3 — <code>kodiere()</code> liefert sie."),
                punkte([
                    "Das Mapping folgt der <b>globalen</b> Reihenfolge "
                    "<code>KLASSEN</code>, nicht den gerade vorliegenden Daten.",
                    "Ein je Fold gefitteter <code>LabelEncoder</code> "
                    "verschöbe in einem Fold ohne Brand die Zahlen.",
                    "Die Wahrscheinlichkeitsspalten zeigten danach auf die "
                    "<b>falschen Klassen</b> — ohne Fehlermeldung.",
                ], eng=True), abstand=10)),
            block("3 · Macro-AUROC kann undefiniert sein", stapel(
                absatz("Fehlt eine Klasse im Testfold, ist sie nicht "
                       "berechenbar."),
                punkte([
                    "Dann als <b>fehlend</b> führen — <b>nicht</b> durch 0,5 "
                    "oder 0 ersetzen.",
                    "Ein erfundener Wert zöge den Mittelwert nach unten und "
                    "sähe wie eine Messung aus.",
                    "<code>labels=klassen_modell</code> bringt die Spalten in "
                    "die Reihenfolge, die das Modell benutzt hat — der zweite "
                    "Teil von Fallstrick 2.",
                    "<code>zero_division=0</code> bei Macro-F1 muss gesetzt "
                    "bleiben, sonst bricht der Lauf ab.",
                ], eng=True), abstand=10))), label="Fallstricke")

D.folie("modelle/m03_struktur.py", "Die eigenen Funktionen im Detail", None,
        stapel(
            reihe(
                block("tune(name, train)", stapel(
                    einaus("Trainingsrahmen des Folds, Verfahren",
                           "die Parameter als dict"),
                    punkte([
                        "Der Fallstrick aus m02 gilt unverändert: Der innere CV "
                        "<b>muss</b> nach Stadtteil gruppieren.",
                        "<b>f1_macro statt Accuracy</b>, weil die "
                        "Mehrheitsklasse allein über 0,8 Accuracy erreicht. Ein "
                        "darauf optimiertes Tuning wählte Modelle, die die drei "
                        "seltenen Klassen ignorieren — genau das, was die "
                        "Fragestellung nicht will.",
                    ], eng=True), abstand=9)),
                block("suchraum(name)", stapel(
                    einaus("Verfahrensname", "dict Parametername → Verteilung"),
                    punkte([
                        "Beide Verfahren sind hier <b>nackte Schätzer</b> statt "
                        "Pipelines — keine Skalierung nötig.",
                        "Eine Ausnahme: <code>tweedie_variance_power</code> "
                        "steuert die Verlustfunktion der <i>Regression</i> und "
                        "ist bei <code>multi:softprob</code> bedeutungslos. "
                        "XGBoost nähme ihn stillschweigend an — ein Sechstel "
                        "des Budgets auf einer wirkungslosen Dimension.",
                    ], eng=True), abstand=9))),
            reihe(
                block("ein_lauf(...) und fitte(...)", punkte([
                    "Die Wahrscheinlichkeiten für die AUROC kommen aus einem "
                    "<b>zweiten</b> Aufruf, damit "
                    "<code>inferenz_sekunden</code> die reine "
                    "Klassenvorhersage misst und vergleichbar bleibt.",
                    "<code>fitte()</code> kapselt, dass XGBoost die Gewichte "
                    "beim Fit bekommt und der RF sie als Hyperparameter hat.",
                ], eng=True)),
                block("hold_out(panel, parameter, folds)", punkte([
                    "Gewählt wird der Parametersatz des Folds mit dem "
                    "<b>höchsten</b> Macro-F1 in Wiederholung 0.",
                    "Das Baseline-Modell stammt seit 10.08.2026 aus "
                    "<code>v1_baselines.logit_glm()</code> — vorher war es hier "
                    "ein zweites Mal nachgebaut.",
                ], eng=True))),
            abstand=15), label="Funktionen")

# ------------------------------------------------------------------ m04
D.kapitel("m04_shap.py",
          "Interpretation. Welche Merkmale tragen die Vorhersage — und was "
          "wären sie wert, wenn man sie weglässt?",
          [("690", "Zeilen"), ("10", "Funktionen"), ("9", "Ergebnisdateien")])

D.folie("modelle/m04_shap.py", "Attribution und Ablation",
        "Zwei Antworten auf Unterfrage 1 — und der Grund, warum es beide "
        "braucht.",
        stapel(
            reihe(
                block("Attribution", stapel(
                    absatz("<b>Welcher Anteil der SHAP- bzw. "
                           "Koeffizientenmasse entfällt auf eine "
                           "Faktorgruppe?</b>"),
                    punkte([
                        "Sagt, wie ein Modell seine <b>Aufmerksamkeit "
                        "verteilt</b>.",
                        "Summiert sich je Modell auf 100 %.",
                        "Dateien: <code>gruppen.csv</code>, "
                        "<code>faktorgruppen_menge.csv</code>",
                    ], eng=True), abstand=10)),
                block("Ablation", stapel(
                    absatz("<b>Was kostet es, die Gruppe wegzulassen?</b>"),
                    punkte([
                        "Sagt, was die Gruppe <b>wert</b> ist.",
                        "Jede Gruppe wird einmal entfernt, alles andere bleibt "
                        "gleich. Die Verschlechterung ist der Beitrag.",
                        "Datei: <code>ablation_faktorgruppen_mittel.csv</code>",
                    ], eng=True), abstand=10))),
            kasten("Warum die zweite Frage die härtere ist", absatz(
                "Ein Merkmal kann <b>viel Masse binden und trotzdem ersetzbar "
                "sein</b>, weil ein anderes dieselbe Information trägt. Unter "
                "Kollinearität laufen Attribution und Ablation deshalb "
                "auseinander — und nur die Ablation trennt „bekommt "
                "Aufmerksamkeit“ von „ist unverzichtbar“.")),
            abstand=20),
        label="Konzept")

D.folie("modelle/m04_shap.py", "Die eine Regel",
        "SHAP wird nur für Modelle gerechnet, die ihre Stufe-2-Baseline "
        "schlagen.",
        stapel(
            reihe(
                block("schlagen_die_latte(vergleich)", stapel(
                    einaus("vergleich.csv beider Stränge",
                           "Menge der zugelassenen Kombinationen"),
                    punkte([
                        "Grundlage ist der <b>Primärtest</b> auf den "
                        "Wiederholungsmitteln.",
                        "Verlangt werden <b>beide</b> Bedingungen: mittlere "
                        "Differenz zugunsten des Verfahrens <b>und</b> "
                        "signifikanter Test.",
                        "Ein positiver Mittelwert allein wäre zu wenig.",
                    ], eng=True), abstand=9)),
                stapel(
                    kasten("Warum die Regel nötig ist", absatz(
                        "Für ein Modell, das seine Baseline nicht schlägt, "
                        "<b>erklärt man Rauschen</b>. Eine Abbildung, die "
                        "Beiträge zeigt, wo kein Signal ist, ist schlimmer als "
                        "keine Abbildung.")),
                    kasten("Warum das nicht nach Rosinenpicken aussieht",
                           absatz(
                        "Die übersprungenen Modelle stehen <b>mit Begründung</b> "
                        "in <code>uebersprungen.csv</code>. Die Auswahl ist "
                        "damit nachvollziehbar statt selektiv.")),
                    abstand=13)),
            reihe(
                block("ruhigster_fold(folds)", punkte([
                    "Gerechnet wird auf <b>einem</b> Fold — dem mit dem "
                    "geringsten Extrapolationsanteil in Wiederholung 0.",
                    "Dort liegen die wenigsten Testzeilen außerhalb des "
                    "gelernten Wertebereichs; die Beiträge beruhen also am "
                    "ehesten auf <b>Interpolation</b>.",
                ], eng=True)),
                kasten("Die Folge im Mengenstrang", absatz(
                    "Dort bleibt der Attributionsblock <b>leer</b> — kein "
                    "Vergleichsverfahren schlägt das Poisson-GLM. Das ist ein "
                    "Ergebnis, kein Fehler. Die Antwort auf UF1 kommt deshalb "
                    "aus <code>faktorgruppen_baseline()</code>."))),
            abstand=14), label="Regel")

D.folie("modelle/m04_shap.py", "_beitraege(modell, X, name)",
        "Wie die Beiträge je Merkmal entstehen — und warum XGBoost einen "
        "eigenen Weg geht.",
        stapel(
            einaus("gefittetes Modell, Merkmalsmatrix, Verfahrensname",
                   "Reihe Merkmal → mittlerer absoluter Beitrag"),
            reihe(
                block("Drei Wege, ein Ergebnis", punkte([
                    "<b>Ridge und GLM:</b> standardisierte Koeffizienten — der "
                    "direkte Gegenwert zu SHAP-Beiträgen, kein Explainer nötig.",
                    "<b>Random Forest:</b> <code>TreeExplainer</code>, exakt "
                    "statt approximiert.",
                    "<b>XGBoost:</b> <code>pred_contribs=True</code> — XGBoost "
                    "bringt TreeSHAP selbst mit.",
                    "Mehrklassige Ausgaben werden über die Klassen gemittelt: "
                    "Die Frage lautet „welche Faktorgruppe trägt“, nicht „für "
                    "welche Klasse“.",
                ])),
                kasten("Warum der Umweg bei XGBoost", absatz(
                    "<code>shap.TreeExplainer</code> kann den mehrklassigen "
                    "<code>base_score</code> von XGBoost 3.x nicht lesen und "
                    "bricht mit <code>could not convert string to float</code> "
                    "ab. <code>pred_contribs=True</code> liefert <b>exakt "
                    "dieselben Werte</b>, gerechnet vom selben Algorithmus — "
                    "kein Näherungsverfahren, nur ein anderer Aufrufweg."))),
            kasten("Der Fallstrick beim Deuten", absatz(
                "Die Strukturmerkmale sind untereinander korreliert. SHAP "
                "verteilt den Beitrag dann auf mehrere Merkmale, und einzelne "
                "Werte sind nicht sinnvoll deutbar — "
                "„<code>median_haushaltseinkommen</code> trägt 8 %“ wäre "
                "<b>Scheinpräzision</b>. Deshalb wird auf die drei "
                "Faktorgruppen des Exposés verdichtet; Größenkontrolle und "
                "Saison werden getrennt ausgewiesen."), "warn"),
            abstand=13))

D.folie("modelle/m04_shap.py", "Die beiden Ablationen", None,
        stapel(
            reihe(
                block("ablation_exposition(...)", stapel(
                    einaus("Panel, tuning.csv des Mengenstrangs",
                           "ablation_exposition.csv"),
                    punkte([
                        "Entfernt <b>einen</b> Baustein: Die Baumverfahren "
                        "passen direkt auf <code>anzahl_einsaetze</code> an "
                        "statt über die Rate.",
                        "Gemessen: ohne Expositionsbehandlung <b>RF 67,7</b> "
                        "und <b>XGBoost 61,7</b> RMSE — mit ihr <b>36,4</b> und "
                        "<b>35,7</b>.",
                        "Der Spezifikationsunterschied ist damit ein "
                        "<b>Vielfaches</b> des Verfahrensunterschieds.",
                    ], eng=True), abstand=9)),
                block("ablation_faktorgruppen(...)", stapel(
                    einaus("beide Panels, tuning.csv beider Stränge",
                           "ablation_faktorgruppen.csv"),
                    punkte([
                        "Jede Faktorgruppe wird einmal weggelassen, alles "
                        "andere bleibt gleich.",
                        "<b>Mengenstrang:</b> abladiert wird das Poisson-GLM — "
                        "es ist das beste Modell des Strangs und hat keinen "
                        "Hyperparameter. Dann ändert sich ausschließlich die "
                        "Merkmalsmenge.",
                        "<b>Strukturstrang:</b> RF und XGBoost, die beide die "
                        "Baseline schlagen.",
                    ], eng=True), abstand=9))),
            reihe(
                kasten("Die Einschränkung, die berichtet werden muss", absatz(
                    "Bei den Baumverfahren stammen die Hyperparameter aus dem "
                    "<b>vollen</b> Merkmalssatz und werden nicht neu gesucht — "
                    "sonst änderte sich zweierlei gleichzeitig. Die gemessene "
                    "Verschlechterung enthält dadurch einen Anteil, der auf "
                    "eine nicht mehr passende Einstellung entfällt."), "warn"),
                kasten("_ablation_auswerten(roh)", absatz(
                    "Dreht das <b>Vorzeichen</b> so, dass ein positiver Wert "
                    "immer „Verschlechterung durch Weglassen“ heißt — bei RMSE "
                    "ist klein besser, bei Macro-F1 groß. Ohne diese Drehung "
                    "liest man eine der beiden Tabellen genau falsch herum."))),
            abstand=14), label="Funktionen")

D.folie("modelle/m04_shap.py", "Die drei übrigen Auswertungen", None,
        stapel(
            reihe(
                block("faktorgruppen_baseline(...)", punkte([
                    "Beantwortet UF1 für den Mengenstrang — <b>aus der "
                    "Baseline</b>, weil dort jedes Vergleichsverfahren "
                    "übersprungen wird.",
                    "Das ist kein Notbehelf: Das beste Modell des Strangs "
                    "<b>ist</b> das Poisson-GLM.",
                    "Vergleichbar gemacht über |Koeffizient| × "
                    "Standardabweichung — sonst hinge die Größe an der Einheit "
                    "(Einkommen in Dollar bekäme einen winzigen Koeffizienten).",
                ], eng=True)),
                block("_vif(panel)", punkte([
                    "Zwei Bezugsmengen, und beide werden ausgewiesen.",
                    "Absicht war, jede Merkmalskombination <b>einmal</b> zu "
                    "zählen. Ein <code>drop_duplicates()</code> leistet das "
                    "nicht: Seit der Kriminalitätsindex monatlich rolliert, "
                    "sind 3 757 von 3 828 Zeilen eindeutig.",
                    "<code>stadtteil_jahr</code> ist die Ebene, auf der die "
                    "Merkmale tatsächlich variieren — <b>diese</b> Zahl gehört "
                    "in den Text.",
                ], eng=True))),
            reihe(
                block("extrapolation_aufschluesseln(...)", punkte([
                    "Macht aus einer Plausibilitätsaussage eine Zahl: je "
                    "Merkmal, je Stadtteil, und der Spearman-Zusammenhang "
                    "zwischen Extrapolationsanteil und Fehler.",
                ], eng=True)),
                kasten("Die Abgrenzung, auf die es ankommt", absatz(
                    "Verboten wäre, die <b>Testmenge</b> nach Extrapolationsgrad "
                    "zu schneiden und darin nach Verfahrensunterschieden zu "
                    "suchen — das wäre ein nachträglicher Zuschnitt der "
                    "Auswertung. Hier bleibt die Einheit der <b>Fold</b>, und "
                    "die Frage lautet, warum Folds unterschiedlich schwer sind. "
                    "Die Primäraussage bleibt unberührt."))),
            abstand=14), label="Funktionen")

# ------------------------------------------------------------------ m05
D.kapitel("m05_abbildungen.py",
          "Zehn Abbildungen für Kapitel 7. Dieses Skript rechnet nichts — "
          "es liest nur.",
          [("1 103", "Zeilen"), ("21", "Funktionen"), ("10", "Abbildungen")])

D.folie("modelle/m05_abbildungen.py", "Warum gepaart dargestellt wird",
        "Der Neuschnitt vom 07.08.2026 — und der messbare Grund dafür.",
        stapel(
            reihe(
                kasten("Was der erste Satz zeigte", punkte([
                    "Boxplots der <b>Rohwerte</b> je Verfahren.",
                    "Die 50 Läufe unterscheiden sich darin, <i>welche</i> "
                    "Stadtteile im Testfold liegen.",
                    "Bayview hat ein Vielfaches der Einsätze von Seacliff — der "
                    "RMSE schwankt zwischen <b>13 und 76</b>, unabhängig vom "
                    "Verfahren.",
                    "Streuung der Rohwerte: 12,4 bis 15,5. "
                    "Verfahrensunterschied: <b>rund 2</b>.",
                ]), "warn"),
                block("Was der Neuschnitt zeigt", punkte([
                    "Jedes Verfahren sieht <b>dieselben</b> Folds.",
                    "Bildet man die Differenz je Lauf, kürzt sich die "
                    "Fold-Streuung heraus.",
                    "Streuung der gepaarten Differenz über die zehn "
                    "Wiederholungsmittel: <b>2,4 bis 4,3</b>.",
                    "Es ist <b>dieselbe Paarung</b>, auf der auch der "
                    "Wilcoxon-Test beruht — die Abbildung zeigt die getestete "
                    "Größe.",
                ]))),
            kasten("Der zweite, einfachere Fehler des alten Satzes", absatz(
                "Balken ab null, während sich alles zwischen 33,98 und 36,51 "
                "abspielte. Die Unterschiede lagen in den obersten sechs "
                "Prozent der Bildhöhe.")),
            abstand=17),
        label="Datei")

D.folie("modelle/m05_abbildungen.py", "Die zehn Abbildungen", None,
        stapel(
            tabelle(["", "Abbildung", "Zeigt", "Beantwortet"], [
                [("A1", "fn"), "gegen_baseline",
                 "gepaarte Differenz zur Stufe-2-Baseline, ein Punkt je "
                 "Wiederholung", "Primäraussage"],
                [("A2", "fn"), "foldstruktur",
                 "Rohwerte je Fold — die Streuung stammt aus dem Fold",
                 "Begründung für A1"],
                [("A3", "fn"), "spezifikation",
                 "Verfahrenswahl gegen Spezifikationswahl", "UF4"],
                [("A4", "fn"), "laufzeit_guete",
                 "einkernige Trainingszeit gegen Güte", "UF3"],
                [("A5", "fn"), "holdout", "die einmalige Schlussbewertung",
                 "Validierung"],
                [("A6", "fn"), "faktorgruppen",
                 "Anteil je Faktorgruppe, gestapelt auf 100 %", "UF1"],
                [("A7", "fn"), "extrapolation",
                 "Extrapolationsanteil gegen Fehler, mit Spearman-ρ",
                 "Limitation"],
                [("A8", "fn"), "hyperparameter",
                 "Lage der fünf Fold-Parametersätze im Suchraum", "Kapitel 8"],
                [("A9", "fn"), "parallelisierung",
                 "Faktor, um den das parallele Fitten schneller wird",
                 "UF3, zweite Hälfte"],
                [("A10", "fn"), "qq_residuen",
                 "QQ-Diagramm der Residuen", "Auflage 10.08."],
            ], eng=True),
            abstand=14), label="Übersicht")

D.folie("modelle/m05_abbildungen.py", "Anforderungen an die Darstellung",
        "Gestaltung war im Gutachten ein eigenes Bewertungskriterium.",
        stapel(
            reihe(
                block("Satz und Format", punkte([
                    "<b>PDF, nicht PNG</b> — Rasterbilder werden im Druck "
                    "unscharf.",
                    "In der <b>Endgröße</b> erzeugen, nicht in LaTeX "
                    "schrumpfen: sonst steht dort 5-pt-Schrift. Mindestens 9 pt.",
                    "<b>Keine Titel</b> in der Abbildung — die Bildunterschrift "
                    "in LaTeX ist der Titel.",
                    "Verfahren zusätzlich über <b>Schraffur und Marker</b> "
                    "unterscheiden, nicht allein über Farbe.",
                ])),
                block("Inhaltliche Pflichten", punkte([
                    "Achsen mit Einheit und deutschem <b>Dezimalkomma</b>.",
                    "Bei Differenzen und R² die <b>Nulllinie</b> einzeichnen — "
                    "das Vorzeichen ist die Aussage.",
                    "An jeder Differenzachse muss stehen, <b>welche Seite "
                    "besser ist</b>: bei RMSE links, bei Macro-F1 rechts.",
                    "Bei jeder Streuung benennen, <b>worüber</b> sie gebildet "
                    "ist — zehn Wiederholungsmittel, nicht 50 Einzelläufe.",
                ]))),
            kasten("Die Prüffrage, die am ehesten gestellt wird", absatz(
                "<b>„Schneidet in A1 die Nulllinie eine der Boxen?“</b> — Dann "
                "darf im Text <b>kein</b> Unterschied zur Baseline behauptet "
                "werden, den der Test nicht deckt. Die Abbildung und der Text "
                "müssen dieselbe Aussage tragen."), "frage"),
            abstand=15), label="Vorgaben")

D.folie("modelle/m05_abbildungen.py", "Die Hilfsfunktionen",
        "Formatierung, die inhaltlich trägt.",
        stapel(
            reihe(
                block("_komma(...) · _prozent(...) · _sekunden(wert)", punkte([
                    "<code>stellen</code> ist <b>nicht kosmetisch</b>: Macro-F1 "
                    "liegt zwischen 0,328 und 0,334 — mit zwei "
                    "Nachkommastellen stünde an allen Achsenmarken „0,33“.",
                    "<code>vorzeichen</code> setzt auf Differenzachsen ein "
                    "explizites Plus; sonst liest sich „2,5“ wie ein "
                    "Absolutwert statt wie ein Abstand.",
                    "<code>_sekunden</code>: zwei Nachkommastellen reichen für "
                    "die Ensembles (5,83 s), nicht für Ridge (0,011 s).",
                ], eng=True)),
                block("_gepaarte_differenz()", punkte([
                    "Liefert je Verfahren die zehn Wiederholungsmittel der "
                    "Differenz zur Baseline.",
                    "Gepaart wird auf <code>(wiederholung, fold)</code> — also "
                    "auf <b>identischen Testzeilen</b>.",
                    "Damit zeigt die Abbildung die <i>getestete</i> Größe und "
                    "nicht eine andere, die zufällig ähnlich aussieht.",
                ], eng=True))),
            reihe(
                block("_lage_im_suchraum(name, parameter, wert)", punkte([
                    "Normiert einen gefundenen Wert auf 0…1 in <b>seinem</b> "
                    "Suchraum — <code>alpha</code> läuft über sechs "
                    "Zehnerpotenzen, <code>subsample</code> über 0,4 Einheiten.",
                    "Fällt ein Wert aus seinem Raum, gibt es "
                    "<code>None</code>: dann hat sich der Suchraum seit dem "
                    "Lauf geändert, und die Zeile <b>fehlt</b>, statt eine "
                    "falsche Lage vorzutäuschen.",
                ], eng=True)),
                block("_hyperparameter_lagen() · _faktorgruppen_balken()",
                      punkte([
                    "In der Regression steht jeder Suchlauf <b>zweimal</b> in "
                    "tuning.csv — gesucht wurde aber nur einmal, auf der Rate. "
                    "Ohne Entdopplung stünden zehn statt fünf Punkte je "
                    "Parameter.",
                    "<code>_faktorgruppen_balken</code> hält zusammen, dass der "
                    "Mengenbalken <b>Koeffizienten</b> und die Strukturbalken "
                    "<b>SHAP-Werte</b> zeigen — daher die Fußzeile in A6.",
                ], eng=True))),
            abstand=14))

D.folie(P, "Was man über diesen Ordner wissen muss", None,
        stapel(
            reihe(
                kasten("1 · Ein Lauf, eine Spezifikation", absatz(
                    "Nichts wird wiederverwendet, kein Zwischenstand "
                    "eingelesen. <code>tuning.csv</code> ist ein <b>Ergebnis</b> "
                    "dieses Laufs, kein Eingang. Wer die Spezifikation ändert, "
                    "rechnet neu.")),
                kasten("2 · Getestet wird gegen Stufe 2", absatz(
                    "Nicht gegen die triviale Referenz. Der Primärtest läuft "
                    "auf zehn Wiederholungsmitteln, gepaart, mit Effektgröße "
                    "und Intervall daneben. Die 50 Einzelläufe laufen als "
                    "gekennzeichnete Sensitivität mit.")),
                kasten("3 · Interpretiert wird nur, was gewinnt", absatz(
                    "SHAP nur für Modelle, die ihre Baseline schlagen — sonst "
                    "erklärt man Rauschen. Im Mengenstrang bleibt dieser Block "
                    "leer, und die Antwort auf UF1 kommt aus den Koeffizienten "
                    "des Poisson-GLM."))),
            kasten("Der Satz, mit dem man den Ordner verteidigt", absatz(
                "<b>„Der Vergleich misst die Verfahren, nicht die "
                "Modellierungsentscheidungen — deshalb sehen alle Modelle "
                "dieselben Merkmale, dieselben Folds und dieselbe "
                "Expositionsbehandlung.“</b> Wo das einmal nicht galt, ist es "
                "als Fehler in der Spezifikation korrigiert und dokumentiert "
                "worden, nicht als Ergebnis über die Verfahren berichtet.")),
            abstand=20),
        label="Zusammenfassung")

if __name__ == "__main__":
    schreibe(D, "modelle/ · Verfahrensvergleich",
             pathlib.Path("/home/claude/out/folien/03_modelle.pdf"))
