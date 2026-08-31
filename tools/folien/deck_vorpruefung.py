"""Foliensatz 02 - vorpruefung/ (Messlatte und Eignung)."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from renderer import (Deck, absatz, block, einaus, fluss, kasten, kpi, punkte,
                      reihe, schreibe, stapel, tabelle)

D = Deck(
    "vorpruefung",
    "vorpruefung/ — die Messlatte und ihre Berechtigung",
    "Bevor ein Vergleichsverfahren antritt, muss feststehen, was es schlagen "
    "soll und warum es überhaupt antreten darf. Sechs Dateien beantworten das.",
    [("Bachelorarbeit", "SFFD-Einsatzprognose"),
     ("Phase", "CRISP-DM 4 — Vorstufe"),
     ("Ergebnis", "Baselines · Eignungsprüfung · Obergrenzen")])

P = "vorpruefung"

# ------------------------------------------------------------------ Ordner
D.folie(P, "Was dieser Ordner leistet",
        "Er beantwortet drei Fragen, die vor jedem Verfahrensvergleich "
        "geklärt sein müssen.",
        stapel(
            reihe(
                block("1 · Woran wird gemessen?", stapel(
                    absatz("Ein Ergebnis ohne Bezugspunkt ist keines. "
                           "<code>v1_baselines.py</code> baut zwei Messlatten "
                           "und lässt sie unter demselben Protokoll mitlaufen "
                           "wie die Modelle."),
                    punkte(["Stufe 1 — trivial, ohne jedes Merkmal",
                            "Stufe 2 — die einfachste zur Datenform passende Form"],
                           eng=True), abstand=10)),
                block("2 · Dürfen die Verfahren antreten?", stapel(
                    absatz("<code>v2_eignung.py</code> prüft, ob die "
                           "Datenform die gewählten Verfahren überhaupt "
                           "rechtfertigt — mit formalen Tests statt Augenmaß."),
                    punkte(["sechs Belege, davon einer als Auflage Schröter",
                            "Anforderungen je Verfahren mit p-Wert"],
                           eng=True), abstand=10)),
                block("3 · Was ist überhaupt erreichbar?", stapel(
                    absatz("<code>v3</code> und <code>v4</code> stecken die "
                           "Grenzen ab: Wie viel Struktur überträgt sich, und "
                           "wo liegt die Obergrenze der Klassifikation?"),
                    punkte(["v3 — hält die Nichtlinearität out-of-sample?",
                            "v4 — zwei Decken über dem Strukturstrang"],
                           eng=True), abstand=10))),
            kasten("Warum das eine eigene Stufe ist", absatz(
                "Ohne diesen Ordner wäre jeder Vergleich eine Rangliste ohne "
                "Maßstab. Erst die Messlatte macht aus „Random Forest erreicht "
                "RMSE 36,4“ eine Aussage — nämlich, ob sich der Mehraufwand "
                "gegenüber einem GLM gelohnt hat.")),
            abstand=20),
        label="Ordner")

D.folie(P, "Die sechs Dateien", None,
        stapel(
            tabelle(["Datei", "Beantwortet", "Erzeugt", "Läuft über"], [
                [("run.py", "fn"), "Startet die zwei Pflichtschritte",
                 "nichts eigenes", ("run.py", "fn")],
                [("v0_aufteilung.py", "fn"),
                 "Wie entstehen die 10 Wiederholungen?",
                 "keine Datei — liefert Datenrahmen", ("einzeln", "z")],
                [("v1_baselines.py", "fn"), "Was müssen die Verfahren schlagen?",
                 "baselines_*.csv", ("run.py", "fn")],
                [("v2_eignung.py", "fn"), "Passen die Verfahren zur Datenform?",
                 "eignungspruefung.md, annahmen.csv", ("run.py", "fn")],
                [("v3_spezifikation.py", "fn"),
                 "Überträgt sich die Nichtlinearität?",
                 "spezifikation_*.csv", ("einzeln", "z")],
                [("v4_decke.py", "fn"), "Wie gut kann die Klassifikation werden?",
                 "decke*.csv, decke.md", ("einzeln", "z")],
            ], eng=True),
            kasten("Reihenfolge", absatz(
                "<code>python vorpruefung/run.py</code> fährt v1 und dann v2 — "
                "zwingend in dieser Reihenfolge, weil die Eignungsprüfung die "
                "Baseline-Werte <b>liest</b> statt sie neu zu rechnen. "
                "<code>v0</code> ist ein Selbsttest, <code>v3</code> und "
                "<code>v4</code> sind eigenständige Zusatzbelege.")),
            abstand=18),
        label="Ordner")

D.folie(P, "Das Stufenmodell der Baselines",
        "Die zentrale Begriffsordnung des gesamten Projekts.",
        stapel(
            fluss([
                {"nr": "Stufe 1", "titel": "Triviale Referenz",
                 "text": "Benutzt KEIN Merkmal. Gesamtmittelwert bzw. häufigste "
                         "Klasse. Frage: steckt in den Merkmalen überhaupt "
                         "Information?"},
                {"nr": "Stufe 2", "titel": "Einfachste passende Form", "akt": True,
                 "text": "Benutzt alle Merkmale, aber in der simpelsten Form. "
                         "Poisson-GLM bzw. multinomiales Logit. Kein freier "
                         "Hyperparameter."},
                {"nr": "Stufe 3", "titel": "Vergleichsverfahren",
                 "text": "Ridge, Random Forest, XGBoost in modelle/. Sie müssen "
                         "STUFE 2 schlagen, nicht Stufe 1."},
            ]),
            reihe(
                kasten("Die Regel, die daraus folgt", absatz(
                    "Wer einen freien Parameter hat, wird mit demselben Budget "
                    "getunt. Wer keinen hat, wird angepasst. <b>Kein Modell "
                    "läuft mit einer unbegründeten Voreinstellung.</b>")),
                kasten("Der häufigste Lesefehler", absatz(
                    "Stufe 2 ist im Mengenstrang ein <b>Poisson-GLM</b>, im "
                    "Strukturstrang ein <b>multinomiales Logit</b>. Das sind "
                    "zwei verschiedene Modelle. „Die Baseline gewinnt“ gilt "
                    "immer nur für einen der beiden Stränge."), "warn")),
            abstand=20),
        label="Konzept")

# ------------------------------------------------------------------ v0
D.kapitel("v0_aufteilung.py",
          "Die eine Stelle, an der die Fold-Zuteilung je Wiederholung "
          "entsteht. Ohne sie wären die zehn Wiederholungen wertlos.",
          [("201", "Zeilen"), ("4", "Funktionen"), ("10", "Wiederholungen")])

D.folie("vorpruefung/v0_aufteilung.py", "Warum diese Datei existiert",
        "Die Grundaufteilung steht bereits als Spalte in der Parquet-Datei — "
        "sie reicht für die Wiederholungen trotzdem nicht.",
        stapel(
            reihe(
                kasten("Der naheliegende Weg — und warum er scheitert", punkte([
                    "Ein <b>Versatz</b> würde die Stadtteile reihum um n "
                    "Plätze verschieben.",
                    "Zwei Stadtteile landen aber genau dann in derselben "
                    "Gruppe, wenn ihre Plätze modulo 6 übereinstimmen — "
                    "<b>unabhängig vom Versatz</b>.",
                    "Der Versatz rotiert also nur die <i>Beschriftung</i> der "
                    "Gruppen, nicht ihre Zusammensetzung.",
                ]), "warn"),
                block("Der zweite, schlimmere Fehler", punkte([
                    "Rotiert die Beschriftung, rotiert auch Gruppe 0 — "
                    "und <b>Gruppe 0 ist das Hold-out</b>.",
                    "Gemessen: bei Versatz 1 liegt kein einziger der sechs "
                    "ursprünglichen Hold-out-Stadtteile mehr im Hold-out.",
                    "Die Wiederholungen 1 bis 9 hätten auf genau den "
                    "Stadtteilen trainiert, die unberührt bleiben müssen.",
                ]))),
            kasten("Die Lösung", absatz(
                "Gemischt wird <b>innerhalb der Rangblöcke</b>: Block 0 sind "
                "die Plätze 0–4, Block 1 die Plätze 5–9 und so fort. Jeder Fold "
                "bekommt weiterhin genau einen Stadtteil aus jedem Block — die "
                "doppelte Stratifizierung überlebt, die Foldgrößen bleiben "
                "6/6/6/6/6, aber die <b>Zusammensetzung</b> ändert sich "
                "wirklich.")),
            abstand=16),
        label="Datei")

D.folie("vorpruefung/v0_aufteilung.py", "Die vier Funktionen", None,
        stapel(
            reihe(
                block("selten_je_stadtteil(klassifikation)", stapel(
                    einaus("klassifikation.parquet",
                           "Reihe Stadtteil → Zahl brand-dominierter Monate"),
                    punkte([
                        "Das Stratifizierungsmaß. Steht in keiner Datei, "
                        "deshalb muss auch der Regressionsstrang die "
                        "Klassifikationsdatei mitlesen.",
                        "<b>Kein Leakage:</b> geht in kein Modell ein, "
                        "bestimmt nur die Testgruppen.",
                    ], eng=True), abstand=9)),
                block("wiederholte_aufteilung(daten, wiederholung, selten)",
                      stapel(
                    einaus("Datenrahmen, Wiederholung 0–9, das "
                           "Stratifizierungsmaß",
                           "Kopie mit neu belegter fold-Spalte"),
                    punkte([
                        "Wiederholung 0 reproduziert die Datei <b>bitgenau</b> "
                        "— geprüft per <code>assert</code>, nicht behauptet.",
                        "Hold-out-Zeilen behalten <code>fold = 0</code> und "
                        "bleiben in jeder Wiederholung ausgeschlossen.",
                    ], eng=True), abstand=9))),
            reihe(
                block("entwicklung_und_holdout(daten)", punkte([
                    "Liefert zwei Masken: 30 Entwicklungs- gegen 6 "
                    "Hold-out-Stadtteile.",
                    "Die <b>einzige</b> Stelle im Repository, die "
                    "<code>ist_holdout == 1</code> auswertet.",
                ], eng=True)),
                block("_selbsttest()", punkte([
                    "Prüft Foldgrößen, mindestens einen Brand-Testfall je "
                    "Fold, unverändertes Hold-out, zehn verschiedene "
                    "Partitionen ohne Dubletten.",
                    "Und: kein Stadtteil je zugleich Trainings- und Testfall.",
                ], eng=True))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Warum brauchen Sie zehn Wiederholungen?“</b> — Bei 29 "
                "Entwicklungsstadtteilen schwankt ein einzelner Fold stark. "
                "Die Wiederholungen dienen der <b>Streuungsschätzung</b>, nicht "
                "der Modellwahl. Alle drei Aufrufer müssen dieselbe Zuteilung "
                "sehen, sonst testet der gepaarte Wilcoxon still auf "
                "verschiedenen Zeilen."), "frage"),
            abstand=14), label="Funktionen")

# ------------------------------------------------------------------ v1
D.kapitel("v1_baselines.py",
          "Die Messlatte. Zwei Stufen, beide Stränge, unter demselben "
          "Protokoll wie die Vergleichsverfahren.",
          [("330", "Zeilen"), ("8", "Funktionen"), ("250", "Läufe")])

D.folie("vorpruefung/v1_baselines.py", "Warum die Baseline mitläuft statt "
        "nur zu existieren",
        "Eine Ergänzung, die den Charakter der Datei geändert hat.",
        stapel(
            reihe(
                block("Vorher", punkte([
                    "Es lief nur die Aufteilung, die als <code>fold</code>-Spalte "
                    "in der Datei steht — also <b>fünf</b> Läufe.",
                    "Die Vergleichsverfahren erzeugen <b>fünfzig</b>.",
                    "Für 45 der 50 Läufe gab es keinen Gegenwert.",
                ])),
                kasten("Nachher", punkte([
                    "Die Baseline läuft über alle 10 Wiederholungen × 5 Folds.",
                    "Die Primäraussage ist ein <b>gepaarter</b> Test „Verfahren "
                    "gegen Stufe 2“ — der braucht je Lauf einen Gegenwert auf "
                    "<b>denselben</b> Testzeilen.",
                    "Damit ist die Baseline ein <b>Mitbewerber unter "
                    "identischem Protokoll</b>, kein Referenzwert.",
                ]))),
            reihe(
                kpi([("250", "Baseline-Läufe"), ("200", "Zeilen Regression"),
                     ("wenige s", "Rechenzeit"), ("Auflage C", "Schröter: gleiche "
                                                  "Merkmale und Splits")])),
            abstand=18),
        label="Datei")

D.folie("vorpruefung/v1_baselines.py", "poisson_glm(train, test, merkmale)",
        "Stufe 2 des Mengenstrangs — das Modell, das am Ende kein "
        "Vergleichsverfahren schlägt.",
        stapel(
            einaus("Trainings- und Testrahmen, optional ein reduzierter "
                   "Merkmalssatz",
                   "Vorhersagen auf der Originalskala, eine Zahl je "
                   "Stadtteil-Monat"),
            reihe(
                block("Die Spezifikation", punkte([
                    "Poisson-GLM mit kanonischem <code>log</code>-Link, "
                    "unpenalisierte Maximum-Likelihood.",
                    "<code>log(Bevölkerung)</code> geht als <b>Offset</b> ein — "
                    "Koeffizient fest auf 1.",
                    "Das Modell schätzt Einsätze <b>je Einwohner</b> und "
                    "rechnet am Ende hoch.",
                    "Ohne den Offset sagt es vor allem die Stadtteilgröße "
                    "vorher.",
                ])),
                kasten("Warum Poisson und nicht Negative Binomial", punkte([
                    "Die Daten sind überdispers (Index 62,8), die Annahme "
                    "<code>Var = μ</code> also verletzt.",
                    "Beschädigt werden dadurch die <b>Standardfehler</b>, nicht "
                    "die Konsistenz des bedingten Mittelwerts "
                    "(Gourieroux, Monfort & Trognon 1984).",
                    "Diese Baseline liefert <b>nur Punktvorhersagen</b> — keine "
                    "Koeffiziententests, keine Konfidenzintervalle. Sie ist "
                    "davon nicht betroffen.",
                ]))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Wo kommt die zweite Zielgröße her?“</b> — Aus derselben "
                "Anpassung, geteilt durch die Bevölkerung. Ein eigenes "
                "Ratenmodell wäre eine <i>zweite Spezifikation</i> und damit "
                "unfair gegenüber den Vergleichsverfahren."), "frage"),
            abstand=13))

D.folie("vorpruefung/v1_baselines.py", "logit_glm(train, merkmale)",
        "Stufe 2 des Strukturstrangs — und ein behobener Konstruktionsfehler.",
        stapel(
            einaus("Trainingsrahmen, optional ein reduzierter Merkmalssatz",
                   "das angepasste MODELL, nicht die Vorhersage"),
            reihe(
                block("Die Spezifikation", punkte([
                    "Multinomiales Logit, linear in den Log-Odds, "
                    "unpenalisiert (<code>C = ∞</code>).",
                    "Kein freier Hyperparameter, also kein Tuning.",
                    "<code>class_weight=\"balanced\"</code> statt Resampling — "
                    "<b>kein SMOTE</b>, keine duplizierte oder gelöschte Zeile.",
                ])),
                block("Warum das Modell zurückkommt", punkte([
                    "Beide Aufrufer brauchen aus <b>derselben</b> Anpassung "
                    "drei Dinge: Klassenvorhersage, Wahrscheinlichkeiten und "
                    "die Klassenreihenfolge.",
                    "Ein zweites Fitten dafür wäre Verschwendung.",
                    "Konvergenzwarnungen werden <b>nicht</b> abgefangen — der "
                    "Aufrufer zählt und berichtet sie.",
                ]))),
            kasten("Der behobene Fehler — eine Asymmetrie", absatz(
                "Bis zum 10.08.2026 baute <code>m03_struktur.hold_out()</code> "
                "dieses Modell ein <b>zweites Mal</b> nach: dieselben vier "
                "Argumente, an zwei Orten aufgeschrieben. Ändert jemand eines "
                "davon, misst die Kreuzvalidierung still gegen ein anderes "
                "Modell als die Schlussbewertung — und keine Prüfung schlägt "
                "an. Der Mengenstrang war immer richtig gebaut; der Fehler war "
                "die Asymmetrie zwischen beiden."), "warn"),
            abstand=13))

D.folie("vorpruefung/v1_baselines.py", "Die Auswertungsfunktionen",
        "Wie aus 250 Läufen berichtbare Zahlen werden.",
        stapel(
            reihe(
                block("regression(panel, selten) · klassifikation(kl, selten)",
                      stapel(
                    einaus("das jeweilige Panel und das Stratifizierungsmaß",
                           "Datenrahmen der Einzelläufe"),
                    punkte([
                        "Regression: 200 Zeilen — 50 Läufe × 2 Zielgrößen × "
                        "2 Modelle.",
                        "Klassifikation: Stufe 1 sagt immer die häufigste "
                        "Klasse. <b>Accuracy hoch, Macro-F1 niedrig</b> — genau "
                        "deshalb ist Macro-F1 maßgeblich.",
                    ], eng=True), abstand=9)),
                block("_zweistufig(df, schluessel, masse)", stapel(
                    einaus("Datenrahmen der Einzelläufe",
                           "je Modell eine Zeile mit beiden Streuungen"),
                    punkte([
                        "<b>Stufe 1:</b> je Wiederholung über die 5 Folds "
                        "mitteln.",
                        "<b>Stufe 2:</b> die Streuung <i>dieser zehn</i> Werte "
                        "berichten.",
                        "<code>std_folds</code> über alle 50 Läufe ist zu "
                        "optimistisch — es sind dieselben 30 Stadtteile in zehn "
                        "Gruppierungen.",
                    ], eng=True), abstand=9))),
            reihe(
                block("bewerte_regression(y_true, y_pred)", punkte([
                    "RMSE, MAE und R² — immer auf der <b>Originalskala</b> der "
                    "Zielgröße.",
                ], eng=True)),
                kasten("_macro_auroc(...)", punkte([
                    "Liefert <code>NaN</code>, wenn im Testfold eine Klasse "
                    "fehlt — <b>nicht 0,5 und nicht 0</b>.",
                    "Ein erfundener Wert zöge den Mittelwert nach unten und "
                    "sähe wie ein Messergebnis aus.",
                ], eng=True))),
            abstand=14), label="Funktionen")

# ------------------------------------------------------------------ v2
D.kapitel("v2_eignung.py",
          "Sechs Belege, dass die gewählten Verfahren zur Datenform passen. "
          "Abschnitt 6 ist eine ausdrückliche Auflage des Betreuers.",
          [("667", "Zeilen"), ("12", "Funktionen"), ("6", "Belege")])

D.folie("vorpruefung/v2_eignung.py", "Die sechs Belege im Überblick", None,
        stapel(
            tabelle(["#", "Funktion", "Befund", "Konsequenz"], [
                [("1", "z"), ("dispersion()", "fn"),
                 "Zähldaten sind überdispers (Index 62,8)",
                 "zähldatengerechte Verlustfunktionen"],
                [("2", "z"), ("linearitaet()", "fn"),
                 "Pearson und Spearman klaffen auseinander",
                 "Ridge auf log(1+y), nicht roh"],
                [("3", "z"), ("spezifikation()", "fn"),
                 "RESET verwirft die lineare Form",
                 "Random Forest und XGBoost"],
                [("4", "z"), ("extrapolation()", "fn"),
                 "Teststadtteile liegen oft außerhalb",
                 "Limitation, keine Verfahrensfrage"],
                [("5", "z"), ("klassifikation()", "fn"),
                 "Merkmale trennen auch die Einsatzart",
                 "zweiter Strang gerechtfertigt"],
                [("6", "z"), ("annahmen()", "fn"),
                 "Anforderungen je Verfahren, mit p-Wert",
                 "Auflage Schröter vom 10.08.2026"],
            ], eng=True),
            reihe(
                kasten("Wo gerechnet wird", absatz(
                    "Ausschließlich auf den <b>Trainingsstadtteilen von "
                    "Fold 1</b> — die Teststadtteile dürfen keine "
                    "Modellentscheidung beeinflussen. Ausgenommen sind "
                    "Abschnitt 4 und die aus v1 gelesenen Referenzwerte.")),
                kasten("Was die Prüfung NICHT leistet", absatz(
                    "Sie unterscheidet <b>nicht</b> zwischen Random Forest und "
                    "XGBoost. Welche der beiden Strategien gewinnt, ist die "
                    "empirische Forschungsfrage der Arbeit — vorab nötig ist "
                    "nur, dass beide plausibel sind."), "warn")),
            abstand=16),
        label="Datei")

D.folie("vorpruefung/v2_eignung.py", "dispersion(train)",
        "Beleg 1 — und ein Beispiel dafür, wie Dokumentation von der "
        "Umsetzung wegdriften kann.",
        stapel(
            einaus("Trainingszeilen von Fold 1",
                   "Textabschnitt im Bericht, Dispersionsindex je Zielgröße"),
            reihe(
                block("Die Messung", absatz(
                    "Der Index <code>Varianz / Mittelwert</code> misst, wie "
                    "weit die Zähldaten die Poisson-Annahme "
                    "<code>Var = μ</code> verletzen. Gemessen: <b>62,8</b>.")),
                block("Folge 1 — Verlustfunktion", absatz(
                    "Ein quadratischer Fehler auf rohen Zähldaten ist bei "
                    "diesem Index unangemessen. Daraus folgen "
                    "<code>reg:tweedie</code> für XGBoost und "
                    "<code>criterion=\"poisson\"</code> für den Random Forest.")),
                kasten("Folge 2 — Baseline", absatz(
                    "Beschädigt sind die <b>Standardfehler</b>, nicht die "
                    "Konsistenz des bedingten Mittelwerts. Eine Baseline mit "
                    "reinen Punktvorhersagen ist nicht betroffen — das "
                    "Poisson-GLM bleibt Stufe 2."))),
            kasten("Was hier gelernt wurde", absatz(
                "Bis zum 10.08.2026 schloss dieser Abschnitt aus der "
                "Überdispersion, Poisson scheide aus. Die "
                "Entscheidung dagegen fiel am 06.08. — der Abschnitt "
                "argumentierte danach <b>gegen die eigene Umsetzung</b>, und "
                "der erzeugte Bericht trug den Widerspruch weiter. Kein "
                "Rechenfehler, sondern Drift. Der gemessene Index ist "
                "unverändert; er trägt nur eine andere Schlussfolgerung."),
                "warn"),
            abstand=13))

D.folie("vorpruefung/v2_eignung.py", "Belege 2 bis 5", None,
        stapel(
            reihe(
                block("linearitaet(train)", stapel(
                    einaus("Trainingszeilen von Fold 1",
                           "Textabschnitt und zwei Abbildungen"),
                    punkte([
                        "Pearson misst den <b>linearen</b>, Spearman den "
                        "<b>monotonen</b> Zusammenhang — klaffen sie "
                        "auseinander, ist der Zusammenhang gekrümmt.",
                        "Bewertet wird nur, wo die Korrelation substanziell "
                        "ist; nahe null ist der Abstand Rauschen.",
                        "Auflage R7: erst plotten, dann über lineare Modelle "
                        "reden.",
                    ], eng=True), abstand=9)),
                block("spezifikation(train)", stapel(
                    einaus("Trainingszeilen von Fold 1",
                           "F-Wert, p-Wert, adjustiertes R²"),
                    punkte([
                        "Der RESET-Test prüft, ob Potenzen der Vorhersage noch "
                        "etwas erklären. <b>F = 215,2.</b>",
                        "45 Interaktionsterme heben das adjustierte R² von "
                        "0,805 auf 0,919.",
                        "Beides sind <b>In-Sample</b>-Größen — die Übertragung "
                        "prüft v3.",
                    ], eng=True), abstand=9))),
            reihe(
                block("extrapolation(panel)", punkte([
                    "Anteil der Testzeilen außerhalb der Trainingsspanne.",
                    "Ein hoher Anteil ist eine <b>Limitation der Datenlage</b>, "
                    "keine Verfahrensfrage: Baumverfahren extrapolieren "
                    "grundsätzlich nicht.",
                ], eng=True)),
                block("klassifikation(kl)", punkte([
                    "Kruskal-Wallis je Merkmal über die vier Klassen — "
                    "nichtparametrisch, verträgt ungleich große Gruppen.",
                    "Trennt kein Merkmal, ist die Zielgröße für <b>jedes</b> "
                    "Verfahren nicht vorhersagbar.",
                    "Eigene Frage: Krümmung bei der <i>Anzahl</i> sagt nichts "
                    "über die <i>Art</i>.",
                ], eng=True))),
            abstand=14), label="Funktionen")

D.folie("vorpruefung/v2_eignung.py", "annahmen(train, befunde)",
        "Beleg 6 — die Auflage. Und die Zeilensorte, auf die es ankommt.",
        stapel(
            einaus("Trainingszeilen von Fold 1, Klassifikationspanel",
                   "Textabschnitt, annahmen.csv, qq_residuen.csv"),
            reihe(
                block("Drei Sorten von Zeilen", punkte([
                    "<b>erfüllt</b> — die Anforderung besteht und ist "
                    "eingehalten",
                    "<b>verletzt</b> — sie besteht und ist verletzt; dann steht "
                    "in der Spalte „Konsequenz“, was daraus folgt",
                    "<b>nicht erforderlich</b> — das Verfahren stellt diese "
                    "Anforderung gar nicht",
                ])),
                kasten("Warum die dritte Sorte die wichtigste ist", absatz(
                    "Dass Random Forest keine Verteilungsannahme hat, ist eine "
                    "<b>Aussage über das Verfahren</b> — und der halbe Grund, "
                    "warum es im Vergleich steht. Eine Tabelle, die nur "
                    "verletzte Annahmen zeigt, lässt Baumverfahren "
                    "voraussetzungslos aussehen; eine, die sie ganz weglässt, "
                    "beantwortet die Auflage nicht."))),
            reihe(
                block("Drei neue Tests", punkte([
                    "<b>Cameron & Trivedi (1990)</b> — Hilfsregression auf "
                    "Überdispersion. Der Index aus Abschnitt 1 ist eine "
                    "Kennzahl, <i>kein</i> Test.",
                    "<b>Breusch-Pagan</b> — Varianzgleichheit, wörtlich in der "
                    "Auflage genannt.",
                    "<b>Jarque-Bera</b> — Normalität der Residuen samt Schiefe "
                    "und Wölbung.",
                ], eng=True)),
                kasten("Was hier bewusst NICHT steht", punkte([
                    "Die <b>Multikollinearität</b>. Der VIF wird in "
                    "<code>m04_shap._vif()</code> gerechnet, weil seine einzige "
                    "echte Konsequenz die Interpretation der Beiträge betrifft.",
                    "Ihn hier zu wiederholen hieße, dieselbe Zahl an zwei Orten "
                    "zu führen — genau die Fehlerquelle, die "
                    "<code>tools/pruefe_zahlen.py</code> bewacht.",
                ], eng=True))),
            abstand=13))

D.folie("vorpruefung/v2_eignung.py", "Die Hilfsfunktionen",
        "Formatierung ist hier kein Beiwerk — sie entscheidet über "
        "Maschinenlesbarkeit.",
        stapel(
            reihe(
                block("log(txt) · speichere(fig, name)", punkte([
                    "<code>log()</code> gibt eine Zeile aus <b>und</b> hängt "
                    "sie an den Berichtstext an — der Bericht entsteht "
                    "nebenbei.",
                    "<code>speichere()</code> legt eine Abbildung ab und "
                    "vermerkt sie im Bericht.",
                ], eng=True)),
                block("_z(wert, stellen) · _p(wert)", punkte([
                    "Deutsches Dezimalkomma auf Teststatistik und p-Wert.",
                    "Unter 0,001 wird <b>begrenzt statt beziffert</b>: "
                    "<code>4.0e-47</code> ist keine lesbare Information, die "
                    "Aussage lautet „praktisch null“.",
                    "Bei n = 3 036 findet ein Test fast jede Abweichung — die "
                    "Effektgröße trägt, nicht die Nachkommastelle.",
                ], eng=True))),
            reihe(
                block("Z(...) — eine Zeile der Anforderungstabelle", punkte([
                    "<code>statistik</code> ist die <b>lesbare</b> Fassung mit "
                    "Dezimalkomma, <code>wert</code> dieselbe Zahl "
                    "<b>maschinenlesbar</b>.",
                    "Beides, weil <code>tools/pruefe_zahlen.py</code> den "
                    "Sollwert aus dieser Datei zieht. Eine Zeichenkette, die "
                    "man parst, ist eine Zeichenkette, die sich beim nächsten "
                    "Formatwechsel anders parst.",
                ], eng=True)),
                block("main()", punkte([
                    "Rechnet die sechs Belege und schreibt Bericht, Tabellen "
                    "und Abbildungen.",
                    "Abschnitt 6 <b>übernimmt</b> die Kennzahlen aus 1, 3 und "
                    "4, statt sie neu zu rechnen — zweimal gerechnet hieße zwei "
                    "Zahlen, die auseinanderlaufen können.",
                ], eng=True))),
            abstand=15), label="Funktionen")

# ------------------------------------------------------------------ v3
D.kapitel("v3_spezifikation.py",
          "Hält die diagnostizierte Nichtlinearität auch out-of-sample? "
          "Die Antwort trägt den stärksten Befund der Arbeit.",
          [("242", "Zeilen"), ("6", "Funktionen"), ("200", "Anpassungen")])

D.folie("vorpruefung/v3_spezifikation.py", "Die Frage hinter der Datei",
        "v2 stellt eine Diagnose. Diese Datei prüft, ob sie sich überträgt.",
        stapel(
            reihe(
                kasten("Was v2 zeigt — in-sample", punkte([
                    "RESET verwirft die lineare Spezifikation deutlich "
                    "(<b>F = 215,2</b>).",
                    "45 Interaktionsterme heben das adjustierte R² von 0,805 "
                    "auf 0,919.",
                    "Beide Kennzahlen behandeln 3 828 Zeilen als "
                    "<b>unabhängig</b>.",
                ]), "warn"),
                block("Warum das nicht reicht", punkte([
                    "Tatsächlich liegen <b>30 unabhängige Stadtteile</b> mit je "
                    "132 Monaten vor.",
                    "Ein F-Test mit n = 3 828 findet praktisch jede Abweichung "
                    "signifikant.",
                    "Adjustiertes R² korrigiert für die Zahl der Parameter — "
                    "nicht für die geklumpte Struktur.",
                ]))),
            reihe(
                kasten("Die zwei Fragen, sauber getrennt", stapel(
                    absatz("<b>Diagnose (v2):</b> <i>Steckt</i> in diesen Daten "
                           "Struktur jenseits der Geraden?"),
                    absatz("<b>Übertragung (v3):</b> <i>Überträgt</i> sich "
                           "diese Struktur auf unbekannte Stadtteile?"),
                    abstand=8)),
                kasten("Kein Modellvorschlag", absatz(
                    "Keine der drei Erweiterungen tritt im Verfahrensvergleich "
                    "an. Sie dienen ausschließlich der Interpretation des "
                    "Hauptbefundes."))),
            abstand=18),
        label="Datei")

D.folie("vorpruefung/v3_spezifikation.py", "Die vier Spezifikationen",
        "Grundlage ist immer das Stufe-2-Poisson-GLM — nur die Merkmalsmatrix "
        "wächst.",
        stapel(
            tabelle(["Spezifikation", "Terme", "Zusätzlich enthalten"], [
                [("linear", "fn"), ("12", "z"),
                 "die 10 Prädiktoren + monat_sin + monat_cos"],
                [("quadrate", "fn"), ("22", "z"),
                 "die Quadrate der 10 Prädiktoren"],
                [("interaktionen", "fn"), ("57", "z"),
                 "alle 45 Paarprodukte der Prädiktoren"],
                [("beides", "fn"), ("67", "z"), "Quadrate und Paarprodukte"],
            ]),
            reihe(
                block("Warum die Saisonterme außen vor bleiben", absatz(
                    "<code>monat_sin² + monat_cos² = 1</code> ist exakt "
                    "kollinear mit der Konstanten — das Modell wäre nicht "
                    "identifiziert. Sie werden weder quadriert noch gekreuzt.")),
                kasten("Warum standardisiert wird", absatz(
                    "Das Quadrat des Medianeinkommens liegt bei 1e10, das "
                    "Produkt zweier Prädiktoren bei 1e9 — die IRLS-Iteration "
                    "bricht auf dieser Konditionierung zusammen. "
                    "<b>Mathematisch ist die Standardisierung folgenlos:</b> "
                    "der aufgespannte Raum ist derselbe."))),
            kasten("Die Selbstprüfung, die daraus folgt", absatz(
                "Weil die Standardisierung folgenlos ist, <b>muss</b> die "
                "Spalte <code>linear</code> die Stufe-2-Baseline aus v1 exakt "
                "reproduzieren. <code>_selbsttest()</code> prüft das auf drei "
                "Nachkommastellen und bricht sonst ab — weicht sie ab, sieht "
                "dieses Skript andere Merkmale oder andere Folds, und jeder "
                "Vergleich darin ist wertlos.")),
            abstand=14), label="Konzept")

D.folie("vorpruefung/v3_spezifikation.py", "Die sechs Funktionen", None,
        stapel(
            reihe(
                block("entwerfe(train, test, spezifikation)", punkte([
                    "Baut die Merkmalsmatrizen, jeweils mit Konstante an "
                    "Position 0.",
                    "Mittelwert und Streuung stammen <b>nur aus dem "
                    "Training</b> — der Teststadtteil darf die Transformation "
                    "nicht mitbestimmen.",
                    "Quadrate und Paarprodukte entstehen erst <b>nach</b> der "
                    "Standardisierung.",
                ], eng=True)),
                block("ein_lauf() · alle_laeufe() · zweistufig()", punkte([
                    "<code>ein_lauf</code>: eine Poisson-Anpassung, eine "
                    "Bewertung auf der Originalskala.",
                    "<code>alle_laeufe</code>: 10 Wiederholungen × 5 Folds × "
                    "4 Spezifikationen = <b>200 Anpassungen</b>.",
                    "<code>zweistufig</code>: dieselbe Mittelungsregel wie "
                    "überall — maßgeblich ist die Streuung der zehn "
                    "Wiederholungsmittel.",
                ], eng=True))),
            reihe(
                kasten("Konvergenz — ein Befund, kein Ärgernis", absatz(
                    "Mit 67 Termen auf 3 036 Trainingszeilen konvergiert die "
                    "IRLS-Iteration nicht in jedem Fold. Nicht konvergierte "
                    "Anpassungen werden <b>gezählt und mitberichtet</b>, nicht "
                    "stillschweigend übergangen und nicht entfernt — sie sind "
                    "Teil des Befundes, dass diese Spezifikation zu den Daten "
                    "nicht passt.")),
                kasten("Der Prüfauftrag, auf den es ankommt", absatz(
                    "Ist der Abstand <code>linear</code> ↔ "
                    "<code>interaktionen</code> größer als der Abstand "
                    "<code>linear</code> ↔ Random Forest? <b>Nur dann</b> trägt "
                    "die Aussage: die Spezifikation bewegt mehr als die "
                    "Verfahrenswahl."), "frage")),
            abstand=16), label="Funktionen")

# ------------------------------------------------------------------ v4
D.kapitel("v4_decke.py",
          "Zwei Obergrenzen über dem Strukturstrang. Sie verwandeln ein "
          "schwaches Ergebnis in einen methodischen Befund.",
          [("307", "Zeilen"), ("7", "Funktionen"), ("200", "Bootstrap-Ziehungen")])

D.folie("vorpruefung/v4_decke.py", "Warum es diese Datei gibt",
        "Der Strukturstrang erreicht Macro-F1 um 0,33. Gegen 1,0 gehalten "
        "sieht das misslungen aus — und diese Lesart ist falsch.",
        stapel(
            reihe(
                kasten("Der Denkfehler", absatz(
                    "Ein Macro-F1 von 0,33 gegen die <b>1,0</b> einer "
                    "fehlerfreien Vorhersage zu halten unterstellt, dass 1,0 "
                    "bei dieser Zielgröße und diesem Merkmalssatz überhaupt "
                    "erreichbar wäre. Das ist sie nicht."), "warn"),
                block("Die Antwort", punkte([
                    "Zwei Obergrenzen begrenzen den Strukturstrang.",
                    "Beide entstehen <b>vor jeder Modellwahl</b> — die eine in "
                    "der Konstruktion der Zielgröße, die andere in der Struktur "
                    "der Merkmale.",
                    "Sie zu beziffern ist keine nachträgliche Entlastung, "
                    "sondern die Voraussetzung dafür, 0,33 überhaupt einordnen "
                    "zu können.",
                ]))),
            reihe(
                kpi([("0,6404", "Decke A — Label-Rauschen"),
                     ("0,4572", "Decke B — Stadtteilwissen"),
                     ("0,22", "Mehrheitsklasse"),
                     ("~0,33", "erreicht")])),
            kasten("Die richtige Frage", absatz(
                "Nicht „warum nur 0,33?“, sondern: <b>Wie viel des überhaupt "
                "Erreichbaren wird ausgeschöpft?</b>")),
            abstand=17),
        label="Datei")

D.folie("vorpruefung/v4_decke.py", "decke_a(panel) — das Label-Rauschen",
        "Die Obergrenze, die schon in der Konstruktion der Zielgröße steckt.",
        stapel(
            einaus("Panel mit den vier Anteilsspalten und der Einsatzzahl N",
                   "mittlerer Macro-F1, Streuung über die Ziehungen, Kippanteil"),
            reihe(
                block("Das Problem", punkte([
                    "<code>dominante_einsatzart</code> ist <b>kein beobachtetes "
                    "Merkmal</b>, sondern der argmax über vier Anteilsspalten "
                    "desselben Stadtteil-Monats.",
                    "Wo zwei Anteile dicht beieinander liegen, entscheidet der "
                    "<b>Zufall der Monatsziehung</b>, welche Klasse gewinnt — "
                    "nicht die Struktur des Stadtteils.",
                ])),
                kasten("Die Messung", punkte([
                    "Parametrischer Bootstrap: jeder Stadtteil-Monat wird aus "
                    "<code>Multinomial(N, p_beobachtet)</code> neu gezogen.",
                    "Der Macro-F1 zwischen beobachtetem und neu gezogenem Label "
                    "ist die Güte eines Modells, das die wahren "
                    "Klassenwahrscheinlichkeiten <b>exakt kennt</b>.",
                    "<b>Kein Verfahren kann darüber hinaus.</b>",
                ]))),
            kasten("Fallstricke", absatz(
                "Zeilen mit <code>N = 0</code> werden <b>ausgeschlossen</b>, "
                "nicht auf eine Klasse gesetzt — <code>rng.multinomial</code> "
                "lieferte sonst stumm einen Nullvektor, dessen argmax immer auf "
                "die erste Klasse zeigt. Und der Bootstrap braucht einen festen "
                "<code>RANDOM_STATE</code>, sonst schwankt Decke A zwischen zwei "
                "Läufen und die Zahl in der Arbeit passt nicht mehr zur Zahl in "
                "der CSV."), "warn"),
            abstand=13))

D.folie("vorpruefung/v4_decke.py", "decke_b(panel) — die Grenze des "
        "Stadtteilwissens",
        "Die bindende Obergrenze. Sie folgt direkt aus der Merkmalsstruktur.",
        stapel(
            einaus("Panel mit Stadtteil- und Klassenspalte",
                   "Macro-F1 der Zuweisung, Trefferanteil, Verteilung der "
                   "Modalklassen"),
            reihe(
                block("Die Herleitung", punkte([
                    "Alle Prädiktoren sind <b>stadtteilgebunden</b>: die "
                    "baulichen konstant über den gesamten Zeitraum, die "
                    "sozialen konstant je Stadtteil-Jahr, der "
                    "Kriminalitätsindex zu 90 % zwischen den Stadtteilen.",
                    "Ein Modell kann daraus nur <b>Stadtteilwissen</b> ziehen.",
                    "Die Obergrenze ist damit die Güte einer Vorhersage, die "
                    "jedem Stadtteil-Monat die <b>Modalklasse seines "
                    "Stadtteils</b> zuweist.",
                ])),
                kasten("Warum sie so tief liegt", absatz(
                    "Decke B liegt <b>deutlich unter</b> Decke A. Der Grund "
                    "steht in der Ergebnistabelle: Die Modalklassen der "
                    "Stadtteile sind fast alle dieselbe. Stadtteilwissen "
                    "unterscheidet die Stadtteile also kaum."))),
            kasten("Kolloquiumsfrage", absatz(
                "<b>„Woher wissen Sie, dass Decke B die bindende ist?“</b> — "
                "Weil sie unter Decke A liegt. Ein Prüfauftrag der Datei lautet "
                "genau so: Liegt Decke B <i>unter</i> Decke A? Wenn nicht, ist "
                "etwas falsch — Stadtteilwissen kann das Label-Rauschen nicht "
                "unterbieten."), "frage"),
            abstand=13))

D.folie("vorpruefung/v4_decke.py", "Die übrigen Funktionen", None,
        stapel(
            reihe(
                block("marge(panel)", stapel(
                    einaus("Panel mit den vier Anteilsspalten",
                           "Verteilung des Abstands top1 − top2"),
                    punkte([
                        "Ein kleiner Abstand heißt: das Label hätte bei einer "
                        "anderen Monatsziehung anders gelautet.",
                        "Macht sichtbar, wie groß der Anteil solcher Zeilen "
                        "ist.",
                    ], eng=True), abstand=9)),
                block("ausschoepfung(modelle, basis, a, b)", stapel(
                    einaus("Modellwerte, Mehrheitsklassen-Basis, Decke A und B",
                           "eine Quote je Verfahren und Decke"),
                    punkte([
                        "<code>(Modell − Mehrheitsklasse) / (Decke − "
                        "Mehrheitsklasse)</code>",
                        "Der Rohquotient <code>Modell/Decke</code> wäre "
                        "<b>geschönt</b>: Der Sockel von Macro-F1 0,22 ist "
                        "keine Leistung des Modells.",
                    ], eng=True), abstand=9))),
            reihe(
                block("bericht(...) · main(argv) · _macro_f1(a, b)", punkte([
                    "<code>bericht</code> ist reine Formatierung — hier wird "
                    "nichts gerechnet. Ziehungszahl und Startwert stehen im "
                    "Kopf, damit die Datei ohne den Code lesbar bleibt.",
                    "<code>main</code> filtert ohne das Argument "
                    "<code>holdout</code> auf <code>ist_holdout == 0</code>. "
                    "Die Decke ist zwar eine Eigenschaft der <i>Zielgröße</i> "
                    "und berührt keinen Prädiktor — die Sperre gilt trotzdem "
                    "konstruktiv, nicht nach Ermessen.",
                ], eng=True)),
                kasten("Wie die Zahlen zu lesen sind", punkte([
                    "<b>Decke A ist eine Obergrenze, kein Zielwert.</b> Dass "
                    "ein Modell sie nicht erreicht, ist kein Mangel.",
                    "Bindend ist Decke B.",
                ], eng=True))),
            abstand=15), label="Funktionen")

D.folie(P, "Was man über diesen Ordner wissen muss", None,
        stapel(
            reihe(
                kasten("1 · Die Baseline ist ein Mitbewerber", absatz(
                    "Sie läuft über dieselben 50 Läufe, dieselben Folds und "
                    "dieselben Merkmale wie die Vergleichsverfahren. Nur so "
                    "lässt sich <b>gepaart</b> testen — und nur so erfüllt der "
                    "Vergleich Auflage C.")),
                kasten("2 · Die Verfahrenswahl ist belegt", absatz(
                    "Sechs Belege, davon einer als ausdrückliche Auflage. Die "
                    "Prüfung entscheidet <b>nicht</b> zwischen Random Forest "
                    "und XGBoost — das ist die empirische Forschungsfrage.")),
                kasten("3 · Die Grenzen sind beziffert", absatz(
                    "v3 zeigt, dass die Spezifikation mehr bewegt als die "
                    "Verfahrenswahl. v4 zeigt, dass über dem Strukturstrang "
                    "eine Decke bei 0,4572 liegt. Beides sind <b>Befunde</b>, "
                    "keine Entschuldigungen."))),
            kasten("Der Satz, mit dem man den Ordner verteidigt", absatz(
                "<b>„Die Vorprüfung sagt, woran gemessen wird und was "
                "überhaupt messbar ist — bevor das erste Vergleichsverfahren "
                "startet.“</b> Ohne sie wäre jedes Ergebnis eine Rangliste ohne "
                "Maßstab.")),
            abstand=20),
        label="Zusammenfassung")

if __name__ == "__main__":
    schreibe(D, "vorpruefung/ · Messlatte und Eignung",
             pathlib.Path("/home/claude/out/folien/02_vorpruefung.pdf"))
