# Befunde der Umsetzung

> **Lebensdauer:** waechst waehrend der Implementierung von `modelle/`, wird nie
> umgeschrieben. Grundlage fuer die Limitationen in Kapitel 8 und die kritische
> Reflexion, die das Gutachten verlangt (R6, R9).
>
> Hier steht, was beim Bauen aufgefallen ist: luckenhafte oder widerspruechliche
> Stellen der Spezifikation, Ergebnisse gegen dokumentierte Entscheidungen,
> Schwaechen im vorhandenen Code, notgedrungene Annahmen. **Nicht** hierher
> gehoeren normale TODOs, Stilfragen und alles, was direkt und eindeutig
> loesbar war.

**Schwere:** ⛔ blockierend · 🔴 hoch · 🟡 mittel · ⚪ gering

| | Datum | Fundstelle | Schwere | Status |
|---|---|---|---|---|
| B-1 | 05.08. | `04_MODELLIERUNG.md` §2 | 🔴 | geloest |
| B-2 | 05.08. | `prep/s2_datensaetze.py:97` | ⛔ | geloest |
| B-3 | 05.08. | `prep/s2_datensaetze.py:97` | ⛔ | geloest |
| B-4 | 05.08. | `vorpruefung/v1_baselines.py:93` | 🔴 | geloest |
| B-5 | 05.08. | `04_MODELLIERUNG.md` §5, `#34` | 🟡 | entschaerft, Rest bleibt |
| B-6 | 05.08. | `06_RISIKEN.md` R-10 | 🟡 | entschieden, Doku folgt |
| B-7 | 05.08. | `requirements.txt` | ⛔ | geloest |
| B-8 | 05.08. | `requirements.txt` | 🔴 | geloest |
| B-9 | 05.08. | `modelle/m02_menge.py` Docstring | ⚪ | geloest |
| B-10 | 05.08. | `modelle/m02_menge.py:71,74` | 🟡 | offen, Vorschlag unten |
| B-11 | 05.08. | `03_STAND.md` §4 | 🟡 | offen, Vorschlag unten |
| B-12 | 05.08. | `04_MODELLIERUNG.md` §2 | ⚪ | Doku-Ungenauigkeit |
| B-13 | 05.08. | `CLAUDE.md` §Praktisches | ⚪ | Schaetzung zu niedrig |
| B-14 | 05.08. | `04_MODELLIERUNG.md` §Hold-out | 🟡 | ausgelegt, Regel dokumentiert |
| B-15 | 05.08. | `04_MODELLIERUNG.md` §Sonderfaelle | 🟡 | gemessen, Doku zu erweitern |
| B-16 | 05.08. | `m02_menge.py:71,74,137` | 🔴 | offen, Vorschlag unten |
| B-17 | 05.08. | `m04_shap.py` / shap 0.52 | 🔴 | umgangen |
| B-18 | 05.08. | `04_MODELLIERUNG.md` §m04 | 🟡 | Bezugsmenge korrigiert |
| B-19 | 05.08. | `06_RISIKEN.md` R-9 | 🟡 | **beziffert - R-9 entfaellt** |
| B-20 | 05.08. | plattformabhaengige Rundung | ⚪ | benannt |
| B-21 | 06.08. | `04_MODELLIERUNG.md` §Hyperparameter-Suche | 🟡 | **neu entstanden**, bewusst getragen |
| B-22 | 06.08. | `m02_menge.aggregiere()` | 🟡 | selbst gefunden, behoben |
| B-23 | 06.08. | `phase_tuning()`, `json.dumps(..., default=str)` | 🔴 | selbst gefunden, behoben |
| B-24 | 06.08. | XGBoost, Kernzahl | 🔴 | **gemessen im Lauf** — Ergebnisse unberührt, gehört in Kap. 6 |
| B-25 | 06.08. | `m02_menge.main()` | 🟡 | behoben |
| B-26 | 06.08. | `06_RISIKEN.md` R-1, Vortest 28.07. | 🔴 | **Ergebnis widerspricht der Erwartung** |
| B-27 | 06.08. | `leakage_diagnose.csv` | 🟡 | **B-21 gemessen: kein Effekt** |
| B-28 | 06.08. | `menge_mittel.csv`, `parallel_gewinn` | ⚪ | Nebenbefund |
| B-29 | 06.08. | `06_RISIKEN.md` R-2 | 🔴 | **Erwartung widerlegt** — beide Verfahren schlagen Stufe 2 |
| B-30 | 06.08. | beide Straenge zusammen | 🔴 | **Kernbefund der Arbeit** |
| B-31 | 06.08. | `06_RISIKEN.md` R-3, `03_STAND.md` §3 | 🔴 | **Extrapolationserklaerung widerlegt** |
| B-32 | 06.08. | Aufschluesselung der Extrapolation | 🟡 | deskriptiv, fuer Kap. 4 |
| B-33 | 06.08. | `results/shap/ablation_exposition.csv` | 🔴 | **Mechanismus belegt — Kernbefund** |
| B-34 | 06.08. | `06_RISIKEN.md` R-9, B-19 | 🔴 | R-9 war berechtigt, unser Test war der falsche |
| B-35 | 06.08. | `results/shap/faktorgruppen_menge.csv` | 🟡 | UF1 fuer den Mengenstrang geschlossen |
| B-36 | 06.08. | `v1_baselines.klassifikation()` | 🔴 | **Baseline war ungetunt — Vorsprung halbiert sich** |

---

## B-1 · Die dokumentierte Aufruffolge reproduziert die Datensaetze nicht

**Fundstelle:** `docs/04_MODELLIERUNG.md`, Abschnitt 2, Codeblock:
`d = ergaenze_aufteilung(daten, versatz=versatz)`

**Was aufgefallen ist.** Der Aufruf laesst das Argument `selten` weg. Ohne
dieses Argument sortiert `ergaenze_aufteilung()` allein nach Bevoelkerung und
nicht zusaetzlich nach brand-dominierten Monaten — die doppelte Stratifizierung
aus Decision Log #30 entfaellt also still. Gemessen am 05.08.2026: **30 von 35
Stadtteilen** landen in einem anderen Fold als in der Parquet-Datei. Alle in
`03_STAND.md` berichteten Kennzahlen — Brand-Testfaelle 13·9·6·3·2,
Extrapolationsanteile 40,9/33,3/57,4/33,3/3,6 %, saemtliche Baseline-Werte —
gehoeren zur stratifizierten Fassung und waeren nicht mehr gueltig gewesen.

Erschwerend: Der Fehler ist unsichtbar. Der Lauf bricht nicht ab, die Zahlen
sehen plausibel aus, sie beziehen sich nur auf eine andere Aufteilung.

**Wie ausgelegt.** `selten` wird aus `klassifikation.parquet` rekonstruiert,
wortgleich zu `prep/s2_datensaetze.run()`, und in
`vorpruefung/v0_aufteilung.selten_je_stadtteil()` gekapselt. Nachweis: Mit
`selten` ist die erzeugte `fold`-Spalte bei Wiederholung 0 bitgenau identisch
zur Datei; die Pruefung laeuft als `assert` bei **jedem** Aufruf, nicht nur im
Selbsttest.

**Fuer die Arbeit.** Nebenbefund mit eigener Aussage: Die Fold-Zuteilung ist
nicht aus den Dateien rekonstruierbar, weil die Klassenspalte im
Regressionsdatensatz fehlt. Der Regressionsstrang muss deshalb
`klassifikation.parquet` mitlesen. Das ist kein Leakage — die Zahl geht in kein
Modell ein, sie bestimmt nur, welche Stadtteile gemeinsam getestet werden.

---

## B-2 · Der Versatz rotiert das Hold-out mit

**Fundstelle:** `prep/s2_datensaetze.py`, Zeile 97:
`gruppe = {st: (i + versatz) % (N_FOLDS + 1) for i, st in enumerate(ordnung)}`

**Was aufgefallen ist.** Gruppe 0 ist das Hold-out. Der Versatz verschiebt die
Gruppennummern, also wandert auch, welche Stadtteile Gruppe 0 bilden. Gemessen:
bei `versatz = 1` liegt **kein einziger** der sechs urspruenglichen
Hold-out-Stadtteile noch im Hold-out; sie stehen stattdessen in Training und
Test der Kreuzvalidierung.

Haette man die dokumentierte Schleife `for versatz in range(10)` so
ausgefuehrt, waeren die sechs Hold-out-Stadtteile in neun von zehn
Wiederholungen mittrainiert worden. Die abschliessende Schlussbewertung waere
wertlos gewesen, ohne dass es irgendwo auffaellt — und zwar dauerhaft, denn ein
einmal gesehenes Hold-out laesst sich nicht zurueckholen.

**Schwere.** Blockierend. Das ist der gravierendste Fund dieser Umsetzung.

**Wie ausgelegt.** `vorpruefung/v0_aufteilung.wiederholte_aufteilung()` haelt
`ist_holdout` fest und verteilt ausschliesslich die 29 Entwicklungsstadtteile
neu. Der Selbsttest prueft fuer alle zehn Wiederholungen, dass die
Hold-out-Menge unveraendert ist und keine Hold-out-Zeile in einer Trainings-
oder Testmaske auftaucht.

**Nicht geaendert:** `prep/s2_datensaetze.py` selbst. Die Funktion ist fuer
ihren eigentlichen Zweck — die einmalige Grundaufteilung mit `versatz = 0` —
korrekt. Nur als Werkzeug fuer Wiederholungen ist sie ungeeignet.

---

## B-3 · Der Versatz erzeugt keine verschiedenen Aufteilungen

**Fundstelle:** dieselbe Zeile wie B-2.

**Was aufgefallen ist.** Zyklisches Austeilen heisst: Stadtteil auf Platz *i*
kommt in Gruppe `(i + versatz) % 6`. Zwei Stadtteile liegen genau dann in
derselben Gruppe, wenn ihre Plaetze modulo 6 uebereinstimmen — und das haengt
**nicht vom Versatz ab**. Der Versatz benennt die Gruppen um, er stellt sie
nicht neu zusammen.

Gemessen ueber `versatz` 0 bis 9:

| Variante | verschiedene Fold-Partitionen |
|---|---|
| wie dokumentiert (Hold-out rotiert mit) | **6** von 10, vier Dubletten |
| Hold-out festgehalten, sonst unveraendert | **1** von 10 — alle identisch |

Die zweite Zeile ist der eigentliche Befund: Haette man B-2 naiv repariert und
nur das Hold-out festgehalten, waeren die zehn Wiederholungen zehn exakte
Kopien desselben Laufs gewesen. `std_wiederholungen` — der Wert, den `R-5`
ausdruecklich als **massgeblich** bezeichnet — waere exakt 0 gewesen, und zwar
plausibel aussehend.

**Wie ausgelegt.** Statt zu rotieren wird **innerhalb der Rangbloecke
gemischt**: Die nach `(selten, bev)` sortierten Stadtteile werden in Bloecke zu
je fuenf geteilt, je Wiederholung mit Seed `RANDOM_STATE + w` permutiert und
dann ausgeteilt. Jeder Fold erhaelt weiterhin genau einen Stadtteil aus jedem
Rangblock — die doppelte Stratifizierung ueberlebt unveraendert, die
Foldgroessen bleiben 6/6/6/6/5 — aber die Zusammensetzung aendert sich
tatsaechlich. Nachgewiesen: 10 von 10 verschiedene Partitionen, kein Fold ohne
Brand-Testfall (Minimum 2).

**Fuer die Arbeit.** In Kapitel 6 ist zu schreiben, wie die Wiederholungen
gebildet werden — „mit unterschiedlichem Versatz" waere falsch.

---

## B-4 · Die Baseline lag nur fuer ein Zehntel der Laeufe vor

**Fundstelle:** `vorpruefung/v1_baselines.py`, Zeile 93:
`for k in range(1, N_FOLDS + 1)`

**Was aufgefallen ist.** Die Baselines laufen ueber die `fold`-Spalte, wie sie
in der Datei steht — das ist Wiederholung 0. Ergebnis: fuenf Werte je
Zielgroesse und Stufe. Die Vergleichsverfahren erzeugen 50. Fuer den gepaarten
Wilcoxon-Test nach #34 braucht es aber je Lauf ein Paar auf **denselben
Testzeilen**; fuer die Wiederholungen 1 bis 9 existierte kein Gegenwert.

Waere nur auf den fuenf vorhandenen Folds gepaart worden, haette der Test die
Primaeraussage nie stuetzen koennen: Bei n = 5 ist das kleinste erreichbare
zweiseitige p 0,0625 und damit strukturell groesser als α = 0,05.

**Wie ausgelegt.** Entscheidung Lukas, 05.08.2026: Die Baselines rechnen im
Originalskript ueber alle zehn Wiederholungen. Kostenpruefung vorab: 50
Negative-Binomial-Laeufe zusammen **11,2 Sekunden** — gegenueber Stunden fuer
das Tuning der Vergleichsverfahren also ohne Gewicht. Die Werte der
Wiederholung 0 bleiben bitgenau erhalten und werden per Diff gegen eine vorher
gesicherte Kopie nachgewiesen.

---

## B-5 · Der gepaarte Test setzt Unabhaengigkeit voraus, die es nicht gibt

**Fundstelle:** `docs/04_MODELLIERUNG.md` Abschnitt 5, Decision Log #34:
„gepaarter Wilcoxon-Test ueber alle Fold-Ergebnisse".

**Was aufgefallen ist.** `R-5` erkennt die Abhaengigkeit der 50 Laeufe
ausdruecklich an — aber nur als Problem der **Streuungsschaetzung**. Der
Wilcoxon-Test selbst setzt ebenfalls unabhaengige Paare voraus. Ueber 50
abhaengige Differenzen gerechnet, faellt sein p-Wert **zu klein** aus; die
Holm-Korrektur hilft dagegen nicht, denn sie korrigiert Mehrfachvergleiche,
nicht Pseudoreplikation.

Es sind 29 Entwicklungsstadtteile. Unabhaengigkeit muesste aus Daten kommen,
nicht aus einem Resampling-Verfahren; sie ist daher grundsaetzlich nicht
herstellbar.

**Wie ausgelegt.** Entscheidung Lukas, 05.08.2026: Der **Primaertest laeuft auf
den 10 Wiederholungsmitteln**, nicht auf den 50 Einzelwerten — dieselbe
zweistufige Logik, die `R-5` fuer die Streuung ohnehin verlangt. Der Test ueber
alle 50 wandert als ausdruecklich gekennzeichnete Sensitivitaet in dieselbe
CSV-Datei (Spalte `teststufe`).

Erreichbarkeit geprueft:

| Teststufe | n | kleinstes zweiseitiges p | reicht fuer α = 0,05 | fuer Holm α/6 |
|---|---|---|---|---|
| Folds einer Wiederholung | 5 | 0,0625 | nein | nein |
| **Wiederholungsmittel** | **10** | **0,00195** | ja | ja |
| alle Laeufe | 50 | ~10⁻¹⁵ | ja, zu optimistisch | ja, zu optimistisch |

**Was offen bleibt — gehoert in Kapitel 8.** Auch die zehn Wiederholungsmittel
sind nicht unabhaengig; es sind dieselben 29 Stadtteile in zehn Gruppierungen.
Der Test kontrolliert die Fold-Schwankung, nicht den kleinen Umfang an
Analyseeinheiten. Das berichtete Konfidenzintervall ist also **enger als die
wahre Unsicherheit** (Nadeau & Bengio 2003). Deshalb wird unabhaengig vom
p-Wert immer die mittlere gepaarte Differenz mit Konfidenzintervall und die
Zahl gewonnener Laeufe berichtet.

Dass die Arbeit das aushaelt, liegt an #34: Die Antwort auf die Forschungsfrage
haengt an drei Bausteinen, nicht an einem p-Wert.

---

## B-6 · Testfamilie: die Doku sagt 7, die Skripte koennen nur 6 und 1

**Fundstelle:** `docs/06_RISIKEN.md` R-10 und `docs/04_MODELLIERUNG.md`
Fallstrick 2: „den kleinsten gegen α/7 pruefen".

**Was aufgefallen ist.** Die sieben Tests verteilen sich auf zwei Skripte —
sechs paarweise Vergleiche in `m02` (3 Verfahrenspaare × 2 Zielgroessen), einer
in `m03`. Holm-Bonferroni braucht aber **alle** p-Werte der Familie
gleichzeitig. Getrennt laufende Skripte koennen die Familie also nicht bilden,
ohne voneinander zu lesen.

**Wie ausgelegt.** Entscheidung Lukas, 05.08.2026: **zwei Familien.** `m02`
rechnet Holm ueber seine 6 Tests, `m03` hat einen einzigen Test und wird nicht
korrigiert. Begruendung: Regression und Klassifikation beantworten verschiedene
Teilfragen; ein Zufallstreffer im einen Strang macht den anderen nicht falsch.

**Konsequenz, ehrlich zu benennen:** Der Klassifikationsvergleich RF gegen
XGBoost laeuft damit **ungekorrigiert** gegen α = 0,05 statt gegen α/7 = 0,0071.
Bei der Regression ist der Unterschied klein (α/6 = 0,0083 statt 0,0071).

**Noch zu tun:** `06_RISIKEN.md` R-10 und `04_MODELLIERUNG.md` Fallstrick 2 von
„7 Tests / α/7" auf „zwei Familien, 6 + 1" umschreiben. Solange das aussteht,
widersprechen sich Dokumentation und Code.

---

## B-7 · `xgboost` fehlte in `requirements.txt` und im venv

**Fundstelle:** `requirements.txt` (alte Fassung), venv des Repos.

**Was aufgefallen ist.** `xgboost` stand zwar ungepinnt in der alten
`requirements.txt`, war im venv aber **nicht installiert** — gepruefte
`dist-info`-Verzeichnisse: pandas, numpy, scikit-learn, scipy, statsmodels,
pyarrow, matplotlib, seaborn, geopandas, shapely — kein xgboost. Damit war
eines der drei Regressions- und eines der zwei Klassifikationsverfahren auf der
Zielmaschine nicht lauffaehig.

**Geloest.** `xgboost==3.4.0` gepinnt; das Wheel ist `py3-none-win_amd64` und
damit auch unter Python 3.14 installierbar (geprueft auf PyPI).

---

## B-8 · `shap` war nirgends verzeichnet

**Fundstelle:** `requirements.txt`, `modelle/m04_shap.py`.

**Was aufgefallen ist.** `m04_shap.py` ist als Pflichtbestandteil geplant
(Unterfrage 1, blockweise Interpretation), aber `shap` stand weder in den
Anforderungen noch im venv. Waere erst beim Ausfuehren von `m04` aufgefallen —
also am spaetesten moeglichen Zeitpunkt.

**Geloest.** `shap==0.52.0` gepinnt, `cp312-abi3`-Wheel, laeuft auf 3.12 bis
3.14.

Zusaetzlich richtiggestellt: Die alten Pins `pandas==2.1.4` und
`pyarrow==15.0.0` waren falsch — installiert sind 3.0.2 und 24.0.0. Eine
Versionsangabe in Kapitel 6, die sich auf die alte Datei stuetzt, waere unwahr
gewesen.

---

## B-9 · `m02_menge.py` hatte keinen Pruefauftrags-Block

**Fundstelle:** Modul-Docstring von `modelle/m02_menge.py`.

**Was aufgefallen ist.** `CLAUDE.md` Abschnitt 4 verlangt fuer **jedes** Skript
in `modelle/` einen Block „Pruefauftraege" am Ende des Docstrings, der nach
jedem Lauf abzuarbeiten ist. `m03`, `m04` und `m05` haben einen, ausgerechnet
`m02` — das zuerst laufen soll — nicht.

**Geloest.** Block aus `04_MODELLIERUNG.md` (Sonderfaelle, Fallstricke 1–4) und
den Risiken R-1, R-3, R-9 abgeleitet und nachgetragen.

---

## B-10 · Die Laufzeiten sind zwischen den Verfahren nicht vergleichbar

**Fundstelle:** `modelle/m02_menge.py` Zeilen 71 und 74 —
`RandomForestRegressor(..., n_jobs=-1)` und `XGBRegressor(..., n_jobs=-1)`.

**Was aufgefallen ist.** Random Forest und XGBoost trainieren ueber alle Kerne,
`Ridge` ist einkernig. `ein_lauf()` misst Wanduhrzeit. Unterfrage 3 fragt nach
dem **Trainings- und Inferenzaufwand** — gemessen wird aber ein Gemisch aus
Rechenaufwand und Parallelisierungsgrad, und das Verhaeltnis verschiebt sich
mit der Kernzahl der Maschine. Auf einem 32-Kerner saehe Random Forest deutlich
besser aus als auf einem Vierkerner, ohne dass sich am Verfahren etwas aendert.

Erste Messung auf zwei Kernen, Fold 1, `anzahl_einsaetze`:

| Verfahren | fit | predict |
|---|---|---|
| Ridge | 0,02 s | 0,0046 s |
| Random Forest | 1,85 s | 0,1408 s |
| XGBoost | 3,33 s | 0,0269 s |

**✅ GELOEST am 06.08.2026** — und zwar so, dass daraus eine zusaetzliche
Aussage wird statt eines Vorbehalts.

Der berichtete Aufwand wird **einkernig** gemessen, fuer alle Verfahren gleich.
Das ist der Rechenaufwand, und er ist maschinenunabhaengiger als eine
Wanduhrzeit unter Vollast. Der **Parallelisierungsgewinn** wird getrennt
erhoben: `ein_lauf(..., auch_parallel=True)` fittet in Wiederholung 0 ein
zweites Mal ueber alle Kerne; `menge_mittel.csv` und `struktur_mittel.csv`
fuehren `train_sekunden_parallel_mean` und `parallel_gewinn`.

Erste Messung auf zwei Kernen (400 Baeume, Fold 1, `anzahl_einsaetze`):

| Verfahren | 1 Kern | alle Kerne | Gewinn |
|---|---|---|---|
| Ridge | 0,02 s | 0,01 s | 2,65× (Messrauschen bei dieser Groessenordnung) |
| Random Forest | 4,88 s | 2,82 s | 1,73× |
| XGBoost | 0,41 s | 0,32 s | 1,25× |

**Ein Befund faellt dabei sofort auf:** Einkernig ist XGBoost mit 0,41 s **rund
zwoelfmal schneller** als Random Forest mit 4,88 s. In der frueheren Messung
mit `n_jobs=-1` lag XGBoost scheinbar hinten (3,33 s gegen 1,85 s). Die
Rangfolge zwischen den beiden Ensembles kippt also mit der Betriebsart — genau
der Grund, warum sie einheitlich gemessen werden muss. Der Abstand zu Ridge
(zwei Groessenordnungen) ist davon unberuehrt.

**Fuer Kapitel 7** sind damit zwei getrennte Saetze moeglich statt eines
unklaren: „Rechenaufwand je Fit" und „wie gut skaliert das Verfahren ueber
Kerne". Der zweite gehoert zu Unterfrage 4 und war vorher gar nicht messbar.

---

## B-11 · Die berichteten Baseline-Zahlen aendern sich

**Fundstelle:** `docs/03_STAND.md` Abschnitt 4; dieselben Werte zitiert in
`CLAUDE.md` §3, `06_RISIKEN.md` R-2 und `04_MODELLIERUNG.md`.

**Was aufgefallen ist.** R² 0,472 · RMSE 37,44 · Macro-F1 0,290 stammen aus
fuenf Folds **einer** Wiederholung. Sobald die Baselines ueber alle zehn laufen
(B-4), entstehen andere Mittelwerte. Nur die neuen sind mit den Verfahren
vergleichbar, weil nur sie auf denselben Laeufen beruhen.

**Wie ausgelegt.** Entscheidung Lukas, 05.08.2026: Die 50er-Fassung wird die
berichtete Messlatte. Die alten Werte bleiben in `baselines_mittel.csv` unter
`basis = wiederholung_0` erhalten und in `03_STAND.md` als „Stand vor dem
Modelllauf" sichtbar — nichts wird still ueberschrieben.

**Noch zu tun:** Nach dem finalen Lauf pruefen, ob eine der Zitatstellen die
alte Zahl in einer Aussage traegt, die mit der neuen nicht mehr stimmt. Der
kritische Fall waere `06_RISIKEN.md` R-2: Dort begruendet der Abstand
0,290 gegen 0,223 die Aussage „der Klassifikationsstrang traegt wenig".

---

## B-12 · `fold_masken()` liefert Masken, nicht Datensaetze

**Fundstelle:** `docs/04_MODELLIERUNG.md` Abschnitt 2:
`train, test = fold_masken(daten, k)   # k = 1..5`

**Was aufgefallen ist.** Die Namen legen zwei DataFrames nahe; zurueck kommen
zwei boolesche Serien. Wer dem Codeblock folgt und `train[MERKMALE]` schreibt,
bekommt keinen Fehler, sondern eine Spaltenauswahl auf einer Serie — je nach
Aufruf einen Absturz oder stillen Unsinn. In `v1_baselines.py` ist es richtig
gemacht (`train, test = panel[tr], panel[te]`), in der Doku irrefuehrend.

**Geloest** durch konsequente Benennung `tr, te` fuer Masken in allen neuen
Dateien. Doku-Korrektur steht aus.

---

## B-13 · Die Laufzeitschaetzung ist zu niedrig

**Fundstelle:** `CLAUDE.md`, Abschnitt „Praktisches": „etwa 45 bis 60 Minuten
(8.400 Modellanpassungen)".

**Was aufgefallen ist.** Gemessen auf zwei Kernen kosten 12 Tuning-Fits 28 s
(Ridge), 51 s (Random Forest), 29 s (XGBoost). Hochgerechnet auf 8.400
Anpassungen sind das rund **sieben Stunden** auf dieser Maschine. Dominierend
ist nicht der Fit — Ridge fittet in 0,02 s — sondern der Prozess-Overhead der
parallelen Suche.

Auf mehr Kernen faellt das deutlich, aber eine Stunde duerfte auch dort knapp
sein. Relevant fuer die Planung, nicht fuer die Ergebnisse. Die wahrscheinliche
Ursache steht in B-16.

---

## B-14 · Welche Hyperparameter bekommt das Hold-out?

**Fundstelle:** `docs/04_MODELLIERUNG.md`, Abschnitt „Hold-out": „mit den in der
Kreuzvalidierung gewaehlten Hyperparametern neu trainiert".

**Was aufgefallen ist.** Das Tuning liefert **fuenf** Parametersaetze je
Zielgroesse und Verfahren, einen je Fold. Welcher davon fuer die
Schlussbewertung gilt, sagt die Spezifikation nicht. Die Frage ist nicht
kosmetisch: Bei Random Forest unterscheiden sich die Saetze in `max_depth` und
`max_features` teils erheblich.

**Wie ausgelegt.** Gewaehlt wird der Satz des Folds mit dem besten Guetemass in
**Wiederholung 0** — niedrigstes RMSE in `m02`, hoechstes Macro-F1 in `m03`.
Deterministisch, aus reinen Entwicklungsdaten, und der gewaehlte Fold steht als
Spalte `fold_der_parameter` in `holdout.csv`, ist also nachpruefbar.

**Alternative, bewusst verworfen:** einmal auf allen 29 Entwicklungsstadtteilen
neu tunen. Methodisch etwas sauberer, kostet aber sechs zusaetzliche
Suchlaeufe und weicht staerker vom Wortlaut der Spezifikation ab. Falls in der
Sprechstunde gefragt wird: beide Wege sind vertretbar, der gewaehlte ist der
dokumentierte.

---

## B-15 · Auch XGBoost liefert negative Vorhersagen

**Fundstelle:** `docs/04_MODELLIERUNG.md`, Abschnitt „Sonderfaelle": „Negative
Vorhersagen nach `expm1`. **Ridge** auf `log(1+y)` kann [...] Werte unter −1
liefern".

**Was aufgefallen ist.** Die Spezifikation erwartet negative Vorhersagen nur
bei Ridge. Gemessen im Probelauf trat der Fall bei **XGBoost** auf der
Zielgroesse `einsaetze_je_1000_ew` auf, bei Ridge dagegen gar nicht. Der Grund
ist einleuchtend, sobald man ihn sieht: XGBoost minimiert `reg:squarederror`
ohne jede Positivitaetsschranke, waehrend Ridge auf `log(1+y)` schaetzt und
`expm1` nie unter −1 fallen kann. Die vermeintlich gefaehrdete Variante ist
also die sicherere.

**Wie ausgelegt.** `ein_lauf()` gibt seit dem 05.08.2026 `n_negativ` und
`y_hat_min` zurueck, fuer **alle** Verfahren. Nicht gekappt — nur gezaehlt.
`menge_mittel.csv` fuehrt `n_negativ_gesamt` je Verfahren.

**Fuer die Arbeit.** Der Satz in Kapitel 7 muss allgemein formuliert werden,
nicht auf Ridge bezogen. Er wird dadurch sogar interessanter: Er zeigt, dass
die Zielgroessentransformation eine Nebenwirkung hat, die man ihr nicht ansieht.

---

## B-16 · Verschachtelte Parallelisierung bremst das Tuning aus

**Fundstelle:** `modelle/m02_menge.py` Zeilen 71 und 74
(`n_jobs=-1` im Schaetzer) zusammen mit Zeile 137
(`RandomizedSearchCV(..., n_jobs=-1)`); gleiches Muster in `m03_struktur.py`.

**Was aufgefallen ist.** Die Suche startet Prozesse ueber alle Kerne, und jeder
dieser Prozesse startet seinerseits einen Random Forest bzw. XGBoost, der
wiederum alle Kerne beanspruchen will. Auf zwei Kernen fuehrte das dazu, dass
ein Probelauf mit **Budget 2** nach 15 Minuten noch in Phase 1 stand — bei
einem Sollbudget von 50.

Das ist kein Fehler im Sinne falscher Ergebnisse: Die Zahlen stimmen, es dauert
nur ein Vielfaches. Fuer Unterfrage 3 ist es dennoch relevant, weil dieselbe
Ueberzeichnung auch die gemessenen Trainingszeiten beeinflusst (vgl. B-10).

**✅ BEHOBEN am 06.08.2026** (Freigabe Lukas, `verfahren()` und `tune()` durften
angefasst werden). Die Modelle laufen jetzt einkernig (`N_JOBS_MODELL = 1`),
parallelisiert wird allein die Suche (`N_JOBS_SUCHE = -1`).

Gemessen auf zwei Kernen, Tuning eines Folds:

| Verfahren | vorher (Budget 3) | nachher (Budget 5) | je Iteration |
|---|---|---|---|
| Ridge | 28 s | 3,3 s | **rund 14× schneller** |
| Random Forest | 51 s | 28,3 s | rund 3× schneller |

Hochgerechnet faellt der volle Lauf damit von etwa sieben auf ein bis zwei
Stunden — auf zwei Kernen. Die gefundenen Parameter aendern sich nicht.

**Nachgewiesen, dass die Kernzahl nur die Dauer beeinflusst, nicht das
Ergebnis:** `ein_lauf(..., auch_parallel=True)` fittet dasselbe Modell ein
zweites Mal ueber alle Kerne und prueft die Vorhersagen per `assert` auf
Gleichheit. Bestanden fuer alle fuenf Verfahren-Zielgroessen-Kombinationen.
Ohne diesen Nachweis waere die Umstellung eine Behauptung.

---

## B-17 · `shap` kann XGBoost 3.x nicht lesen

**Fundstelle:** `modelle/m04_shap.py`, `shap.TreeExplainer`.

**Was aufgefallen ist.** Bei einem mehrklassigen XGBoost-Modell bricht
`shap.TreeExplainer` ab:

```
ValueError: could not convert string to float:
'[-1.3113022E-6,-2.1457672E-6,-8.34465E-7,4.2915344E-6]'
```

Ursache: XGBoost 3.x speichert `base_score` bei mehreren Klassen als **Vektor**,
der Modell-Loader von `shap` 0.52.0 erwartet einen Skalar. Geprueft mit shap
0.52.0 und xgboost 3.2.0; die gepinnte Kombination auf der Zielmaschine
(xgboost 3.4.0) hat dasselbe Format.

Waere erst beim Ausfuehren von `m04` aufgefallen — also nach den mehrstuendigen
Modelllaeufen.

**Wie ausgelegt.** Fuer XGBoost wird dessen **eigenes TreeSHAP** benutzt:
`booster.predict(DMatrix, pred_contribs=True)`. Das ist derselbe Algorithmus,
exakt und nicht genaehert, nur ohne den defekten Parser. Random Forest laeuft
weiterhin ueber `shap.TreeExplainer`, dort tritt das Problem nicht auf.

**Fuer die Arbeit.** In der Methodenbeschreibung ist zu schreiben, dass die
SHAP-Werte fuer XGBoost aus der Bibliothek selbst stammen — sonst passt die
Quellenangabe nicht zur Umsetzung.

---

## B-18 · Die VIF-Entdopplung laeuft ins Leere

**Fundstelle:** `docs/04_MODELLIERUNG.md`, Abschnitt `m04_shap.py`: „Zu rechnen
auf den EINDEUTIGEN Stadtteil-Merkmalskombinationen, nicht auf allen Zeilen:
Die Strukturmerkmale sind innerhalb eines Jahres konstant".

**Was aufgefallen ist.** Die Begruendung stimmt fuer ACS und Land Use, aber
nicht mehr fuer alle Praediktoren: Seit Decision Log #17 ist
`log_kriminalitaetsindex` ein **monatlich rollierender** Index. Ein
`drop_duplicates()` ueber alle zehn Praediktoren liefert deshalb **3.757 von
3.828** Zeilen — die Entdopplung entfernt 1,9 % statt der beabsichtigten rund
90 %.

**Wie ausgelegt.** `_vif()` weist zwei Bezugsmengen aus: `stadtteil_jahr` (eine
Zeile je Stadtteil und Jahr, 319 Zeilen — die Ebene, auf der die
Strukturmerkmale tatsaechlich variieren) und `alle_zeilen` zum Vergleich.

**Was das fuer die dokumentierte Zahl heisst.** Gemessen auf `stadtteil_jahr`
und den 29 Entwicklungsstadtteilen liegt der hoechste VIF bei **12,29**
(`median_haushaltseinkommen`). Die bisher berichteten 11,5 stammen aus einer
anderen Bezugsmenge. Die inhaltliche Aussage bleibt unveraendert — deutliche
Multikollinearitaet, blockweise interpretieren —, aber die Zahl ist vor der
Verwendung im Text zu ersetzen.

---

## B-19 · R-9 ist beziffert und faellt weg

**Fundstelle:** `docs/06_RISIKEN.md` R-9, „Spezifikationsasymmetrie zwischen
Baseline und Vergleichsverfahren".

**Was aufgefallen ist.** R-9 nimmt an, die Negative Binomial habe durch den
Offset `log(Bevoelkerung)` einen strukturellen Vorteil, weil dessen Koeffizient
fest auf 1 steht, waehrend die Vergleichsverfahren den Zusammenhang schaetzen
muessen. Beim Schreiben der Zusatzvariante fiel auf: **`log_bevoelkerung` steht
in `PRAEDIKTOREN`** und ist damit auch in der Offset-Variante ein freies
Merkmal. Der Offset legt also nur einen Ausgangspunkt fest, den ein frei
geschaetzter Koeffizient wieder verschieben kann. Beide Spezifikationen sind
rechnerisch nahezu aequivalent.

**Gemessen** ueber alle 50 Laeufe, gepaart:

| Zielgroesse | Vorteil des Offsets (RMSE) | groesste Einzelabweichung |
|---|---|---|
| `anzahl_einsaetze` | **−0,0017** | 0,0066 |
| `einsaetze_je_1000_ew` | **−0,0000** | 0,0007 |

Negatives Vorzeichen heisst: Die Variante **ohne** Offset ist minimal besser.
Der Vorteil ist nicht klein, er ist nicht vorhanden.

**Konsequenz.** R-9 kann aus dem Register genommen werden — nicht weil das
Risiko entschaerft wurde, sondern weil es nie bestand. In Kapitel 8 wird aus
dem Vorbehalt eine Fussnote mit einer Zahl. Das ist ein besseres Ergebnis als
die Entschaerfung, weil es die Frage endgueltig schliesst.

---

## B-20 · Plattformabhaengige Rundung in der letzten Stelle

**Fundstelle:** `results/regression/baselines_folds.csv`, Vergleich der
Wiederholung 0 vor und nach der Erweiterung.

**Was aufgefallen ist.** Die neu gerechneten Werte weichen um bis zu
**5,2·10⁻¹²** von den zuvor gespeicherten ab (relativ rund 10⁻¹³). Die Ursache
ist nicht die Aenderung: Die alte Datei entstand unter Windows mit Python 3.14,
die Kontrollrechnung unter Linux mit Python 3.10 und anderen BLAS-Bibliotheken.
Die Klassifikationswerte stimmen exakt ueberein, weil sie auf drei
Nachkommastellen gerundet gespeichert werden.

**Fuer die Arbeit.** Bestaetigt, warum `requirements.txt` exakt gepinnt gehoert
und warum die Laufumgebung protokolliert wird. Fuer die berichteten Zahlen
(drei bis vier Nachkommastellen) ohne Bedeutung.

---

## B-21 · Das Tuning auf Wiederholung 0 ist seit B-3 nicht mehr harmlos

**Fundstelle:** `docs/04_MODELLIERUNG.md`, Abschnitt „Hyperparameter-Suche":
„Getunt wird **einmal auf Wiederholung 0**; die gewaehlten Parameter gelten fuer
die Wiederholungen 1–9." Umgesetzt in `m02_menge.phase_tuning()` und
`m03_struktur.phase_tuning()`.

**Was aufgefallen ist — und warum es vorher niemandem auffallen konnte.** Unter
dem urspruenglich dokumentierten `versatz`-Verfahren waren alle Wiederholungen
**dieselbe Partition** mit anderen Nummern (B-3). Ein Parametersatz, der auf
Wiederholung 0 gefunden wurde, traf in Wiederholung 7 auf exakt dieselben
Trainingsstadtteile. Die Vereinfachung war damit folgenlos.

Seit die Wiederholungen sich tatsaechlich unterscheiden, gilt das nicht mehr:

```
Wiederholung 0, Fold 1:  Training auf Stadtteilen A  -> Parameter P gewaehlt
Wiederholung 3, Fold 1:  Test auf Stadtteilen B, mit B teilweise in A
                         -> P wird angewandt, obwohl es unter Kenntnis von B
                            ausgewaehlt wurde
```

Die Hyperparameter tragen also Information ueber Stadtteile, die in anderen
Wiederholungen im Test stehen. Das ist eine Form von Leakage — schwach, aber
vorhanden.

**Wie schwer wiegt es.** Gering, aus zwei Gruenden. Erstens ist ein
Parametersatz eine sehr grobe Zusammenfassung: vier bis sechs Zahlen, gewaehlt
ueber einen inneren CV, der selbst nach Stadtteil gruppiert. Zweitens betrifft
es nicht den Vergleich, sondern alle Verfahren gleichermassen — die
Fairness-Regel bleibt gewahrt, weil jedes Verfahren dieselbe Vereinfachung
erfaehrt.

**Warum trotzdem nicht behoben.** Sauber waere, je Wiederholung neu zu tunen.
Das kostet das **Zehnfache** der Tuning-Zeit — bei 8.000 Anpassungen im Tuning
also 80.000. Der Aufwand steht in keinem Verhaeltnis zum erwarteten Effekt.

**Was stattdessen zu tun ist.** In Kapitel 6 nicht nur schreiben „getunt wird
einmal, das ist eine Vereinfachung", sondern **warum** sie eine ist: weil die
Wiederholungen verschiedene Aufteilungen sind. Der bisher vorgesehene Satz
haette den Punkt verdeckt. Gehoert zusaetzlich in die Limitationen (Kapitel 8).

---

## B-22 · Rechenfehler beim Parallelisierungsgewinn

**Fundstelle:** `modelle/m02_menge.aggregiere()` und
`m03_struktur.aggregiere()`, Spalte `parallel_gewinn`, Fassung vom 06.08.2026.

**Was aufgefallen ist.** Der Quotient stand zunaechst als
`train_sekunden_mean / train_sekunden_parallel_mean`. Der Zaehler mittelt ueber
**alle 50 Laeufe**, der Nenner nur ueber die **5 Laeufe der Wiederholung 0** —
denn nur dort wird parallel gemessen. Der ausgewiesene „Gewinn" haette damit die
Schwankung zwischen den Wiederholungen enthalten statt des
Parallelisierungseffekts.

Selbst gefunden vor dem ersten echten Lauf, beim Durchgehen der Frage „welche
neuen Risiken hat die Implementierung geschaffen".

**Behoben:** Zaehler und Nenner werden beide aus Wiederholung 0 gebildet.

**Lehre fuer die Pruefauftraege:** Kennzahlen, die aus zwei verschieden
erhobenen Groessen gebildet werden, sind eine eigene Fehlerklasse. Sie sehen
plausibel aus, weil beide Bestandteile stimmen.

---

## B-23 · Hyperparameter konnten als Zeichenketten zurueckkommen

**Fundstelle:** `m02_menge.phase_tuning()` und `m03_struktur.phase_tuning()`,
`json.dumps(p, default=str)`.

**Was aufgefallen ist.** `RandomizedSearchCV.best_params_` liefert je nach
scipy- und numpy-Fassung NumPy-Skalare. `np.float64` erbt von `float` und
ueberlebt `json.dumps` zufaellig — `np.int64` erbt **nicht** von `int`. Fuer
einen solchen Wert haette der Notausgang `default=str` gegriffen, und aus
`n_estimators=321` waere die Zeichenkette `"321"` geworden. Beim Wiedereinlesen
haette `set_params(n_estimators="321")` den Lauf abgebrochen — **nach** dem
Tuning, also nach der teuersten Phase.

Auf der Testumgebung (Python 3.10, numpy 2.2, scipy 1.15) trat es nicht auf:
Dort kamen native `int` zurueck. Auf der Zielumgebung (Python 3.14, numpy 2.4,
scipy 1.17) ist das nicht garantiert. Ein Fehler, der von der Paketversion
abhaengt und erst nach Stunden zuschlaegt, ist die unangenehmste Sorte.

Gefunden, weil die Probelaeufe `tune()` durch einen Stub ersetzt hatten und
dieser Pfad deshalb ungeprueft war — die Frage „ist der Code wirklich fertig"
hat genau darauf gezeigt.

**Behoben.** `_rein_python()` wandelt NumPy-Skalare vor der Serialisierung
explizit um, und `default=` entfaellt: Ein unbekannter Typ soll laut auffallen,
nicht still zur Zeichenkette werden.

**Gegengeprueft** mit echtem Tuning: `tune()` → `tuning.csv` →
`_parameter_je_fold()` → `set_params()` → `fit()`, inklusive Anwendung der
Parameter aus Wiederholung 0 auf Wiederholung 3. Alle Typen nach dem
Wiedereinlesen korrekt.

---

## B-24 · XGBoost ist nicht threaddeterministisch

**Fundstelle:** erster echter Lauf von `m02_menge.py` auf der Zielumgebung,
06.08.2026, Phase 2, Wiederholung 0.

**Was aufgefallen ist.** `ein_lauf(..., auch_parallel=True)` fittet dasselbe
Modell mit denselben Parametern und demselben `random_state` zweimal — einmal
einkernig, einmal ueber alle Kerne — und verglich die Vorhersagen. Bei Ridge und
Random Forest waren sie identisch. Bei XGBoost lag die groesste Abweichung bei
**34,66**, gemessen auf `anzahl_einsaetze` mit einem Mittelwert von rund 76.

Das ist keine Rundung, das sind andere Modelle.

**Ursache.** XGBoost baut die Histogramme parallel und reduziert sie ueber die
Threads. Die Summierungsreihenfolge von Fliesskommazahlen haengt damit an der
Threadzahl; winzige Unterschiede in den Gradientensummen kippen knapp
benachbarte Split-Kandidaten, und ueber mehrere hundert Baeume — hier bis zu 898
bei `learning_rate` 0,026 — verstaerkt sich die Abweichung. `random_state`
steuert die Stichprobenziehung, nicht die Reduktionsreihenfolge.

**Warum die Ergebnisse unberuehrt bleiben.** Alle berichteten Guetemasse
stammen aus dem **einkernigen** Fit (`N_JOBS_MODELL = 1`, Decision Log #40).
Der parallele Fit dient allein der Zeitmessung. Die Entscheidung, einkernig zu
messen, war fuer die Vergleichbarkeit der Laufzeiten gedacht — sie stellt
nebenbei die Reproduzierbarkeit sicher. Ein gluecklicher Nebeneffekt, der
hier ausdruecklich genannt sei.

**Behandlung.** Der urspruengliche `assert` hat den Lauf beendet. Das war
falsch konstruiert: Ein **Diagnosewert darf einen mehrstuendigen Lauf nicht
abbrechen**, zumal er nichts ueber die Gueltigkeit der Ergebnisse aussagt.
Ersetzt durch die gemessene Spalte `parallel_abweichung` je Lauf und
`parallel_abweichung_max` je Verfahren; `main()` weist am Ende darauf hin.

**Fuer die Arbeit — das ist ein verwertbarer Befund, kein Makel.** Kapitel 6
behauptet Reproduzierbarkeit. Diese Behauptung ist fuer XGBoost **nur mit
Angabe der Threadzahl** haltbar. Zu schreiben ist also nicht „`random_state`
ist gesetzt", sondern: `random_state` ist gesetzt **und** die Modelle laufen
einkernig, weil XGBoost sonst je nach Kernzahl andere Baeume erzeugt — belegt
mit der gemessenen Abweichung. Das ist genau die Sorte Detail, die den
Unterschied zwischen einer behaupteten und einer geprueften Reproduzierbarkeit
ausmacht.

---

## B-25 · Ein Abbruch in Phase 2 vernichtete das Tuning

**Fundstelle:** `m02_menge.main()`, Ablauf der Phasen.

**Was aufgefallen ist.** Als der Lauf am 06.08.2026 in Phase 2 abbrach (B-24),
waren die rund **50 Minuten** aus Phase 1 verloren — obwohl `tuning.csv`
laengst geschrieben war und die gefundenen Parameter allein von Wiederholung 0
abhaengen, die durch die Parquet-Dateien festliegt.

**Behoben.** `phase_tuning()` liest eine vollstaendige `tuning.csv` wieder ein,
statt neu zu suchen; die Zeilenzahl wird gegen die erwartete geprueft.
Neuberechnung erzwingt man mit dem Argument `neutuning`. Gleiches in `m03`.

**Nebeneffekt fuer die Arbeit:** Die Trennung von Tuning und Bewertung ist
damit auch praktisch belegt und nicht nur behauptet — die Parameter stehen als
Datei zwischen beiden Phasen und sind fuer Kapitel 6.3 nachlesbar.

---

## B-26 · Kein Verfahren schlaegt die Stufe-2-Baseline

**Fundstelle:** erster vollstaendiger Lauf von `m02_menge.py`, 06.08.2026;
`results/regression/vergleich.csv`, Rolle `primaer`, Teststufe `wiederholung`.

**Das Ergebnis.** Gepaarte Differenz gegen die Negative Binomial, positiv heisst
das Verfahren ist besser:

| Zielgroesse | Verfahren | Differenz RMSE | gewonnen | p | Befund |
|---|---|---|---|---|---|
| `anzahl_einsaetze` | Ridge | −1,04 | 2/10 | 0,084 | nicht unterscheidbar |
| `anzahl_einsaetze` | Random Forest | −18,25 | 0/10 | 0,002 | **signifikant schlechter** |
| `anzahl_einsaetze` | XGBoost | −13,13 | 0/10 | 0,002 | **signifikant schlechter** |
| `einsaetze_je_1000_ew` | Ridge | −0,27 | 1/10 | 0,049 | **signifikant schlechter** |
| `einsaetze_je_1000_ew` | Random Forest | −0,49 | 1/10 | 0,027 | **signifikant schlechter** |
| `einsaetze_je_1000_ew` | XGBoost | +0,13 | 7/10 | 0,432 | nicht unterscheidbar |

**Kein einziges der drei Verfahren schlaegt die Messlatte** — in keiner der
beiden Zielgroessen. Zweimal lautet der Befund „nicht unterscheidbar", viermal
„signifikant schlechter".

**Das ist ein Ergebnis, kein Fehler** (Gutachten R6, `CLAUDE.md` Abschnitt 4).
Es beantwortet Unterfrage 2 klar: Der Mehraufwand der drei
Vergleichsverfahren lohnt sich auf diesem Datensatz nicht.

**Widerspruch zu R-1.** Das Risikoregister erwartete auf Basis des Vortests vom
28.07.2026 das Gegenteil: „Bei `einsaetze_je_1000_ew` sieht es umgekehrt aus:
RF 0,584 ± 0,19 gegen Ridge −0,087 ± 0,89, ein Abstand von 0,67." Gemessen wird
jetzt R² 0,283 fuer Ridge gegen 0,163 fuer Random Forest — die Reihenfolge hat
sich umgedreht. Erklaerbar: Der Vortest lief ungetunt, mit 20 statt 50
Fold-Ergebnissen und auf der Fold-Zuteilung **vor** der doppelten
Stratifizierung. Er war ausdruecklich als vorlaeufig gekennzeichnet. R-1 ist
damit erledigt — die Frage „sind die Verfahren unterscheidbar" ist bei
`anzahl_einsaetze` mit ja beantwortet, bei der Rate mit nein.

**Die Zahlen oben stammen aus dem Lauf VOR der Korrektur der Verlustfunktion
(#42).** Nach der Umstellung auf Tweedie/Poisson lauten sie −21,42 und −17,68
bei `anzahl_einsaetze` sowie +0,22 (p 0,275), +0,04 (p 0,846) und −0,27
(p 0,049) bei der Rate. **Der Befund selbst ist unveraendert: kein Verfahren
schlaegt die Baseline, unter beiden Spezifikationen.** Massgebliche Zahlen in
`03_STAND.md`, Abschnitt 5.1.

**ERKLAERUNG KORRIGIERT am 06.08.2026 (B-31).** Hier stand zuvor, die Ursache
sei die Extrapolation: Bei 33,7 % Testzeilen ausserhalb des Trainingsbereichs
seien parametrische Modelle im Vorteil, weil Baeume dem Randblatt zuordnen.
**Das wurde geprueft und trifft nicht zu** — der Rueckstand der Baumverfahren
haengt nicht vom Extrapolationsanteil ab (Spearman +0,020 und +0,011, p ≈ 0,9).

Was die Daten stuetzen, ist die **Groessenskalierung**: Bei `anzahl_einsaetze`
liegen die Baumverfahren 20,4 bzw. 16,6 RMSE hinter Ridge, bei
`einsaetze_je_1000_ew` 0,49 bzw. 0,31 **davor**. Der einzige Unterschied
zwischen beiden Zielgroessen ist die Einwohnerzahl. Negative Binomial und Ridge
bilden „Einsaetze wachsen ungefaehr proportional zur Bevoelkerung" ueber die
Log-Verknuepfung direkt ab; ein Baum muss das aus stueckweise konstanten Splits
nachbauen.

**Status dieser Erklaerung: plausibel und mit dem Zielgroessenvergleich
vereinbar, aber nicht direkt getestet.** Sie ist im Text als solche zu
kennzeichnen — nicht noch einmal als gesichert weiterzureichen. Ein direkter
Test waere moeglich (siehe „Offene Pruefungen" am Ende dieser Datei).

---

## B-27 · Das Tuning-Leakage ist messbar folgenlos

**Fundstelle:** `results/regression/leakage_diagnose.csv`, erster Lauf.

**Was gemessen wurde.** B-21 befuerchtete, der Vorsprung gegen die Baseline
falle in den Wiederholungen 1–9 systematisch groesser aus als in der
leakage-freien Wiederholung 0. Ergebnis, in Einheiten von `std_folds`:

| Zielgroesse | Verfahren | Differenz W1-9 gegen W0 |
|---|---|---|
| `anzahl_einsaetze` | Ridge | +0,017 |
| `anzahl_einsaetze` | Random Forest | +0,119 |
| `anzahl_einsaetze` | XGBoost | −0,181 |
| `einsaetze_je_1000_ew` | Ridge | −0,016 |
| `einsaetze_je_1000_ew` | Random Forest | −0,340 |
| `einsaetze_je_1000_ew` | XGBoost | −0,106 |

**Kein systematisches Muster.** Waere das Leakage wirksam, muessten alle sechs
Differenzen **positiv** sein. Zwei sind es, vier nicht, und die groesste
Abweichung geht in die entgegengesetzte Richtung. Die Streuung liegt in der
Groessenordnung der Fold-Schwankung selbst.

**Konsequenz.** Die Vereinfachung „einmal tunen auf Wiederholung 0" ist
empirisch unbedenklich. In Kapitel 6 ist sie weiterhin zu benennen, jetzt aber
mit dieser Messung statt mit einem Vorbehalt — dieselbe Aufloesung wie bei R-9.
Die Frage an Schroeter kann entfallen oder als Information formuliert werden.

---

## B-28 · Parallelisierung macht XGBoost hier langsamer

**Fundstelle:** `results/regression/menge_mittel.csv`, Spalte `parallel_gewinn`.

Gemessen auf der Zielmaschine: Ridge 1,45 und 1,02 · Random Forest 1,82 und
2,18 · **XGBoost 0,64 und 0,71**.

Ein Wert unter 1 heisst: Der Fit ueber alle Kerne dauert **laenger** als der
einkernige. Bei 3.036 Zeilen und zwoelf Merkmalen uebersteigt der Aufwand fuer
Threadverwaltung und Histogramm-Reduktion den Nutzen der Verteilung. Random
Forest profitiert dagegen, weil seine Baeume unabhaengig sind und sich ohne
Kommunikation verteilen lassen.

**Fuer Unterfrage 4 verwertbar:** Parallelisierbarkeit ist kein Freibrief. Auf
Datensaetzen dieser Groessenordnung kann sie negativ ausfallen — eine Aussage,
die ohne die getrennte Messung (Decision Log #39) nicht moeglich gewesen waere.

---

## B-29 · In der Klassifikation schlagen BEIDE Verfahren die Stufe-2-Baseline

**Fundstelle:** erster vollstaendiger Lauf von `m03_struktur.py`, 06.08.2026;
`results/klassifikation/vergleich.csv`.

**Das Ergebnis**, gepaart gegen die multinomiale logistische Regression
(Macro-F1 0,298), positiv heisst besser:

| Verfahren | Macro-F1 | Differenz | gewonnen | p | Befund |
|---|---|---|---|---|---|
| Random Forest | 0,3276 ± 0,0129 | +0,0296 | 10/10 | 0,002 | **signifikant besser** |
| XGBoost | 0,3343 ± 0,0128 | +0,0362 | 9/10 | 0,004 | **signifikant besser** |
| RF gegen XGBoost | – | −0,0067 | 2/10 | 0,131 | nicht unterscheidbar |

Macro-AUROC bestaetigt es: 0,735 und 0,751 gegen 0,711 der Baseline.

**Widerspruch zu R-2 — in die guenstige Richtung.** Das Risikoregister hielt
fest, der Mehraufwand sei im Klassifikationsstrang „vorab **nicht** belegt",
weil eine flache Baum-Sonde (Macro-F1 0,270) die logistische Regression nicht
schlug. Getunte Ensembles schlagen sie nun beide, und zwar in 10 von 10 bzw.
9 von 10 Wiederholungen — die Richtung ist damit eindeutig, nicht knapp.

**Was von R-2 bestehen bleibt: der geringe Ertrag.** 0,334 gegenueber 0,223 der
Mehrheitsklasse ist bei einem Maximum von 1,0 weiterhin ein Bruchteil des
moeglichen Signals. Der Strang traegt also **mehr als befuerchtet, aber wenig
in absoluten Zahlen**. Beide Haelften gehoeren in die Limitationen.

**Einschraenkung, die dazugesagt werden muss:** In den 29
Entwicklungsstadtteilen liegen insgesamt nur **33 brand-dominierte Monate**
(13 · 9 · 6 · 3 · 2 je Fold). Macro-F1 mittelt ueber vier Klassen gleich stark;
ein Viertel des Guetemasses haengt damit an zwei bis dreizehn Testfaellen je
Fold. Das erklaert einen Teil der Fold-Streuung (`std_folds` 0,05 gegen
`std_wiederholungen` 0,013) und begrenzt die Praezision der Aussage.

---

## B-30 · Der Kernbefund: der Mehraufwand lohnt sich je nach Aufgabe verschieden

**Fundstelle:** beide Straenge zusammen, 06.08.2026.

Dies ist kein Fehler und keine Luecke, sondern das inhaltlich wichtigste
Ergebnis der Arbeit — hier festgehalten, weil es beim Zusammenfuehren der
beiden Laeufe entstand und in keiner Einzelauswertung sichtbar ist.

| Strang | Schlaegt ein Verfahren die Stufe-2-Baseline? |
|---|---|
| **Menge** (`anzahl_einsaetze`, `einsaetze_je_1000_ew`) | **Nein — keines von dreien**, in keiner der beiden Zielgroessen |
| **Struktur** (`dominante_einsatzart`) | **Ja — beide**, signifikant |

Dieselben Merkmale, dieselben Stadtteile, dieselben Folds, dieselben zwei
Ensembles. Nur die Aufgabe wechselt — und mit ihr das Vorzeichen der Antwort.

**ACHTUNG — Erklaerungen ueberarbeitet am 06.08.2026.** Die urspruengliche
Fassung nannte zwei Mechanismen und behauptete, beide seien „durch die Messung
bestaetigt". Der erste ist inzwischen widerlegt, der zweite nie geprueft worden.
Stand jetzt:

- **Menge: NICHT die Extrapolation.** Geprueft und ausgeschlossen (B-31).
  Was die Daten stuetzen, ist die Groessenskalierung — die Baumverfahren
  verlieren dort, wo die Zielgroesse von der Einwohnerzahl dominiert wird, und
  gewinnen, wo sie herausgerechnet ist. **Plausibel, nicht direkt getestet.**
- **Struktur: Form der Klassengrenze.** Die Zielgroesse entsteht als `argmax`
  ueber vier Anteile; die Grenze liegt dort, wo sich zwei Anteile schneiden —
  im Merkmalsraum eine Schnittflaeche, keine Hyperebene (`06_RISIKEN.md`, R-2).
  **Ebenfalls plausibel, ebenfalls nicht getestet.** Als Indiz dafuer, dass die
  Grenze schwer zu ziehen ist: In **20,8 %** der Zeilen liegen der groesste und
  der zweitgroesste Anteil weniger als 0,10 auseinander.

**Zu Unterfrage 4.** Hier stand zuvor, die Frage sei „besser beantwortet, als
eine einheitliche Antwort es koennte". Das war Schoenfaerberei. Nuechtern:

Die **Beobachtung** ist belastbar — auf demselben Datensatz, mit denselben
Merkmalen, Stadtteilen und Folds lohnt sich der Mehraufwand in der Menge nicht
und in der Struktur schon. Die **Erklaerung** dafuer ist es derzeit nicht. Ohne
sie bleibt UF4 eine Beobachtung ohne Mechanismus, und das ist fuer eine
Implikationsfrage zu wenig. Der Mechanismustest sollte deshalb gerechnet werden
(siehe unten).

---

## B-33 · Der Mechanismus ist belegt: es ist die Groessenskalierung

**Fundstelle:** `m04_shap.ablation_exposition()`,
`results/shap/ablation_exposition.csv`. Gemessen 06.08.2026 auf
Wiederholung 0 (5 Folds); der volle Lauf ueber alle 10 Wiederholungen steht aus
und wird die Groessenordnung bestaetigen oder korrigieren.

**Der Test.** Die Baumverfahren wurden auf der RATE trainiert und ihre
Vorhersage mit der Einwohnerzahl zurueckmultipliziert — genau die Rechnung, die
die Negative Binomial ueber ihren Offset vornimmt. Bewertet gegen
`anzahl_einsaetze`, also gegen dieselbe Zielgroesse wie im Hauptlauf.

| Modell | RMSE | R² | Rueckstand gegen die Baseline |
|---|---|---|---|
| **Negative Binomial (mit Offset)** | **37,44** | 0,472 | – |
| Random Forest, direkt auf der Anzahl | 67,71 | −1,536 | **+30,27** |
| Random Forest, ueber die Rate | **36,43** | 0,523 | **−1,00** |
| XGBoost, direkt auf der Anzahl | 61,70 | −0,637 | **+24,27** |
| XGBoost, ueber die Rate | **35,74** | 0,607 | **−1,69** |

**Der gesamte Rueckstand verschwindet.** Von +30 bzw. +24 RMSE auf −1,0 und
−1,7 — beide Baumverfahren ziehen an der Baseline vorbei, sobald ihnen die
Groessenskalierung vorgegeben wird statt sie lernen zu lassen.

**Damit ist die Erklaerung belegt, nicht mehr vermutet.** Baumverfahren geben je
Blatt einen festen Wert aus und koennen „Einsaetze = Bevoelkerung x Risiko"
nicht abbilden; sie ziehen Extremwerte zur Blattmitte, und RMSE auf der
Originalskala wird von den grossen Stadtteilen dominiert (Tenderloin 280,
Seacliff 6,4). Negative Binomial und Ridge bekommen die Multiplikation ueber die
Log-Verknuepfung geschenkt.

**Der entscheidende Faktor ist damit nicht die Verfahrensklasse, sondern die
Spezifikation.** Das ist eine uebertragbare Aussage und die beste Antwort auf
Unterfrage 4, die dieser Datensatz hergibt:

> Bei tabellarischen Prognoseaufgaben mit einer Groessen- oder Expositionsgroesse
> entscheidet weniger die Wahl des Verfahrens als die Frage, ob die
> Groessenbeziehung in der Modellspezifikation abgebildet ist. Verfahren mit
> Log-Verknuepfung oder Offset bekommen sie geschenkt; Baumverfahren muessen sie
> aus stueckweise konstanten Splits nachbauen und scheitern daran. Wird ihnen
> dieselbe Struktur vorgegeben, sind sie konkurrenzfaehig.

**STATUS — wichtig.** Diagnostik, **nicht** Teil des Verfahrensvergleichs. Der
Hauptbefund bleibt unveraendert: Unter der vorab festgelegten Spezifikation
schlaegt kein Verfahren die Stufe-2-Baseline. Der Test erklaert, **warum**, und
zeigt, was sich aendern wuerde — er ersetzt das Ergebnis nicht.

**Warum die Spezifikation NICHT nachtraeglich geaendert wird.** Es waere
verlockend, den Baumverfahren die Groessenskalierung im Hauptlauf zu geben und
ein freundlicheres Ergebnis zu berichten. Dagegen sprechen drei Dinge: Erstens
hat #29/R-9 ausdruecklich festgelegt, den Vorteil **nicht** auszugleichen,
sondern zu beziffern — genau das ist hier geschehen. Zweitens waere die
Aenderung durch das Ergebnis motiviert und nicht durch ein Argument. Drittens
verlangt Ridge dieselbe Struktur nicht und kommt trotzdem zurecht; dass
Baumverfahren sie brauchen, **ist** der Befund. Ihn wegzukonstruieren hiesse,
das Ergebnis zu loeschen.

---

## B-34 · R-9 war berechtigt — unser Test war der falsche

**Fundstelle:** `06_RISIKEN.md` R-9, entschaerft durch B-19 am 05.08.2026.

**Was passiert ist.** R-9 vermutete eine Spezifikationsasymmetrie: Die Negative
Binomial bekommt `log(Bevoelkerung)` als Offset, die Vergleichsverfahren nur als
gewoehnliches Merkmal. Wir haben das geprueft, indem wir der Baseline den Offset
**wegnahmen** — Ergebnis: −0,0017 RMSE, also nichts. Daraus wurde geschlossen,
das Risiko habe nie bestanden, und R-9 wurde aus dem Register genommen.

**Das war der falsche Test.** Er beantwortet die Frage „gewinnt die Baseline
durch den Offset?" Die relevante Frage lautete aber „**verlieren die
Vergleichsverfahren, weil sie ihn nicht haben?**" — und darauf lautet die
Antwort nach B-33: ja, um **24 bis 30 RMSE**.

Beide Messungen sind korrekt und widersprechen sich nicht. Fuer ein Modell mit
Log-Verknuepfung und freiem Koeffizienten auf `log_bevoelkerung` ist der Offset
redundant; er legt nur einen Ausgangspunkt fest, den der Koeffizient wieder
verschiebt. Fuer einen Baum, der weder Log-Verknuepfung noch Koeffizienten hat,
ist er es nicht.

**Konsequenz.** R-9 kommt ins Register zurueck — nicht als Risiko fuer die
Baseline, sondern als **Spezifikationsasymmetrie zulasten der Baumverfahren**,
jetzt beziffert. B-19 bleibt gueltig, aber unvollstaendig; die Ergaenzung steht
hier.

**Lehre, die in die Reflexion gehoert:** Eine Asymmetrie laesst sich von zwei
Seiten pruefen, und die beiden Pruefungen koennen zu entgegengesetzten
Schluessen fuehren. Wir haben die bequemere Seite gemessen und daraus eine
Entwarnung abgeleitet. Das ist derselbe Fehlertyp wie bei der
Extrapolationserklaerung (B-31): eine plausible Aussage nicht bis zur
entscheidenden Messung durchgezogen.

---

## B-36 · Die Klassifikations-Baseline war ungetunt — der Vorsprung halbiert sich

**Fundstelle:** `vorpruefung/v1_baselines.klassifikation()`, Fassung bis
06.08.2026: `LogisticRegression(max_iter=2000, class_weight="balanced")`.

**Was aufgefallen ist.** `C` blieb auf dem scikit-learn-Vorgabewert 1,0.
Waehrend Random Forest und XGBoost je 50 Tuning-Iterationen bekamen, lief die
Messlatte mit einer Voreinstellung. Das ist eine Asymmetrie **zugunsten der
Vergleichsverfahren** — und ausgerechnet im Klassifikationsstrang, dem
einzigen, in dem sie die Baseline schlagen (B-29).

**Nach dem Tuning** (gleiches Budget, gleicher innerer CV, gleiches Scoring):

| | vorher (C = 1,0) | getunt |
|---|---|---|
| Macro-F1 | 0,298 | **0,314** |
| Macro-AUROC | 0,711 | **0,757** |
| Accuracy | 0,588 | 0,643 |

**Der Vorsprung der Verfahren halbiert sich mehr als:**

| | vorher | jetzt |
|---|---|---|
| Random Forest gegen Stufe 2 | +0,0296 | **+0,0136** |
| XGBoost gegen Stufe 2 | +0,0362 | **+0,0203** |

Ob er signifikant bleibt, entscheidet der naechste `m03`-Lauf. Bei einer
Streuung `std_wiederholungen` von 0,013 ist ein mittlerer Vorsprung von 0,0136
nicht mehr komfortabel.

**Die gewaehlten Werte sind aufschlussreich:** C = 0,0013 · 0,0039 · 0,0054 ·
0,0013 · 0,0189 je Fold. Alle liegen zwei bis drei Groessenordnungen **unter**
dem Vorgabewert 1,0. Das optimale Modell ist also weit staerker regularisiert
als die Voreinstellung — bei 23 Trainingsstadtteilen und zwoelf Merkmalen
plausibel, aber ohne Tuning nicht zu erraten.

**Achtung, Randlage:** In zwei von fuenf Folds liegt das Optimum praktisch am
unteren Rand des Suchraums (1e-3). Das Optimum koennte darunter liegen, dann
waere die Baseline noch besser und der Vorsprung noch kleiner. Der Suchraum
wurde symmetrisch zu Ridges `alpha` (1e-3 bis 1e3) gewaehlt; ihn nur fuer die
Baseline zu erweitern waere eine neue Asymmetrie. Dokumentiert statt einseitig
geaendert — im Text zu benennen.

**Lehre, dieselbe wie bei B-31 und B-34:** Eine Asymmetrie faellt nur auf, wenn
man beide Seiten prueft. Die Baseline nicht zu tunen sah wie Sparsamkeit aus
(„sie ist die Referenz und keine Kandidatin") und war in der Regression auch
korrekt — dort hat die Negative Binomial keinen freien Hyperparameter. In der
Klassifikation traf dieselbe Begruendung nicht zu, und das ist niemandem
aufgefallen, bis die Frage gestellt wurde, welche Modelle eigentlich getunt
werden.

---

## Offene Pruefungen

**~~1 · Mechanismustest zur Groessenskalierung~~** ✅ **erledigt am 06.08.2026,
siehe B-33.** Der Mechanismus ist belegt.

**~~2 · Faktorgruppen fuer den Mengenstrang (UF1)~~** ✅ **erledigt, siehe
B-35.**

**3 · Beides ueber alle 10 Wiederholungen rechnen.** Die Zahlen in B-33 und
B-35 stammen aus Wiederholung 0. Der volle Lauf laeuft mit `m04_shap.py` mit
und ist vor der Verwendung im Text abzuwarten.

---

## B-35 · Faktorgruppen im Mengenstrang — die Antwort auf UF1

**Fundstelle:** `m04_shap.faktorgruppen_negbin()`,
`results/shap/faktorgruppen_menge.csv`. Standardisierte Beitraege
(|Koeffizient| x Standardabweichung), Negative Binomial auf dem Fold mit dem
geringsten Extrapolationsanteil.

**Warum aus der Baseline.** `m04` ueberspringt alle Regressionsmodelle, weil
keines seine Baseline schlaegt — Beitraege eines unterlegenen Modells
auszuweisen hiesse, Rauschen zu erklaeren. Das **beste Modell des
Mengenstrangs ist die Negative Binomial**; ihre Koeffizienten beantworten UF1
direkt. Das ist kein Notbehelf, sondern die Konsequenz des Befunds.

| Faktorgruppe | Anteil |
|---|---|
| baulich | **31,0 %** |
| kriminalitaetsbezogen | 25,6 % |
| soziooekonomisch | 23,2 % |
| Groessenkontrolle | 15,3 % |
| Saison | 4,9 % |

**Alle drei Faktorgruppen des Exposes tragen bei**, und zwar in vergleichbarer
Groessenordnung — das beantwortet UF1 mit ja.

**Zwei Einzelbefunde, die im Text stehen sollten:**

- Staerkstes Einzelmerkmal ist `log_kriminalitaetsindex` mit 25,6 % (p < 0,0001).
  Der Kriminalitaetsindex traegt allein so viel wie die gesamte
  soziooekonomische Gruppe.
- `median_haushaltseinkommen` traegt **0,3 %** bei einem p-Wert von **0,80** —
  praktisch nichts. Auch `leerstandsquote_pct` ist nicht signifikant (p = 0,17).
  Das klassische soziooekonomische Merkmal schlechthin liefert keinen Beitrag,
  sobald Armuts- und Akademikerquote im Modell stehen. Erklaerbar ueber die
  Multikollinearitaet — `median_haushaltseinkommen` hat mit 12,29 den hoechsten
  VIF ueberhaupt.
- `anteil_wohngebaeude_pct` wirkt mit −0,338 **negativ**: Je hoeher der
  Wohnanteil, desto weniger Einsaetze je Einwohner. Plausibel, weil gewerblich
  gepraegte Gebiete tagsueber eine viel hoehere Anwesenheitsbevoelkerung haben,
  als die Wohnbevoelkerung ausweist.

---

## B-31 · Die Extrapolation erklaert den Rueckstand der Baumverfahren NICHT

**Fundstelle:** `06_RISIKEN.md` R-3 („die Verfahren werden ungleich getroffen"),
`03_STAND.md` Abschnitt 3 („Die Spanne von 3,6 % bis 57,4 % erklaert einen
erheblichen Teil der Fold-Streuung"), sowie meine eigene Deutung in B-26 und
B-30. Gemessen am 06.08.2026 aus `menge_folds.csv`.

**Der entscheidende Test.** Wenn Baumverfahren verlieren, WEIL sie nicht
extrapolieren koennen, dann muss ihr Rueckstand gegenueber Ridge mit dem
Extrapolationsanteil eines Laufs wachsen. Gemessen ueber alle 50 Laeufe
(Spearman):

| Zielgroesse | Verfahren | rho | p | mittlerer Rueckstand |
|---|---|---|---|---|
| `anzahl_einsaetze` | Random Forest | **+0,020** | 0,891 | +20,38 |
| `anzahl_einsaetze` | XGBoost | **+0,011** | 0,939 | +16,64 |
| `einsaetze_je_1000_ew` | Random Forest | −0,238 | 0,095 | −0,49 |
| `einsaetze_je_1000_ew` | XGBoost | −0,077 | 0,593 | −0,31 |

**Der Zusammenhang ist null.** Der Rueckstand betraegt konstant rund 20 RMSE,
ob ein Fold 3,6 % oder 57,4 % Extrapolation hat. Die Baumverfahren sind
ueberall schlechter, nicht dort, wo sie extrapolieren muessten.

Auch der einfachere Zusammenhang traegt nicht: Extrapolationsanteil gegen RMSE
ergibt rho 0,14 bis 0,31, und die Werte sind fuer Ridge (0,184) und Random
Forest (0,185) praktisch identisch. Waere Extrapolation der Hebel, muesste
Ridge deutlich flacher liegen.

**Damit ist auch die Aussage in `03_STAND.md` nicht haltbar**, die Spanne
erklaere „einen erheblichen Teil der Fold-Streuung". Bei rho ≈ 0,18 sind das
rund 3 % der Rangvarianz.

**Was stattdessen passt — und was die Daten stuetzen.** Der Unterschied
zwischen den beiden Zielgroessen ist genau die Groessenskalierung:

| | Baumverfahren gegen Ridge |
|---|---|
| `anzahl_einsaetze` (von der Einwohnerzahl dominiert) | **+20,4 / +16,6 schlechter** |
| `einsaetze_je_1000_ew` (Einwohnerzahl herausgerechnet) | **−0,49 / −0,31 besser** |

Wird die Bevoelkerung herausgerechnet, drehen die Baumverfahren von deutlich
schlechter auf leicht besser. Der plausible Mechanismus ist damit nicht die
Extrapolation, sondern die **multiplikative Struktur**: Negative Binomial
(log-Verknuepfung) und Ridge (`log(1+y)`) bilden „Einsaetze wachsen ungefaehr
proportional zur Einwohnerzahl" direkt ab; ein Baum muss diesen Zusammenhang
aus achsenparallelen, stueckweise konstanten Splits nachbauen. Auf der Rate
entfaellt die Aufgabe — und dort sind sie konkurrenzfaehig.

Das passt auch zu B-26: Die Umstellung auf Tweedie/Poisson (#42) verschlechterte
`anzahl_einsaetze` und verbesserte die Rate — dieselbe Trennlinie.

**Wie damit umzugehen ist.** Die Extrapolation bleibt eine dokumentierte
Eigenschaft des Validierungsrahmens (33,7 %, R-3) und begrenzt die
Generalisierbarkeit. Sie darf aber **nicht mehr als Erklaerung fuer den
Verfahrensunterschied** verwendet werden — weder in Kapitel 7 noch in 8. Der
Satz muss lauten: geprueft und nicht bestaetigt. Das ist ein staerkerer Beitrag
als die urspruengliche Vermutung, weil er eine naheliegende Erklaerung
ausschliesst statt sie zu wiederholen.

**Selbstkritik, die in die Reflexion gehoert:** Die Extrapolationserklaerung
stand in R-3, klang plausibel und wurde von mir in B-26 und B-30 als gesichert
weitergereicht, ohne sie zu pruefen. Genau davor warnt das Gutachtenkriterium
zur kritischen Reflexion. Sie war zwei Zeilen Auswertung entfernt.

---

## B-32 · Woher die Extrapolation kommt

Deskriptiv, aus demselben Lauf. Erklaert nicht den Verfahrensunterschied
(B-31), beschreibt aber den Validierungsrahmen.

**Kein Merkmal dominiert.** Anteil der Testzeilen, die im jeweiligen Merkmal
ausserhalb des Trainingsbereichs liegen, gemittelt ueber die fuenf Folds:

| Merkmal | Anteil |
|---|---|
| `anteil_risikogewerbe_pct` | 10,0 % |
| `log_bevoelkerung` | 8,8 % |
| `median_miete` | 7,3 % |
| `anteil_altbau_vor_1940_pct` | 6,7 % |
| `anteil_wohngebaeude_pct` | 6,7 % |
| … bis `akademikerquote_pct` | 4,1 % |
| `monat_sin`, `monat_cos` | 0,0 % |

Die Summe der Einzelanteile betraegt 62,7 %, waehrend 33,7 % der Zeilen in
mindestens einem Merkmal ausbrechen — die Faelle ueberlappen also stark.
**Ein einzelnes Merkmal zu entfernen wuerde kaum etwas aendern.** Die
Saisonmerkmale brechen nie aus, weil Sinus und Kosinus beschraenkt sind und
jeder Fold alle zwoelf Monate enthaelt.

**Es ist eine Eigenschaft von Stadtteilen, nicht von Zeilen.** Weil die
Strukturmerkmale innerhalb eines Stadtteils nahezu konstant sind, bricht ein
Stadtteil entweder ganz aus oder gar nicht:

- **9 von 29** Stadtteilen liegen mit **100 %** ihrer Zeilen ausserhalb —
  Chinatown, South Of Market, Financial District/South Beach, Marina, Haight
  Ashbury, Sunset/Parkside, Twin Peaks, Seacliff, Presidio
- **16 von 29** liegen bei **0 %**
- nur wenige liegen dazwischen (Tenderloin 45 %)

**Fuer Kapitel 4 und 8 verwertbar:** Die Zahl „33,7 % der Testzeilen" ist
irrefuehrend praezise. Die richtige Formulierung lautet: Rund ein Drittel der
Stadtteile San Franciscos ist in mindestens einem Strukturmerkmal so
ungewoehnlich, dass kein anderer Stadtteil sie abdeckt. Das ist eine Aussage
ueber die Stadt, nicht ueber die Modelle.

---

## Was NICHT verifiziert werden konnte

Zur Ehrlichkeit gehoert, die Grenzen der Pruefung zu benennen.

**Die Zielumgebung — teilweise geschlossen am 06.08.2026.** Alle Pruefungen
liefen unter Python 3.10, pandas 2.3, scikit-learn 1.7, numpy 2.2; die
Ergebnisse entstehen unter Python 3.14, pandas 3.0, scikit-learn 1.8,
numpy 2.4.

`v0_aufteilung.py` und `v1_baselines.py` sind inzwischen auf der Zielumgebung
gelaufen. **Beide Ausgaben stimmen mit der Testumgebung ueberein**, auf allen
berichteten Stellen: Fold-Zuteilung und Brand-Testfaelle identisch, Baselines
37,436 / 0,472 / 4,142 / −0,237 (Wiederholung 0) und 37,272 / 0,477 / 4,407 /
0,024 (alle Wiederholungen), Klassifikation 0,223 und 0,298, Macro-AUROC 0,711,
null Konvergenzwarnungen.

Das ist ein belastbarer Beleg fuer den Reproduzierbarkeitsabschnitt in
Kapitel 6: Der Aufbau liefert dieselben Zahlen ueber zwei Betriebssysteme und
vier Hauptversionsspruenge hinweg. Es bestaetigt zugleich die Einschaetzung aus
B-20 — die Abweichungen liegen in den letzten Bits und nicht in den berichteten
Stellen.

**Was offen bleibt:** `m03_struktur.tune()` uebergibt `sample_weight` als
Fit-Parameter an `RandomizedSearchCV`; das Metadata-Routing hat sich zwischen
scikit-learn-Fassungen bewegt. Dieser Pfad ist auf der Zielumgebung noch nicht
gelaufen. B-23 war ein Fall derselben Klasse.

**Ein ununterbrochener Volllauf.** Die Testumgebung raeumt
Hintergrundprozesse nach wenigen Minuten ab. Jede Phase ist einzeln und in
Kombination geprueft — Phase 1 mit echtem Tuning, Phasen 2 bis 5 mit
vorgegebenen Parametern, die Uebergabe zwischen beiden gezielt gegengeprueft —
aber nie alles in einem Durchlauf bei vollem Budget.
