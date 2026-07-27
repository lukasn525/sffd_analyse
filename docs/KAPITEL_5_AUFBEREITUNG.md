# Kapitel 5 – Data Preparation: was wurde wie umgesetzt

Stand 2026-07-27. Diese Datei ist die **Schreibvorlage für Kapitel 5**: Für jeden
Arbeitsschritt steht hier, was passiert, wo es im Code steht, welche Zahlen
herauskommen und welche Entscheidung dahintersteht.

Reihenfolge und Benennung der Schritte entsprechen dem Ablauf im Code – die
Dateinamen in `prep/` sind so nummeriert, dass die Abfolge aus der Dateiliste
ablesbar ist.

| # | Schritt | Datei | Ergebnis |
|---|---|---|---|
| 1 | Laden | `prep/s01_laden.py` | `data/raw/*` |
| 2 | Auswählen | `prep/s02_einsaetze.py` | – (Teil von Schritt 3) |
| 3 | Joinen | `prep/s02_einsaetze.py` | – |
| 4 | Raten berechnen | `prep/s02_einsaetze.py` | `einsaetze.parquet` |
| 5 | Aggregieren | `prep/s03_datensaetze.py` | – |
| 6 | Lags berechnen | `prep/s03_datensaetze.py` | `regression.parquet` + `klassifikation.parquet` |
| 7 | Eignungsprüfung | `prep/s04_eignungspruefung.py` | `results/eignungspruefung/` |

Alles zusammen läuft mit einem Befehl: `python prep\build.py`.

---

## Schritt 1 – Laden

**Datei:** `prep/s01_laden.py` · **Funktionen:** `fetch_*`, `run_download`

Sieben Quellen werden über die offenen APIs von DataSF und des US Census Bureau
geladen. Die Downloads sind über Schalter in `prep/config.py` gesteuert und
stehen per Default auf `False`; die Rohdaten liegen gecacht in `data/raw/`.

| Quelle | ID | Umfang |
|---|---|---|
| SFFD Fire Incidents | `wr8u-xric` | 719.989 Einsätze, 2003–2026 |
| Census-Tract-Crosswalk | `sevw-6tgi` | Tract ↔ Stadtteil |
| ACS 5-Year Estimates | Census API | 5 Jahrgänge × 9 Variablen |
| SFPD Kriminalität ab 2018 | `e3si-785i` | monatlich voraggregiert |
| SFPD Kriminalität historisch | `tmnf-yvry` | 2014–2017, mit Koordinaten |
| Land Use 2020 | `ygi5-84iq` | Parzellen mit Geometrie |
| Stadtteilgrenzen | `j2bu-swwd` | Polygone für Spatial Joins |

**Für die Arbeit erwähnenswert:** Zwei Kriminalitätsquellen sind nötig, weil der
aktuelle SFPD-Datensatz erst 2018-01 beginnt, der Analysezeitraum aber 2015-01.
Das ist keine Redundanz, sondern eine Lücke in den Quelldaten.

---

## Schritt 2 – Auswählen

**Datei:** `prep/s02_einsaetze.py` · **Funktion:** `prepare_sffd`
sowie `prep/s03_datensaetze.py` · **Funktionen:** `aggregiere`, `balanciertes_panel`

Die Auswahl erfolgt an zwei Stellen, weil sie zwei verschiedene Dinge betrifft:
**Zeilen bereinigen** (Einsatz-Ebene) und **Analyseeinheiten festlegen**
(Stadtteil-Ebene).

| Filter | Wirkung | Begründung |
|---|---|---|
| Dedup nach `einsatz_nummer` | −269 Zeilen (0,04 %) | Mehrfach gemeldete Einsatznummern in den Quelldaten (#7) |
| Antwortzeit 0–60 min | −1,7 % | Ausreißer und Fehlerfassungen; Ankunft vor Alarm ist unmöglich |
| Zeitraum 2015-01 – 2025-12 | 132 Monate | Fest verdrahtet, nicht aus den Daten abgeleitet – sonst verschiebt sich die Analyse bei jedem Download (#18) |
| Park-/Institutionsgebiete raus | −3 Stadtteile | Golden Gate Park hat 45 Einwohner; jede Pro-Kopf-Größe wird dort beliebig groß (#19) |
| Stadtteile ohne durchgängige ACS-Abdeckung raus | −3 Stadtteile | Treasure Island, Lakeshore, Mission Bay – sonst unbalanciertes Panel (#15) |

**Ergebnis:** 41 → **35 Stadtteile**, 719.989 Einsätze im Rohbestand, davon
350.481 im Analysezeitraum und in den 35 Stadtteilen.

**Für die Arbeit erwähnenswert:** Der Ausschluss der Parkgebiete ist eine
Entscheidung über die *Analyseeinheit*, keine Ausreißerbereinigung nach der
Zielgröße. Der Unterschied ist methodisch wichtig – Zeilen nach ihrem y-Wert zu
entfernen wäre angreifbar, Gebiete ohne Wohnbevölkerung aus einem
bevölkerungsbezogenen Risikomodell zu nehmen ist es nicht.

---

## Schritt 3 – Joinen

**Datei:** `prep/s02_einsaetze.py` · **Funktionen:** `year_aware_join`,
`berechne_kriminalitaetsindex`, `_join_landuse`

Drei Merkmalsblöcke werden an jeden Einsatz angespielt. Jeder hat eine andere
zeitliche Auflösung, und genau daraus entsteht der Aufwand.

### 3a ACS – sozioökonomisch, Snapshot je Jahrgang

Tract → Stadtteil über den Crosswalk; Mediane bevölkerungsgewichtet, Zähler und
Nenner summiert. Jeder Einsatz erhält den **letzten tatsächlich publizierten**
Jahrgang: `acs_jahr ≤ Einsatzjahr − 1`.

Der Versatz um ein Jahr ist der entscheidende Punkt: Die ACS-5-Jahres-Schätzung
für Jahr *y* erscheint erst rund Dezember *y+1*. Ohne den Versatz hätte ein
Einsatz aus 2023 den Jahrgang 2023 bekommen – zum Prognosezeitpunkt gar nicht
veröffentlicht. Das Modell wäre nicht implementierbar gewesen (#4, #11).

### 3b Kriminalität – relativer Index je Stadtteil × Monat

Der aufwendigste Teil der Aufbereitung, und zwar aus einem inhaltlichen Grund.

Definition (Location Quotient):

```
Index(i,t) = [Delikte(i, 12-Monats-Fenster endend in t−1) / Einwohner(i)]
             ÷ [dasselbe stadtweit]
```

Lesart: 1,0 = Kriminalitätsbelastung wie im Stadtdurchschnitt desselben Monats.

Drei Probleme werden gleichzeitig gelöst (#17):

1. **Zeitvarianz.** Die früheren Merkmale waren über den gesamten Zeitraum
   kumuliert und je Stadtteil konstant – 0 % Zeitvarianz. Jetzt: im Mittel 128
   verschiedene Werte je Stadtteil über 132 Monate.
2. **Leakage.** Die alte Kumulation umfasste auch die Testmonate. Das Fenster
   endet jetzt strikt im Vormonat.
3. **Strukturbruch.** SFPD hat im Mai 2018 von CABLE auf das Crime Data
   Warehouse umgestellt; absolute Fallzahlen sind über den Bruch nicht
   vergleichbar. Ein stadtweiter Niveausprung wirkt auf Zähler und Nenner
   gleichermaßen und **kürzt sich im Quotienten heraus**. Nachgerechnet:
   Rangkorrelation der Stadtteile 2017 vs. 2019 = **0,975**.

Der historische SFPD-Datensatz enthält keine Stadtteilspalte, nur Koordinaten –
daher ein Spatial Join gegen dieselbe Geometrie wie bei Land Use.

*Verbleibende Limitation für Kap. 6.3:* Eine Verschiebung in der
**Zusammensetzung** der erfassten Delikte, die einzelne Stadtteile stärker
trifft als andere, kürzt sich nicht heraus.

### 3c Land Use – baulich, Snapshot 2020

Spatial Join Parzellen-Centroid → Stadtteil-Polygon, Trefferquote **99,5 %**,
danach Aggregation je Stadtteil.

*Limitation:* nur ein Jahrgang verfügbar, also über den gesamten Zeitraum
konstant. Die drei baulichen Merkmale erklären Niveauunterschiede zwischen
Stadtteilen, nicht deren zeitliche Entwicklung. Das gehört in die Interpretation.

---

## Schritt 4 – Raten berechnen

**Datei:** `prep/s02_einsaetze.py` · **Funktionen:** `berechne_quoten`, `safe_ratio`

Aus den gejointen Zählvariablen werden sieben Anteilswerte gebildet. `safe_ratio`
setzt das Ergebnis auf `NaN`, wenn der Nenner ≤ 0 ist – keine stille Division
durch Null.

| Rate | Formel |
|---|---|
| `armutsquote_pct` | Personen unter der Armutsgrenze / Grundgesamtheit |
| `akademikerquote_pct` | Bachelor-Abschlüsse / Grundgesamtheit Bildung |
| `leerstandsquote_pct` | leerstehende / gesamte Wohneinheiten |
| `anteil_altbau_vor_1940_pct` | Parzellen vor 1940 / Parzellen mit Baujahr |
| `anteil_altbau_vor_1960_pct` | dito, Robustheitsvariante |
| `anteil_wohngebaeude_pct` | Wohnparzellen / alle Parzellen |
| `anteil_risikogewerbe_pct` | Fläche RETAIL/ENT + PDR / Gesamtfläche |

Anschließend werden alle Spalten auf deutsche Namen umgestellt (Mapping in
`config.py`, Abschnitt 9).

**Ergebnis: `data/processed/einsaetze.parquet` – 719.989 × 50.**

Die 50 Spalten enthalten bewusst auch die **Zähler und Nenner** hinter den
Quoten. Sie sind keine Modellmerkmale, machen aber jede Rate nachrechenbar –
`armutsquote_pct = armutsbevoelkerung / armuts_grundgesamtheit`.

---

## Schritt 5 – Aggregieren

**Datei:** `prep/s03_datensaetze.py` · **Funktion:** `aggregiere`

Vom Einzeleinsatz auf **Stadtteil × Monat**. Vier Punkte, die begründet gehören:

1. **Vollständiges Raster.** Es wird ein Kreuzprodukt aller Stadtteile mit allen
   Monaten aufgespannt; Kombinationen ohne Einsatz bekommen eine echte **0**.
   Ohne diesen Schritt wären einsatzfreie Monate schlicht nicht vorhanden – das
   Modell würde sie nie sehen. Nullanteil im Ergebnis: 0,02 %.
2. **`ffill` ohne `bfill`.** Fehlende Stadtteilmerkmale werden nur vorwärts
   gefüllt. Rückwärtsfüllen hätte fehlende Werte still mit **Zukunftswerten**
   imputiert – Leakage (#10).
3. **Exposure.** Statt der rohen Einwohnerzahl geht `log(Bevölkerung)` ins
   Modell. Ohne diese Kontrolle sagt das Modell im Kern die Stadtteilgröße
   vorher: `armutsquote_pct` korreliert **+0,20** mit der absoluten Einsatzzahl,
   aber **−0,13** mit Einsätzen je 1.000 Einwohner. Das Vorzeichen des zentralen
   Struktur-Befundes hängt an dieser Entscheidung (#13).
4. **Log-Kriminalitätsindex.** Ein Quotient ist multiplikativ und rechtsschief
   (Schiefe 3,5). Logarithmiert ist er symmetrisch um 0 (Schiefe 0,66).

**Zwischenergebnis:** 38 Stadtteile × 144 Monate (inkl. Vorlauf) = 5.472 Zeilen.

---

## Schritt 6 – Lags berechnen

**Datei:** `prep/s03_datensaetze.py` · **Funktion:** `baue_merkmale`

Zwei Merkmalsarten, die sich nicht aus den Rohdaten ergeben, sondern aus der
Zielgröße selbst.

**Saison.** Der Kalendermonat als Sinus und Kosinus. Als Zahl 1–12 hätten
Dezember und Januar den Abstand 11, obwohl sie benachbart sind – und ein
linearer Koeffizient könnte ein U-förmiges Jahresmuster grundsätzlich nicht
abbilden. Empirisch: 69,8 Einsätze im April gegenüber 83,4 im Dezember (+19,5 %).

**Lags.** `lag_1`, `lag_12`, `rolling_mean_3`, je Stadtteil gebildet. Die
Lag-1-Autokorrelation beträgt 0,96; ohne diese Merkmale schlägt **keines** der
drei Verfahren die naive Vormonats-Baseline (#8).

Leakage-Sicherheit: alle drei strikt rückwärtsgerichtet, `shift(1)` steht **vor**
`rolling(3)`. Der Wert für Monat *t* nutzt *t−1, t−2, t−3*, nie *t* selbst. Das
wird in `tests/test_aufbereitung.py` stichprobenartig gegen die Rohdaten
nachgeschlagen.

**Lag-Vorlauf (#23).** `lag_12` für Januar 2015 braucht Januar 2014. Deshalb
wird ab 2014-01 aggregiert und **erst nach der Lag-Bildung** auf 2015-01
zugeschnitten. Die Vorlaufmonate gehen ausschließlich über `shift()` ein, nie als
eigene Zeile. Das bringt 4.620 statt 4.200 Modellzeilen und sorgt dafür, dass
Regression und Klassifikation denselben Zeitraum abdecken.

### Zwei Merkmalssätze

| Satz | Inhalt | Beantwortet |
|---|---|---|
| **S** | 10 Strukturmerkmale + Saison | Unterfrage 1: Erklärungsbeitrag der Merkmale |
| **S+L** | zusätzlich die drei Lags | die realistische Prognoseaufgabe |

Zwei Sätze, weil der Vormonatswert sonst fast alles erklärt und Armut oder
Altbauanteil in der Feature Importance verschwinden – nicht weil sie irrelevant
wären, sondern weil ihre Wirkung im Vormonatswert steckt.

Bewusst **nicht** enthalten: das rohe `jahr`. Baumverfahren können nicht
extrapolieren und ordnen unbekannte Jahreswerte dem letzten Blatt zu, während
Ridge linear weiterrechnet – das würde genau den Verfahrensvergleich verzerren.

### Was sonst noch in diesem Schritt passiert

- **Zielgrößen der Klassifikation** aus den NFIRS-Serien: vier Gruppen
  (Fehlalarm/Good Intent 48,2 % · Technische Hilfe 24,0 % · Rettung/EMS 14,2 % ·
  Brand 13,6 %) plus die binäre Variante `ist_brand` als Robustheitslauf (#21).
- **Ergebnisvariablen ausgeschlossen** – Sachschaden, Alarmstufe, Antwortzeit
  usw. stehen erst *nach* dem Einsatz fest (#20).
- **CV-Aufteilung als Spalten** `fold` und `ist_holdout`. Damit ist die
  Fairness-Regel in der Datei nachzählbar und hängt nicht davon ab, dass jedes
  Modellskript die richtige Funktion aufruft.
- **Datentypen** auf `float64` vereinheitlicht (#24).

**Ergebnis:**

| | `regression.parquet` | `klassifikation.parquet` |
|---|---|---|
| Zeile | Stadtteil-Monat | Einzeleinsatz |
| Umfang | 4.620 × 24 | 350.481 × 26 |
| Zeitraum | 2015-01 – 2025-12 | identisch |

---

## Schritt 7 – Eignungsprüfung

**Datei:** `prep/s04_eignungspruefung.py` · **Ergebnis:**
`results/eignungspruefung/eignungspruefung_summary.md` + 5 Grafiken

Dieser Schritt gehört ausdrücklich in Kapitel 5. Er ist der **Prüfpunkt von
Prof. Schröter**: „erst plotten, falls keine lineare Baseline vorliegt, kein
lineares Regressionsmodell verwenden." Ohne diesen Nachweis ist die Wahl von
Ridge nicht begründet.

**Methodische Regel:** Alle Diagnosen, die eine Modellentscheidung begründen,
werden **ausschließlich auf dem Trainingsfenster des ersten Folds** gerechnet
(2015-01 – 2021-12, 2.940 Beobachtungen). Diese Monate sind in jedem Fold
Trainingsdaten und in keinem Testdaten. Rein deskriptive Kennzahlen nutzen den
vollen Zeitraum und sind entsprechend gekennzeichnet.

### Die zentralen Befunde

| Prüfung | Ergebnis | Konsequenz |
|---|---|---|
| **Linearität** (Schröter) | OLS R² **0,75** auf Rohskala | Lineare Baseline vorhanden → **Ridge zulässig** |
| Negative Vorhersagen | 7,8 % roh, 0 % nach log | **Ridge auf log(1+y)** (#2) |
| Heteroskedastizität | Breusch-Pagan p ≈ 4·10⁻¹²¹ | Trichter-Residuen → log-Spezifikation bestätigt |
| **Overdispersion** | Dispersionsindex **62,8** | Poisson scheidet aus → **Negative Binomial** als Count-Baseline |
| Zero-Inflation | 0,02 % Nullmonate | kein Zero-Inflated-Modell nötig |
| **VIF Set S** | max. 7,1 (Einkommen) | Erhöht, nicht extrem → klassischer Ridge-Anwendungsfall |
| VIF Set S+L | max. 52,9 (`rolling_mean_3`) | Autoregressive Merkmale; durch L2 abgedeckt, aber **Lags nur blockweise interpretieren** |
| Extrapolationsbedarf | 2,1 % über Trainingsmaximum | unkritisch für Random Forest und XGBoost |
| Nichtlinearität | Spearman ≫ Pearson bei 1 von 10 | begründet die Baumverfahren neben Ridge |
| Klassenbalance | 3,6 : 1 | mit `class_weight="balanced"` beherrschbar |
| Basisratendrift | 5,3 Prozentpunkte | Schwelle des binären Laufs je Fold kalibrieren |
| Designmatrix | float64, keine NaN/inf | direkt an alle drei Verfahren übergebbar |

**Gesamturteil: alle harten Kriterien erfüllt.** Drei Auflagen legen die
Spezifikation fest – Ridge auf log(1+y), Lags log(1+x), Schwellenkalibrierung
binär. `prep/build.py` endet mit Exit-Code 1, wenn ein hartes Kriterium reißt.

---

## Absicherung

`tests/test_aufbereitung.py` prüft **die fertigen Parquet-Dateien**, nicht den
Code, der sie erzeugt – 14 Prüfungen, alle grün. Die wichtigsten:

- Lags werden stichprobenartig gegen die Rohdaten nachgeschlagen (Leakage)
- Panel rechteckig, 35 × 132 = 4.620, keine NaN
- Fold-Spalten konsistent zur zentralen Definition
- keine Ergebnisvariablen im Klassifikationsdatensatz
- Designmatrix modelltauglich

Für Kapitel 5 genügt ein Satz mit Verweis auf die Datei – als Qualitätsnachweis
ist sie mehr wert als zusätzlicher Analysecode.

---

## Umfang der Umsetzung

Zur Einordnung, falls in der Arbeit der Aufwand beschrieben werden soll:

| Baustein | Codezeilen | Anmerkung |
|---|---|---|
| Schritte 5+6 (aggregieren, Lags, Zielgrößen) | 255 | der Kern |
| Schritte 2–4 (auswählen, joinen, Raten) | 382 | davon ~85 der Kriminalitätsindex |
| Schritt 1 (laden) | 242 | überwiegend API-Paginierung, 7 Quellen |
| Schritt 7 (Eignungsprüfung) | 562 | Berichts- und Grafikerzeugung für Kap. 5 |
| Festlegungen (`config.py`) | 178 | reine Konstanten, keine Logik |
| Validierung (`cv.py`) | 173 | Zeitschnitte und Gütemaße |
| Orchestrierung (`build.py`) | 89 | – |

Die eigentliche Aufbereitungslogik – Schritte 2 bis 6 – umfasst **637 Zeilen**.
Der Rest ist Datenbeschaffung, Diagnostik für die Arbeit und Konfiguration.
