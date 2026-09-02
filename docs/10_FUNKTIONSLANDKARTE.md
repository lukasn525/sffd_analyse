# Funktionslandkarte — jede Funktion, ihr Zweck, ihr Aufrufer

> **Lebensdauer:** ändert sich, wenn Funktionen dazukommen, wegfallen oder
> umbenannt werden. Die Tabellen sind aus dem Quelltext erzeugt (AST), die
> Fließtexte von Hand geschrieben. Stand **02.09.2026**.
>
> **Wozu diese Datei.** Sie beantwortet für jede Funktion des Repos drei
> Fragen: Wo steht sie, was tut sie, wer ruft sie. Sie ersetzt nicht
> `docs/08_FUNKTIONSDOKUMENTATION.md` — dort steht ausführlich, *warum* eine
> Funktion so aussieht (Stand 17.08.2026, eingefroren). Hier steht knapp,
> *was* sie ist, und zwar auf dem aktuellen Stand.
>
> **Die Spalte „Gerufen von" ist importbewusst aufgelöst.** Ein Aufruf zählt
> nur, wenn die rufende Datei den Namen tatsächlich importiert hat. Sonst
> stünden gleichnamige Helfer (`_md`, `bericht`, `main`, `ein_lauf`) wechselseitig
> als Aufrufer voneinander — sie sind aber verschiedene Funktionen.

---

## 0. Die Zahl, um die es geht

Der Abgabecode umfasst **8.281 Zeilen** in 16 Dateien. Netto, also ohne
Leerzeilen, Docstrings und Kommentare, sind es **4.089 Zeilen**. Diese Tabelle
wird bei jedem Lauf des Erzeugers neu gemessen, sie kann nicht veralten:

| Ordner | Dateien | LOC brutto | **netto** | Funktionen |
|---|---:|---:|---:|---:|
| `prep/` | 4 | 1.586 | 767 | 28 |
| `vorpruefung/` | 6 | 1.897 | 892 | 42 |
| `modelle/` ohne `m05` | 4 | 2.544 | 1.173 | 49 |
| `modelle/m05_abbildungen.py` | 1 | 1.835 | 1.068 | 32 |
| `tests/` | 1 | 419 | 189 | 23 |
| **Abgabe gesamt** | **16** | **8.281** | **4.089** | **174** |
| `tools/` (nicht Abgabe) | 16 | 7.534 | 5.373 | 105 |

**26 % des Abgabecodes ist Matplotlib in `m05`.** Diese 32 Funktionen
folgen alle demselben Muster (CSV lesen → Achsen → beschriften → speichern);
wer eine verstanden hat, hat alle verstanden.

---

## 1. Der Lernpfad — in dieser Reihenfolge lesen

Die Abhängigkeiten laufen streng in eine Richtung. Wer sie in dieser
Reihenfolge liest, muss nie vorgreifen.

| # | Was | Wo | netto | Danach kannst du erklären |
|---|---|---|---:|---|
| 1 | Die zwei Konfigurationen | `prep/config.py`, `modelle/config_modelle.py` | 153 | welche Merkmale es gibt, welche Suchräume, warum |
| 2 | Wie die Daten entstehen | `prep/s1_daten.py`, `s2_datensaetze.py`, `build.py` | 635 | woher jede Spalte kommt und wie die Folds zustande kommen |
| 3 | Die eine Stelle mit den Folds | `vorpruefung/v0_aufteilung.py` | 82 | warum alle Verfahren dieselben Zeilen sehen |
| 4 | Die Messlatte | `vorpruefung/v1_baselines.py` | 159 | wogegen gemessen wird und warum diese zwei Stufen |
| 5 | Warum diese drei Verfahren | `vorpruefung/v2_eignung.py` | 385 | die sechs Belege der Verfahrenswahl |
| 6 | **Das Muster** | `modelle/m02_menge.py` | 405 | Tuning → Bewertung → Aggregation → Vergleich |
| 7 | Dasselbe Muster nochmal | `modelle/m03_struktur.py` | 377 | *fast nichts Neues* — siehe Abschnitt 2 |
| 8 | Die Gegenproben | `v3_spezifikation`, `v4_decke` | 266 | was die Ergebnisse einschränkt |
| 9 | Die Interpretation | `modelle/m04_shap.py` | 370 | Unterfrage 1: welche Merkmale tragen |
| 10 | Die Bilder | `modelle/m05_abbildungen.py` | 1.068 | ein Muster, 18-mal angewandt |
| 11 | Die Prüfungen | `tests/test_aufbereitung.py` | 189 | was zugesichert ist |

Etappen 1 bis 6 sind **1.819 Nettozeilen** — der Kern. Alles danach ist
Wiederholung des Musters, Gegenprobe oder Darstellung.

---

## 2. Die drei Muster, die sich wiederholen

Wer diese drei erkennt, muss zwei Drittel des Codes nicht einzeln lesen.

### Muster A — `m02` und `m03` sind Zwillinge

Beide Dateien haben **dieselbe Phasenfolge und 16 gleichnamige Funktionen**:

`verfahren` → `suchraum` → `tune` → `ein_lauf` → `extrapolationsanteil` →
`phase_tuning` → `_rein_python` → `_parameter_je_fold` → `phase_bewertung` →
`aggregiere` → `_gepaart` → `vergleiche` → `leakage_diagnose` → `hold_out` →
`uebernehmen` → `main`

Die Unterschiede sind vier, und alle sind fachlich:

| | `m02_menge` (Regression) | `m03_struktur` (Klassifikation) |
|---|---|---|
| Verfahren | Ridge, RF, XGBoost | RF, XGBoost (#31) |
| Maß | RMSE, MAE, R² | Macro-F1, Macro-AUROC |
| Zusätzlich | `_holm` (drei Verfahren ⇒ Testfamilie) | `kodiere`, `_gewichte`, `_macro_auroc` |
| Pipeline | mit Scaler (Ridge braucht ihn) | ohne Scaler, beides Bäume |

**Lies `m02` gründlich, `m03` nur im Diff.**

### Muster B — die 18 Abbildungsfunktionen

Jede `aN_*(plt, FuncFormatter)` in `m05` tut dasselbe: CSV aus `results/`
lesen, Achsen anlegen, mit `_komma`/`_prozent`/`_dez` deutsch beschriften,
per `_text` einen Erläuterungsblock daruntersetzen, speichern. `main` ruft
sie der Reihe nach. Die vorangestellten `_*`-Helfer bereiten je eine
Abbildung datenseitig vor.

### Muster C — Bericht-Skripte

`vorpruefung/v4_decke`, `tools/panelprofil` und
`tools/parametersensitivitaet` haben denselben Bauplan: rechnende Funktionen →
`_md`/`md` (Datenrahmen zu Markdown) → `bericht` (Tabellen zusammensetzen) →
`main` (schreibt CSV plus `.md`). Gleiche Funktionsnamen, drei verschiedene
Dateien — nicht verwechseln.

---

## 3. Der Datenfluss auf einen Blick

```
data/raw/  ──prep/s1_daten──►  Einsatzebene
                                    │
                     prep/s2_datensaetze  (aggregieren, Zielgrößen,
                                    │      fold + ist_holdout als SPALTE)
                                    ▼
        data/processed/regression.parquet · klassifikation.parquet
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
  vorpruefung/v0_aufteilung   vorpruefung/v1_baselines   vorpruefung/v2_eignung
  (die einzige Stelle,        (die Messlatte,            (sechs Belege der
   an der Folds entstehen)     Stufe 1 und 2)             Verfahrenswahl)
        │                           │
        └────────────┬──────────────┘
                     ▼
        modelle/m02_menge · m03_struktur     ──►  results/regression/
                     │                              results/klassifikation/
                     ▼
              modelle/m04_shap                ──►  results/shap/
                     │
                     ▼
          modelle/m05_abbildungen             ──►  results/abbildungen/
                                                   (rechnet nichts, liest nur)
```

Dass `fold` und `ist_holdout` **Spalten der Parquet-Datei** sind und nicht zur
Laufzeit entstehen, ist die konstruktive Absicherung der Fairness-Regel: Alle
Verfahren sehen zwangsläufig dieselben Zeilen.

---

## 4. `prep/` — die Daten


### `prep/config.py`

> Konfiguration der Datenaufbereitung.  
> **0 Funktionen, 0 Zeilen in Funktionsrümpfen.**

Reine Konstanten, keine Funktionen: Pfade, Zeitraum, die Merkmalslisten,
`N_STADTTEILE_ERWARTET = 36`, die `DOWNLOAD_*`-Schalter. **Von 265 Zeilen sind
93 Kommentar** — jede Festlegung ist an Ort und Stelle begründet. Das ist die
Datei, mit der man anfängt.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|


### `prep/s1_daten.py`

> Schritt 1: Rohdaten beschaffen und auf Einsatzebene zusammenfuehren.  
> **15 Funktionen, 538 Zeilen in Funktionsrümpfen.**

Zwei Einstiegspunkte, beide von `build.main` gerufen: `run_download()` holt
die Rohquellen (nur wenn der Schalter in `config` das erlaubt), `run_join()`
führt sie auf **Einsatzebene** zusammen. Dazwischen liegen die Fachfunktionen
in der Reihenfolge, in der sie gebraucht werden — SFFD aufbereiten, Tracts auf
Stadtteile umlegen, den zum Prognosezeitpunkt *publizierten* ACS-Jahrgang
wählen, den Kriminalitätsindex bauen, Parzellen zuordnen.

Die drei Funktionen, an denen die meiste Fachlogik hängt, sind
`acs_snapshot` (Publikationsversatz), `kriminalitaetsindex` (relativer Index
aus zwei SFPD-Quellen) und `tract_zu_stadtteil` (Zuordnung über
Zensusgrenzen hinweg).

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 112–121 | `_get(url, params)` | 10 | Ruft eine URL mit Wiederholversuchen ab | `lade_datasf`, `run_download` |
| 124–172 | `lade_datasf(name, limit)` | 49 | Holt eine DataSF-Quelle vollstaendig und setzt die Spaltentypen | `run_download` |
| 175–198 | `lade_acs(year)` | 24 | Holt die ACS 5-Year Estimates auf Tract-Ebene fuer San Francisco County | `run_download` |
| 201–245 | `run_download()` | 45 | Laedt alle Rohquellen nach data/raw | **extern:** `build:main` |
| 251–276 | `prepare_sffd(df)` | 26 | Bereitet Dedup, Antwortzeit, Zeitmerkmale und Stadtteilnamen auf | `run_join` |
| 282–309 | `tract_zu_stadtteil(geoids, crosswalk)` | 28 | Ordnet Census Tracts einem Stadtteil zu, auch ueber Zensusgrenzen hinweg | `acs_je_neighborhood` |
| 312–346 | `acs_je_neighborhood(acs, crosswalk)` | 35 | Aggregiert Census Tracts auf Stadtteile | `run_join` |
| 349–365 | `acs_snapshot(jahr, acs_years)` | 17 | Waehlt den zum Prognosezeitpunkt tatsaechlich publizierten ACS-Jahrgang | `join_acs`, `kriminalitaetsindex` |
| 368–391 | `join_acs(sffd, nb_per_year)` | 24 | Fuegt jedem Einsatz den passenden ACS-Jahrgang an | `run_join` |
| 397–409 | `neighborhoods_gdf()` | 13 | Laedt die Neighborhood-Polygone | `crime_monatlich`, `land_use_je_neighborhood` |
| 415–470 | `crime_monatlich()` | 56 | Zaehlt Delikte je Stadtteil und Monat aus beiden SFPD-Quellen | `kriminalitaetsindex` |
| 473–538 | `kriminalitaetsindex(nb_per_year)` | 66 | Berechnet den relativen Kriminalitaetsindex je Stadtteil und Monat | `run_join` |
| 544–595 | `land_use_je_neighborhood()` | 52 | Ordnet Parzellen-Centroide Stadtteilen zu und aggregiert je Stadtteil | `run_join` |
| 613–629 | `berechne_quoten(df)` | 17 | Rechnet Anteilswerte in [0,1] | `run_join` |
| 635–710 | `run_join()` | 76 | Fuehrt alle Rohquellen zur Einsatztabelle zusammen | **extern:** `build:main` |


### `prep/s2_datensaetze.py`

> Schritt 2: die beiden finalen Datensaetze samt Validierungsrahmen.  
> **10 Funktionen, 393 Zeilen in Funktionsrümpfen.**

Hier entsteht die Analyseeinheit **Stadtteil × Monat** und damit beides, was
später gelesen wird. `aggregiere` baut das Panel, `baue_regression` und
`baue_klassifikation` legen die Zielgrößen an, `ergaenze_aufteilung` schreibt
`fold` und `ist_holdout` **als Spalten** in die Datei — das ist die wichtigste
Stelle des ganzen Repos für die Fairness-Regel. `pruefe_zuschnitt` bricht ab,
wenn der Zuschnitt nicht stimmt; `run` fährt alles und gibt beide Datenrahmen
zurück. `fold_masken` und `beschreibe_splits` gehören zum Validierungsrahmen
und werden von außen mitbenutzt.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 63–95 | `ergaenze_aufteilung(daten, versatz, selten)` | 33 | Schreibt `fold` (0..N_FOLDS) und `ist_holdout` in den Datensatz | `run` **extern:** `test_aufbereitung:test_aufteilungsspalten_konsistent` |
| 98–112 | `fold_masken(daten, k)` | 15 | Liefert Trainings- und Testmaske des Folds k aus den Spalten der Datei | **extern:** `m02_menge:phase_bewertung`, `m02_menge:phase_tuning` … |
| 115–136 | `beschreibe_splits(daten)` | 22 | Fasst die Aufteilung lesbar zusammen, fuer Kapitel 5.2 und 5.4 | *Einstiegspunkt* |
| 142–150 | `_monat_minus(jahr_monat, monate)` | 9 | Verschiebt einen jahr_monat-Schluessel um n Monate zurueck | `baue_regression` **extern:** `test_aufbereitung:test_lags_gegen_rohdaten` |
| 153–178 | `_setze_datentypen(d, merkmale)` | 26 | Vereinheitlicht die Datentypen auf modelltaugliche NumPy-Typen | `baue_klassifikation`, `baue_regression` |
| 184–253 | `aggregiere(von, bis, mit_parkgebieten, verbose)` | 70 | Verdichtet die Einsatz-Ebene zu Stadtteil x Monat, vollstaendiges Raster | `baue_regression` **extern:** `test_aufbereitung:test_lags_gegen_rohdaten` |
| 256–330 | `baue_regression(vorlauf, verbose)` | 75 | Baut den vollstaendigen Regressionsdatensatz | `run` |
| 336–411 | `baue_klassifikation(regression, verbose)` | 76 | Baut die Anteile der vier NFIRS-Gruppen je Stadtteil und Monat | `run` |
| 417–443 | `pruefe_zuschnitt(r)` | 27 | Prueft, ob der Analysezuschnitt der Festlegung entspricht | `run` |
| 446–485 | `run(verbose)` | 40 | Baut beide finalen Datensaetze, traegt die Folds ein und schreibt sie | **extern:** `build:main` |


### `prep/build.py`

> DER EINE BEFEHL. Erzeugt aus den Rohdaten die beiden finalen Datensaetze.  
> **3 Funktionen, 62 Zeilen in Funktionsrümpfen.**

Die Fassade. `main()` fährt `s1_daten.run_join()` und `s2_datensaetze.run()`
nacheinander und druckt am Ende den Steckbrief. 106 Zeilen, davon 30 die
eigentliche Steuerung.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 37–44 | `schritt(nummer, titel)` | 8 | Gibt die Ueberschrift eines Arbeitsschrittes aus und startet die Uhr | `main` |
| 47–70 | `uebersicht()` | 24 | Steckbrief der erzeugten Dateien: Zeilen, Spalten, Groesse, Zeitraum | `main` |
| 73–102 | `main()` | 30 | Faehrt beide Aufbereitungsschritte, dann die Uebersicht | *Einstiegspunkt* |


---

## 5. `vorpruefung/` — die Messlatte und die Eignung


### `vorpruefung/v0_aufteilung.py`

> Wiederholte Splits - die eine Stelle, an der die Fold-Zuteilung entsteht.  
> **4 Funktionen, 140 Zeilen in Funktionsrümpfen.**

**Die wichtigste kleine Datei des Repos.** Drei öffentliche Funktionen, und
alle drei werden von außen gerufen:

- `selten_je_stadtteil` liefert die Sortiergröße der Zuteilung (brand-dominierte Monate je Stadtteil),
- `wiederholte_aufteilung` belegt die `fold`-Spalte für Wiederholung *r* neu,
- `entwicklung_und_holdout` ist **die einzige Stelle im Repo, die `ist_holdout == 1` auswertet**.

`wiederholte_aufteilung` enthält eine Zusicherung, die trägt: Für
Wiederholung 0 muss die neu gerechnete Zuteilung Zeile für Zeile der
`fold`-Spalte aus der Parquet-Datei entsprechen, sonst bricht sie ab. Damit
kann die Zuteilung nicht stillschweigend auseinanderlaufen.

Der `_selbsttest` prüft über alle zehn Wiederholungen: Foldgrößen 6/6/6/6/6,
mindestens ein Brand-Testfall je Fold, unverändertes Hold-out, zehn
verschiedene Partitionen, kein Stadtteil gleichzeitig Trainings- und Testfall.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 49–62 | `selten_je_stadtteil(klassifikation)` | 14 | Zahl der brand-dominierten Monate je Stadtteil | `_selbsttest` **extern:** `m02_menge:main`, `m03_struktur:main` … |
| 65–115 | `wiederholte_aufteilung(daten, wiederholung, selten)` | 51 | Belegt die fold-Spalte fuer eine Wiederholung neu | `_selbsttest` **extern:** `m02_menge:phase_bewertung`, `m02_menge:phase_tuning` … |
| 118–128 | `entwicklung_und_holdout(daten)` | 11 | Masken der Schlussbewertung: 30 Entwicklungs- gegen 6 Hold-out-Stadtteile | **extern:** `m02_menge:hold_out`, `m03_struktur:hold_out` |
| 134–197 | `_selbsttest()` | 64 | Selbsttest ueber alle 10 Wiederholungen | *Einstiegspunkt* |


### `vorpruefung/v1_baselines.py`

> Stufe 1 und 2: die Messlatte.  
> **8 Funktionen, 260 Zeilen in Funktionsrümpfen.**

Die zwei Stufen der Messlatte. `poisson_glm` ist Stufe 2 der Regression (mit
Offset, #45), `logit_glm` Stufe 2 der Klassifikation — beide unpenalisiert per
Maximum-Likelihood, **ohne freien Hyperparameter**. `regression` und
`klassifikation` fahren sie über zehn Wiederholungen × fünf Folds, `_zweistufig`
mittelt erst je Wiederholung, dann darüber.

`bewerte_regression` und `_macro_auroc` werden auch von `modelle/` benutzt —
damit rechnen Baseline und Vergleichsverfahren die Gütemaße **mit demselben
Code**, nicht nur nach derselben Formel.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 59–70 | `bewerte_regression(y_true, y_pred)` | 12 | RMSE, MAE und R2 auf der Originalskala der Zielgroesse | `regression` **extern:** `m02_menge:hold_out`, `m04_shap:ablation_faktorgruppen` … |
| 74–108 | `poisson_glm(train, test, merkmale)` | 35 | Stufe 2 der Regression: Poisson-GLM mit Offset | `regression` **extern:** `m02_menge:hold_out`, `m04_shap:ablation_faktorgruppen` |
| 111–138 | `logit_glm(train, merkmale)` | 28 | Stufe 2 der Klassifikation: multinomiales Logit | `klassifikation` **extern:** `m03_struktur:hold_out`, `m04_shap:ablation_faktorgruppen` |
| 141–178 | `regression(panel, selten)` | 38 | Beide Mengen-Zielgroessen, Stufe 1 und 2, je Wiederholung und Fold | `run` |
| 181–206 | `_zweistufig(df, schluessel, masse)` | 26 | Zweistufige Aggregation ueber die Laeufe eines Durchgangs | `klassifikation`, `regression` |
| 210–265 | `klassifikation(kl, selten)` | 56 | Beide Stufen der Klassifikation, je Wiederholung und Fold | `run` |
| 268–287 | `_macro_auroc(y_true, proba, klassen_modell, klassen_alle)` | 20 | Macro-AUROC (One-vs-Rest), NaN wenn im Testfold eine Klasse fehlt | `klassifikation` |
| 291–335 | `run()` | 45 | Fuehrt beide Straenge aus und schreibt die drei Ergebnisdateien | **extern:** `run:main` |


### `vorpruefung/v2_eignung.py`

> Eignungspruefung: Passen die gewaehlten Verfahren zu den Zielgroessen?  
> **12 Funktionen, 595 Zeilen in Funktionsrümpfen.**

Sechs Belege, warum genau diese Verfahren. Jeder ist eine eigene Funktion und
schreibt in denselben Berichtstext (`log`, `speichere`):

1. `dispersion` — Überdispersion der Zähl-Zielgrößen (spricht gegen reines Poisson),
2. `linearitaet` — Korrelationen und Residuenbild (Auflage R7),
3. `spezifikation` — RESET-Test und Interaktionsterme,
4. `extrapolation` — Anteil der Teststadtteile außerhalb des Gelernten,
5. `klassifikation` — trennen dieselben Merkmale auch die Einsatzart,
6. `annahmen` — die Anforderungstabelle je Verfahren mit drei formalen Tests
   (Cameron & Trivedi, Breusch-Pagan, Jarque-Bera; Auflage aus der
   Sprechstunde vom 10.08.2026).

`annahmen` ist mit 186 Zeilen die längste Funktion der Vorprüfung; sie ist
lang, weil sie eine Tabelle zusammensetzt, nicht weil sie kompliziert rechnet.
Die verschachtelte `Z()` baut je eine Zeile davon.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 58–65 | `log(txt)` | 8 | Gibt eine Zeile aus und haengt sie an den Berichtstext an | `annahmen`, `dispersion`, `extrapolation` … |
| 68–77 | `speichere(fig, name)` | 10 | Legt eine Abbildung im Ergebnisordner ab und vermerkt sie im Bericht | `linearitaet` |
| 81–129 | `dispersion(train)` | 49 | Beleg 1: Dispersionsindex der beiden Zaehl-Zielgroessen | `main` |
| 133–225 | `linearitaet(train)` | 93 | Beleg 2: Korrelationen und Residuenbild (Auflage R7) | `main` |
| 229–278 | `spezifikation(train)` | 50 | Beleg 3: RESET-Test und Interaktionsterme | `main` |
| 282–314 | `extrapolation(panel)` | 33 | Beleg 4: Anteil der Teststadtteile ausserhalb des Gelernten | `main` |
| 318–391 | `klassifikation(kl)` | 74 | Beleg 5: Trennen dieselben Merkmale auch die Einsatzart? | `main` |
| 395–401 | `_z(wert, stellen)` | 7 | Teststatistik mit deutschem Dezimalkomma | `annahmen` |
| 404–418 | `_p(wert)` | 15 | p-Wert deutsch; unter 0,001 wird begrenzt statt beziffert | `annahmen` |
| 421–606 | `annahmen(train, befunde)` | 186 | Beleg 6: Anforderungen je Verfahren mit formalen Tests | `main` |
| 484–499 | `Z(verfahren, anforderung, pruefung, statistik, p, status, konsequenz, wert)` | 16 | Baut eine Zeile der Anforderungstabelle | `annahmen` |
| 610–663 | `main()` | 54 | Rechnet die sechs Belege und schreibt Bericht, Tabellen und Abbildungen | **extern:** `run:main` |


### `vorpruefung/v3_spezifikation.py`

> Haelt die diagnostizierte Nichtlinearitaet out-of-sample nach?  
> **6 Funktionen, 167 Zeilen in Funktionsrümpfen.**

Die Gegenprobe zu Beleg 3 (B-41): Die Eignungsprüfung hat Nichtlinearität
*in-sample* diagnostiziert — hält sie auch out-of-sample? `alle_laeufe` fährt
10 × 5 × 4 = 200 Poisson-Anpassungen über vier Spezifikationen, `zweistufig`
mittelt wie überall zweistufig.

Der `_selbsttest` ist die Kontrolle, die das Ergebnis erst belastbar macht:
Die Spezifikation `linear` muss die Stufe-2-Baseline reproduzieren. Tut sie es
nicht, rechnet die Datei etwas anderes als sie behauptet.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 62–101 | `entwerfe(train, test, spezifikation)` | 40 | Merkmalsmatrizen fuer Training und Test, auf Trainingsdaten zentriert | `ein_lauf` |
| 104–132 | `ein_lauf(train, test, spezifikation)` | 29 | Eine Poisson-Anpassung, eine Bewertung auf der Originalskala | `alle_laeufe` |
| 135–151 | `alle_laeufe(panel, selten)` | 17 | 10 Wiederholungen x 5 Folds x 4 Spezifikationen = 200 Anpassungen | `run` |
| 154–171 | `zweistufig(df)` | 18 | Erst je Wiederholung ueber die Folds, dann ueber die Wiederholungen | `run` |
| 174–198 | `_selbsttest(mittel)` | 25 | Prueft, ob die Spezifikation `linear` die Stufe-2-Baseline reproduziert | `run` |
| 201–238 | `run()` | 38 | Rechnet alle 200 Anpassungen und schreibt die beiden Ergebnisdateien | *Einstiegspunkt* |


### `vorpruefung/v4_decke.py`

> Wie gut KANN die Einsatzart mit diesen Merkmalen ueberhaupt vorhergesagt werden?  
> **10 Funktionen, 282 Zeilen in Funktionsrümpfen.**

Wie gut *kann* die Einsatzart mit diesen Merkmalen überhaupt vorhergesagt
werden? Zwei Obergrenzen: `decke_a` beziffert das Label-Rauschen der
argmax-Bildung über einen parametrischen Bootstrap, `decke_b` das reine
Stadtteilwissen (Modalklasse je Stadtteil). `marge` misst, wie knapp die
Klassenentscheidung ausfällt, `ausschoepfung` setzt die gemessenen Macro-F1
baselinekorrigiert dagegen.

Mit dem Argument `holdout` rechnet dieselbe Datei die Decken inklusive der
sechs zurückgehaltenen Stadtteile.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 84–90 | `_macro_f1(a, b)` | 7 | Macro-F1 zweier Klassenreihen; fehlende Klassen zaehlen als 0 | `decke_a`, `decke_b`, `main` |
| 93–121 | `decke_a(panel)` | 29 | Decke A: Label-Rauschen des argmax, parametrischer Bootstrap | `main` |
| 124–138 | `decke_b(panel)` | 15 | Decke B: Modalklasse je Stadtteil, Obergrenze des Stadtteilwissens | `main` |
| 141–160 | `marge(panel)` | 20 | Abstand zwischen groesstem und zweitgroesstem Klassenanteil | `main` |
| 163–191 | `_modellwerte(mit_holdout)` | 29 | Macro-F1 je Verfahren aus derselben Bewertung, aus der die Decken stammen | `main` |
| 194–217 | `ausschoepfung(modelle, basis, a, b)` | 24 | Baselinekorrigierte Quote je Verfahren gegen beide Decken | `main` |
| 220–240 | `_md(df)` | 21 | Markdown-Tabelle von Hand | `bericht` |
| 233–234 | `zelle(x)` | 2 | **— kein Docstring —** | `_md` |
| 243–296 | `bericht(tab, aus, mrg, kipp, treffer, modal, n_stadtteile, quelle)` | 54 | Setzt die Ergebnistabellen zu decke.md zusammen | `main` |
| 299–379 | `main(argv)` | 81 | Rechnet beide Decken, Marge und Ausschoepfung; schreibt vier Dateien | *Einstiegspunkt* |


### `vorpruefung/run.py`

> Der eine Befehl der Vorpruefung.  
> **2 Funktionen, 31 Zeilen in Funktionsrümpfen.**

Fassade über `v1_baselines.run()` und `v2_eignung.main()`. Die Reihenfolge ist
zwingend — `v2` liest die Baseline-Werte, die `v1` schreibt. 65 Zeilen, keine
Rechnung.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 29–35 | `schritt(nummer, titel)` | 7 | Gibt die Ueberschrift eines Arbeitsschrittes aus | `main` |
| 38–61 | `main()` | 24 | Faehrt beide Schritte der Vorpruefung nacheinander | *Einstiegspunkt* |


---

## 6. `modelle/` — der Vergleich


### `modelle/config_modelle.py`

> Konfiguration der Modellierung. Gegenstueck zu prep/config.py.  
> **0 Funktionen, 0 Zeilen in Funktionsrümpfen.**

Gegenstück zu `prep/config.py`, ohne Funktionen: `RANDOM_STATE = 42`,
Suchräume, Budget, Fold- und Wiederholungszahlen. **65 von 110 Zeilen sind
Kommentar** — jeder Suchraum trägt seine Begründung (#49, #50) an Ort und Stelle.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|


### `modelle/m02_menge.py`

> Verfahrensvergleich fuer die MENGE der Einsatzlast.  
> **18 Funktionen, 764 Zeilen in Funktionsrümpfen.**

**Die Datei, die man wirklich lesen muss.** Vier Phasen, jede eine Funktion,
alle von `main` in dieser Reihenfolge gefahren:

| Phase | Funktion | Was passiert |
|---|---|---|
| 1 | `phase_tuning` | je Zielgröße × Verfahren × Fold **einmal** `tune()`, auf Wiederholung 0 |
| 2 | `phase_bewertung` | 10 Wiederholungen × 5 Folds × 3 Verfahren × 2 Zielgrößen = 300 Läufe |
| 3 | `aggregiere` | zweistufig mitteln: erst je Wiederholung über die Folds, dann darüber |
| 4 | `vergleiche` | gepaarter Wilcoxon auf RMSE, zwei Rollen, zwei Teststufen |

Darunter liegen die Bausteine: `verfahren` baut die ungetunte Pipeline,
`suchraum` übersetzt die Config in scipy-Verteilungen, `ein_lauf` ist **ein**
Fit plus Bewertung plus Zeitmessung und liefert eine CSV-Zeile.

Drei Stellen verdienen besondere Aufmerksamkeit:

- **`ein_lauf` (119 Z.)** ist der Kern. Hier steht die Spezifikation aus #43:
  Geschätzt wird immer die *Rate*, für die absolute Zahl wird mit der
  Einwohnerzahl zurückmultipliziert. Dieselbe Konstruktion wie beim Poisson-GLM.
- **`hold_out`** ist die einzige Funktion, die die sechs zurückgehaltenen
  Stadtteile anfasst — und sie läuft nur, wenn `main` das Argument `holdout`
  bekommen hat.
- **`uebernehmen`** liest Tuning und Bewertung aus `results/`, statt sie neu zu
  rechnen. Das ist der Grund, warum ein Wiederholungslauf nicht immer 55 Minuten braucht.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 130–171 | `verfahren(name, n_jobs)` | 42 | Baut die ungetunte Pipeline fuer ein Verfahren | `ein_lauf`, `tune` |
| 174–204 | `suchraum(name)` | 31 | Uebersetzt SUCHRAEUME aus der Config in scipy-Verteilungen | `tune` |
| 210–239 | `tune(name, train, ziel)` | 30 | Sucht die Hyperparameter auf den Trainingsstadtteilen eines Folds | `phase_tuning` |
| 245–363 | `ein_lauf(name, parameter, train, test, ziel, auch_parallel, mit_vorhersagen)` | 119 | Ein Fit, eine Vorhersage, mit Zeitmessung - eine Zeile fuer die CSV | `hold_out`, `phase_bewertung` |
| 366–378 | `extrapolationsanteil(train, test)` | 13 | Anteil der Testzeilen ausserhalb des Trainings-Wertebereichs | `ein_lauf` |
| 384–437 | `phase_tuning(panel, selten)` | 54 | Phase 1: je Zielgroesse, Verfahren und Fold einmal tune() | `main` |
| 440–454 | `_rein_python(p)` | 15 | Wandelt NumPy-Skalare in native Typen, bevor sie nach JSON gehen | `phase_tuning` |
| 457–465 | `_parameter_je_fold(parameter)` | 9 | Liest tuning.csv als Nachschlagetabelle | `hold_out`, `phase_bewertung` |
| 468–509 | `phase_bewertung(panel, parameter, selten)` | 42 | Phase 2: 10 Wiederholungen x 5 Folds x 3 Verfahren x 2 Zielgroessen | `main` |
| 517–566 | `aggregiere(folds)` | 50 | Phase 3: zweistufig mitteln, erst je Wiederholung, dann darueber | `main` |
| 572–589 | `_holm(p)` | 18 | Holm-Bonferroni ueber eine Testfamilie | `vergleiche` |
| 592–615 | `_gepaart(a, b)` | 24 | Gepaarter Wilcoxon samt der Kennzahlen, die ohne p-Wert tragen | `paar`, `vergleiche` |
| 618–702 | `vergleiche(folds, baselines)` | 85 | Phase 4: gepaarter Wilcoxon auf RMSE, zwei Rollen und zwei Teststufen | `main` |
| 658–671 | `paar(links, rechts)` | 14 | Legt eine Vergleichszeile fuer vergleich.csv an | `vergleiche` |
| 705–739 | `leakage_diagnose(folds, baselines)` | 35 | Beziffert, was das Tuning auf Wiederholung 0 kostet (B-21) | `main` |
| 745–805 | `hold_out(panel, parameter, folds, selten)` | 61 | Einmalige Schlussbewertung: 30 Stadtteile trainieren, 6 bewerten | `main` |
| 811–866 | `uebernehmen(panel)` | 56 | Liest Tuning und Bewertung aus results/, statt sie neu zu rechnen | `main` |
| 869–934 | `main(argv)` | 66 | Faehrt die vier Phasen und schreibt alle Ergebnisdateien | *Einstiegspunkt* |


### `modelle/m03_struktur.py`

> Verfahrensvergleich fuer die STRUKTUR der Einsatzlast.  
> **21 Funktionen, 683 Zeilen in Funktionsrümpfen.**

**Derselbe Bauplan wie `m02`** — siehe Muster A oben. Neu sind nur vier
Funktionen, und alle vier sind Klassifikationsmechanik: `kodiere` (Klassennamen
zu 0–3 nach der globalen Reihenfolge `KLASSEN`), `_gewichte`
(`class_weight="balanced"` von Hand, weil XGBoost es nicht kennt), `fitte`
(Schätzer mit diesen Gewichten anpassen) und `_macro_auroc`.

Es fehlt `_holm`: Bei zwei Verfahren statt drei gibt es keine Testfamilie zu
korrigieren.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 99–123 | `verfahren(name, n_jobs)` | 25 | Baut die ungetunte Pipeline. Kein Scaler, beide Verfahren sind Baeume | `ein_lauf`, `fitte`, `tune` |
| 126–159 | `suchraum(name)` | 34 | Uebersetzt SUCHRAEUME in scipy-Verteilungen, ohne Praefix | `tune` |
| 162–176 | `kodiere(y)` | 15 | Klassennamen -> Integer 0..3 nach der globalen Reihenfolge KLASSEN | `ein_lauf`, `hold_out`, `tune` |
| 179–186 | `_gewichte(y_int)` | 8 | class_weight="balanced" von Hand, fuer XGBoost | `ein_lauf`, `fitte`, `tune` |
| 192–222 | `tune(name, train)` | 31 | Wie m02.tune, aber mit f1_macro als Scoring | `phase_tuning` |
| 228–317 | `ein_lauf(name, parameter, train, test, auch_parallel, mit_vorhersagen)` | 90 | Ein Fit, eine Vorhersage, mit Zeitmessung - eine Zeile fuer die CSV | `hold_out`, `phase_bewertung` |
| 249–268 | `fitte(kerne)` | 20 | Fittet einen Schaetzer mit den Klassengewichten des Verfahrens | `ein_lauf` |
| 320–332 | `extrapolationsanteil(train, test)` | 13 | Anteil der Testzeilen ausserhalb des Trainings-Wertebereichs | `ein_lauf` |
| 335–359 | `_gepaart(a, b)` | 25 | Gepaarter Wilcoxon samt der Kennzahlen, die ohne p-Wert tragen | `paar`, `vergleiche` |
| 362–382 | `_macro_auroc(y_true, proba, klassen_modell)` | 21 | Macro-AUROC (One-vs-Rest), NaN wenn eine Klasse im Test fehlt | `ein_lauf`, `hold_out` |
| 388–412 | `phase_tuning(panel, selten)` | 25 | Phase 1: je Verfahren und Fold einmal tune() auf Wiederholung 0 | `main` |
| 415–425 | `_rein_python(p)` | 11 | Wandelt NumPy-Skalare in native Typen, wortgleich zu m02_menge | `phase_tuning` |
| 428–435 | `_parameter_je_fold(parameter)` | 8 | Liest tuning.csv als Nachschlagetabelle | `hold_out`, `phase_bewertung` |
| 438–475 | `phase_bewertung(panel, parameter, selten)` | 38 | Phase 2: 10 Wiederholungen x 5 Folds x 2 Verfahren = 100 Zeilen | `main` |
| 483–519 | `aggregiere(folds)` | 37 | Phase 3: zweistufig mitteln, wie in m02 | `main` |
| 522–579 | `vergleiche(folds, baselines)` | 58 | Phase 4: gepaarter Wilcoxon auf Macro-F1 | `main` |
| 550–562 | `paar(links, rechts)` | 13 | Legt eine Vergleichszeile fuer vergleich.csv an | `vergleiche` |
| 582–608 | `leakage_diagnose(folds, baselines)` | 27 | Beziffert, was das Tuning auf Wiederholung 0 kostet | `main` |
| 611–685 | `hold_out(panel, parameter, folds)` | 75 | Einmalige Schlussbewertung, mit Macro-F1 als Auswahlkriterium | `main` |
| 691–734 | `uebernehmen(panel)` | 44 | Liest Tuning und Bewertung aus results/, statt sie neu zu rechnen | `main` |
| 737–801 | `main(argv)` | 65 | Faehrt die vier Phasen und schreibt alle Ergebnisdateien | *Einstiegspunkt* |


### `modelle/m04_shap.py`

> Interpretation: Welche Merkmale tragen die Vorhersage?  
> **10 Funktionen, 579 Zeilen in Funktionsrümpfen.**

Unterfrage 1 — welche Merkmale tragen die Vorhersage. Sechs Auswertungen, die
`main` (203 Z., reine Ablaufsteuerung) nacheinander fährt:

- `schlagen_die_latte` wählt aus, **für wen** SHAP überhaupt sinnvoll ist:
  nur (Zielgröße, Verfahren), die ihre Stufe-2-Baseline schlagen,
- `ruhigster_fold` wählt den Fold mit dem geringsten Extrapolationsanteil,
- `_beitraege` liefert den mittleren absoluten Beitrag je Merkmal — SHAP bei
  Bäumen, Koeffizient bei Ridge,
- `ablation_faktorgruppen` (94 Z.) misst, was eine Faktorgruppe wert ist, indem
  sie weggelassen wird — die **Gegenprobe zur Attribution**,
- `ablation_exposition` prüft die Spezifikation aus #43,
- `_vif` beziffert Multikollinearität auf zwei Bezugsmengen.

Attribution (was ein Modell benutzt) und Ablation (was fehlt, wenn man es
wegnimmt) sind zwei verschiedene Fragen. Dass beide hier stehen, ist Absicht;
ihr Auseinanderfallen ist B-47.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 91–114 | `schlagen_die_latte(vergleich)` | 24 | Welche (Zielgroesse, Verfahren) schlagen ihre Stufe-2-Baseline? | `main` |
| 117–125 | `ruhigster_fold(folds)` | 9 | Fold mit dem geringsten Extrapolationsanteil in Wiederholung 0 | `main` |
| 128–162 | `_beitraege(modell, X, name)` | 35 | Mittlerer absoluter Beitrag je Merkmal: SHAP oder Koeffizient | `main` |
| 165–214 | `extrapolation_aufschluesseln(panel, selten, folds)` | 50 | Schluesselt den Extrapolationsanteil nach Merkmal und Stadtteil auf | `main` |
| 217–267 | `ablation_exposition(panel, selten, parameter)` | 51 | Ablation: Was leistet die Expositionsbehandlung? | `main` |
| 270–363 | `ablation_faktorgruppen(reg, kl, selten, tuning_kl, mit_baeumen)` | 94 | Ablation: Was ist eine Faktorgruppe wert? (Unterfrage 1) | `main` |
| 366–400 | `_ablation_auswerten(roh)` | 35 | Verschlechterung je Gruppe gegenueber dem vollen Merkmalssatz | `main` |
| 403–446 | `faktorgruppen_baseline(panel, selten, fold)` | 44 | Beitrag der drei Faktorgruppen im Mengenstrang, aus der Baseline | `main` |
| 449–482 | `_vif(panel)` | 34 | VIF auf zwei Bezugsmengen | `main` |
| 485–687 | `main()` | 203 | Rechnet die sechs Auswertungen zu Unterfrage 1 | *Einstiegspunkt* |


### `modelle/m05_abbildungen.py`

> Alle Abbildungen der Kapitel 4 und 7 - aus den CSV-Dateien, nicht von Hand.  
> **32 Funktionen, 1569 Zeilen in Funktionsrümpfen.**

**Rechnet nichts.** Liest ausschließlich die CSV-Dateien der vorherigen
Schritte und zeichnet daraus 21 PDF. Deshalb steht sie am Ende der Laufordnung
und deshalb dauert sie unter einer Minute.

Der Aufbau ist streng nach Muster B (oben): fünf Formatierhelfer
(`_matplotlib`, `_komma`, `_prozent`, `_dez`, `_sekunden`, `_text`), dann
paarweise ein `_*`-Datenaufbereiter und die zugehörige `aN_*`-Zeichenfunktion.
`main` ruft die 18 Abbildungen der Reihe nach.

Zum Lesen genügen zwei: `a1_gegen_baseline` (die Primäraussage) und
`a15_attribution_ablation` (die komplexeste). Der Rest ist dasselbe Muster mit
anderen Spalten.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 164–174 | `_sekunden(wert)` | 11 | Beschriftet Sekundenwerte lesbar | `a9_parallelisierung` |
| 184–202 | `_matplotlib()` | 19 | Setzt Schriftgroessen, Schrift und Rahmen fuer alle Abbildungen | `main` |
| 205–218 | `_komma(stellen, vorzeichen)` | 14 | Deutsches Dezimalkomma auf den Achsen | `a10_qq_residuen`, `a11_differenzen`, `a12_decken` … |
| 221–228 | `_prozent(stellen)` | 8 | Prozentwert mit deutschem Dezimalkomma | `a15_attribution_ablation`, `a17_panelstruktur`, `a6_faktorgruppen` … |
| 231–237 | `_text(pfad)` | 7 | Setzt einen Textblock unter die Abbildung | `_faktorgruppen_balken`, `_gepaarte_differenz`, `_hyperparameter_lagen` … |
| 241–254 | `_speichere(fig, datei)` | 14 | Legt eine Abbildung in results/abbildungen ab und schliesst sie | `a10_qq_residuen`, `a11_differenzen`, `a12_decken` … |
| 257–290 | `_gepaarte_differenz()` | 34 | Je Verfahren die 10 Wiederholungsmittel der Differenz zur Baseline | `a1_gegen_baseline` |
| 293–346 | `a1_gegen_baseline()` | 54 | A1: jedes Verfahren gegen seine Stufe-2-Baseline (Primaeraussage) | *Einstiegspunkt* |
| 350–397 | `a2_foldstruktur()` | 48 | A2: Rohwerte je Fold - Begruendung fuer die Paarung in A1 | *Einstiegspunkt* |
| 401–437 | `_spezifikationszeilen()` | 37 | Sammelt die Balkenwerte fuer A3 aus drei Ergebnisdateien | `a3_spezifikation` |
| 440–492 | `a3_spezifikation()` | 53 | A3: Verfahren gegen Spezifikation (Unterfrage 4) | *Einstiegspunkt* |
| 496–577 | `a4_laufzeit_guete()` | 82 | A4: Aufwand gegen Guete, ein Punkt je Verfahren (Unterfrage 3) | *Einstiegspunkt* |
| 581–640 | `a5_holdout()` | 60 | A5: die einmalige Auswertung auf den sechs zurueckgehaltenen Stadtteilen | *Einstiegspunkt* |
| 644–671 | `_faktorgruppen_balken()` | 28 | Anteile je Faktorgruppe fuer einen Strang | `a6_faktorgruppen` |
| 674–734 | `a6_faktorgruppen()` | 61 | A6: Welche Faktorgruppe traegt wie viel? (Unterfrage 1) | *Einstiegspunkt* |
| 738–800 | `a7_extrapolation()` | 63 | A7: Extrapolationsanteil gegen Fehler, 50 Punkte je Verfahren | *Einstiegspunkt* |
| 804–835 | `_lage_im_suchraum(name, parameter, wert)` | 32 | Relative Lage eines gefundenen Wertes in seinem Suchraum, 0 bis 1 | `_hyperparameter_lagen` |
| 838–871 | `_hyperparameter_lagen()` | 34 | Bereitet die Fold-Parametersaetze fuer A8 auf | `a8_hyperparameter` |
| 874–952 | `a8_hyperparameter()` | 79 | A8: Stabilitaet der Modellwahl bei 30 Entwicklungsstadtteilen | *Einstiegspunkt* |
| 956–1024 | `a9_parallelisierung()` | 69 | A9: Parallelisierungsgewinn je Verfahren (Unterfrage 3, zweite Haelfte) | *Einstiegspunkt* |
| 1028–1076 | `a10_qq_residuen()` | 49 | A10: QQ-Diagramm der Residuen der linearen Spezifikation | *Einstiegspunkt* |
| 1091–1100 | `_dez(wert, stellen)` | 10 | Deutsches Dezimalkomma fuer Beschriftungen im Bild | `a11_differenzen`, `a12_decken`, `a13_umschlag` … |
| 1103–1199 | `a11_differenzen()` | 97 | Gepaarte Differenzen mit Konfidenzintervall, je Strang eine Abbildung | *Einstiegspunkt* |
| 1132–1145 | `_zeilen(df, ersatz)` | 14 | **— kein Docstring —** | `a11_differenzen` |
| 1202–1284 | `a12_decken()` | 83 | Die beiden Obergrenzen des Strukturstrangs mit den erreichten Werten | *Einstiegspunkt* |
| 1287–1353 | `a13_umschlag()` | 67 | Kreuzvalidierung gegen Hold-out im Strukturstrang | *Einstiegspunkt* |
| 1356–1433 | `a14_ueberanpassung()` | 78 | Trainingsguete gegen Kreuzvalidierungsguete je Verfahren | *Einstiegspunkt* |
| 1436–1545 | `a15_attribution_ablation()` | 110 | Attribution und Ablation je Faktorgruppe, nebeneinander | *Einstiegspunkt* |
| 1569–1619 | `a16_einsatzlast()` | 51 | A16: Einsatzlast je Stadtteil - Lage und Streuung ueber 132 Monate | *Einstiegspunkt* |
| 1621–1684 | `a18_foldstruktur()` | 64 | A18: Struktur der Aufteilung - Beleg fuer Kapitel 5.4 | *Einstiegspunkt* |
| 1690–1783 | `a17_panelstruktur()` | 94 | A17: Varianzanteile und zeitliche Aufloesung der zwoelf Modellmerkmale | *Einstiegspunkt* |
| 1787–1831 | `main()` | 45 | Erzeugt alle siebzehn Abbildungen nacheinander | *Einstiegspunkt* |


---

## 7. `tests/`


### `tests/test_aufbereitung.py`

> Prüfungen der Datenaufbereitung – gesammelt an einer Stelle.  
> **23 Funktionen, 305 Zeilen in Funktionsrümpfen.**

**20** Prüfungen an den erzeugten Parquet-Dateien, gesammelt an einer Stelle:
23 Funktionen, davon 20 `test_*`. (`CLAUDE.md` und README sprachen bis zum
02.09.2026 von 19 — der Lauf meldet 20/20.) Sie prüfen Zeilenzahl, Stadtteilzahl, Datentypen,
fehlende Werte, Konsistenz von `fold` und `ist_holdout`, Plausibilität der
Exposition und der Anteile. `main` sammelt die Ergebnisse und gibt Exitcode 1
bei Fehlern.

Es sind **Zusicherungen über die Daten**, keine Unit-Tests der Funktionen.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 54–59 | `regression()` | 6 | **— kein Docstring —** | `test_aufteilungsspalten_konsistent`, `test_datentypen_modelltauglich`, `test_exposition_plausibel` … |
| 62–67 | `klassifikation()` | 6 | **— kein Docstring —** | `test_anteile_konsistent`, `test_aufteilungsspalten_konsistent`, `test_datentypen_modelltauglich` … |
| 73–84 | `test_panel_rechteckig_und_vollstaendig()` | 12 | Vollständiges Kreuzprodukt Stadtteil x Monat, keine fehlenden Werte | *Einstiegspunkt* |
| 87–97 | `test_zeitraum_festgesetzt()` | 11 | Der Zeitraum kommt aus Konstanten, nicht aus den Daten | *Einstiegspunkt* |
| 100–121 | `test_datentypen_modelltauglich()` | 22 | Kein Merkmal darf einen pandas-eigenen (nullable) Typ haben | *Einstiegspunkt* |
| 124–130 | `test_exposure_und_kriminalitaetsindex_vorhanden()` | 7 | Die log-Transformationen sind gebildet und vollständig | *Einstiegspunkt* |
| 136–158 | `test_folds_ordnung_und_holdout()` | 23 | Kein Stadtteil ist zugleich Trainings- und Testfall | *Einstiegspunkt* |
| 161–171 | `test_jeder_fold_deckt_den_vollen_zeitraum()` | 11 | Ein Teststadtteil wird mit allen seinen Monaten getestet | *Einstiegspunkt* |
| 174–191 | `test_aufteilungsspalten_konsistent()` | 18 | `fold` und `ist_holdout` in der Datei müssen zu prep/s2_datensaetze.py passen | *Einstiegspunkt* |
| 194–207 | `test_folds_decken_die_groessenspanne_ab()` | 14 | Stratifizierung nach Bevölkerung: Kein Fold besteht nur aus Großstadtteilen | *Einstiegspunkt* |
| 213–223 | `test_merkmale_vollstaendig()` | 11 | Der Merkmalssatz ist vollständig, die Rate ist gebildet | *Einstiegspunkt* |
| 226–234 | `test_saison_zyklisch()` | 9 | sin/cos liegen auf dem Einheitskreis, Dezember grenzt an Januar | *Einstiegspunkt* |
| 237–254 | `test_lags_gegen_rohdaten()` | 18 | Der zentrale Leakage-Test: Lags gegen die Rohdaten nachschlagen | *Einstiegspunkt* |
| 257–262 | `test_lags_nicht_gegenwartsbezogen()` | 6 | Gegenprobe gegen ein vergessenes shift(): lag_1 darf nicht der Istwert sein | *Einstiegspunkt* |
| 265–273 | `test_vorlauf_ohne_eigene_zeilen()` | 9 | Die Vorlaufmonate liefern Lag-Werte, aber keine eigenen Beobachtungen | *Einstiegspunkt* |
| 279–287 | `test_keine_ergebnisvariablen()` | 9 | Wichtigster Test des Strukturteils | *Einstiegspunkt* |
| 290–305 | `test_struktur_gleiche_abgrenzung_wie_regression()` | 16 | Beide Teile der Arbeit beruhen auf demselben Datenbestand | *Einstiegspunkt* |
| 308–316 | `test_anteile_konsistent()` | 9 | Die vier Anteile summieren sich je Zeile auf 1 und passen zu den Zählungen | *Einstiegspunkt* |
| 319–329 | `test_zielklasse_konsistent()` | 11 | Die dominante Einsatzart ist argmax ueber die vier Anteile | *Einstiegspunkt* |
| 332–344 | `test_seltene_klasse_in_jedem_fold()` | 13 | Brand muss in jedem Test-Fold vorkommen | *Einstiegspunkt* |
| 347–357 | `test_struktur_hat_signal()` | 11 | Die Anteile variieren zwischen Stadtteilen – sonst gäbe es nichts zu erklären | *Einstiegspunkt* |
| 360–394 | `test_exposition_plausibel()` | 35 | Die Exposition muss der Groessenordnung der Stadt entsprechen | *Einstiegspunkt* |
| 398–415 | `main()` | 18 | **— kein Docstring —** | *Einstiegspunkt* |


---

## 8. `tools/` — nicht Abgabe, aber Teil des ZIP

Seit dem 31.08.2026 geht `tools/` mit ins Abgabe-ZIP: `fairness.py` erzeugt die
Zahlen von R-24 und Kapitel 8.3, `suchdiagnose.py` die Budgetdiagnose in 6.4,
`deskriptiv.py` und `codebook.py` die Tabellen aus Kapitel 4,
`pruefe_zahlen.py` die Zusicherung, dass Text und `results/` zusammenpassen.

Für das Verständnis der Arbeit sind sie zweitrangig — **mit einer Ausnahme:**

### `tools/pruefe_zahlen.py` — der Zahlenwächter

Die einzige Datei in `tools/`, die man kennen sollte. Sie prüft **148 Werte**
aus `results/` gegen die Stellen in `docs/`, an denen sie stehen, plus fünf
Strukturprüfungen. `baue_pruefungen` (227 Z.) ist die Registrierungsliste —
lang, aber trivial: eine Zeile je Wert. `abschnitte` zerlegt ein
Markdown-Dokument an den nummerierten Überschriften, damit ein Wert nicht in
einem beliebigen Kapitel „gefunden" wird. Exit-Code 1 meldet, welche Stelle
nicht mehr passt.



### `tools/panelprofil.py`

> Wer sind die 30 und wer sind die 6? - Profil beider Panelhaelften.  
> **7 Funktionen, 200 Zeilen in Funktionsrümpfen.**

Rein deskriptiv, kein Modell: Wer sind die 30 und wer sind die 6?
`stadtteile` liefert eine Zeile je Stadtteil mit Zuteilungsrang, Größe,
Einsatzlast und Modalklasse; `klassenverteilung` und `zielgroessen` vergleichen
beide Panelhälften.

**Das trägt die Erklärung des Hold-out-Bruchs im Strukturstrang** (R-2): 37 von
70 brand-dominierten Monaten liegen im Hold-out, der Brand-Anteil ist dort
4,67 % gegen 0,83 % in der Entwicklung. Rang 1 der Zuteilungsordnung fällt
nach der Regel aus #30 zwangsläufig in Gruppe 0.

**Geht nicht in die Arbeit** (Entscheidung 02.09.2026, nach Schröters Regel
vom 24.08.: Analysen ohne Bezug zu einer Forschungsfrage gehören raus oder in
den Anhang). Deshalb liegt die Datei in `tools/` und ihre Zahlen stehen in
`06_RISIKEN.md` unter R-2, nicht in `03_STAND.md`. Der Zahlenwächter prüft sie
trotzdem — sie sind einmal unbemerkt über eine Korrektur hinweg veraltet.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 74–92 | `lade()` | 19 | Liest beide Datensaetze und prueft die gemeinsame Aufteilung | `main` |
| 95–126 | `stadtteile(reg, kl)` | 32 | Eine Zeile je Stadtteil: Zuteilung, Groesse, Einsatzlast, Klassenlage | `main` |
| 129–146 | `klassenverteilung(kl)` | 18 | Klassenanteile je Panelhaelfte | `main` |
| 149–172 | `zielgroessen(reg)` | 24 | Verteilung beider Zielgroessen je Panelhaelfte | `main` |
| 175–181 | `_md(df)` | 7 | Datenrahmen als Markdown-Tabelle. Reine Formatierung | `bericht` |
| 184–232 | `bericht(st, kv, zg)` | 49 | Setzt die Tabellen zu panelprofil.md zusammen | `main` |
| 235–285 | `main(argv)` | 51 | Schreibt drei CSV-Dateien und die Lesefassung | *Einstiegspunkt* |


### `tools/parametersensitivitaet.py`

> Wie viel haengt an der Wahl des Hyperparametersatzes? - Kreuzprobe ueber die Folds.  
> **7 Funktionen, 181 Zeilen in Funktionsrümpfen.**

Die Kreuzprobe: `kreuzprobe` bewertet **jeden Testfold mit jedem der fünf
getunten Parametersätze**. Die Diagonale ist die berichtete Konfiguration, die
20 übrigen Zellen sind Sätze, die auf anderen Stadtteilen gefunden wurden.
`zusammenfassung` stellt eigenen gegen fremden Satz, `_kontrolle` prüft, ob die
Diagonale den Hauptlauf reproduziert.

Ergebnis: Der eigene Satz ist nicht systematisch besser — bei den Baumverfahren
sogar schlechter. Dritte unabhängige Bestätigung derselben These neben `v3` und
der Budgetdiagnose aus #49.

**Geht nicht in die Arbeit** (Entscheidung 02.09.2026). Die Messung schließt
aber die in B-42 benannte offene Frage; ihr Ergebnis steht deshalb in
`06_RISIKEN.md` unter R-4. **Wird die Parameter-Asymmetrie in Kapitel 8
erwähnt, ist „nicht gemessen" falsch** — dann gehört eine Zeile aus R-4 hinein.

| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |
|---|---|---:|---|---|
| 82–100 | `parametersaetze(pfad, zielgroesse)` | 19 | Liest die getunten Saetze je Verfahren und Fold aus tuning.csv | `kreuzprobe` |
| 103–153 | `kreuzprobe(strang)` | 51 | Bewertet jeden Testfold mit jedem Parametersatz | `main` |
| 156–186 | `zusammenfassung(matrix, strang)` | 31 | Eigener gegen fremde Parametersaetze, je Verfahren | `main` |
| 189–214 | `_kontrolle(matrix, strang)` | 26 | PRUEFAUFTRAG 3: Diagonale gegen den Hauptlauf | `main` |
| 217–231 | `bericht(teile)` | 15 | Setzt die Zusammenfassungen zu bericht.md zusammen. Reine Formatierung | `main` |
| 219–223 | `md(df)` | 5 | **— kein Docstring —** | `bericht` |
| 234–267 | `main(argv)` | 34 | Rechnet die Kreuzprobe und schreibt drei Dateien | *Einstiegspunkt* |


Die übrigen elf Dateien im Überblick:

| Datei | Zweck | Fkt. |
|---|---|---:|
| `codebook.py` | Merkmalstabelle mit Skalenniveau für Kapitel 4 | 8 |
| `deskriptiv.py` | deskriptiver Befundteil von Kapitel 4 | 10 |
| `rohbefunde.py` | Qualitätsteil von Kapitel 4, ACS-Rohbefunde | 2 |
| `fairness.py` | hängt die Prognosegüte am Sozialprofil? (R-24) | 4 |
| `suchdiagnose.py` | war die Hyperparametersuche am Limit? | 8 |
| `sichere_ergebnisse.py` | `results/` nach `archiv/` kopieren, mit Manifest | 5 |
| `aufraeumen.py` | verwaiste Artefakte finden (Vorschau, löscht nichts) | 7 |
| `funktionsdoku.py` | erzeugt das Docstring-Archiv | 5 |
| `folien/renderer.py` | rendert Foliensätze nach HTML und PDF | 18 |
| `folien/deck_prep.py` | Foliensatz 01 — Datenaufbereitung | 0 |
| `folien/deck_vorpruefung.py` | Foliensatz 02 — Messlatte und Eignung | 0 |
| `folien/deck_modelle.py` | Foliensatz 03 — Verfahrensvergleich | 0 |
| `landkarte.py` | erzeugt diese Datei | 4 |

Die drei `deck_*.py` haben **null Funktionen** — sie sind reine Inhaltsdaten
(2.513 Zeilen verschachtelte Listen), die `renderer.py` in Folien verwandelt.
Für das Verständnis des Codes tragen sie nichts bei.

---

## 9. Was du für das Kolloquium können musst

Nach Erfahrung mit dieser Art Prüfung reichen fünf Antworten:

1. **„Wo entstehen die Folds?"** → `vorpruefung/v0_aufteilung.py`, und sie
   stehen als Spalte in der Parquet-Datei. Deshalb sehen alle Verfahren
   dieselben Zeilen.
2. **„Was passiert in einem Lauf?"** → `m02_menge.ein_lauf`: ein Fit auf der
   Rate, eine Vorhersage, Rückmultiplikation mit der Einwohnerzahl,
   Zeitmessung, eine CSV-Zeile.
3. **„Warum zweimal mitteln?"** → `aggregiere`: erst je Wiederholung über die
   fünf Folds, dann über die zehn Wiederholungen. Die 50 Läufe sind nicht
   unabhängig (#37).
4. **„Warum wird das Hold-out nur einmal gelesen?"** → Nur `hold_out` wertet
   `ist_holdout == 1` aus, und nur mit dem Argument `holdout`. Ohne das Argument
   filtert `main` die Zeilen heraus, bevor irgendetwas rechnet.
5. **„Woher wissen Sie, dass die Zahlen im Text stimmen?"** →
   `tools/pruefe_zahlen.py`, 148 Wertprüfungen gegen `results/`, Exit-Code 1
   bei Abweichung.
