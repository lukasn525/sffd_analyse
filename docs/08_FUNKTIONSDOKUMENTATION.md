# Funktionsdokumentation — vollstaendiges Archiv

> **Lebensdauer:** Momentaufnahme des Quelltextes vom 17.08.2026. Erzeugt aus den Docstrings und Kommentarbloecken aller Python-Dateien.
>
> **Wozu diese Datei.** Der Quelltext traegt die Begruendungen verdichtet; hier stehen sie vollstaendig. Wer wissen will, warum eine Funktion so aussieht, wie sie aussieht, findet es hier — im Code steht die Kurzfassung.
>
> **EINGEFRORENE MOMENTAUFNAHME.** Erzeugt am 17.08.2026 von
> `tools/funktionsdoku.py`, BEVOR die Docstrings im Quelltext auf das
> Zielformat verdichtet wurden. Ab diesem Zeitpunkt ist sie die
> ausfuehrliche Fassung: Im Code steht die Kurzform, hier die vollstaendige
> Begruendung. Ein erneuter Lauf des Erzeugers wuerde die Kurzform liefern
> und dieses Archiv entwerten — deshalb nicht ueberschreiben.
>
> **Nachtrag 31.08.2026.** Die in den Docstrings zitierten Ergebniswerte sind
> Stand 17.08.2026 und damit VOR der Crosswalk-Korrektur vom 29.08.2026
> (35 statt 36 Stadtteile, vollständiger Neulauf). Für berichtete Zahlen gilt
> ausschließlich `results/`; die Begründungen der Funktionen bleiben gültig.

---

## Inhalt

- **Aufbereitung** (`prep/`)
  - `config.py`
  - `s1_daten.py`
  - `s2_datensaetze.py`
  - `build.py`
- **Vorpruefung** (`vorpruefung/`)
  - `run.py`
  - `v0_aufteilung.py`
  - `v1_baselines.py`
  - `v2_eignung.py`
  - `v3_spezifikation.py`
  - `v4_decke.py`
- **Modellierung** (`modelle/`)
  - `config_modelle.py`
  - `m02_menge.py`
  - `m03_struktur.py`
  - `m04_shap.py`
  - `m05_abbildungen.py`
- **Absicherung** (`tests/`)
  - `test_aufbereitung.py`
- **Werkzeuge (nicht Abgabe)** (`tools/`)
  - `codebook.py`
  - `suchdiagnose.py`
  - `pruefe_zahlen.py`
  - `sichere_ergebnisse.py`
  - `aufraeumen.py`

---


# Aufbereitung — `prep/`


## `prep/config.py`

*357 Zeilen · 0 Funktionen*

### Modulkopf

```text
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
```

### Kommentarbloecke (19)

**Zeile 22**

```text
==========================================================================
 1  PFADE
 ==========================================================================
```

**Zeile 37**

```text
==========================================================================
 2  DOWNLOADS  (Schritt: prep/s1_daten.py)
 ==========================================================================
 Alle Schalter stehen per Default auf False. `python prep/build.py` laeuft
 dann allein aus data/raw und braucht weder Internet noch API-Key.
 Zum Neuladen einer Quelle den jeweiligen Schalter auf True setzen.
```

**Zeile 69**

```text
Historische SFPD-Daten: Startdatum. Der Kriminalitaetsindex nutzt ein
 rollierendes 12-Monats-Fenster, das im VORMONAT endet; fuer den ersten
 Analysemonat 2015-01 werden daher die Delikte aus 2014 benoetigt.
 Die Lag-Vorlaufmonate (2014) brauchen KEINEN Kriminalitaetsindex - sie liefern
 ausschliesslich Einsatzzaehlungen fuer die Lags und werden nach der
 Lag-Bildung wieder entfernt.
```

**Zeile 78**

```text
==========================================================================
 3  JOINS  (Schritt: prep/s1_daten.py)
 ==========================================================================
 Publikationsverzoegerung der ACS-5-Jahres-Schaetzungen: Jahrgang y erscheint
 erst ca. Dezember y+1. Ein Modell, das im Jahr y schon den Jahrgang y nutzt,
 waere zum Prognosezeitpunkt nicht implementierbar (Decision Log #11).
```

**Zeile 97**

```text
==========================================================================
 4  ANALYSEZEITRAUM UND ANALYSEEINHEITEN
 ==========================================================================
 Hart fixiert, NICHT aus den Daten abgeleitet: Jeder Lauf liefert denselben
 Zeitraum, egal wie weit der letzte DataSF-Download reicht (Decision Log #18).

 START 2015-01: frueheste Periode mit vollstaendigen ACS-Merkmalen unter
   Beruecksichtigung des Publikationsversatzes (Decision Log #5, #11).
 ENDE  2025-12: letztes vollstaendiges Kalenderjahr (Decision Log #12).
```

**Zeile 109**

```text
Lag-Vorlauf (Decision Log #23, 2026-07-27)
 --------------------------------------------------------------------------
 lag_12 fuer Januar 2015 braucht Januar 2014. Frueher fehlte dieser Monat im
 Panel, weshalb das erste Jahr je Stadtteil per dropna verlorenging und die
 Regression erst 2016-01 begann - waehrend die Klassifikation ab 2015-01 lief.

 Die Lags brauchen ausschliesslich `anzahl_einsaetze` aus der Vergangenheit,
 KEINE ACS-Merkmale. Der Grund fuer START = 2015 (Akademikerquote) betrifft nur
 die Praediktoren der Zielzeile, nicht deren Vergangenheitswerte. Einsatzzahlen
 liegen bis 2003 vor.

 Ablauf: ab START-VORLAUF aggregieren -> Lags bilden -> auf START zuschneiden.
 Die Vorlaufmonate gehen ausschliesslich ueber shift() ein, nie als eigene
 Zeile. Ergebnis: 4.620 statt 4.200 Modellzeilen, beide Datensaetze decken
 denselben Zeitraum ab.
```

**Zeile 126**

```text
Stadtteile ohne nennenswerte Wohnbevoelkerung (Decision Log #19). Fuer ein
 bevoelkerungsbezogenes Risikomodell keine sinnvolle Analyseeinheit: jede
 Pro-Kopf-Groesse wird dort beliebig gross, weil der Nenner gegen null geht.
   Golden Gate Park  45 Einwohner, Kriminalitaetsindex im Median 186
   Lincoln Park     299 Einwohner
   McLaren Park     507 Einwohner, zusaetzlich Census-Artefakt (Armutsquote 0,90)
 Median aller uebrigen Stadtteile: 14.435 Einwohner.
```

**Zeile 135**

```text
Ein Monat gilt als verdaechtig unvollstaendig, wenn seine stadtweite
 Einsatzzahl unter diesem Anteil des Median-Monats liegt. Nur Warnung, kein
 automatischer Filter - massgeblich bleibt ENDE.
```

**Zeile 140**

```text
==========================================================================
 5  MERKMALE DER REGRESSION
 ==========================================================================
 Praediktoren gemaess Expose: soziooekonomisch, kriminalitaetsbezogen, baulich.

 log_bevoelkerung statt roher Einwohnerzahl (Exposure, Decision Log #13): ohne
 diese Kontrolle sagt das Modell im Kern die Stadtteilgroesse vorher. Die
 Bevoelkerung ist die Groesse mit dem Vorzeichenwechsel - sie korreliert +0,20
 mit der absoluten Einsatzzahl, aber -0,42 mit Einsaetzen je 1.000 Einwohner.
 (Die frueher hier genannte Armutsquote wechselt das Vorzeichen NICHT: +0,49
 absolut, +0,46 pro Kopf. Korrektur vom 28.07.2026, docs/02_ENTSCHEIDUNGEN.md)

 log_kriminalitaetsindex (Decision Log #17/#19): der Index ist ein Quotient,
 also multiplikativ und rechtsschief. Logarithmiert ist er symmetrisch um 0
 (0 = Stadtdurchschnitt, +0,69 = doppelt so hoch). Fuer Baumverfahren ist die
 monotone Transformation wirkungsneutral, deshalb einheitlich fuer ALLE
 Modelle (Fairness-Regel).
```

**Zeile 165**

```text
Rohwerte: keine Modellmerkmale, aber im Datensatz mitgefuehrt fuer den
 Offset des Poisson-GLM, die Raten-Sensitivitaet und die Deskription in
 Kap. 5.1. Bis Decision Log #45 (06.08.2026) hiess das hier "NegBin-Offset";
 die Stufe-2-Baseline ist seither ein Poisson-GLM. Der Offset selbst ist
 unveraendert log(gesamtbevoelkerung) - nur die Verteilungsannahme des
 Modells hat gewechselt, nicht die Rolle dieser Spalte.
```

**Zeile 174**

```text
Saison: Kalendermonat als Sinus/Kosinus.
 Der Monat als ZAHL 1-12 waere eine schlechte Kodierung - Dezember und Januar
 haetten den Abstand 11, obwohl sie benachbart sind, und ein linearer
 Koeffizient koennte ein U-foermiges Jahresmuster grundsaetzlich nicht
 abbilden. sin/cos legen die Monate auf ein Zifferblatt.
```

**Zeile 181**

```text
Lags: Vergangenheitswerte der Zielgroesse, je Stadtteil. Sie bleiben im
 Datensatz, sind aber KEIN Modellmerkmal mehr (Decision Log #29).

 Grund: Der Verfahrensvergleich laeuft seit dem 28.07.2026 ueber einen
 STADTTEIL-Split - trainiert wird auf 23 Stadtteilen je Fold, getestet auf
 unbekannten. Der Vormonatswert eines Teststadtteils waere dabei technisch
 verfuegbar, denn es ist seine eigene Vergangenheit. Genau dann erklaert aber
 wieder seine Historie das Ergebnis statt seiner Struktur - und die
 Forschungsfrage bliebe unbeantwortet.

 Wozu sie dann noch da sind: zur DESKRIPTION der zeitlichen Struktur in
 Kapitel 4 (Autokorrelation Lag 1 innerhalb eines Stadtteils). Es ist KEIN
 zweiter Analysestrang mit Zeitschnitt geplant - der waere ein zweiter
 Validierungsrahmen und verstiesse gegen R1 und R8 (praezisiert 04.08.2026,
 Decision Log #29). Sie formen den Datensatz auch nicht: Das dropna auf die
 Lags entfernt dank Vorlauf null Zeilen.
 Leakage-sicher gebildet: strikt rueckwaertsgerichtet, shift() VOR rolling().
```

**Zeile 200**

```text
Ein Merkmalssatz - identisch fuer Ridge, Random Forest und XGBoost.

 Bewusst NICHT enthalten: das rohe `jahr` und die Stadtteil-ID. Baumverfahren
 koennen nicht extrapolieren und ordnen unbekannte Werte dem letzten Blatt zu,
 waehrend Ridge linear weiterrechnet - das wuerde den Verfahrensvergleich
 verzerren. Eine Stadtteil-ID waere unter einem Stadtteil-Split ohnehin
 sinnlos: Der Teststadtteil ist im Training nie vorgekommen.
```

**Zeile 211**

```text
==========================================================================
 6  MERKMALE UND ZIELGROESSEN DER KLASSIFIKATION
 ==========================================================================
 NFIRS-Codes sind hierarchisch; die fuehrende Ziffer bezeichnet die Serie.
 Zusammengefasst wird entlang der fachlichen Bedeutung, nicht nach Haeufigkeit
 (Decision Log #21).
   100 Brand · 200 Ueberdruck/Explosion ohne Feuer · 300 Rettungsdienst
   400 Gefahrenlage · 500 Serviceeinsatz · 600 Good Intent · 700 Fehlalarm
   800 Naturereignis · 900 Sonstige
```

**Zeile 234**

```text
Zielgroessen der Klassifikation: die ZUSAMMENSETZUNG der Einsatzlast je
 Stadtteil und Monat, nicht die Art des einzelnen Einsatzes (Decision Log #29).

 Warum der Wechsel: Innerhalb eines Stadtteil-Monats tragen ALLE Einsaetze
 identische Strukturmerkmale. 350.481 Einzeleinsaetze enthielten nur 4.619
 verschiedene Profile; ein perfektes Modell auf den Strukturmerkmalen haette
 49,9 % Treffer erreicht gegenueber 48,2 % fuer blosses Raten. Auf Stadtteil-
 ebene ist dieselbe Frage dagegen beantwortbar: Der Fehlalarm-Anteil laesst
 sich fuer einen unbekannten Stadtteil mit R2 0,66 vorhersagen.
```

**Zeile 255**

```text
Ergebnisvariablen - duerfen NIEMALS Merkmal sein. Sie stehen erst nach dem
 Einsatz fest oder sind eine Folge der Einsatzart; ihre Verwendung waere
 Leakage im engeren Sinn (Decision Log #20).
```

**Zeile 266**

```text
==========================================================================
 7  VALIDIERUNG  (Schritt: prep/s2_datensaetze.py, Teil A)
 ==========================================================================
 STADTTEIL-SPLIT (Decision Log #29). Die Forschungsfrage lautet: Laesst sich
 aus Strukturmerkmalen vorhersagen, wie viele und welche Einsaetze ein
 Stadtteil hat? Diese Frage prueft man, indem man einen Stadtteil komplett
 zurueckhaelt - nicht, indem man die Zeitachse schneidet. Bei einem Zeitschnitt
 steht jeder Stadtteil in Training UND Test; das Modell kennt sein Niveau
 bereits und die Strukturmerkmale muessen nichts erklaeren.

     29 Stadtteile -> 5 Folds (6/6/6/6/5)   6 Stadtteile -> Hold-out
     jeder Stadtteil ist genau einmal Testfall, nie zugleich Trainingsfall

 Zuteilung stratifiziert nach Bevoelkerung: Die Stadtteile werden nach
 Einwohnerzahl sortiert und reihum auf die Gruppen verteilt. Damit deckt jede
 Gruppe die gesamte Groessenspanne ab - sonst laege im Test zufaellig nur
 Downtown oder nur Seacliff. Stratifiziert wird nach einem PRAEDIKTOR, nicht
 nach der Zielgroesse; sonst floesse Testinformation in die Gruppenbildung ein.
 Die Stadtteile werden reihum auf N_FOLDS + 1 Gruppen verteilt. Gruppe 0 ist
 das Hold-out, die Gruppen 1..N_FOLDS sind die Folds. Bei 35 Stadtteilen
 ergibt das 6 Hold-out-Stadtteile und Folds der Groesse 6, 6, 6, 6, 5.
```

**Zeile 289**

```text
==========================================================================
 8  SPALTENNAMEN  englisch -> deutsch
 ==========================================================================
 Die Rohquellen (DataSF, Census) liefern englische Namen; ab dem Ende von
 prep/s1_daten.py heisst im Projekt alles deutsch. Dieses Mapping ist die
 einzige Stelle, an der der Wechsel passiert. Steht hier unten, weil man es
 beim Arbeiten praktisch nie anfasst - im Gegensatz zu allem darueber.
```

**Zeile 333**

```text
── SFPD Kriminalitaet ────────────────────────────────────────────────────
 Relativer Index je Stadtteil x Monat (Location Quotient gegen den
 Stadtdurchschnitt desselben Monats, rollierendes 12-Monats-Fenster endend
 im Vormonat). Ersetzt die frueheren statischen Anteile.
```


## `prep/s1_daten.py`

*595 Zeilen · 14 Funktionen*

### Modulkopf

```text
Schritt 1: Rohdaten beschaffen und auf Einsatzebene zusammenfuehren.

Eingang:  DataSF- und Census-APIs (nur wenn DOWNLOAD_* in config.py True ist)
          data/raw/*
Ausgang:  data/processed/einsaetze.parquet   (ein Einsatz je Zeile, ~720.000)

Vier Quellen: ACS (soziooekonomisch, je Stadtteil x Jahrgang, mit
Publikationsversatz), Crime (relativer Index je Stadtteil x Monat), Land Use
(baulich, Snapshot 2020) und die Neighborhood-Geometrie fuer beide Spatial
Joins. Die Ebene bleibt der EINZELEINSATZ - aggregiert wird erst in
s2_datensaetze.py.

Ausfuehren:
  python prep/s1_daten.py          # Download (soweit aktiviert) + Join
  python prep/s1_daten.py join     # nur Join
```

### Kommentarbloecke (9)

**Zeile 65**

```text
==========================================================================
 TEIL A  DOWNLOAD
 ==========================================================================
 Gesteuert allein ueber die DOWNLOAD_*-Schalter in config.py. Stehen alle auf
 False (Default), arbeitet die Aufbereitung aus data/raw und braucht weder
 Internet noch API-Key.
 ==========================================================================
 Deklarative Quellenliste. Jede DataSF-Quelle unterscheidet sich nur in
 Ressourcen-ID, Feldauswahl, Filter und Nachbearbeitung der Spaltentypen -
 deshalb eine Tabelle und eine Schleife statt sechs fast gleicher Funktionen.
```

**Zeile 223**

```text
==========================================================================
 TEIL B  SFFD - Einsatzdaten
 ==========================================================================
```

**Zeile 247**

```text
==========================================================================
 TEIL C  ACS - soziooekonomische Merkmale
 ==========================================================================
```

**Zeile 301**

```text
==========================================================================
 TEIL D  GEOMETRIE
 ==========================================================================
```

**Zeile 313**

```text
==========================================================================
 TEIL E  CRIME - relativer Kriminalitaetsindex je Stadtteil x Monat
 ==========================================================================
```

**Zeile 436**

```text
==========================================================================
 TEIL F  LAND USE - bauliche Merkmale
 ==========================================================================
```

**Zeile 490**

```text
==========================================================================
 TEIL G  QUOTEN
 ==========================================================================
```

**Zeile 521**

```text
==========================================================================
 Ablauf
 ==========================================================================
```

**Zeile 553**

```text
Kein statischer Fallback: Ein statischer Crime-Join wuerde Delikte aus dem
 Testzeitraum in die Trainingsmerkmale tragen (Leakage) und jede
 Zeitvarianz beseitigen.
```

### Funktionen

#### `_get(url, params)`

*Zeile 112 · 5 Zeilen*

_kein Docstring_

#### `lade_datasf(name, limit)`

*Zeile 119 · 41 Zeilen*

```text
Eine DataSF-Quelle vollstaendig holen und die Spaltentypen setzen.
```

#### `lade_acs(year)`

*Zeile 162 · 21 Zeilen*

```text
ACS 5-Year Estimates auf Tract-Ebene fuer San Francisco County.

    Eigene Funktion, weil die Census-API ein anderes Format liefert als DataSF:
    Kopfzeile plus Datenzeilen als verschachtelte Liste.
```

#### `run_download()`

*Zeile 185 · 36 Zeilen*

_kein Docstring_

#### `prepare_sffd(df)`

*Zeile 226 · 19 Zeilen*

```text
Dedup, Antwortzeit, Zeit-Features, Stadtteilnamen normalisieren.
```

#### `acs_je_neighborhood(acs, crosswalk)`

*Zeile 250 · 15 Zeilen*

```text
Tract -> Neighborhood. Mediane bevoelkerungsgewichtet, Zaehler summiert.
```

#### `acs_snapshot(jahr, acs_years)`

*Zeile 267 · 14 Zeilen*

```text
Letzter zum Prognosezeitpunkt TATSAECHLICH PUBLIZIERTER ACS-Jahrgang.

    Bedingung: acs_jahr <= Einsatzjahr - ACS_PUBLIKATIONS_LAG. Zwei Stufen der
    Absicherung: "letzter verfuegbarer" statt "zeitlich naechster" Snapshot
    (Decision Log #4) und zusaetzlich die reale Publikationsverzoegerung von
    ~1 Jahr (#11) - ohne sie haette ein Einsatz aus 2023 den Jahrgang 2023
    bekommen, der erst Ende 2024 erschienen ist.

    Vor dem ersten Snapshot gibt es keinen vergangenen Jahrgang; Rueckgriff auf
    den aeltesten als dokumentierte Limitation - die Hauptanalyse beginnt 2015.
```

#### `join_acs(sffd, nb_per_year)`

*Zeile 283 · 16 Zeilen*

```text
Jeder Einsatz bekommt den zu seinem Jahr passenden ACS-Snapshot.
```

#### `neighborhoods_gdf()`

*Zeile 304 · 7 Zeilen*

```text
Neighborhood-Polygone. Beide Spatial Joins nutzen dieselbe Geometrie,
    damit sich Kriminalitaets- und Baumerkmale auf identische Flaechen beziehen.
```

#### `crime_monatlich()`

*Zeile 316 · 51 Zeilen*

```text
Deliktzahlen je Neighborhood und Monat aus beiden SFPD-Quellen.

    Modern (e3si-785i, ab 2018-01) ist bereits voraggregiert; da der Index ALLE
    Straftaten zaehlt, werden die Kategorien summiert - eine Harmonisierung der
    Kategorienschemata eruebrigt sich. Historisch (tmnf-yvry, bis 2017) hat
    keine Stadtteilspalte, daher Spatial Join der Koordinaten.
```

#### `kriminalitaetsindex(nb_per_year)`

*Zeile 369 · 65 Zeilen*

```text
Relativer Kriminalitaetsindex je Neighborhood und Monat.

    Definition (Location Quotient der Kriminalitaetsbelastung):

        rate(i,t)     = Delikte(i, Fenster endend in t-1) / Einwohner(i)
        rate(Stadt,t) = Delikte(Stadt, gleiches Fenster) / Einwohner(Stadt)
        index(i,t)    = rate(i,t) / rate(Stadt,t)

    Lesart: 1,0 = Belastung wie im Stadtdurchschnitt desselben Monats.

    Warum relativ statt absolut? Der SFPD-Systemwechsel im Mai 2018 veraendert
    das stadtweite Niveau. Ein multiplikativer Niveausprung wirkt auf Zaehler
    und Nenner gleich und kuerzt sich heraus. Verbleibende Limitation: Eine
    Verschiebung in der ZUSAMMENSETZUNG der erfassten Delikte, die einzelne
    Stadtteile staerker trifft, kuerzt sich nicht heraus (Kap. 6.3).

    Kein Leakage: Das Fenster endet strikt im Vormonat.

    `crime_rate_raw` (Delikte je 1.000 Ew.) ist NUR deskriptiv fuer Kapitel 5.1,
    kein Modellmerkmal - sie enthaelt den Bruch von 2018.
```

#### `land_use_je_neighborhood()`

*Zeile 439 · 49 Zeilen*

```text
Parzellen-Centroid -> Neighborhood, dann Aggregation je Neighborhood.

    Statisch (Snapshot 2020, einziger verfuegbarer Jahrgang).
```

#### `berechne_quoten(df)`

*Zeile 505 · 14 Zeilen*

```text
Anteilswerte in [0,1]; Nenner <= 0 ergibt NaN statt Division durch Null.

    Kriminalitaet taucht hier NICHT auf: Sie geht als relativer Index je
    Stadtteil x Monat ein (Decision Log #17), nicht als Anteil.
```

#### `run_join()`

*Zeile 524 · 66 Zeilen*

_kein Docstring_


## `prep/s2_datensaetze.py`

*445 Zeilen · 9 Funktionen*

### Modulkopf

```text
Schritt 2: die beiden finalen Datensaetze samt Validierungsrahmen.

Eingang:  data/processed/einsaetze.parquet        (ein Einsatz je Zeile)
Ausgang:  data/processed/regression.parquet       Stadtteil x Monat, Menge
          data/processed/klassifikation.parquet   Stadtteil x Monat, Struktur

BEIDE liegen auf derselben Analyseeinheit - Stadtteil x Monat. Die eine misst
die MENGE der Einsatzlast, die andere ihre ZUSAMMENSETZUNG. Damit laufen beide
Teile der Arbeit durch denselben Rahmen: gleiche Zeilen, gleiche Merkmale,
gleiche Folds (Gutachten R1, Decision Log #29).

Der Validierungsrahmen steht ebenfalls hier, weil die Aufteilung als SPALTEN in
die Dateien geschrieben wird. "Alle Verfahren sehen identische Folds" ist damit
eine Zusage ueber den DATENSATZ, nicht ueber die Algorithmen - sie haengt nicht
davon ab, dass jedes Modellskript die richtige Funktion aufruft.

  TEIL A  Stadtteil-Split: Folds und Hold-out
  TEIL B  Menge        Aggregation, Exposure, Rate, Saison, Lags
  TEIL C  Struktur     Anteile der vier NFIRS-Gruppen

Steckbrief und Kennzahlen der erzeugten Dateien: docs/03_STAND.md

Ausfuehren:
  python prep/s2_datensaetze.py          # beide Datensaetze bauen
  python prep/s2_datensaetze.py splits   # nur die Aufteilung anzeigen
```

### Kommentarbloecke (16)

**Zeile 60**

```text
==========================================================================
 TEIL A  STADTTEIL-SPLIT
 ==========================================================================
 Geprueft wird, indem ganze Stadtteile zurueckgehalten werden: 6 ins Hold-out,
 die uebrigen 29 auf 5 Folds (6/6/6/6/5), jeder genau einmal Testfall. Ein
 Zeitschnitt wuerde die Forschungsfrage nicht pruefen - dort steht jeder
 Stadtteil in Training UND Test (Decision Log #29).
 ==========================================================================
```

**Zeile 140**

```text
==========================================================================
 Von beiden Datensaetzen genutzt
 ==========================================================================
```

**Zeile 175**

```text
==========================================================================
 TEIL B  REGRESSION
 ==========================================================================
```

**Zeile 219**

```text
Nur VORWAERTS fuellen. KEIN bfill: Rueckwaertsfuellen wuerde fehlende Werte
 (z. B. akademikerquote vor ACS 2014) still mit ZUKUNFTSWERTEN imputieren -
 Leakage (Decision Log #10). Echte NaN bleiben sichtbar.
```

**Zeile 229**

```text
Kriminalitaetsindex logarithmieren (#17/#19): 0 = Stadtdurchschnitt.
 Nullwerte wuerden -inf erzeugen und werden zu NaN, damit sie sichtbar
 bleiben statt still zum Extremwert zu werden.
```

**Zeile 260**

```text
LAGS - Strikt rueckwaertsgerichtet und je Stadtteil gebildet, nie ueber
 Stadtteilgrenzen hinweg. Beim gleitenden Mittel steht `shift(1)` VOR
 `rolling(3)`: der Wert fuer Monat t verwendet t-1, t-2, t-3, nie t selbst.
```

**Zeile 269**

```text
Vorlauf abschneiden. Erst danach greift die NaN-Pruefung des balancierten
 Panels - die Vorlaufmonate haben absichtlich keine Strukturmerkmale (der
 Kriminalitaetsindex beginnt erst 2015-01) und duerfen die Stadtteilauswahl
 nicht beeinflussen.
```

**Zeile 279**

```text
Balanciertes Panel (Decision Log #15): Stadtteile ohne durchgaengige
 ACS-Abdeckung fliegen GANZ raus. Zeilenweises dropna erzeugte sonst ein
 unbalanciertes Panel - ein Stadtteil tritt mitten in der Zeitreihe hinzu.
```

**Zeile 290**

```text
Sicherheitsnetz bei zu kurzem Vorlauf: Anlaufmonate ohne lag_12 entfallen
 fuer ALLE Verfahren gleichermassen, sonst liefen sie auf unterschiedlichen
 Zeilen (Fairness-Regel).
```

**Zeile 298**

```text
Zweite Zielgroesse: Einsaetze je 1.000 Einwohner. Fuer den Vergleich
 zwischen unterschiedlich grossen Stadtteilen ist die Rate die
 aussagekraeftigere Groesse - die absolute Zahl bildet vor allem die
 Einwohnerzahl ab (Decision Log #29).
```

**Zeile 304**

```text
REPRODUZIERBARKEITSVERTRAG - diese Sortierung darf nicht veraendert
 werden: Random Forest und XGBoost ziehen ihre Bootstrap- bzw.
 Subsample-Stichproben ueber Zeilenpositionen. Eine andere Reihenfolge
 liefert trotz identischem random_state leicht andere Baeume (empirisch
 17,2587 statt 17,2974 RMSE in Fold 1). Ridge ist reihenfolgeinvariant.
```

**Zeile 315**

```text
==========================================================================
 TEIL C  STRUKTUR DER EINSATZLAST
 ==========================================================================
```

**Zeile 364**

```text
Zielgroesse der Klassifikation: die haeufigste Einsatzart des Monats.
 Eine echte Klasse, kein gesetzter Schwellwert - argmax ueber die vier
 NFIRS-Gruppen. Damit entfaellt die Begruendungslast, die eine kuenstliche
 Einteilung einer Zaehlgroesse mit sich braechte (Altman & Royston 2006).
```

**Zeile 395**

```text
==========================================================================
 Ablauf
 ==========================================================================
```

**Zeile 408**

```text
Fold-Zuteilung EINMAL fuer beide Datensaetze, stratifiziert nach der
 seltensten Klasse und der Bevoelkerung. Sie muss identisch sein, sonst
 waeren die Ergebnisse der beiden Straenge nicht vergleichbar.
```

**Zeile 423**

```text
Einzige Kennzahl, die hier gedruckt wird: die Brand-Testfaelle je Fold.
 Sie ist der Grund fuer die doppelte Stratifizierung (#30) und der
 einzige Wert, der beim Lauf tatsaechlich kontrolliert werden muss.
 Alles Weitere steht in docs/03_STAND.md und wird von den Tests geprueft.
```

### Funktionen

#### `ergaenze_aufteilung(daten, versatz, selten)`

*Zeile 68 · 35 Zeilen*

```text
Schreibt `fold` (0..N_FOLDS) und `ist_holdout` in den Datensatz.

    Die Stadtteile werden reihum auf N_FOLDS + 1 Gruppen verteilt; Gruppe 0 ist
    das Hold-out. Wer wohin kommt, haengt allein von der Sortierreihenfolge ab,
    und die stratifiziert doppelt (Decision Log #30):

      1. `selten`  nach der Zahl brand-dominierter Monate. Ohne dieses Kriterium
                   hatte ein Fold regelmaessig KEINEN Brand-Testfall - Macro-F1
                   mittelt dann ueber eine im Test nicht vorhandene Klasse.
      2. `bev`     nach Bevoelkerung bei Gleichstand, damit kein Fold nur aus
                   Grossstadtteilen besteht. Sonst waere die Fold-Streuung ein
                   Groesseneffekt statt eines Modellunterschieds.

    Kein Leakage: Das Modell bekommt keine zusaetzliche Information, es wird nur
    festgelegt, welche Stadtteile gemeinsam getestet werden - wie bei
    `StratifiedGroupKFold`.

    `versatz` verschiebt den Startpunkt der Austeilung, fuer WIEDERHOLTE Splits.
    Bei 29 Entwicklungsstadtteilen schwankt ein einzelner Fold stark; erst ueber
    Wiederholungen gemittelt ist die Schaetzung stabil.
```

#### `fold_masken(daten, k)`

*Zeile 105 · 12 Zeilen*

```text
Trainings- und Testmaske des Folds k - allein aus den Spalten der Datei.

    Test  = die Stadtteile dieses Folds, mit allen ihren Monaten.
    Train = alle uebrigen Entwicklungs-Stadtteile, ohne das Hold-out.
    Kein Stadtteil ist je zugleich Trainings- und Testfall.
```

#### `beschreibe_splits(daten)`

*Zeile 119 · 19 Zeilen*

```text
Menschenlesbare Zusammenfassung der Aufteilung (fuer Kap. 5.2/5.4).

    Welcher Stadtteil in welchem Fold getestet wird, und dass jeder Fold den
    vollen Zeitraum abdeckt - das unterscheidet den Stadtteil-Split vom frueher
    verwendeten Zeitschnitt.
```

#### `_monat_minus(jahr_monat, monate)`

*Zeile 143 · 5 Zeilen*

```text
Verschiebt einen jahr_monat-Schluessel um n Monate zurueck.
```

#### `_setze_datentypen(d, merkmale)`

*Zeile 150 · 23 Zeilen*

```text
Vereinheitlicht die Datentypen auf modelltaugliche NumPy-Typen.

    Merkmale float64 · Schluessel, Zaehlgroessen und Steuerspalten int64 ·
    stadtteil str.

    Notwendig, weil EINE nullable Int64-Spalte im Merkmalssatz genuegt, damit
    `X.to_numpy()` ein object-Array liefert - sklearn faengt das still ab,
    XGBoost lehnt es ab (Decision Log #24).
```

#### `aggregiere(von, bis, mit_parkgebieten, verbose)`

*Zeile 178 · 64 Zeilen*

```text
Einsatz-Ebene -> Stadtteil x Monat, vollstaendiges Raster.

    `von`/`bis` sind jahr_monat-Schluessel INKLUSIVE Lag-Vorlauf.
```

#### `baue_regression(vorlauf, verbose)`

*Zeile 244 · 69 Zeilen*

```text
Der vollstaendige Regressionsdatensatz.

    LAG-VORLAUF (Decision Log #23): Aggregiert wird ab START minus `vorlauf`
    Monaten, damit lag_12 schon fuer den ersten Analysemonat definiert ist.
    Danach Zuschnitt auf START - die Vorlaufmonate gehen ausschliesslich ueber
    shift() ein, nie als eigene Zeile.
```

#### `baue_klassifikation(regression, verbose)`

*Zeile 318 · 75 Zeilen*

```text
Anteile der vier NFIRS-Gruppen je Stadtteil und Monat.

    Zielgroesse ist die ZUSAMMENSETZUNG der Einsatzlast, nicht die Art des
    einzelnen Einsatzes (Decision Log #29). Innerhalb eines Stadtteil-Monats
    tragen alle Einsaetze identische Strukturmerkmale; auf Einzeleinsatz-Ebene
    war deshalb nichts zu holen - ein perfektes Modell haette 49,9 % Treffer
    erreicht gegenueber 48,2 % fuer blosses Raten. Auf dieser Ebene ist die
    Frage beantwortbar.

    Zeilen, Zeitraum, Stadtteile, Merkmale und Folds werden dem
    Regressionsdatensatz ENTNOMMEN. Beide Teile der Arbeit beruhen damit
    zwingend auf demselben Datenbestand und derselben Aufteilung.
```

#### `run(verbose)`

*Zeile 398 · 34 Zeilen*

```text
Beide finalen Datensaetze bauen, Folds eintragen und schreiben.

    Die Fold-Zuteilung erfolgt EINMAL und wird auf beide Datensaetze angewandt -
    nur so sehen Menge und Struktur dieselben Stadtteile im Test (Fairness-Regel).
```


## `prep/build.py`

*92 Zeilen · 3 Funktionen*

### Modulkopf

```text
DER EINE BEFEHL. Erzeugt aus den Rohdaten die beiden finalen Datensaetze.

    python prep/build.py

Ablauf:

    1  s1_daten.py        -> data/raw/*  (nur was in config.py auf True steht)
                          -> data/processed/einsaetze.parquet   Zwischenstand
    2  s2_datensaetze.py  -> data/processed/regression.parquet      FINAL
                          -> data/processed/klassifikation.parquet  FINAL

Danach ist die Aufbereitung fertig. Die beiden FINAL markierten Dateien sind
modellfertig: identische Zeilen, Merkmale und Folds fuer alle Verfahren.

Dieser Ordner erzeugt DATEN und sonst nichts. Die Messlatte (Baselines) und die
Eignungspruefung liegen in vorpruefung/, der Verfahrensvergleich in modelle/.

Argumente (optional):
    python prep/build.py daten        wie ohne Argument
    python prep/build.py tests        anschliessend die Pruefungen laufen lassen

Downloads werden ueber die DOWNLOAD_*-Schalter in config.py gesteuert. Stehen
sie auf False (Default), arbeitet der Befehl allein aus data/raw und braucht
weder Internet noch API-Key.
```

### Funktionen

#### `schritt(nummer, titel)`

*Zeile 43 · 3 Zeilen*

_kein Docstring_

#### `uebersicht()`

*Zeile 48 · 16 Zeilen*

_kein Docstring_

#### `main()`

*Zeile 66 · 23 Zeilen*

_kein Docstring_


# Vorpruefung — `vorpruefung/`


## `vorpruefung/run.py`

*47 Zeilen · 2 Funktionen*

### Modulkopf

```text
DER EINE BEFEHL der Vorpruefung.

    python vorpruefung/run.py

Ablauf:

    1  v1_baselines.py  -> results/{regression,klassifikation}/baselines_*.csv
                           Stufe 1 (trivial) und Stufe 2 (einfachste passende Form)
    2  v2_eignung.py    -> results/eignungspruefung/eignungspruefung.md
                           Welche Verfahrensklasse passt zu welcher Zielgroesse?

Reihenfolge ist zwingend: Die Eignungspruefung LIEST die Baseline-Werte.

Danach steht fest, was die Vergleichsverfahren in modelle/ schlagen muessen und
warum sie ueberhaupt antreten. Voraussetzung ist ein Lauf von prep/build.py.
```

### Funktionen

#### `schritt(nummer, titel)`

*Zeile 25 · 2 Zeilen*

_kein Docstring_

#### `main()`

*Zeile 29 · 15 Zeilen*

_kein Docstring_


## `vorpruefung/v0_aufteilung.py`

*224 Zeilen · 4 Funktionen*

### Modulkopf

```text
Wiederholte Splits - die eine Stelle, an der die Fold-Zuteilung je Wiederholung
entsteht.

WOZU DIESE DATEI UEBERHAUPT EXISTIERT
--------------------------------------------------------------------------
Die Grundaufteilung steht als Spalten `fold` und `ist_holdout` in beiden
Parquet-Dateien; erzeugt hat sie `prep/s2_datensaetze.ergaenze_aufteilung()`.
Fuer die WIEDERHOLTEN Splits (docs/04_MODELLIERUNG.md, Abschnitt 2) reicht sie
nicht aus. Zwei nachgewiesene Gruende, beide am 05.08.2026 am Datensatz
gemessen (docs/07_BEFUNDE.md, B-1 und B-2):

  1  Der `versatz` verteilt die Stadtteile reihum auf N_FOLDS + 1 = 6 Gruppen.
     Wer i den Platz i belegt, landet in Gruppe (i + versatz) % 6. Zwei
     Stadtteile liegen also genau dann in derselben Gruppe, wenn ihre Plaetze
     modulo 6 uebereinstimmen - UNABHAENGIG vom Versatz. Der Versatz rotiert
     damit nur die Beschriftung der Gruppen, nicht ihre Zusammensetzung. Ueber
     versatz 0..9 entstehen 6 verschiedene Konstellationen, davon 4 Dubletten.

  2  Rotiert die Beschriftung, rotiert auch Gruppe 0 - und Gruppe 0 IST das
     Hold-out. Gemessen: bei versatz = 1 liegt kein einziger der sechs
     urspruenglichen Hold-out-Stadtteile mehr im Hold-out. Die Wiederholungen
     1 bis 9 wuerden auf genau den Stadtteilen trainieren und testen, die bis
     zur Schlussbewertung unberuehrt bleiben muessen.

Deshalb hier eine eigene Funktion mit drei Zusagen:

  Das Hold-out bleibt FEST      die sechs Stadtteile mit ist_holdout == 1 aus
                                der Datei, in jeder Wiederholung dieselben
  Die Stratifizierung bleibt    sortiert nach brand-dominierten Monaten, bei
                                Gleichstand nach Bevoelkerung (Decision Log #30)
  Wiederholung 0 = die Datei    bitgenau dieselbe fold-Spalte; das wird bei
                                jedem Aufruf per assert nachgeprueft

WIE DIE WIEDERHOLUNGEN ENTSTEHEN
--------------------------------------------------------------------------
Die 29 Entwicklungsstadtteile werden wie bisher nach (selten, bev) absteigend
sortiert und reihum ausgeteilt. Neu ist nur, dass vor dem Austeilen INNERHALB
der Rangbloecke gemischt wird - Block 0 sind die Plaetze 0-4, Block 1 die
Plaetze 5-9 und so fort:

    Rangblock 0   [Bayview, Bernal, Portola, Seacliff, Twin Peaks]
    Rangblock 1   [...]                        -> jeder Block liefert genau
    ...                                           EINEN Stadtteil je Fold

Damit bekommt jeder Fold weiterhin genau einen Stadtteil aus jedem Rangblock -
die doppelte Stratifizierung ueberlebt das Mischen unveraendert, und die
Foldgroessen bleiben 6/6/6/6/5. Anders als beim Versatz aendert sich aber die
ZUSAMMENSETZUNG der Folds, nicht nur ihre Nummer. Genau das brauchen die
wiederholten Splits.

Kein Leakage: Gemischt wird ausschliesslich die Frage, welche Stadtteile
gemeinsam getestet werden. Kein Modell sieht dadurch eine Zeile mehr.

Benutzt von `vorpruefung/v1_baselines.py`, `modelle/m02_menge.py` und
`modelle/m03_struktur.py` - alle drei muessen dieselbe Zuteilung sehen, sonst
vergleicht der gepaarte Wilcoxon-Test still auf verschiedenen Zeilen.

Selbsttest:
  python vorpruefung/v0_aufteilung.py
```

### Kommentarbloecke (1)

**Zeile 166**

```text
==========================================================================
 Selbsttest - beantwortet die vier Fragen, an denen diese Datei haengt
 ==========================================================================
```

### Funktionen

#### `selten_je_stadtteil(klassifikation)`

*Zeile 84 · 15 Zeilen*

```text
Zahl der brand-dominierten Monate je Stadtteil - das Stratifizierungsmass.

    Wortgleich zu dem, was `prep/s2_datensaetze.run()` beim Bau der Dateien
    gerechnet hat. Es steht hier noch einmal, weil die Modellskripte den Wert
    brauchen, er aber nicht in den Dateien abgelegt ist: In `regression.parquet`
    gibt es keine Klassenspalte, und die fold-Spalte allein sagt nicht, WIE sie
    zustande kam.

    Fuer die Regression heisst das: `klassifikation.parquet` mitlesen, auch wenn
    nur die Menge modelliert wird. Das ist kein Leakage - die Zahl geht in kein
    Modell ein, sie bestimmt nur, welche Stadtteile gemeinsam getestet werden.
```

#### `wiederholte_aufteilung(daten, wiederholung, selten)`

*Zeile 101 · 52 Zeilen*

```text
Schreibt die fold-Spalte fuer eine Wiederholung. Hold-out bleibt fest.

    `wiederholung` 0 liefert exakt die Aufteilung aus der Datei - das wird per
    assert geprueft, nicht nur behauptet. 1 bis WIEDERHOLUNGEN-1 liefern
    verschiedene Zusammensetzungen bei gleicher Stratifizierung.

    `selten` ist die Reihe aus `selten_je_stadtteil()`. Fehlt sie, wird nur nach
    Bevoelkerung stratifiziert - das reproduziert die Dateien NICHT und ist nur
    fuer Sonderfaelle gedacht.

    Rueckgabe ist eine Kopie mit neu belegter Spalte `fold`; `ist_holdout` wird
    unveraendert uebernommen. Die Hold-out-Zeilen behalten fold = 0 und werden
    von `fold_masken()` in jeder Wiederholung ausgeschlossen.
```

#### `entwicklung_und_holdout(daten)`

*Zeile 155 · 9 Zeilen*

```text
Masken fuer die Schlussbewertung: 29 Entwicklungs- gegen 6 Hold-out-Stadtteile.

    Das Gegenstueck zu `fold_masken()` fuer den einen Lauf, der das Hold-out
    ueberhaupt anfassen darf. Steht hier und nicht im Modellskript, damit es
    genau eine Stelle im Repo gibt, an der `ist_holdout == 1` gelesen wird.
```

#### `_selbsttest()`

*Zeile 169 · 52 Zeilen*

_kein Docstring_


## `vorpruefung/v1_baselines.py`

*378 Zeilen · 8 Funktionen*

### Modulkopf

```text
Stufe 1 und 2: die Messlatte.

Eine Baseline ist eine bewusst einfache Regel, die dieselbe Aufgabe loest und
dieselben Daten sieht. Sie legt fest, was die Vergleichsverfahren mindestens
schlagen muessen. Es gibt zwei Stufen:

  STUFE 1  Triviale Referenz - benutzt KEIN einziges Merkmal.
           Regression:    Gesamtmittelwert der Trainingsstadtteile
           Klassifikation: immer die haeufigste Klasse
           Beantwortet: Steckt in den Merkmalen ueberhaupt Information?

  STUFE 2  Einfachste Referenz, die zur DATENFORM passt - benutzt alle Merkmale,
           aber in der simpelsten Form.
           Regression:    Poisson-GLM mit Offset (Zaehldaten mit Exposition)
           Klassifikation: multinomiale logistische Regression (nominale Klassen)
           Beide: kanonischer Link, unpenalisierte Maximum-Likelihood, KEIN
           freier Hyperparameter - deshalb kein Tuning (Decision Log #45).
           Beantwortet: Wie weit kommt man mit der einfachen Form?

Stufe 3 sind die Vergleichsverfahren in modelle/. Ihre Aufgabe ist zu zeigen,
dass sie Stufe 2 schlagen - sonst hat sich der Mehraufwand nicht gelohnt.

Alle Baselines laufen ueber denselben STADTTEIL-SPLIT wie die Modelle: Der
Teststadtteil ist unbekannt. Das Hold-out bleibt unberuehrt.

WARUM UEBER ALLE 10 WIEDERHOLUNGEN (Ergaenzung 05.08.2026)
--------------------------------------------------------------------------
Bis dahin lief hier nur die Aufteilung, die als `fold`-Spalte in den Dateien
steht - also fuenf Laeufe. Die Vergleichsverfahren erzeugen 50. Die
Primaeraussage nach Decision Log #34 ist aber ein GEPAARTER Test „Verfahren
gegen Stufe 2", und der braucht je Lauf einen Gegenwert auf DENSELBEN
Testzeilen. Fuer 45 der 50 Laeufe gab es keinen.

Die Baseline ist damit kein Referenzwert, sondern ein Mitbewerber unter
identischem Protokoll - so verlangt es auch Schroeters Auflage C („fuer alle
Vergleichsmodelle identische Merkmale und Splits"). Der Aufwand faellt nicht
ins Gewicht: 50 GLM-Anpassungen kosten zusammen wenige Sekunden.

Eingang:  data/processed/{regression,klassifikation}.parquet
Ausgang:  results/regression/baselines_{folds,mittel}.csv
          results/klassifikation/baselines_klasse.csv

Ausfuehren:
  python vorpruefung/v1_baselines.py
```

### Funktionen

#### `bewerte_regression(y_true, y_pred)`

*Zeile 78 · 8 Zeilen*

```text
RMSE, MAE, R2 - immer auf der ORIGINALSKALA der Zielgroesse.
```

#### `poisson_glm(train, test, merkmale)`

*Zeile 89 · 46 Zeilen*

```text
Stufe 2 der Regression: vorhergesagte Einsatzzahlen.

    `merkmale` ist der Merkmalssatz; ohne Angabe der volle. Der Parameter
    existiert allein fuer die Faktorgruppen-Ablation in `m04_shap.py`, die
    dasselbe Modell mit einer weggelassenen Gruppe anpasst. Ohne ihn muesste
    die Ablation die Spezifikation nachbauen - und dann gaebe es sie zweimal.
    Der Offset bleibt in jeder Variante bestehen: Er ist keine Merkmalsspalte.

    Poisson-GLM mit kanonischem log-Link, per unpenalisierter
    Maximum-Likelihood angepasst. `log(Bevoelkerung)` geht als OFFSET ein, also
    mit fest auf 1 gesetztem Koeffizienten: Das Modell schaetzt Einsaetze JE
    EINWOHNER und multipliziert am Ende hoch - sonst sagt es vor allem die
    Stadtteilgroesse vorher (#13). Kein freier Hyperparameter, also kein Tuning.

    WARUM POISSON UND NICHT NEGATIVE BINOMIAL (Decision Log #45). Die Zaehldaten
    sind ueberdispers (Dispersionsindex 62,8), die Poisson-Varianzannahme
    Var = mu ist also verletzt. Das ist folgenlos fuer den Zweck dieser
    Baseline: Der Poisson-Schaetzer bleibt konsistent, solange der BEDINGTE
    MITTELWERT richtig spezifiziert ist, unabhaengig von der Varianzstruktur
    (Gourieroux, Monfort & Trognon 1984, "Pseudo Maximum Likelihood Methods",
    Econometrica 52, 701-720). Was die Ueberdispersion beschaedigt, sind die
    STANDARDFEHLER - und die werden hier nicht verwendet, weil die Baseline
    ausschliesslich Punktvorhersagen liefert. Keine Koeffiziententests, keine
    Konfidenzintervalle.

    Die Negative Binomial waere die Erweiterung fuer korrekte Inferenz. Sie
    loest ein Problem, das wir nicht haben, und ist damit nicht mehr "die
    einfachste Form, die zur Datenform passt".

    Die Rate entsteht aus DERSELBEN Anpassung, geteilt durch die Bevoelkerung -
    ein zweites Modell waere eine zweite Spezifikation.
```

#### `logit_glm(train, merkmale)`

*Zeile 137 · 41 Zeilen*

```text
Stufe 2 der Klassifikation: das angepasste multinomiale Logit.

    `merkmale` wie bei `poisson_glm()`: ohne Angabe der volle Satz, sonst der
    reduzierte fuer die Faktorgruppen-Ablation.

    DIE EINZIGE STELLE, AN DER DIESES MODELL SPEZIFIZIERT IST (10.08.2026).
    Bis dahin baute `m03_struktur.hold_out()` es ein zweites Mal nach -
    dieselben vier Argumente, an zwei Orten aufgeschrieben. Aendert jemand
    eines davon, misst die Kreuzvalidierung still gegen ein anderes Modell als
    die Schlussbewertung, und keine Pruefung schlaegt an: `pruefe_zahlen.py`
    vergleicht Dokumentation gegen `results/`, nicht Code gegen Code.

    Der Mengenstrang war immer richtig gebaut - `m02_menge.hold_out()`
    importiert `poisson_glm` aus dieser Datei. Der Fehler war die Asymmetrie:
    derselbe Gedanke, einmal umgesetzt und einmal nicht.

    Linear in den Log-Odds, unpenalisiert (C = inf; `penalty=None` ist seit
    scikit-learn 1.8 veraltet, die Schaetzung ist bitgleich). Kein freier
    Hyperparameter, also kein Tuning (#45). `class_weight="balanced"` statt
    Resampling - kein SMOTE, keine duplizierte oder geloeschte Zeile.

    RUECKGABE IST DAS MODELL, nicht die Vorhersage - anders als bei
    `poisson_glm`. Beide Aufrufer brauchen aus DERSELBEN Anpassung drei Dinge:
    Klassenvorhersage, Wahrscheinlichkeiten und die Klassenreihenfolge des
    Modells. Ein zweites Fitten dafuer waere Verschwendung.

    KONVERGENZWARNUNGEN werden hier bewusst NICHT abgefangen. Sie gehoeren dem
    Aufrufer: `klassifikation()` zaehlt sie und berichtet sie
    (docs/04_MODELLIERUNG.md, Sonderfaelle). Wuerde diese Funktion sie
    schlucken, waere die Zahl still null.
```

#### `regression(panel, selten)`

*Zeile 180 · 37 Zeilen*

```text
Beide Mengen-Zielgroessen, Stufe 1 und 2, je Wiederholung und Fold.

    Die Rate ergibt sich aus derselben Poisson-Vorhersage geteilt durch die
    Bevoelkerung - ein zweites Modell waere eine zweite Spezifikation und damit
    unfair gegenueber den Vergleichsverfahren.

    50 Laeufe (10 Wiederholungen x 5 Folds) x 2 Zielgroessen x 2 Modelle
    (Nullmarke, Poisson-GLM) = 200 Zeilen.
```

#### `_zweistufig(df, schluessel, masse)`

*Zeile 219 · 28 Zeilen*

```text
Zweistufige Aggregation ueber alle Laeufe des Durchgangs.

    ZWEISTUFIG heisst: erst je Wiederholung ueber die 5 Folds mitteln, dann die
    Streuung DIESER Werte berichten.

      `std_folds`            ueber alle 50 Einzellaeufe. Zu optimistisch, weil
                             die Laeufe nicht unabhaengig sind - es sind
                             dieselben 29 Stadtteile in zehn Gruppierungen.
      `std_wiederholungen`   ueber die 10 Wiederholungsmittel. MASSGEBLICH
                             (docs/06_RISIKEN.md, R-5).

    Eine Datei beschreibt genau einen Durchgang. Frueher fuehrte sie zusaetzlich
    Zeilen fuer Wiederholung 0 allein - das war historischer Ballast und vor
    allem eine Falle: Wer den Filter vergisst, bekommt stillschweigend die
    falsche Baseline. Wer diese Werte braucht, filtert `baselines_folds.csv`
    auf `wiederholung == 0`; dort steht jeder Einzellauf.
```

#### `klassifikation(kl, selten)`

*Zeile 250 · 67 Zeilen*

```text
Beide Stufen der Klassifikation, je Wiederholung und Fold.

    STUFE 1, Mehrheitsklasse: sagt immer die im Training haeufigste Einsatzart
    vorher. Accuracy faellt hoch aus, Macro-F1 niedrig - genau deshalb ist
    Macro-F1 das massgebliche Guetemass.

    STUFE 2, multinomiale logistische Regression: das Gegenstueck zum
    Poisson-GLM - dieselbe Modellklasse, derselbe kanonische Link, derselbe
    Verzicht auf einen Strafterm. Sie ist die einfachste Form, die zu einer
    nominalen Zielgroesse passt. RF und XGBoost muessen SIE schlagen, nicht
    die Mehrheitsklasse (Decision Log #33). Spezifiziert ist sie in
    `logit_glm()` - an genau einer Stelle, siehe dort.

    Zwei Ergaenzungen vom 05.08.2026, beide additiv:
      - Schleife ueber die 10 Wiederholungen, damit m03 gepaart testen kann
      - Macro-AUROC wird fuer Stufe 2 mitgerechnet. Ohne sie gaebe es fuer das
        zweite Guetemass der Klassifikation keine Messlatte. Fuer die
        Mehrheitsklasse ist sie nicht definiert (eine konstante Vorhersage hat
        keine Rangfolge) und bleibt leer - NICHT 0,5, das waere eine erfundene
        Zahl.

    Konvergenzwarnungen werden GEZAEHLT und zurueckgegeben, nicht unterdrueckt
    (docs/04_MODELLIERUNG.md, Sonderfaelle).
```

#### `_macro_auroc(y_true, proba, klassen_modell, klassen_alle)`

*Zeile 319 · 18 Zeilen*

```text
Macro-AUROC (One-vs-Rest), oder NaN wenn im Testfold eine Klasse fehlt.

    NICHT durch 0,5 oder 0 ersetzen: Ein erfundener Wert zoege den Mittelwert
    nach unten und saehe wie ein Messergebnis aus (docs/04_MODELLIERUNG.md,
    Sonderfaelle). Durch die doppelte Stratifizierung (#30) sollte der Fall
    nicht eintreten - wenn doch, muss er sichtbar bleiben.
```

#### `run()`

*Zeile 340 · 35 Zeilen*

_kein Docstring_


## `vorpruefung/v2_eignung.py`

*699 Zeilen · 12 Funktionen*

### Modulkopf

```text
Eignungspruefung: Passen die gewaehlten Verfahren zu den Zielgroessen?

Zweiter Schritt der Vorpruefung. Setzt `v1_baselines.py` voraus - die
Baseline-Werte werden gelesen, nicht neu gerechnet.

Sechs Belege, mehr nicht:

  1  Zaehldaten sind ueberdispers          ->  zaehldatengerechte Verlust-
                                               funktionen (#42); die Stufe-2-
                                               Baseline bleibt das Poisson-GLM
                                               (#45, Begruendung in Abschnitt 1)
  2  Zusammenhaenge sind nicht linear      ->  Ridge auf log(1+y), nicht roh
  3  Lineare Spezifikation reicht nicht    ->  Random Forest und XGBoost
  4  Teststadtteile liegen oft ausserhalb  ->  Limitation, keine Verfahrensfrage
  5  Merkmale trennen auch die Einsatzart  ->  RF und XGBoost im 2. Strang
  6  Anforderungen je Verfahren geprueft   ->  Tabelle mit Teststatistik und
                                               p-Wert, Auflage vom 10.08.2026

Abschnitt 6 ist Auflage Schroeter (10.08.2026): "Pruefung ob die Algorithmen
auf den Daten passen, z.B. Varianzgleichheit, linearer Zusammenhang ... Jeder
Algorithmus sollte dargestellt werden ... Test laufen lassen: in Tabelle
Statistiken mit p-Werten anzeigen." Die Abschnitte 1 bis 5 belegen die
VERFAHRENSWAHL, Abschnitt 6 fuehrt die Anforderungen je Verfahren zusammen -
einschliesslich der Zeilen, in denen eine Anforderung GAR NICHT besteht. Genau
die gehoeren hin: Dass Baumverfahren keine Verteilungsannahme haben, ist eine
Aussage und keine Auslassung.

Abschnitt 2 ist Auflage Schroeter (R7): "erstmal plotten, falls keine lineare
Baseline, KEIN lineares Regressionsmodell." Deshalb Streudiagramme und
Residuenanalyse, beides als Abbildung.

Abschnitt 5 ist noetig, weil die Regression den Klassifikationsstrang NICHT
mitbeantwortet: Dass der Zusammenhang zur Anzahl gekruemmt ist, sagt nichts
darueber, ob dieselben Merkmale die Art der Einsaetze trennen koennen.

Was diese Pruefung NICHT leistet: Sie unterscheidet nicht zwischen Random
Forest und XGBoost. Welche der beiden Strategien gewinnt, ist die empirische
Forschungsfrage der Arbeit - vorab noetig ist nur, dass beide plausibel sind.

Gerechnet wird ausschliesslich auf den TRAININGSSTADTTEILEN VON FOLD 1 - die
Teststadtteile duerfen keine Modellentscheidung beeinflussen. Ausgenommen sind
Abschnitt 4 (Extrapolation, betrifft alle Folds naturgemaess) und die aus
v1_baselines.py gelesenen Referenzwerte.

Der Bericht ist ein BEFUNDBLATT, keine Kapitelvorlage: Er liefert Zahlen und
Abbildungen, die Argumentation fuer Kapitel 6.2 wird von Hand geschrieben.

Eingang:  data/processed/{regression,klassifikation}.parquet
          results/klassifikation/baselines_klasse.csv
Ausgang:  results/eignungspruefung/eignungspruefung.md + 2 Abbildungen
          results/eignungspruefung/annahmen.csv      Abschnitt 6, maschinenlesbar
          results/eignungspruefung/qq_residuen.csv   Rohdaten fuer Abbildung A10

Ausfuehren:
  python vorpruefung/v2_eignung.py
```

### Kommentarbloecke (5)

**Zeile 186**

```text
Massgeblich ist das MAXIMUM beider Korrelationen, nicht Pearson allein.
 Grund: Ist ein Zusammenhang stark gekruemmt, faellt Pearson gerade
 deshalb ab - ein reiner Pearson-Filter wuerde also ausgerechnet den
 staerksten Kruemmungsbefund aussortieren. Umgekehrt bleibt der Zweck
 erhalten: Liegen BEIDE Korrelationen nahe null, ist ihr Abstand Rauschen
 und sagt nichts ueber die Funktionsform.
```

**Zeile 387**

```text
DIE SPALTE "je Fold" ZEIGT NUR WIEDERHOLUNG 0 - nachgezogen 10.08.2026.
 `v1_baselines.py` liefert seit dem 05.08. 50 Laeufe statt 5 (10
 Wiederholungen x 5 Folds), damit m03 gepaart testen kann. Diese Tabelle
 hat weiterhin alle Zeilen aufgereiht: eine Zelle mit 50 durch Punkte
 getrennten Werten, unlesbar und als "je Fold" auch falsch beschriftet.

 Der MITTELWERT bleibt bewusst ueber ALLE Laeufe gebildet - das ist der
 Wert, der in 03_STAND.md steht und gegen den m03 antritt. Gezeigt werden
 die fuenf Folds der Wiederholung 0, weil sie die Aufteilung aus der Datei
 sind (v0_aufteilung) und damit die nachvollziehbare.
```

**Zeile 494**

```text
--- Ueberdispersion, formal (Cameron & Trivedi 1990) -----------------
 Hilfsregression: z = ((y - mu)^2 - y) / mu auf mu, ohne Konstante. Der
 Koeffizient ist der Dispersionsparameter alpha der NB2-Form, H0 lautet
 alpha = 0 (Equidispersion). Einseitig, weil Unterdispersion hier keine
 sinnvolle Gegenhypothese waere.
```

**Zeile 506**

```text
--- Streuung und Verteilung der Residuen -----------------------------
 Geprueft wird das lineare Modell, fuer das Ridge steht: OLS auf log(1+y)
 mit denselben zwoelf Merkmalen. Ridge selbst hat denselben Erwartungswert
 und unterscheidet sich nur durch den Strafterm.
```

**Zeile 668**

```text
Die Abschnitte 1, 3 und 4 geben ihre Kennzahlen zurueck, damit
 Abschnitt 6 sie nicht ein zweites Mal rechnen muss. Zweimal gerechnet
 hiesse zwei Zahlen, die auseinanderlaufen koennen.
```

### Funktionen

#### `log(txt)`

*Zeile 81 · 3 Zeilen*

_kein Docstring_

#### `speichere(fig, name)`

*Zeile 86 · 5 Zeilen*

_kein Docstring_

#### `dispersion(train)`

*Zeile 94 · 68 Zeilen*

```text
Beleg 1: Die Zaehldaten sind ueberdispers - und was daraus folgt.

    NEU GEFASST AM 10.08.2026. Bis dahin schloss dieser Abschnitt aus der
    Overdispersion, Poisson scheide aus und die Negative Binomial sei die
    passende Baseline. Decision Log #45 hat am 06.08. das Gegenteil entschieden
    und ist am 08.08. freigegeben worden - der Abschnitt argumentierte danach
    gegen die eigene Umsetzung, und die erzeugte `eignungspruefung.md` trug den
    Widerspruch weiter. Kein Rechenfehler, sondern Drift.

    DIE KORREKTE FOLGERUNG hat zwei Aeste, und nur der erste betrifft die
    Baseline:

      Verlustfunktion   Ein quadratischer Fehler auf rohen Zaehldaten ist bei
                        diesem Dispersionsindex unangemessen. Daraus folgt
                        `reg:tweedie` fuer XGBoost und `criterion="poisson"`
                        fuer den Random Forest (#42). Das ist die eigentliche
                        Konsequenz aus dieser Messung.

      Baseline          Die Overdispersion verletzt die Poisson-Varianzannahme
                        Var = mu. Beschaedigt werden dadurch die
                        STANDARDFEHLER, nicht die Konsistenz des geschaetzten
                        bedingten Mittelwerts (Gourieroux, Monfort & Trognon
                        1984). Eine Baseline, die ausschliesslich
                        Punktvorhersagen liefert - keine Koeffiziententests,
                        keine Konfidenzintervalle -, ist davon nicht betroffen.
                        Das Poisson-GLM bleibt Stufe 2 (#45).

    Die Negative Binomial waere die Erweiterung fuer korrekte INFERENZ. Sie
    loest damit ein Problem, das diese Baseline nicht hat, und bringt mit dem
    Dispersionsparameter eine zusaetzliche Groesse mit - sie ist dann nicht
    mehr "die einfachste Form, die zur Datenform passt".

    Der gemessene Index bleibt unveraendert und wird weiterhin berichtet. Er
    ist nicht falsch geworden, er traegt nur eine andere Schlussfolgerung.
```

#### `linearitaet(train)`

*Zeile 165 · 94 Zeilen*

```text
Beleg 2: Auflage Schroeter (R7) - plotten, bevor Ridge eingesetzt wird.

    Zwei Diagnosen. Pearson misst den LINEAREN, Spearman den MONOTONEN
    Zusammenhang; klaffen sie auseinander, ist der Zusammenhang gekruemmt.
    Bewertet wird das nur, wo die Korrelation ueberhaupt substanziell ist - bei
    einer Korrelation nahe null ist der Abstand Rauschen.

    Danach Ridge einmal auf der Rohskala und einmal auf log(1+y), mit
    Residuenbild. Ein Trichter zeigt, dass der Fehler mit dem Niveau waechst.
```

#### `spezifikation(train)`

*Zeile 262 · 45 Zeilen*

```text
Beleg 3: Der RESET-Test verwirft die lineare Spezifikation.

    Er prueft, ob Potenzen der Vorhersage noch etwas erklaeren. Tun sie das,
    hat das lineare Modell Struktur uebrig gelassen. Die Interaktionsterme
    zeigen anschliessend, dass ein Teil davon Wechselwirkungen sind.
```

#### `extrapolation(panel)`

*Zeile 310 · 25 Zeilen*

```text
Beleg 4: Wie oft liegt ein Teststadtteil ausserhalb des Gelernten?
```

#### `klassifikation(kl)`

*Zeile 338 · 84 Zeilen*

```text
Beleg 5: Taugen dieselben Merkmale auch fuer die Einsatzart?

    Eine Frage, die die Regression NICHT mitbeantwortet. Dass der Zusammenhang
    zur ANZAHL gekruemmt ist, sagt nichts darueber, ob dieselben Merkmale die
    ART trennen koennen - das sind zwei verschiedene Fragen an dieselben Spalten.

    Geprueft wird per Kruskal-Wallis je Merkmal ueber die vier Klassen:
    nichtparametrisch, vertraegt ungleich grosse Gruppen. Trennt kein Merkmal,
    ist die Zielgroesse mit diesen Praediktoren nicht vorhersagbar - und zwar
    fuer JEDES Verfahren.

    Danach werden die beiden Baseline-Stufen aus v1_baselines.py berichtet, um
    das Signal ins Verhaeltnis zu setzen: Wie viel davon schoepft ein lineares
    Modell aus? OB flexiblere Verfahren mehr herausholen, beantwortet m03 - mit
    getunten Modellen ueber alle Wiederholungen und nicht mit einer Vorschau.
```

#### `_z(wert, stellen)`

*Zeile 425 · 3 Zeilen*

```text
Teststatistik mit deutschem Dezimalkomma - wie die p-Spalte daneben.
```

#### `_p(wert)`

*Zeile 430 · 12 Zeilen*

```text
p-Wert deutsch. Unter 0,001 wird nicht mehr beziffert, sondern begrenzt.

    Grund: `4.0e-47` ist keine Information, die jemand liest - die Aussage ist
    "praktisch null". Bei n = 3.036 findet ein Test ohnehin fast jede
    Abweichung; die Effektgroesse traegt, nicht die Nachkommastelle.
```

#### `annahmen(train, befunde)`

*Zeile 444 · 205 Zeilen*

```text
Beleg 6: Was verlangt jedes Verfahren - und haelt der Datensatz das?

    AUFLAGE SCHROETER, 10.08.2026. Verlangt sind drei Dinge: die Anforderungen
    JE VERFAHREN dargestellt, formale Tests statt Augenmass, und beides in
    einer Tabelle mit Teststatistik und p-Wert.

    DREI SORTEN VON ZEILEN, und die dritte ist die wichtigste:

      erfuellt            die Anforderung besteht und ist eingehalten
      verletzt            sie besteht und ist verletzt - dann steht in der
                          Spalte "Konsequenz", was daraus folgt
      nicht erforderlich  das Verfahren stellt diese Anforderung gar nicht

    Die dritte Sorte wegzulassen waere der Fehler. Dass Random Forest keine
    Verteilungsannahme hat, ist eine AUSSAGE ueber das Verfahren - und sie ist
    der halbe Grund, warum es im Vergleich steht. Eine Tabelle, die nur
    verletzte Annahmen zeigt, laesst die Baumverfahren voraussetzungslos
    aussehen; eine, die sie ganz weglaesst, beantwortet die Auflage nicht.

    DREI NEUE TESTS, die es vorher nicht gab:

      Cameron & Trivedi (1990)  Hilfsregression auf Ueberdispersion. Der
                                Dispersionsindex aus Abschnitt 1 ist eine
                                Kennzahl, kein Test - hier steht der t-Wert.
      Breusch-Pagan             Varianzgleichheit der Residuen. Woertlich in
                                der Auflage genannt.
      Jarque-Bera               Normalitaet der Residuen, dazu Schiefe und
                                Woelbung. Die zugehoerige Abbildung ist A10;
                                die Rohdaten dafuer schreibt diese Funktion
                                nach `qq_residuen.csv`, gezeichnet wird in
                                m05 - dieses Skript erzeugt Befunde, keine
                                druckfertigen Abbildungen.

    WAS HIER NICHT STEHT: die Multikollinearitaet. Der VIF wird in
    `m04_shap._vif()` gerechnet, weil seine einzige echte Konsequenz die
    Interpretation der Beitraege betrifft. Ihn hier zu wiederholen hiesse,
    dieselbe Zahl an zwei Orten zu fuehren - genau die Fehlerquelle, die
    `tools/pruefe_zahlen.py` bewacht.
```

##### innere Funktion `Z()`

```text
Eine Zeile der Anforderungstabelle.

        `statistik` ist die LESBARE Fassung mit Dezimalkomma, `wert` dieselbe
        Zahl maschinenlesbar. Beides, weil `tools/pruefe_zahlen.py` den Sollwert
        aus dieser Datei zieht und "t = 17,2" dafuer erst geparst werden
        muesste - eine Zeichenkette, die man parst, ist eine Zeichenkette, die
        sich beim naechsten Formatwechsel anders parst.
```

#### `main()`

*Zeile 652 · 44 Zeilen*

_kein Docstring_


## `vorpruefung/v3_spezifikation.py`

*264 Zeilen · 6 Funktionen*

### Modulkopf

```text
Haelt die diagnostizierte Nichtlinearitaet out-of-sample nach?

    python vorpruefung/v3_spezifikation.py

Eingang: data/processed/regression.parquet
Ausgang: results/spezifikation/spezifikation_{folds,mittel}.csv

STAND: vollstaendig, 07.08.2026.

--------------------------------------------------------------------------
WOZU DIESES SKRIPT
--------------------------------------------------------------------------
`v2_eignung.py` weist nach, dass die lineare Spezifikation nicht ausreicht:
der RESET-Test verwirft sie deutlich (F = 215,2 bei Potenzen bis 2), und 45
Interaktionsterme heben das adjustierte R2 von 0,805 auf 0,919. Daraus wurde
die Wahl der Baumverfahren begruendet - sie fangen Kruemmung und
Wechselwirkungen ohne Zutun ab.

Beide Kennzahlen sind IN-SAMPLE-Groessen, berechnet auf 3.828 Zeilen, die als
unabhaengig behandelt werden. Tatsaechlich liegen 29 unabhaengige Stadtteile
mit je 132 Monaten vor. Ein F-Test mit n = 3.828 findet praktisch jede
Abweichung signifikant, und adjustiertes R2 korrigiert fuer die Zahl der
Parameter, nicht fuer die geklumpte Struktur.

Die Diagnose beantwortet also: STECKT in diesen Daten Struktur jenseits der
Geraden? Der Verfahrensvergleich beantwortet eine andere Frage: UEBERTRAEGT
sich diese Struktur auf unbekannte Stadtteile? Dieses Skript stellt genau
diese zweite Frage - mit demselben Modell, demselben Split und denselben 50
Laeufen wie die Baseline, nur mit erweiterter Merkmalsmatrix.

Es ist damit kein Modellvorschlag. Keine der drei Erweiterungen tritt im
Verfahrensvergleich an; sie dienen ausschliesslich der Interpretation des
Hauptbefundes (docs/07_BEFUNDE.md, B-41).

--------------------------------------------------------------------------
DIE VIER SPEZIFIKATIONEN
--------------------------------------------------------------------------
Grundlage ist immer das Stufe-2-Modell aus `v1_baselines.py`: Poisson-GLM mit
log-Link und `log(Bevoelkerung)` als Offset, unpenalisiert angepasst.

  linear          12 Terme   die 10 Praediktoren + monat_sin + monat_cos
  quadrate        22 Terme   zusaetzlich die Quadrate der 10 Praediktoren
  interaktionen   57 Terme   zusaetzlich alle 45 Paarprodukte der Praediktoren
  beides          67 Terme   Quadrate und Paarprodukte

Die Saisonterme werden WEDER quadriert NOCH gekreuzt. monat_sin^2 +
monat_cos^2 = 1 ist exakt kollinear mit der Konstanten; das Modell waere nicht
identifiziert. Die 45 Paarprodukte entsprechen genau den 45 Interaktionstermen,
die `v2_eignung.py` bewertet - deshalb ist der Vergleich derselbe.

STANDARDISIERUNG. Die Merkmale werden je Fold auf den TRAININGSDATEN
z-standardisiert, bevor Quadrate und Produkte gebildet werden. Das ist keine
Modellentscheidung, sondern numerische Notwendigkeit: Das Quadrat des
Medianeinkommens liegt in der Groessenordnung 1e10, das Produkt zweier
Praediktoren bei 1e9, und die IRLS-Iteration bricht auf dieser Konditionierung
zusammen. Mathematisch ist die Standardisierung folgenlos - der aufgespannte
Raum von {1, x, x^2} ist derselbe wie der von {1, z, z^2}, die Vorhersagen sind
identisch. Die Kennzahlen der Spalte `linear` muessen deshalb exakt die
Stufe-2-Baseline aus `results/regression/baselines_mittel.csv` reproduzieren;
das prueft `_selbsttest()` und bricht sonst ab.

KONVERGENZ. Mit 67 Termen auf 3.036 Trainingszeilen konvergiert die
IRLS-Iteration nicht in jedem Fold. Nicht konvergierte Anpassungen werden
GEZAEHLT und mitberichtet, nicht stillschweigend uebergangen und nicht
entfernt - eine nicht konvergierte Anpassung ist Teil des Befundes, dass diese
Spezifikation zu den Daten nicht passt.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Reproduziert `linear` die Stufe-2-Baseline auf drei Nachkommastellen?
    Wenn nein: Abbruch, dann ist die Merkmalsmatrix nicht dieselbe.
  - Wie viele der 200 Anpassungen sind nicht konvergiert, und in welchen
    Spezifikationen? Die Zahl gehoert in den Text.
  - Ist der Abstand `linear` zu `interaktionen` groesser als der Abstand
    `linear` zu Random Forest? Nur dann traegt die Aussage "die Spezifikation
    bewegt mehr als die Verfahrenswahl" (docs/07_BEFUNDE.md, B-41).
  - Das Hold-out bleibt unberuehrt: keine Zeile mit ist_holdout == 1.
```

### Kommentarbloecke (1)

**Zeile 169**

```text
Eine divergierte Anpassung kann beliebig grosse Vorhersagen liefern; die
 Kennzahlen werden dann unbrauchbar gross. Das ist so gewollt und wird
 nicht abgeschnitten - es IST das Ergebnis.
```

### Funktionen

#### `entwerfe(train, test, spezifikation)`

*Zeile 111 · 37 Zeilen*

```text
Merkmalsmatrizen fuer Training und Test, auf Trainingsdaten zentriert.

    Gibt (X_train, X_test, Namen) zurueck, jeweils MIT Konstante an Position 0.
    Mittelwert und Streuung stammen ausschliesslich aus dem Training - der
    Teststadtteil darf die Transformation nicht mitbestimmen.
```

#### `ein_lauf(train, test, spezifikation)`

*Zeile 150 · 25 Zeilen*

```text
Eine Poisson-Anpassung, eine Bewertung auf der Originalskala.
```

#### `alle_laeufe(panel, selten)`

*Zeile 177 · 13 Zeilen*

```text
10 Wiederholungen x 5 Folds x 4 Spezifikationen = 200 Anpassungen.
```

#### `zweistufig(df)`

*Zeile 192 · 15 Zeilen*

```text
Erst je Wiederholung ueber die Folds, dann ueber die Wiederholungen.

    Dieselbe Regel wie ueberall sonst (docs/06_RISIKEN.md, R-5): massgeblich
    ist die Streuung der 10 Wiederholungsmittel, nicht die der 50 Einzellaeufe.
```

#### `_selbsttest(mittel)`

*Zeile 209 · 21 Zeilen*

```text
Die lineare Spezifikation MUSS die Stufe-2-Baseline reproduzieren.

    Wenn nicht, sieht das Skript andere Merkmale oder andere Folds als
    `v1_baselines.py` - dann ist jeder Vergleich in dieser Datei wertlos.
```

#### `run()`

*Zeile 232 · 29 Zeilen*

_kein Docstring_


## `vorpruefung/v4_decke.py`

*313 Zeilen · 7 Funktionen*

### Modulkopf

```text
Wie gut KANN die Einsatzart bei dieser Zielgroesse ueberhaupt vorhergesagt werden?

    python vorpruefung/v4_decke.py            Entwicklungspanel, 29 Stadtteile
    python vorpruefung/v4_decke.py holdout    zusaetzlich die 6 gesperrten

Eingang: data/processed/klassifikation.parquet
         results/klassifikation/struktur_mittel.csv (optional, fuer die Quoten)
Ausgang: results/klassifikation/decke.csv
         results/klassifikation/decke_marge.csv
         results/klassifikation/decke_ausschoepfung.csv
         results/klassifikation/decke.md
         mit Argument "holdout" dieselben Dateien mit Endung _holdout

STAND: vollstaendig, 17.08.2026.

--------------------------------------------------------------------------
WOZU DIESES SKRIPT
--------------------------------------------------------------------------
Der Strukturstrang erreicht Macro-F1 um 0,33. Gegen die 1,0 einer fehlerfreien
Vorhersage gehalten sieht das nach einem misslungenen Modell aus. Diese Lesart
ist falsch, und dieses Skript belegt, warum: Sie vergleicht das Ergebnis mit
einer Obergrenze, die bei DIESER Zielgroesse und DIESEM Merkmalssatz gar nicht
erreichbar ist.

Zwei Obergrenzen begrenzen den Strukturstrang, und beide entstehen VOR jeder
Modellwahl - die eine in der Konstruktion der Zielgroesse, die andere in der
Struktur der Merkmale. Sie zu beziffern ist keine nachtraegliche Entlastung,
sondern die Voraussetzung dafuer, Macro-F1 0,33 ueberhaupt einordnen zu
koennen. Ohne sie bleibt jede Aussage ueber den Strukturstrang eine Vermutung.

  DECKE A - LABEL-RAUSCHEN AUS DEM ARGMAX
  `dominante_einsatzart` ist kein beobachtetes Merkmal, sondern der argmax
  ueber vier Anteilsspalten desselben Stadtteil-Monats. Wo zwei Anteile dicht
  beieinander liegen, entscheidet der Zufall der Monatsziehung, welche Klasse
  gewinnt - nicht die Struktur des Stadtteils.
  Gemessen wird das mit einem parametrischen Bootstrap: Jeder Stadtteil-Monat
  wird aus Multinomial(N, p_beobachtet) neu gezogen und geprueft, ob der argmax
  kippt. Der Macro-F1 zwischen dem beobachteten und dem neu gezogenen Label ist
  die Guete, die ein Modell erreichte, das die wahren Klassenwahrscheinlichkeiten
  EXAKT kennt. Kein Verfahren kann darueber hinaus.

  DECKE B - GRENZE DES STADTTEILWISSENS
  Alle zwoelf Praediktoren sind stadtteilgebunden: die baulichen konstant ueber
  den gesamten Zeitraum, die sozialen konstant je Stadtteil-Jahr, der
  Kriminalitaetsindex zu 90 Prozent zwischen den Stadtteilen. Ein Modell kann
  aus ihnen nur Stadtteilwissen ziehen. Die zugehoerige Obergrenze ist deshalb
  die Guete einer Vorhersage, die jedem Stadtteil-Monat die Modalklasse SEINES
  Stadtteils zuweist - mehr traegt Stadtteilwissen nicht.
  Diese Decke liegt DEUTLICH unter Decke A. Der Grund steht in der Tabelle
  `decke.csv`: Die Modalklassen der Stadtteile sind fast alle dieselbe.

  AUSSCHOEPFUNG
  Berichtet wird die baselinekorrigierte Quote

      (Modell - Mehrheitsklasse) / (Decke - Mehrheitsklasse)

  Der Rohquotient Modell/Decke waere geschoent: Ein Modell, das nur die
  Mehrheitsklasse nachbaut, erreicht bereits Macro-F1 0,22 - dieser Sockel
  gehoert nicht zur Leistung des Modells und darf nicht mitgezaehlt werden.

--------------------------------------------------------------------------
FALLSTRICKE
--------------------------------------------------------------------------
  1  DAS HOLD-OUT BLEIBT GESPERRT. Ohne das Argument "holdout" wird auf
     `ist_holdout == 0` gefiltert, wie in m02 und m03. Die Decke ist zwar eine
     Eigenschaft der ZIELGROESSE und beruehrt keine Praediktoren - aber die
     Sperre gilt konstruktiv fuer alle Skripte, nicht nach Ermessen.

  2  MONATE OHNE EINSAETZE gibt es in dieser Tabelle nicht (N >= 1 in allen
     Zeilen), das Skript prueft es trotzdem. Bei N = 0 waere p undefiniert und
     `rng.multinomial` wuerde stumm einen Nullvektor liefern, dessen argmax
     immer auf die erste Klasse zeigt - eine erfundene Beobachtung.

  3  DER BOOTSTRAP BRAUCHT EINEN FESTEN RANDOM_STATE. Ohne ihn schwankt Decke A
     zwischen zwei Laeufen, und die Zahl in der Arbeit passt nicht mehr zur
     Zahl in der CSV. RANDOM_STATE steht in config_modelle.py und ist derselbe wie im
     Verfahrensvergleich.

  4  DECKE A IST EINE OBERGRENZE, KEIN ZIELWERT. Sie beziffert, was bei
     perfekter Kenntnis der Klassenwahrscheinlichkeiten uebrig bliebe. Dass ein
     Modell sie nicht erreicht, ist kein Mangel - Decke B ist die bindende.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Liegt Decke B UNTER Decke A? Wenn nicht, ist etwas falsch: Stadtteilwissen
    kann das Label-Rauschen nicht unterbieten.
  - Liegen beide Decken UEBER dem Macro-F1 der Mehrheitsklasse? Eine Decke
    unterhalb der trivialen Baseline waere ein Rechenfehler.
  - Wie viele Stadtteile teilen dieselbe Modalklasse? Je hoeher diese Zahl,
    desto enger Decke B - das ist die inhaltliche Begruendung des Befundes.
  - Passt die Zeilenzahl? 4 Zeilen in decke.csv, 6 in decke_marge.csv.
```

### Kommentarbloecke (1)

**Zeile 290**

```text
Der Hold-out-Lauf schreibt in EIGENE Dateien. Sonst ueberschriebe die
 Schlussbewertung die Zahlen des Entwicklungspanels, auf die sich
 Kapitel 7.2 bezieht - derselbe Fehler, den m02 und m03 mit einer
 getrennten holdout.csv vermeiden.
```

### Funktionen

#### `_macro_f1(a, b)`

*Zeile 120 · 2 Zeilen*

_kein Docstring_

#### `decke_a(panel)`

*Zeile 124 · 25 Zeilen*

```text
Label-Rauschen des argmax, parametrischer Bootstrap.

    Jeder Stadtteil-Monat wird aus Multinomial(N, p_beobachtet) neu gezogen.
    Zurueck kommen der mittlere Macro-F1 zwischen beobachtetem und neu
    gezogenem Label, dessen Streuung ueber die Ziehungen und der Anteil der
    Zeilen, deren argmax dabei mindestens einmal kippt.

    FALLSTRICK 2: Zeilen mit N = 0 werden vorher ausgeschlossen, nicht auf eine
    Klasse gesetzt.
```

#### `decke_b(panel)`

*Zeile 151 · 12 Zeilen*

```text
Obergrenze des Stadtteilwissens: Modalklasse je Stadtteil.

    Mehr als die haeufigste Klasse seines Stadtteils kann ein Modell aus
    stadtteilgebundenen Merkmalen nicht ableiten. Zurueck kommen der Macro-F1
    dieser Zuweisung, der Anteil der so korrekt getroffenen Zeilen und die
    Verteilung der Modalklassen ueber die Stadtteile.
```

#### `marge(panel)`

*Zeile 165 · 17 Zeilen*

```text
Wie knapp faellt der argmax aus?

    Abstand zwischen dem groessten und dem zweitgroessten Klassenanteil je
    Stadtteil-Monat. Ein kleiner Abstand heisst: das Label haette bei einer
    anderen Monatsziehung anders gelautet.
```

#### `ausschoepfung(modelle, basis, a, b)`

*Zeile 184 · 18 Zeilen*

```text
Baselinekorrigierte Quote je Verfahren gegen beide Decken.

    (Modell - Mehrheitsklasse) / (Decke - Mehrheitsklasse). Der Rohquotient
    Modell/Decke waere geschoent, weil der Sockel der Mehrheitsklasse keine
    Leistung des Modells ist.
```

#### `bericht(tab, aus, mrg, kipp, treffer, modal, n_stadtteile)`

*Zeile 204 · 38 Zeilen*

_kein Docstring_

#### `main(argv)`

*Zeile 244 · 66 Zeilen*

_kein Docstring_


# Modellierung — `modelle/`


## `modelle/config_modelle.py`

*150 Zeilen · 0 Funktionen*

### Modulkopf

```text
Konfiguration der Modellierung. Gegenstueck zu prep/config.py.

DIE TRENNLINIE zwischen beiden Dateien ist nicht "Daten gegen Modelle", sondern:

    prep/config.py        was in die Parquet-Dateien GESCHRIEBEN wird
    modelle/config_modelle.py   was nur beim RECHNEN gilt

Deshalb stehen Praediktoren, Zielgroessen, Klassen und N_FOLDS weiterhin in
prep/config.py und nicht hier: Sie bestimmen, welche Spalten die Datensaetze
haben und wie die Spalten `fold` und `ist_holdout` belegt sind. Die
Modellskripte LESEN diese Festlegungen, sie treffen sie nicht. Zoege man sie
hierher, gaebe es zwei Dateien, die zwingend uebereinstimmen muessen - und
genau das ist die Fehlerquelle, die der Aufbau vermeiden soll.

Was hier steht, beruehrt keine einzige Datei auf der Platte.

Bezug: docs/04_MODELLIERUNG.md
```

### Kommentarbloecke (8)

**Zeile 21**

```text
==========================================================================
 1  HYPERPARAMETER-SUCHE
 ==========================================================================
 Nur die SUCHRAEUME stehen hier - die Suche selbst laeuft im jeweiligen
 Modellskript. Ein separates Tuning-Skript wuerde die besten Parameter in eine
 Datei auslagern, die veralten kann, ohne dass es auffaellt.

 Gleiches Budget fuer alle Verfahren -> fairer Vergleich (Bergstra & Bengio
 2012 zur Randomized Search, Probst et al. 2019 zu den RF-Raeumen).

 WARUM 100 UND NICHT 50 (Decision Log #50, 13.08.2026) - hergeleitet, nicht
 gewaehlt. Bergstra und Bengio (2012, S. 296) geben die geschlossene Form

     P = 1 - (1 - v/V)^T

 fuer die Wahrscheinlichkeit an, mit T Zufallsziehungen mindestens einmal in
 einen Zielbereich vom relativen Volumen v/V zu treffen. Fuer v/V = 0,05:

     T =  50  ->  92,3 %
     T = 100  ->  99,4 %

 Der Ausdruck enthaelt die DIMENSION des Suchraums nicht. Genau deshalb
 bekommen Ridge mit einem und XGBoost mit sieben Hyperparametern dasselbe
 Budget, ohne dass dem hoeherdimensionalen Verfahren ein Nachteil entsteht -
 das ist die Antwort auf den naheliegenden Einwand, XGBoost braeuchte mehr.

 Die beiden Prozentzahlen sind UNSERE Anwendung ihrer Formel, nicht ihre
 Aussage. Die verbreitete Angabe "60 Ziehungen" steht nicht in dem Papier
 (im Volltext geprueft, 13.08.2026); dessen eigene Simulation rechnet mit 1 %.

 Der eigentliche Anlass ist #49: Ein weiterer Suchraum verduennt die gute
 Region. Wer den Raum oeffnet, muss das Budget mitziehen.

 GEMESSEN (tools/suchdiagnose.py, 13.08.2026): Die Verdopplung allein ist bei
 VIER VON FUENF Verfahren wirkungslos - Random Forest im Mengenstrang gewinnt
 exakt null, Ridge +0,0065, beide Strukturverfahren +0,0001. Nur XGBoost in
 der Regression gewinnt spuerbar. Genau so gehoert es in Kapitel 6.
```

**Zeile 61**

```text
==========================================================================
 SUCHRAEUME - vier davon am 13.08.2026 erweitert (Decision Log #49)
 ==========================================================================
 Erweitert wurde dort, wo der beste gefundene Wert AN der Grenze lag: In sechs
 von sieben geprueften Parametern lag mindestens ein Fold-Sieger ausserhalb.
 Die Begruendung ist eine Aussage ueber die SUCHE, nicht ueber das Ergebnis -
 das Optimum lag an der Grenze, also war die Grenze falsch gesetzt.

 NICHT erweitert wurden max_features, min_samples_leaf, subsample und
 colsample_bytree: Ihre Grenzen sind natuerlich (alle Merkmale, eine
 Beobachtung je Blatt, der ganze Datensatz) - dahinter existiert nichts. Dass
 der Random Forest dort ans Limit geht, ist ein BEFUND ueber das Verfahren.
 Ebenfalls nicht erweitert: n_estimators, nur ein Fold lag nahe der Grenze und
 es ist der groesste Laufzeittreiber.
```

**Zeile 77**

```text
War 1e-3 bis 1e3; zwei von fuenf Folds lagen ausserhalb (1052 und
 1,1e-05). Ridge ist das einzige Verfahren, das die Baseline gesichert
 NICHT schlaegt - dort will man die Grenze nicht binden lassen.
```

**Zeile 84**

```text
War [None, 8, 12, 16, 24]. Zwei Aenderungen: nach oben erweitert
 (Sieger bei 32, 32 und 48), und `None` ans ENDE gestellt. `None`
 heisst unbegrenzte Tiefe, ist also faktisch der TIEFSTE Wert - an
 erster Stelle verdrehte es jede Auswertung, die die Listenposition
 als Tiefe liest (betraf auch Abbildung A8).
```

**Zeile 96**

```text
War 3 bis 10 - DER wichtigste Fund. Im Strukturstrang waehlten am
 07.08. vier von fuenf Folds den Wert 3, also die Untergrenze. Mit
 geoeffneter Grenze waehlen sie 2, 2 und 1. Das Modell wollte flacher
 sein, als es durfte. Zusammen mit reg_lambda ist das die plausibelste
 Erklaerung fuer R-2: Im Strukturstrang schlagen beide Baumverfahren
 die Baseline in der Kreuzvalidierung und verlieren auf dem Hold-out,
 waehrend die Baseline dort BESSER wird - Ueberanpassung, die der
 Suchraum zum Teil erzwungen hat.
```

**Zeile 110**

```text
Exponent der Tweedie-Varianzfunktion, Var = mu^p (Decision Log #42).
 1 waere Poisson, 2 waere Gamma; dazwischen liegt der ueberdisperse
 Bereich, in dem dieser Datensatz liegt (Dispersionsindex 62,8). Ihn
 fest auf 1,5 zu setzen waere eine Konvention ohne Grund - also wird
 er getunt wie jeder andere Hyperparameter.

 UNTERGRENZE 1,01 statt 1,1 (#45): Die Baseline ist ein Poisson-GLM,
 also der Grenzfall p = 1. Ein Suchraum, der diesen Grenzfall
 ausschliesst, verbietet XGBoost genau die Loesung, die dem
 Referenzmodell entspricht - das waere eine Ungleichbehandlung, wie sie
 #42 und #43 gerade beseitigt haben. `reg:tweedie` verlangt 1 < p < 2,
 deshalb 1,01 und nicht 1,0.
 Gilt nur in der REGRESSION; m03 entfernt ihn (Klassifikation).
```

**Zeile 125**

```text
----------------------------------------------------------------------
 DIE BASELINES STEHEN HIER NICHT - und zwar aus einem Grund (#45)
 ----------------------------------------------------------------------
 Beide Messlatten sind verallgemeinerte lineare Modelle mit dem fuer die
 Datenform kanonischen Link, per unpenalisierter Maximum-Likelihood
 angepasst: Poisson mit Offset fuer die Zaehldaten, multinomiales Logit
 fuer die nominalen Klassen. Sie haben KEINEN freien Hyperparameter -
 es gibt nichts zu suchen.

 Das ist keine Sparsamkeit gegenueber der Baseline, sondern die Definition
 von Stufe 2: die einfachste Form, die zur Datenform passt. Ein Strafterm
 waere eine Erweiterung dieser Form und braechte einen Regler mit sich,
 den man dann waehlen muesste.

 Regel, die daraus folgt und fuer ALLE Modelle gilt: Was einen freien
 Parameter hat, wird mit demselben Budget getunt. Was keinen hat, wird
 angepasst. Kein Modell laeuft mit einer unbegruendeten Voreinstellung.
```

**Zeile 144**

```text
==========================================================================
 2  WIEDERHOLTE SPLITS
 ==========================================================================
 Bei 29 Entwicklungsstadtteilen schwankt ein einzelner Fold massiv. Deshalb
 wird die Fold-Zuteilung mehrfach mit unterschiedlichem Versatz gebildet und
 ueber alle Laeufe gemittelt (docs/04_MODELLIERUNG.md, Abschnitt 2).
```


## `modelle/m02_menge.py`

*918 Zeilen · 17 Funktionen*

### Modulkopf

```text
Verfahrensvergleich fuer die MENGE der Einsatzlast.

Zwei Zielgroessen (`anzahl_einsaetze`, `einsaetze_je_1000_ew`) x drei Verfahren
(Ridge, Random Forest, XGBoost) x 10 Wiederholungen x 5 Folds = 300 Laeufe.

    python modelle/m02_menge.py            Tuning, Bewertung, Aggregation, Vergleich
    python modelle/m02_menge.py holdout    zusaetzlich die einmalige Schlussbewertung

Ausgang: results/regression/menge_folds.csv · menge_mittel.csv
                            tuning.csv · vergleich.csv · holdout.csv

Spezifikation: docs/04_MODELLIERUNG.md. Die dort genannten Fallstricke sind hier
im Code markiert - wer eine der vier Stellen aendert, sollte den Abschnitt lesen.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE nach jedem Lauf
--------------------------------------------------------------------------
Nachgetragen am 05.08.2026 - dieser Block fehlte, obwohl CLAUDE.md ihn fuer
jedes Skript in modelle/ verlangt (docs/07_BEFUNDE.md, B-9). Abzuarbeiten nach
JEDEM Lauf, nicht nur beim ersten.

  1  Schlaegt jedes Verfahren die Stufe-2-Baseline - je Zielgroesse einzeln?
     Wenn nein, lautet das Ergebnis "der Mehraufwand lohnt sich hier nicht".
     Das ist ein Befund, kein Fehler (Gutachten R6).
  2  Ueberlappen sich die Streuungsbereiche zweier Verfahren? Dann ist "nicht
     unterscheidbar" zu berichten, keine Rangfolge (R-6, R-1).
  3  Wie oft liefert ein Verfahren NEGATIVE Vorhersagen? Nicht kappen - die
     Haeufigkeit ist auszuweisen (Spalte n_negativ). Erwartet: keine, seit
     Tweedie und Poisson eine Log-Verknuepfung haben (#42, B-15).
  4  Passt die Zeilenzahl? 30 in tuning.csv (davon 15 gesucht, 15 zwischen den
     Zielgroessen geteilt, #43), 300 in menge_folds.csv.
  5  Wurde das Hold-out beruehrt? Ohne Argument darf keine Zeile mit
     ist_holdout == 1 gelesen worden sein - main() filtert sie deshalb
     unwiderruflich heraus, bevor irgendetwas rechnet.
  6  Ist std_wiederholungen deutlich kleiner als std_folds? Erwartet ja. Waere
     es null, waeren die Wiederholungen Dubletten (B-3).
  7  Steht der Extrapolationsanteil bei rund 34,6 % ueber alle Laeufe? Starke
     Abweichung heisst, dass die Aufteilung nicht die dokumentierte ist.
  8  Laufzeiten: ALLE Verfahren einkernig gemessen, der Parallelisierungs-
     gewinn getrennt (#39/#40). Kernzahl der Maschine protokollieren.
  9  Weicht eine Vorhersage zwischen einkernigem und parallelem Fit ab? Bei
     XGBoost erwartet (B-24), bei Ridge und RF nicht. Spalte
     parallel_abweichung_max.
 10  UEBERANPASSUNG (#51): Wie gross ist `ueberanpassung_RMSE` je Verfahren?
     Erwartet: bei Ridge klein, bei den Baumverfahren gross. Der Wert ist
     ZWISCHEN Konfigurationen zu vergleichen, nicht als Verhaeltnis zwischen
     Verfahren - Baeume interpolieren ihre Trainingsdaten konstruktionsbedingt
     (Begruendung an der Fundstelle in `ein_lauf`).
 11  Ist `ueberanpassung_RMSE` gegenueber der Sicherung vom 07.08. gesunken?
     Vergleich gegen `archiv/2026-08-14_budget50/`. NUR FUER `07_BEFUNDE.md`,
     nicht fuer die Kapitel: Nach #52 wird ausschliesslich der neue Lauf
     berichtet, ein Vorher-Nachher-Vergleich ausdruecklich nicht. Faellt die
     Ueberanpassung NICHT, ist das ein Befund, der die Erklaerung zu R-2
     schwaecht - und den man kennen sollte, bevor Kapitel 8 geschrieben wird.

STAND: vollstaendig, 06.08.2026.
```

### Kommentarbloecke (21)

**Zeile 85**

```text
Die Stufe-2-Baseline, gegen die die Primaeraussage laeuft (#34). Der Name
 muss zu vorpruefung/v1_baselines.POISSON passen - er wird zum Filtern der
 Spalte `modell` in baselines_folds.csv benutzt, ein Tippfehler liefert also
 stillschweigend eine leere Vergleichsmenge.
```

**Zeile 91**

```text
Der gepaarte Test laeuft auf RMSE. Begruendung: Bei der Rate ist R2 kein
 tragfaehiges Mass (docs/03_STAND.md, Abschnitt 4) - der Mittelwert wird
 negativ, obwohl die Baseline in jedem Fold besser ist als die Nullmarke. Zwei
 verschiedene Testmetriken fuer zwei Zielgroessen waeren schwerer zu
 verteidigen als eine. MAE und R2 wandern als Spalten mit und werden
 nachrichtlich berichtet.
```

**Zeile 100**

```text
==========================================================================
 PARALLELISIERUNG - eine Entscheidung mit zwei Gruenden
 ==========================================================================
 Die Modelle laufen EINKERNIG, parallelisiert wird nur die Hyperparametersuche.

 Grund 1, praktisch: `RandomizedSearchCV(n_jobs=-1)` um einen Schaetzer mit
 `n_jobs=-1` startet Prozesse ueber alle Kerne, von denen jeder seinerseits
 alle Kerne beansprucht. Die Prozesse blockieren sich gegenseitig; gemessen am
 05.08.2026 auf zwei Kernen stand ein Probelauf mit Budget 2 nach 15 Minuten
 noch in Phase 1 (docs/07_BEFUNDE.md, B-16).

 Grund 2, inhaltlich und wichtiger: Unterfrage 3 fragt nach dem TRAININGS- UND
 INFERENZAUFWAND. Ridge ist einkernig, weil eine geschlossene Loesung nichts zu
 parallelisieren hat; RF und XGBoost skalieren ueber Kerne. Misst man sie in
 unterschiedlichen Betriebsarten, vergleicht man Rechenaufwand und
 Parallelisierungsgrad in einer Zahl - und die haengt dann an der Kernzahl der
 Maschine statt am Verfahren.

 Deshalb: Der berichtete Aufwand wird EINKERNIG gemessen, fuer alle Verfahren
 gleich. Der Parallelisierungsgewinn ist eine eigene, ebenfalls interessante
 Groesse und wird in JEDEM Lauf getrennt miterhoben (siehe `ein_lauf`).
```

**Zeile 124**

```text
==========================================================================
 EXPOSITION - jedes Verfahren modelliert die RATE (Decision Log #43)
 ==========================================================================
 Ein Satz, gueltig fuer alle vier Modelle: geschaetzt wird `einsaetze_je_1000_ew`,
 und fuer `anzahl_einsaetze` wird mit der Einwohnerzahl zurueckmultipliziert.
 Genau diese Konstruktion verwendet das Poisson-GLM ueber seinen Offset seit
 jeher - `v1_baselines.regression()` leitet die Rate ebenfalls aus EINER
 Anpassung ab.

 WARUM GEAENDERT (06.08.2026, nach dem zweiten Modelllauf):
 Der Mechanismustest hat gemessen, was die alte Spezifikation kostete
 (docs/07_BEFUNDE.md, B-33): Bei `anzahl_einsaetze` lag Random Forest mit
 67,7 gegen 37,4 RMSE hinter der Baseline - modelliert er die Rate und rechnet
 zurueck, sind es 36,4. Der gesamte Rueckstand von 24 bis 30 RMSE stammte aus
 der Spezifikation, nicht aus dem Verfahren.

 Grund fuer die Korrektur, nicht das Ergebnis: Die Forschungsfrage lautet,
 welches der drei VERFAHREN die hoechste Prognoseguete erzielt. Verlieren zwei
 davon, weil ihnen die Expositionsstruktur vorenthalten wurde, misst der
 Vergleich die Modellierungsentscheidung statt der Verfahren. Bei Zaehldaten
 mit Expositionsgroesse ist deren explizite Behandlung Standard - XGBoost
 bietet dafuer sogar `base_margin`. Sie wegzulassen war ein Fehler derselben
 Art wie der quadratische Verlust (#42).

 Die Gegenprobe - Baumverfahren OHNE Expositionsbehandlung - ist kein zweiter
 Betriebsmodus dieses Skripts, sondern eine eigene Ablation in
 `m04_shap.ablation_exposition()`. Hier gibt es keinen Schalter: Der Lauf
 hat genau eine Spezifikation.
```

**Zeile 154**

```text
---------------------------------------------------------------------------
 BAUSTEIN 1  Die Pipeline
 ---------------------------------------------------------------------------
```

**Zeile 256**

```text
---------------------------------------------------------------------------
 BAUSTEIN 2  Das Tuning
 ---------------------------------------------------------------------------
```

**Zeile 296**

```text
---------------------------------------------------------------------------
 BAUSTEIN 3  Ein einzelner Lauf
 ---------------------------------------------------------------------------
```

**Zeile 332**

```text
EXPOSITION (#43): Geschaetzt wird immer die Rate; fuer die absolute Zahl
 wird mit der Einwohnerzahl zurueckmultipliziert. Dieselbe Konstruktion
 wie beim Poisson-GLM. Die Zeitmessung bleibt unberuehrt - die
 Ruecktransformation ist eine Multiplikation und steht ausserhalb.
```

**Zeile 360**

```text
Aendert die Kernzahl das ERGEBNIS? Gemessen statt behauptet - und
 gemessen statt abgebrochen: Ein Diagnosewert darf einen mehrstuendigen
 Lauf nicht beenden. Die berichteten Guetemasse stammen ohnehin aus dem
 einkernigen Fit; der parallele dient allein der Zeitmessung.

 BEFUND vom 06.08.2026: Bei XGBoost ist die Abweichung erheblich
 (gemessen bis 34,7 bei einem Mittelwert von rund 76), bei Ridge und
 Random Forest null. Ursache ist die parallele Reduktion der
 Histogramme - eine andere Summierungsreihenfolge kippt knapp
 benachbarte Split-Kandidaten, und ueber hunderte Baeume schaukelt sich
 das auf. Das ist eine Aussage ueber die Reproduzierbarkeit von
 XGBoost und gehoert in Kapitel 6 (docs/07_BEFUNDE.md, B-24).
```

**Zeile 374**

```text
UEBERANPASSUNGSNACHWEIS, ergaenzt 14.08.2026 (Decision Log #51).
 Dieselbe Guete auf den TRAININGSstadtteilen. Der Abstand zwischen beiden
 ist der Standardnachweis fuer Ueberanpassung - ohne ihn bleibt die
 Diagnose eine Auslegung der Hold-out-Abweichung.

 KEIN zweiter Fit: nur eine zusaetzliche Vorhersage auf Daten, die das
 Modell schon gesehen hat. Sie steht NACH der Zeitmessung, damit
 Unterfrage 3 unberuehrt bleibt.

 Verglichen wird auf der BERICHTETEN Skala: `train[ziel]`, nicht die
 Groesse, auf der angepasst wurde (das ist immer die Rate, #43).

 WIE DIE ZAHL ZU LESEN IST - und wie nicht. Ein Random Forest mit
 `min_samples_leaf = 1` passt die Trainingsdaten KONSTRUKTIONSBEDINGT
 nahezu perfekt an; jeder Baum interpoliert seine eigene Stichprobe. Ein
 Trainings-R2 von 0,98 ist dort also erwartbar und nicht schon der Beweis
 einer krankhaften Ueberanpassung. Der Abstand ist deshalb NICHT als
 "Verfahren A ueberanpasst 16-mal staerker als B" zu lesen.

 Aussagekraeftig ist er in zwei Richtungen:
   - zwischen KONFIGURATIONEN desselben Verfahrens (vor und nach der
     Erweiterung der Suchraeume, #49) - dort ist der Vergleich sauber
   - als Groessenordnung gegen die linearen Modelle, die per Konstruktion
     nicht interpolieren koennen
 Der saubere Wert fuer Baeume waere die Out-of-Bag-Schaetzung; sie steht
 nur beim Random Forest zur Verfuegung und waere gegenueber Ridge und
 XGBoost asymmetrisch. Bewusst nicht erhoben, hier benannt.
```

**Zeile 436**

```text
---------------------------------------------------------------------------
 ORCHESTRIERUNG
 ---------------------------------------------------------------------------
```

**Zeile 466**

```text
EXPOSITION (#43): Alle Modelle werden auf der RATE angepasst; fuer
 `anzahl_einsaetze` wird die Vorhersage nur zurueckmultipliziert. Es gibt
 also nur EIN Modell je Verfahren und Fold - und damit auch nur eine
 Suche. Beide Zielgroessen erhalten denselben Parametersatz, genau wie bei
 der Baseline, die ebenfalls einmal angepasst wird.

 Deshalb laeuft die Suche hier ueber (Verfahren x Fold) = 15 Durchgaenge,
 und die 30 Zeilen der tuning.csv entstehen erst danach durch Zuordnung zu
 beiden Zielgroessen. Frueher lief die Schleife ueber die Zielgroessen und
 die zweite "uebernahm" von der ersten - das Protokoll wies die Suche dann
 unter `anzahl_einsaetze` aus, obwohl auf der Rate gesucht wurde
 (docs/07_BEFUNDE.md, B-37).
```

**Zeile 589**

```text
Parallelisierungsgewinn: Faktor, um den der Fit ueber alle Kerne
 schneller ist. Bei Ridge zu erwarten: rund 1 - eine geschlossene Loesung
 hat nichts zu verteilen. Das ist selbst eine Aussage fuer UF4.
 Beide Zeiten stammen aus denselben 50 Laeufen.
```

**Zeile 595**

```text
Groesste Abweichung zwischen einkernigem und parallelem Modell. Null
 heisst threadunabhaengig; alles darueber ist ein Reproduzierbarkeits-
 befund und gehoert berichtet (B-24).
```

**Zeile 602**

```text
UEBERANPASSUNG: Trainingsguete und der Abstand zur Testguete. Ein grosser
 positiver Wert heisst, das Modell erklaert die Trainingsstadtteile viel
 besser als unbekannte - genau das ist Ueberanpassung. Beim Poisson-GLM
 und bei Ridge ist ein kleiner Abstand zu erwarten, bei den Baumverfahren
 ein grosser (docs/06_RISIKEN.md, R-2).
```

**Zeile 622**

```text
---------------------------------------------------------------------------
 FALLSTRICK 2  Mehrfachvergleiche (R-10)
 ---------------------------------------------------------------------------
```

**Zeile 704**

```text
GEPAART heisst: auf denselben Laeufen. Deshalb wird ueber die
 Schluessel VERBUNDEN und nicht auf gleiche Reihenfolge vertraut -
 sonst subtrahiert man stillschweigend verschiedene Testmengen
 voneinander. Fehlt ein Gegenstueck, bricht der Lauf ab.
```

**Zeile 739**

```text
Holm je Teststufe getrennt, nur innerhalb der sekundaeren Familie.
 ZWEI FAMILIEN, nicht sieben Tests: Regression und Klassifikation
 beantworten verschiedene Teilfragen (Entscheidung 05.08.2026, B-6).
 m03_struktur.py hat genau einen Test und wird nicht korrigiert.
```

**Zeile 793**

```text
---------------------------------------------------------------------------
 FALLSTRICK 4  Das Hold-out
 ---------------------------------------------------------------------------
```

**Zeile 821**

```text
DIE BASELINES GEHOEREN DAZU. Ohne sie hat die Schlussbewertung keinen
 Bezugspunkt: Die Primaeraussage nach #34 lautet "Verfahren gegen
 Stufe-2-Baseline", und genau die soll der Hold-out pruefen. Ein RMSE von
 23,7 ist ohne die Referenz daneben keine Aussage (docs/07_BEFUNDE.md,
 B-38). Beide Baselines haben keine Hyperparameter - es gibt nichts zu
 waehlen und damit auch nichts, was der Hold-out beeinflussen koennte.
```

**Zeile 874**

```text
FALLSTRICK 4, konstruktiv: Ohne das Argument "holdout" wird der Datensatz
 HIER auf die Entwicklungsstadtteile eingeschraenkt. Alles Folgende kann
 die Hold-out-Zeilen nicht mehr sehen, auch nicht versehentlich.
```

### Funktionen

#### `verfahren(name, n_jobs)`

*Zeile 157 · 65 Zeilen*

```text
Baut die ungetunte Pipeline fuer ein Verfahren.

    `n_jobs` steuert nur die Parallelisierung, nicht das Ergebnis - siehe den
    Block PARALLELISIERUNG oben. Die Voreinstellung ist EINKERNIG, damit die
    gemessenen Laufzeiten zwischen den Verfahren vergleichbar bleiben.

    FALLSTRICK: Ridge braucht zweierlei, und beides gehoert IN die Pipeline,
    nicht davor. Der StandardScaler, weil der L2-Strafterm alle Koeffizienten
    gleich behandelt und Merkmale in verschiedenen Einheiten sonst
    unterschiedlich hart bestraft wuerden. Und die log-Transformation der
    ZIELGROESSE ueber TransformedTargetRegressor - der rechnet nach der
    Vorhersage automatisch mit expm1 zurueck, sodass die Guetemasse auf der
    Originalskala entstehen. Wer log(1+y) von Hand rechnet, vergisst die
    Ruecktransformation irgendwann.

    Random Forest und XGBoost bekommen keine Zieltransformation: Sie sind gegen
    Skalen unempfindlich, und eine transformierte Zielgroesse wuerde die
    Guetemasse zwischen den Verfahren unvergleichbar machen.

    VERLUSTFUNKTION (Decision Log #42, 06.08.2026) - eine Korrektur, kein
    Feintuning. Bis dahin rechneten beide Baumverfahren mit dem QUADRATISCHEN
    FEHLER auf rohen Zaehldaten, waehrend die Baseline eine
    Zaehldaten-Likelihood mit log-Verknuepfung benutzte und Ridge auf log(1+y)
    schaetzte. Zwei Modelle rechneten multiplikativ, zwei additiv - bei
    Einsatzzahlen von 6 bis 280 und einem Dispersionsindex von 62,8. Der
    quadratische Fehler gewichtet dort einen Fehler von 20 bei Tenderloin
    genauso wie bei Seacliff, wo er das Dreifache des Gesamtwerts ausmacht.

    Das war eine Ungleichbehandlung in der Spezifikation, nicht ein Ergebnis
    ueber die Verfahren. Sie ist derselbe Gedanke, mit dem die Negative
    Binomial als Baseline begruendet wurde: die einfachste Form, die zur
    DATENFORM passt.

      XGBoost  `reg:tweedie`. Die Varianz waechst mit mu hoch p, der Exponent
               wird getunt (1,1 bis 1,9). Das ist das Gegenstueck zur Negative
               Binomial, deren Varianz mit mu + alpha*mu^2 waechst; Poisson
               (p = 1) unterstellt Varianz = mu und ist bei diesem
               Dispersionsindex viel zu eng.
      RF       `criterion="poisson"`. scikit-learn kennt kein Tweedie fuer
               Waelder - Poisson ist die naechstgelegene verfuegbare Wahl.
               Diese Einschraenkung ist selbst ein berichtbarer Befund ueber
               das Verfahren und gehoert in Kapitel 8, nicht stillschweigend
               weggelassen.
      Ridge    unveraendert. `log(1+y)` leistet dasselbe bereits.
```

#### `suchraum(name)`

*Zeile 224 · 30 Zeilen*

```text
Uebersetzt SUCHRAEUME aus der Config in scipy-Verteilungen.

    Die Config haelt die Raeume bewusst als einfache Tupel ("loguniform", a, b),
    damit sie ohne scipy lesbar bleibt. Hier werden daraus die Objekte, die
    RandomizedSearchCV erwartet.

    Der Praefix haengt am Pipeline-Aufbau: Bei Ridge liegt der Schaetzer zwei
    Ebenen tief (Pipeline -> TransformedTargetRegressor -> Ridge), bei den
    Baumverfahren direkt.
```

#### `tune(name, train, ziel)`

*Zeile 259 · 35 Zeilen*

```text
Sucht die besten Hyperparameter auf den Trainingsstadtteilen eines Folds.

    FALLSTRICK, der die ganze Arbeit entwerten kann: Der innere CV MUSS nach
    Stadtteil gruppieren. RandomizedSearchCV nimmt voreingestellt KFold und
    schneidet zufaellig nach Zeilen - ein Stadtteil hat aber 132 Zeilen, von
    denen dann etwa 100 im inneren Training und 32 in der inneren Validierung
    laegen. Da die Strukturmerkmale innerhalb eines Jahres konstant sind, waeren
    das faktisch dieselben Zeilen: Die Hyperparameter wuerden auf einen
    geleakten Schaetzwert optimiert, und der Vorteil des aeusseren
    Stadtteil-Splits waere verspielt. Man sieht es den Zahlen nicht an - sie
    waeren nur zu gut.

    Rueckgabe sind die PARAMETER, nicht das Modell. Wer `best_estimator_`
    weiterverwendet, hat auf dem inneren Trainingsanteil trainiert statt auf
    allen Trainingsstadtteilen des Folds - und ein Viertel der Daten verschenkt.

    ZWEITER FALLSTRICK, seit 06.08.2026 behoben: Der Schaetzer laeuft hier
    EINKERNIG, parallelisiert wird allein die Suche. Zuvor stand `n_jobs=-1` an
    beiden Stellen, und die Prozesse haben sich gegenseitig blockiert (B-16).
```

#### `ein_lauf(name, parameter, train, test, ziel, auch_parallel)`

*Zeile 299 · 122 Zeilen*

```text
Ein Fit, eine Vorhersage, mit Zeitmessung - eine Zeile fuer die CSV.

    FALLSTRICK: Die Zeit wird UM `fit` und `predict` herum gemessen, nicht um
    die ganze Funktion. Sonst steckt die Metrikberechnung mit in der Zahl, und
    Unterfrage 3 misst etwas anderes, als sie behauptet.

    Gemessen wird EINKERNIG - fuer alle drei Verfahren gleich. Das ist der
    Aufwand, der in Unterfrage 3 berichtet wird, und er ist zwischen den
    Verfahren vergleichbar, weil keines einen Parallelisierungsvorteil
    mitbringt.

    Mit `auch_parallel=True` wird zusaetzlich ein zweiter Fit ueber alle Kerne
    gemessen. Die Differenz ist der Parallelisierungsgewinn - eine eigene
    Aussage fuer Unterfrage 4: Ridge hat als geschlossene Loesung nichts zu
    parallelisieren, die Ensembles skalieren. Im Lauf steht das Argument in
    JEDEM Aufruf auf True; ein Mass, das nur auf einem Teil der Laeufe beruht,
    waere eine Ausnahme im Lauf.

    Ergaenzt am 05.08.2026 um `n_negativ` und `y_hat_min`: Ridge auf log(1+y)
    kann nach expm1 Werte unter null liefern. Die werden NICHT gekappt - das
    waere ein Eingriff -, aber ihre Haeufigkeit ist auszuweisen
    (docs/04_MODELLIERUNG.md, Sonderfaelle). Ohne diese zwei Felder muesste man
    dafuer jedes Modell ein zweites Mal fitten.
```

#### `extrapolationsanteil(train, test)`

*Zeile 423 · 11 Zeilen*

```text
Anteil der Testzeilen, die in mindestens einem Merkmal ausserhalb des
    Trainings-Wertebereichs liegen.

    Erklaert spaeter, warum ein Fold aus der Reihe faellt. Erfasst bewusst nur
    die Spanne je Merkmal, nicht unbekannte KOMBINATIONEN - das echte
    Extrapolationsproblem ist also eher groesser (docs/06_RISIKEN.md, R-3).
```

#### `phase_tuning(panel, selten)`

*Zeile 439 · 63 Zeilen*

```text
Je Zielgroesse, Verfahren und Fold einmal `tune()` - 30 Zeilen.

    KEINE WIEDERVERWENDUNG. `tuning.csv` ist ein Ergebnis dieses Laufs, kein
    Eingang. Fruehere Fassungen wurden hier eingelesen, um bei einem Abbruch
    die teuerste Phase nicht zu verlieren - das hat aber genau die Fehlerklasse
    erzeugt, die dieses Projekt sonst ueberall vermeidet: Nach einer Aenderung
    der Spezifikation waeren Parameter aus einer anderen Welt stillschweigend
    weiterverwendet worden. Ein Lauf, eine Spezifikation, keine Ausnahmen.

    Getunt wird ausschliesslich auf Wiederholung 0; die gefundenen Parameter
    gelten fuer alle zehn Wiederholungen (#34). Das ist eine bewusste
    Vereinfachung: Die Wiederholungen unterscheiden sich nur in der
    Fold-Zuteilung und dienen der Streuungsschaetzung, nicht der Modellwahl.
    Sie ist im Text zu benennen.

    Die Parameter landen sowohl als einzelne Spalten (lesbar fuer Kapitel 6.3)
    als auch als JSON (verlustfrei fuer den Wiedereinlesen-Weg).

    ZUR SPALTE `tuning_sekunden`: Sie steht bei beiden Zielgroessen auf
    demselben Wert, weil die Suche einmal stattgefunden hat. Eine Summe ueber
    alle 30 Zeilen zaehlt die Suchzeit deshalb doppelt - die tatsaechliche
    Dauer von Phase 1 ist die Summe ueber die 15 eindeutigen
    (Verfahren, Fold)-Paare.
```

#### `_rein_python(p)`

*Zeile 504 · 17 Zeilen*

```text
NumPy-Skalare in native Typen wandeln, BEVOR sie nach JSON gehen.

    Warum das noetig ist: `RandomizedSearchCV.best_params_` liefert je nach
    scipy- und numpy-Fassung `np.int64`/`np.float64` statt `int`/`float`.
    `np.float64` erbt von `float` und ueberlebt `json.dumps` zufaellig,
    `np.int64` erbt NICHT von `int`. Mit `default=str` als Notausgang wuerde
    daraus die Zeichenkette "287", und `set_params(n_estimators="287")` bricht
    ab - mitten im mehrstuendigen Lauf, nach dem Tuning.

    Ob es auftritt, haengt an der Paketversion; hier lief es durch, auf einer
    anderen Kombination nicht zwingend. Deshalb explizit wandeln statt hoffen -
    und ohne `default=`, damit ein unbekannter Typ laut auffaellt statt still
    zur Zeichenkette zu werden (docs/07_BEFUNDE.md, B-23).
```

#### `_parameter_je_fold(parameter)`

*Zeile 523 · 5 Zeilen*

```text
tuning.csv -> {(zielgroesse, verfahren, fold): dict}.
```

#### `phase_bewertung(panel, parameter, selten)`

*Zeile 530 · 33 Zeilen*

```text
10 Wiederholungen x 5 Folds x 3 Verfahren x 2 Zielgroessen = 300 Zeilen.

    Trainiert wird je Fold auf allen Trainingsstadtteilen - mit den Parametern
    aus Phase 1, aber einem FRISCHEN Modell. Der `best_estimator_` aus dem
    Tuning waere auf nur drei Vierteln der Trainingsstadtteile gefittet.
```

#### `aggregiere(folds)`

*Zeile 570 · 50 Zeilen*

```text
Zweistufig mitteln - erst je Wiederholung, dann darueber.

    FALLSTRICK 1 (R-5): Die 50 Fold-Ergebnisse sind NICHT unabhaengig - es sind
    dieselben 29 Stadtteile in zehn Gruppierungen. Ein Konfidenzintervall aus
    std_folds/sqrt(50) waere deshalb zu eng. Massgeblich ist
    `std_wiederholungen`: erst je Wiederholung ueber die 5 Folds mitteln, das
    ergibt 10 Werte, und deren Standardabweichung wird berichtet.

    Beide Spalten wandern mit, damit der Unterschied sichtbar bleibt.
```

#### `_holm(p)`

*Zeile 625 · 17 Zeilen*

```text
Holm-Bonferroni: p-Werte aufsteigend, kleinster gegen alpha/m, dann
    alpha/(m-1), bis zur ersten Nichtablehnung.

    Zurueckgegeben werden angepasste p-Werte, die direkt gegen alpha geprueft
    werden koennen - das ist dieselbe Entscheidung wie der schrittweise
    Vergleich, nur bequemer. Uniform staerker als Bonferroni bei gleicher
    Fehlerkontrolle; es gibt keinen Grund, darauf zu verzichten.
```

#### `_gepaart(a, b)`

*Zeile 644 · 22 Zeilen*

```text
Ein gepaarter Wilcoxon plus die Zahlen, die auch ohne p-Wert tragen.

    `a` ist das Verfahren, `b` der Gegner. Bei RMSE ist klein besser, die
    Differenz b - a ist also der VORTEIL von a.
```

#### `vergleiche(folds, baselines)`

*Zeile 668 · 84 Zeilen*

```text
Gepaarter Wilcoxon auf RMSE, zwei Rollen und zwei Teststufen.

    ROLLEN
      primaer     jedes Verfahren gegen die Stufe-2-Baseline (3 x 2 = 6 Tests).
                  KEINE Testfamilie - jede Frage ist nach #34 vorab einzeln
                  formuliert, deshalb keine Korrektur.
      sekundaer   jedes Verfahrenspaar (3 Paare x 2 Zielgroessen = 6 Tests).
                  Eine Familie, darauf Holm-Bonferroni.

    TESTSTUFEN (docs/07_BEFUNDE.md, B-5)
      wiederholung  n = 10, gemittelt je Wiederholung. DAS IST DER PRIMAERTEST.
                    Die 50 Einzellaeufe sind Pseudoreplikation - dieselben 29
                    Stadtteile, nur anders gruppiert. Ein Wilcoxon darueber
                    liefert zu kleine p-Werte.
      lauf          n = 50, alle Einzellaeufe. Ausdruecklich als Sensitivitaet
                    gefuehrt, nicht als Ergebnis.

    Auch die zehn Wiederholungsmittel sind nicht unabhaengig - es bleiben 29
    Einheiten. Das berichtete Konfidenzintervall ist daher enger als die wahre
    Unsicherheit (Nadeau & Bengio 2003). Deshalb stehen mittlere Differenz,
    Konfidenzintervall und gewonnene Laeufe IMMER daneben, unabhaengig vom p.
```

##### innere Funktion `paar()`

_kein Docstring_

#### `leakage_diagnose(folds, baselines)`

*Zeile 754 · 37 Zeilen*

```text
Beziffert, was das Tuning auf Wiederholung 0 kostet (B-21).

    Getunt wird einmal, auf Wiederholung 0. Dort stammen die Parameter aus dem
    Trainingssatz genau dieses Folds - der Vorsprung gegen die Baseline ist
    sauber gemessen. In den Wiederholungen 1 bis 9 werden dieselben Parameter
    auf andere Aufteilungen angewandt; im Mittel waren dort 78 % der
    Teststadtteile in der Menge, auf der die Parameter gesucht wurden.

    Waere der Effekt bedeutsam, muesste der Vorsprung in W1-9 SYSTEMATISCH
    groesser ausfallen als in W0. Diese Funktion misst genau das.

    Die Diagnose ist bewusst als schwach zu lesen: W0 ist auch eine andere
    Aufteilung als W1-9, der Unterschied ist also konfundiert. Ein deutlicher
    Effekt waere sichtbar, ein kleiner nicht von Fold-Schwankung zu trennen.
    Sie kostet dafuer keine zusaetzliche Rechenzeit - dieselbe Logik, mit der
    R-9 von einem Vorbehalt zu einer Zahl wurde.
```

#### `hold_out(panel, parameter, folds, selten)`

*Zeile 796 · 64 Zeilen*

```text
EINMALIG: auf allen 29 Entwicklungsstadtteilen trainieren, auf den 6
    Hold-out-Stadtteilen bewerten.

    WELCHE PARAMETER? Das Tuning liefert fuenf Saetze je Zielgroesse und
    Verfahren, einen je Fold. Die Spezifikation sagt "die in der
    Kreuzvalidierung gewaehlten", legt aber nicht fest, welcher davon
    (docs/07_BEFUNDE.md, B-14). Gewaehlt ist der Satz des Folds mit dem
    niedrigsten RMSE in Wiederholung 0 - deterministisch, nachvollziehbar und
    ausschliesslich aus Entwicklungsdaten. Der gewaehlte Fold steht in der
    Ausgabespalte `fold_der_parameter`.

    ZU BERICHTEN ist, dass dies EINE Messung an SECHS Einheiten ist - kein
    Mittelwert, keine Streuung. Die Zahl ist deutlich unsicherer als die
    Kreuzvalidierungswerte und darf nicht als deren Bestaetigung gelesen
    werden (R-4).
```

#### `main(argv)`

*Zeile 862 · 53 Zeilen*

_kein Docstring_


## `modelle/m03_struktur.py`

*679 Zeilen · 20 Funktionen*

### Modulkopf

```text
Verfahrensvergleich fuer die STRUKTUR der Einsatzlast.

Eine Zielgroesse (`dominante_einsatzart`, vier Klassen) x zwei Verfahren
(Random Forest, XGBoost) x 10 Wiederholungen x 5 Folds = 100 Laeufe.

    python modelle/m03_struktur.py            Tuning, Bewertung, Aggregation, Vergleich
    python modelle/m03_struktur.py holdout    zusaetzlich die einmalige Schlussbewertung

Ausgang: results/klassifikation/struktur_folds.csv · struktur_mittel.csv
                                tuning.csv · vergleich.csv · holdout.csv

AUFBAU: Spiegelt m02_menge.py. Dieselben sieben Funktionen, dieselbe Reihenfolge,
dieselben Fallstricke. Wer m02 gelesen hat, kennt die Struktur - hier stehen nur
die Unterschiede.

STAND: vollstaendig, 06.08.2026.

--------------------------------------------------------------------------
WAS ANDERS IST ALS IN m02
--------------------------------------------------------------------------
  Zielgroesse   `dominante_einsatzart`, vier ungeordnete Klassen
  Verfahren     nur RandomForestClassifier und XGBClassifier - Ridge hat auf
                einer nominalen Zielgroesse keine Entsprechung (Decision Log #31)
  Guetemasse    Macro-F1 (Hauptmass) und Macro-AUROC; Accuracy nur nachrichtlich
  Baseline      Stufe 2 ist die multinomiale logistische Regression (#33),
                nicht die Mehrheitsklasse
  Scoring       beim Tuning "f1_macro" statt RMSE
  Holm          entfaellt. Es gibt genau EINEN sekundaeren Test (RF gegen
                XGBoost); eine Familie aus einem Test braucht keine Korrektur.
                Entscheidung vom 05.08.2026: Regression und Klassifikation sind
                getrennte Testfamilien (docs/07_BEFUNDE.md, B-6).

--------------------------------------------------------------------------
DREI FALLSTRICKE, die es in m02 nicht gibt
--------------------------------------------------------------------------
  1  KLASSENGEWICHTE statt Resampling. `class_weight="balanced"` beim Random
     Forest, `sample_weight` beim XGBClassifier. KEIN SMOTE, kein Over- oder
     Undersampling - das waere ein Eingriff in die Datenverteilung und wuerde
     die Vergleichbarkeit mit den Baselines brechen.

  2  LABEL-ENCODER EINMAL GLOBAL fitten, nicht je Fold. XGBClassifier erwartet
     Integer-Labels 0..3. Wird der Encoder je Fold neu gefittet, verschiebt sich
     das Mapping in Folds, in denen eine Klasse nicht auftritt - und die
     Wahrscheinlichkeitsspalten zeigen dann auf die falschen Klassen.
     Nach der Vorhersage die Spalten auf die Reihenfolge von KLASSEN
     zurueckbringen.

  3  MACRO-AUROC KANN UNDEFINIERT SEIN, wenn eine Klasse im Testfold fehlt.
     Durch die doppelte Stratifizierung (#30) sollte das nicht vorkommen -
     falls doch, den Wert als FEHLEND fuehren und nicht durch null ersetzen,
     sonst zieht er den Mittelwert nach unten. `zero_division=0` bei Macro-F1
     muss gesetzt bleiben, sonst bricht der Lauf ab.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE nach jedem Lauf
--------------------------------------------------------------------------
  - Schlaegt ueberhaupt ein Verfahren Stufe 2? Wenn nein, ist das ein
    berichtbares Ergebnis und kein Fehler (docs/06_RISIKEN.md, R-2).
  - Hat jeder Fold Brand-Testfaelle? In Wiederholung 0 erwartet: 13 · 9 · 6 · 3 · 2.
  - Liegt Accuracy deutlich ueber Macro-F1? Das ist normal und selbst ein
    Argument fuer die Metrikwahl - siehe docs/03_STAND.md.
  - Wie viele Laeufe haben keine definierte Macro-AUROC? Erwartet: keiner.
  - Passt die Zeilenzahl? 10 in tuning.csv, 100 in struktur_folds.csv.
  - Wurde das Hold-out beruehrt? Ohne Argument darf keine Zeile mit
    ist_holdout == 1 gelesen worden sein.
  - UEBERANPASSUNG (#51): Wie gross ist `ueberanpassung_macro_f1`? Dieser
    Strang ist der, in dem Kreuzvalidierung und Hold-out sich widersprechen
    (R-2, B-42) - hier entscheidet sich, ob Ueberanpassung die Erklaerung ist.
  - Ist der Wert gegenueber `archiv/2026-08-14_budget50/` gesunken? XGBoost
    waehlte dort vier von fuenf Mal die kleinstmoegliche Baumtiefe. NUR FUER
    `07_BEFUNDE.md`: Nach #52 wird kein Vorher-Nachher-Vergleich berichtet.
    Die Antwort entscheidet aber, wie belastbar die Ueberanpassungserklaerung
    zu R-2 in Kapitel 8 formuliert werden darf.
```

### Kommentarbloecke (11)

**Zeile 101**

```text
Die vier Klassen in FESTER Reihenfolge - abgeleitet aus ANTEILE, also aus
 derselben Quelle, aus der die Zielgroesse per argmax entsteht. Diese Liste
 ist der globale Label-Encoder (Fallstrick 2): Index = Integer-Label.
```

**Zeile 107**

```text
Muss zu vorpruefung/v1_baselines.LOGREG passen - der Name filtert die Spalte
 `modell` in baselines_klasse.csv, ein Tippfehler liefert also stillschweigend
 eine leere Vergleichsmenge. `hold_out()` importiert die Konstante direkt.
```

**Zeile 114**

```text
Wie in m02: Modelle einkernig, nur die Suche parallel. Begruendung dort im
 Block PARALLELISIERUNG (docs/07_BEFUNDE.md, B-16). Der berichtete Aufwand
 muss zwischen den Verfahren vergleichbar sein, und der Parallelisierungs-
 gewinn ist eine eigene Groesse.
```

**Zeile 122**

```text
---------------------------------------------------------------------------
 BAUSTEIN 1  Die Pipeline
 ---------------------------------------------------------------------------
```

**Zeile 205**

```text
---------------------------------------------------------------------------
 BAUSTEIN 2  Das Tuning
 ---------------------------------------------------------------------------
```

**Zeile 240**

```text
---------------------------------------------------------------------------
 BAUSTEIN 3  Ein einzelner Lauf
 ---------------------------------------------------------------------------
```

**Zeile 277**

```text
Anteil der Zeilen, die einkernig und parallel verschieden
 klassifiziert werden. KEIN Abbruch - gemessen und berichtet: Bei
 XGBoost ist die Vorhersage threadabhaengig (docs/07_BEFUNDE.md,
 B-24). Die berichteten Guetemasse stammen aus dem einkernigen Fit.
```

**Zeile 283**

```text
UEBERANPASSUNGSNACHWEIS, ergaenzt 14.08.2026 - wie in m02, siehe dort.
 Eine zusaetzliche Vorhersage auf den Trainingsstadtteilen, kein zweiter
 Fit, nach der Zeitmessung. Hier ist der Wert besonders wichtig: Der
 Strukturstrang ist der, in dem Kreuzvalidierung und Hold-out sich
 widersprechen (R-2, B-42), und die Baseline auf dem Hold-out BESSER wird,
 waehrend beide Baumverfahren einbrechen.
```

**Zeile 370**

```text
---------------------------------------------------------------------------
 ORCHESTRIERUNG
 ---------------------------------------------------------------------------
```

**Zeile 570**

```text
Wie in m02 gehoeren beide Baselines dazu - ohne Bezugspunkt ist ein
 Macro-F1 von 0,33 keine Aussage (docs/07_BEFUNDE.md, B-38).

 EINE SPEZIFIKATION, ZWEI AUFRUFER (10.08.2026). Bis dahin baute diese
 Funktion das Logit selbst nach - dieselben vier Argumente, an zwei Orten
 aufgeschrieben. Aendert jemand eines davon, misst die Kreuzvalidierung
 still gegen ein anderes Modell als die Schlussbewertung, und keine
 Pruefung schlaegt an. m02 war immer richtig gebaut und holt `poisson_glm`
 aus derselben Datei; hier fehlte genau das.
```

**Zeile 598**

```text
FALLSTRICK 2 auch hier: Die Wahrscheinlichkeitsspalten der
 logistischen Regression stehen in alphabetischer Reihenfolge
 ihrer Klassennamen, nicht in der von KLASSEN. Erst umsortieren,
 dann bewerten - `roc_auc_score` verlangt aufsteigend sortierte
 Labels und liefert sonst gar nichts (B-38).
```

### Funktionen

#### `verfahren(name, n_jobs)`

*Zeile 125 · 23 Zeilen*

```text
Baut die ungetunte Pipeline. Kein Scaler - beide Verfahren sind Baeume.

    `n_jobs` steuert nur die Parallelisierung, nicht das Ergebnis. Voreinstellung
    einkernig, damit die Laufzeiten vergleichbar bleiben.

    FALLSTRICK 1: Die Klassenverteilung ist stark schief (79 % Fehlalarm). Statt
    zu resampeln bekommen beide Verfahren GEWICHTE. Beim Random Forest geht das
    als Hyperparameter (`class_weight="balanced"`), beim XGBClassifier ueber
    `sample_weight` beim Fit - das Verfahren kennt keinen entsprechenden
    Parameter. Beides bewirkt dasselbe: seltene Klassen zaehlen mehr, ohne dass
    eine einzige Zeile dupliziert oder geloescht wird.
```

#### `suchraum(name)`

*Zeile 150 · 33 Zeilen*

```text
Uebersetzt SUCHRAEUME in scipy-Verteilungen - wie in m02, ohne Praefix.

    Beide Verfahren sind hier nackte Schaetzer statt Pipelines, weil keine
    Skalierung noetig ist. Die Suchraeume sind dieselben wie in der Regression;
    das ist Absicht: Es wechselt nur die Verlustfunktion, nicht der
    Ensemble-Mechanismus (docs/04_MODELLIERUNG.md, Abschnitt 3).

    EINE AUSNAHME: `tweedie_variance_power` steuert die Verlustfunktion der
    REGRESSION (Decision Log #42) und ist bei `multi:softprob` bedeutungslos.
    XGBoost wuerde ihn stillschweigend annehmen und ignorieren - er wuerde dann
    ein Sechstel des Tuning-Budgets auf eine wirkungslose Dimension verschwenden
    und in tuning.csv eine Zahl ausweisen, die nichts bedeutet.
```

#### `kodiere(y)`

*Zeile 185 · 12 Zeilen*

```text
Klassennamen -> Integer 0..3 nach der GLOBALEN Reihenfolge KLASSEN.

    FALLSTRICK 2: Das Mapping haengt bewusst NICHT von den Daten ab, die gerade
    vorliegen. Ein je Fold gefitteter LabelEncoder wuerde in einem Fold ohne
    Brand die Zahlen verschieben, und die Wahrscheinlichkeitsspalten zeigten
    danach auf die falschen Klassen - ohne Fehlermeldung.
```

#### `_gewichte(y_int)`

*Zeile 199 · 4 Zeilen*

```text
`class_weight='balanced'` von Hand - fuer XGBoost, das keinen hat.
```

#### `tune(name, train)`

*Zeile 208 · 30 Zeilen*

```text
Wie m02.tune, aber mit `f1_macro` als Scoring.

    FALLSTRICK aus m02 gilt unveraendert: Der innere CV MUSS nach Stadtteil
    gruppieren, sonst stehen dieselben 132 Zeilen eines Stadtteils in innerem
    Training und innerer Validierung.

    Warum f1_macro und nicht Accuracy: Die Mehrheitsklasse allein erreicht ueber
    0,8 Accuracy. Ein darauf optimiertes Tuning wuerde Modelle waehlen, die die
    drei seltenen Klassen ignorieren - genau das, was die Fragestellung nicht
    will (docs/03_STAND.md, Abschnitt 4).
```

#### `ein_lauf(name, parameter, train, test, auch_parallel)`

*Zeile 243 · 65 Zeilen*

```text
Ein Fit, eine Vorhersage, mit Zeitmessung - eine Zeile fuer die CSV.

    Wie in m02 wird die Zeit UM `fit` und `predict` herum gemessen, und zwar
    EINKERNIG fuer beide Verfahren. Die Wahrscheinlichkeiten fuer die AUROC
    kommen aus einem zweiten Aufruf, damit `inferenz_sekunden` die reine
    Klassenvorhersage misst und zwischen den Verfahren vergleichbar bleibt.

    `auch_parallel=True` misst denselben Fit zusaetzlich ueber alle Kerne.
    Im Lauf steht es in jedem Aufruf auf True - keine Ausnahmen.
```

##### innere Funktion `fitte()`

_kein Docstring_

#### `extrapolationsanteil(train, test)`

*Zeile 310 · 11 Zeilen*

```text
Anteil der Testzeilen ausserhalb des Trainings-Wertebereichs.

    Wortgleich zu m02_menge. Bewusst dupliziert statt importiert: Ein
    gemeinsames Hilfsmodul fuer zwei Aufrufer braechte mehr Indirektion als
    Ersparnis, und m03 soll unabhaengig von m02 lauffaehig bleiben
    (docs/04_MODELLIERUNG.md, Abschnitt 4).
```

#### `_gepaart(a, b)`

*Zeile 323 · 23 Zeilen*

```text
Ein gepaarter Wilcoxon plus die Zahlen, die auch ohne p-Wert tragen.

    Wortgleich zu m02_menge; siehe dort. `a` ist das Verfahren, `b` der Gegner,
    die Differenz b - a ist der Vorteil von a. Bei Macro-F1 ist gross besser,
    die Aufrufstelle dreht die Argumente entsprechend.
```

#### `_macro_auroc(y_true, proba, klassen_modell)`

*Zeile 348 · 20 Zeilen*

```text
Macro-AUROC (One-vs-Rest), oder NaN wenn eine Klasse im Test fehlt.

    FALLSTRICK 3: NICHT durch 0,5 ersetzen. Ein erfundener Wert saehe wie eine
    Messung aus und zoege den Mittelwert nach unten. Fehlend heisst fehlend.

    `labels=klassen_modell` bringt die Wahrscheinlichkeitsspalten in die
    Reihenfolge, die das Modell tatsaechlich benutzt hat - der zweite Teil von
    Fallstrick 2.
```

#### `phase_tuning(panel, selten)`

*Zeile 373 · 21 Zeilen*

```text
Je Verfahren und Fold einmal `tune()` auf Wiederholung 0 - 10 Zeilen.

    Wie in m02 wird nichts wiederverwendet: `tuning.csv` ist ein Ergebnis
    dieses Laufs, kein Eingang.
```

#### `_rein_python(p)`

*Zeile 396 · 9 Zeilen*

```text
NumPy-Skalare in native Typen wandeln - wortgleich zu m02_menge.

    `np.int64` erbt nicht von `int` und ueberlebt `json.dumps` nicht. Ohne
    diese Wandlung wuerde aus 287 die Zeichenkette "287", und `set_params`
    braeche nach dem Tuning ab (docs/07_BEFUNDE.md, B-23).
```

#### `_parameter_je_fold(parameter)`

*Zeile 407 · 3 Zeilen*

_kein Docstring_

#### `phase_bewertung(panel, parameter, selten)`

*Zeile 412 · 26 Zeilen*

```text
10 Wiederholungen x 5 Folds x 2 Verfahren = 100 Zeilen.
```

#### `aggregiere(folds)`

*Zeile 445 · 30 Zeilen*

```text
Zweistufig - wie in m02. Massgeblich ist `std_wiederholungen` (R-5).
```

#### `vergleiche(folds, baselines)`

*Zeile 477 · 50 Zeilen*

```text
Gepaarter Wilcoxon auf Macro-F1 - zwei primaere Tests, ein sekundaerer.

    KEIN HOLM. Die sekundaere Familie besteht aus einem einzigen Test (Random
    Forest gegen XGBoost); eine Korrektur ueber einen Test ist die Identitaet.
    Die Spalte `p_holm` bleibt deshalb leer, `n_tests_familie` steht auf 1.
    Entscheidung vom 05.08.2026: Regression und Klassifikation sind getrennte
    Testfamilien (docs/07_BEFUNDE.md, B-6). Das ist in Kapitel 7 zu benennen -
    dieser Vergleich laeuft ungekorrigiert gegen alpha = 0,05.

    Teststufen wie in m02: `wiederholung` (n = 10) ist der Primaertest, `lauf`
    (n = 50) die ausdruecklich gekennzeichnete Sensitivitaet (B-5).
```

##### innere Funktion `paar()`

_kein Docstring_

#### `leakage_diagnose(folds, baselines)`

*Zeile 529 · 24 Zeilen*

```text
Beziffert, was das Tuning auf Wiederholung 0 kostet - wie in m02.

    Bei Macro-F1 ist GROSS besser, der Vorsprung ist also Verfahren minus
    Baseline. Ausfuehrliche Begruendung in `m02_menge.leakage_diagnose`.
```

#### `hold_out(panel, parameter, folds)`

*Zeile 555 · 68 Zeilen*

```text
EINMALIG - wie in m02, mit Macro-F1 statt RMSE als Auswahlkriterium.

    Gewaehlt wird der Parametersatz des Folds mit dem HOECHSTEN Macro-F1 in
    Wiederholung 0. Zu berichten ist, dass dies EINE Messung an SECHS Einheiten
    ist (R-4).
```

#### `main(argv)`

*Zeile 625 · 51 Zeilen*

_kein Docstring_


## `modelle/m04_shap.py`

*793 Zeilen · 10 Funktionen*

### Modulkopf

```text
Interpretation: Welche Merkmale tragen die Vorhersage?

    python modelle/m04_shap.py

Eingang: results/regression/{menge_folds,vergleich}.csv
         results/klassifikation/{struktur_folds,vergleich}.csv
         results/*/tuning.csv · data/processed/{regression,klassifikation}.parquet
Ausgang: results/shap/beitraege.csv · gruppen.csv · uebersprungen.csv
         faktorgruppen_menge.csv · vif.csv
         extrapolation_{merkmale,stadtteile,zusammenhang}.csv
         ablation_exposition.csv
         ablation_faktorgruppen.csv · ablation_faktorgruppen_mittel.csv

    python modelle/m04_shap.py --ohne-baeume    Ablation nur fuer GLM und Logit

STAND: vollstaendig, 05.08.2026. Faktorgruppen-Ablation ergaenzt 13.08.2026.
Setzt m02 und m03 voraus.

--------------------------------------------------------------------------
ZWEI ANTWORTEN AUF UNTERFRAGE 1 - und warum es beide braucht
--------------------------------------------------------------------------
  ATTRIBUTION   `gruppen.csv`, `faktorgruppen_menge.csv`. Welcher Anteil der
                SHAP- bzw. Koeffizientenmasse entfaellt auf eine Faktorgruppe?
                Sagt, wie ein Modell seine Aufmerksamkeit verteilt.

  ABLATION      `ablation_faktorgruppen_mittel.csv`. Was kostet es, die Gruppe
                wegzulassen? Sagt, was sie WERT ist.

Die zweite Frage ist die haertere: Ein Merkmal kann viel Masse binden und
trotzdem ersetzbar sein, weil ein anderes dieselbe Information traegt. Erst die
Ablation trennt das.

--------------------------------------------------------------------------
DIE EINE REGEL
--------------------------------------------------------------------------
SHAP wird NUR fuer Modelle gerechnet, die ihre Stufe-2-Baseline schlagen. Fuer
alle anderen erklaert man Rauschen - und eine Abbildung, die Beitraege zeigt, wo
kein Signal ist, ist schlimmer als keine Abbildung. Das Skript prueft das selbst
und ueberspringt Modelle, die die Latte reissen; die uebersprungenen stehen mit
Begruendung in `uebersprungen.csv`, damit die Auswahl nachvollziehbar ist und
nicht wie Rosinenpicken aussieht.

Massgeblich ist der PRIMAERTEST auf den Wiederholungsmitteln (teststufe
"wiederholung"): mittlere Differenz zugunsten des Verfahrens UND signifikant.

--------------------------------------------------------------------------
WAS GERECHNET WIRD
--------------------------------------------------------------------------
  TreeExplainer   fuer Random Forest und XGBoost
  Koeffizienten   fuer Ridge - dort braucht es kein SHAP. Der StandardScaler
                  steht in der Pipeline, also sind die Koeffizienten bereits
                  standardisiert und untereinander vergleichbar.
  Fold            EIN Fold, nicht alle - der mit dem GERINGSTEN
                  Extrapolationsanteil in Wiederholung 0. Begruendung: Dort
                  liegen die wenigsten Testzeilen ausserhalb des gelernten
                  Wertebereichs, die Beitraege beruhen also am ehesten auf
                  Interpolation. Die Wahl steht in der Ausgabe und ist im Text
                  zu nennen.

--------------------------------------------------------------------------
FALLSTRICK: BLOCKWEISE INTERPRETIEREN
--------------------------------------------------------------------------
Die Strukturmerkmale sind untereinander korreliert. SHAP verteilt den Beitrag
dann auf mehrere Merkmale, und einzelne Werte sind nicht sinnvoll deutbar -
"median_haushaltseinkommen traegt 8 %" waere eine Scheinpraezision.

Deshalb zusammenfassen zu den drei Faktorgruppen des Exposes; `log_bevoelkerung`
(Groessenkontrolle) und die Saison werden getrennt ausgewiesen, weil sie in
keine der drei Gruppen gehoeren. Das beantwortet Unterfrage 1 direkt: Welche
Faktorgruppe traegt wie viel?

--------------------------------------------------------------------------
HIERHER VERSCHOBEN: DER VIF
--------------------------------------------------------------------------
Die Multikollinearitaetspruefung lag frueher in der Eignungspruefung, entschied
dort aber nichts - Ridge ist durch den L2-Strafterm robust dagegen, Baumverfahren
interessiert sie nicht. Ihre einzige echte Konsequenz betrifft genau diese
Interpretation. Deshalb steht sie hier.

Gerechnet auf den EINDEUTIGEN Stadtteil-Merkmalskombinationen, nicht auf allen
Zeilen: Die Strukturmerkmale sind innerhalb eines Jahres konstant, ueber alle
Zeilen zaehlte jede Kombination bis zu zwoelfmal und der VIF waere kuenstlich
stabilisiert.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Stimmt die Rangfolge der Faktorgruppen zwischen Random Forest und XGBoost
    ueberein? Wenn nicht, ist das ein Befund fuer Kapitel 8, kein Fehler.
  - Passt sie zu den Korrelationen aus der Eignungspruefung? Dort lagen
    log_kriminalitaetsindex und anteil_risikogewerbe_pct vorn.
  - Wird eine Faktorgruppe als praktisch bedeutungslos ausgewiesen? Das waere
    eine der wenigen wirklich inhaltlichen Aussagen der Arbeit.
  - Liegt der maximale VIF noch bei rund 11,5? Ein deutlich anderer Wert hiesse,
    dass sich die Merkmalsbasis geaendert hat.
  - ABLATION: Reproduziert die Variante `voll` im Mengenstrang exakt die
    Stufe-2-Baseline aus `results/regression/baselines_folds.csv`? Wenn nicht,
    sieht die Ablation andere Merkmale oder andere Folds als v1, und jeder
    Vergleich darin ist wertlos. Am 13.08.2026 geprueft: Differenz 0,0.
  - ABLATION: Welche Gruppen haben ein NEGATIVES Vorzeichen, verbessern die
    Prognose also durch ihr Weglassen? Das ist ein Befund fuer Kapitel 7 und
    KEINE Aufforderung, den Merkmalssatz zu kuerzen - er ist durch das Expose
    und die Fairness-Regel gebunden. Nachtraeglich zu kuerzen waere eine
    ergebnisgetriebene Spezifikationswahl.
```

### Kommentarbloecke (3)

**Zeile 320**

```text
OHNE Exposition: direkt auf der absoluten Zahl anpassen.
 Der Hauptlauf tut das Gegenteil (Rate schaetzen, mit der
 Bevoelkerung zurueckrechnen); die Differenz ist der Effekt.
```

**Zeile 656**

```text
EXPOSITION (#43): Fuer `anzahl_einsaetze` wurde das bewertete
 Modell auf der RATE angepasst. Wird hier direkt auf der Anzahl
 gefittet, erklaert SHAP ein anderes Modell als das, dessen
 Guetemasse berichtet werden - und niemand saehe es den Zahlen
 an. Die Beitraege beziehen sich also auf das Ratenmodell; das
 ist im Text zu benennen.
```

**Zeile 760**

```text
--- Ablation der Faktorgruppen (UF1, zweite Antwort) ---
 Attribution sagt, wie ein Modell seine Aufmerksamkeit verteilt.
 Diese Ablation sagt, was die Gruppe wert ist. Siehe Docstring dort.
```

### Funktionen

#### `schlagen_die_latte(vergleich)`

*Zeile 142 · 21 Zeilen*

```text
Welche (Zielgroesse, Verfahren) schlagen ihre Stufe-2-Baseline?

    Grundlage ist der Primaertest auf den Wiederholungsmitteln. Verlangt werden
    BEIDE Bedingungen: die mittlere Differenz muss zugunsten des Verfahrens
    ausfallen UND der Test muss signifikant sein. Ein positiver Mittelwert
    allein waere zu wenig - genau davor warnt R-6.
```

#### `ruhigster_fold(folds)`

*Zeile 165 · 5 Zeilen*

```text
Der Fold mit dem geringsten Extrapolationsanteil in Wiederholung 0.
```

#### `_beitraege(modell, X, name)`

*Zeile 172 · 36 Zeilen*

```text
Mittlerer absoluter Beitrag je Merkmal - SHAP oder Koeffizient.

    Bei Ridge stehen standardisierte Koeffizienten; sie sind der direkte
    Gegenwert zu SHAP-Beitraegen und brauchen keinen Explainer. Bei den
    Baumverfahren rechnet der TreeExplainer exakt statt zu approximieren.

    Mehrklassige Ausgaben werden ueber die Klassen gemittelt - die Frage lautet
    "welche Faktorgruppe traegt", nicht "fuer welche Klasse".

    WARUM XGBOOST EINEN EIGENEN WEG GEHT: `shap.TreeExplainer` kann den
    mehrklassigen `base_score` von XGBoost 3.x nicht lesen und bricht mit
    `could not convert string to float` ab (geprueft mit shap 0.52.0 und
    xgboost 3.2.0, docs/07_BEFUNDE.md, B-17). XGBoost bringt TreeSHAP aber
    selbst mit - `pred_contribs=True` liefert exakt dieselben Werte, gerechnet
    vom selben Algorithmus. Kein Naeherungsverfahren, nur ein anderer Aufrufweg.
```

#### `extrapolation_aufschluesseln(panel, selten, folds)`

*Zeile 210 · 57 Zeilen*

```text
Woher kommen die 33,7 % Extrapolation - und was folgt daraus?

    WARUM DAS HIER STEHT: `03_STAND.md` behauptet, die Spanne des
    Extrapolationsanteils von 3,6 % bis 57,4 % erklaere „einen erheblichen Teil
    der Fold-Streuung". Das war eine Plausibilitaetsaussage ohne Messung. Diese
    Funktion macht eine Zahl daraus. Sie erklaert damit den zentralen Befund
    des Mengenstrangs (R-3, `07_BEFUNDE.md` B-26) und gehoert deshalb zur
    Interpretation, nicht zum Verfahrensvergleich.

    ABGRENZUNG ZU #34 - wichtig, das ist keine Haarspalterei:
    Verboten ist, die TESTMENGE nach Extrapolationsgrad aufzuteilen und dort
    nach Verfahrensunterschieden zu suchen; das waere ein nachtraeglicher
    Zuschnitt der Auswertung. Hier wird nichts aufgeteilt und nichts neu
    verglichen. Die Einheit ist der FOLD, und die Frage lautet, warum Folds
    unterschiedlich schwer sind. Die Primaeraussage bleibt unberuehrt.

    Drei Auswertungen:
      1  je Merkmal    wie oft liegt es allein ausserhalb des Trainingsbereichs
      2  je Stadtteil  wie stark bricht er aus, wenn er im Test steht
      3  je Verfahren  Zusammenhang zwischen Extrapolationsanteil eines Laufs
                       und dem dort gemessenen Fehler (Spearman, ueber alle
                       50 Laeufe)
```

#### `ablation_exposition(panel, selten, parameter)`

*Zeile 269 · 66 Zeilen*

```text
ABLATION: Was leistet die Expositionsbehandlung?

    Der Hauptlauf modelliert die Rate und multipliziert mit der Einwohnerzahl
    zurueck (#43) - fuer alle vier Modelle gleich. Diese Ablation entfernt
    genau diesen einen Baustein bei den Baumverfahren und laesst sie direkt auf
    `anzahl_einsaetze` anpassen. Alles andere bleibt identisch: dieselben
    Folds, dieselben Merkmale, dieselben Hyperparameter.

    Es wird also EIN Bestandteil der Spezifikation isoliert. Das ist der Zweck
    einer Ablation und der Grund, warum die Hyperparameter bewusst NICHT neu
    gesucht werden - sonst aenderte man zwei Dinge gleichzeitig.

    WAS SIE BEANTWORTET. Unterfrage 4 fragt nach Implikationen fuer die
    Modellauswahl. Die Ablation zeigt, ob die Wahl des Verfahrens oder die
    Spezifikation den groesseren Hebel hat - und liefert damit eine
    uebertragbare Aussage statt eines knappen Rankings.

    Frueher gemessen (`07_BEFUNDE.md`, B-33): Ohne Expositionsbehandlung lagen
    Random Forest bei 67,7 und XGBoost bei 61,7 RMSE, mit ihr bei 36,4 und
    35,7. Der Unterschied zwischen den Spezifikationen eines Verfahrens ist
    damit ein Vielfaches des Unterschieds zwischen den Verfahren.

    DIE FRAGE. Bei `anzahl_einsaetze` liegen die Baumverfahren rund 20 RMSE
    hinter Ridge, bei `einsaetze_je_1000_ew` leicht davor. Der einzige
    Unterschied zwischen beiden Zielgroessen ist die Einwohnerzahl. Die
    Vermutung lautet: Baeume koennen „Einsaetze = Bevoelkerung x Risiko" nicht
    nachbauen, weil sie je Blatt einen festen Wert ausgeben und Extremwerte zur
    Blattmitte ziehen — und weil RMSE auf der Originalskala von den grossen
    Stadtteilen dominiert wird (Tenderloin 280, Seacliff 6,4).

    Spiegelbild zu R-9: Dort wurde der Offset der Baseline WEGGENOMMEN, Ergebnis
    null. Hier fehlt er den Baeumen. Kein Widerspruch — fuer ein Modell mit
    Log-Verknuepfung und freiem Koeffizienten auf `log_bevoelkerung` ist der
    Offset redundant, fuer einen Baum ohne beides nicht.
```

#### `ablation_faktorgruppen(reg, kl, selten, tuning_kl, mit_baeumen)`

*Zeile 337 · 117 Zeilen*

```text
UNTERFRAGE 1, zweite Antwort: Was ist eine Faktorgruppe WERT?

    WARUM ES DIESE FUNKTION BRAUCHT. `beitraege.csv` und `gruppen.csv`
    beantworten UF1 ueber ATTRIBUTION - welcher Anteil der Koeffizienten- bzw.
    SHAP-Masse auf eine Gruppe entfaellt. Das sagt, wie ein Modell seine
    Aufmerksamkeit verteilt. Es sagt NICHT, was die Gruppe wert ist: Ein
    Merkmal kann viel Masse binden und trotzdem ersetzbar sein, weil ein
    anderes dieselbe Information traegt.

    Die Ablation misst das Fehlende direkt. Jede Gruppe wird einmal
    weggelassen, alles andere bleibt gleich - dieselben Folds, dieselben
    Zeilen, dieselbe Spezifikation. Die Verschlechterung ist der Beitrag.

    Dasselbe Muster wie `ablation_exposition()`, nur auf die Merkmalsgruppen
    statt auf die Expositionsbehandlung angewandt.

    WELCHES MODELL JE STRANG - nach derselben Regel wie der Rest von m04:
    abladiert wird das Modell, dessen Beitraege berichtet werden.

      Menge      das Poisson-GLM. Kein Vergleichsverfahren schlaegt es
                 (B-26), es IST das beste Modell des Strangs. Und es hat
                 keinen Hyperparameter - die Ablation ist dadurch sauber:
                 Was sich aendert, ist ausschliesslich die Merkmalsmenge.

      Struktur   Random Forest und XGBoost. Beide schlagen die Stufe-2-
                 Baseline in der Kreuzvalidierung (B-29), fuer beide werden
                 SHAP-Beitraege berichtet. Das Logit laeuft zum Vergleich mit.

    EINE EINSCHRAENKUNG, die zu berichten ist: Bei den Baumverfahren stammen
    die Hyperparameter aus dem VOLLEN Merkmalssatz und werden nicht neu
    gesucht - genau wie in `ablation_exposition()`, damit sich nur EIN Ding
    aendert. Die gemessene Verschlechterung enthaelt dadurch einen Anteil, der
    auf eine nicht mehr passende Einstellung entfaellt und nicht auf die
    fehlende Information. Beim Poisson-GLM besteht dieses Problem nicht.

    KEIN SIGNIFIKANZTEST. Die Testfamilien sind mit #38 festgelegt - zwei,
    eine je Strang. Weitere Tests hier wuerden die Korrekturstruktur beruehren
    und muessten in Holm eingehen. Die Ablation ist deskriptiv gemeint:
    berichtet werden Mittelwert, Streuung ueber die zehn Wiederholungsmittel
    (R-5) und die Zahl der Wiederholungen, in denen die Gruppe fehlte.

    Der Offset des Poisson-GLM bleibt in JEDER Variante bestehen, auch wenn
    die Groessenkontrolle weggelassen wird: `log(Bevoelkerung)` geht als
    Offset ein, nicht als Merkmalsspalte. Weggelassen wird nur der Praediktor
    `log_bevoelkerung`.
```

#### `_ablation_auswerten(roh)`

*Zeile 456 · 35 Zeilen*

```text
Verschlechterung je Gruppe gegenueber dem vollen Merkmalssatz.

    Gepaart je Lauf: Die Variante und der volle Satz laufen auf demselben Fold
    derselben Wiederholung. Gemittelt wird zweistufig - erst je Wiederholung
    ueber die Folds, dann darueber (R-5), weil die 50 Laeufe nicht unabhaengig
    sind.

    Das VORZEICHEN ist so gedreht, dass ein positiver Wert immer
    "Verschlechterung durch Weglassen" heisst - bei RMSE ist klein besser, bei
    Macro-F1 gross. Ohne diese Drehung liest man eine der beiden Tabellen
    genau falsch herum.
```

#### `faktorgruppen_baseline(panel, selten, fold)`

*Zeile 493 · 50 Zeilen*

```text
Beitrag der drei Faktorgruppen im MENGENSTRANG - aus der Baseline.

    WARUM AUS DER BASELINE. Unterfrage 1 fragt nach dem Erklaerungsbeitrag der
    drei Faktorgruppen. Fuer die Struktur liefert ihn SHAP. Fuer die Menge
    nicht: `m04` ueberspringt dort alle Modelle, weil keines seine Baseline
    schlaegt — und Beitraege eines unterlegenen Modells auszuweisen hiesse,
    Rauschen zu erklaeren.

    Die Loesung liegt im Ergebnis selbst: Das **beste Modell des Mengenstrangs
    ist das Poisson-GLM**. Seine Koeffizienten beantworten UF1 direkt und
    ehrlich. Dass die Antwort aus der Baseline statt aus einem
    Vergleichsverfahren kommt, ist kein Notbehelf, sondern die Konsequenz des
    Befunds.

    VERGLEICHBAR GEMACHT ueber standardisierte Beitraege |Koeffizient| x
    Standardabweichung des Merkmals. Ohne diesen Schritt haengt die Groesse
    eines Koeffizienten an der Einheit des Merkmals — Einkommen in Dollar
    bekaeme automatisch einen winzigen Koeffizienten.

    Gerechnet auf demselben Fold wie die SHAP-Werte, damit beide Straenge
    dieselbe Datengrundlage haben.
```

#### `_vif(panel)`

*Zeile 545 · 38 Zeilen*

```text
VIF auf zwei Bezugsmengen - und der Grund, warum es zwei sein muessen.

    Die Absicht der Spezifikation war, jede Merkmalskombination nur EINMAL zu
    zaehlen: Die Strukturmerkmale sind innerhalb eines Jahres konstant, ueber
    alle Zeilen zaehlte jede Kombination bis zu zwoelfmal, und der VIF waere
    kuenstlich stabilisiert.

    Ein `drop_duplicates()` auf allen Praediktoren leistet das aber NICHT: Seit
    Decision Log #17 ist `log_kriminalitaetsindex` ein MONATLICH rollierender
    Index. Damit ist fast jede Zeile eindeutig - gemessen 3.757 von 3.828 - und
    die Entdopplung laeuft ins Leere (docs/07_BEFUNDE.md, B-18).

    Deshalb zwei ausgewiesene Bezugsmengen:

      stadtteil_jahr   eine Zeile je Stadtteil und Jahr. Das ist die Ebene, auf
                       der die ACS- und Land-Use-Merkmale tatsaechlich variieren,
                       und die Zahl, die in den Text gehoert.
      alle_zeilen      zum Vergleich, damit der Unterschied sichtbar ist.
```

#### `main()`

*Zeile 585 · 205 Zeilen*

```text
Sechs Auswertungen zu Unterfrage 1, in dieser Reihenfolge.

    1  ATTRIBUTION. Fuer jede Kombination aus Zielgroesse und Verfahren, die
       `schlagen_die_latte()` durchlaesst, werden auf dem ruhigsten Fold
       SHAP-Beitraege berechnet und zu Faktorgruppen verdichtet
       -> beitraege.csv, gruppen.csv, uebersprungen.csv.
       Schlaegt kein Modell seine Baseline, bleibt der Block leer - das ist
       ein Ergebnis, kein Fehler (R-2). Genau das ist im Mengenstrang der Fall.
    2  EXTRAPOLATION aufgeschluesselt nach Merkmal und Stadtteil, plus der
       Zusammenhang zum RMSE -> extrapolation_*.csv.
    3  ABLATION DER EXPOSITION: dieselben Baumverfahren ohne die Rueck-
       transformation ueber die Einwohnerzahl -> ablation_exposition.csv.
    4  FAKTORGRUPPEN DES MENGENSTRANGS aus der Baseline, weil dort Schritt 1
       leer bleibt -> faktorgruppen_menge.csv.
    5  ABLATION DER FAKTORGRUPPEN: was kostet das Weglassen einer Gruppe
       -> ablation_faktorgruppen.csv, ablation_faktorgruppen_mittel.csv.
    6  VIF als Kollinearitaetsmass -> vif.csv.

    Das Hold-out wird vor Schritt 1 herausgefiltert und nie wieder angefasst;
    dieses Skript kennt kein "holdout"-Argument. Die Schritte 3 bis 5 sind
    Zusatzbelege und beruehren den Verfahrensvergleich nicht - sie erklaeren
    ihn nur. Mit `--ohne-baeume` laeuft Schritt 5 nur auf den GLM-Baselines,
    was die Laufzeit von rund zehn auf unter eine Minute drueckt.
```


## `modelle/m05_abbildungen.py`

*1129 Zeilen · 21 Funktionen*

### Modulkopf

```text
Alle Abbildungen fuer Kapitel 7 - aus den CSV-Dateien, nicht von Hand.

    python modelle/m05_abbildungen.py

Eingang: results/regression/*.csv · results/klassifikation/*.csv
         results/spezifikation/*.csv
         results/shap/{ablation_exposition,gruppen,faktorgruppen_menge,
                       extrapolation_zusammenhang}.csv
Ausgang: results/abbildungen/*.pdf

STAND: neu gefasst 07.08.2026, um A6-A9 erweitert 10.08.2026. Setzt m02, m03,
m04, v1 und v3 voraus.

Dieses Skript RECHNET NICHTS. Es liest nur. Dadurch laesst sich eine Darstellung
aendern, ohne die Modelle neu zu rechnen, und nach einem neuen Lauf ist ein
Befehl genug.

--------------------------------------------------------------------------
WARUM DER SATZ AM 07.08.2026 NEU GESCHNITTEN WURDE
--------------------------------------------------------------------------
Der erste Satz bestand aus Boxplots der Rohwerte je Verfahren und einem
Balkendiagramm gegen die Baseline. Beides zeigte den Vergleich nicht, und zwar
aus einem messbaren Grund.

Die 50 Laeufe unterscheiden sich darin, WELCHE Stadtteile im Testfold liegen.
Bayview hat ein Vielfaches der Einsaetze von Seacliff, also schwankt der RMSE
zwischen 13 und 76 - unabhaengig vom Verfahren. Die Streuung der Rohwerte
betraegt 12,4 bis 15,5 RMSE, der Verfahrensunterschied rund 2. Ein Boxplot der
Rohwerte zeigt daher fast ausschliesslich Fold-Streuung.

Jedes Verfahren sieht aber DIESELBEN Folds. Bildet man die Differenz je Lauf,
kuerzt sich die Fold-Streuung heraus: Die Streuung der gepaarten Differenz
ueber die 10 Wiederholungsmittel betraegt 2,4 bis 4,3. Gepaarte Daten ungepaart
darzustellen verschenkt genau die Information, fuer die das Design gebaut wurde
- und es ist dieselbe Paarung, auf der der Wilcoxon-Test beruht (#34).

Das alte A2 hatte zusaetzlich einen einfachen Darstellungsfehler: Balken ab
null, waehrend sich alles zwischen 33,98 und 36,51 abspielt. Die Unterschiede
lagen in den obersten sechs Prozent der Bildhoehe.

--------------------------------------------------------------------------
ZEHN ABBILDUNGEN
--------------------------------------------------------------------------
A1 bis A5 tragen den Verfahrensvergleich, A6 bis A9 die Interpretation, A10
die Annahmenpruefung. Alle zehn lesen ausschliesslich CSV - die Regel "dieses
Skript rechnet nichts" gilt unveraendert auch fuer die fuenf neuen.

  a1_gegen_baseline.pdf   Gepaarte Differenz zur Stufe-2-Baseline, ein Punkt je
                          Wiederholung, beide Straenge nebeneinander. Das ist
                          die Primaeraussage nach Decision Log #34.

  a2_foldstruktur.pdf     Die Rohwerte je Fold, Verfahren als Linien. Zeigt,
                          dass die Streuung aus dem Fold stammt und nicht aus
                          dem Verfahren - die Begruendung fuer A1.

  a3_spezifikation.pdf    Was bewegt mehr: die Wahl des Verfahrens oder die
                          Wahl der Spezifikation? Antwort auf Unterfrage 4,
                          Grundlage von B-41.

  a4_laufzeit_guete.pdf   Trainingszeit gegen Prognoseguete, ein Punkt je
                          Verfahren. Unterfrage 3.

  a5_holdout.pdf          Die einmalige Auswertung auf den sechs
                          zurueckgehaltenen Stadtteilen, beide Straenge.

  a6_faktorgruppen.pdf    Welche der drei Faktorgruppen des Exposes traegt wie
                          viel? Das ist UNTERFRAGE 1 - die einzige der vier, zu
                          der es bisher keine Abbildung gab, sondern nur
                          Konsolenausgabe von m04.

  a7_extrapolation.pdf    Extrapolationsanteil eines Laufs gegen den dort
                          gemessenen Fehler, 50 Punkte je Verfahren, mit
                          Spearman-rho. Macht R-3 sichtbar und liefert die
                          Begruendung, die A2 nur behauptet.

  a8_hyperparameter.pdf   Wie stabil ist die Modellwahl? Die fuenf Fold-
                          Parametersaetze je Verfahren, jeder auf seine Lage im
                          eigenen Suchraum normiert. Grundlage fuer Kapitel 8.

  a9_parallelisierung.pdf Parallelisierungsgewinn je Verfahren. Zweite Haelfte
                          von Unterfrage 3, die A4 nicht zeigt: A4 traegt die
                          EINKERNIGE Zeit auf, hier steht, was Kerne bringen -
                          und wo sie nichts bringen.

  a10_qq_residuen.pdf     QQ-Diagramm der Residuen der linearen Spezifikation.
                          Auflage vom 10.08.2026, gehoert zu Abschnitt 6 der
                          Eignungspruefung. Zeigt, WO die Verteilung von der
                          Normalverteilung abweicht - der Test sagt nur, DASS.

--------------------------------------------------------------------------
ANFORDERUNGEN AN DIE DARSTELLUNG
--------------------------------------------------------------------------
Sie landen im gedruckten Dokument, und Gestaltung war im Gutachten ein eigenes
Bewertungskriterium.

  Format        PDF, nicht PNG. Rasterbilder werden im Druck unscharf.
  Groesse       In der ENDGROESSE erzeugen, nicht gross erzeugen und in LaTeX
                schrumpfen - sonst steht dort 5-pt-Schrift. Mindestens 9 pt.
  Titel         KEINE Titel in der Abbildung. Die Bildunterschrift in LaTeX ist
                der Titel; beides doppelt sich sonst.
  Graustufen    Verfahren zusaetzlich ueber Schraffur und Marker unterscheiden,
                nicht allein ueber Farbe.
  Achsen        Beschriftung mit Einheit, deutsches Dezimalkomma.
  Nulllinie     Wo eine Differenz oder ein R2 dargestellt wird, ist sie
                einzuzeichnen - das Vorzeichen ist die Aussage.
  Richtung      Bei jeder Differenzachse muss dastehen, welche Seite besser
                ist. Bei RMSE ist das links, bei Macro-F1 rechts - wer das
                verwechselt, liest das Ergebnis genau falsch herum.
  Streuung      IMMER benennen, worueber sie gebildet ist: ueber die 10
                Wiederholungsmittel, nicht ueber die 50 Einzellaeufe (R-5).

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Sind alle fuenf PDF entstanden und in LaTeX einbindbar?
  - Schneidet in A1 die Nulllinie eine der Boxen? Dann darf im Text kein
    Unterschied zur Baseline behauptet werden, den der Test nicht deckt (R-6).
  - Traegt jede Differenzachse die Richtungsangabe, und zeigt sie bei Macro-F1
    in die andere Richtung als bei RMSE?
  - Stimmt der Referenzwert in A3 mit `linear` aus v3 und mit der
    Stufe-2-Baseline aus v1 ueberein? Alle drei muessen dieselbe Zahl sein.
  - In Graustufen ausdrucken: sind die Verfahren noch unterscheidbar?
  - A6: Summiert sich jeder Balken auf 100 %? Steht in der Fusszeile, dass der
    Mengenbalken KOEFFIZIENTEN und die Strukturbalken SHAP-Werte zeigen? Die
    beiden Groessen sind nicht dasselbe und duerfen nicht als eine gelesen
    werden.
  - A7: Liegen die drei Verfahren bei gleichem x uebereinander? Muessen sie -
    der Extrapolationsanteil ist eine Eigenschaft des Folds, nicht des
    Verfahrens. Andernfalls stimmt die Fold-Zuordnung nicht.
  - A8: Klebt ein Parameter am Rand seines Suchraums (Lage nahe 0 oder 1)? Dann
    war der Raum zu eng gewaehlt, und das gehoert in die Limitationen.
  - A9: Steht die Linie bei 1,0 und ist beschriftet? Werte UNTER 1 heissen
    "parallel langsamer" - ohne die Linie liest man sie als Gewinn.
```

### Kommentarbloecke (9)

**Zeile 175**

```text
--------------------------------------------------------------------------
 Nur fuer A6 bis A9
 --------------------------------------------------------------------------
 Reihenfolge der Faktorgruppen: die drei des Exposes zuerst, danach die zwei
 getrennt gefuehrten Groessen. Identisch zu GRUPPEN in m04_shap.py - stuenden
 sie in anderer Reihenfolge, waeren Abbildung und Tabelle nicht vergleichbar.
```

**Zeile 184**

```text
Helle Fuellung plus eigene Schraffur je Gruppe. In gestapelten Balken traegt
 die Schraffur die Unterscheidung, nicht der Grauwert - fuenf Grautoene sind
 im Druck nicht mehr sicher auseinanderzuhalten.
```

**Zeile 539**

```text
Die Stufe-2-Baseline als Linie. Ohne sie ist der sehr enge
 Wertebereich nicht einzuordnen - 0,9 RMSE Unterschied saehen aus wie
 ein Abgrund, obwohl alle drei Verfahren dicht an der Latte liegen.
```

**Zeile 610**

```text
Die Baselines heissen in der CSV so, wie sie in Kapitel 5 heissen -
 ausgeschrieben. Als Achsenbeschriftung sind sie zu lang und
 ueberdecken die Nachbarfelder; hier stehen die Kurzformen.
```

**Zeile 708**

```text
Zahl nur, wo das Segment sie traegt. Ein Label, das in den
 Nachbarn ragt, ist schlimmer als kein Label. Der weisse Kasten
 dahinter ist noetig, weil die Schraffur sonst durch die Ziffern
 laeuft - im Druck sind sie dann nicht mehr sicher lesbar.
```

**Zeile 851**

```text
In der Regression steht jeder Suchlauf ZWEIMAL in der Datei, einmal
 je Zielgroesse - gesucht wurde aber nur einmal, auf der Rate (#43).
 Ohne diese Entdopplung stuenden hier zehn statt fuenf Punkte je
 Parameter, und die Streuung saehe nur halb so gross aus.
```

**Zeile 890**

```text
Menge vor Struktur, innerhalb dessen die Verfahrensreihenfolge der Arbeit
 (Ridge, Random Forest, XGBoost) - nicht die alphabetische, in der Ridge
 zwischen den Ensembles stuende.
```

**Zeile 898**

```text
Y-Positionen mit LUECKE zwischen den Straengen. Ohne die Luecke bliebe
 kein Platz fuer die Blockueberschriften, und ohne die Ueberschriften ist
 nicht ablesbar, welcher Block welcher ist - die Verfahrensnamen
 wiederholen sich in beiden.
```

**Zeile 978**

```text
Im Mengenstrang stammen beide Zielgroessen aus DERSELBEN Anpassung
 (#43) - fuer `anzahl_einsaetze` wird die Ratenvorhersage nur
 zurueckmultipliziert. Die Zeiten sind deshalb bis auf Messrauschen
 identisch; zwei Balken taeuschten zwei Messungen vor.
```

### Funktionen

#### `_sekunden(wert)`

*Zeile 216 · 8 Zeilen*

```text
Sekunden lesbar beschriften.

    Zwei Nachkommastellen reichen fuer die Ensembles (5,83 s), nicht fuer Ridge
    (0,011 s) - dort stuende sonst zweimal "0,01 s" und die Abbildung
    behauptete, der parallele Fit sei gleich schnell gewesen.
```

#### `_matplotlib()`

*Zeile 226 · 13 Zeilen*

_kein Docstring_

#### `_komma(FuncFormatter, stellen, vorzeichen)`

*Zeile 241 · 13 Zeilen*

```text
Deutsches Dezimalkomma auf den Achsen.

    `stellen` ist nicht kosmetisch: Macro-F1 liegt zwischen 0,328 und 0,334 -
    mit zwei Nachkommastellen stuenden an allen Achsenmarken dieselben "0,33",
    und die Abbildung waere sinnlos.

    `vorzeichen` setzt auf Differenzachsen ein explizites Plus. Ohne das liest
    sich "2,5" wie ein Absolutwert statt wie ein Abstand.
```

#### `_prozent(FuncFormatter, stellen)`

*Zeile 256 · 4 Zeilen*

```text
Prozentachse mit deutschem Dezimalkomma.
```

#### `_text(pfad)`

*Zeile 262 · 2 Zeilen*

_kein Docstring_

#### `_gepaarte_differenz()`

*Zeile 267 · 32 Zeilen*

```text
Je Verfahren die 10 Wiederholungsmittel der Differenz zur Baseline.

    Gepaart wird auf (wiederholung, fold) - also auf identischen Testzeilen.
    Genau diese Paarung liegt auch dem Wilcoxon-Test in m02/m03 zugrunde; die
    Abbildung zeigt damit dieselbe Groesse, die getestet wird, und nicht eine
    andere, die zufaellig aehnlich aussieht.
```

#### `a1_gegen_baseline(plt, FuncFormatter)`

*Zeile 301 · 54 Zeilen*

```text
Die Primaeraussage: jedes Verfahren gegen seine Stufe-2-Baseline.

    Dargestellt sind die 10 Wiederholungsmittel als Punkte und ihre Verteilung
    als Kasten. Ein Fehlerbalken waere hier die schlechtere Wahl: Bei zehn
    Werten zeigt der Punktschwarm die Verteilung selbst, statt sie durch eine
    Kennzahl zu ersetzen, die Symmetrie unterstellt.

    Die Nulllinie ist die Baseline. Wo der Kasten sie schneidet, ist der
    Unterschied nicht gesichert - unabhaengig davon, was der Mittelwert sagt.
```

#### `a2_foldstruktur(plt, FuncFormatter)`

*Zeile 358 · 43 Zeilen*

```text
Warum in A1 gepaart wird: die Folds bewegen alle Verfahren gemeinsam.

    Gezeigt wird eine einzelne Wiederholung, sonst waeren es 50 Linien. Die
    Aussage haengt nicht an der Auswahl - die uebrigen neun sehen genauso aus,
    was sich an der Streuungszerlegung in der Fusszeile ablesen laesst.
```

#### `_spezifikationszeilen()`

*Zeile 404 · 36 Zeilen*

```text
(Beschriftung, RMSE, Gruppe) fuer A3 - alles aus CSV, nichts von Hand.
```

#### `a3_spezifikation(plt, FuncFormatter)`

*Zeile 442 · 55 Zeilen*

```text
Unterfrage 4: Was bewegt mehr - das Verfahren oder die Spezifikation?

    Alle Werte sind RMSE auf `anzahl_einsaetze`, gemittelt ueber dieselben 50
    Laeufe. Sie sind damit unmittelbar vergleichbar; es ist kein Wechsel des
    Massstabs zwischen den Gruppen im Spiel.

    Die Balken sind nach Gruppen sortiert, nicht global - sonst stuende die
    Referenz mitten zwischen den Verfahren und die Gruppierung waere nicht
    ablesbar.
```

#### `a4_laufzeit_guete(plt, FuncFormatter)`

*Zeile 500 · 80 Zeilen*

```text
Unterfrage 3: Aufwand gegen Guete.

    Die Zeitachse ist logarithmisch, weil zwischen Ridge und den Ensembles
    Groessenordnungen liegen - linear waere Ridge ein Punkt auf der Null.
```

#### `a5_holdout(plt, FuncFormatter)`

*Zeile 583 · 61 Zeilen*

```text
Die einmalige Auswertung auf den sechs zurueckgehaltenen Stadtteilen.

    Anders als in A1 gibt es hier KEINE Streuung - das Hold-out wird genau
    einmal ausgewertet. Fehlerbalken waeren an dieser Stelle falsch; die
    Einmaligkeit ist der Zweck des Hold-outs.

    Alle drei Stufen stehen nebeneinander, damit sichtbar bleibt, wovon der
    Abstand gemessen wird.
```

#### `_faktorgruppen_balken()`

*Zeile 647 · 29 Zeilen*

```text
(Strang, Beschriftung, Anteile je Gruppe) - alles aus CSV.

    ZWEI QUELLEN, ZWEI GROESSEN - das ist der Grund fuer die Fusszeile der
    Abbildung. Der Mengenbalken zeigt standardisierte KOEFFIZIENTEN des
    Poisson-GLM, die Strukturbalken zeigen SHAP-BEITRAEGE. Beide sind auf
    Summe 1 normiert und damit nebeneinander lesbar, aber sie sind nicht
    dieselbe Groesse und duerfen nicht als eine gelesen werden.

    Dass die Menge aus der Baseline kommt, ist kein Notbehelf: m04 ueberspringt
    dort jedes Vergleichsverfahren, weil keines seine Stufe-2-Baseline schlaegt
    - und Beitraege eines unterlegenen Modells waeren erklaertes Rauschen. Das
    beste Modell des Mengenstrangs IST das Poisson-GLM.
```

#### `a6_faktorgruppen(plt, FuncFormatter)`

*Zeile 678 · 62 Zeilen*

```text
UNTERFRAGE 1: Welche Faktorgruppe traegt wie viel?

    Gestapelte Balken statt gruppierter: Die Anteile summieren sich je Modell
    auf 100 %, und genau diese Aufteilung ist die Aussage. Gruppierte Balken
    wuerden zum Vergleich EINER Gruppe zwischen den Modellen einladen - das
    traegt hier nicht, weil die Werte aus verschiedenen Groessen stammen.

    Die Segmente sind ueber die Schraffur unterschieden, nicht ueber den
    Grauwert. Fuenf Grautoene in einem Balken sind im Schwarzweissdruck nicht
    mehr sicher zu trennen.
```

#### `a7_extrapolation(plt, FuncFormatter)`

*Zeile 743 · 65 Zeilen*

```text
Warum manche Folds schwer sind - R-3 als Bild statt als Vorbehalt.

    Ein Punkt je Lauf, 50 je Verfahren. Die drei Verfahren liegen bei gleichem
    x uebereinander, weil der Extrapolationsanteil eine Eigenschaft des FOLDS
    ist und nicht des Verfahrens - das ist kein Darstellungsfehler, sondern
    die halbe Aussage.

    ABGRENZUNG ZU #34, dieselbe wie in `m04.extrapolation_aufschluesseln`: Hier
    wird die Testmenge NICHT nach Extrapolationsgrad geschnitten und darin nach
    Verfahrensunterschieden gesucht. Die Einheit bleibt der Lauf, die Frage
    lautet, warum Laeufe unterschiedlich schwer sind. Die Primaeraussage bleibt
    unberuehrt.
```

#### `_lage_im_suchraum(name, parameter, wert)`

*Zeile 811 · 30 Zeilen*

```text
Relative Lage eines gefundenen Wertes in SEINEM Suchraum, 0 bis 1.

    Ohne diese Normierung liessen sich die Parameter nicht in eine Abbildung
    bringen: `alpha` laeuft ueber sechs Zehnerpotenzen, `subsample` ueber 0,4
    Einheiten. Die Frage lautet ohnehin nicht "welcher Wert", sondern "wie weit
    streuen die fuenf Folds in dem Raum, der zur Verfuegung stand".

    Die Umrechnung spiegelt `m02.suchraum()`: loguniform wird logarithmisch
    normiert, `int` und `uniform` linear, `choice` ueber die Position in der
    Liste aus config_modelle. Faellt ein Wert aus seinem Raum, gibt es None -
    dann hat sich der Suchraum seit dem Lauf geaendert, und die Zeile fehlt in
    der Abbildung, statt eine falsche Lage vorzutaeuschen.
```

#### `_hyperparameter_lagen()`

*Zeile 843 · 25 Zeilen*

```text
Je (Strang, Verfahren, Parameter) die fuenf Fold-Werte, auf 0..1 normiert.
```

#### `a8_hyperparameter(plt, FuncFormatter)`

*Zeile 870 · 83 Zeilen*

```text
Wie stabil ist die Modellwahl bei 29 Entwicklungsstadtteilen?

    Jede Zeile ist ein Hyperparameter, die fuenf Punkte sind die fuenf Folds.
    Die graue Strecke ist der volle Suchraum. Streuen die Punkte ueber die
    ganze Strecke, hat die Kreuzvalidierung diesen Parameter nicht bestimmt -
    das Tuning waehlt dann faktisch zufaellig.

    Das ist eine Aussage fuer Kapitel 8 und keine Fehlermeldung: Bei 23
    Trainingsstadtteilen je Fold ist genau dieses Verhalten zu erwarten, und es
    ist der ehrlichere Umgang damit, es zu zeigen statt die fuenf Parameter-
    saetze nur zu mitteln.

    Die Spannweite rechts ist die Kennzahl dazu: 1,00 heisst "von einem Rand
    des Suchraums zum anderen".
```

#### `a9_parallelisierung(plt, FuncFormatter)`

*Zeile 956 · 73 Zeilen*

```text
Die zweite Haelfte von Unterfrage 3: Was bringen zusaetzliche Kerne?

    A4 traegt die EINKERNIGE Trainingszeit auf - das ist der Aufwand, der
    zwischen den Verfahren vergleichbar ist. Diese Abbildung zeigt die andere
    Groesse, die im selben Lauf miterhoben wurde: den Faktor, um den derselbe
    Fit ueber alle Kerne schneller wird.

    Die Linie bei 1,0 ist nicht Dekoration. Ein Wert DARUNTER heisst, dass der
    parallele Fit LANGSAMER war - der Verwaltungsaufwand der Threads uebersteigt
    den Gewinn. Ohne die Linie liest man solche Balken als kleinen Gewinn.

    Bei Ridge ist ein Wert um 1 zu erwarten: Eine geschlossene Loesung hat
    nichts zu verteilen. Auch das ist ein Ergebnis fuer Unterfrage 4 und kein
    Messfehler.
```

#### `a10_qq_residuen(plt, FuncFormatter)`

*Zeile 1032 · 57 Zeilen*

```text
Halten die Residuen der linearen Spezifikation die Normalverteilung?

    Schroeter hat den QQ-Plot am 10.08.2026 selbst genannt. Er beantwortet eine
    Frage, die ein Test allein nicht beantwortet: WO die Abweichung liegt.
    Jarque-Bera sagt nur, DASS die Verteilung nicht normal ist - bei n = 3.036
    sagt er das ohnehin fast immer.

    Zu lesen ist die Abbildung an den ENDEN. Liegen die Punkte in der Mitte auf
    der Geraden und biegen nur aussen ab, ist die Verteilung im Kern normal und
    hat schwere Raender - genau das, was bei Einsatzzahlen zu erwarten ist.
    Eine Kruemmung ueber die ganze Laenge waere etwas anderes und schwerer
    wiegend.

    WOZU DAS UEBERHAUPT GEZEIGT WIRD, obwohl Normalitaet fuer die Punktprognose
    nicht erforderlich ist (siehe die Tabelle in Abschnitt 6 der
    Eignungspruefung): Die Anforderung wird geprueft und die Antwort lautet
    "besteht hier nicht". Das ist eine Aussage. Sie ungeprueft zu lassen waere
    keine.

    Gezeichnet, nicht gerechnet - die Quantile stehen fertig in
    `qq_residuen.csv`, erzeugt von `v2_eignung.annahmen()`.
```

#### `main()`

*Zeile 1092 · 34 Zeilen*

_kein Docstring_


# Absicherung — `tests/`


## `tests/test_aufbereitung.py`

*380 Zeilen · 22 Funktionen*

### Modulkopf

```text
Prüfungen der Datenaufbereitung – gesammelt an einer Stelle.

Diese Datei ist bewusst vom Analysecode getrennt: Die Module unter `prep/`
bleiben dadurch lesbar und eignen sich als Code-Beleg im Anhang der Arbeit,
während die Absicherung hier vollständig nachvollziehbar bleibt.

Geprüft werden die fertigen Datensätze in `data/processed/`, nicht der Code, der
sie erzeugt. Damit fällt auch auf, wenn jemand eine Datei von Hand ändert.

Gegenstand:

  1. Analysedatensatz  rechteckig, vollständig, fester Zeitraum
  2. Stadtteil-Split   kein Stadtteil zugleich Trainings- und Testfall,
                       unberührtes Hold-out, Aufteilungsspalten konsistent
  3. Merkmale          Lags gegen die Rohdaten verifiziert, kein Leakage
  4. Struktur          keine Ergebnisvariablen, Anteile konsistent

Ausführen:
  python tests/test_aufbereitung.py     # ohne weitere Abhängigkeiten
  pytest tests/                          # falls pytest vorhanden

Setzt voraus, dass `python prep/build.py` gelaufen ist.
```

### Kommentarbloecke (4)

**Zeile 68**

```text
---------------------------------------------------------------------------
 1. Analysedatensatz
 ---------------------------------------------------------------------------
```

**Zeile 131**

```text
---------------------------------------------------------------------------
 2. Zeitschnitte
 ---------------------------------------------------------------------------
```

**Zeile 208**

```text
---------------------------------------------------------------------------
 3. Merkmale
 ---------------------------------------------------------------------------
```

**Zeile 274**

```text
---------------------------------------------------------------------------
 4. Struktur der Einsatzlast
 ---------------------------------------------------------------------------
```

### Funktionen

#### `regression()`

*Zeile 52 · 6 Zeilen*

_kein Docstring_

#### `klassifikation()`

*Zeile 60 · 6 Zeilen*

_kein Docstring_

#### `test_panel_rechteckig_und_vollstaendig()`

*Zeile 71 · 12 Zeilen*

```text
Vollständiges Kreuzprodukt Stadtteil x Monat, keine fehlenden Werte.

    Ein unbalanciertes Panel wäre der gefährlichste stille Fehler: Tritt ein
    Stadtteil mitten in der Zeitreihe hinzu, springen die Testfenster-Summen
    allein dadurch, ohne dass sich am Modell etwas ändert.
```

#### `test_zeitraum_festgesetzt()`

*Zeile 85 · 11 Zeilen*

```text
Der Zeitraum kommt aus Konstanten, nicht aus den Daten.

    Sonst verschiebt sich die Analyse bei jedem neuen Download – und ein
    unvollständiger Randmonat kann unbemerkt ins Testfenster geraten
    (Decision Log #12: Januar 2026 mit 258 statt ~3.300 Einsätzen).
```

#### `test_datentypen_modelltauglich()`

*Zeile 98 · 22 Zeilen*

```text
Kein Merkmal darf einen pandas-eigenen (nullable) Typ haben.

    Die ACS-Aggregation liefert `median_haushaltseinkommen` und `median_miete`
    als `Int64`. Solange nur scikit-learn rechnet, fällt das nicht auf – der
    StandardScaler wandelt still um. Sobald aber eine einzige Int64-Spalte im
    Merkmalssatz steht, liefert `X.to_numpy()` ein **object**-Array statt
    float64, und XGBoost lehnt den DataFrame ab ("dtypes for data must be int,
    float, bool or category"). Der Fehler träte also erst beim dritten der drei
    zu vergleichenden Verfahren auf – und säße dann im Preprocessing.
```

#### `test_exposure_und_kriminalitaetsindex_vorhanden()`

*Zeile 122 · 7 Zeilen*

```text
Die log-Transformationen sind gebildet und vollständig.
```

#### `test_folds_ordnung_und_holdout()`

*Zeile 134 · 23 Zeilen*

```text
Kein Stadtteil ist zugleich Trainings- und Testfall.

    Das ist der zentrale Punkt des Stadtteil-Splits: Sobald ein Stadtteil in
    beiden Mengen steht, kennt das Modell sein Niveau bereits und die
    Strukturmerkmale müssen nichts mehr erklären – genau die Frage, die die
    Arbeit stellt, bliebe dann unbeantwortet.
```

#### `test_jeder_fold_deckt_den_vollen_zeitraum()`

*Zeile 159 · 11 Zeilen*

```text
Ein Teststadtteil wird mit allen seinen Monaten getestet.

    Andernfalls vermischten sich Stadtteil- und Zeitschnitt, und die Fold-
    Streuung wäre nicht mehr interpretierbar.
```

#### `test_aufteilungsspalten_konsistent()`

*Zeile 172 · 18 Zeilen*

```text
`fold` und `ist_holdout` in der Datei müssen zu prep/s2_datensaetze.py passen.

    Die Spalten sind der Grund, warum die Fairness-Regel nachzählbar ist. Wären
    sie veraltet – etwa weil jemand die Stadtteilliste geändert, aber den
    Datensatz nicht neu gebaut hat –, liefen alle drei Verfahren auf falschen,
    aber untereinander identischen Splits: der Vergleich bliebe fair, das
    Ergebnis wäre trotzdem falsch.
```

#### `test_folds_decken_die_groessenspanne_ab()`

*Zeile 192 · 14 Zeilen*

```text
Stratifizierung nach Bevölkerung: Kein Fold besteht nur aus Großstadtteilen.

    Sonst wäre die Streuung über die Folds ein Größeneffekt und kein
    Modellunterschied.
```

#### `test_merkmale_vollstaendig()`

*Zeile 211 · 11 Zeilen*

```text
Der Merkmalssatz ist vollständig, die Rate ist gebildet.
```

#### `test_saison_zyklisch()`

*Zeile 224 · 9 Zeilen*

```text
sin/cos liegen auf dem Einheitskreis, Dezember grenzt an Januar.
```

#### `test_lags_gegen_rohdaten()`

*Zeile 235 · 18 Zeilen*

```text
Der zentrale Leakage-Test: Lags gegen die Rohdaten nachschlagen.

    Ein verrutschtes `shift` oder ein Wert, der über eine Stadtteilgrenze hinweg
    gezogen wird, ist im Code nicht zu sehen und rechnet das Ergebnis lautlos
    schön. Deshalb wird stichprobenartig direkt nachgeschlagen – inklusive der
    Vorlaufmonate, die für lag_12 des ersten Analysemonats gebraucht werden.
```

#### `test_lags_nicht_gegenwartsbezogen()`

*Zeile 255 · 6 Zeilen*

```text
Gegenprobe gegen ein vergessenes shift(): lag_1 darf nicht der Istwert sein.
```

#### `test_vorlauf_ohne_eigene_zeilen()`

*Zeile 263 · 9 Zeilen*

```text
Die Vorlaufmonate liefern Lag-Werte, aber keine eigenen Beobachtungen.

    Sonst enthielte der Datensatz Zeilen ohne gültige Strukturmerkmale – der
    Kriminalitätsindex beginnt erst 2015-01 (Decision Log #23).
```

#### `test_keine_ergebnisvariablen()`

*Zeile 277 · 9 Zeilen*

```text
Wichtigster Test des Strukturteils.

    Sachschaden, Löschfahrzeuge, Alarmstufe und Antwortzeit stehen erst nach dem
    Einsatz fest. Rutscht eine dieser Spalten in den Merkmalssatz, sieht das
    Ergebnis gut aus und ist wertlos.
```

#### `test_struktur_gleiche_abgrenzung_wie_regression()`

*Zeile 288 · 16 Zeilen*

```text
Beide Teile der Arbeit beruhen auf demselben Datenbestand.

    Gleiche Analyseeinheit, gleiche Stadtteile, gleicher Zeitraum und – der
    entscheidende Punkt – dieselbe Fold-Zuordnung. Nur dann ist der Vergleich
    zwischen Menge und Struktur überhaupt zulässig (Gutachten R1).
```

#### `test_anteile_konsistent()`

*Zeile 306 · 9 Zeilen*

```text
Die vier Anteile summieren sich je Zeile auf 1 und passen zu den Zählungen.
```

#### `test_zielklasse_konsistent()`

*Zeile 317 · 11 Zeilen*

```text
Die dominante Einsatzart ist argmax ueber die vier Anteile.

    Eine echte Klasse, kein gesetzter Schwellwert - deshalb entfaellt die
    Begruendungslast einer kuenstlichen Einteilung (Altman & Royston 2006).
```

#### `test_seltene_klasse_in_jedem_fold()`

*Zeile 330 · 13 Zeilen*

```text
Brand muss in jedem Test-Fold vorkommen.

    Von 70 brand-dominierten Monaten liegen 35 allein in Bayview Hunters Point.
    Ohne Stratifizierung nach der seltenen Klasse hatte in drei von vier
    Aufteilungen ein Fold null Brand-Testfaelle - Macro-F1 mittelt dann ueber
    eine Klasse, die gar nicht vorkommt, und springt zwischen den Folds.
```

#### `test_struktur_hat_signal()`

*Zeile 345 · 11 Zeilen*

```text
Die Anteile variieren zwischen Stadtteilen – sonst gäbe es nichts zu erklären.

    Der Brandanteil schwankt zwischen den Stadtteilen um mehr als den Faktor 2;
    genau diese Variation soll durch Strukturmerkmale erklärt werden.
```

#### `main()`

*Zeile 359 · 18 Zeilen*

_kein Docstring_


# Werkzeuge (nicht Abgabe) — `tools/`


## `tools/codebook.py`

*452 Zeilen · 8 Funktionen*

### Modulkopf

```text
Codebook - die eine grosse Merkmalstabelle fuer Kapitel 4.

    python tools/codebook.py            erzeugt die Tabelle
    python tools/codebook.py -v         zusaetzlich die Spalten je Datensatz

Ausgang: results/codebook/merkmale.csv · merkmale.md

NICHT TEIL DER ABGABE - das SKRIPT. Die erzeugte Tabelle schon: Sie gehoert in
Kapitel 4. Der Ordner `tools/` wird vor dem Packen geloescht, `results/` nicht;
deshalb ist die Ausgabe bewusst selbsttragend und enthaelt alles, was die
Tabelle im Text braucht.

Auflage Schroeter vom 10.08.2026, woertlich: "Codebook und Variablenbuch:
Skalenniveau ... Jedes Merkmal in einer Tabelle auflisten (Wertebereich etc.)
eine grosse Tabelle. Was, wie, wofuer" - und ausdruecklich: NICHT fuer jedes
Merkmal eine eigene deskriptive Statistik.

--------------------------------------------------------------------------
DIE AUFTEILUNG, AUF DER DAS SKRIPT BERUHT
--------------------------------------------------------------------------
Eine Haelfte der Tabelle ist GEMESSEN, die andere BEHAUPTET:

  gemessen     Wertebereich, Zeilenzahl, fehlende Werte, Zahl der
               Auspraegungen, in welchem Datensatz die Spalte steht. Entsteht
               bei jedem Lauf neu aus den Parquet-Dateien und kann deshalb
               nicht veralten.

  behauptet    Skalenniveau, Einheit, Quelle, Was/Wie/Wofuer. Das steht so in
               keiner Datei und muss von Hand gepflegt werden - unten in META.

Die Trennung ist der Zweck der Uebung. Waeren die Wertebereiche abgeschrieben,
waeren sie beim naechsten Pipeline-Lauf still falsch.

--------------------------------------------------------------------------
DIE WAECHTERFUNKTION
--------------------------------------------------------------------------
Das Skript bricht mit Exit-Code 1 ab, wenn

  - eine Spalte in den Parquet-Dateien steht, aber nicht in META
    -> ein neues Merkmal waere sonst stillschweigend undokumentiert
  - ein META-Eintrag auf keine Spalte passt
    -> ein entferntes Merkmal wuerde sonst weiter in Kapitel 4 stehen

Damit ist die Tabelle nicht nur einmal richtig, sondern bleibt es.

--------------------------------------------------------------------------
EIN BEFUND, DER IN DIE TABELLE GEHOERT
--------------------------------------------------------------------------
Fuenf Spalten tragen die Endung `_pct`, enthalten aber ANTEILE von 0 bis 1,
keine Prozentwerte: `armutsquote_pct` steht auf 0,36 und meint 36 %. Wer den
Namen liest statt den Wertebereich, berichtet den Faktor 100 falsch. Die
Spalte "Einheit" weist das deshalb ausdruecklich aus.

Umbenannt wird nichts - die Namen stehen in den fertigen Parquet-Dateien und
in allen bisherigen Ergebnissen. Dokumentiert wird es.
```

### Kommentarbloecke (2)

**Zeile 98**

```text
==========================================================================
 META - die behauptete Haelfte. Reihenfolge = Reihenfolge in der Tabelle.
 ==========================================================================
```

**Zeile 305**

```text
==========================================================================
 Die gemessene Haelfte
 ==========================================================================
```

### Funktionen

#### `M(skala, einheit, quelle, was, wie, wofuer, schluessel)`

*Zeile 94 · 2 Zeilen*

_kein Docstring_

#### `_de(x, stellen)`

*Zeile 308 · 3 Zeilen*

```text
Deutsche Zahlformatierung mit Tausenderpunkt.
```

#### `spanne(s, schluessel)`

*Zeile 313 · 12 Zeilen*

```text
Wertebereich - bei Zahlen min bis max, sonst die Auspraegungen.
```

#### `gemessen(datensaetze)`

*Zeile 327 · 25 Zeilen*

```text
Je Spalte: Wertebereich, Zeilen, fehlende Werte, Auspraegungen, Herkunft.

    Steht eine Spalte in beiden Dateien, wird der REGRESSIONS-Datensatz
    ausgewiesen - er ist die Obermenge (die Klassifikation ist eine echte
    Teilmenge, Decision Log #31). Die Spalte "Datensatz" haelt fest, wo sie
    ueberhaupt vorkommt, damit der Unterschied sichtbar bleibt.
```

#### `waechter(datensaetze)`

*Zeile 355 · 14 Zeilen*

```text
Jede Spalte dokumentiert, jeder Eintrag belegt - sonst Abbruch.
```

#### `baue()`

*Zeile 371 · 21 Zeilen*

_kein Docstring_

#### `als_markdown(df)`

*Zeile 394 · 28 Zeilen*

```text
Eine grosse Tabelle, wie verlangt - plus zwei Saetze Lesehilfe.
```

#### `main(argv)`

*Zeile 424 · 25 Zeilen*

_kein Docstring_


## `tools/suchdiagnose.py`

*403 Zeilen · 8 Funktionen*

### Modulkopf

```text
Suchdiagnose - war die Hyperparametersuche am Limit?

    python tools/suchdiagnose.py            beide Straenge, alle Verfahren
    python tools/suchdiagnose.py menge      nur die Regression
    python tools/suchdiagnose.py struktur   nur die Klassifikation
    python tools/suchdiagnose.py --nur-xgboost   das billigste sinnvolle Mass
    python tools/suchdiagnose.py --test     Rauchtest, Budget 6, ~3 min

`--test` schreibt nach `results/suchdiagnose_test/` und laesst die echte
Ausgabe unberuehrt. Vor einem zweistuendigen Lauf einmal ausfuehren - er
prueft beide Straenge einmal durch, damit ein Fehler nicht erst nach der
ganzen Rechenzeit auffaellt.

Ausgang: results/suchdiagnose/kurve.csv · raender.csv · zusammenfassung.md

NICHT TEIL DER ABGABE als Skript. Die Befunde schon - sie beantworten eine
Auflage aus der Sprechstunde vom 10.08.2026 mit einer Messung statt mit einem
Argument.

--------------------------------------------------------------------------
DIE ZWEI FRAGEN
--------------------------------------------------------------------------
  1  WAR DAS BUDGET ZU KLEIN?   `tuning.csv` haelt nur den Gewinner fest, nicht
     den Weg dorthin. Diese Diagnose schreibt jede einzelne Ziehung mit ihrem
     inneren Guetewert mit und bildet daraus die SUCHKURVE: bester Wert nach
     n Ziehungen. Steigt sie nach Ziehung 50 noch, war Budget 50 zu klein.
     Laeuft sie flach aus, war sie es nicht.

  2  STAND DER ZAUN AN DER FALSCHEN STELLE?  Ein Suchraum ist eine Festlegung,
     keine Naturkonstante. Liegt der beste gefundene Wert AM RAND, liegt das
     Optimum vermutlich dahinter - und die Suche durfte nie hin.

--------------------------------------------------------------------------
ZWEI SORTEN RAND - der Unterschied entscheidet
--------------------------------------------------------------------------
Gemessen am Lauf vom 07.08. (Budget 50, fuenf Folds):

  NATUERLICH, nichts zu tun
    random_forest max_features       4/5 waehlen 1,0 - das sind ALLE Merkmale
    random_forest min_samples_leaf   3/5 waehlen 1  - weniger als eine
                                     Beobachtung je Blatt gibt es nicht
    Das ist ein BEFUND, kein Mangel: Der Wald will maximale Flexibilitaet.

  WILLKUERLICH, hier kann etwas fehlen
    xgboost max_depth (Struktur)     4/5 waehlen 3 - die UNTERGRENZE
    xgboost max_depth (Menge)        3/5 am Rand, beide Enden getroffen
    ridge alpha, xgboost reg_lambda  je 1/5 am Rand

Der erste Fall ist der wichtigste des Projekts: In der Klassifikation will
XGBoost den flachsten Baum, den es darf. Genau dort widersprechen sich
Kreuzvalidierung und Hold-out (R-2, B-42) - das Muster von Ueberanpassung.
Wird die Untergrenze geoeffnet und XGBoost waehlt dann Tiefe 1 oder 2, war der
Suchraum die Ursache.

--------------------------------------------------------------------------
WIE VERGLICHEN WIRD - ein Lauf statt zwei
--------------------------------------------------------------------------
Naheliegend waeren zwei Durchgaenge, einer je Suchraum. Das kostet doppelt.
Stattdessen laeuft NUR der erweiterte Raum, und bei jeder Ziehung wird
vermerkt, ob sie auch im ALTEN Raum gelegen haette. Daraus entstehen zwei
Kurven aus denselben Ziehungen, denselben Folds und demselben Startwert:

    bester Wert ueber alle Ziehungen          -> erweiterter Raum
    bester Wert ueber die Teilmenge "alt"     -> urspruenglicher Raum

Ehrlich dazu: Die Teilmenge ist kleiner als 100, der Vergleich also nicht bei
gleichem Budget. Die Zahl der Ziehungen je Teilmenge wird deshalb mitberichtet.

--------------------------------------------------------------------------
WAS DIESE DIAGNOSE NICHT LEISTET
--------------------------------------------------------------------------
Der innere Guetewert ist NICHT die Testleistung. Ein besserer innerer Wert
garantiert kein besseres Ergebnis auf unbekannten Stadtteilen - er sagt nur,
dass die Suche noch etwas gefunden hat. Ob sich das auf die Prognose
uebertraegt, zeigt erst der Hauptlauf.

Die Diagnose beantwortet also: War die Suche am Limit? Nicht: Wird das
Ergebnis besser?

--------------------------------------------------------------------------
WAS SIE NICHT ANFASST
--------------------------------------------------------------------------
  - `results/regression/` und `results/klassifikation/` bleiben unberuehrt
  - das HOLD-OUT wird nicht gelesen; gefiltert wird wie in m02/m03, bevor
    irgendetwas rechnet
  - `config_modelle.SUCHRAEUME` wird nicht veraendert, nur lokal ueberlagert
```

### Kommentarbloecke (4)

**Zeile 113**

```text
==========================================================================
 Die erweiterten Suchraeume - NUR wo der Rand willkuerlich ist
 ==========================================================================
 Nicht erweitert werden max_features, min_samples_leaf, subsample und
 colsample_bytree: Deren Grenzen sind natuerlich (alle Merkmale, eine
 Beobachtung, der ganze Datensatz). Dahinter existiert nichts.

 Ebenfalls NICHT erweitert: n_estimators. Nur ein Fold lag nahe der Grenze,
 und mehr Baeume sind der groesste Laufzeittreiber. Bewusste Auslassung.
```

**Zeile 127**

```text
ZWEI Aenderungen. Erstens nach oben erweitert. Zweitens die
 REIHENFOLGE korrigiert: `None` heisst unbegrenzte Tiefe, ist also
 faktisch der TIEFSTE Wert - stand in der alten Liste aber an erster
 Stelle. Jede Auswertung, die die Listenposition als Tiefe liest,
 bekam damit ein verdrehtes Bild (betrifft auch Abbildung A8).
```

**Zeile 147**

```text
Steuert die Verlustfunktion der REGRESSION und ist bei `multi:softprob`
 bedeutungslos. XGBoost nimmt den Parameter stillschweigend an und ignoriert
 ihn - ein Sechstel des Budgets ginge auf eine wirkungslose Dimension, und die
 Suchkurve des Strukturstrangs fiele dadurch zu flach aus.
 `m03_struktur.suchraum()` entfernt ihn aus demselben Grund; ohne diese Zeile
 weicht die Diagnose vom Hauptlauf ab (gefunden im Rauchtest am 13.08.2026).
```

**Zeile 286**

```text
`max_depth = None` heisst UNBEGRENZTE Tiefe. Ueber den DataFrame
 wird daraus NaN, und das sieht wie ein fehlender Wert aus statt
 wie der tiefste moegliche. Deshalb ausgeschrieben.
```

### Funktionen

#### `erweitert(name, strang)`

*Zeile 156 · 7 Zeilen*

```text
Suchraum eines Verfahrens mit den Erweiterungen ueberlagert.
```

#### `_verteilungen(raum, praefix)`

*Zeile 165 · 18 Zeilen*

```text
Spezifikation -> scipy-Verteilungen. Wortgleich zu m02.suchraum().
```

#### `im_alten_raum(name, parameter)`

*Zeile 185 · 20 Zeilen*

```text
Haette diese Ziehung auch im urspruenglichen Suchraum liegen koennen?

    Nur die erweiterten Parameter werden geprueft - die uebrigen sind
    unveraendert und liegen zwangslaeufig drin.
```

#### `eine_suche(strang, name, train, fold)`

*Zeile 208 · 35 Zeilen*

```text
Ein Suchlauf mit Budget 100. Gibt JEDE Ziehung zurueck, nicht nur den Sieger.
```

#### `_md(df)`

*Zeile 245 · 13 Zeilen*

```text
Markdown-Tabelle von Hand.

    NICHT `DataFrame.to_markdown()`: Das braucht `tabulate`, und das steht
    weder in `requirements.txt` noch im gemessenen `requirements_lauf.txt`.
    Der Aufruf waere erst nach ein bis zwei Stunden Rechenzeit gescheitert -
    beim Schreiben des Berichts, also nach der ganzen Arbeit.
```

#### `kurve(df)`

*Zeile 260 · 16 Zeilen*

```text
Bester Wert nach n Ziehungen - einmal gesamt, einmal nur "alter Raum".

    Beide Guetemasse sind so gerichtet, dass GROSS besser ist
    (neg_root_mean_squared_error und f1_macro), deshalb genuegt das laufende
    Maximum.
```

#### `raender(df)`

*Zeile 278 · 18 Zeilen*

```text
Wo liegt der SIEGER je Fold - im erweiterten Bereich oder im alten?
```

#### `main(argv)`

*Zeile 299 · 101 Zeilen*

_kein Docstring_


## `tools/pruefe_zahlen.py`

*747 Zeilen · 19 Funktionen*

### Modulkopf

```text
Zahlenwaechter - prueft die Dokumentation gegen die Ergebnisdateien.

NICHT TEIL DER ABGABE. Dieser Ordner ist ein Arbeitswerkzeug und kann vor dem
Packen des Abgabe-ZIP geloescht werden. Er erzeugt keine Ergebnisse und wird
von keinem Skript in prep/, vorpruefung/ oder modelle/ importiert.

WOZU
--------------------------------------------------------------------------
`CLAUDE.md` legt fest: jede Ergebniszahl steht in `docs/03_STAND.md` und nur
dort. Diese Regel haelt genau so lange, wie jemand sie nach jedem Lauf von
Hand nachzieht. Am 07.08.2026 hat sie nicht gehalten - Abschnitt 4 berichtete
die Negative Binomial (RMSE 37,27), waehrend Abschnitt 5 derselben Datei das
Poisson-GLM (33,98) auswies, und `06_RISIKEN.md` empfahl in R-9 das Gegenteil
dessen, was Decision Log #43 umgesetzt hatte.

Kein Rechenfehler, sondern Drift: Eine Zahl lebte an zwei Orten, und nur einer
wurde gepflegt. Dagegen hilft keine Sorgfalt, sondern ein Exit-Code.

WIE
--------------------------------------------------------------------------
Jede Pruefung sagt: "Wert X aus Datei Y muss in Abschnitt Z von Dokument D
vorkommen." Der Sollwert wird bei jedem Lauf NEU aus results/ gelesen, nie aus
einem Dokument uebernommen. Gesucht wird abschnittsweise, damit eine Zahl, die
zufaellig anderswo steht, nicht als Treffer durchgeht.

Drei Arten von Befund:
  FEHLER    Der Sollwert steht nicht im geforderten Abschnitt.
  ALTLAST   Ein frueher gueltiger Wert steht noch da. Warnung, kein Fehler -
            historische Verweise ("Bis zum 06.08. stand hier ...") sind
            erwuenscht und werden erkannt.
  HINWEIS   Struktur stimmt nicht (Abschnitt umbenannt, Datei fehlt).

Exit-Code 0 = sauber, 1 = mindestens ein FEHLER.

AUSFUEHREN
--------------------------------------------------------------------------
  python tools/pruefe_zahlen.py            alle Pruefungen
  python tools/pruefe_zahlen.py -v         zusaetzlich die bestandenen zeigen

Nach jedem Lauf von m02/m03/m04/v1/v3 aufrufen. Schlaegt etwas fehl, ist die
DOKUMENTATION nachzuziehen - nicht die Pruefung anzupassen.
```

### Kommentarbloecke (12)

**Zeile 58**

```text
==========================================================================
 1  Werte aus den Ergebnisdateien holen
 ==========================================================================
```

**Zeile 105**

```text
==========================================================================
 2  Dokumente in Abschnitte zerlegen
 ==========================================================================
```

**Zeile 162**

```text
==========================================================================
 3  Die Pruefungen
 ==========================================================================
```

**Zeile 383**

```text
---- Anforderungen je Verfahren (§7, Auflage 10.08.2026) -------------
 Die drei formalen Tests aus `v2_eignung.annahmen()`. Sollwert ist die
 NUMERISCHE Spalte `statistik_wert`, nicht die lesbare `statistik` - eine
 Zeichenkette, die man parst, parst sich beim naechsten Formatwechsel
 anders.
```

**Zeile 401**

```text
==========================================================================
 4  Abgeleitete Behauptungen - die Klasse Fehler, die niemand nachrechnet
 ==========================================================================
```

**Zeile 439**

```text
Stand nach dem finalen Lauf vom 16.08.2026 (#49/#50/#52). Bis
 dahin war "ridge vs xgboost|einsaetze_je_1000_ew" die einzige
 trennbare Verfahrenspaarung; mit den erweiterten Suchraeumen
 verschlechtert sich XGBoost und liegt nun selbst gesichert hinter
 der Baseline. R-1 ist damit vollstaendig eingetreten: KEINE
 Verfahrenspaarung ist mehr trennbar, nur noch Abstaende zur
 Baseline sind signifikant.
```

**Zeile 488**

```text
Berichte, die die Wahl der Baseline BEGRUENDEN - erzeugt, nicht von Hand
 geschrieben. Nur diese werden geprueft: In docs/ und in 07_BEFUNDE.md sind
 Rueckblicke auf die Negative Binomial erwuenscht, dort greift ALTLASTEN.
```

**Zeile 548**

```text
b) Ein verworfenes Modell darf genannt werden - aber nur MARKIERT.
    Die Abgrenzung "warum nicht die Negative Binomial" gehoert in den
    Bericht; sie muss nur als Abgrenzung erkennbar sein und nicht wie
    eine Setzung dastehen. Massstab ist dieselbe HISTORIE-Regel, die
    auch fuer Altlasten in docs/ gilt.
```

**Zeile 564**

```text
==========================================================================
 5  Altlasten - frueher gueltige Werte, die noch herumstehen
    HISTORIE und UMFELD stehen hier und werden von pruefe_baselinename
    mitbenutzt - dieselbe Regel, was ein zulaessiger Rueckblick ist.
 ==========================================================================
```

**Zeile 585**

```text
`ausser` seit 16.08.2026: Im finalen Lauf ist 0,020 der p-Wert von
 XGBoost gegen die Baseline (§5.1). Ohne die Ausnahme meldet der Waechter
 eine echte Zahl als Altlast - ein Fehlalarm, der die uebrigen Meldungen
 entwertet, weil man aufhoert hinzusehen.
```

**Zeile 593**

```text
Werte aus dem Lauf mit Budget 50 (07.08.2026). Nach Decision Log #52 wird
 ausschliesslich der finale Lauf berichtet - kein Vorher-Nachher, keine
 zweite Ergebnisreihe. Taucht eine dieser Zahlen in docs/ auf, ist sie
 entweder ein Rueckblick (dann markiert) oder ein Verstoss gegen #52.
```

**Zeile 651**

```text
==========================================================================
 6  Lauf
 ==========================================================================
```

### Funktionen

#### `tab(pfad)`

*Zeile 64 · 8 Zeilen*

```text
CSV aus results/, einmal gelesen und gemerkt.
```

#### `wert(pfad, spalte)`

*Zeile 74 · 12 Zeilen*

```text
Ein einzelner Wert. Bricht ab, wenn der Filter nicht genau eine Zeile trifft.

    Das ist Absicht: Trifft er mehrere, hat sich das Format der Datei geaendert
    und die Pruefung waere ab da stillschweigend falsch.
```

#### `mittel(pfad, spalte)`

*Zeile 88 · 8 Zeilen*

```text
Mittelwert ueber alle Zeilen, die der Filter trifft.
```

#### `summe(pfad, spalte)`

*Zeile 98 · 5 Zeilen*

_kein Docstring_

#### `abschnitte(datei)`

*Zeile 116 · 25 Zeilen*

```text
Zerlegt ein Markdown-Dokument an den nummerierten Ueberschriften.

    Es entstehen zwei Ebenen von Schluesseln:
      "4"    aus `## 4. Die Baselines`      - das ganze Kapitel
      "5.1"  aus `### 5.1 Menge ...`        - nur dieser Unterabschnitt

    Die feine Ebene ist wichtig: Ein Wert, der im Kapitel noch einmal
    legitim vorkommt (etwa 35,88 in der Ergebnistabelle UND in der Ablation),
    wuerde eine Pruefung auf Kapitelebene bestehen lassen, obwohl die
    eigentliche Zeile falsch ist.
```

#### `zahlen_in(text)`

*Zeile 146 · 14 Zeilen*

```text
Alle deutsch formatierten Zahlen eines Textes als float.

    Erkennt Minuszeichen (U+2212) ebenso wie Bindestriche und den Punkt als
    Tausendertrenner. `1.234,5` -> 1234.5
```

#### `suchraum(text, anker)`

*Zeile 179 · 9 Zeilen*

```text
Der Text, in dem der Sollwert stehen muss.

    Mit Anker sind das nur die Zeilen, die ihn enthalten - also in aller Regel
    die eine Tabellenzeile, um die es geht. Ohne Anker der ganze Abschnitt.
```

#### `baue_pruefungen()`

*Zeile 198 · 201 Zeilen*

```text
Alle tragenden Zahlen der Arbeit, je mit ihrer Quelle.

    Aufgenommen wird, was in Kapitel 5 bis 9 als Zahl auftaucht oder eine
    Aussage traegt. Nicht aufgenommen wird, was nur beschreibend ist.
```

##### innere Funktion `add()`

_kein Docstring_

#### `pruefe_verhaeltnisse(erg)`

*Zeile 404 · 19 Zeilen*

```text
Saetze der Form "X-mal schneller". Sie altern unbemerkt mit.
```

#### `pruefe_negative_vorhersagen(erg)`

*Zeile 425 · 9 Zeilen*

```text
Die Aussage 'keine negativen Vorhersagen' muss gelten, nicht gehofft sein.
```

#### `pruefe_signifikanzen(erg)`

*Zeile 436 · 31 Zeilen*

```text
Welche Paarungen sind signifikant? Aendert sich das, aendert sich Kapitel 7.
```

#### `pruefe_holdout_unberuehrt(erg)`

*Zeile 469 · 12 Zeilen*

```text
Das Hold-out darf genau eine Auswertung haben - je Verfahren eine Zeile.
```

#### `pruefe_baselinename(erg)`

*Zeile 494 · 68 Zeilen*

```text
Nennt der erzeugte Bericht dieselbe Stufe-2-Baseline, die gerechnet wurde?

    ANLASS, 10.08.2026. `v2_eignung.py` schloss aus der Overdispersion, Poisson
    scheide aus und die Negative Binomial sei die passende Count-Baseline -
    waehrend `v1_baselines.py` seit Decision Log #45 ein Poisson-GLM anpasst.
    Der Bericht argumentierte damit gegen die eigene Umsetzung, und zwar in
    genau dem Dokument, das die Wahl der Baseline belegen soll.

    WARUM DIE VIER UEBRIGEN PRUEFUNGEN DAS NICHT GEFUNDEN HABEN - zwei Gruende,
    beide behebbar nur durch diese Pruefung:

      1  Sie lesen `docs/`. Dies hier ist eine ERZEUGTE Datei unter `results/`,
         die bis heute von keiner Pruefung angefasst wurde.
      2  Es ist keine Zahl, sondern ein NAME. Der Zahlenwaechter sucht
         ausschliesslich nach deutsch formatierten Zahlen.

    Sollwert ist die Spalte `modell` der Baseline-Dateien, Stufe 2. Sie
    entsteht bei jedem Lauf neu aus dem, was tatsaechlich angepasst wurde - der
    Name kann also nicht veralten, ohne dass diese Pruefung es merkt.
```

#### `_als_zahl(muster)`

*Zeile 613 · 2 Zeilen*

_kein Docstring_

#### `pruefe_altlasten(erg)`

*Zeile 620 · 29 Zeilen*

```text
Findet frueher gueltige Werte, die noch unmarkiert herumstehen.

    Ein Rueckblick ist erlaubt und erwuenscht - er ist daran zu erkennen, dass
    er entweder eine Markierung traegt ("bis zum 06.08. stand hier ...") oder
    den heutigen Wert danebenstellt ("von 37,27 auf 33,98"). Fehlt beides, ist
    die Zahl vermutlich stehen geblieben.
```

#### `laufe(ausfuehrlich)`

*Zeile 654 · 86 Zeilen*

_kein Docstring_

#### `_de(x)`

*Zeile 742 · 2 Zeilen*

_kein Docstring_


## `tools/sichere_ergebnisse.py`

*196 Zeilen · 5 Funktionen*

### Modulkopf

```text
Ergebnisse sichern - vor jedem Lauf, der `results/` ueberschreibt.

    python tools/sichere_ergebnisse.py                 mit automatischem Namen
    python tools/sichere_ergebnisse.py budget50        mit eigenem Namen
    python tools/sichere_ergebnisse.py --liste         zeigt vorhandene Sicherungen
    python tools/sichere_ergebnisse.py alt --hinweis "Lauf vom 07.08., Budget 50"

--------------------------------------------------------------------------
EINE GRENZE, DIE MAN KENNEN MUSS
--------------------------------------------------------------------------
Das Skript liest die Konfiguration **zum Zeitpunkt der Sicherung** aus
`config_modelle.py`. Es kann NICHT wissen, mit welcher Einstellung die
Dateien in `results/` tatsaechlich entstanden sind - diese Information steht
nirgends in den Ergebnissen.

Wurde die Konfiguration nach dem Lauf und vor der Sicherung geaendert,
beschreibt das Manifest die falsche. Genau das ist am 14.08.2026 passiert:
`TUNING_BUDGET` stand bereits auf 100 (#50), waehrend die gesicherten
Ergebnisse aus dem Lauf mit Budget 50 stammten.

Daraus die Regel: **erst sichern, dann die Konfiguration aendern.** Wo das
nicht mehr geht, `--hinweis` benutzen - der Text steht im Manifest ganz oben.

Ausgang: archiv/JJJJ-MM-TT_<name>/

NICHT TEIL DER ABGABE. Arbeitswerkzeug wie `pruefe_zahlen.py`.

--------------------------------------------------------------------------
WOZU
--------------------------------------------------------------------------
`results/` ist die einzige Stelle, an der die Ergebnisse liegen, und jeder
Lauf ueberschreibt sie. Der Ordner ist zudem in `.gitignore` - es gibt also
weder eine Versionierung noch ein Zurueck. Ein Lauf mit geaenderter
Konfiguration ist damit unumkehrbar, solange niemand vorher kopiert.

Genau das passiert am Sonntag: Budget 100 und vier erweiterte Suchraeume
(#49, #50) erzeugen andere Hyperparameter und damit andere Guetemasse. Ohne
Sicherung waere der Stand vom 07.08. weg - und mit ihm die Vergleichsbasis
fuer die Frage, ob die Aenderung etwas gebracht hat.

--------------------------------------------------------------------------
WAS GESICHERT WIRD - und warum ein Manifest dazugehoert
--------------------------------------------------------------------------
Kopiert wird der gesamte Baum `results/`. Daneben entsteht `manifest.md` mit

  - Datum, Uhrzeit, Git-Commit und Branch
  - der KONFIGURATION, die diese Ergebnisse erzeugt hat: Tuning-Budget,
    Wiederholungen, Folds, Random State und alle Suchraeume im Wortlaut
  - Zahl der Dateien, Gesamtgroesse
  - MD5-Summe je Ergebnisdatei

Das Manifest ist der eigentliche Wert. Eine Kopie ohne Konfiguration
beantwortet spaeter nicht die Frage, die man dann hat: WELCHE Einstellung hat
diese Zahlen erzeugt? Die MD5-Summen erlauben zudem den Nachweis, dass ein
Wiederholungslauf bitgleich reproduziert - genau die Behauptung aus Kapitel 6.
```

### Funktionen

#### `git()`

*Zeile 75 · 7 Zeilen*

```text
Git-Angabe oder ein Strich - ein fehlendes Repo ist kein Abbruchgrund.
```

#### `md5(pfad)`

*Zeile 84 · 6 Zeilen*

_kein Docstring_

#### `konfiguration()`

*Zeile 92 · 26 Zeilen*

```text
Die Einstellungen, die den gesicherten Stand erzeugt haben.

    Wird IMPORTIERT, nicht abgeschrieben - sonst sichert das Manifest, was
    jemand einmal hineingeschrieben hat, statt was tatsaechlich galt.
```

#### `manifest(ziel, dateien, hinweis)`

*Zeile 120 · 21 Zeilen*

_kein Docstring_

#### `main(argv)`

*Zeile 143 · 50 Zeilen*

_kein Docstring_


## `tools/aufraeumen.py`

*204 Zeilen · 7 Funktionen*

### Modulkopf

```text
Aufraeumer - entfernt Artefakte, die kein Skript des Repos mehr erzeugt.

NICHT TEIL DER ABGABE. Wie `pruefe_zahlen.py` ein Arbeitswerkzeug: Es erzeugt
kein Ergebnis und wird von keinem Skript in prep/, vorpruefung/ oder modelle/
importiert.

    python tools/aufraeumen.py              VORSCHAU - loescht nichts
    python tools/aufraeumen.py --wirklich   loescht

--------------------------------------------------------------------------
WOZU
--------------------------------------------------------------------------
`results/eignungspruefung/` enthaelt Abbildungen aus mindestens drei Fassungen
von `v2_eignung.py`. Zwei davon sind aktuell, neun stammen vom 27.07. und
03.08. und werden von keiner Codezeile mehr geschrieben.

Das ist nicht nur unordentlich, sondern eine Falle derselben Art, gegen die
`pruefe_zahlen.py` gebaut wurde: `02_linearitaet.png` und
`01_streudiagramme.png` sind byte-identisch - dieselbe Abbildung unter zwei
Namen. Wer in LaTeX den alten Namen einbindet, bekommt ohne Fehlermeldung ein
Bild vom Juli. Nur faellt es hier nicht durch einen Exit-Code auf, sondern erst
im gedruckten Dokument.

--------------------------------------------------------------------------
WIE DIE LISTE ENTSTEHT - und warum sie nicht hier steht
--------------------------------------------------------------------------
Die Namen der aktuellen Abbildungen sind NICHT in dieser Datei aufgezaehlt.
Sie werden aus dem Quelltext von `v2_eignung.py` gelesen: Was dort als
Zeichenkette mit Endung .png oder .md vorkommt, gilt als aktuell, alles andere
im Ordner als verwaist.

Der Grund ist derselbe wie ueberall in diesem Projekt: Eine Liste an zwei
Orten laeuft auseinander. Nennt jemand eine Abbildung in `v2_eignung.py` um,
zieht dieses Skript automatisch nach. Stuende die Liste hier, wuerde es beim
naechsten Lauf die neue Abbildung loeschen.

--------------------------------------------------------------------------
WAS BEWUSST NICHT GELOESCHT WIRD
--------------------------------------------------------------------------
`data/sample/*.csv` und `results/sffd_fire_incidents_report.pdf` sind in git
verzeichnet und stammen aus der Zeit vor der Pipeline. Sie werden von keinem
Skript gelesen, aber sie zu entfernen ist eine Entscheidung ueber den
Repo-Inhalt und braucht einen Commit - kein Aufraeumen. Sie werden nur
gemeldet.
```

### Funktionen

#### `erzeugte_namen(skript)`

*Zeile 74 · 12 Zeilen*

```text
Dateinamen, die dieses Skript schreibt - aus seinem Quelltext gelesen.

    Bewusst grob: Jede Zeichenkette mit passender Endung zaehlt, auch wenn sie
    nur gelesen und nicht geschrieben wird. Der Fehler geht damit in die
    sichere Richtung - im Zweifel bleibt eine Datei stehen, statt dass eine
    gebrauchte verschwindet.
```

#### `verwaiste_dateien()`

*Zeile 88 · 16 Zeilen*

```text
Dateien in den geprueften Ordnern, die kein Skript mehr erzeugt.
```

#### `pycache_ordner()`

*Zeile 106 · 19 Zeilen*

```text
Alle __pycache__. Sie werden beim naechsten Import neu angelegt.

    Der eigentliche Anlass sind die Reste geloeschter Module - `m01_eignung`,
    `m02_regression`, `m03_klassifikation`, `s3_baselines`. Zu denen gibt es
    keine .py mehr; ein `import m01_eignung` wuerde die alte .pyc trotzdem
    nicht laden, aber die Dateien behaupten eine Struktur, die es nicht gibt.
```

#### `leere_ordner()`

*Zeile 127 · 9 Zeilen*

_kein Docstring_

#### `groesse(pfad)`

*Zeile 139 · 4 Zeilen*

_kein Docstring_

#### `zeige(titel, eintraege)`

*Zeile 145 · 12 Zeilen*

_kein Docstring_

#### `main(argv)`

*Zeile 159 · 42 Zeilen*

_kein Docstring_

