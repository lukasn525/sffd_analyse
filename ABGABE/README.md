# Vorhersage von Feuerwehreinsätzen in San Francisco

Code zur Bachelorarbeit „Vorhersage von Feuerwehreinsätzen mittels Machine
Learning, ein Verfahrensvergleich am Beispiel der Stadtteile San Franciscos“
(FOM Hochschule, B.Sc. Wirtschaftsinformatik).

Verglichen werden Ridge Regression, Random Forest und XGBoost auf
Stadtteildaten. Analyseeinheit ist der Stadtteil je Monat: 36 Stadtteile über
132 Monate (Januar 2015 bis Dezember 2025). Zwei Stränge werden untersucht:

- **Menge:** Zahl der Einsätze und Einsätze je 1.000 Einwohner (Regression,
  alle drei Verfahren)
- **Struktur:** überwiegende Einsatzart (Klassifikation, Random Forest und
  XGBoost)

Referenz sind in beiden Strängen zwei Baselines: ein Modell ohne Merkmale und
ein verallgemeinertes lineares Modell ohne Hyperparameter (Poisson-GLM mit
Offset bzw. multinomiale logistische Regression).

## Umgebung

Gerechnet wurde unter Windows mit Python 3.14. `requirements.txt` hält die
Versionen aller Pakete fest, mit denen die Ergebnisse entstanden sind.

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Daten

`data/raw/` enthält die Rohdaten in dem Stand, in dem sie abgerufen wurden:
aus DataSF die Einsätze der Feuerwehr, die Kriminalität, die Flächennutzung,
die Stadtteilgrenzen und die Zuordnung der Census Tracts zu den Stadtteilen,
vom Census Bureau die American Community Survey. Von den Einsatzmeldungen der
Feuerwehr enthält `fire_incidents.parquet` nur diejenigen mit Stadtteil,
erfasster Ankunft und einer Antwortzeit von 0 bis 60 Minuten. Die übrigen
verwirft die Aufbereitung ohnehin.

Die Aufbereitung rechnet ohne Netzzugang allein aus diesem Stand. Geladen wird
nur, wenn ein `DOWNLOAD_*`-Schalter in `prep/config.py` auf `True` steht. Ein
neuer Abruf überschreibt die Dateien in `data/raw/` mit dem aktuellen Stand der
Portale; die Zahlen der Arbeit lassen sich danach nicht mehr nachrechnen.

Die Abfrage der American Community Survey erwartet einen Census-API-Schlüssel.
Er liegt der Abgabe nicht bei. Beantragt wird er kostenlos unter
<https://api.census.gov/data/key_signup.html>, gesetzt vor dem Lauf in der
Umgebungsvariable `CENSUS_API_KEY`:

```bash
set CENSUS_API_KEY=<eigener Schlüssel>          # Eingabeaufforderung
$env:CENSUS_API_KEY = "<eigener Schlüssel>"     # PowerShell
```

`data/processed/` entsteht durch `prep/build.py`.

## Ablauf

Die Reihenfolge ist verbindlich, weil spätere Schritte die Ergebnisse
früherer lesen.

```bash
python prep/build.py                        # Aufbereitung: zwei Analysedatensaetze
python tests/test_aufbereitung.py           # Pruefungen an den erzeugten Dateien
python prep/rohbefunde.py                   # Befunde zu den Rohquellen
python prep/deskriptiv.py                   # Beschreibung des Panels
python prep/codebook.py                     # Merkmalstabelle
python vorpruefung/v0_aufteilung.py         # Selbsttest der wiederholten Aufteilung
python vorpruefung/panelprofil.py           # Profil der Entwicklungs- und Hold-out-Stadtteile
python vorpruefung/run.py                   # Baselines und Eignungspruefung
python vorpruefung/v3_spezifikation.py      # Gegenprobe zur Eignungspruefung
python modelle/m02_menge.py holdout         # Regression: Tuning, Kreuzvalidierung, Schlussbewertung
python modelle/m03_struktur.py holdout      # Klassifikation: Tuning, Kreuzvalidierung, Schlussbewertung
python vorpruefung/v4_decke.py              # Obergrenzen des Strukturstrangs
python vorpruefung/v4_decke.py holdout      # dieselben Obergrenzen inklusive Hold-out
python modelle/suchdiagnose.py              # Reicht das Budget der Hyperparametersuche?
python modelle/parametersensitivitaet.py    # Spannen und Kreuzprobe der Parametersaetze
python modelle/trennschaerfe.py             # Trennschaerfe der gepaarten Tests
python modelle/m04_shap.py                  # Faktorgruppen, Ablation, VIF
```

Hinweise zum Ablauf:

- **`holdout`:** Sechs Stadtteile sind als Hold-out zurückgehalten. Ohne das
  Argument `holdout` sind sie für den Code unerreichbar. Es gehört nur in den
  Lauf, der die Schlussbewertung einmalig ausführt.
- **`vorpruefung/run.py`** startet `v1_baselines.py` und `v2_eignung.py`
  nacheinander. Die Eignungsprüfung liest die Werte der Baselines.
- **Laufzeit:** Ein vollständiger Lauf ohne `suchdiagnose.py` dauert auf der
  in der Arbeit genannten Maschine rund drei Stunden, den größten Teil davon
  `m02_menge.py` und `m03_struktur.py`. `suchdiagnose.py` wiederholt die
  gesamte Hyperparametersuche und dauert zusätzlich mehrere Stunden. Mit
  `--test` läuft sie als kurzer Probelauf.
- **Wiederaufnahme:** `m02_menge.py holdout --weiter` und
  `m03_struktur.py holdout --weiter` übernehmen Tuning und Kreuzvalidierung
  aus `results/`, statt sie neu zu rechnen.

Einzelschritte:

```bash
python prep/s1_daten.py join            # nur die Zusammenfuehrung, ohne Download
python prep/s2_datensaetze.py splits    # Aufteilung in Folds und Hold-out anzeigen
python vorpruefung/v1_baselines.py      # nur die Baselines
python vorpruefung/v2_eignung.py        # nur die Eignungspruefung
```

## Reproduzierbarkeit

Alle Zufallsschritte verwenden den Startwert `RANDOM_STATE = 42` aus
`modelle/config_modelle.py`. Ein Wiederholungslauf reproduziert Gütemaße,
Hyperparameter, Baselines, SHAP-Beiträge und die Spezifikationsgegenprobe
exakt. Nicht reproduzierbar sind die Laufzeiten und der Vergleich zwischen
einkernigem und parallelem Rechnen, weil XGBoost über mehrere Kerne nicht in
jedem Lauf dieselbe Vorhersage liefert. Bewertet wird deshalb auf einem Kern.

Alle Verfahren erhalten dieselben Zeilen, dieselben Merkmale und dieselbe
Aufteilung. Die Zuordnung zu Folds und Hold-out steht als Spalte `fold` bzw.
`ist_holdout` in den Analysedateien.

## Aufbau

```
prep/              Aufbereitung und Beschreibung der Daten
  config.py          Konstanten der Aufbereitung
  s1_daten.py        Rohdaten zusammenfuehren, ein Einsatz je Zeile
  s2_datensaetze.py  Aggregation auf Stadtteil x Monat, Aufteilung
  build.py           fuehrt s1 und s2 in einem Befehl aus
  rohbefunde.py, deskriptiv.py, codebook.py   beschreiben die Daten
vorpruefung/       Aufteilung, Baselines, Eignungspruefung, Obergrenzen
modelle/           Hyperparametersuche, Kreuzvalidierung, Tests,
                   Schlussbewertung und Interpretation
  config_modelle.py  Suchraeume, Budget, Wiederholungen, Startwert
tests/             Pruefungen der Aufbereitung
docs/              Datenherkunft, Datenfluss, Ergebnisverzeichnis
data/raw/          Rohdaten
data/processed/    erzeugte Analysedateien
results/           Ergebnisse als Tabellen und Berichte
```

Die beiden Analysedateien sind `data/processed/regression.parquet` und
`data/processed/klassifikation.parquet`. Beide liegen auf der Einheit
Stadtteil je Monat vor, ohne fehlende Werte.

## Dokumentation

`docs/` ergänzt die README um vier kurze Nachschlagedateien:

- `datenherkunft.md`: Portal, Abrufdatum, Filter und Nutzungsbedingungen
  jeder Rohdatei, dazu die bekannten Fehler in den Rohdaten
- `datenfluss.md`: welches Skript welche Dateien liest und schreibt
- `ergebnisverzeichnis.md`: welche Datei in `results/` hinter welcher Tabelle
  der Arbeit steht
- `codebook.md`: Verweis auf das Codebook in `results/codebook/`, das
  `prep/codebook.py` aus den Analysedateien erzeugt
