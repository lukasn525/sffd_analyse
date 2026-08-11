# tools/ — Arbeitswerkzeuge, **nicht Teil der Abgabe**

Dieser Ordner wird vor dem Packen des Abgabe-ZIP **gelöscht**. Er erzeugt keine
Ergebnisse und wird von keinem Skript in `prep/`, `vorpruefung/` oder
`modelle/` importiert. Nichts in `results/` und nichts in der Thesis hängt
davon ab.

```
python tools/pruefe_zahlen.py        # Zahlenwächter, Exit-Code 0 = sauber
python tools/pruefe_zahlen.py -v     # zusätzlich die bestandenen Prüfungen

python tools/aufraeumen.py           # Aufräumer, VORSCHAU — löscht nichts
python tools/aufraeumen.py --wirklich
```

---

# `pruefe_zahlen.py` — der Zahlenwächter

## Wozu

`CLAUDE.md` legt fest: Jede Ergebniszahl steht in `docs/03_STAND.md` und nur
dort. Diese Regel hält genau so lange, wie jemand sie nach jedem Lauf von Hand
nachzieht — und am 07.08.2026 hat sie nicht gehalten. Abschnitt 4 berichtete
die Negative Binomial (RMSE 37,27), Abschnitt 5 derselben Datei das Poisson-GLM
(33,98); `06_RISIKEN.md` empfahl in R-9 das Gegenteil dessen, was Decision Log
#43 umgesetzt hatte; die Laufzeiten in §5.4 stammten aus einem früheren Lauf.

Kein Rechenfehler, sondern Drift: Eine Zahl lebte an zwei Orten, und nur einer
wurde gepflegt. Dagegen hilft keine Sorgfalt, sondern ein Exit-Code.

## Wie zu lesen

| Befund | Bedeutung |
|---|---|
| **FEHLER** | Der Sollwert aus `results/` steht nicht an der geforderten Stelle. Exit-Code 1. |
| **ALTLAST** | Ein früher gültiger Wert steht noch da, ohne Rückblick-Markierung und ohne den heutigen Wert daneben. Warnung — historische Verweise („von 37,27 auf 33,98") sind erwünscht und werden erkannt. |
| **HINWEIS** | Struktur passt nicht: Überschrift umbenannt, Tabelle umgebaut, Datei fehlt. Die betroffene Prüfung wurde **übersprungen** — das ist die gefährlichste Kategorie, weil sie stillschweigend Deckung verliert. |

**Bei einem Fehler wird die Dokumentation nachgezogen, nicht die Prüfung
angepasst.** Die Ergebnisdateien sind die Wahrheit.

## Wann laufen lassen

Nach jedem Lauf von `v1_baselines.py`, `v3_spezifikation.py`, `m02`, `m03`,
`m04` — und einmal unmittelbar vor der Abgabe.

## Was er prüft

113 Wertprüfungen und vier Strukturprüfungen. Jede Wertprüfung liest ihren
Sollwert bei jedem Lauf **neu aus `results/`**, nie aus einem Dokument, und
sucht ihn zeilengenau: nicht „steht die Zahl irgendwo im Kapitel", sondern
„steht sie in der Tabellenzeile, die zu diesem Verfahren gehört".

Abgedeckt sind beide Baselines, alle Modellergebnisse beider Stränge, die
Wilcoxon-Differenzen und p-Werte, das Hold-out, die Ablation, die
Spezifikationsgegenprobe, Extrapolation, VIF und die Faktorgruppen — dazu die
Zahlen, die in `06_RISIKEN.md` eine Risikoeinstufung tragen.

Die vier Strukturprüfungen fangen ab, was keine einzelne Zahl ist:

- **Verhältnisse** — „Ridge ist N-mal schneller als …". Solche Sätze altern
  unbemerkt mit; N wird nachgerechnet.
- **Negative Vorhersagen** — die Aussage „keine, in keinem der 300 Läufe" muss
  gelten, nicht gehofft sein.
- **Signifikanzmuster** — welche Paarungen signifikant sind, ist in Kapitel 7
  und in R-1/R-2 festgeschrieben. Ändert sich das Muster, bricht die Prüfung.
- **Hold-out-Einmaligkeit** — je Verfahren und Zielgröße genau eine Zeile.
  Mehr hieße, das Hold-out wurde mehrfach ausgewertet.

## Grenzen

Der Wächter prüft **Zahlen und Struktur, keine Aussagen**. Ob „nicht
unterscheidbar" die richtige Schlussfolgerung ist, ob eine Begründung trägt und
ob ein Risiko richtig eingestuft ist, sieht er nicht. Er verhindert Drift, er
ersetzt kein Lesen.

Gegengeprüft am 08.08.2026: Fünf absichtlich eingebaute Fehler — falsche
Baseline in der Tabelle, verdrehte RMSE- und R²-Werte, ein falscher
Hold-out-Wert und eine umbenannte Überschrift — wurden alle gefunden, die
Überschrift korrekt als HINWEIS statt als stiller Durchläufer.

---

# `aufraeumen.py` — der Aufräumer

Entfernt Artefakte, die **kein Skript des Repos mehr erzeugt**. Ohne Argument
nur Vorschau; gelöscht wird erst mit `--wirklich`.

## Wozu

Dieselbe Fehlerklasse wie beim Zahlenwächter, nur eine Ebene tiefer.
`results/eignungspruefung/` enthält Abbildungen aus drei Fassungen von
`v2_eignung.py`: zwei aktuelle, neun aus Juli und Anfang August.

Der Anlass ist nicht Ordnungsliebe. `02_linearitaet.png` und
`01_streudiagramme.png` sind **byte-identisch** — dieselbe Abbildung unter zwei
Namen. Wer in LaTeX den alten Namen einbindet, bekommt ohne Fehlermeldung ein
Bild vom 27.07. Der Zahlenwächter sieht das nicht; es fällt erst im gedruckten
Dokument auf, wenn überhaupt.

## Wie die Liste entsteht

Die Namen der aktuellen Abbildungen stehen **nicht** im Aufräumer. Er liest sie
aus dem Quelltext von `v2_eignung.py`: Was dort als Zeichenkette mit Endung
`.png` oder `.md` vorkommt, gilt als aktuell — alles andere im Ordner als
verwaist.

Grund ist dieselbe Regel wie überall im Projekt: Eine Liste an zwei Orten läuft
auseinander. Wird eine Abbildung in `v2_eignung.py` umbenannt, zieht der
Aufräumer automatisch nach. Stünde die Liste in `tools/`, würde er beim nächsten
Lauf die neue Abbildung löschen.

Die Erkennung ist bewusst großzügig: Jede passende Zeichenkette zählt, auch
gelesene Dateien. Der Fehler geht damit in die sichere Richtung — im Zweifel
bleibt eine Datei stehen.

## Was er anfasst

| Gruppe | Inhalt |
|---|---|
| Verwaiste Ergebnisdateien | `results/eignungspruefung/` — 9 PNG und `eignungspruefung_summary.md` |
| Bytecode-Caches | alle `__pycache__/`, darunter Reste gelöschter Module (`m01_eignung`, `m02_regression`, `m03_klassifikation`, `s3_baselines`) |
| Leere Ordner | `.dist/`, `data/interim/` — nur wenn tatsächlich leer |

## Was er bewusst **nicht** anfasst

`data/sample/*.csv` und `results/sffd_fire_incidents_report.pdf` sind in git
verzeichnet und stammen aus der Zeit vor der Pipeline. Kein Skript liest sie,
aber sie zu entfernen ist eine Entscheidung über den Repo-Inhalt und braucht
einen Commit — kein Aufräumen. Der Aufräumer meldet sie nur.

## Gegenprobe

Nach `--wirklich` legt `python vorpruefung/v2_eignung.py` die beiden aktuellen
Abbildungen neu an; die `__pycache__` entstehen beim nächsten Import von selbst.
