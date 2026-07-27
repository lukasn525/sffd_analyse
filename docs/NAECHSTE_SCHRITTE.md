# Nächste Schritte – Data Preparation dauerhaft korrekt

> **Hinweis (2026-07-27):** Dieses Dokument nennt teilweise die alten Dateinamen
> (`pipeline/`, `modellierung/`, `analyse/`). Seit dem Struktur-Umbau liegt die
> gesamte Aufbereitung in `prep/`, die Modellskripte in `modelle/`. Die Zuordnung
> alt → neu steht in `ORIENTIERUNG.md`, Abschnitt 5 (Decision Log #22). Inhaltlich
> gilt alles Folgende unverändert weiter.

Stand 2026-07-26. Diese Datei ist bewusst in einfacher Sprache geschrieben und
beschreibt, was zu tun ist und **warum**. Technische Details stehen in
`CLAUDE.md` (Decision Log) und `docs/PREPROCESSING_AUDIT_2026-07-26.md`.

---

## Schritt 1 – Du lädst die Polizeidaten neu (ca. 15–30 Min, einmalig)

Das ist der einzige Schritt, den ich nicht für dich erledigen kann, weil aus
meiner Umgebung kein Zugriff auf DataSF möglich ist.

In PowerShell im Projektordner, Zeile für Zeile:

```powershell
cd C:\Users\lukas\Git\sffd_analyse
.\venv\Scripts\Activate.ps1

python pipeline\01_fetch.py test        # 1) Erreichbarkeit prüfen
python pipeline\01_fetch.py crime       # 2) beide Polizeidatensätze laden
python pipeline\02_join.py              # 3) Kriminalitätsindex + Joins
python pipeline\03_features.py          # 4) Merkmale berechnen

python modellierung\aggregation.py      # 5) Selbsttest Panel
python modellierung\cv.py               # 6) Selbsttest Zeitschnitte
python modellierung\demo_modellierung.py  # 7) Demo neu rechnen
```

Die Schalter in `01_fetch.py` müssen **nicht** mehr von Hand editiert werden –
das Argument `crime` setzt `DOWNLOAD_CRIME` und `DOWNLOAD_CRIME_HISTORISCH` nur
für diesen einen Lauf. Weitere Argumente: `sffd`, `acs`, `crosswalk`, `landuse`,
`neighborhoods`, `alle`.

**Warum zwei Datensätze?** Der Polizeidatensatz, den du bisher benutzt hast,
beginnt erst 2018. Dein Analysezeitraum beginnt 2015. Für 2015–2017 gab es
schlicht keine Kriminalitätsdaten – deshalb war das Merkmal bisher ein einziger
fester Wert pro Stadtteil für alle Jahre. Der zweite Datensatz schließt diese
Lücke.

**Was passiert, wenn du es nicht machst?** Die Pipeline bricht ab und sagt dir,
welche Datei fehlt. Das ist Absicht: Vorher ist sie stillschweigend auf die
fehlerhafte statische Variante zurückgefallen, und genau solche stillen
Rückfälle sind es, die man in der Verteidigung nicht erklären kann.

---

## Schritt 2 – Was beim Kriminalitätsindex passiert (nur zum Verstehen)

Bisher war das Merkmal „Anteil Gewaltdelikte an allen Delikten", einmal über den
gesamten Zeitraum berechnet. Drei Probleme auf einmal:

1. **Es war überall gleich.** Ein Stadtteil hatte 2015 denselben Wert wie 2025.
   Ein Merkmal ohne Zeitverlauf kann nichts über zeitliche Entwicklung aussagen.
2. **Es enthielt die Zukunft.** In den Wert für 2015 flossen auch Delikte aus
   2024 ein. Ein Modell, das 2015 prognostizieren soll, hätte diese Information
   damals nicht gehabt.
3. **Es maß das Falsche.** Ein Anteil sagt, *welche Art* von Delikten passiert,
   nicht *wie viele*. Ein sehr sicherer und ein sehr unsicherer Stadtteil können
   denselben Anteil haben.

Neu ist ein **relativer Kriminalitätsindex** pro Stadtteil und Monat:

> Wie viele Straftaten kamen im Stadtteil in den letzten 12 Monaten auf je
> 1.000 Einwohner – im Verhältnis zum Stadtdurchschnitt derselben 12 Monate?

- **1,0** = so belastet wie San Francisco im Schnitt
- **2,0** = doppelt so belastet
- **0,5** = halb so belastet

Das Zeitfenster endet immer im **Vormonat**. Für Januar 2015 werden also die
Delikte von Januar bis Dezember 2014 gezählt – nichts, was in der Zukunft liegt.

**Warum relativ und nicht einfach „Delikte je 1.000 Einwohner"?** Im Mai 2018 hat
die Polizei ihr Erfassungssystem gewechselt. Dadurch springt die Zahl der
erfassten Fälle stadtweit, ohne dass sich die Kriminalität wirklich geändert
hätte. Bei einer absoluten Rate hättest du diesen Sprung mitten in deinen Daten
und das Modell würde ihn für ein echtes Signal halten. Beim Verhältnis zum
Stadtdurchschnitt kürzt sich der Sprung heraus, weil er Zähler und Nenner
gleichermaßen trifft. Die absolute Rohrate bleibt trotzdem als Spalte erhalten –
aber nur für beschreibende Statistik in Kapitel 5.1, nicht als Modellmerkmal.

**Was du in Kapitel 6 als Limitation schreiben musst:** Der Systemwechsel kürzt
sich nur heraus, soweit er alle Stadtteile gleich betrifft. Wenn sich die
*Zusammensetzung* der erfassten Delikte verschoben hat und das einzelne
Stadtteile stärker trifft, bleibt ein Rest. Das lässt sich prüfen, indem du den
Index über den Übergang 2017 → 2019 anschaust: springen einzelne Stadtteile
auffällig, ist der Effekt vorhanden.

---

## Schritt 3 – Was danach noch offen ist

Nach Schritt 1 ist die Data Preparation in dem Sinne „fertig", dass sie kein
Leakage mehr enthält und reproduzierbar ist. Für **langfristig korrekt** fehlen
noch diese Punkte, in dieser Reihenfolge:

### 3.1 Eignungsprüfung neu rechnen (ca. 1 Std.)

`results/eignungspruefung/` stammt vom 18.07. und beruht auf Daten, die es so
nicht mehr gibt. Zwei Dinge müssen dabei anders gemacht werden als damals:

- **Nur auf Trainingsdaten rechnen**, nicht auf dem Gesamtdatensatz. VIF und
  Linearitätsprüfung sind Teil der Modellwahl – wer sie auf allen Daten rechnet,
  schaut auf das Testfenster.
- **Mit den neuen Merkmalen**, also mit `log_bevoelkerung` und
  `kriminalitaetsindex` statt der alten Spalten.

Das ist deine formale Begründung dafür, dass Ridge überhaupt zulässig ist –
Schröters ausdrücklicher Prüfpunkt.

### 3.2 Klassifikationsteil aufsetzen (ca. 1 Tag)

Bisher existiert dafür **kein Code**. Drei Dinge sind zu entscheiden bzw. zu
schreiben:

- Zielgröße `ist_brand` (NFIRS-Code beginnt mit „1"), Ebene Einzeleinsatz.
- `bataillon` und die Zeitmerkmale müssen kodiert werden (One-Hot), und zwar
  **identisch für alle drei Verfahren**, damit der Vergleich fair bleibt. Das
  gehört in einen `ColumnTransformer` innerhalb der Modell-Pipeline, nicht davor.
- **Das Pseudo-Signal-Problem muss im Text stehen.** Alle Einsätze desselben
  Stadtteils tragen dieselben Stadtteilmerkmale. Bei 700.000 Einsätzen und 38
  Stadtteilen wiederholen sich diese Werte tausendfach. Das Modell wirkt dadurch
  besser, als es ist. Das ist keine Katastrophe, aber es muss benannt werden,
  sonst wird es dir in der Verteidigung vorgehalten.

### 3.3 Zwei Sensitivitätsanalysen (je ca. 2 Std.)

- **Zielgröße als Rate:** einmal alles mit „Einsätze je 1.000 Einwohner" statt
  absoluten Zahlen rechnen. Ein Absatz im Ergebnisteil reicht. Grund: Die
  Vorzeichen einiger Zusammenhänge hängen an dieser Wahl, und das solltest du
  gezeigt haben, bevor jemand danach fragt.
- **Modell mit Stadtteil-ID:** einmal zusätzlich die Stadtteil-Identität als
  Merkmal aufnehmen. Die Differenz der Gütemaße ist ein direktes Maß dafür,
  wie viel deine Merkmale *nicht* erklären – ein starkes Argument im
  Limitationen-Kapitel.

### 3.4 Dokumentation für Kapitel 5.2 (ca. 1 Tag)

- **Merkmalstabelle**: pro Spalte Quelle, Ebene, zeitliche Auflösung, ab wann
  real verfügbar, Anteil fehlender Werte, Berechnungsformel.
- **Join-Protokoll**: Zeilenzahl vor und nach jedem Verknüpfungsschritt, Anteil
  verworfener Einsätze, Plausibilitätscheck „Summe der Stadtteilbevölkerungen ≈
  Einwohnerzahl von San Francisco".
- **Ein Satz zur Fairness**, wörtlich: Rohdaten, Splits und Gütemaße sind für
  alle drei Verfahren identisch; modellspezifische Aufbereitung (Standardisierung
  und Log-Transformation für Ridge) läuft ausschließlich innerhalb der jeweiligen
  Pipeline pro Fold.

### 3.5 Leitfaden korrigieren (15 Min)

In `docs/UMSETZUNGSLEITFADEN_MODELLIERUNG.md` steht unter **A2**, der Einbruch im
letzten Fold sei ein Zeichen von Nichtstationarität. Das war falsch – es war ein
unvollständiger Randmonat (Januar 2026 mit 258 statt 3.300 Einsätzen). Der
Abschnitt muss umgeschrieben werden, sonst steht in deiner Arbeit später eine
Erklärung für ein Phänomen, das es nicht gibt.

---

## Was ab jetzt dauerhaft feststeht

Diese Dinge sind als Konstanten im Code fixiert und ändern sich nicht mehr,
egal wie oft du die Daten neu lädst:

| Festlegung | Wert | Wo im Code |
|---|---|---|
| Analysezeitraum | 2015-01 bis 2025-12 (132 Monate) | `aggregation.py`: `START`, `ENDE` |
| Stadtteile | 35 | s. zwei Zeilen tiefer |
| … ohne ACS-Abdeckung | Treasure Island, Lakeshore, Mission Bay | `aggregation.py`: `balanciertes_panel()` |
| … ohne Wohnbevölkerung | Golden Gate Park, Lincoln Park, McLaren Park | `aggregation.py`: `PARKGEBIETE` |
| Beobachtungen | 4.620 (rechteckiges Panel, keine Lücken) | Selbsttest in `aggregation.py` |
| ACS-Versatz | 1 Jahr | `02_join.py`: `ACS_PUBLIKATIONS_LAG` |
| Crime-Fenster | 12 Monate, endend im Vormonat | `02_join.py`: `CRIME_FENSTER_MONATE` |
| End-Hold-out | 2025-01 bis 2025-12, beim Tuning unberührt | `cv.py`: `split_holdout()` |
| CV-Folds | 3 Folds à 12 Testmonate, expanding window | `cv.py`: `zeit_folds()` |

Jede dieser Dateien hat einen Selbsttest: `python modellierung/aggregation.py`
und `python modellierung/cv.py` prüfen automatisch, ob Panel, Zeitraum und
Splits noch stimmen. Wenn du künftig Daten neu lädst und etwas nicht mehr passt,
schlagen die Tests an, statt dass es unbemerkt durchläuft.
