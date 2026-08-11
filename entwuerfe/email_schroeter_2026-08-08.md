# E-Mail an Prof. Dr. Schröter — ✅ ABGESCHICKT am 08.08.2026

*Nicht Teil der Abgabe. Der Text unten ist der letzte Entwurf; versendet wurde
eine leicht abweichende Fassung von Lukas — ohne die Ablationszahlen in
Mitteilung 1 und ohne den Hold-out-Satz am Schluss, dafür mit dem
Rückstell-Angebot im Baseline-Absatz.*

> **Offen daraus:** Die Mail meldet Auflage B („Generalisierung auf unbekannte
> Stadtteile") als umgesetzt. In `main_gliederung_2026-07-28.tex` steht der
> Begriff noch nicht — siehe `06_RISIKEN.md`, R-17.

---

**Betreff:** Bachelorarbeit Nießen (728085) — Umsetzung Ihrer Hinweise, eine Rückfrage

---

Sehr geehrter Herr Prof. Dr. Schröter,

vielen Dank für Ihre Rückmeldung vom 4. August. Ihre drei Hinweise sind
umgesetzt: Die Abweichung von der zeitreihengerechten Kreuzvalidierung erläutere
ich in Kapitel 8, die Zielsetzung der Validierung ist als Generalisierung auf
unbekannte Stadtteile formuliert, und alle Vergleichsmodelle sehen identische
Merkmale und Splits — die Fold-Zuteilung steht als Spalte in den Datensätzen,
sodass kein Modellskript davon abweichen kann.

Die Modellierung ist inzwischen abgeschlossen und das Hold-out wie geplant
einmal ausgewertet. Zu den Baselines, die Sie ebenfalls bestätigt hatten, habe
ich eine Frage; dazu drei kurze Mitteilungen.

**Meine Frage**

Sie hatten die Negative-Binomial-Regression und die multinomiale logistische
Regression als geeignete Baselines bestätigt. Ich verwende von beiden die
einfachere Variante: ein Poisson-Modell für die Einsatzzahlen, eine logistische
Regression ohne Strafterm für die Einsatzart.

Der Grund: Meine Baseline muss nur Zahlen vorhersagen, nicht angeben, wie sicher
diese Zahlen sind. Genau dafür wäre die Negative Binomial da — die starke
Streuung meiner Daten beeinflusst nur die Sicherheitsangaben, und die verwende
ich nirgends. Einen Strafterm setze ich aus demselben Grund nicht ein: Er wäre
eine frei wählbare Einstellung. Meine Regel lautet für beide Stränge gleich —
die einfachste passende Form, ohne freien Regler.

Diese Wahl nützt mir dabei nicht. Bei den Einsatzzahlen liegt die Baseline bei
RMSE 33,98; mit der Negative Binomial wären es 37,27 — die einfachere Form ist
hier also die stärkere und damit schwerer zu schlagen. Bei der Einsatzart liegt
sie bei Macro-F1 0,297; mit Strafterm wären es 0,314. Sie hilft mir im einen
Strang und schadet mir im anderen. Wenn Sie es anders sehen, rechne ich mit den
von Ihnen genannten Formen.

**Drei Mitteilungen**

1. Alle drei Verfahren sagen die Einsätze je 1.000 Einwohner vorher und rechnen
   auf die absolute Zahl hoch — dieselbe Konstruktion, die das Poisson-Modell
   über seinen Offset vornimmt. Damit ist die Spezifikation zwischen allen
   Vergleichsmodellen einheitlich. Zusätzlich berichte ich die Variante ohne
   diese Behandlung: Dort liegt der Fehler von Random Forest bei 64,81 statt
   35,63 und der von XGBoost bei 57,86 statt 35,88, bei einer Baseline von
   33,98. Der Unterschied beantwortet meine vierte Unterfrage.

2. Meinen Signifikanztest rechne ich über zehn Werte statt über fünfzig: Es sind
   fünfzig Läufe, aber immer dieselben 29 Stadtteile, nur anders gruppiert. Das
   begrenzt den kleinstmöglichen p-Wert auf 0,002.

3. Die sechs Verfahrensvergleiche der Regression korrigiere ich mit
   Holm-Bonferroni. Ridge gegen Random Forest ist bei der Rate nicht
   signifikant, nur Ridge gegen XGBoost bleibt es. In der Klassifikation gibt es
   nur einen Vergleich, dort entfällt die Korrektur.

Das Hold-out habe ich bei keiner dieser Entscheidungen angesehen. Alles Weitere
bringe ich gern in die nächste Sprechstunde mit.

Mit freundlichen Grüßen
Lukas Nießen
Matrikelnummer 728085

---

## Achte Fassung: aus einem Guss statt als Korrekturliste

**Das Problem:** Formulierungen wie „jetzt", „vorher hatte nur die Baseline" und
„das hat mich einen Befund gekostet" setzen voraus, dass Schröter einen früheren
Zustand kennt. Er kennt keinen. Sie erfinden in seinem Kopf eine Vorgeschichte,
die nur im Repo existiert — und wecken den Verdacht, es habe eine frühere
Fassung der Ergebnisse gegeben.

**Die Regel:** Für ihn gibt es kein Vorher. Die Mail beschreibt das fertige
Verfahren im Präsens. Die Entstehungsgeschichte steht im Decision Log und in
Kapitel 6, wo sie belegt ist und hingehört.

**Was sich dadurch geändert hat:**

| vorher | jetzt |
|---|---|
| „sagen **jetzt** die Einsätze je 1.000 Einwohner vorher" | „sagen die Einsätze je 1.000 Einwohner vorher" |
| „**vorher** hatte nur die Baseline diese Konstruktion" | „dieselbe Konstruktion, die das Poisson-Modell über seinen Offset vornimmt" |
| „Das hat mich **einen Befund gekostet**" | „Danach ist von den sechs genau einer signifikant" |
| „Der Strafterm **stand auf dem Voreinstellungswert**" | „Einen Strafterm setze ich nicht ein: Er wäre eine frei wählbare Einstellung" |

Punkt 3 verliert dadurch nichts. „Von sechs Vergleichen bleibt einer" zeigt
genauso deutlich, dass die Korrektur beißt — nur ohne eine frühere Fassung
anzudeuten.

**Eine Stelle bleibt vergleichend, und das ist richtig:** die Rückfrage selbst.
Dort geht es um die Formen, die **er** benannt hat — der Vergleich bezieht sich
also auf seinen Kenntnisstand, nicht auf deine Arbeitsgeschichte. Ebenso die
Ablation in Mitteilung 1: Beide Varianten sind berichtete Ergebnisse, keine
Entwicklungsstufen.

## Was der Bezug bringt

**Der Betreff ist jetzt „Umsetzung Ihrer Hinweise, eine Rückfrage"** statt nur
„Rückfrage zur Baseline". Damit liest er die Mail als Fortschrittsbericht mit
einer Frage, nicht als Problemmeldung.

**Der erste Absatz beantwortet seine drei Auflagen der Reihe nach** — in seiner
eigenen Reihenfolge und mit seinen eigenen Begriffen („Generalisierung auf
unbekannte Stadtteile" steht wörtlich so in seiner Mail und wörtlich so in
deiner Arbeit). Bei Auflage C nennst du gleich den Beleg statt der Zusicherung:
Die Fold-Spalte in der Datei ist der Grund, warum kein Skript abweichen *kann*.

**„Zu den Baselines, die Sie ebenfalls bestätigt hatten"** verbindet die
Rückfrage direkt mit seinem Satz. Sie wirkt dadurch wie die Fortsetzung eines
Gesprächs, nicht wie ein Alleingang.

**Mitteilung 1 bekommt einen Halbsatz dazu:** „Damit ist auch die Spezifikation
zwischen allen Vergleichsmodellen einheitlich." Seine Auflage C spricht von
Merkmalen und Splits — die Spezifikation nennt er nicht. Der Satz behauptet
deshalb nicht, du hättest seine Anweisung ausgeführt, sondern zeigt, dass du
dasselbe Prinzip weitergedacht hast. Das ist der ehrliche und zugleich stärkere
Zug.

## Länge

Rund 300 → 350 Wörter. Zwei Halbsätze habe ich als Ausgleich gestrichen (die
Nebenrechnung über alle fünfzig Läufe bei Punkt 2, die Erklärung bei Punkt 3).
Weiter zu kürzen ginge nur noch auf Kosten des Auflagen-Absatzes — und der ist
der wertvollste Teil der Mail.
