# Kapitel 5 — Arbeitsstand

**Diese Datei ist die einzige Stelle, an der steht, wo wir stehen.**
Sie wird nach jedem Unterabschnitt fortgeschrieben. Angelegt 24.08.2026.

Zugehörige Dateien:

| Datei | Rolle |
|---|---|
| `docs/Kapitel5_Geruest.md` | inhaltliche Vorlage, Absatz für Absatz |
| `docs/kapitel5_abgrenzung.tex` | Kommentarblock vor `\section{Data Preparation}` |
| `docs/kapitel5-1_datenauswahl.tex` | 5.1, Stand wie in Overleaf |
| `docs/kapitel5-2_zusammenfuehrung.tex` | 5.2, Stand wie in Overleaf |
| `docs/kapitel5-2_gleitobjekte.tex` | Quelltext 1 zu 5.2 |
| `docs/kapitel5_listings.tex` | alle Codeausschnitte, Sammeldatei |
| `docs/literatur_kapitel5.bib` | fünf `.bib`-Einträge, an `literatur.bib` anhängen |
| `docs/03_STAND.md` | **jede Zahl kommt von dort und nur von dort** |

---

## 1. Der Ablauf, fünf Schritte je Unterabschnitt

1. **Ich schreibe** einen Unterabschnitt: fertiger Fließtext als LaTeX, mit
   gesetzten `\footcite`, Tabellen, Abbildungs- und Listing-Verweisen. Nicht
   mehr als einen pro Runde — sonst tippst du gegen einen Stapel an.
2. **Du tippst ihn in Overleaf ein** und kompilierst.
3. **Du validierst** inhaltlich und stilistisch und sagst mir, **was du
   geändert hast** — auch Kleinigkeiten. Ein Satz genügt: „gekürzt, den Absatz
   zur Bereinigung umgestellt, Fußnote 3 raus."
4. **Ich ziehe nach**: `Kapitel5_Geruest.md` bekommt den tatsächlichen Stand,
   diese Datei bekommt eine Zeile in Abschnitt 4, und alles, was daraus im
   Code oder in den Listings folgt, wird im selben Zug geändert.
5. **Erst dann** kommt der nächste Unterabschnitt.

**Warum Schritt 3 nicht ausfallen darf:** Wenn deine Änderungen nicht
zurückfließen, beschreibt das Gerüst nach zwei Runden einen Text, den es nicht
mehr gibt — und dann schreibe ich 5.4 gegen ein 5.1, das anders lautet. Das
ist der einzige Weg, auf dem dieser Ablauf kaputtgeht.

---

## 2. Zeitplan

| | Unterabschnitt | Umfang | Besonderheit |
|---|---|---|---|
| ~~Tag 1~~ | ~~5.1 Datenauswahl und Bereinigung~~ | 1,5 S., 5 Absätze | **fertig** |
| | ~~5.2 Zusammenführung der Datenquellen~~ | 1,0 S., 5 Absätze | **fertig** |
| | 5.3 Konstruktion der Merkmale und Zielgrößen | 2,0 S., 8 Absätze | Listings L2, L3; Formel im Fließtext; Zielgrößentabelle offen (G3) |
| **Tag 2** | 5.4 Analyserahmen und Baselines | 2,5 S., 9 Absätze | **der lange** — Listings L4, L5, L6, zwei Tabellen, Auflage D wörtlich |
| | Durchgang | – | Verweise, Blindabsätze, Zahlen gegen `03_STAND.md`, Formalia |

5.4 ist so groß wie 5.1 und 5.2 zusammen. Wenn Tag 1 eng wird, lieber 5.3 auf
den Morgen von Tag 2 schieben, als 5.4 anzuschneiden.

---

## 3. Status

| Unterabschnitt | geschrieben | eingetippt | validiert | Gerüst nachgezogen |
|---|---|---|---|---|
| 5.1 | ✅ 24.08. **Fassung 3** | ✅ 24.08. | ✅ 24.08. | ✅ 24.08. |
| 5.2 | ✅ 24.08. Fassung 1 | ✅ 24.08. | ✅ 24.08. | ✅ 24.08. |
| 5.3 | – | – | – | – |
| 5.4 | – | – | – | – |
| Abgrenzungsblock | ✅ 24.08. | – | – | – |
| Listings L1–L6 | ✅ 24.08. | – | – | – |
| `.bib` (5 Einträge) | ✅ 24.08. | – | – | – |

---

## 4. Änderungen am Gerüst — chronologisch

| Datum | Was |
|---|---|
| 24.08. | Gerüst angelegt, Variante C angenommen |
| 24.08. | G1 bestätigt: Variante C |
| 24.08. | G2 entschieden: 2.6 trägt den Verfügbarkeitsgrundsatz → in 5.1 nur der Verweis, **kein** zweiter `\footcite{Bergmeir2012}` |
| 24.08. | G3: Zielgrößentabelle wird gerade angefasst → 5.3 wird so geschrieben, dass der Text ohne sie trägt |
| 24.08. | G5 vertagt |
| 24.08. | **Rückmeldung zu Fassung 1:** Einstieg gehört nicht nach 5.1 → neuer Absatz 1 über die zentrale Festlegung in `config.py`; Absatz 2 positiv statt aufzählend; **Abbildung A0 entfällt ganz**; die zwei verworfenen Aggregationsebenen wandern nach 5.3 Absatz 8 |
| 24.08. | **5.1 Fassung 3:** Einstieg nach dem Bauplan von 5.2 umgebaut (Befund mit Zahl → was → warum → wo hinterlegt → Ergebnis mit Zahl). Schlusssatz nennt nur den Rahmen: 35 Stadtteile über 132 Monate |
| 24.08. | **Zahlenfehler abgefangen:** „719.989 Einsatzmeldungen **und** 35 Stadtteile über 132 Monate" stellte zwei verschiedene Bezugsmengen nebeneinander — 719.989 ist der entdoppelte Gesamtbestand ab 2003 über alle 41 Stadtteile, im Rahmen liegen 350.481. 719.989 steht jetzt nur noch in 5.2 |
| 24.08. | **5.2 geschrieben und übernommen.** Ohne Fußnote (alle Quellen in Kapitel 4 belegt); Überschrift ohne die Zahl „vier"; Kriminalitätsschnitt auf Januar 2018 datiert wie in 4.1 |
| 24.08. | **Beschriftungen der Codeausschnitte** auf eine Zeile gekürzt — Gegenstand und Fundstelle, keine Begründung. Gilt für alle acht in `kapitel5_listings.tex` |
| 24.08. | **Drei `\label` in Overleaf gesetzt:** `sec:modellbewertung` (2.6), `sec:datengrundlage` (4.1), `sec:eda` (4.2). Kapitel 4 hatte vorher gar keine |
| 24.08. | 5.1 Fassung 1 geschrieben. **Abweichung vom Gerüst:** die Angabe „23 Einsatzspalten, davon bleiben 6" ist nicht reproduzierbar und wurde durch die nachgezählte Kette 63 Felder / 15 angefordert / 10 gesperrt ersetzt |

---

## 5. Wenn diese Sitzung endet

Meine Erinnerung an das Gespräch endet mit der Sitzung. Die Dateien bleiben —
im Repo und im Projekt. Deshalb genügt in einer neuen Sitzung dieser Satz:

> Wir schreiben Kapitel 5 der Bachelorarbeit. Lies zuerst
> `docs/Kapitel5_Arbeitsstand.md`, dann `docs/Kapitel5_Geruest.md` und
> `docs/kapitel5_abgrenzung.tex`. Schreib den nächsten Unterabschnitt nach
> Abschnitt 3 der Arbeitsstandsdatei. Zahlen ausschließlich aus
> `docs/03_STAND.md`.

Solange Abschnitt 3 und 4 dieser Datei gepflegt sind, verliert eine neue
Sitzung nichts außer dem Tonfall.

---

## 6. Regeln, die für jeden Unterabschnitt gelten

Kurzfassung des Abgrenzungsblocks — beim Schreiben und beim Validieren
danebenlegen.

1. Jede Zahl steht in `docs/03_STAND.md`. Keine aus einer älteren Fassung.
2. Jede Zahl mit Bezugsmenge: 35 Stadtteile, 29 Entwicklungs-, 23
   Trainingsstadtteile von Fold 1.
3. Kapitel 5 nennt **keine Verteilungskennzahl**, Kapitel 4 **keine
   Rechenvorschrift**.
4. Jeder Abschnitt beginnt mit dem Befund aus Kapitel 3 oder 4, den er
   beantwortet — ein Halbsatz.
5. Absatzform: was gemacht wurde · warum · was dadurch nicht gilt.
6. Keine Chronologie. Die Endkonfiguration ist die gewollte; Reflexion
   konzentriert in Kapitel 8.
7. Zwischen `\section` und `\subsection` steht nichts.
8. Kein Verweis auf einen Anhang.
9. Formalia: keine Kursivschrift, keine Anführungszeichen, Module und
   Funktionen in `\texttt{}`.
