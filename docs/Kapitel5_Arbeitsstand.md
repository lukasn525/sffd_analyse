# Kapitel 5 — Arbeitsstand

> **ÜBERHOLT (31.08.2026).** Maßgeblich sind `03_STAND.md`, `04_MODELLIERUNG.md`,
> `06_RISIKEN.md`, `07_BEFUNDE.md` und die eingereichte `main.tex`. Zahlen und
> Zeilennummern in dieser Datei stammen aus der Zeit VOR der Crosswalk-Korrektur
> vom 29.08.2026 (35 statt 36 Stadtteile, vollständiger Neulauf) und wurden
> bewusst NICHT nachgezogen: Die Datei ist als datierter Entwurf Teil der
> Arbeitsdokumentation.

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
| `docs/kapitel5-3_merkmale.tex` | 5.3, Stand wie in Overleaf |
| `docs/kapitel5-3_gleitobjekte.tex` | Quelltexte 2/3 und Merkmalstabelle zu 5.3 |
| `docs/kapitel5-4_rahmen_baselines.tex` | 5.4, Fassung 1 |
| `docs/kapitel5-4_gleitobjekte.tex` | Abbildung, 7 Quelltexte, 3 Tabellen zu 5.4 |
| `docs/literatur_kapitel5-4_killip.bib` | sechster `.bib`-Eintrag, an `literatur.bib` anhängen |
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
| | ~~5.3 Konstruktion der Merkmale und Zielgrößen~~ | 2,0 S., 10 Absätze | **eingetippt**, Restpunkte: mehr Code, 1–2 sprachliche Anpassungen |
| **Tag 2** | 5.4 Analyserahmen und Baselines | **4,5–5,5 S.**, 16 Absätze | **geschrieben 25.08.** — 7 Quelltexte, 3 Tabellen, 1 Abbildung, Auflagen A–D |
| | Durchgang | – | Verweise, Blindabsätze, Zahlen gegen `03_STAND.md`, Formalia |

5.4 ist so groß wie 5.1 und 5.2 zusammen. Wenn Tag 1 eng wird, lieber 5.3 auf
den Morgen von Tag 2 schieben, als 5.4 anzuschneiden.

---

## 3. Status

| Unterabschnitt | geschrieben | eingetippt | validiert | Gerüst nachgezogen |
|---|---|---|---|---|
| 5.1 | ✅ 24.08. **Fassung 3** | ✅ 24.08. | ✅ 24.08. | ✅ 24.08. |
| 5.2 | ✅ 24.08. Fassung 1 | ✅ 24.08. | ✅ 24.08. | ✅ 24.08. |
| 5.3 | ✅ 24.08. Fassung 1 | ✅ 25.08. | teilweise — 2 Restpunkte | ✅ 25.08. |
| 5.4 | ✅ 25.08. Fassung 1 | – | – | – |
| Abgrenzungsblock | ✅ 24.08. | – | – | – |
| Listings L1–L6 | ✅ 24.08. | – | – | – |
| `.bib` (5 Einträge) | ✅ 24.08. | ✅ 24.08. **in `literatur.bib`** | – | – |
| `.bib` Killip2004 | ✅ 25.08. | – | – | – |

---

## 4. Änderungen am Gerüst — chronologisch

| Datum | Was |
|---|---|
| 25.08. | **Vier Einwände von Lukas zu 5.4 geprüft — alle vier berechtigt.** (1) Abbildung A2 zeigt laut `m05_abbildungen.py` Z. 328 den RMSE je Fold für Ridge, RF und XGBoost, ist also eine Ergebnisabbildung und gehört nach 7.1. (2) Der Wiederholungs-Absatz doppelte Kapitel 7.1 Z. 2812 fast wortgleich und beschreibt ein Laufzeitprotokoll, keine Eigenschaft der Dateien. (3) Der Fairness-Absatz schloss aus einem Config-Block auf Verhalten. (4) Auflage D stand als Werturteil ohne Beleg |
| 25.08. | **Neue Abbildung A18 gebaut** (`a18_foldstruktur`, Funktion in `modelle/m05_abbildungen.py` Z. 1626, in der Erzeugungsliste registriert): Foldgrößen, brand-dominierte Monate und Größenspanne in einem Bild. Zahlen gegen die Parquet-Dateien geprüft — 6/6/6/6/5 plus 6 im Hold-out, Brandmonate 37/13/9/6/3/2, Summe 70 |
| 25.08. | **Direkt in Overleaf gesetzt:** Abbildung auf A18 umgestellt, `lst:streuung` entfernt (war nach der Kürzung verwaist), Fairness-Absatz umformuliert, `lst:merkmalsliste` um die Importzeilen aus `m02_menge.py` und `m03_struktur.py` erweitert, `lst:test-fairness` neu, Absatz zu Auflage D mit `\footcite[4]{Probst2019}` belegt, Wechselwirkungen mit `\footcite[362]{Hastie2009}`, Macro-F1-Aussage auf 2.6 verwiesen. Kompilat: 0 Fehler, 0 Warnungen |
| 25.08. | **Probst 2019 am Volltext geprüft:** S. 4, Abschnitt 3.2 — „Defaults settings … are usually provided by software packages, in an often ad hoc or heuristic manner". **Hastie et al. 2009 S. 362** — „The interaction level of tree-based approximations is limited by the tree size J" |
| 25.08. | **Offen, kosmetisch:** zwei Overfull-Boxen in 5.4 (2,6 pt bei Z. 1799, 6,1 pt bei Z. 1922). Ein Trennmuster in der Präambel hat sie nicht bewegt, auch nicht nach `\begin{document}` — die Ursache ist kein Kompositum. Der Block wurde wieder entfernt |
| 25.08. | **Killip 2004 am Volltext geprüft und eingebaut.** Die drei Gleichungen stehen auf S. 206; Gleichung 3 gilt ausdrücklich für gleich große Cluster, und genau das ist hier erfüllt, weil das Panel rechteckig ist. Nachgerechnet: $DE = 1 + 0{,}926 \cdot 131 = 122{,}3$, $ESS = 4.620/122{,}3 = 37{,}8$ — die Zahlen 122 und rund 38 stimmen. Absatz 7 trägt jetzt `\footcite[206]{Killip2004}` |
| 25.08. | **Acht Codekommentare im Repo nachgezogen**, `py_compile` fehlerfrei: zwei in `ergaenze_aufteilung`, einer in `_setze_datentypen`, drei in `poisson_glm` und `logit_glm`, dazu `config.py` „Deskription 5.1" → „4.1" und `test_aufbereitung.py` „NegBin-Offset" → „Poisson-Offset". Die Zeilennummern in den Beschriftungen der Quelltexte sind auf den neuen Stand gesetzt |
| 25.08. | **5.4 geschrieben, Fassung 1.** 16 Absätze statt der geplanten 9, sieben Quelltexte statt drei, drei Tabellen statt zwei. Grund: Der Auflagenblock in `main.tex` (Z. 1585–1635) führt vier offene Auflagen, die alle hier ihren Ort haben. Umfang dadurch 4,5 bis 5,5 Seiten statt 2,5 — drei Quelltexte und ein Absatz sind als kürzbar markiert |
| 25.08. | **Alle Kommentarblöcke zu Kapitel 5 in `main.tex` gelesen.** Neu gegenüber dem Gerüst ist der Block Z. 1585–1635 („Auflagen aus Schröters Freigabe vom 04.08.2026", Stand 25.08.): Auflage A (Abweichung vom Exposé nirgends erläutert), B (wörtliche Formulierung ist zur Zusage geworden, R-17), C (identische Merkmale und Splits belegen), dazu Hold-out-Zusammensetzung und effektive Stichprobe |
| 25.08. | **Am Volltext von `main.tex` geprüft:** „Generalisierung auf unbekannte Stadtteile" steht bisher **nur in Kommentaren** (Z. 437, 1602, 1767), „Designeffekt" ebenfalls nur in Kommentaren. Die ICC 0,926 steht dagegen bereits im Text (4.2, Z. 948). 5.4 bringt beide Formulierungen erstmals in den Fließtext |
| 25.08. | **Befund für Kapitel 2:** Abschnitt 2.4 `sec:referenzmodelle` enthält bislang **keinen Text**, nur Kommentare — auch die Unterabschnitte zur Poisson-Regression und zum multinomialen Logit. 5.4 verweist deshalb auf 2.6 und 3.2 und trägt die Spezifikation selbst |
| 25.08. | **Zwei Quellen werden in 5.4 erstmals zitiert:** `Roberts2017` und `Gourieroux1981`. Sie stehen seit 24.08. in `literatur.bib`, erschienen aber noch in keinem Literaturverzeichnis, weil biblatex nur zitierte Einträge druckt |
| 25.08. | **Fünf Codekommentare sind nachzuziehen**, bevor die Quelltexte von 5.4 eingetippt werden — Liste am Ende von `kapitel5-4_gleitobjekte.tex` |
| 24.08. | **Fünf `.bib`-Einträge direkt in Overleaf eingefügt** (Z. 820–953 in `literatur.bib`): `Kaufman2011`, `Altman2006`, `Brantingham1997`, `Roberts2017`, `Gourieroux1981`. Registerformat wie die übrigen; Felder ohne Einrückung, weil der Editor sie beim Tippen sonst kaskadierend erhöht |
| 24.08. | **Quellenprüfung abgeschlossen.** Kaufman, Brantingham und Altman am lokalen Volltext geprüft (Zitate wörtlich entnommen); Roberts visuell, weil das PDF keine Textebene hat — Titelseite und S. 925 gelesen |
| 24.08. | **5.3 geschrieben.** Zehn Absätze statt acht; Zielgrößentabelle mit Kennwerten entfällt endgültig (G3), weil `tab:kennzahlen` in 4.2 dieselben Werte führt. Merkmalstabelle in fünf Punkten korrigiert |
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
