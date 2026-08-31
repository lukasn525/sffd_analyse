# Codeausschnitte Kapitel 5 — Auswahl, Format, Einbindung (22.08.2026)

> **ÜBERHOLT (31.08.2026).** Maßgeblich sind `03_STAND.md`, `04_MODELLIERUNG.md`,
> `06_RISIKEN.md`, `07_BEFUNDE.md` und die eingereichte `main.tex`. Zahlen und
> Zeilennummern in dieser Datei stammen aus der Zeit VOR der Crosswalk-Korrektur
> vom 29.08.2026 (35 statt 36 Stadtteile, vollständiger Neulauf) und wurden
> bewusst NICHT nachgezogen: Die Datei ist als datierter Entwurf Teil der
> Arbeitsdokumentation.

Am Text ist nichts geändert. Diese Notiz wählt die Ausschnitte aus, liefert sie
als fertige `lstlisting`-Blöcke (`kapitel5_listings.tex`) und sagt, was im Code
nachgezogen werden muss, bevor sie eingesetzt werden.

---

## 1. Zwei Vorgaben, die sich widersprechen

| Quelle | Zahl der Ausschnitte für Kapitel 5 |
|---|---|
| Kommentarblock in `main.tex` vor Kapitel 5 | **3** (`safe_ratio`, Lag-Bildung, Fold-Zuteilung) |
| `docs/09_SCHREIBPLAN.md` §4, nach Auflage 10.08.2026 | **9** |

Der Schreibplan ist der jüngere und der begründete Stand — er verteilt rund 20
Ausschnitte über die Kapitel 5 bis 7 und benennt ausdrücklich Serialisierung
und Multithreading als Auflage. Der Kommentarblock ist an dieser Stelle
veraltet.

**Vorschlag: sieben Ausschnitte plus einer in Reserve.** Begründung ist das
Seitenbudget. Schröter am 10.08.: Code zählt zu den 40–60 Inhaltsseiten,
Abbildungen nicht. Am 22.08. kam dazu, dass der Anhang nicht zählt und
möglichst entfallen soll — die zwölf Ausschnitte, die laut Schreibplan dorthin
auswandern sollten, haben damit keinen Fluchtweg mehr.

| | Zeilen | Seiten bei `\footnotesize` |
|---|---:|---:|
| 9 Ausschnitte wie geplant | ~150 | ~2,5 |
| **7 Ausschnitte, gekürzt (Vorschlag)** | **~95** | **~1,6** |

---

## 2. Die Auswahl

Reihenfolge = Reihenfolge im Kapitel. Die Abschnittsnummern folgen der
Empfehlung (b) aus dem Kommentarblock, also mit eigenem 5.1 „Analyseeinheit und
Grundsatz“ und Verschiebung aller übrigen um eins.

| # | Label | Quelle | Zeilen | Abschnitt (neu / alt) | Was er belegt |
|---|---|---|---:|---|---|
| L1 | `lst:acs-versatz` | `s1_daten.acs_snapshot` | 306–322, 337 | 5.3 / 5.2 | `acs_jahr ≤ Einsatzjahr − 1` — der Verfügbarkeitsgrundsatz in vier Zeilen |
| L2 | `lst:quoten` | `s1_daten.berechne_quoten` | 559–584 | 5.4 / 5.3 | Nenner ≤ 0 ergibt NaN statt Division durch null |
| L3 | `lst:krimindex` | `s1_daten.kriminalitaetsindex` | 465–488 | 5.4 / 5.3 | **Der wichtigste Ausschnitt.** Location Quotient, Fenster endend im Vormonat — Leakage-Vermeidung konstruktiv, nicht behauptet |
| L4 | `lst:lags` | `s2_datensaetze.baue_regression` | 269–283 | 5.4 / 5.3 | `shift(1)` vor `rolling(3)`, Lag-Vorlauf |
| L5 | `lst:dtypes` | `s2_datensaetze._setze_datentypen` | 147–170 | 5.5 / 5.4 | **Serialisierung** (Auflage 10.08.): eine nullable `Int64`-Spalte macht `X.to_numpy()` zum `object`-Array |
| L6 | `lst:folds` | `s2_datensaetze.ergaenze_aufteilung` | 61–89 | 5.5 / 5.4 | Doppelte Stratifizierung; ohne sie hatte ein Fold null Brand-Testfälle |
| L7 | `lst:poisson` | `vorpruefung/v1_baselines.poisson_glm` | 74–104 | 5.5 / 5.4 | Stufe-2-Baseline: Offset, unpenalisiert, kein freier Parameter — Auflage D belegt statt behauptet |
| L8 | `lst:tests` | `tests/test_aufbereitung.py` | 255–260, 277–285 | 5.5 / 5.4 | *Reserve.* Zeigt, dass die 19 Prüfungen keine Dekoration sind |

**Gestrichen gegenüber dem Schreibplan:** `join_acs` (325–354) geht in L1 auf —
die eine Zuweisung `sffd["acs_year"] = …` trägt den Punkt, die übrigen 25 Zeilen
sind Merge-Mechanik. `test_lags_nicht_gegenwartsbezogen` und
`test_keine_ergebnisvariablen` sind zu einem Block (L8) zusammengezogen.

### Eine Dopplung, die auffallen wird

**L3 und L4 zeigen beide `shift(1)` vor `rolling()`.** Bei der Regel „ein Befund
wird einmal genannt und danach nur zitiert“ ist das genau der Fall, den der
Kommentarblock verbietet. Drei Auswege:

1. L4 auf `lag_1`/`lag_12` reduzieren und für das gleitende Mittel auf L3
   verweisen — billig, kostet nichts an Substanz.
2. L4 streichen, die Lag-Bildung im Fließtext abhandeln. Vertretbar: Die Lags
   sind seit dem Stadtteil-Split **kein Modellmerkmal** mehr, sie stehen nur
   noch für die Deskription in Kapitel 4 in der Datei. Ein eigener Ausschnitt
   für ein Nicht-Merkmal ist schwer zu verteidigen.
3. Beide behalten und im Text ausdrücklich sagen, dass dieselbe Regel an zwei
   Stellen angewandt wird.

Empfehlung: **2.** Damit bleiben sechs Ausschnitte, rund 1,4 Seiten.

---

## 3. Einbindung in Overleaf

### 3.1 Bei `listings` bleiben, nicht auf `minted` wechseln

`minted` braucht `--shell-escape` und Pygments; auf Overleaf läuft es, kostet
aber Compile-Zeit und macht die Datei außerhalb von Overleaf schwer baubar.
`pythonstil` ist laut Schreibplan im Preamble schon definiert — dabei bleiben.

### 3.2 Der Preamble-Block

Falls `pythonstil` mit `\lstdefinestyle` definiert ist, funktionieren die Blöcke
in `kapitel5_listings.tex` unverändert. Falls nicht (etwa
`\lstdefinelanguage{pythonstil}` oder ein globales `\lstset`), entweder
`style=pythonstil` in den Blöcken anpassen oder die Definition angleichen:

```latex
\usepackage{listings}
\usepackage{xcolor}

\lstdefinestyle{pythonstil}{
  language=Python,
  basicstyle=\ttfamily\footnotesize,
  commentstyle=\color{black!55},   % NICHT \itshape -- Formalia M6: keine Kursivschrift
  keywordstyle=\bfseries,          % druckt in Graustufen sauber
  stringstyle=\color{black!70},
  numbers=left, numberstyle=\tiny\color{black!50}, numbersep=8pt,
  showstringspaces=false,
  breaklines=true, breakatwhitespace=true,
  breakindent=0pt, postbreak=\mbox{$\hookrightarrow$\space},
  frame=lines, framesep=6pt,
  xleftmargin=16pt,
  captionpos=b,
  inputencoding=utf8, extendedchars=true,
  literate=%
    {ä}{{\"a}}1 {ö}{{\"o}}1 {ü}{{\"u}}1
    {Ä}{{\"A}}1 {Ö}{{\"O}}1 {Ü}{{\"U}}1
    {ß}{{\ss}}1 {–}{{--}}1 {„}{{\glqq}}1 {“}{{\grqq}}1
}

% Deutsche Benennung
\renewcommand{\lstlistingname}{Quelltext}
\renewcommand{\lstlistlistingname}{Quellcodeverzeichnis}
```

**Die `literate`-Zeile ist nicht optional.** `listings` liest UTF-8 nicht von
selbst; ohne sie verschwinden Umlaute im Satz oder brechen den Lauf. Betroffen
ist heute nur L8 — `tests/test_aufbereitung.py` ist die einzige der vier
Quelldateien mit echten Umlauten.

### 3.3 Quellcodeverzeichnis

Bei rund 20 Ausschnitten fällig, im Vorspann neben Abbildungs- und
Tabellenverzeichnis:

```latex
\lstlistoflistings
```

Nachprüfen, ob die FOM-Vorlage dafür eine eigene Umgebung vorsieht (sie tut es
für Abbildungen und Tabellen); wenn ja, dieselbe Mechanik verwenden, damit die
Seitenzahl im Inhaltsverzeichnis stimmt.

### 3.4 Nicht als Float setzen

`\begin{lstlisting}[float=htbp]` lässt den Ausschnitt wandern — bei sechs
Ausschnitten auf zwölf Seiten landet er dann irgendwo, und der Fließtext
verweist ins Leere. Die Blöcke in `kapitel5_listings.tex` sind bewusst
**ohne** `float` gesetzt: Sie stehen dort, wo sie im Quelltext stehen. Bei einem
ungünstigen Seitenumbruch hilft `\needspace{14\baselineskip}` (Paket
`needspace`) vor dem Block.

### 3.5 Beschriftung und Verweis

Die Beschriftung trägt Datei und Zeilenbereich — das ist die einzige Brücke
zwischen Arbeit und Repositorium, und ein Zweitgutachter sucht danach:

```latex
caption={Relativer Kriminalitätsindex je Stadtteil und Monat.
         Quelle: \texttt{prep/s1\_daten.py}, Z.~465--488, gekürzt}
```

Unterstriche in der Beschriftung müssen escaped werden (`s1\_daten`), im
Listing-Körper nicht. Im Fließtext dann `\autoref{lst:krimindex}` bzw.
`Quelltext~\ref{lst:krimindex}`. Formalia M6 verlangt Module und Funktionen in
Monospace — also durchgehend `\texttt{berechne\_quoten}`, nie kursiv.

### 3.6 Die Alternative, die ich nicht empfehle

`\lstinputlisting[firstline=465,lastline=488]{code/s1_daten.py}` bindet die
Datei direkt ein und kann per Definition nicht driften. Zwei Gründe dagegen:
Die Ausschnitte sind gekürzt (weggelassene Zeilen mitten im Bereich gehen
nicht), und jede Codeänderung verschiebt die Zeilennummern still — der falsche
Ausschnitt erscheint dann ohne Fehlermeldung. Einfügen ist hier das kleinere
Risiko, solange die Regel aus 4.1 eingehalten wird.

---

## 4. Codeseitige Arbeit, bevor die Blöcke eingesetzt werden

### 4.1 Die Regel

**Jede Zeile in `kapitel5_listings.tex` steht zeichengleich so in der
Quelldatei.** Gekürzt wurde ausschließlich durch Weglassen ganzer Zeilen, nie
durch Umformulieren. Wo ein Kommentar zu lang war, steht er gar nicht im
Ausschnitt — seine Begründung gehört ohnehin in den Fließtext, und der
Schreibplan sagt genau das („die Docstrings tragen die Begründung schon, du
musst sie im Fließtext nur aufgreifen“).

Damit gibt es keine zweite Fassung des Codes. Soll ein Kommentar im Ausschnitt
anders lauten, wird er **zuerst in der `.py`-Datei geändert** und von dort
übernommen.

### 4.2 Was jetzt nachgezogen werden muss

| | Datei, Zeile | Steht da | Muss lauten | Warum |
|---|---|---|---|---|
| C1 | `tests/test_aufbereitung.py:127` | `# Rohwerte bleiben erhalten (NegBin-Offset, …)` | `Poisson-Offset` | Seit #45 ist die Stufe-2-Baseline das Poisson-GLM. Der Kommentar nennt ein Modell, das die Arbeit ausdrücklich verwirft |
| C2 | `tests/test_aufbereitung.py:5–6` | „eignen sich als Code-Beleg **im Anhang der Arbeit**“ | ohne Anhangsbezug | Entscheidung vom 22.08.2026: möglichst ohne Anhang |
| C3 | `prep/config.py:76–77` | `# Antwortzeit-Plausibilitaetsfenster in Minuten` | zusätzlich: dass 0–60 eine **gesetzte** Grenze ist | Bereits als offener Punkt vermerkt. Sobald Kapitel 5 das Fenster nennt, muss der Satz „das ist eine Annahme, keine Datenkonsequenz“ auch im Code stehen |
| C4 | `prep/s1_daten.py:451`, `prep/config.py:114`, `prep/s2_datensaetze.py:39`, `:110`, `tests/…:127` | Verweise auf „Kapitel 5.1“, „5.2 und 5.4“ | je **+1**, sobald Empfehlung (b) steht | Fünf Stellen, alle rein redaktionell — aber sie werden falsch, sobald 5.1 „Analyseeinheit und Grundsatz“ existiert |

C4 ist der Grund, die Gliederungsentscheidung **vor** dem Schreiben zu treffen
und nicht danach.

### 4.3 Drei Stellen im Kommentarblock von `main.tex`, die nicht stimmen

Keine davon steht im Fließtext, alle wären beim Schreiben übernommen worden.

- **„Ausrueckzeit auf 0-60 min begrenzt (~1,7 % entfallen)“.** Es heißt
  *Antwortzeit* (`arrival_dttm − alarm_dttm`, Spalte `response_time_min`), und
  auf dem aktuellen Datenstand entfällt keine einzige Zeile: 720.258 − 269
  Dubletten = 719.989. Der Kommentarblock sagt zwei Absätze weiter oben selbst
  das Richtige („OHNE Entfallquote, es entfällt keine einzige Zeile“) — er
  widerspricht sich also innerhalb desselben Blocks.
- **„LISTING 1: safe_ratio“.** Eine Funktion dieses Namens gibt es nicht. Der
  Schutz gegen Division durch null sitzt in `berechne_quoten` als
  `n.where(n > 0, np.nan)`. Entweder den Kommentar auf `berechne_quoten`
  umschreiben oder den Helfer im Code tatsächlich herauslösen. Ich empfehle
  Ersteres — ein Refactoring nur für einen Ausschnitt lohnt vier Tage vor der
  Frist nicht.
- **„Reihenfolge folgt der Pipeline: s1_daten.py → s2_datensaetze.py →
  s3_baselines.py“.** `prep/s3_baselines.py` existiert nicht. Die Baselines
  stehen in `vorpruefung/v1_baselines.py`. Das ist mehr als ein Tippfehler: Die
  Auflage Schröters, die Baselines in die Data Preparation zu nehmen, ist im
  Code dadurch umgesetzt, dass sie in einem **eigenen Ordner vor** der
  Modellierung liegen — das gehört so in den Text, sonst fragt das Kolloquium,
  warum das Kapitel eine Datei beschreibt, die es nicht gibt.

---

## 5. Zwei Dinge außer der Reihe

**Der Census-API-Schlüssel steht im Klartext in `prep/config.py:41`.** Wenn
`config.py` je als Ausschnitt gedruckt oder das Repositorium beigelegt wird,
ist er offen. Er taucht in keinem der sieben Ausschnitte auf — trotzdem vor der
Abgabe auf eine Umgebungsvariable umstellen und den alten Schlüssel bei Census
zurückziehen.

**Umlaute im Code.** `prep/` und `vorpruefung/` schreiben durchgängig `ue`/`ae`,
`tests/` echte Umlaute. Im Druck fällt „Ueberdispersion“ als Tippfehler auf.
Drei Möglichkeiten: so lassen und bei der ersten Beschriftung eine Fußnote
setzen (ASCII-Konvention der Quelldateien); die vier Dateien in einem Durchgang
auf Umlaute umstellen (nur Kommentare, kein Verhaltensrisiko, aber ein
Neulauf-Nachweis ist sauberer); oder in den Ausschnitten weitgehend auf
Kommentare verzichten. Betroffen sind heute nur L4 und L5 — die übrigen
Ausschnitte tragen keinen oder einen umlautfreien Kommentar.
