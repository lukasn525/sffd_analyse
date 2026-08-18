# Schreibplan Bachelorarbeit — Stand 18.08.2026

Abgabe **07.10.2026** (KW 41, Mittwoch). Heute KW 34. **Acht Kalenderwochen.**

Gerechnet ist alles. Der finale Lauf vom 16.08. ist ausgewertet, das Hold-out
einmalig gezogen, seit dem 08.08. ist keine Festlegung mehr unabgestimmt.
Was fehlt, ist der Text.

---

## 0. Was in dieser Sitzung passiert ist

| | |
|---|---|
| `main.tex` | 23 Korrekturen gegen `03_STAND.md`, Protokoll in Abschnitt 6 |
| Overleaf | auf den lokalen Stand gebracht, kompiliert fehlerfrei, 25 Seiten |
| `tools/deskriptiv.py` | neu, gegen die echten Daten gelaufen |

Der Overleaf-Stand war 13 Tage alt (1.620 gegen 2.065 Zeilen) und enthielt
nichts, was lokal fehlte — außer den beiden Bilddateien, die dort bleiben.
Die Bibliothek war in beiden identisch: 21 Einträge im `filecontents`-Block.

---

## 1. Die Reihenfolge — und warum nicht 1 bis 9

Von vorn nach hinten zu schreiben ist hier die schlechteste Option. Drei
Gründe, jeder für sich ausreichend:

**Das Ergebnis ist unbequem und muss von hinten her erzählt werden.** Im
Mengenstrang schlägt kein Verfahren die Stufe-2-Baseline; im Strukturstrang
kehrt sich das Kreuzvalidierungsergebnis auf dem Hold-out um. Kapitel 6.2
begründet, warum über ein einfaches lineares Modell hinausgegangen wurde —
wenn dieses Kapitel geschrieben wird, ohne dass der Wortlaut von Kapitel 7
feststeht, verspricht es die Baumverfahren zu stark und muss neu geschrieben
werden. Genau davor warnt der Abgrenzungsblock in `main.tex`.

**Kapitel 4 darf keinen Befund zweimal bringen.** Die Regel „ein Befund wird
einmal genannt und danach nur noch zitiert" lässt sich nur einhalten, wenn man
weiß, welche Befunde die späteren Kapitel schon verbraucht haben. Kapitel 4
zuerst zu schreiben erzeugt die Doppelung fast zwangsläufig — und
„umfangreich, aber nicht fokussiert" ist im Gutachten zum Anwendungsprojekt
abgestraft worden.

**Einleitung und Fazit versprechen, was die Mitte liefert.** Beide gehören ans
Ende. Kapitel 1.2 nennt heute noch vier Unterfragen; welche davon wie klar
beantwortet sind, steht erst nach Kapitel 8 fest.

### Die Reihenfolge

| # | Kapitel | Seiten | Warum hier |
|---:|---|---:|---|
| 1 | **3 Business Understanding** | 3 | Kurz, und es legt die Zielgrößen und Erfolgskriterien fest, die Kapitel 5 bis 7 benutzen. Setzt das Vokabular. |
| 2 | **5 Data Preparation** | 8 | Das Fundament. Hier stehen Analyseeinheit, Stadtteil-Split, Fairness-Regel und beide Baselines — alles, worauf sich 6 und 7 beziehen. Sechs der zwanzig Codeausschnitte sitzen hier. |
| 3 | **7 Evaluation** | 11 | **Vor Kapitel 6.** Das Ergebnis steht fest und ist die Zielmarke, auf die 6 zulaufen muss. Umgekehrt geschrieben widerspricht 6 dem Kapitel 7. |
| 4 | **6 Modelling** | 8 | Jetzt als Anlauf auf ein bekanntes Ergebnis. Die Eignungsprüfung begründet weiterhin, warum über das lineare Modell hinausgegangen wurde — mit dem Vorwärtsverweis auf B-41 von Anfang an eingebaut. |
| 5 | **4 Data Understanding** | 7 | Jetzt weißt du, welcher Befund in 5, 6 oder 7 schon verbraucht ist. Grundlage: `results/deskriptiv/befunde.md`. |
| 6 | **8 Diskussion** | 5 | Deutung. Braucht 6 und 7 fertig. |
| 7 | **2 Grundlagen** | 16 | Siehe unten — wird gesplittet, nicht am Stück geschrieben. |
| 8 | **1 Einleitung** | 3 | Verspricht, was geliefert wurde. |
| 9 | **9 Fazit** | 2 | Beantwortet die vier Unterfragen mit den Wörtern aus 7 und 8. |

### Kapitel 2 wird gesplittet — das ist der wichtigste Kunstgriff

16 Seiten sind der größte Block der Arbeit. Am Stück ans Ende gelegt ist das
zu riskant, an den Anfang gelegt produziert es ein Grundlagenkapitel, das
Dinge erklärt, die nachher niemand braucht (das „Historienkapitel" aus den
Bewertungstipps). Deshalb drei Teile mit drei verschiedenen Terminen:

| Teil | Wann | Warum dann |
|---|---|---|
| **2.3 Untersuchte Verfahren**, **2.4 Modelloptimierung und -bewertung** | **jederzeit, ab sofort** | Reines Lehrbuchwissen, unabhängig von den Befunden. Quellen liegen größtenteils schon in der Datenbank (Hoerl, Breiman, Chen, Cameron, Bergstra, Probst, Bergmeir). Ideal für Tage, an denen der empirische Teil nicht vorangeht. |
| **2.1, 2.2, 2.5, 2.6** Forschungsstand | nach der Quellenvalidierung | Braucht die neuen Quellen und die tabellarische Synthese, die im Gutachten gefordert ist. |
| **2.7 Forschungslücke und Erwartungen** | **ganz zuletzt, nach 8.1** | Das Scharnier. Die hier abgeleiteten Erwartungen werden in 8.1 gegen die Befunde gestellt — und beide sind widerlegt. Der Absatz muss so formuliert sein, dass die Widerlegung in 8.1 nicht wie ein Bruch wirkt. |

---

## 2. Terminplan

| KW | Datum | Was | Ergebnis am Ende der Woche |
|---|---|---|---|
| **34** | 18.–23.08. | Kapitel 3 + Start Kapitel 5. Parallel: Quellenliste an mich, Validierung läuft | 3 fertig, 5.1/5.2 im Rohtext |
| **35** | 24.–30.08. | Kapitel 5 fertig (5.3, 5.4 mit Auflage D). Nebenher 2.3 | 5 fertig, `literatur.bib` validiert |
| **36** | 31.08.–06.09. | Kapitel 7.1 + 7.2, inklusive Decken vor der Ergebnistabelle | 7 zur Hälfte |
| **37** | 07.–13.09. | Kapitel 7.3 + 7.4 + 7.5, dann Kapitel 6.1/6.2. Nebenher 2.4 | 7 fertig |
| **38** | 14.–20.09. | Kapitel 6 fertig, Kapitel 4 | 6 und 4 fertig |
| **39** | 21.–27.09. | Kapitel 8, dann 2.1/2.2/2.5/2.6 | 8 fertig, 2 zu drei Vierteln |
| **40** | 28.09.–04.10. | 2.7, Kapitel 1, Kapitel 9. Codeausschnitte einsetzen, Abbildungen einbinden, Verzeichnisse | Vollständiger Text |
| **41** | 05.–07.10. | Korrekturdurchgang Formalia, Zahlenwächter über `main.tex`, Abgabe-ZIP | **Abgabe 07.10.** |

**Zwei Puffer sind eingebaut**: Kapitel 2.3/2.4 lässt sich in jede Lücke
schieben, und die Woche 41 ist für Formalia reserviert, nicht für Inhalt.
Wenn eine Woche kippt, kippt nicht der Plan.

**Ein Risiko ist nicht abgedeckt**: Wenn die Quellenvalidierung mehr als 40
Quellen nachrecherchieren muss, verschiebt sich Kapitel 2 nach hinten. Deshalb
steht die Quellenliste in KW 34 auf dem Plan und nicht später.

---

## 3. Das Seitenbudget — hier ist ein Konflikt

Die Umfangsangaben in `main.tex` summieren sich auf **63 Seiten**. Dazu kommen
rund 20 Codeausschnitte à 15 bis 25 Zeilen, also grob **8 weitere Seiten**.

| | Seiten |
|---|---:|
| Kapitel 1 bis 9 laut `main.tex` | 63 |
| ~20 Codeausschnitte | ~8 |
| **Summe** | **~71** |
| **Vorgabe** | **40–60** |

Schröter am 10.08.: Zu den 40 bis 60 Seiten zählen alle Inhaltsseiten
**inklusive Code und Tabellen**, Abbildungen **nicht**.

**Das geht so nicht auf.** Drei Hebel, in dieser Reihenfolge:

1. **Die Frage an Schröter stellen**: Zählt der Anhang zu den Inhaltsseiten?
   Wenn nein, wandern 12 der 20 Ausschnitte dorthin und das Problem ist zur
   Hälfte gelöst. Diese Frage gehört in die nächste Sprechstunde oder eine
   E-Mail — sie ist billig zu stellen und teuer zu raten.
2. **Kapitel 2 von 16 auf 12 kürzen.** Die Vorgabe lautet High-Level; 16
   Seiten Grundlagen bei 8 Seiten Modelling ist das falsche Verhältnis und
   genau das, was unter „Methodenkapitel-Overkill" fällt.
3. **Kapitel 7 von 11 auf 9.** Der Mengenstrang wird nach B10 ohnehin auf
   eine Zielgröße verdichtet; die Rate bekommt einen Absatz plus
   Anhangstabelle.

Damit landest du bei 63 − 4 − 2 = 57 plus die Ausschnitte im Fließtext. Das
passt.

---

## 4. Die zwanzig Codeausschnitte

Auflage vom 10.08.2026: rund 20 nicht-triviale Ausschnitte, **ausdrücklich
Serialisierung und Multithreading**. Der Code ist durchgehend dokumentiert —
die Docstrings tragen die Begründung schon, du musst sie im Fließtext nur
aufgreifen.

**Regel für alle**: Der Ausschnitt zeigt eine **Entscheidung**, nicht eine
Zeile Pandas. Wenn sich ein Ausschnitt mit „das lädt eine Datei"
zusammenfassen lässt, ist er falsch gewählt.

### Kapitel 5 — Data Preparation (9)

| # | Quelle | Zeilen | Was er zeigt |
|---:|---|---:|---|
| 1 | `s1_daten.berechne_quoten` | 571–586 | Nenner ≤ 0 ergibt NaN statt Division durch Null → 5.3 |
| 2 | `s1_daten.kriminalitaetsindex` | 431–502, gekürzt | Location Quotient, rollierendes Fenster **endend im Vormonat**. Der wichtigste Ausschnitt der Arbeit — er belegt die Leakage-Vermeidung konstruktiv → 5.3 |
| 3 | `s1_daten.join_acs` | 325–354 | Publikationsversatz `acs_jahr ≤ Einsatzjahr − 1` → 5.2 |
| 4 | `s2_datensaetze.aggregiere`, Lag-Teil | ~176–248, Auszug | `shift(1)` **vor** `rolling(3)`, Lag-Vorlauf von 12 Monaten → 5.3 |
| 5 | `s2_datensaetze.ergaenze_aufteilung` | 61–92 | Doppelte Stratifizierung; ohne sie hatte ein Fold null Brand-Testfälle → 5.4 |
| 6 | **`s2_datensaetze._setze_datentypen`** | 147–170 | **SERIALISIERUNG.** Eine nullable `Int64`-Spalte genügt, damit `X.to_numpy()` ein `object`-Array liefert; sklearn fängt das still ab, XGBoost lehnt es ab → 5.4 |
| 7 | `v1_baselines.poisson_glm` | 74–107 | Die Stufe-2-Baseline: unpenalisiert, mit Offset, kein freier Parameter. Auflage D wird hier belegt statt behauptet → 5.4 |
| 8 | `tests.test_lags_nicht_gegenwartsbezogen` | 255–263 | Kurz. Zeigt, dass die 19 Prüfungen keine Dekoration sind → 5.4 |
| 9 | `tests.test_keine_ergebnisvariablen` | 277–288 | Leakage konstruktiv ausgeschlossen: was nicht im Datensatz ist, kann nicht ins Modell → 5.1 |

### Kapitel 6 — Modelling (7)

| # | Quelle | Zeilen | Was er zeigt |
|---:|---|---:|---|
| 10 | `m02_menge.verfahren` | 126–170 | Die drei Schätzer nebeneinander, `n_jobs` als Parameter → 6.3 |
| 11 | `m02_menge.suchraum` | 170–206 | Suchräume, inklusive der vier nach #49 erweiterten → 6.3 |
| 12 | **`m02_menge.tune`** | 206–241 | **MULTITHREADING I.** `RandomizedSearchCV(n_jobs=-1)` um einen Schätzer mit `n_jobs=1` — die eigentliche Entscheidung ist, warum **nicht beides** → 6.1 |
| 13 | **`m02_menge.ein_lauf`, `auch_parallel`-Block** | 285–305 | **MULTITHREADING II.** Derselbe Fit einkernig und über alle Kerne, Differenz ist der Parallelisierungsgewinn. Enthält die Messung, die B-24 belegt: XGBoost ist nicht threaddeterministisch → 6.1 und 7.3 |
| 14 | `m02_menge.ein_lauf`, Expositionsteil | 271–283 | #43 — alle Verfahren modellieren die Rate und multiplizieren zurück. Der methodische Kern der Arbeit in acht Zeilen → 6.1 |
| 15 | `m03_struktur.kodiere` + `_gewichte` | 161–191 | Label-Encoding und `class_weight="balanced"` — warum Accuracy dadurch fällt und Macro-F1 steigt → 6.1 |
| 16 | `v3_spezifikation.entwerfe` | 62–104 | Die Spezifikationsgegenprobe: quadratische Terme und Interaktionen systematisch erzeugt → 6.2 |

### Kapitel 7 — Evaluation (4)

| # | Quelle | Zeilen | Was er zeigt |
|---:|---|---:|---|
| 17 | `m02_menge._gepaart` | 559–585 | Gepaarter Wilcoxon **auf den 10 Wiederholungsmitteln**, nicht auf den 50 Läufen → 7.1 |
| 18 | `m02_menge._holm` | 539–559 | Holm-Bonferroni über die Testfamilie → 7.1 |
| 19 | `m04_shap.ablation_faktorgruppen` | 270–365, gekürzt | Jede Gruppe einmal weggelassen — die zweite Antwort auf UF1 → 7.4 |
| 20 | `v4_decke.decke_a` | 83–114 | Multinomiale Neuziehung: in 12,5 % der Fälle kippt der argmax. Erzeugt die Obergrenze, gegen die 7.2 gemessen wird → 7.2 |

**Reserve, falls einer wegfällt**: `m03_struktur._macro_auroc`,
`codebook.waechter`, `m02_menge.hold_out`, `v0_aufteilung._selbsttest`.

---

## 5. Abbildungen und Tabellen

Elf Abbildungen liegen fertig in `results/abbildungen/` — **eingebunden ist
keine einzige.** `main.tex` enthält genau ein `\includegraphics`, das
FOM-Logo. Sie kosten nichts (zählen nicht zu den 40–60 Seiten) und müssen
nicht beschrieben, aber begründet werden: warum diese Daten, diese Abbildung,
diese Darstellung. Die Begründungen stehen in den Docstrings von
`m05_abbildungen.py`.

| Abbildung | Kapitel |
|---|---|
| `a0_pipeline` | 5, Einstieg |
| `a2_foldstruktur` | 5.4 |
| `a10_qq_residuen` | 6.2, Anforderungen je Verfahren |
| `a8_hyperparameter` | 6.4, Lage der gewählten Werte im Suchraum |
| `a1_gegen_baseline` | 7.1, die Primäraussage |
| `a5_holdout` | 7.1 und 7.2 |
| `a4_laufzeit_guete` | 7.3 |
| `a9_parallelisierung` | 7.3 |
| `a6_faktorgruppen` | 7.4 |
| `a3_spezifikation` | 8.1 — Spannweite Verfahrenswahl gegen Spezifikationswahl |
| `a7_extrapolation` | 8.3 |

**Ein Formalproblem**: `results/eignungspruefung/01_streudiagramme.png` und
`02_residuen.png` sind PNG. Die Auflage lautet Vektorformat (PDF oder SVG).
Beide gehören nach 6.2 und sind neu zu erzeugen.

**Bildunterschriften**: Wo eine Abbildung beide Zielgrößen zeigt, der Text
aber nur `anzahl_einsaetze` berichtet, muss die Unterschrift sagen, dass die
zweite als Robustheitsprüfung mitläuft. Sonst steht in der Abbildung mehr als
im Text, ohne dass der Leser weiß warum.

**Vier Tabellen tragen Kapitel 7**: Aggregate Regression, Aggregate
Klassifikation, Vergleichstabelle mit Wilcoxon-Ergebnissen, Hold-out. Die
große Merkmalstabelle aus `results/codebook/merkmale.md` gehört in Kapitel 4
beziehungsweise den Anhang.

---

## 6. Was ich an `main.tex` geändert habe

23 Ersetzungen, alle gegen `docs/03_STAND.md` Stand 16.08.2026 geprüft.
Fünf davon sind mehr als Kosmetik:

**Die Negative Binomial stand noch überall.** Sie ist seit #45 durch das
unpenalisierte Poisson-GLM ersetzt, aber die Schreibanleitungen zu 5.4, 6.2,
7.1 und 8.1 nannten weiter sie — samt der alten Zahlen R² 0,472 und RMSE
37,27. Korrekt sind R² 0,542 und RMSE 33,98. Hätte man danach geschrieben,
wäre die Baseline der ganzen Arbeit falsch benannt gewesen.

**„In der Klassifikation gibt es kein Pendant" war falsch.** Seit #33 ist das
multinomiale Logit das Gegenstück zum Poisson-GLM. Die Messlatte im
Strukturstrang ist Macro-F1 0,297, nicht die Mehrheitsklasse mit 0,223.

**Der Exposé-Satz zur Time-Series-Cross-Validation stand noch im Fließtext**
von 6.1, obwohl seit #29 der Stadtteil-Split gilt. Entfernt; die Abweichung
gehört nach 8.3, der Beleg Bergmeir & Benítez bleibt an anderer Stelle
brauchbar.

**Die Spezifikationsasymmetrie in 8.1 existiert nicht mehr.** Der alte Text
hätte den Vorwurf reproduziert, nur die Baseline bekomme die Bevölkerung als
Offset. Seit #43 modellieren alle Verfahren die Rate. Übrig bleibt ein
schwächerer Rest, der zu benennen ist — aber es ist kein ungleicher Vergleich
mehr.

**Der Extrapolationsanteil war zweimal falsch.** Er lautete 31 %; korrekt
sind 33,7 % (Wiederholung 0), 34,6 % (alle 50 Läufe) und 7,6 % (Hold-out) —
mit Bezugsmenge. Und die Behauptung, er treffe Baumverfahren und Ridge
unterschiedlich, ist durch B-31 gemessen und widerlegt: Der Zusammenhang ist
bei Ridge am stärksten und beim Random Forest am schwächsten.

**Eine Änderung solltest du prüfen, weil sie inhaltlich ist**: Unterfrage 2
in Kapitel 1.2 nannte noch „naive Baselines" und „zeitreihengerechte
Kreuzvalidierung". Ich habe sie auf den Wortlaut aus `CLAUDE.md` gezogen —
Baselines und stadtteilweise Kreuzvalidierung. Das ist eine Änderung an einer
Forschungsfrage aus dem angemeldeten Exposé. Sie ist durch #29 gedeckt und
von Schröter am 04.08. freigegeben, aber sie ist deine Entscheidung, nicht
meine.

Ergänzt habe ich außerdem: die wörtliche Auflage D mit ihrem Vorbehalt, die
Formulierung „Generalisierung auf unbekannte Stadtteile" als offene Zusage an
zwei Stellen (R-17), die Anforderungstabelle je Verfahren in 6.2, die Decken
des Strukturstrangs vor der Ergebnistabelle in 7.2, GLM/ICC/NFIRS im
Abkürzungsverzeichnis und die Liste der sechs Bibliographie-Einträge, die in
Kommentaren angekündigt sind, aber noch fehlen — Altman 2006, Hurlbert 1984,
Roberts et al. 2017, Gourieroux et al. 1984, Ramsey 1969, Hsu et al. 2025.
Ohne sie bricht der Build, sobald die betreffenden Passagen geschrieben sind.

---

## 7. `tools/deskriptiv.py`

Neu, liegt im Repo, gegen deine echten Daten gelaufen. Erzeugt
`results/deskriptiv/` mit `befunde.md` als Lesefassung für 4.2.

Es rechnet **nur die Befundseite**: Verteilung je Merkmal, Varianzzerlegung,
zeitliche Auflösung, Zusammenhänge. Kein RESET-Test, kein VIF, keine
Residuenanalyse — die stehen in `v2_eignung.py` und gehören nach 6.2. Genau
das ist der Grenzfall (a) aus dem Abgrenzungsblock.

**Es ist nicht dasselbe wie `codebook.py`.** Das Codebook sagt, *was* ein
Merkmal ist (Skalenniveau, Einheit, Was/Wie/Wofür) — Schröters Auflage vom
10.08. verbietet dort ausdrücklich die deskriptive Statistik je Merkmal.
Dieses Skript sagt, *wie es aussieht*. Getrennte Skripte, getrennte Ausgaben,
kein Wert an zwei Stellen.

Drei Werte hat es unabhängig reproduziert und bestätigt damit `03_STAND.md`:

| | |
|---|---|
| Dispersionsindex `anzahl_einsaetze` | 62,8 |
| Zwischen-Varianzanteil | 92,5 % |
| Designeffekt / effektive Stichprobe | 122 / 38 |

Der stärkste neue Befund ist die **Auflösungstabelle**: Der
Kriminalitätsindex nimmt je Stadtteil 128 verschiedene Werte über 132 Monate
an, die ACS-Merkmale 4, die drei baulichen Merkmale **1**. Das ist Mechanismus
1 aus B-47 — und es erklärt in einer Tabelle, warum Attribution und Ablation
in 7.4 auseinanderfallen.

Läuft mit deinem venv (`venv\Scripts\python.exe tools\deskriptiv.py`); die
Prüfaufträge stehen am Ende des Docstrings.

---

## 8. Was ich als Nächstes übernehmen kann

| | Maßnahme | Auslöser |
|---|---|---|
| **M1** | **Quellenvalidierung.** Jede Quelle: existiert sie, stimmen Metadaten und DOI, ist sie zitierfähig nach Schröters Regel (Paper für den Forschungsstand, Lehrbücher nur für Grundlagen), in welchen Abschnitt gehört sie. Ergebnis: fertige `literatur.bib` plus Zuordnungstabelle. Ausstehend sind außerdem sechs bereits eingeplante Einträge und fünf unverifizierte Bestandsquellen. | du schickst die Liste |
| **M2** | **Stichpunkt-Gerüst je Kapitel.** Absatz für Absatz, mit Zahl, Quelle, Abbildung, Tabelle. Format entscheiden wir pro Kapitel. | pro Kapitel, wenn du drankommst |
| **M3** | **Codeausschnitte als fertige `lstlisting`-Blöcke** mit Beschriftung und Zeilenverweis. Der `pythonstil` ist im Preamble schon definiert; ein Quellcodeverzeichnis fehlt noch und ist bei 20 Ausschnitten fällig. | wenn Kapitel 5 steht |
| **M4** | **Abbildungen einbinden** — elf `figure`-Blöcke mit Unterschrift und Quellenangabe, plus die beiden PNG der Eignungsprüfung als PDF neu erzeugt. | jederzeit |
| **M5** | **Zahlenwächter auf `main.tex` ausweiten.** `tools/pruefe_zahlen.py` liest heute nur `docs/`. Die Thesis ist die letzte ungeschützte Driftfläche — und die 23 Korrekturen von heute sind der Beleg, dass sie driftet. | vor dem ersten Ergebniskapitel, also KW 35 |
| **M6** | **Formalia-Durchgang**: keine Kursivschrift, keine Anführungszeichen, keine Unterstreichungen, „o.A." bei unbekanntem Autor, p-Werte auf 95/99/99,5, Module und Funktionen in Monospace. | KW 41 |
| **M7** | **Demo-Modus fürs Kolloquium.** Ein voller Lauf dauert rund drei Stunden und ist nicht vorführbar. Vorschlag: ein Skript, das aus `results/` heraus in unter zwei Minuten einen Fold live nachrechnet. | nach der Abgabe, vor dem 15.12. |

**Zwei Dinge, die nur du kannst**: die Frage nach dem Anhang im Seitenbudget
an Schröter stellen, und entscheiden, ob Unterfrage 2 in der geänderten
Fassung bleibt.

---

## 9. Reihenfolge für morgen

1. Quellenliste schicken → ich starte M1
2. Kapitel 3 schreiben (3 Seiten, das Vokabular für alles Weitere)
3. `venv\Scripts\python.exe tools\deskriptiv.py` laufen lassen und
   `results/deskriptiv/befunde.md` überfliegen — das ist dein Kapitel 4 in Rohform
4. E-Mail an Schröter: zählt der Anhang zu den 40–60 Seiten?
