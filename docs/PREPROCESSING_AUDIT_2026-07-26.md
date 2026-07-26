# Preprocessing-Audit gegen die 11 Prüfaufträge

> **Umsetzungsstand 2026-07-26:** Alle Punkte aus **P1 sind umgesetzt**, mit einer
> bewussten Ausnahme: P1.4 (Crime zeitbewusst) wurde vertagt, siehe Decision Log #16.
> Zusätzlich kam beim Umbau ein weiterer Befund hinzu (unbalanciertes Panel durch
> Mission Bay, Decision Log #15). Die betroffenen Abschnitte unten sind mit
> **[ERLEDIGT]** markiert; P2 und P3 stehen unverändert offen.

Stand des Repos zum Zeitpunkt der Prüfung: Commit `b94d12e`, geprüft am 2026-07-26.
Grundlage: `pipeline/01–03`, `modellierung/aggregation.py`, `modellierung/demo_modellierung.py`,
`analyse/eignungspruefung.py`, `CLAUDE.md`, `docs/UMSETZUNGSLEITFADEN_MODELLIERUNG.md`.
Alle Zahlen unten sind auf dem tatsächlichen Datenstand nachgerechnet, nicht aus der Doku übernommen.

---

## Teil 1 – Abgleich der 11 Punkte

| # | Prüfauftrag | Status im Repo | Befund |
|---|---|---|---|
| 0 | Feature-Inventar | **teilweise** | `DATA_DICTIONARY.md` + `results/eignungspruefung/` decken Quelle, NaN-Anteil, Berechnungslogik ab. Es fehlen zwei Spalten: **Aggregationsebene** und **ab wann real verfügbar**. Genau diese beiden decken die Punkte 4 und 11 auf. |
| 1 | Ebene der Klassifikation | **faktisch entschieden, nicht dokumentiert** | Regression auf Stadtteil×Monat (`aggregation.py`), Klassifikation auf Einzeleinsatz (Leitfaden Schritt 7). Das ist **Option 2**. Es existiert noch **kein Zeile Code** dafür, das Pseudo-Signal-Problem ist nirgends benannt, und die Validierung sieht keine Gruppierung nach Stadtteil vor. Kein Eintrag im Decision Log. |
| 2 | Panel-Grid vollständig | **implementiert, aber mit Bug** | Reindex auf volles Kreuzprodukt ist vorhanden und verifiziert korrekt (11.357 Zeilen = 41 × 277 Monate, lückenlos). **Aber:** Die Randmonat-Logik schneidet nur den *letzten* Monat ab → siehe Befund B1. COVID/2020–21 ist weder ausgeschlossen noch per Dummy markiert noch in den Limitationen behandelt. |
| 3 | Zielgröße charakterisiert | **erledigt, außer Exposure** | Dispersionsindex 65,8 (Mittel 68,9 / Varianz 4.529) → NegBin bestätigt. Nullanteil 0,23 % → keine Zero-Inflation. **Exposure ist ungeklärt** → siehe Befund B3, das ist der inhaltlich schwerste Punkt. |
| 4 | Zeitliche Verfügbarkeit | **nur zur Hälfte gelöst** | ACS-Join ist auf „letzter Snapshot ≤ Einsatzjahr" umgestellt (Decision Log #4) – das behebt das *Zukunfts*-Leakage, **nicht** die Publikationsverzögerung: Einsätze aus 2023 erhalten ACS 2023, das erst Ende 2024 erscheint. Kriminalität: **nicht gelagged**, weil `crime_raw.parquet` keine Datumsspalte hat → statischer Fallback ist aktiv (verifiziert, Spalten: `neighborhood, incident_category, count, is_violent, is_property`). Bauliche Merkmale: Stichtag 2020, verifiziert **0 % Zeitvarianz**. |
| 5 | Zwei Feature-Sets | **erledigt** | Set S und Set S+L sind in `demo_modellierung.py` implementiert, identische Zeilen und Folds. Decision Log #8. Nichts zu tun. |
| 6 | Kalendermerkmale | **erledigt** | `monat_sin`/`monat_cos` vorhanden, rohes `jahr` bewusst nicht als Feature (Leitfaden A2). Nichts zu tun. |
| 7 | Split | **überwiegend erledigt, ein Loch** | Forward Chaining läuft über globale `jahr_monat`-Schnitte, alle Stadtteile teilen dieselbe Trennlinie – korrekt. **Gap ist hier nicht nötig**: alle Lag-/Rolling-Features sind strikt rückwärtsgerichtet (`shift(1)` vor `rolling`), ein Testmonat greift nie auf Zukunftswerte zu. Was **fehlt**: ein End-Hold-out. Fold 3 ist zugleich der letzte Zeitraum – beim Tuning wird darauf zwangsläufig geschaut. |
| 8 | Alles Lernende in der Pipeline | **erledigt für Ridge, offen für den Rest** | `make_pipeline(StandardScaler(), Ridge())`, Fit nur auf Train – korrekt, kein globaler Scaler. `log1p` ist parameterfrei, unkritisch. Fehlende Werte werden per `dropna` behandelt, nicht imputiert – zulässig, muss aber als Entscheidung stehen. Für die Klassifikation (One-Hot `bataillon`) existiert noch nichts. Kein `ColumnTransformer` – bei rein numerischen Features noch verzichtbar, bei der Klassifikation nicht mehr. |
| 9 | Stadtteil-Identität | **erledigt** | Keine Stadtteil-ID im Modell – entspricht der Empfehlung. Offen ist nur die Robustheitsvariante *mit* ID für den Anhang. |
| 10 | Multikollinearität & Linearität | **erledigt, ein Formfehler** | VIF (max. 10,0 Einkommen / 8,4 Miete) und Linearitätsprüfung inkl. Residuenplots liegen vor, Ridge-Entscheidung ist damit hergeleitet. **Beide wurden auf dem Gesamtdatensatz gerechnet, nicht nur auf Trainingsdaten.** Methodisch ist das im Kolloquium angreifbar, auch wenn es das Ergebnis kaum ändern wird. |
| 11 | Datenqualität am Join | **weitgehend offen** | Nicht dokumentiert: Anteil Einsätze ohne verwertbaren Stadtteilbezug, Zeilenzahl vor/nach jedem Join, Gewichtungsregel des Crosswalks (Mediane sind bevölkerungsgewichtet, Zähler/Nenner summiert – für Flächenmerkmale gilt das nicht), Plausibilitätscheck Σ Stadtteilbevölkerung ≈ Stadtbevölkerung. Bekannt: Land-Use-Match 99,5 %, Treasure Island + Lakeshore ohne ACS. |

---

## Teil 2 – Drei Befunde, die in keinem der Dokumente stehen

### B1 – Phantom-Monat Januar 2026 verzerrt den letzten CV-Fold (KRITISCH, Bugfix)

`aggregation.py` schneidet nur den *maximalen* `jahr_monat` ab. Die Rohdaten enthalten
2026-02 mit **1** Einsatz und 2026-01 mit **258** Einsätzen (bei einem Normalwert von ~3.300).
Abgeschnitten wird nur 2026-02. Januar 2026 bleibt als voller Monat im Panel – mit
39 Zeilen und einem Mittelwert von 6,5 statt ~78 Einsätzen.

Dieser eine Monat liegt im Testfenster des letzten Folds. Wirkung, nachgerechnet:

| letzter Fold | naive Baseline R² | RMSE |
|---|---|---|
| Test 2025-02 … 2026-01 (Ist-Zustand) | **0,740** | 38,2 |
| Test 2025-01 … 2025-12 (bereinigt) | **0,955** | 16,3 |

**Konsequenz:** Hürde **A2 im Umsetzungsleitfaden („Fold-Instabilität / Nichtstationarität,
Trendbrüche, COVID-Ära") ist eine Fehldiagnose.** Fold 3 bricht nicht wegen
Nichtstationarität ein, sondern wegen eines unvollständigen Randmonats. Alle
Fold-3-Zahlen in `results/demo_modellierung/` und die daraus abgeleitete Interpretation
in `CLAUDE.md` Abschnitt 3 sind zu verwerfen und neu zu rechnen.

**Fix:** Randmonat-Abschneidung an einem Vollständigkeitskriterium festmachen, nicht am
Maximum – z. B. alle Monate verwerfen, deren stadtweite Einsatzzahl unter einem Bruchteil
(z. B. 50 %) des Median-Monats liegt, oder schlicht ein hart gesetztes `ENDE = 202512`
als Konstante im Code. Letzteres ist für eine Abschlussarbeit die ehrlichere Variante,
weil reproduzierbar und dokumentierbar.

### B2 – Kriminalitätsmerkmale enthalten weiterhin Zukunftsinformation

Verifiziert: `anteil_gewaltdelikte_pct` und `anteil_eigentumsdelikte_pct` haben über den
gesamten Zeitraum **0 % Zeitvarianz** und **100 % Between-Stadtteil-Varianz**. Sie sind
kumulierte Anteile über 2003–2026, berechnet aus Daten, die zum Trainingszeitpunkt
noch nicht existierten. Der zeitbewusste Code in `02_join.py` (`aggregate_crime_zeitbewusst`)
ist geschrieben, greift aber nicht, weil die lokale Rohdatei keine Datumsspalte hat.
Decision Log #3 markiert das korrekt als offen – es ist **noch nicht erledigt**.

Nebenbefund: auch der zeitbewusste Pfad liefert nur eine **kumulierte Jahressumme bis
Vorjahr**. Als Anteil (Gewalt/Gesamt) ist das über die Jahre nahezu konstant und trägt
faktisch keine Dynamik. Wenn die Kriminalitätsmerkmale einen echten Zeitbeitrag leisten
sollen, brauchst du **Jahres- oder Monatswerte statt kumulierter Summen**, sinnvollerweise
als Rate je 1.000 Einwohner mit Lag t−1.

### B3 – Der Struktur-Befund kippt bei Bevölkerungsnormierung das Vorzeichen

Nachgerechnet auf Stadtteil × Monat (2014–2026, 5.571 Zeilen, 39 Stadtteile):

| Prädiktor | r mit absoluter Einsatzzahl | r mit Einsätzen je 1.000 Einwohner |
|---|---|---|
| `armutsquote_pct` | **+0,198** | **−0,125** |
| `anteil_risikogewerbe_pct` | **+0,700** | **−0,122** |
| `gesamtbevoelkerung` | +0,395 | – |

Der in der Eignungsprüfung berichtete OLS-R² von 0,74 und der „stärkste Prädiktor
Risikogewerbe r=0,69" beschreiben also ganz überwiegend **Stadtteilgröße und -dichte**,
nicht Risiko. Bei Normierung auf die Wohnbevölkerung dreht sich das Vorzeichen –
was seinerseits nicht die „richtigere" Zahl ist (Financial District und Tenderloin haben
eine Tagbevölkerung weit über der Wohnbevölkerung), sondern zeigt: **die Exposure-Frage
entscheidet die inhaltliche Aussage der Arbeit.** Das ist die Frage, die im Kolloquium
mit Sicherheit kommt, und sie ist im Repo nirgends adressiert.

---

## Teil 3 – Was für Data Preparation konkret noch zu tun ist

Reihenfolge = Abarbeitungsreihenfolge. P1 blockiert die Modellierung, P2 ist vor der
Ergebnisinterpretation fällig, P3 ist Dokumentation für Kapitel 5.2.

### P1 – blockierend, vor jedem weiteren Modelllauf — **[ERLEDIGT bis auf P1.4]**

1. **[ERLEDIGT]** **Randmonat-Bug gefixt** (B1): Konstante `ENDE = 202512` in
   `aggregation.py`, zusätzlich `pruefe_randmonate()` als Warnung für künftige
   Downloads. Demo neu gerechnet, `CLAUDE.md` §3 ersetzt. **Leitfaden A2 muss noch
   umgeschrieben werden** (von „Nichtstationarität" zu „Datenartefakt, behoben").
2. **[ERLEDIGT]** **Exposure** (B3/E1): `log_bevoelkerung` ersetzt `gesamtbevoelkerung`
   in `PRAEDIKTOREN`; der Rohwert bleibt als Spalte erhalten für den NegBin-Offset und
   die Raten-Sensitivität. Decision Log #13.
3. **[ERLEDIGT]** **ACS-Publikationsverzögerung** (E2): `ACS_PUBLIKATIONS_LAG = 1` in
   `02_join.py`, Bedingung jetzt `acs_jahr ≤ Einsatzjahr − 1`. Pipeline (02 → 03) neu
   gerechnet. Analysezeitraum startet nun 2015. Decision Log #11.
4. **[VERTAGT]** **Crime zeitbewusst** – bewusst offen gelassen (Decision Log #16).
   Randbedingung: DataSF `e3si-785i` deckt nur 2018-01 bis 2026-07 ab; ein Lag-t−1-Join
   wäre erst ab 2019 möglich und kostete ~37 % der Beobachtungen. Bis zur Entscheidung
   gilt: Kriminalitätsmerkmale sind **querschnittlich** zu formulieren.
5. **[ERLEDIGT]** **End-Hold-out** (Punkt 7): `modellierung/cv.py` neu angelegt, letzte
   12 Monate (2025-01–2025-12) als Hold-out, beim Tuning unberührt; zusätzlich
   `inneres_fenster()` für die Hyperparameter-Suche. Decision Log #14.
6. **[NEU, ERLEDIGT]** **Balanciertes Panel**: Beim Umbau zeigte sich, dass Mission Bay
   erst ab ACS 2021 als eigene Analyseeinheit existiert. Zeilenweises `dropna` hätte ein
   unbalanciertes Panel erzeugt (Stadtteil tritt mitten in der Zeitreihe hinzu →
   Testfenster-Summen springen). `balanciertes_panel()` schließt Stadtteile ohne
   durchgängige Abdeckung aus → **38 statt 39 Stadtteile**. Decision Log #15.

### P2 – vor der Ergebnisinterpretation

6. **Klassifikationsteil aufsetzen** (Punkt 1): Zielvariable `ist_brand` (NFIRS 100er),
   Ebene Einzeleinsatz, One-Hot `bataillon` einheitlich für alle drei Modelle im
   `ColumnTransformer`, Split über dieselben Monatsgrenzen. Pseudo-Signal-Problem
   (Stadtteilmerkmale wiederholen sich über alle Einsätze desselben Stadtteils) als
   Absatz im Methodenkapitel. Decision-Log-Eintrag ergänzen.
7. **VIF und Linearitätsprüfung auf Trainingsdaten neu rechnen** (Punkt 10) – Ergebnis
   wird sich kaum ändern, der Formfehler verschwindet.
8. **COVID-Behandlung festlegen** (Punkt 2, Entscheidung E3).
9. **Robustheitsvariante mit Stadtteil-ID** rechnen (Punkt 9) – die Differenz der
   Gütemaße ist dein Maß für unbeobachtete Stadtteil-Heterogenität und liefert ein
   starkes Limitationen-Argument.

### P3 – Dokumentation für Kapitel 5.2

10. **Feature-Inventar** als Tabelle: Spalte, Quelle, Aggregationsebene, zeitliche
    Auflösung, ab wann real verfügbar, NaN-Anteil, Berechnungslogik. Die beiden
    fehlenden Spalten (Ebene, Verfügbarkeit) einbauen – das ist gleichzeitig die
    Rohfassung von Kapitel 5.1/5.2.
11. **Join-Protokoll** (Punkt 11): Zeilenzahl vor/nach jedem Join, Anteil verworfener
    Einsätze ohne Stadtteilbezug, Gewichtungsregel Crosswalk, Plausibilitätscheck
    Σ Stadtteilbevölkerung vs. SF-Gesamtbevölkerung.
12. **Fairness-Satz wörtlich ins Methodenkapitel**: „Rohdaten, Splits und Gütemaße sind
    für alle drei Verfahren identisch; modellspezifische Aufbereitung (Standardisierung
    und log-Transformation für Ridge) findet ausschließlich innerhalb der jeweiligen
    sklearn-Pipeline je Fold statt und ist dokumentiert." Nimmt die naheliegende
    Kolloquiumsfrage vorweg.

---

## Teil 4 – Zu treffende Entscheidungen und ihre Auswirkungen

### E1 – Exposure: absolute Einsatzzahl oder Rate? (höchste Tragweite)

| Option | Auswirkung |
|---|---|
| **A – absolute Counts beibehalten**, `gesamtbevoelkerung` bleibt Feature (Ist-Zustand) | Kein Aufwand. Aber die Arbeit sagt faktisch Stadtteilgröße vorher; Armut und Risikogewerbe erscheinen wichtig, weil große dichte Stadtteile viele Einsätze haben. R² bleibt hoch (0,74 OLS), die inhaltliche Aussage ist angreifbar. **Muss dann mindestens als Limitation ausgeschrieben werden.** |
| **B – log(Bevölkerung) als Offset** (NegBin) bzw. als Feature (Ridge/RF/XGB) | Statistisch die sauberste Variante für Zähldaten, Zielgröße bleibt Count → F1/RMSE/Gütemaße unverändert vergleichbar, Exposé-treu. Interpretation der Koeffizienten wird zur Rate. **Empfehlung.** |
| **C – Zielgröße auf Einsätze je 1.000 Einwohner umstellen** | Inhaltlich am klarsten, aber: keine Zähldaten mehr → NegBin-Baseline und die gesamte Overdispersion-Argumentation entfallen, Decision Log #2 und die Eignungsprüfung müssen neu geschrieben werden. Hoher Umbauaufwand. |
| **D – beides berichten** (Hauptmodell A oder B, Sensitivität C) | Beste Absicherung im Kolloquium, ein zusätzlicher Rechendurchlauf. Realistisch, weil die Pipeline ohnehin parametrisiert ist. |

Empfehlung: **B als Hauptmodell, C als Sensitivitätsanalyse in einem Absatz.** Damit bleibt
die Count-Argumentation (NegBin, Overdispersion) intakt und der Größeneffekt ist kontrolliert.

### E2 – ACS-Verzögerung: welcher Versatz?

| Option | Auswirkung |
|---|---|
| **kein Versatz** (Ist-Zustand) | Modell nutzt Daten, die zum Prognosezeitpunkt nicht publiziert waren → nicht implementierbar, klassischer Kolloquiumseinwand. |
| **+1 Jahr** | Realistisch (ACS 5-Jahr für y erscheint ~Dez y+1). Analysezeitraum beginnt dann 2015 statt 2014, ca. −470 Beobachtungen. **Empfehlung.** |
| **+2 Jahre** | Maximal konservativ, sicher implementierbar. Kostet ein weiteres Jahr und macht die Merkmale noch träger. |

### E3 – Umgang mit 2020/2021

| Option | Auswirkung |
|---|---|
| **Ausschluss 2020–2021** | Sauberes Panel, aber −936 Beobachtungen (~17 %) und ein Loch mitten in der Zeitreihe → Lag-12-Features brechen. **Nicht empfohlen.** |
| **Dummy `ist_pandemie`** | Billig, transparent, Bäume und Ridge können ihn nutzen. Kostet ein Feature. **Empfehlung.** |
| **nur Limitation** | Kein Aufwand, aber die Fold-Ergebnisse tragen den Strukturbruch unerklärt mit. |

Hinweis: Der empirische Einbruch ist moderat (Jahresmittel 69,3 → 62,6 → 65,4), also
kein dramatischer Bruch. Ein Dummy plus zwei Sätze in Kapitel 6 reicht.

### E4 – Klassifikationsebene (Punkt 1)

Die Entscheidung ist faktisch gefallen (Einzeleinsatz, **Option 2**) und ist die
exposénäheste. Zu tun bleibt nur: **Decision-Log-Eintrag** plus **expliziter Absatz zum
Pseudo-Signal-Problem**. Auswirkung bei Nichtbehandlung: der Vorwurf, die
Klassifikationsergebnisse seien durch 41 sich wiederholende Stadtteilprofile künstlich
aufgebläht, bleibt unbeantwortet. Alternative Option 3 (getrennte Counts je Kategorie)
wäre kohärenter, verlangt aber ein neues Zielgrößen-Design und den Verzicht auf
F1/AUROC – gegenüber dem Exposé eine größere Abweichung als das Pseudo-Signal-Problem.

### E5 – Kriminalitätsmerkmale, falls der Neu-Download scheitert

| Option | Auswirkung |
|---|---|
| **Neu-Download + Jahreswerte mit Lag t−1** | Merkmale werden echt zeitvariant, Leakage weg. **Empfehlung.** |
| **statisch belassen, als Querschnittsmerkmal deklarieren** | Ehrlich, aber die Merkmale tragen dann keinerlei Dynamik und die Formulierung „kriminalitätsbezogene Merkmale sagen Einsätze vorher" ist nicht mehr haltbar – nur noch „beschreiben Niveauunterschiede". |
| **streichen** | Widerspricht dem Exposé (Kriminalität ist eine der drei Merkmalsgruppen). Nicht empfohlen. |

### E6 – `akademikerquote_pct` (49 % NaN)

Ist-Zustand: `dropna` → Zeitraum 2014–2026, 5.571 Zeilen, 39 Stadtteile. Das ist die
ehrliche Variante und bereits umgesetzt (Decision Log #5, #10). Mit E2 (+1 Jahr)
verschiebt sich der Start auf 2015. **Keine neue Entscheidung nötig**, nur die
Sensitivitätsanalyse „voller Zeitraum ohne dieses Feature" steht noch aus.

---

## Kurzfassung

Von den 11 Punkten sind **4 vollständig erledigt** (5, 6, 9 und im Kern 3),
**4 teilweise** (0, 2, 7, 10), **3 offen** (1, 4, 11).
Dazu kommen drei im Repo nicht dokumentierte Befunde: der **Phantom-Monat 2026-01**
(entwertet Fold 3 und die A2-Diagnose), die **weiterhin statischen Kriminalitätsmerkmale**,
und der **Größeneffekt**, der den zentralen Struktur-Befund bei Bevölkerungsnormierung
im Vorzeichen kippt. Der Bugfix (P1.1) ist billig und dringend; die Exposure-Entscheidung
(E1) ist die inhaltlich folgenreichste und sollte vor dem nächsten Sprechstundentermin
mit Schröter geklärt werden.
