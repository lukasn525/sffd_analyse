# Umbau: eine Preprocessing-Pipeline, ein Befehl, zwei Datensätze

Stand 2026-07-27 · **Status: UMGESETZT.** Alle 12 Schritte aus Abschnitt 8 sind
abgeschlossen, `python prep/build.py` läuft in ~35 s durch, `tests/` zeigt 14/14.
Der Nachweis der Bitgleichheit (Schritt 4) ist erbracht:
`pd.testing.assert_frame_equal` gegen den eingefrorenen Referenzstand bestanden,
alle 22 Spalten der 4.200 alten Zeilen zellengleich. Erst danach wurde der
Lag-Vorlauf aktiviert.

**Nachtrag 2026-07-27 (Decision Log #24):** Ein anschliessender Audit der beiden
Datensätze fand zwei Prädiktoren mit pandas-eigenem Typ `Int64` – für XGBoost
unbrauchbar. Alle Merkmale sind jetzt `float64`. Ausserdem wurden
`regression_datensatz.py` und `klassifikation_datensatz.py` zu
`prep/datensaetze.py` zusammengelegt: **7 statt 9 Dateien**, und die
Klassifikation muss den Regressionsdatensatz nicht mehr von der Platte lesen.

Dieses Dokument bleibt als Begründung und Nachweis erhalten (Decision Log #22,
#23, #24). Wer wissen will, wie die Struktur *heute* aussieht, liest
`ORIENTIERUNG.md`.

**Ziel:** `python prep/build.py` erzeugt aus den Rohdaten die beiden finalen
Datensätze in `data/processed/`. Diese beiden Dateien sind das Einzige, was die
drei Algorithmen jemals lesen.

**Entschieden (2026-07-27):**

- `prep/` enthält **alles außer den Algorithmen selbst** — also auch CV-Splits,
  Suchräume und die Eignungsprüfung.
- Der **Lag-Vorlauf** wird eingebaut: die Regression beginnt künftig ebenfalls
  2015-01 statt 2016-01.
- Es gibt **kein eigenes Tuning-Skript**. Suchräume stehen in `prep/config.py`,
  die Suche läuft im jeweiligen Modellskript.

---

## 1. Warum es aktuell wirr ist

Nicht weil der Code schlecht wäre, sondern weil er in drei Etappen gewachsen ist
und die Struktur nie nachgezogen wurde. Fünf Ursachen:

| # | Problem | Konkret |
|---|---|---|
| 1 | **Preprocessing liegt in zwei Ordnern** | `pipeline/` (Rohdaten→Quoten) und `modellierung/` (Panel→Lags) sind beide Data Preparation. Die Grenze verläuft mitten durch die Aufbereitung. |
| 2 | **Namenskollision** | `pipeline/03_features.py` und `modellierung/features.py` meinen völlig Verschiedenes. |
| 3 | **Fünf Befehle statt einem** | `01_fetch` → `02_join` → `03_features` → `aggregation` → `features`. Die Reihenfolge muss man wissen. |
| 4 | **Der finale Datensatz existiert nicht als Datei** | Er entsteht nur im Speicher. Man kann ihn nicht ansehen, nicht prüfen, nicht als Beleg beilegen. |
| 5 | **Konstanten an vier Stellen** | `ACS_YEARS` steht **doppelt** (`01_fetch.py` *und* `02_join.py`). Zeitraum in `aggregation.py`, Merkmalssätze in `features.py`, ACS-Versatz in `02_join.py`. |

---

## 2. Zielstruktur — drei Ordner statt fünf

```text
sffd_analyse/
│
├── prep/                              ◀── alles Festgelegte. Ein Befehl.
│   ├── config.py                      #  EINZIGE Wahrheit: Pfade · Zeitraum ·
│   │                                  #  Stadtteil-Ausschlüsse · Prädiktoren ·
│   │                                  #  Merkmalssätze S / S+L · ACS-Versatz ·
│   │                                  #  DOWNLOAD_*-Schalter · API-Keys ·
│   │                                  #  CV-Konfiguration · Suchräume
│   ├── download.py                    #  DataSF + Census → data/raw
│   ├── join.py                        #  data/raw → Einsatz-Ebene + Quoten
│   ├── regression_datensatz.py        #  → data/processed/regression.parquet
│   ├── klassifikation_datensatz.py    #  → data/processed/klassifikation.parquet
│   ├── cv.py                          #  Zeitschnitte · Folds · Hold-out · Gütemaße
│   ├── eignungspruefung.py            #  Linearität · VIF · Overdispersion · Balance
│   ├── deskriptiv.py                  #  beschreibende Kennzahlen (Kap. 5.1)
│   ├── spaltennamen.py                #  englisch → deutsch
│   └── build.py                       ◀── DER EINE BEFEHL
│
├── modelle/                           ◀── nur was tatsächlich schätzt
│   ├── baselines.py                   #  naiv · saisonal · NegBin
│   ├── train_regression.py            #  Ridge · Random Forest · XGBoost
│   ├── train_klassifikation.py        #  dieselben drei, 4 Klassen
│   └── shap_analyse.py
│
├── tests/test_aufbereitung.py
│
└── data/
    ├── raw/                           #  unverändert
    └── processed/
        ├── einsaetze.parquet          #  Zwischenstand, Einsatz-Ebene
        ├── regression.parquet     ◀── FINAL   4.620 × 23
        └── klassifikation.parquet ◀── FINAL 350.481 × 24
```

`analyse/` verschwindet, der Inhalt zieht nach `prep/`. **Drei Ordner: `prep`, `modelle`, `tests`.**

Dateinamen kollidieren nirgends mehr: `regression_datensatz.py` (erzeugt Daten)
gegen `train_regression.py` (schätzt Modelle) ist auch beim Überfliegen eindeutig.

### Der eine Befehl

```powershell
python prep\build.py
```

Ablauf: `download` → `join` → beide Datensätze → Kurzbericht (Zeilen, Spalten,
Zeitraum, Stadtteile je Datei). Die `DOWNLOAD_*`-Schalter stehen in `config.py`
und sind per Default alle `False`; dann läuft der Befehl in unter einer Minute
aus `data/raw`. Auf `True` gestellt lädt derselbe Befehl vorher neu.

Jeder Schritt bleibt einzeln aufrufbar (`python prep\join.py`).

---

## 3. Wo `cv.py` hingehört — meine Bewertung

**In `prep/`.** Drei Gründe:

1. **Es schätzt nichts.** `cv.py` enthält Zeitschnitte, Foldgrenzen, das innere
   Validierungsfenster und die Formeln für RMSE/MAE/R²/F1/AUROC. Alles davon sind
   Festlegungen über die Daten, keine Ergebnisse.
2. **Es wird von beiden Seiten gebraucht.** Nicht nur von den Modellen, sondern
   auch von der Eignungsprüfung (VIF und Linearität dürfen nur auf Trainingsdaten
   gerechnet werden — dafür braucht sie die Foldgrenzen). Läge `cv.py` in
   `modelle/`, müsste `prep/` aus `modelle/` importieren.
3. **Die Fairness-Regel ist eine Preprocessing-Zusage.** „Alle drei Verfahren
   sehen identische Folds" ist eine Aussage über den Datensatz, nicht über die
   Algorithmen.

**Zusätzlich schreibt `regression_datensatz.py` zwei Spalten mit:** `fold`
(1/2/3, sonst leer) und `ist_holdout` (0/1). Damit steht die Aufteilung
nachzählbar in der Datei, statt von einem korrekten Funktionsaufruf abzuhängen.
`cv.py` erzeugt diese Spalten und behält die Gütemaße.

---

## 4. Der Lag-Vorlauf: Regression beginnt künftig 2015

### Warum es heute 2016 ist

Das Panel läuft 2015-01 – 2025-12. `lag_12` für Januar 2015 bräuchte Januar 2014 —
den gibt es im Panel nicht, also entfernt `dropna` das erste Jahr je Stadtteil.

### Warum das unnötig ist

Die Lags brauchen ausschließlich `anzahl_einsaetze` aus der Vergangenheit — keine
ACS-Merkmale. Der Grund für START = 2015 ist die Akademikerquote
(ACS-Publikationsversatz, #11), und die betrifft nur die Prädiktoren der
**Zielzeile**, nicht deren Vergangenheitswerte. Einsatzzahlen liegen bis 2003 vor.

### Umsetzung

`config.py` bekommt `VORLAUF_MONATE = 12`. `regression_datensatz.py` aggregiert
ab `2014-01`, bildet die Lags, **schneidet danach** auf `START = 2015-01` zu.

| | vorher | nachher |
|---|---|---|
| Modellzeilen | 4.200 | **4.620** (+10 %) |
| Zeitraum Regression | 2016-01 – 2025-12 | **2015-01 – 2025-12** |
| Zeitraum Klassifikation | 2015-01 – 2025-12 | unverändert |
| Testfenster der 3 Folds | 2022 · 2023 · 2024 | **identisch** |
| Trainingsfenster | | 12 Monate länger |
| End-Hold-out | 2025 | unverändert |

Der wichtige Punkt: **die Testfenster verschieben sich nicht.** Es kommt nur
Trainingsmaterial hinzu. Die Demo-Kennzahlen ändern sich dadurch leicht und sind
neu zu rechnen, aber der Vergleich zwischen den Verfahren bleibt strukturgleich.

Leakage-Prüfung: Die Vorlaufmonate gehen ausschließlich über `shift()` ein, nie
als eigene Zeile. Ein Testmonat greift weiterhin nie auf Werte nach seinem
eigenen Zeitpunkt zu. `tests/test_aufbereitung.py` prüft das bereits
(`test_lags_nicht_gegenwartsbezogen`) und muss nur neue Sollzahlen bekommen.

→ **Decision Log #23** (Lag-Vorlauf, Zeitraumangleichung beider Datensätze).

---

## 5. Tuning ohne Tuning-Skript

Ein separates `tuning.py` erzeugt ein Übergabeproblem: Die besten Parameter landen
in einer JSON, und ab dann hängt die Reproduzierbarkeit an einer Datei, die
veralten kann, ohne dass es jemand merkt.

Stattdessen:

- **Suchräume** (`alpha ∈ [1e-3, 1e3]`, `n_estimators 200–1000`, …) stehen als
  Dict `SUCHRAEUME` in `prep/config.py` — das ist Konfiguration, kein Ergebnis,
  und gehört damit nach deiner Regel in `prep/`.
- **Budget** (50 Iterationen je Modell, identisch für alle — Fairness) ebenfalls dort.
- Die `RandomizedSearchCV` selbst läuft in `modelle/train_regression.py` auf den
  Folds aus `prep/cv.py`, in einem Durchlauf mit dem finalen Training.

Damit steht jede Festlegung in `prep/`, und in `modelle/` liegt nur, was rechnet.

---

## 6. Die beiden finalen Datensätze

Zwei, weil die **Analyseebenen** verschieden sind (Exposé, Kap. 3) — eine Datei
würde bedeuten, die 4.620 Panelzeilen 76-fach zu duplizieren.

### `regression.parquet` — 4.620 Zeilen × 23 Spalten

```text
Schlüssel      stadtteil · jahr · monat · jahr_monat
Zielgröße      anzahl_einsaetze
Struktur (10)  median_haushaltseinkommen · armutsquote_pct · akademikerquote_pct
               median_miete · leerstandsquote_pct · log_bevoelkerung
               log_kriminalitaetsindex · anteil_altbau_vor_1940_pct
               anteil_wohngebaeude_pct · anteil_risikogewerbe_pct
Saison (2)     monat_sin · monat_cos
Lags (3)       lag_1 · lag_12 · rolling_mean_3
Aufteilung     fold · ist_holdout                                    ← neu
Nebenrechnung  gesamtbevoelkerung (NegBin-Offset) · kriminalitaetsindex (roh)
```

### `klassifikation.parquet` — 350.481 Zeilen × 24 Spalten

```text
Schlüssel      einsatz_nummer · stadtteil · jahr · monat · jahr_monat
Zielgrößen     einsatzart_gruppe (4 Klassen) · ist_brand
Block A (10)   identisch zur Struktur oben
Block B (6)    stunde_sin · stunde_cos · monat_sin · monat_cos · ist_nacht · ist_wochenende
Kategorial     wochentag   (One-Hot erst im ColumnTransformer)
Aufteilung     fold · ist_holdout
Optional       bataillon   (Robustheitslauf, Schalter in config.py)
```

Beide Dateien teilen garantiert dieselbe Abgrenzung:
`klassifikation_datensatz.py` importiert Zeitraum und Stadtteilliste aus
`regression_datensatz.py`. **Ergebnisvariablen** (Sachschaden, Alarmstufe,
Antwortzeit …) sind in beiden ausgeschlossen und werden von `tests/` geprüft.

Merkmalssätze **S** und **S+L** sind Spaltenlisten in `config.py`, kein eigener Datensatz.

---

## 7. Was wohin wandert

| Heute | Künftig | Anmerkung |
|---|---|---|
| `pipeline/01_fetch.py` | `prep/download.py` | Konstanten nach `config.py`, Logik unverändert |
| `pipeline/02_join.py` | `prep/join.py` | + Quotenberechnung aus `03_features.py` |
| `pipeline/03_features.py` | *entfällt* | ~40 Zeilen, künstlich abgetrennt |
| `pipeline/column_names.py` | `prep/spaltennamen.py` | nur umbenannt |
| `pipeline/run_pipeline.py` | `prep/build.py` | ruft jetzt auch beide Datensätze |
| `modellierung/aggregation.py` + `features.py` | `prep/regression_datensatz.py` | zusammengelegt, **schreibt eine Datei** |
| `modellierung/klassifikation_daten.py` | `prep/klassifikation_datensatz.py` | **schreibt eine Datei** |
| `modellierung/cv.py` | `prep/cv.py` | + erzeugt `fold` / `ist_holdout` |
| `analyse/eignungspruefung.py` | `prep/eignungspruefung.py` | Folds jetzt aus `prep/cv.py` |
| `analyse/deskriptiv.py` | `prep/deskriptiv.py` | liest `einsaetze.parquet` |
| `analyse/dashboard.py` | *entfällt* | explorativ, nicht in der Abgabe |
| `modellierung/demo_modellierung.py` | `modelle/baselines.py` + `train_regression.py` | wird beim Ausbau aufgeteilt |

**Netto:** 13 Dateien in 3 Ordnern → **13 Dateien in 2 Ordnern**, davon 10 in `prep/`.
Aufrufe für den Datensatz: 5 → **1**. Zwischen-Parquets: 3 → **1**.

---

## 8. Reihenfolge der Umsetzung

`pipeline/`, `modellierung/` und `analyse/` bleiben unangetastet, bis Schritt 8 grün ist.

| # | Schritt | Prüfung |
|---|---|---|
| 0 | **Referenz einfrieren:** heutigen Modelldatensatz als `_referenz.parquet` | 4.200 Zeilen |
| 1 | `prep/config.py` — alle Konstanten an einen Ort, `ACS_YEARS`-Doppelung auflösen | – |
| 2 | `prep/spaltennamen.py`, `prep/download.py` | läuft mit allen Schaltern `False` folgenlos durch |
| 3 | `prep/join.py` → `einsaetze.parquet` | Zeilen/Spalten wie heutiges `sf_fire_risk_features.parquet` |
| 4 | `prep/regression_datensatz.py`, **noch ohne Vorlauf** | **bitgleich zu `_referenz.parquet`** |
| 5 | Vorlauf aktivieren (`VORLAUF_MONATE = 12`) | 4.620 Zeilen ab 2015-01; die 4.200 alten Zeilen bleiben zellengleich |
| 6 | `prep/cv.py` + `fold`/`ist_holdout`-Spalten | Testfenster weiterhin 2022/23/24, Hold-out 2025 |
| 7 | `prep/klassifikation_datensatz.py` | 350.481 Zeilen, 35 Stadtteile, 13,6 % Brand |
| 8 | `prep/build.py`, `tests/` auf neue Imports + neue Sollzahlen | **11/11** |
| 9 | `prep/eignungspruefung.py`, `prep/deskriptiv.py` | Kennzahlen wie bisher (bis auf den Vorlauf-Effekt) |
| 10 | `modelle/` anlegen, Demo aufteilen, Zahlen neu rechnen | Ranking der Verfahren bleibt |
| 11 | **Erst jetzt:** `pipeline/`, `modellierung/`, `analyse/` löschen | – |
| 12 | Doku: `README` · `ORIENTIERUNG` · `CLAUDE.md` · `ABGABE.md` · `DATA_DICTIONARY` | Dateinamen und Zahlen stimmen |

Schritt 4 ist die eigentliche Absicherung: Ist der neue Datensatz Zelle für Zelle
identisch, ist der Umbau nachweislich **reine Umstrukturierung**. Erst Schritt 5
ändert bewusst etwas — sauber getrennt und einzeln belegbar. Genau diese Trennung
braucht das Methodenkapitel.

---

## 9. Was sich ausdrücklich NICHT ändert

Decision Log #1–#21 gilt unverändert; es ändern sich nur Dateinamen in den Verweisen:

- 35 Stadtteile · Parkgebiete ausgeschlossen · balanciertes Panel
- ACS-Publikationsversatz +1 Jahr · Kriminalitätsindex als Location Quotient
- `ffill` ohne `bfill` · Exposure `log_bevoelkerung`
- Sortierung `(jahr_monat, stadtteil)` — **Reproduzierbarkeitsvertrag**, wird 1:1
  übernommen, sonst weichen RF und XGBoost trotz gleichem `random_state` ab
- Lag-Logik `shift` vor `rolling` · Fairness-Regel · End-Hold-out 2025

**Neue Decision-Log-Einträge:**

- **#22** Restrukturierung ohne inhaltliche Änderung (Nachweis: Bitgleichheit, Schritt 4)
- **#23** Lag-Vorlauf, Regression ab 2015-01, beide Datensätze deckungsgleich

Zu #22: Der Umbau weicht von der in `CLAUDE.md` Abschnitt 4 als „bestehende
Prep-Pipeline (nicht verändern)" dokumentierten Struktur ab. Inhaltlich passiert
nichts — aber es gehört dokumentiert und Schröter gegenüber erwähnt.

---

## 10. Wie es danach aussieht

```python
# modelle/train_regression.py – vollständiger Dateneinstieg:
import pandas as pd
from prep.config import FEATURE_SETS, SUCHRAEUME, PFAD_REGRESSION

d = pd.read_parquet(PFAD_REGRESSION)
train = d[d["fold"] == 1]
X, y = train[FEATURE_SETS["S+L"]], train["anzahl_einsaetze"]
```

Keine Aufrufreihenfolge, keine Nebenwirkungen. Und auf „woher kommt diese Spalte?"
gibt es genau drei mögliche Antworten: `join.py`, `regression_datensatz.py`,
`klassifikation_datensatz.py`.
