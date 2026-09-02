"""Funktionslandkarte - erzeugt docs/10_FUNKTIONSLANDKARTE.md aus dem Quelltext.

    python tools/landkarte.py

Zweck: Fuer jede Funktion des Repos beantworten, wo sie steht, was sie tut und
wer sie ruft. Tabellen und Groessenangaben werden bei jedem Lauf neu gemessen -
deshalb kann die Ausgabe nicht veralten. Die Fliesstexte zwischen den Tabellen
stehen als Zeichenketten in diesem Skript und sind von Hand geschrieben; wer
sie aendern will, aendert sie hier, nicht in der erzeugten Datei.

Warum der Aufrufgraph importbewusst ist: Im Repo gibt es gleichnamige Helfer in
verschiedenen Dateien (`_md`, `bericht`, `main`, `ein_lauf`). Ein rein
namensbasierter Graph wiese sie wechselseitig als Aufrufer voneinander aus. Ein
Aufruf zaehlt hier nur, wenn die rufende Datei den Namen importiert hat.

NICHT TEIL DER ARBEIT - Hilfsmittel wie der Rest von tools/.

Prueffauftraege
---------------
1  Stimmt die gemeldete Zahl der Funktionen mit `grep -c "^def "` ueber alle
   Dateien ueberein? Weicht sie ab, hat eine Datei einen Syntaxfehler und wurde
   stillschweigend uebersprungen.
2  Hat jede Datei aus prep/, vorpruefung/, modelle/ und tests/ einen
   Fliesstext? Neue Dateien bekommen sonst nur eine Tabelle ohne Einordnung.
3  Nennen die Fliesstexte noch existierende Funktionsnamen? Nach einer
   Umbenennung zeigt die Tabelle den neuen, die Prosa den alten.
"""
import ast, io, json, os, datetime, tokenize
from collections import defaultdict
from pathlib import Path


R = Path(__file__).resolve().parents[1]
ORD = ["prep", "vorpruefung", "modelle", "tests", "tools"]
dateien = []
for o in ORD:
    dateien += sorted((R/o).rglob("*.py"))

modul_von = {}                      # modulname -> rel-Pfad
for p in dateien:
    modul_von[p.stem] = str(p.relative_to(R)).replace("\\", "/")

info, importiert, mod_import = {}, defaultdict(dict), defaultdict(set)

for p in dateien:
    rel = str(p.relative_to(R)).replace("\\", "/")
    src = p.read_text(encoding="utf-8", errors="replace")
    tree = ast.parse(src)
    info[(rel, "__modul__")] = dict(moddoc=ast.get_docstring(tree) or "")
    # welche Namen holt diese Datei woher?
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module in modul_von:
            for al in n.names:
                importiert[rel][al.asname or al.name] = modul_von[n.module]
        elif isinstance(n, ast.Import):
            for al in n.names:
                if al.name in modul_von:
                    mod_import[rel].add(al.asname or al.name)
    for f in [n for n in ast.walk(tree)
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
        d = ast.get_docstring(f)
        erste = next((z.strip() for z in (d or "").splitlines() if z.strip()), "")
        a = f.args
        args = [x.arg for x in list(a.posonlyargs)+list(a.args)+list(a.kwonlyargs)]
        end = getattr(f, "end_lineno", f.lineno)
        info[(rel, f.name)] = dict(datei=rel, name=f.name, von=f.lineno, bis=end,
                                   laenge=end-f.lineno+1, args=args, doc=erste,
                                   ruf=set())

# Aufrufe aufloesen: Ziel ist (zieldatei, zielname)
for p in dateien:
    rel = str(p.relative_to(R)).replace("\\", "/")
    tree = ast.parse(p.read_text(encoding="utf-8", errors="replace"))
    for f in [n for n in ast.walk(tree)
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
        for n in ast.walk(f):
            if not isinstance(n, ast.Call):
                continue
            fn, ziel = n.func, None
            if isinstance(fn, ast.Name):
                nm = fn.id
                if (rel, nm) in info:              # lokal definiert
                    ziel = (rel, nm)
                elif nm in importiert[rel]:        # from x import nm
                    ziel = (importiert[rel][nm], nm)
            elif isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Name):
                nm0 = fn.value.id
                if nm0 in mod_import[rel] and nm0 in modul_von:
                    ziel = (modul_von[nm0], fn.attr)
                elif nm0 in importiert[rel]:       # from x import y as m; m.nm()
                    ziel = (importiert[rel][nm0], fn.attr)
            if ziel and ziel in info and ziel != (rel, f.name):
                info[ziel]["ruf"].add(f"{rel}:{f.name}")

for k, v in info.items():
    if "ruf" in v:
        v["ruf"] = sorted(v["ruf"])

D = {f'{k[0]}::{k[1]}': v for k, v in info.items()}


def messe(pfad):
    """LOC brutto und netto (ohne Leerzeilen, Kommentare, Docstrings)."""
    src = pfad.read_text(encoding="utf-8", errors="replace")
    L = src.splitlines()
    leer = sum(1 for z in L if not z.strip())
    kom = set()
    try:
        for tk in tokenize.generate_tokens(io.StringIO(src).readline):
            if tk.type == tokenize.COMMENT:
                kom.add(tk.start[0])
    except Exception:
        pass
    st = sum(1 for i in kom if L[i-1].strip().startswith("#"))
    dok = 0
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                          ast.AsyncFunctionDef)):
            d = ast.get_docstring(n, clean=False)
            if d:
                dok += len(d.splitlines())
    return len(L), len(L) - leer - st - dok


def de(n):
    return f"{n:,}".replace(",", ".")


fkt = {}
for k, v in D.items():
    datei, name = k.split("::")
    if name == "__modul__":
        continue
    fkt.setdefault(datei, []).append(v)
for d in fkt:
    fkt[d].sort(key=lambda x: x["von"])

def moddoc(datei):
    v = D.get(f"{datei}::__modul__", {}).get("moddoc", "")
    return (v.strip().splitlines() or [""])[0]

def tabelle(datei, kurz=None):
    """Markdown-Tabelle aller Funktionen einer Datei."""
    kurz = kurz or {}
    z = ["| Zeilen | Funktion | Z. | Was sie tut | Gerufen von |",
         "|---|---|---:|---|---|"]
    for f in fkt.get(datei, []):
        r = f["ruf"]
        lok = sorted({x.split(":")[1] for x in r if x.startswith(datei + ":")})
        ext = sorted({x for x in r if not x.startswith(datei + ":")})
        ruft = []
        if lok:
            ruft.append("`" + "`, `".join(lok[:3]) + "`" + (" …" if len(lok) > 3 else ""))
        if ext:
            ruft.append("**extern:** " + ", ".join(
                "`" + x.split("/")[-1].replace(".py", "") + "`"
                for x in ext[:2]) + (" …" if len(ext) > 2 else ""))
        if not ruft:
            ruft = ["*Einstiegspunkt*"]
        doc = kurz.get(f["name"]) or f["doc"] or "**— kein Docstring —**"
        args = ", ".join(f["args"])
        z.append("| %d–%d | `%s(%s)` | %d | %s | %s |" % (
            f["von"], f["bis"], f["name"], args, f["laenge"],
            doc.rstrip("."), " ".join(ruft)))
    return "\n".join(z)

def block(datei, prosa, kurz=None):
    n = len(fkt.get(datei, []))
    ln = sum(f["laenge"] for f in fkt.get(datei, []))
    return (f"\n### `{datei}`\n\n"
            f"> {moddoc(datei)}  \n"
            f"> **{n} Funktionen, {ln} Zeilen in Funktionsrümpfen.**\n\n"
            f"{prosa}\n\n{tabelle(datei, kurz)}\n")

# ==========================================================================
P = []
A = P.append

# --- gemessene Groessen, damit diese Datei nicht selbst veraltet ------------
GRP, NETTO = {}, {}
for o in ["prep", "vorpruefung", "modelle", "tests", "tools"]:
    for f in sorted((R/o).rglob("*.py")):
        rel = str(f.relative_to(R)).replace("\\", "/")
        b, n = messe(f)
        schl = ("modelle/m05" if rel.endswith("m05_abbildungen.py")
                else o)
        g = GRP.setdefault(schl, [0, 0, 0, 0])
        g[0] += 1; g[1] += b; g[2] += n; g[3] += len(fkt.get(rel, []))
        NETTO[rel] = n

ABG = [sum(GRP[k][i] for k in GRP if k != "tools") for i in range(4)]
LOC_TAB = "\n".join([
    "| Ordner | Dateien | LOC brutto | **netto** | Funktionen |",
    "|---|---:|---:|---:|---:|",
    f"| `prep/` | {GRP['prep'][0]} | {de(GRP['prep'][1])} | {de(GRP['prep'][2])} | {GRP['prep'][3]} |",
    f"| `vorpruefung/` | {GRP['vorpruefung'][0]} | {de(GRP['vorpruefung'][1])} | {de(GRP['vorpruefung'][2])} | {GRP['vorpruefung'][3]} |",
    f"| `modelle/` ohne `m05` | {GRP['modelle'][0]} | {de(GRP['modelle'][1])} | {de(GRP['modelle'][2])} | {GRP['modelle'][3]} |",
    f"| `modelle/m05_abbildungen.py` | 1 | {de(GRP['modelle/m05'][1])} | {de(GRP['modelle/m05'][2])} | {GRP['modelle/m05'][3]} |",
    f"| `tests/` | {GRP['tests'][0]} | {de(GRP['tests'][1])} | {de(GRP['tests'][2])} | {GRP['tests'][3]} |",
    f"| **Abgabe gesamt** | **{ABG[0]}** | **{de(ABG[1])}** | **{de(ABG[2])}** | **{ABG[3]}** |",
    f"| `tools/` (nicht Abgabe) | {GRP['tools'][0]} | {de(GRP['tools'][1])} | {de(GRP['tools'][2])} | {GRP['tools'][3]} |",
])
M5 = GRP["modelle/m05"]
N = NETTO
_ETAPPEN = [
    ("1", "Die zwei Konfigurationen", "`prep/config.py`, `modelle/config_modelle.py`",
     ["prep/config.py", "modelle/config_modelle.py"],
     "welche Merkmale es gibt, welche Suchräume, warum"),
    ("2", "Wie die Daten entstehen", "`prep/s1_daten.py`, `s2_datensaetze.py`, `build.py`",
     ["prep/s1_daten.py", "prep/s2_datensaetze.py", "prep/build.py"],
     "woher jede Spalte kommt und wie die Folds zustande kommen"),
    ("3", "Die eine Stelle mit den Folds", "`vorpruefung/v0_aufteilung.py`",
     ["vorpruefung/v0_aufteilung.py"],
     "warum alle Verfahren dieselben Zeilen sehen"),
    ("4", "Die Messlatte", "`vorpruefung/v1_baselines.py`",
     ["vorpruefung/v1_baselines.py", "vorpruefung/run.py"],
     "wogegen gemessen wird und warum diese zwei Stufen"),
    ("5", "Warum diese drei Verfahren", "`vorpruefung/v2_eignung.py`",
     ["vorpruefung/v2_eignung.py"], "die sechs Belege der Verfahrenswahl"),
    ("6", "**Das Muster**", "`modelle/m02_menge.py`", ["modelle/m02_menge.py"],
     "Tuning → Bewertung → Aggregation → Vergleich"),
    ("7", "Dasselbe Muster nochmal", "`modelle/m03_struktur.py`",
     ["modelle/m03_struktur.py"], "*fast nichts Neues* — siehe Abschnitt 2"),
    ("8", "Die Gegenproben", "`v3_spezifikation`, `v4_decke`",
     ["vorpruefung/v3_spezifikation.py", "vorpruefung/v4_decke.py"],
     "was die Ergebnisse einschränkt"),
    ("9", "Die Interpretation", "`modelle/m04_shap.py`", ["modelle/m04_shap.py"],
     "Unterfrage 1: welche Merkmale tragen"),
    ("10", "Die Bilder", "`modelle/m05_abbildungen.py`",
     ["modelle/m05_abbildungen.py"], "ein Muster, 18-mal angewandt"),
    ("11", "Die Prüfungen", "`tests/test_aufbereitung.py`",
     ["tests/test_aufbereitung.py"], "was zugesichert ist"),
]
LERNPFAD = "\n".join(
    ["| # | Was | Wo | netto | Danach kannst du erklären |", "|---|---|---|---:|---|"]
    + [f"| {a} | {b} | {c} | {de(sum(N[x] for x in fs))} | {e} |"
       for a, b, c, fs, e in _ETAPPEN])
KERN = sum(sum(N[x] for x in fs) for a, b, c, fs, e in _ETAPPEN if a in "123456")

A(f"""# Funktionslandkarte — jede Funktion, ihr Zweck, ihr Aufrufer

> **Lebensdauer:** ändert sich, wenn Funktionen dazukommen, wegfallen oder
> umbenannt werden. Die Tabellen sind aus dem Quelltext erzeugt (AST), die
> Fließtexte von Hand geschrieben. Stand **{datetime.date.today().strftime('%d.%m.%Y')}**.
>
> **Wozu diese Datei.** Sie beantwortet für jede Funktion des Repos drei
> Fragen: Wo steht sie, was tut sie, wer ruft sie. Sie ersetzt nicht
> `docs/08_FUNKTIONSDOKUMENTATION.md` — dort steht ausführlich, *warum* eine
> Funktion so aussieht (Stand 17.08.2026, eingefroren). Hier steht knapp,
> *was* sie ist, und zwar auf dem aktuellen Stand.
>
> **Die Spalte „Gerufen von" ist importbewusst aufgelöst.** Ein Aufruf zählt
> nur, wenn die rufende Datei den Namen tatsächlich importiert hat. Sonst
> stünden gleichnamige Helfer (`_md`, `bericht`, `main`, `ein_lauf`) wechselseitig
> als Aufrufer voneinander — sie sind aber verschiedene Funktionen.

---

## 0. Die Zahl, um die es geht

Der Abgabecode umfasst **{de(ABG[1])} Zeilen** in {ABG[0]} Dateien. Netto, also ohne
Leerzeilen, Docstrings und Kommentare, sind es **{de(ABG[2])} Zeilen**. Diese Tabelle
wird bei jedem Lauf des Erzeugers neu gemessen, sie kann nicht veralten:

{LOC_TAB}

**{100*M5[2]//ABG[2]} % des Abgabecodes ist Matplotlib in `m05`.** Diese {M5[3]} Funktionen
folgen alle demselben Muster (CSV lesen → Achsen → beschriften → speichern);
wer eine verstanden hat, hat alle verstanden.

---

## 1. Der Lernpfad — in dieser Reihenfolge lesen

Die Abhängigkeiten laufen streng in eine Richtung. Wer sie in dieser
Reihenfolge liest, muss nie vorgreifen.

{LERNPFAD}

Etappen 1 bis 6 sind **{de(KERN)} Nettozeilen** — der Kern. Alles danach ist
Wiederholung des Musters, Gegenprobe oder Darstellung.

---

## 2. Die drei Muster, die sich wiederholen

Wer diese drei erkennt, muss zwei Drittel des Codes nicht einzeln lesen.

### Muster A — `m02` und `m03` sind Zwillinge

Beide Dateien haben **dieselbe Phasenfolge und 16 gleichnamige Funktionen**:

`verfahren` → `suchraum` → `tune` → `ein_lauf` → `extrapolationsanteil` →
`phase_tuning` → `_rein_python` → `_parameter_je_fold` → `phase_bewertung` →
`aggregiere` → `_gepaart` → `vergleiche` → `leakage_diagnose` → `hold_out` →
`uebernehmen` → `main`

Die Unterschiede sind vier, und alle sind fachlich:

| | `m02_menge` (Regression) | `m03_struktur` (Klassifikation) |
|---|---|---|
| Verfahren | Ridge, RF, XGBoost | RF, XGBoost (#31) |
| Maß | RMSE, MAE, R² | Macro-F1, Macro-AUROC |
| Zusätzlich | `_holm` (drei Verfahren ⇒ Testfamilie) | `kodiere`, `_gewichte`, `_macro_auroc` |
| Pipeline | mit Scaler (Ridge braucht ihn) | ohne Scaler, beides Bäume |

**Lies `m02` gründlich, `m03` nur im Diff.**

### Muster B — die 18 Abbildungsfunktionen

Jede `aN_*(plt, FuncFormatter)` in `m05` tut dasselbe: CSV aus `results/`
lesen, Achsen anlegen, mit `_komma`/`_prozent`/`_dez` deutsch beschriften,
per `_text` einen Erläuterungsblock daruntersetzen, speichern. `main` ruft
sie der Reihe nach. Die vorangestellten `_*`-Helfer bereiten je eine
Abbildung datenseitig vor.

### Muster C — Bericht-Skripte

`vorpruefung/v4_decke`, `tools/panelprofil` und
`tools/parametersensitivitaet` haben denselben Bauplan: rechnende Funktionen →
`_md`/`md` (Datenrahmen zu Markdown) → `bericht` (Tabellen zusammensetzen) →
`main` (schreibt CSV plus `.md`). Gleiche Funktionsnamen, drei verschiedene
Dateien — nicht verwechseln.

---

## 3. Der Datenfluss auf einen Blick

```
data/raw/  ──prep/s1_daten──►  Einsatzebene
                                    │
                     prep/s2_datensaetze  (aggregieren, Zielgrößen,
                                    │      fold + ist_holdout als SPALTE)
                                    ▼
        data/processed/regression.parquet · klassifikation.parquet
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
  vorpruefung/v0_aufteilung   vorpruefung/v1_baselines   vorpruefung/v2_eignung
  (die einzige Stelle,        (die Messlatte,            (sechs Belege der
   an der Folds entstehen)     Stufe 1 und 2)             Verfahrenswahl)
        │                           │
        └────────────┬──────────────┘
                     ▼
        modelle/m02_menge · m03_struktur     ──►  results/regression/
                     │                              results/klassifikation/
                     ▼
              modelle/m04_shap                ──►  results/shap/
                     │
                     ▼
          modelle/m05_abbildungen             ──►  results/abbildungen/
                                                   (rechnet nichts, liest nur)
```

Dass `fold` und `ist_holdout` **Spalten der Parquet-Datei** sind und nicht zur
Laufzeit entstehen, ist die konstruktive Absicherung der Fairness-Regel: Alle
Verfahren sehen zwangsläufig dieselben Zeilen.

---

## 4. `prep/` — die Daten
""")

A(block("prep/config.py", """Reine Konstanten, keine Funktionen: Pfade, Zeitraum, die Merkmalslisten,
`N_STADTTEILE_ERWARTET = 36`, die `DOWNLOAD_*`-Schalter. **Von 265 Zeilen sind
93 Kommentar** — jede Festlegung ist an Ort und Stelle begründet. Das ist die
Datei, mit der man anfängt."""))

A(block("prep/s1_daten.py", """Zwei Einstiegspunkte, beide von `build.main` gerufen: `run_download()` holt
die Rohquellen (nur wenn der Schalter in `config` das erlaubt), `run_join()`
führt sie auf **Einsatzebene** zusammen. Dazwischen liegen die Fachfunktionen
in der Reihenfolge, in der sie gebraucht werden — SFFD aufbereiten, Tracts auf
Stadtteile umlegen, den zum Prognosezeitpunkt *publizierten* ACS-Jahrgang
wählen, den Kriminalitätsindex bauen, Parzellen zuordnen.

Die drei Funktionen, an denen die meiste Fachlogik hängt, sind
`acs_snapshot` (Publikationsversatz), `kriminalitaetsindex` (relativer Index
aus zwei SFPD-Quellen) und `tract_zu_stadtteil` (Zuordnung über
Zensusgrenzen hinweg)."""))

A(block("prep/s2_datensaetze.py", """Hier entsteht die Analyseeinheit **Stadtteil × Monat** und damit beides, was
später gelesen wird. `aggregiere` baut das Panel, `baue_regression` und
`baue_klassifikation` legen die Zielgrößen an, `ergaenze_aufteilung` schreibt
`fold` und `ist_holdout` **als Spalten** in die Datei — das ist die wichtigste
Stelle des ganzen Repos für die Fairness-Regel. `pruefe_zuschnitt` bricht ab,
wenn der Zuschnitt nicht stimmt; `run` fährt alles und gibt beide Datenrahmen
zurück. `fold_masken` und `beschreibe_splits` gehören zum Validierungsrahmen
und werden von außen mitbenutzt."""))

A(block("prep/build.py", """Die Fassade. `main()` fährt `s1_daten.run_join()` und `s2_datensaetze.run()`
nacheinander und druckt am Ende den Steckbrief. 106 Zeilen, davon 30 die
eigentliche Steuerung."""))

A("\n---\n\n## 5. `vorpruefung/` — die Messlatte und die Eignung\n")

A(block("vorpruefung/v0_aufteilung.py", """**Die wichtigste kleine Datei des Repos.** Drei öffentliche Funktionen, und
alle drei werden von außen gerufen:

- `selten_je_stadtteil` liefert die Sortiergröße der Zuteilung (brand-dominierte Monate je Stadtteil),
- `wiederholte_aufteilung` belegt die `fold`-Spalte für Wiederholung *r* neu,
- `entwicklung_und_holdout` ist **die einzige Stelle im Repo, die `ist_holdout == 1` auswertet**.

`wiederholte_aufteilung` enthält eine Zusicherung, die trägt: Für
Wiederholung 0 muss die neu gerechnete Zuteilung Zeile für Zeile der
`fold`-Spalte aus der Parquet-Datei entsprechen, sonst bricht sie ab. Damit
kann die Zuteilung nicht stillschweigend auseinanderlaufen.

Der `_selbsttest` prüft über alle zehn Wiederholungen: Foldgrößen 6/6/6/6/6,
mindestens ein Brand-Testfall je Fold, unverändertes Hold-out, zehn
verschiedene Partitionen, kein Stadtteil gleichzeitig Trainings- und Testfall."""))

A(block("vorpruefung/v1_baselines.py", """Die zwei Stufen der Messlatte. `poisson_glm` ist Stufe 2 der Regression (mit
Offset, #45), `logit_glm` Stufe 2 der Klassifikation — beide unpenalisiert per
Maximum-Likelihood, **ohne freien Hyperparameter**. `regression` und
`klassifikation` fahren sie über zehn Wiederholungen × fünf Folds, `_zweistufig`
mittelt erst je Wiederholung, dann darüber.

`bewerte_regression` und `_macro_auroc` werden auch von `modelle/` benutzt —
damit rechnen Baseline und Vergleichsverfahren die Gütemaße **mit demselben
Code**, nicht nur nach derselben Formel."""))

A(block("vorpruefung/v2_eignung.py", """Sechs Belege, warum genau diese Verfahren. Jeder ist eine eigene Funktion und
schreibt in denselben Berichtstext (`log`, `speichere`):

1. `dispersion` — Überdispersion der Zähl-Zielgrößen (spricht gegen reines Poisson),
2. `linearitaet` — Korrelationen und Residuenbild (Auflage R7),
3. `spezifikation` — RESET-Test und Interaktionsterme,
4. `extrapolation` — Anteil der Teststadtteile außerhalb des Gelernten,
5. `klassifikation` — trennen dieselben Merkmale auch die Einsatzart,
6. `annahmen` — die Anforderungstabelle je Verfahren mit drei formalen Tests
   (Cameron & Trivedi, Breusch-Pagan, Jarque-Bera; Auflage aus der
   Sprechstunde vom 10.08.2026).

`annahmen` ist mit 186 Zeilen die längste Funktion der Vorprüfung; sie ist
lang, weil sie eine Tabelle zusammensetzt, nicht weil sie kompliziert rechnet.
Die verschachtelte `Z()` baut je eine Zeile davon."""))

A(block("vorpruefung/v3_spezifikation.py", """Die Gegenprobe zu Beleg 3 (B-41): Die Eignungsprüfung hat Nichtlinearität
*in-sample* diagnostiziert — hält sie auch out-of-sample? `alle_laeufe` fährt
10 × 5 × 4 = 200 Poisson-Anpassungen über vier Spezifikationen, `zweistufig`
mittelt wie überall zweistufig.

Der `_selbsttest` ist die Kontrolle, die das Ergebnis erst belastbar macht:
Die Spezifikation `linear` muss die Stufe-2-Baseline reproduzieren. Tut sie es
nicht, rechnet die Datei etwas anderes als sie behauptet."""))

A(block("vorpruefung/v4_decke.py", """Wie gut *kann* die Einsatzart mit diesen Merkmalen überhaupt vorhergesagt
werden? Zwei Obergrenzen: `decke_a` beziffert das Label-Rauschen der
argmax-Bildung über einen parametrischen Bootstrap, `decke_b` das reine
Stadtteilwissen (Modalklasse je Stadtteil). `marge` misst, wie knapp die
Klassenentscheidung ausfällt, `ausschoepfung` setzt die gemessenen Macro-F1
baselinekorrigiert dagegen.

Mit dem Argument `holdout` rechnet dieselbe Datei die Decken inklusive der
sechs zurückgehaltenen Stadtteile."""))

A(block("vorpruefung/run.py", """Fassade über `v1_baselines.run()` und `v2_eignung.main()`. Die Reihenfolge ist
zwingend — `v2` liest die Baseline-Werte, die `v1` schreibt. 65 Zeilen, keine
Rechnung."""))

A("\n---\n\n## 6. `modelle/` — der Vergleich\n")

A(block("modelle/config_modelle.py", """Gegenstück zu `prep/config.py`, ohne Funktionen: `RANDOM_STATE = 42`,
Suchräume, Budget, Fold- und Wiederholungszahlen. **65 von 110 Zeilen sind
Kommentar** — jeder Suchraum trägt seine Begründung (#49, #50) an Ort und Stelle."""))

A(block("modelle/m02_menge.py", """**Die Datei, die man wirklich lesen muss.** Vier Phasen, jede eine Funktion,
alle von `main` in dieser Reihenfolge gefahren:

| Phase | Funktion | Was passiert |
|---|---|---|
| 1 | `phase_tuning` | je Zielgröße × Verfahren × Fold **einmal** `tune()`, auf Wiederholung 0 |
| 2 | `phase_bewertung` | 10 Wiederholungen × 5 Folds × 3 Verfahren × 2 Zielgrößen = 300 Läufe |
| 3 | `aggregiere` | zweistufig mitteln: erst je Wiederholung über die Folds, dann darüber |
| 4 | `vergleiche` | gepaarter Wilcoxon auf RMSE, zwei Rollen, zwei Teststufen |

Darunter liegen die Bausteine: `verfahren` baut die ungetunte Pipeline,
`suchraum` übersetzt die Config in scipy-Verteilungen, `ein_lauf` ist **ein**
Fit plus Bewertung plus Zeitmessung und liefert eine CSV-Zeile.

Drei Stellen verdienen besondere Aufmerksamkeit:

- **`ein_lauf` (119 Z.)** ist der Kern. Hier steht die Spezifikation aus #43:
  Geschätzt wird immer die *Rate*, für die absolute Zahl wird mit der
  Einwohnerzahl zurückmultipliziert. Dieselbe Konstruktion wie beim Poisson-GLM.
- **`hold_out`** ist die einzige Funktion, die die sechs zurückgehaltenen
  Stadtteile anfasst — und sie läuft nur, wenn `main` das Argument `holdout`
  bekommen hat.
- **`uebernehmen`** liest Tuning und Bewertung aus `results/`, statt sie neu zu
  rechnen. Das ist der Grund, warum ein Wiederholungslauf nicht immer 55 Minuten braucht."""))

A(block("modelle/m03_struktur.py", """**Derselbe Bauplan wie `m02`** — siehe Muster A oben. Neu sind nur vier
Funktionen, und alle vier sind Klassifikationsmechanik: `kodiere` (Klassennamen
zu 0–3 nach der globalen Reihenfolge `KLASSEN`), `_gewichte`
(`class_weight="balanced"` von Hand, weil XGBoost es nicht kennt), `fitte`
(Schätzer mit diesen Gewichten anpassen) und `_macro_auroc`.

Es fehlt `_holm`: Bei zwei Verfahren statt drei gibt es keine Testfamilie zu
korrigieren."""))

A(block("modelle/m04_shap.py", """Unterfrage 1 — welche Merkmale tragen die Vorhersage. Sechs Auswertungen, die
`main` (203 Z., reine Ablaufsteuerung) nacheinander fährt:

- `schlagen_die_latte` wählt aus, **für wen** SHAP überhaupt sinnvoll ist:
  nur (Zielgröße, Verfahren), die ihre Stufe-2-Baseline schlagen,
- `ruhigster_fold` wählt den Fold mit dem geringsten Extrapolationsanteil,
- `_beitraege` liefert den mittleren absoluten Beitrag je Merkmal — SHAP bei
  Bäumen, Koeffizient bei Ridge,
- `ablation_faktorgruppen` (94 Z.) misst, was eine Faktorgruppe wert ist, indem
  sie weggelassen wird — die **Gegenprobe zur Attribution**,
- `ablation_exposition` prüft die Spezifikation aus #43,
- `_vif` beziffert Multikollinearität auf zwei Bezugsmengen.

Attribution (was ein Modell benutzt) und Ablation (was fehlt, wenn man es
wegnimmt) sind zwei verschiedene Fragen. Dass beide hier stehen, ist Absicht;
ihr Auseinanderfallen ist B-47."""))

A(block("modelle/m05_abbildungen.py", """**Rechnet nichts.** Liest ausschließlich die CSV-Dateien der vorherigen
Schritte und zeichnet daraus 21 PDF. Deshalb steht sie am Ende der Laufordnung
und deshalb dauert sie unter einer Minute.

Der Aufbau ist streng nach Muster B (oben): fünf Formatierhelfer
(`_matplotlib`, `_komma`, `_prozent`, `_dez`, `_sekunden`, `_text`), dann
paarweise ein `_*`-Datenaufbereiter und die zugehörige `aN_*`-Zeichenfunktion.
`main` ruft die 18 Abbildungen der Reihe nach.

Zum Lesen genügen zwei: `a1_gegen_baseline` (die Primäraussage) und
`a15_attribution_ablation` (die komplexeste). Der Rest ist dasselbe Muster mit
anderen Spalten."""))

A("\n---\n\n## 7. `tests/`\n")

A(block("tests/test_aufbereitung.py", """**20** Prüfungen an den erzeugten Parquet-Dateien, gesammelt an einer Stelle:
23 Funktionen, davon 20 `test_*`. (`CLAUDE.md` und README sprachen bis zum
02.09.2026 von 19 — der Lauf meldet 20/20.) Sie prüfen Zeilenzahl, Stadtteilzahl, Datentypen,
fehlende Werte, Konsistenz von `fold` und `ist_holdout`, Plausibilität der
Exposition und der Anteile. `main` sammelt die Ergebnisse und gibt Exitcode 1
bei Fehlern.

Es sind **Zusicherungen über die Daten**, keine Unit-Tests der Funktionen."""))

A("""
---

## 8. `tools/` — nicht Abgabe, aber Teil des ZIP

Seit dem 31.08.2026 geht `tools/` mit ins Abgabe-ZIP: `fairness.py` erzeugt die
Zahlen von R-24 und Kapitel 8.3, `suchdiagnose.py` die Budgetdiagnose in 6.4,
`deskriptiv.py` und `codebook.py` die Tabellen aus Kapitel 4,
`pruefe_zahlen.py` die Zusicherung, dass Text und `results/` zusammenpassen.

Für das Verständnis der Arbeit sind sie zweitrangig — **mit einer Ausnahme:**

### `tools/pruefe_zahlen.py` — der Zahlenwächter

Die einzige Datei in `tools/`, die man kennen sollte. Sie prüft **148 Werte**
aus `results/` gegen die Stellen in `docs/`, an denen sie stehen, plus fünf
Strukturprüfungen. `baue_pruefungen` (227 Z.) ist die Registrierungsliste —
lang, aber trivial: eine Zeile je Wert. `abschnitte` zerlegt ein
Markdown-Dokument an den nummerierten Überschriften, damit ein Wert nicht in
einem beliebigen Kapitel „gefunden" wird. Exit-Code 1 meldet, welche Stelle
nicht mehr passt.

""")

A(block("tools/panelprofil.py", """Rein deskriptiv, kein Modell: Wer sind die 30 und wer sind die 6?
`stadtteile` liefert eine Zeile je Stadtteil mit Zuteilungsrang, Größe,
Einsatzlast und Modalklasse; `klassenverteilung` und `zielgroessen` vergleichen
beide Panelhälften.

**Das trägt die Erklärung des Hold-out-Bruchs im Strukturstrang** (R-2): 37 von
70 brand-dominierten Monaten liegen im Hold-out, der Brand-Anteil ist dort
4,67 % gegen 0,83 % in der Entwicklung. Rang 1 der Zuteilungsordnung fällt
nach der Regel aus #30 zwangsläufig in Gruppe 0.

**Geht nicht in die Arbeit** (Entscheidung 02.09.2026, nach Schröters Regel
vom 24.08.: Analysen ohne Bezug zu einer Forschungsfrage gehören raus oder in
den Anhang). Deshalb liegt die Datei in `tools/` und ihre Zahlen stehen in
`06_RISIKEN.md` unter R-2, nicht in `03_STAND.md`. Der Zahlenwächter prüft sie
trotzdem — sie sind einmal unbemerkt über eine Korrektur hinweg veraltet."""))

A(block("tools/parametersensitivitaet.py", """Die Kreuzprobe: `kreuzprobe` bewertet **jeden Testfold mit jedem der fünf
getunten Parametersätze**. Die Diagonale ist die berichtete Konfiguration, die
20 übrigen Zellen sind Sätze, die auf anderen Stadtteilen gefunden wurden.
`zusammenfassung` stellt eigenen gegen fremden Satz, `_kontrolle` prüft, ob die
Diagonale den Hauptlauf reproduziert.

Ergebnis: Der eigene Satz ist nicht systematisch besser — bei den Baumverfahren
sogar schlechter. Dritte unabhängige Bestätigung derselben These neben `v3` und
der Budgetdiagnose aus #49.

**Geht nicht in die Arbeit** (Entscheidung 02.09.2026). Die Messung schließt
aber die in B-42 benannte offene Frage; ihr Ergebnis steht deshalb in
`06_RISIKEN.md` unter R-4. **Wird die Parameter-Asymmetrie in Kapitel 8
erwähnt, ist „nicht gemessen" falsch** — dann gehört eine Zeile aus R-4 hinein."""))

A("""
Die übrigen elf Dateien im Überblick:

| Datei | Zweck | Fkt. |
|---|---|---:|
| `codebook.py` | Merkmalstabelle mit Skalenniveau für Kapitel 4 | 8 |
| `deskriptiv.py` | deskriptiver Befundteil von Kapitel 4 | 10 |
| `rohbefunde.py` | Qualitätsteil von Kapitel 4, ACS-Rohbefunde | 2 |
| `fairness.py` | hängt die Prognosegüte am Sozialprofil? (R-24) | 4 |
| `suchdiagnose.py` | war die Hyperparametersuche am Limit? | 8 |
| `sichere_ergebnisse.py` | `results/` nach `archiv/` kopieren, mit Manifest | 5 |
| `aufraeumen.py` | verwaiste Artefakte finden (Vorschau, löscht nichts) | 7 |
| `funktionsdoku.py` | erzeugt das Docstring-Archiv | 5 |
| `folien/renderer.py` | rendert Foliensätze nach HTML und PDF | 18 |
| `folien/deck_prep.py` | Foliensatz 01 — Datenaufbereitung | 0 |
| `folien/deck_vorpruefung.py` | Foliensatz 02 — Messlatte und Eignung | 0 |
| `folien/deck_modelle.py` | Foliensatz 03 — Verfahrensvergleich | 0 |
| `landkarte.py` | erzeugt diese Datei | 4 |

Die drei `deck_*.py` haben **null Funktionen** — sie sind reine Inhaltsdaten
(2.513 Zeilen verschachtelte Listen), die `renderer.py` in Folien verwandelt.
Für das Verständnis des Codes tragen sie nichts bei.

---

## 9. Was du für das Kolloquium können musst

Nach Erfahrung mit dieser Art Prüfung reichen fünf Antworten:

1. **„Wo entstehen die Folds?"** → `vorpruefung/v0_aufteilung.py`, und sie
   stehen als Spalte in der Parquet-Datei. Deshalb sehen alle Verfahren
   dieselben Zeilen.
2. **„Was passiert in einem Lauf?"** → `m02_menge.ein_lauf`: ein Fit auf der
   Rate, eine Vorhersage, Rückmultiplikation mit der Einwohnerzahl,
   Zeitmessung, eine CSV-Zeile.
3. **„Warum zweimal mitteln?"** → `aggregiere`: erst je Wiederholung über die
   fünf Folds, dann über die zehn Wiederholungen. Die 50 Läufe sind nicht
   unabhängig (#37).
4. **„Warum wird das Hold-out nur einmal gelesen?"** → Nur `hold_out` wertet
   `ist_holdout == 1` aus, und nur mit dem Argument `holdout`. Ohne das Argument
   filtert `main` die Zeilen heraus, bevor irgendetwas rechnet.
5. **„Woher wissen Sie, dass die Zahlen im Text stimmen?"** →
   `tools/pruefe_zahlen.py`, 148 Wertprüfungen gegen `results/`, Exit-Code 1
   bei Abweichung.
""")

ziel = R/"docs"/"10_FUNKTIONSLANDKARTE.md"
ziel.write_text("\n".join(P), encoding="utf-8")
n = len(ziel.read_text(encoding="utf-8").splitlines())
print(f"geschrieben: docs/10_FUNKTIONSLANDKARTE.md  ({n} Zeilen, "
      f"{sum(len(v) for v in fkt.values())} Funktionen erfasst)")
