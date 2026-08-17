"""
Erzeugt die ausfuehrliche Funktionsdokumentation aus dem Quelltext.

    python tools/funktionsdoku.py archiv    docs/08_FUNKTIONSDOKUMENTATION.md
    python tools/funktionsdoku.py pdf       docs/funktionsdoku/*.pdf
    python tools/funktionsdoku.py           beides

Eingang: alle .py-Dateien in prep/, vorpruefung/, modelle/, tests/, tools/
Ausgang: docs/08_FUNKTIONSDOKUMENTATION.md
         docs/funktionsdoku/{prep,vorpruefung,modelle}.pdf

STAND: neu am 17.08.2026.

--------------------------------------------------------------------------
WOZU DIESES SKRIPT
--------------------------------------------------------------------------
Bis zum 17.08.2026 trugen die Docstrings die vollstaendige Begruendung jeder
Funktion - zusammen 3.430 Zeilen Erzaehltext auf 4.596 Zeilen Code. Das ist
beim Lesen des Codes zu viel: Wer eine Funktion verstehen will, sucht die
Kurzfassung, nicht den Aufsatz.

Seither gilt die Arbeitsteilung:

  im Quelltext        fuenf bis sechs Saetze je Funktion, Stichpunkte je Datei
  in der Archivdatei  die vollstaendige Begruendung, wie sie vorher im Code stand
  in den PDFs         dieselbe Fassung, nach Abschnitten getrennt und lesbar
                      gesetzt - zum Durchlesen, nicht zum Nachschlagen

--------------------------------------------------------------------------
FALLSTRICKE
--------------------------------------------------------------------------
  1  JEDER LAUF UEBERSCHREIBT. `docs/08_FUNKTIONSDOKUMENTATION.md` und die drei
     PDFs sind die eingefrorene Fassung von VOR der Verdichtung. Ein erneuter
     Lauf liest den heutigen - gekuerzten - Quelltext und ersetzte die
     ausfuehrliche Fassung durch die kurze. Das Skript verweigert deshalb
     beides, solange nicht `--ueberschreiben` danebensteht.

  2  DIE PDFS BRAUCHEN pandoc UND xelatex. Fehlt eines, meldet das Skript es
     und bricht den PDF-Teil ab, ohne den Archivteil zu gefaehrden.

  3  KOMMENTARBLOECKE AB DREI ZEILEN gelten als Erzaehltext. Kuerzere gelten
     als Codekommentar und bleiben aussen vor - sonst stuenden hier auch
     einzeilige Hinweise wie "# Reihenfolge wichtig".

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Enthaelt das Archiv jede Datei aus REIHENFOLGE? Fehlt eine, ist sie
    umbenannt worden und die Liste veraltet.
  - Sind die drei PDFs entstanden und groesser als 50 kB? Kleinere deuten auf
    einen abgebrochenen xelatex-Lauf.
  - Steht in jedem Funktionseintrag entweder ein Docstring oder der Vermerk
    "kein Docstring"? Ein leerer Eintrag waere ein Parserfehler.
"""

from __future__ import annotations

import ast
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIV = ROOT / "docs" / "08_FUNKTIONSDOKUMENTATION.md"
PDF_ORDNER = ROOT / "docs" / "funktionsdoku"

ABSCHNITTE = [
    ("prep", "Aufbereitung"),
    ("vorpruefung", "Vorpruefung"),
    ("modelle", "Modellierung"),
    ("tests", "Absicherung"),
    ("tools", "Werkzeuge (nicht Abgabe)"),
]
REIHENFOLGE = {
    "prep": ["config.py", "s1_daten.py", "s2_datensaetze.py", "build.py"],
    "vorpruefung": ["run.py", "v0_aufteilung.py", "v1_baselines.py",
                    "v2_eignung.py", "v3_spezifikation.py", "v4_decke.py"],
    "modelle": ["config_modelle.py", "m02_menge.py", "m03_struktur.py",
                "m04_shap.py", "m05_abbildungen.py"],
    "tests": ["test_aufbereitung.py"],
    "tools": ["codebook.py", "suchdiagnose.py", "pruefe_zahlen.py",
              "sichere_ergebnisse.py", "aufraeumen.py", "funktionsdoku.py"],
}
PDF_ABSCHNITTE = ["prep", "vorpruefung", "modelle"]


def kommentarbloecke(quelltext: str) -> list[tuple[int, list[str]]]:
    """Zusammenhaengende Kommentarbloecke ab drei Zeilen.

    FALLSTRICK 3: Kuerzere Bloecke sind Codekommentare und gehoeren nicht in
    die Dokumentation. Zurueck kommt je Block die Startzeile und der Text ohne
    die fuehrenden Rautezeichen.
    """
    treffer: list[tuple[int, list[str]]] = []
    puffer: list[str] = []
    start = 0
    for nummer, zeile in enumerate(quelltext.splitlines(), 1):
        gestutzt = zeile.strip()
        if gestutzt.startswith("#"):
            if not puffer:
                start = nummer
            puffer.append(gestutzt.lstrip("#").rstrip())
        else:
            if len(puffer) >= 3:
                treffer.append((start, puffer[:]))
            puffer = []
    if len(puffer) >= 3:
        treffer.append((start, puffer))
    return treffer


def datei_abschnitt(ordner: str, datei: str) -> list[str]:
    """Der Markdown-Block fuer eine einzelne Quelldatei.

    Aufbau: Kopfzeile mit Umfang, dann Modul-Docstring, dann die
    Kommentarbloecke mit Zeilennummer, dann jede Funktion der obersten Ebene
    mit Signatur, Laenge und Docstring. Innere Funktionen werden eine Stufe
    tiefer angehaengt, damit die Schachtelung sichtbar bleibt.
    """
    pfad = ROOT / ordner / datei
    if not pfad.exists():
        return []
    quelltext = pfad.read_text(encoding="utf-8", errors="replace")
    baum = ast.parse(quelltext)
    alle = [k for k in ast.walk(baum)
            if isinstance(k, (ast.FunctionDef, ast.AsyncFunctionDef))]

    aus = [f"\n## `{ordner}/{datei}`\n",
           f"*{len(quelltext.splitlines())} Zeilen · {len(alle)} Funktionen*\n"]

    kopf = ast.get_docstring(baum, clean=False)
    if kopf:
        aus += ["### Modulkopf\n", "```text", kopf.strip(), "```\n"]

    bloecke = kommentarbloecke(quelltext)
    if bloecke:
        aus.append(f"### Kommentarbloecke ({len(bloecke)})\n")
        for start, block in bloecke:
            text = "\n".join(block).strip()
            if not text or set(text) <= set("-=_ \n"):
                continue
            aus += [f"**Zeile {start}**\n", "```text", text, "```\n"]

    oberste = [k for k in baum.body
               if isinstance(k, (ast.FunctionDef, ast.AsyncFunctionDef))]
    if oberste:
        aus.append("### Funktionen\n")
        for knoten in oberste:
            laenge = (knoten.end_lineno or knoten.lineno) - knoten.lineno + 1
            args = ", ".join(a.arg for a in knoten.args.args)
            aus += [f"#### `{knoten.name}({args})`\n",
                    f"*Zeile {knoten.lineno} · {laenge} Zeilen*\n"]
            text = ast.get_docstring(knoten, clean=False)
            aus += ["```text", text.strip(), "```\n"] if text else ["_kein Docstring_\n"]
            for inner in [k for k in ast.walk(knoten)
                          if isinstance(k, (ast.FunctionDef, ast.AsyncFunctionDef))
                          and k is not knoten]:
                aus.append(f"##### innere Funktion `{inner.name}()`\n")
                itext = ast.get_docstring(inner, clean=False)
                aus += ["```text", itext.strip(), "```\n"] if itext else ["_kein Docstring_\n"]
    return aus


def baue_archiv(stand: str) -> str:
    """Das vollstaendige Archiv als Markdown-Text.

    Enthaelt Kopf, Inhaltsverzeichnis und je Abschnitt alle Quelldateien in
    der Reihenfolge, in der sie ausgefuehrt werden - nicht alphabetisch. Wer
    die Datei von oben nach unten liest, liest die Pipeline entlang.
    """
    aus = ["# Funktionsdokumentation — vollstaendiges Archiv\n",
           f"> Erzeugt von `tools/funktionsdoku.py` am {stand} aus den Docstrings",
           "> und Kommentarbloecken aller Python-Dateien.\n", "---\n", "## Inhalt\n"]
    for ordner, titel in ABSCHNITTE:
        aus.append(f"- **{titel}** (`{ordner}/`)")
        for datei in REIHENFOLGE[ordner]:
            if (ROOT / ordner / datei).exists():
                aus.append(f"  - `{datei}`")
    aus.append("\n---\n")
    for ordner, titel in ABSCHNITTE:
        aus.append(f"\n# {titel} — `{ordner}/`\n")
        for datei in REIHENFOLGE[ordner]:
            aus += datei_abschnitt(ordner, datei)
    return "\n".join(aus) + "\n"


LATEX_VORLAGE = r"""\documentclass[10pt]{article}
\usepackage{fontspec}
\setmainfont{DejaVu Serif}
\setmonofont[Scale=0.78]{DejaVu Sans Mono}
\usepackage[margin=2.3cm]{geometry}
\usepackage{xcolor}
\usepackage{fvextra}
\DefineVerbatimEnvironment{verbatim}{Verbatim}{breaklines,breakanywhere,%
  fontsize=\small,frame=leftline,framerule=0.6pt,rulecolor=\color{gray!45},%
  xleftmargin=6pt,framesep=6pt}
\usepackage[colorlinks=true,linkcolor=black,urlcolor=black]{hyperref}
\usepackage{titlesec}
\titleformat{\section}{\Large\bfseries}{}{0pt}{}
\titleformat{\subsection}{\large\bfseries}{}{0pt}{}
\titleformat{\subsubsection}{\bfseries}{}{0pt}{}
\setcounter{secnumdepth}{0}
\setcounter{tocdepth}{2}
\providecommand{\tightlist}{\setlength{\itemsep}{0pt}\setlength{\parskip}{0pt}}
\usepackage{parskip}
\title{\vspace{-1.5cm}$title$}\author{}\date{$date$}
\begin{document}
\maketitle\thispagestyle{empty}
\tableofcontents
\newpage
$body$
\end{document}
"""


def baue_pdfs(stand: str) -> int:
    """Ein PDF je Abschnitt ueber pandoc und xelatex.

    Die Ueberschriften werden vor der Umwandlung um eine Stufe angehoben: Was
    im Archiv eine Datei ist, wird im PDF ein Kapitel. Das eigene LaTeX-Template
    ist noetig, weil die Standardvorlage von pandoc `lmodern.sty` erwartet, das
    in dieser Umgebung fehlt; ausserdem bricht `fvextra` die langen Zeilen der
    Codebloecke um, statt sie ueber den Rand laufen zu lassen.

    FALLSTRICK 2: Fehlt pandoc oder xelatex, wird der PDF-Teil uebersprungen
    statt abgebrochen - der Archivteil ist wichtiger und soll nicht an einer
    fehlenden LaTeX-Installation scheitern.
    """
    if not shutil.which("pandoc") or not shutil.which("xelatex"):
        print("  pandoc oder xelatex fehlt - PDF-Teil uebersprungen.")
        return 1
    PDF_ORDNER.mkdir(parents=True, exist_ok=True)
    vorlage = PDF_ORDNER / "_vorlage.latex"
    vorlage.write_text(LATEX_VORLAGE, encoding="utf-8")
    titel = dict(ABSCHNITTE)
    fehler = 0
    for ordner in PDF_ABSCHNITTE:
        zeilen: list[str] = []
        for datei in REIHENFOLGE[ordner]:
            for z in datei_abschnitt(ordner, datei):
                # eine Stufe anheben: Datei wird Kapitel
                for tief, flach in (("##### ", "#### "), ("#### ", "### "),
                                    ("### ", "## "), ("## ", "# ")):
                    if z.startswith(tief):
                        z = flach + z[len(tief):]
                        break
                zeilen.append(z)
        md = PDF_ORDNER / f"_{ordner}.md"
        md.write_text("\n".join(zeilen) + "\n", encoding="utf-8")
        ziel = PDF_ORDNER / f"{ordner}.pdf"
        ergebnis = subprocess.run(
            ["pandoc", str(md), "-o", str(ziel), "--pdf-engine=xelatex",
             f"--template={vorlage}", "--no-highlight",
             "-M", f"title=Funktionsdokumentation — {titel[ordner]}",
             "-M", f"date=Stand {stand}"],
            capture_output=True, text=True)
        md.unlink(missing_ok=True)
        if ergebnis.returncode:
            letzte = (ergebnis.stderr.strip().splitlines() or ["unbekannt"])[-1]
            print(f"  {ordner}.pdf FEHLGESCHLAGEN: {letzte[:90]}")
            fehler = 1
        else:
            print(f"  {ziel.relative_to(ROOT)}  {ziel.stat().st_size / 1024:.0f} kB")
    vorlage.unlink(missing_ok=True)
    return fehler


def main(argv: list[str]) -> int:
    stand = "17.08.2026"
    was = argv[0] if argv else "beides"

    if was in ("archiv", "beides"):
        # FALLSTRICK 1: nicht ungefragt ueberschreiben.
        if ARCHIV.exists() and "--ueberschreiben" not in argv:
            print(f"  {ARCHIV.relative_to(ROOT)} existiert bereits.\n"
                  "  Sie ist die eingefrorene Fassung VOR der Verdichtung der\n"
                  "  Docstrings. Ein neuer Lauf wuerde die Kurzfassung schreiben.\n"
                  "  Bewusst gewollt? Dann mit --ueberschreiben aufrufen.")
        else:
            ARCHIV.parent.mkdir(parents=True, exist_ok=True)
            ARCHIV.write_text(baue_archiv(stand), encoding="utf-8")
            print(f"  {ARCHIV.relative_to(ROOT)}  "
                  f"{len(ARCHIV.read_text(encoding='utf-8').splitlines()):,} Zeilen")

    if was in ("pdf", "beides"):
        # FALLSTRICK 1 gilt auch hier: Die PDFs sind dieselbe eingefrorene
        # Fassung, nur gesetzt. Ein neuer Lauf schriebe die Kurzfassung.
        vorhanden = [PDF_ORDNER / f"{o}.pdf" for o in PDF_ABSCHNITTE]
        if any(p.exists() for p in vorhanden) and "--ueberschreiben" not in argv:
            print(f"  {PDF_ORDNER.relative_to(ROOT)}/ enthaelt bereits PDFs.\n"
                  "  Sie zeigen die Fassung VOR der Verdichtung der Docstrings.\n"
                  "  Bewusst gewollt? Dann mit --ueberschreiben aufrufen.")
            return 0
        return baue_pdfs(stand)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
