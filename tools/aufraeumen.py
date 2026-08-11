"""
Aufraeumer - entfernt Artefakte, die kein Skript des Repos mehr erzeugt.

NICHT TEIL DER ABGABE. Wie `pruefe_zahlen.py` ein Arbeitswerkzeug: Es erzeugt
kein Ergebnis und wird von keinem Skript in prep/, vorpruefung/ oder modelle/
importiert.

    python tools/aufraeumen.py              VORSCHAU - loescht nichts
    python tools/aufraeumen.py --wirklich   loescht

--------------------------------------------------------------------------
WOZU
--------------------------------------------------------------------------
`results/eignungspruefung/` enthaelt Abbildungen aus mindestens drei Fassungen
von `v2_eignung.py`. Zwei davon sind aktuell, neun stammen vom 27.07. und
03.08. und werden von keiner Codezeile mehr geschrieben.

Das ist nicht nur unordentlich, sondern eine Falle derselben Art, gegen die
`pruefe_zahlen.py` gebaut wurde: `02_linearitaet.png` und
`01_streudiagramme.png` sind byte-identisch - dieselbe Abbildung unter zwei
Namen. Wer in LaTeX den alten Namen einbindet, bekommt ohne Fehlermeldung ein
Bild vom Juli. Nur faellt es hier nicht durch einen Exit-Code auf, sondern erst
im gedruckten Dokument.

--------------------------------------------------------------------------
WIE DIE LISTE ENTSTEHT - und warum sie nicht hier steht
--------------------------------------------------------------------------
Die Namen der aktuellen Abbildungen sind NICHT in dieser Datei aufgezaehlt.
Sie werden aus dem Quelltext von `v2_eignung.py` gelesen: Was dort als
Zeichenkette mit Endung .png oder .md vorkommt, gilt als aktuell, alles andere
im Ordner als verwaist.

Der Grund ist derselbe wie ueberall in diesem Projekt: Eine Liste an zwei
Orten laeuft auseinander. Nennt jemand eine Abbildung in `v2_eignung.py` um,
zieht dieses Skript automatisch nach. Stuende die Liste hier, wuerde es beim
naechsten Lauf die neue Abbildung loeschen.

--------------------------------------------------------------------------
WAS BEWUSST NICHT GELOESCHT WIRD
--------------------------------------------------------------------------
`data/sample/*.csv` und `results/sffd_fire_incidents_report.pdf` sind in git
verzeichnet und stammen aus der Zeit vor der Pipeline. Sie werden von keinem
Skript gelesen, aber sie zu entfernen ist eine Entscheidung ueber den
Repo-Inhalt und braucht einen Commit - kein Aufraeumen. Sie werden nur
gemeldet.
"""
from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Ordner, deren Inhalt aus dem Quelltext abgeleitet wird: (Ordner, Skript).
GEPRUEFTE_ORDNER = [
    (ROOT / "results" / "eignungspruefung", ROOT / "vorpruefung" / "v2_eignung.py"),
]
GEPRUEFTE_ENDUNGEN = {".png", ".md"}

# Leere Ordner ohne Zweck. Werden nur entfernt, wenn sie TATSAECHLICH leer sind.
LEERE_ORDNER = [ROOT / ".dist", ROOT / "data" / "interim"]

# Nur melden, nicht anfassen - siehe Docstring.
NUR_MELDEN = [
    (ROOT / "data" / "sample", "in git verzeichnet, von keinem Skript gelesen"),
    (ROOT / "results" / "sffd_fire_incidents_report.pdf",
     "in git verzeichnet, aelter als die gesamte Pipeline"),
]


# ==========================================================================
def erzeugte_namen(skript: Path) -> set[str]:
    """Dateinamen, die dieses Skript schreibt - aus seinem Quelltext gelesen.

    Bewusst grob: Jede Zeichenkette mit passender Endung zaehlt, auch wenn sie
    nur gelesen und nicht geschrieben wird. Der Fehler geht damit in die
    sichere Richtung - im Zweifel bleibt eine Datei stehen, statt dass eine
    gebrauchte verschwindet.
    """
    if not skript.exists():
        return set()
    text = skript.read_text(encoding="utf-8")
    return set(re.findall(r'["\']([\w\-.]+\.(?:png|pdf|md|csv))["\']', text))


def verwaiste_dateien() -> list[tuple[Path, str]]:
    """Dateien in den geprueften Ordnern, die kein Skript mehr erzeugt."""
    treffer = []
    for ordner, skript in GEPRUEFTE_ORDNER:
        if not ordner.is_dir():
            continue
        aktuell = erzeugte_namen(skript)
        if not aktuell:
            print(f"  HINWEIS: {skript.name} nicht lesbar - {ordner.name} "
                  f"wird uebersprungen.")
            continue
        for pfad in sorted(ordner.iterdir()):
            if (pfad.is_file() and pfad.suffix in GEPRUEFTE_ENDUNGEN
                    and pfad.name not in aktuell):
                treffer.append((pfad, f"nicht mehr von {skript.name} erzeugt"))
    return treffer


def pycache_ordner() -> list[tuple[Path, str]]:
    """Alle __pycache__. Sie werden beim naechsten Import neu angelegt.

    Der eigentliche Anlass sind die Reste geloeschter Module - `m01_eignung`,
    `m02_regression`, `m03_klassifikation`, `s3_baselines`. Zu denen gibt es
    keine .py mehr; ein `import m01_eignung` wuerde die alte .pyc trotzdem
    nicht laden, aber die Dateien behaupten eine Struktur, die es nicht gibt.
    """
    treffer = []
    for pfad in sorted(ROOT.rglob("__pycache__")):
        if "venv" in pfad.parts or ".git" in pfad.parts:
            continue
        verwaist = sorted(p.name.split(".")[0] for p in pfad.glob("*.pyc")
                          if not (pfad.parent / f"{p.name.split('.')[0]}.py").exists())
        grund = ("Bytecode-Cache"
                 + (f", darunter geloeschte Module: {', '.join(sorted(set(verwaist)))}"
                    if verwaist else ""))
        treffer.append((pfad, grund))
    return treffer


def leere_ordner() -> list[tuple[Path, str]]:
    treffer = []
    for pfad in LEERE_ORDNER:
        if pfad.is_dir() and not any(pfad.iterdir()):
            treffer.append((pfad, "leer, ohne Zweck"))
        elif pfad.is_dir():
            print(f"  HINWEIS: {pfad.relative_to(ROOT)} ist NICHT leer - "
                  f"bleibt stehen.")
    return treffer


# ==========================================================================
def groesse(pfad: Path) -> int:
    if pfad.is_file():
        return pfad.stat().st_size
    return sum(p.stat().st_size for p in pfad.rglob("*") if p.is_file())


def zeige(titel: str, eintraege: list[tuple[Path, str]]) -> int:
    if not eintraege:
        return 0
    print(f"\n  {titel}")
    summe = 0
    for pfad, grund in eintraege:
        b = groesse(pfad)
        summe += b
        art = "/" if pfad.is_dir() else ""
        print(f"    {str(pfad.relative_to(ROOT)) + art:<58} "
              f"{b / 1024:>7.0f} kB   {grund}")
    return summe


def main(argv: list[str]) -> int:
    wirklich = "--wirklich" in argv
    print(f"\n{'=' * 78}\n  AUFRAEUMEN - "
          f"{'LOESCHT' if wirklich else 'VORSCHAU, loescht nichts'}\n{'=' * 78}")

    gruppen = [
        ("Verwaiste Ergebnisdateien", verwaiste_dateien()),
        ("Bytecode-Caches", pycache_ordner()),
        ("Leere Ordner", leere_ordner()),
    ]
    summe = sum(zeige(titel, eintraege) for titel, eintraege in gruppen)
    alle = [p for _, eintraege in gruppen for p, _ in eintraege]

    if not alle:
        print("\n  Nichts zu tun.")
    else:
        print(f"\n  {len(alle)} Eintraege, zusammen {summe / 1024:.0f} kB.")

    print("\n  NUR GEMELDET - Entscheidung ueber den Repo-Inhalt, kein Aufraeumen:")
    for pfad, grund in NUR_MELDEN:
        if pfad.exists():
            print(f"    {str(pfad.relative_to(ROOT)):<58} "
                  f"{groesse(pfad) / 1024:>7.0f} kB   {grund}")

    if not wirklich:
        print("\n  Nichts geloescht. Zum Ausfuehren:"
              "\n  python tools/aufraeumen.py --wirklich")
        return 0

    for pfad in alle:
        if pfad.is_dir():
            shutil.rmtree(pfad)
        else:
            pfad.unlink()
    print(f"\n  {len(alle)} Eintraege geloescht ({summe / 1024:.0f} kB).")
    print("  Gegenprobe: python vorpruefung/v2_eignung.py legt die beiden "
          "aktuellen Abbildungen neu an.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
