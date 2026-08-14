"""
Ergebnisse sichern - vor jedem Lauf, der `results/` ueberschreibt.

    python tools/sichere_ergebnisse.py                 mit automatischem Namen
    python tools/sichere_ergebnisse.py budget50        mit eigenem Namen
    python tools/sichere_ergebnisse.py --liste         zeigt vorhandene Sicherungen
    python tools/sichere_ergebnisse.py alt --hinweis "Lauf vom 07.08., Budget 50"

--------------------------------------------------------------------------
EINE GRENZE, DIE MAN KENNEN MUSS
--------------------------------------------------------------------------
Das Skript liest die Konfiguration **zum Zeitpunkt der Sicherung** aus
`config_modelle.py`. Es kann NICHT wissen, mit welcher Einstellung die
Dateien in `results/` tatsaechlich entstanden sind - diese Information steht
nirgends in den Ergebnissen.

Wurde die Konfiguration nach dem Lauf und vor der Sicherung geaendert,
beschreibt das Manifest die falsche. Genau das ist am 14.08.2026 passiert:
`TUNING_BUDGET` stand bereits auf 100 (#50), waehrend die gesicherten
Ergebnisse aus dem Lauf mit Budget 50 stammten.

Daraus die Regel: **erst sichern, dann die Konfiguration aendern.** Wo das
nicht mehr geht, `--hinweis` benutzen - der Text steht im Manifest ganz oben.

Ausgang: archiv/JJJJ-MM-TT_<name>/

NICHT TEIL DER ABGABE. Arbeitswerkzeug wie `pruefe_zahlen.py`.

--------------------------------------------------------------------------
WOZU
--------------------------------------------------------------------------
`results/` ist die einzige Stelle, an der die Ergebnisse liegen, und jeder
Lauf ueberschreibt sie. Der Ordner ist zudem in `.gitignore` - es gibt also
weder eine Versionierung noch ein Zurueck. Ein Lauf mit geaenderter
Konfiguration ist damit unumkehrbar, solange niemand vorher kopiert.

Genau das passiert am Sonntag: Budget 100 und vier erweiterte Suchraeume
(#49, #50) erzeugen andere Hyperparameter und damit andere Guetemasse. Ohne
Sicherung waere der Stand vom 07.08. weg - und mit ihm die Vergleichsbasis
fuer die Frage, ob die Aenderung etwas gebracht hat.

--------------------------------------------------------------------------
WAS GESICHERT WIRD - und warum ein Manifest dazugehoert
--------------------------------------------------------------------------
Kopiert wird der gesamte Baum `results/`. Daneben entsteht `manifest.md` mit

  - Datum, Uhrzeit, Git-Commit und Branch
  - der KONFIGURATION, die diese Ergebnisse erzeugt hat: Tuning-Budget,
    Wiederholungen, Folds, Random State und alle Suchraeume im Wortlaut
  - Zahl der Dateien, Gesamtgroesse
  - MD5-Summe je Ergebnisdatei

Das Manifest ist der eigentliche Wert. Eine Kopie ohne Konfiguration
beantwortet spaeter nicht die Frage, die man dann hat: WELCHE Einstellung hat
diese Zahlen erzeugt? Die MD5-Summen erlauben zudem den Nachweis, dass ein
Wiederholungslauf bitgleich reproduziert - genau die Behauptung aus Kapitel 6.
"""
from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "prep"))
sys.path.insert(0, str(ROOT / "modelle"))

RESULTS = ROOT / "results"
ARCHIV = ROOT / "archiv"


def git(*args: str) -> str:
    """Git-Angabe oder ein Strich - ein fehlendes Repo ist kein Abbruchgrund."""
    try:
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True,
                              text=True, timeout=20).stdout.strip() or "–"
    except Exception:
        return "–"


def md5(pfad: Path) -> str:
    h = hashlib.md5()
    with open(pfad, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def konfiguration() -> list[str]:
    """Die Einstellungen, die den gesicherten Stand erzeugt haben.

    Wird IMPORTIERT, nicht abgeschrieben - sonst sichert das Manifest, was
    jemand einmal hineingeschrieben hat, statt was tatsaechlich galt.
    """
    zeilen = []
    try:
        from config import N_FOLDS, PRAEDIKTOREN, SAISON
        from config_modelle import (RANDOM_STATE, SUCHRAEUME, TUNING_BUDGET,
                                    WIEDERHOLUNGEN)
        zeilen += ["| Einstellung | Wert |", "|---|---|",
                   f"| `TUNING_BUDGET` | **{TUNING_BUDGET}** |",
                   f"| `WIEDERHOLUNGEN` | {WIEDERHOLUNGEN} |",
                   f"| `N_FOLDS` | {N_FOLDS} |",
                   f"| `RANDOM_STATE` | {RANDOM_STATE} |",
                   f"| Merkmale | {len(PRAEDIKTOREN) + len(SAISON)} |", "",
                   "### Suchräume", "", "```"]
        for verfahren, raum in SUCHRAEUME.items():
            zeilen.append(f"{verfahren}")
            for p, spez in raum.items():
                zeilen.append(f"    {p:<24}{spez}")
        zeilen.append("```")
    except Exception as e:                       # pragma: no cover
        zeilen.append(f"Konfiguration nicht lesbar: {e}")
    return zeilen


def manifest(ziel: Path, dateien: list[Path], hinweis: str = "") -> str:
    gesamt = sum(f.stat().st_size for f in dateien)
    zeilen = [f"# Sicherung {ziel.name}", ""]
    if hinweis:
        zeilen += [f"> **Hinweis:** {hinweis}", ""]
    zeilen += [f"Erstellt {datetime.now():%Y-%m-%d %H:%M}. "
               f"{len(dateien)} Dateien, {gesamt / 1_048_576:.1f} MB.", "",
               f"- Branch: `{git('rev-parse', '--abbrev-ref', 'HEAD')}`",
               f"- Commit: `{git('rev-parse', '--short', 'HEAD')}`",
               f"- Letzte Meldung: {git('log', '-1', '--pretty=%s')}", "",
               "## Konfiguration **zum Zeitpunkt der Sicherung**", "",
               "> Gelesen aus `config_modelle.py`, nicht aus den Ergebnissen —",
               "> die enthalten ihre Einstellung nicht. Wurde die Konfiguration",
               "> nach dem Lauf geändert, beschreibt dieser Abschnitt die",
               "> falsche. Im Zweifel gilt der Hinweis oben.", ""]
    zeilen += konfiguration()
    zeilen += ["", "## Dateien", "", "| Datei | Größe | MD5 |", "|---|---|---|"]
    for f in sorted(dateien):
        zeilen.append(f"| `{f.relative_to(RESULTS)}` | "
                      f"{f.stat().st_size / 1024:.0f} kB | `{md5(f)}` |")
    return "\n".join(zeilen) + "\n"


def main(argv: list[str]) -> int:
    if "--liste" in argv:
        if not ARCHIV.is_dir():
            print("  Noch keine Sicherung vorhanden.")
            return 0
        print(f"\n  Sicherungen in {ARCHIV.relative_to(ROOT)}:\n")
        for d in sorted(ARCHIV.iterdir()):
            if not d.is_dir():
                continue
            n = sum(1 for _ in d.rglob("*") if _.is_file())
            mb = sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
            print(f"    {d.name:<34}{n:>4} Dateien  {mb / 1_048_576:>6.1f} MB")
        return 0

    if not RESULTS.is_dir():
        raise SystemExit("results/ fehlt - es gibt nichts zu sichern.")
    dateien = [f for f in RESULTS.rglob("*") if f.is_file()]
    if not dateien:
        raise SystemExit("results/ ist leer - es gibt nichts zu sichern.")

    hinweis = ""
    if "--hinweis" in argv:
        i = argv.index("--hinweis")
        hinweis = argv[i + 1] if i + 1 < len(argv) else ""
        argv = argv[:i] + argv[i + 2:]

    name = next((a for a in argv if not a.startswith("--")), None)
    marke = f"{datetime.now():%Y-%m-%d}" + (f"_{name}" if name else "")
    ziel = ARCHIV / marke
    if ziel.exists():
        # NICHT ueberschreiben. Eine Sicherung, die eine Sicherung loescht,
        # ist keine.
        i = 2
        while (ARCHIV / f"{marke}_{i}").exists():
            i += 1
        ziel = ARCHIV / f"{marke}_{i}"
        print(f"  {marke} existiert bereits - schreibe nach {ziel.name}")

    ziel.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(RESULTS, ziel)
    (ziel / "manifest.md").write_text(manifest(ziel, dateien, hinweis),
                                      encoding="utf-8")

    gesamt = sum(f.stat().st_size for f in dateien)
    print(f"\n  {len(dateien)} Dateien gesichert ({gesamt / 1_048_576:.1f} MB)")
    print(f"  => {ziel.relative_to(ROOT)}")
    print(f"  => {(ziel / 'manifest.md').relative_to(ROOT)}"
          f"   (Konfiguration und MD5-Summen)")
    print("\n  results/ ist unveraendert. Der Lauf kann starten.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
