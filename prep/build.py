"""
DER EINE BEFEHL. Erzeugt aus den Rohdaten die beiden finalen Datensaetze.

    python prep/build.py            Aufbereitung
    python prep/build.py tests      anschliessend die Pruefungen

Eingang: data/raw/*  (sechs Rohquellen)
Ausgang: data/processed/regression.parquet      4.620 x 25, modellfertig
         data/processed/klassifikation.parquet  4.619 x 29, modellfertig

- Zwei Schritte: s1_daten laedt und verortet, s2_datensaetze baut das Panel.
- Dieser Ordner erzeugt DATEN und sonst nichts. Baselines liegen in
  vorpruefung/, der Verfahrensvergleich in modelle/.
- Downloads steuern die DOWNLOAD_*-Schalter in config.py. Auf False (Default)
  laeuft alles aus data/raw, ohne Internet und ohne API-Key.
- Modellfertig heisst: identische Zeilen, Merkmale und Folds fuer alle
  Verfahren.

Ausfuehrlich: docs/08_FUNKTIONSDOKUMENTATION.md
"""
import subprocess
import sys
import time

import pandas as pd

from config import (PFAD_EINSAETZE, PFAD_KLASSIFIKATION, PFAD_REGRESSION,
                    PROCESSED_DIR, ROOT)

DATEIEN = [
    (PFAD_EINSAETZE,      "Zwischenstand, ein Einsatz je Zeile"),
    (PFAD_REGRESSION,     "FINAL - Menge, Stadtteil x Monat"),
    (PFAD_KLASSIFIKATION, "FINAL - Struktur, Stadtteil x Monat"),
]


def schritt(nummer: str, titel: str) -> float:
    """Gibt die Ueberschrift eines Arbeitsschrittes aus und startet die Uhr.

    Ein:  Nummer wie "1/2", Titel des Schrittes
    Aus:  Startzeitpunkt, gegen den die Dauer gerechnet wird
    """
    print(f"\n{'=' * 78}\n  SCHRITT {nummer}: {titel}\n{'=' * 78}\n")
    return time.time()


def uebersicht() -> None:
    """Steckbrief der erzeugten Dateien: Zeilen, Spalten, Groesse, Zeitraum.

    Ein:  die Konstante DATEIEN mit Pfad und Beschreibung
    Aus:  nichts, reine Konsolenausgabe

    - fehlt eine Datei, wird sie als FEHLT gemeldet statt den Lauf abzubrechen
    - so ist beim Teillauf sofort sichtbar, was noch aussteht
    """
    print(f"\n{'=' * 78}\n  ERGEBNIS\n{'=' * 78}\n")
    for pfad, beschreibung in DATEIEN:
        if not pfad.exists():
            print(f"  {pfad.name:<26} FEHLT")
            continue
        d = pd.read_parquet(pfad)
        # einsaetze.parquet ist Einsatz-Ebene und hat keinen Monatsschluessel.
        zeitraum = (f"{d['jahr_monat'].min()}-{d['jahr_monat'].max()}"
                    if "jahr_monat" in d.columns else "-")
        print(f"  {pfad.name:<26} {len(d):>8,} Zeilen | {len(d.columns):>2} Spalten "
              f"| {pfad.stat().st_size / 1_048_576:>5.1f} MB | {zeitraum}")
        print(f"  {'':<26} {beschreibung}")
    print(f"\n  Die beiden FINAL-Dateien liegen in "
          f"{PROCESSED_DIR.relative_to(ROOT)} und sind modellfertig.")
    print("  Naechster Schritt: python vorpruefung/run.py")


def main() -> int:
    """Faehrt beide Aufbereitungsschritte, dann die Uebersicht.

    Ein:  optional das Argument "tests"
    Aus:  Exitcode 0, bei "tests" der Code des Testlaufs

    - die Reihenfolge ist zwingend: s2_datensaetze liest, was s1_daten schreibt
    """
    import s1_daten
    import s2_datensaetze

    t_gesamt = time.time()

    t = schritt("1/2", "Rohdaten laden und joinen (s1_daten.py)")
    s1_daten.run_download()
    s1_daten.run_join()
    print(f"\n  Dauer: {time.time() - t:.1f}s")

    t = schritt("2/2", "Beide finalen Datensaetze (s2_datensaetze.py)")
    s2_datensaetze.run()
    print(f"\n  Dauer: {time.time() - t:.1f}s")

    uebersicht()
    print(f"\n  Gesamtdauer: {time.time() - t_gesamt:.1f}s")

    if len(sys.argv) > 1 and sys.argv[1] == "tests":
        return subprocess.call([sys.executable,
                                str(ROOT / "tests" / "test_aufbereitung.py")])
    print("  Absicherung:      python tests/test_aufbereitung.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
