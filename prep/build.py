"""
DER EINE BEFEHL. Erzeugt aus den Rohdaten die beiden finalen Datensaetze.

    python prep/build.py

Ablauf:

    1  s1_daten.py        -> data/raw/*  (nur was in config.py auf True steht)
                          -> data/processed/einsaetze.parquet   Zwischenstand
    2  s2_datensaetze.py  -> data/processed/regression.parquet      FINAL
                          -> data/processed/klassifikation.parquet  FINAL
    3  s3_baselines.py    -> results/regression/baselines_*.csv

Danach ist die Aufbereitung fertig. Die beiden FINAL markierten Dateien sind
modellfertig: identische Zeilen, Merkmale und Folds fuer alle drei Verfahren.
Schritt 3 legt die Referenzwerte fest, bevor modelliert wird (Auflage
Schroeter, 27.07.2026). Alles Weitere liegt unter modelle/.

Argumente (optional):
    python prep/build.py daten        wie ohne Argument
    python prep/build.py tests        anschliessend die 14 Pruefungen laufen lassen

Downloads werden ueber die DOWNLOAD_*-Schalter in config.py gesteuert. Stehen
sie auf False (Default), arbeitet der Befehl allein aus data/raw und braucht
weder Internet noch API-Key.
"""
import subprocess
import sys
import time

import pandas as pd

from config import (PFAD_EINSAETZE, PFAD_KLASSIFIKATION, PFAD_REGRESSION,
                    PROCESSED_DIR, ROOT)

DATEIEN = [
    (PFAD_EINSAETZE,      "Zwischenstand, ein Einsatz je Zeile"),
    (PFAD_REGRESSION,     "FINAL - Regression, Stadtteil x Monat"),
    (PFAD_KLASSIFIKATION, "FINAL - Klassifikation, Einzeleinsatz"),
]


def schritt(nummer: str, titel: str) -> float:
    print(f"\n{'=' * 78}\n  SCHRITT {nummer}: {titel}\n{'=' * 78}\n")
    return time.time()


def uebersicht() -> None:
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
    print("  Naechster Schritt: python modelle/m01_eignung.py")


def main() -> int:
    import s1_daten
    import s2_datensaetze
    import s3_baselines

    t_gesamt = time.time()

    t = schritt("1/3", "Rohdaten laden und joinen (s1_daten.py)")
    s1_daten.run_download()
    s1_daten.run_join()
    print(f"\n  Dauer: {time.time() - t:.1f}s")

    t = schritt("2/3", "Beide finalen Datensaetze (s2_datensaetze.py)")
    s2_datensaetze.run()
    print(f"\n  Dauer: {time.time() - t:.1f}s")

    t = schritt("3/3", "Vergleichsgroessen (s3_baselines.py)")
    s3_baselines.run()
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
