"""
DER EINE BEFEHL. Erzeugt aus den Rohdaten die beiden finalen Datensaetze.

    python prep/build.py

Ablauf:

    1  s01_laden.py         -> data/raw/*   (nur was in config.py auf True steht)
    2  s02_einsaetze.py             -> data/processed/einsaetze.parquet
    3  s03_datensaetze.py      -> data/processed/regression.parquet      FINAL
                           -> data/processed/klassifikation.parquet  FINAL
    4  s04_eignungspruefung.py -> results/eignungspruefung/

Schritt 4 prueft, ob Ridge Regression, Random Forest und XGBoost zu den eben
erzeugten Datensaetzen passen, und faellt ein explizites Urteil je Verfahren.
Ein nicht erfuelltes Kriterium fuehrt zu Exit-Code 1 - die Aufbereitung gilt
dann als nicht abgenommen.

Die beiden FINAL markierten Dateien sind das Einzige, was die Modellskripte
unter modelle/ lesen.

Argumente (optional, sonst laeuft alles):
    python prep/build.py daten        nur Schritte 1-3, ohne Eignungspruefung
    python prep/build.py pruefung     nur Schritt 4 auf vorhandenen Datensaetzen

Downloads werden ueber die DOWNLOAD_*-Schalter in config.py gesteuert. Stehen
sie auf False (Default), arbeitet der Befehl allein aus data/raw und braucht
weder Internet noch API-Key.
"""
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


def fertig(t0: float) -> None:
    print(f"\n  Dauer: {time.time() - t0:.1f}s")


def uebersicht() -> None:
    print(f"\n{'=' * 78}\n  ERGEBNIS\n{'=' * 78}\n")
    for pfad, beschreibung in DATEIEN:
        if not pfad.exists():
            print(f"  {pfad.name:<26} FEHLT")
            continue
        d = pd.read_parquet(pfad)
        mb = pfad.stat().st_size / 1_048_576
        zeitraum = (f"{d['jahr_monat'].min()}-{d['jahr_monat'].max()}"
                    if "jahr_monat" in d.columns else "-")
        print(f"  {pfad.name:<26} {len(d):>8,} Zeilen | {len(d.columns):>2} Spalten "
              f"| {mb:>5.1f} MB | {zeitraum}")
        print(f"  {'':<26} {beschreibung}")
    print(f"\n  Die beiden FINAL-Dateien liegen in "
          f"{PROCESSED_DIR.relative_to(ROOT)} und sind das Einzige, was die "
          f"Modellskripte lesen.")


def baue_daten() -> None:
    import s01_laden
    import s02_einsaetze
    import s03_datensaetze

    t = schritt("1/4", "Rohdaten laden (s01_laden.py)")
    s01_laden.run_download()
    fertig(t)

    t = schritt("2/4", "Joinen, Quoten, Eindeutschen (s02_einsaetze.py)")
    s02_einsaetze.run_join()
    fertig(t)

    t = schritt("3/4", "Beide finalen Datensaetze (s03_datensaetze.py)")
    s03_datensaetze.run()
    fertig(t)


def pruefe_eignung() -> bool:
    import s04_eignungspruefung

    t = schritt("4/4", "Eignungspruefung Ridge / Random Forest / XGBoost")
    bestanden = s04_eignungspruefung.main()
    fertig(t)
    return bestanden


def main() -> int:
    was = sys.argv[1] if len(sys.argv) > 1 else "alles"
    t_gesamt = time.time()

    if was in ("alles", "daten"):
        baue_daten()
        uebersicht()

    bestanden = True
    if was in ("alles", "pruefung"):
        bestanden = pruefe_eignung()

    print(f"\n  Gesamtdauer: {time.time() - t_gesamt:.1f}s")
    if not bestanden:
        print("\n  ABGELEHNT: Mindestens ein Eignungskriterium ist nicht "
              "erfuellt (Abschnitt 9 des Berichts).")
        return 1
    if was != "pruefung":
        print("\n  Naechster Schritt: python tests/test_aufbereitung.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
