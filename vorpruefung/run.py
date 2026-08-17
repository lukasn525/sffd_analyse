"""
Der eine Befehl der Vorpruefung.

    python vorpruefung/run.py

Eingang: data/processed/{regression,klassifikation}.parquet
Ausgang: results/{regression,klassifikation}/baselines_*.csv
         results/eignungspruefung/eignungspruefung.md

  - Schritt 1  v1_baselines.py  legt die Messlatte: Stufe 1 (trivial) und
    Stufe 2 (einfachste zur Datenform passende Form)
  - Schritt 2  v2_eignung.py    prueft, welche Verfahrensklasse zu welcher
    Zielgroesse passt
  - Die Reihenfolge ist zwingend: die Eignungspruefung LIEST die
    Baseline-Werte, sie rechnet sie nicht neu
  - v0_aufteilung.py (Selbsttest), v3_spezifikation.py und v4_decke.py
    laufen einzeln und haengen nicht an diesem Befehl
  - Voraussetzung ist ein Lauf von prep/build.py

Ausfuehrliche Fassung: docs/08_FUNKTIONSDOKUMENTATION.md
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def schritt(nummer: str, titel: str) -> None:
    """Gibt die Ueberschrift eines Arbeitsschrittes aus.

    Ein:  Nummer wie "1/2", Titel des Schrittes
    Aus:  nichts, reine Konsolenausgabe
    """
    print(f"\n{'=' * 78}\n  SCHRITT {nummer}: {titel}\n{'=' * 78}\n")


def main() -> int:
    """Faehrt beide Schritte der Vorpruefung nacheinander.

    Ein:  nichts; setzt einen Lauf von prep/build.py voraus
    Aus:  Exitcode 0

    - Schritt 1 v1_baselines.run(), Schritt 2 v2_eignung.main()
    - die Reihenfolge ist zwingend: die Eignungspruefung liest die
      Baseline-Werte, sie rechnet sie nicht neu
    """
    import v1_baselines
    import v2_eignung

    start = time.time()

    schritt("1/2", "Messlatte festlegen (v1_baselines.py)")
    v1_baselines.run()

    schritt("2/2", "Verfahrenseignung pruefen (v2_eignung.py)")
    v2_eignung.main()

    print(f"\n  Gesamtdauer: {time.time() - start:.1f}s")
    print("  Naechster Schritt: modelle/ - der Verfahrensvergleich")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
