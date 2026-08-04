"""
DER EINE BEFEHL der Vorpruefung.

    python vorpruefung/run.py

Ablauf:

    1  v1_baselines.py  -> results/{regression,klassifikation}/baselines_*.csv
                           Stufe 1 (trivial) und Stufe 2 (einfachste passende Form)
    2  v2_eignung.py    -> results/eignungspruefung/eignungspruefung.md
                           Welche Verfahrensklasse passt zu welcher Zielgroesse?

Reihenfolge ist zwingend: Die Eignungspruefung LIEST die Baseline-Werte.

Danach steht fest, was die Vergleichsverfahren in modelle/ schlagen muessen und
warum sie ueberhaupt antreten. Voraussetzung ist ein Lauf von prep/build.py.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def schritt(nummer: str, titel: str) -> None:
    print(f"\n{'=' * 78}\n  SCHRITT {nummer}: {titel}\n{'=' * 78}\n")


def main() -> int:
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
