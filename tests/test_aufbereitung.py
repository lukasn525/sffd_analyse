"""
Prüfungen der Datenaufbereitung – gesammelt an einer Stelle.

Diese Datei ist bewusst vom Analysecode getrennt: Die Module in `pipeline/`,
`modellierung/` und `analyse/` bleiben dadurch lesbar und eignen sich als
Code-Beleg im Anhang der Arbeit, während die Absicherung hier vollständig
nachvollziehbar bleibt.

Geprüft wird, was bei einer Änderung an den Daten oder am Code still kaputtgehen
könnte, ohne dass es in den Ergebnissen auffällt:

  1. Analysepanel      rechteckig, vollständig, fester Zeitraum
  2. Zeitschnitte      Reihenfolge, Disjunktheit, unberührtes Hold-out
  3. Merkmale          Lags gegen die Rohdaten verifiziert, kein Leakage
  4. Klassifikation    keine Ergebnisvariablen, Abgrenzung wie die Regression

Ausführen:
  python tests/test_aufbereitung.py     # ohne weitere Abhängigkeiten
  pytest tests/                          # falls pytest vorhanden
"""
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "modellierung"))

from aggregation import (ENDE, PRAEDIKTOREN, START,  # noqa: E402
                         balanciertes_panel, lade_stadtteil_monat)
from cv import (inneres_fenster, split_holdout,  # noqa: E402
                zeit_folds, zeitachse)
from features import (FEATURES_SL, LAGS, baue_features,  # noqa: E402
                      lade_modelldaten)
from klassifikation_daten import (ERGEBNISVARIABLEN, KLASSEN,  # noqa: E402
                                  MERKMALE_STRUKTUR, MERKMALE_ZEIT,
                                  lade_klassifikationsdaten)

# Erwartungswerte des festgesetzten Analysedatensatzes (Decision Log #15, #18, #19)
N_STADTTEILE = 35
N_MONATE     = 132   # 2015-01 bis 2025-12
N_PANEL      = N_STADTTEILE * N_MONATE          # 4.620
N_MONATE_LAG = N_MONATE - 12                    # nach Lag-Bildung: 120
N_MODELL     = N_STADTTEILE * N_MONATE_LAG      # 4.200


# ---------------------------------------------------------------------------
# 1. Analysepanel
# ---------------------------------------------------------------------------
def test_panel_rechteckig_und_vollstaendig():
    """Vollständiges Kreuzprodukt Stadtteil x Monat, keine fehlenden Werte.

    Ein unbalanciertes Panel wäre der gefährlichste stille Fehler: Tritt ein
    Stadtteil mitten in der Zeitreihe hinzu, springen die Testfenster-Summen
    allein dadurch, ohne dass sich am Modell etwas ändert.
    """
    p = balanciertes_panel(lade_stadtteil_monat())
    assert p["stadtteil"].nunique() == N_STADTTEILE
    assert p.groupby(["jahr", "monat"]).ngroups == N_MONATE
    assert len(p) == N_PANEL, f"{len(p)} statt {N_PANEL} Zeilen"
    assert p[PRAEDIKTOREN].notna().all().all(), "NaN in den Prädiktoren"


def test_zeitraum_festgesetzt():
    """Der Zeitraum wird aus Konstanten abgeleitet, nicht aus den Daten.

    Sonst verschiebt sich die Analyse bei jedem neuen Datendownload – und ein
    unvollständiger Randmonat kann unbemerkt ins Testfenster geraten
    (Decision Log #12: Januar 2026 mit 258 statt ~3.300 Einsätzen).
    """
    p = balanciertes_panel(lade_stadtteil_monat())
    jm = p["jahr"] * 100 + p["monat"]
    assert jm.min() == START and jm.max() == ENDE


def test_exposure_und_kriminalitaetsindex_vorhanden():
    """Die log-Transformationen sind gebildet und vollständig."""
    p = balanciertes_panel(lade_stadtteil_monat())
    assert p["log_bevoelkerung"].notna().all()
    assert p["log_kriminalitaetsindex"].notna().all()
    # Rohwerte bleiben erhalten (NegBin-Offset, Raten-Sensitivität, Kap. 5.1)
    assert {"gesamtbevoelkerung", "kriminalitaetsindex"} <= set(p.columns)


# ---------------------------------------------------------------------------
# 2. Zeitschnitte
# ---------------------------------------------------------------------------
def test_folds_ordnung_und_holdout():
    """Testfenster liegen nach dem Training, Hold-out bleibt unberührt."""
    d = lade_modelldaten()
    entwicklung, holdout = split_holdout(zeitachse(d))
    assert set(entwicklung).isdisjoint(holdout)

    for train, test in zeit_folds(entwicklung):
        assert max(train) < min(test), "Testfenster liegt nicht nach dem Training"
        assert set(test).isdisjoint(holdout), "Fold-Test greift ins Hold-out"
        sub, val = inneres_fenster(train)
        assert max(sub) < min(val) < min(test), "Inneres Fenster falsch geordnet"


# ---------------------------------------------------------------------------
# 3. Merkmale
# ---------------------------------------------------------------------------
def test_merkmale_vollstaendig():
    """Nach der Lag-Bildung bleibt das Panel rechteckig und ohne NaN."""
    d = lade_modelldaten()
    assert len(d) == N_MODELL, f"{len(d)} statt {N_MODELL} Zeilen"
    assert d["stadtteil"].nunique() == N_STADTTEILE
    assert d.groupby(["jahr", "monat"]).ngroups == N_MONATE_LAG
    assert d[FEATURES_SL].notna().all().all()
    assert d["jahr_monat"].max() == ENDE


def test_saison_zyklisch():
    """sin/cos liegen auf dem Einheitskreis, Dezember grenzt an Januar."""
    d = lade_modelldaten()
    assert np.allclose(d["monat_sin"] ** 2 + d["monat_cos"] ** 2, 1.0)
    punkt = lambda m: np.array([np.sin(2 * np.pi * m / 12),
                                np.cos(2 * np.pi * m / 12)])
    assert np.isclose(np.linalg.norm(punkt(12) - punkt(1)),
                      np.linalg.norm(punkt(1) - punkt(2))), \
        "Jahreswechsel wird nicht als Nachbarschaft abgebildet"


def test_lags_gegen_rohdaten():
    """Der zentrale Leakage-Test: Lags gegen die Rohdaten nachschlagen.

    Ein verrutschtes `shift` oder ein Wert, der über eine Stadtteilgrenze
    hinweg gezogen wird, ist im Code nicht zu sehen und rechnet das Ergebnis
    lautlos schön. Deshalb wird stichprobenartig direkt nachgeschlagen.
    """
    d = lade_modelldaten()
    roh = balanciertes_panel(lade_stadtteil_monat())
    roh["jahr_monat"] = roh["jahr"] * 100 + roh["monat"]
    nachschlag = roh.set_index(["stadtteil", "jahr_monat"])["anzahl_einsaetze"]

    def vormonat(jm: int, k: int = 1) -> int:
        jahr, monat = divmod(jm, 100)
        for _ in range(k):
            monat -= 1
            if monat == 0:
                jahr, monat = jahr - 1, 12
        return jahr * 100 + monat

    for _, r in d.sample(200, random_state=42).iterrows():
        st, jm = r["stadtteil"], int(r["jahr_monat"])
        assert r["lag_1"]  == nachschlag[(st, vormonat(jm, 1))]
        assert r["lag_12"] == nachschlag[(st, vormonat(jm, 12))]
        erwartet = np.mean([nachschlag[(st, vormonat(jm, k))] for k in (1, 2, 3)])
        assert np.isclose(r["rolling_mean_3"], erwartet)


def test_lags_nicht_gegenwartsbezogen():
    """Gegenprobe gegen ein vergessenes shift(): lag_1 darf nicht der Istwert sein."""
    d = lade_modelldaten()
    anteil = (d["lag_1"] == d["anzahl_einsaetze"]).mean()
    assert anteil < 0.10, f"lag_1 stimmt in {anteil:.1%} der Fälle mit dem Istwert überein"


# ---------------------------------------------------------------------------
# 4. Klassifikation
# ---------------------------------------------------------------------------
def test_keine_ergebnisvariablen():
    """Wichtigster Test des Klassifikationsteils.

    Sachschaden, Löschfahrzeuge, Alarmstufe und Antwortzeit stehen erst nach
    dem Einsatz fest. Rutscht eine dieser Spalten in den Merkmalssatz, springt
    der AUROC auf über 0,9 und sieht nach einem guten Ergebnis aus.
    """
    d = lade_klassifikationsdaten()
    gefunden = [c for c in ERGEBNISVARIABLEN if c in d.columns]
    assert not gefunden, f"Ergebnisvariable(n) im Datensatz: {gefunden}"


def test_klassifikation_gleiche_abgrenzung_wie_regression():
    """Beide Teile der Arbeit müssen auf demselben Datenbestand beruhen."""
    k = lade_klassifikationsdaten()
    p = balanciertes_panel(lade_stadtteil_monat())
    assert set(k["stadtteil"]) == set(p["stadtteil"])
    assert k["jahr_monat"].min() == START and k["jahr_monat"].max() == ENDE


def test_zielgroessen_konsistent():
    """Mehrklassige und binäre Zielgröße dürfen sich nicht widersprechen."""
    d = lade_klassifikationsdaten()
    assert set(d["einsatzart_gruppe"]) <= set(KLASSEN)
    assert ((d["einsatzart_gruppe"] == "Brand") == (d["ist_brand"] == 1)).all()
    assert not d["einsatz_nummer"].duplicated().any()
    assert d[MERKMALE_STRUKTUR + MERKMALE_ZEIT].notna().all().all()


# ---------------------------------------------------------------------------
def main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    print(f"Prüfungen der Datenaufbereitung ({len(tests)} Tests)\n")
    fehler = 0
    for name, funktion in tests:
        try:
            funktion()
            print(f"  [OK]     {name}")
        except AssertionError as e:
            fehler += 1
            print(f"  [FEHLER] {name}: {e}")
    print(f"\n{len(tests) - fehler}/{len(tests)} bestanden.")
    if not fehler:
        print(f"Analysedatensatz: {N_STADTTEILE} Stadtteile x {N_MONATE} Monate "
              f"= {N_PANEL:,} Beobachtungen ({START}-{ENDE}), "
              f"nach Lag-Bildung {N_MODELL:,}.")
    return 1 if fehler else 0


if __name__ == "__main__":
    sys.exit(main())
