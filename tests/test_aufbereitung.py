"""
Prüfungen der Datenaufbereitung – gesammelt an einer Stelle.

Diese Datei ist bewusst vom Analysecode getrennt: Die Module unter `prep/`
bleiben dadurch lesbar und eignen sich als Code-Beleg im Anhang der Arbeit,
während die Absicherung hier vollständig nachvollziehbar bleibt.

Geprüft werden die fertigen Datensätze in `data/processed/`, nicht der Code, der
sie erzeugt. Damit fällt auch auf, wenn jemand eine Datei von Hand ändert.

Gegenstand:

  1. Analysedatensatz  rechteckig, vollständig, fester Zeitraum
  2. Zeitschnitte      Reihenfolge, Disjunktheit, unberührtes Hold-out,
                       Aufteilungsspalten konsistent zu prep/cv.py
  3. Merkmale          Lags gegen die Rohdaten verifiziert, kein Leakage
  4. Klassifikation    keine Ergebnisvariablen, Abgrenzung wie die Regression

Ausführen:
  python tests/test_aufbereitung.py     # ohne weitere Abhängigkeiten
  pytest tests/                          # falls pytest vorhanden

Setzt voraus, dass `python prep/build.py` gelaufen ist.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "prep"))

from config import (ENDE, ERGEBNISVARIABLEN, FEATURE_SETS,  # noqa: E402
                    KLASSEN, MERKMALE_STRUKTUR, MERKMALE_ZEIT,
                    PFAD_KLASSIFIKATION, PFAD_REGRESSION, PRAEDIKTOREN, START,
                    VORLAUF_MONATE)
from cv import (ergaenze_aufteilung, fold_masken, inneres_fenster,  # noqa: E402
                split_holdout, zeit_folds, zeitachse)
from regression_datensatz import _monat_minus, aggregiere  # noqa: E402

# Erwartungswerte des festgesetzten Analysedatensatzes
# (Decision Log #15, #18, #19, #23)
N_STADTTEILE = 35
N_MONATE     = 132                              # 2015-01 bis 2025-12
N_MODELL     = N_STADTTEILE * N_MONATE          # 4.620
N_EINSAETZE  = 350_481

_cache: dict[str, pd.DataFrame] = {}


def regression() -> pd.DataFrame:
    if "r" not in _cache:
        if not PFAD_REGRESSION.exists():
            raise SystemExit("regression.parquet fehlt – erst 'python prep/build.py'.")
        _cache["r"] = pd.read_parquet(PFAD_REGRESSION)
    return _cache["r"]


def klassifikation() -> pd.DataFrame:
    if "k" not in _cache:
        if not PFAD_KLASSIFIKATION.exists():
            raise SystemExit("klassifikation.parquet fehlt – erst 'python prep/build.py'.")
        _cache["k"] = pd.read_parquet(PFAD_KLASSIFIKATION)
    return _cache["k"]


# ---------------------------------------------------------------------------
# 1. Analysedatensatz
# ---------------------------------------------------------------------------
def test_panel_rechteckig_und_vollstaendig():
    """Vollständiges Kreuzprodukt Stadtteil x Monat, keine fehlenden Werte.

    Ein unbalanciertes Panel wäre der gefährlichste stille Fehler: Tritt ein
    Stadtteil mitten in der Zeitreihe hinzu, springen die Testfenster-Summen
    allein dadurch, ohne dass sich am Modell etwas ändert.
    """
    d = regression()
    assert d["stadtteil"].nunique() == N_STADTTEILE
    assert d.groupby(["jahr", "monat"]).ngroups == N_MONATE
    assert len(d) == N_MODELL, f"{len(d)} statt {N_MODELL} Zeilen"
    assert d[PRAEDIKTOREN].notna().all().all(), "NaN in den Prädiktoren"


def test_zeitraum_festgesetzt():
    """Der Zeitraum kommt aus Konstanten, nicht aus den Daten.

    Sonst verschiebt sich die Analyse bei jedem neuen Download – und ein
    unvollständiger Randmonat kann unbemerkt ins Testfenster geraten
    (Decision Log #12: Januar 2026 mit 258 statt ~3.300 Einsätzen).
    """
    d = regression()
    assert d["jahr_monat"].min() == START, \
        f"Beginn {d['jahr_monat'].min()} statt {START} – Lag-Vorlauf prüfen"
    assert d["jahr_monat"].max() == ENDE


def test_exposure_und_kriminalitaetsindex_vorhanden():
    """Die log-Transformationen sind gebildet und vollständig."""
    d = regression()
    assert d["log_bevoelkerung"].notna().all()
    assert d["log_kriminalitaetsindex"].notna().all()
    # Rohwerte bleiben erhalten (NegBin-Offset, Raten-Sensitivität, Kap. 5.1)
    assert {"gesamtbevoelkerung", "kriminalitaetsindex"} <= set(d.columns)


# ---------------------------------------------------------------------------
# 2. Zeitschnitte
# ---------------------------------------------------------------------------
def test_folds_ordnung_und_holdout():
    """Testfenster liegen nach dem Training, Hold-out bleibt unberührt."""
    entwicklung, holdout = split_holdout(zeitachse(regression()))
    assert set(entwicklung).isdisjoint(holdout)

    for train, test in zeit_folds(entwicklung):
        assert max(train) < min(test), "Testfenster liegt nicht nach dem Training"
        assert set(test).isdisjoint(holdout), "Fold-Test greift ins Hold-out"
        sub, val = inneres_fenster(train)
        assert max(sub) < min(val) < min(test), "Inneres Fenster falsch geordnet"


def test_aufteilungsspalten_konsistent():
    """`fold` und `ist_holdout` in der Datei müssen zu prep/cv.py passen.

    Die Spalten sind der Grund, warum die Fairness-Regel nachzählbar ist. Wären
    sie veraltet – etwa weil jemand den Zeitraum geändert, aber den Datensatz
    nicht neu gebaut hat –, liefen alle drei Verfahren auf falschen, aber
    untereinander identischen Splits: der Vergleich bliebe fair, das Ergebnis
    wäre trotzdem falsch.
    """
    d = regression()
    neu = ergaenze_aufteilung(d.drop(columns=["fold", "ist_holdout"]))
    assert (neu["fold"].to_numpy() == d["fold"].to_numpy()).all()
    assert (neu["ist_holdout"].to_numpy() == d["ist_holdout"].to_numpy()).all()

    # Hold-out darf in keinem Fold-Training oder -Test vorkommen.
    for k in sorted(x for x in d["fold"].unique() if x > 0):
        train, test = fold_masken(d, k)
        assert not (train & (d["ist_holdout"] == 1)).any()
        assert not (test & (d["ist_holdout"] == 1)).any()
        assert d.loc[train, "jahr_monat"].max() < d.loc[test, "jahr_monat"].min()


# ---------------------------------------------------------------------------
# 3. Merkmale
# ---------------------------------------------------------------------------
def test_merkmale_vollstaendig():
    """Beide Merkmalssätze sind vollständig – auch nach der Lag-Bildung."""
    d = regression()
    for name, spalten in FEATURE_SETS.items():
        assert d[spalten].notna().all().all(), f"NaN im Merkmalssatz {name}"
    assert d["jahr_monat"].max() == ENDE


def test_saison_zyklisch():
    """sin/cos liegen auf dem Einheitskreis, Dezember grenzt an Januar."""
    d = regression()
    assert np.allclose(d["monat_sin"] ** 2 + d["monat_cos"] ** 2, 1.0)
    punkt = lambda m: np.array([np.sin(2 * np.pi * m / 12),
                                np.cos(2 * np.pi * m / 12)])
    assert np.isclose(np.linalg.norm(punkt(12) - punkt(1)),
                      np.linalg.norm(punkt(1) - punkt(2))), \
        "Jahreswechsel wird nicht als Nachbarschaft abgebildet"


def test_lags_gegen_rohdaten():
    """Der zentrale Leakage-Test: Lags gegen die Rohdaten nachschlagen.

    Ein verrutschtes `shift` oder ein Wert, der über eine Stadtteilgrenze hinweg
    gezogen wird, ist im Code nicht zu sehen und rechnet das Ergebnis lautlos
    schön. Deshalb wird stichprobenartig direkt nachgeschlagen – inklusive der
    Vorlaufmonate, die für lag_12 des ersten Analysemonats gebraucht werden.
    """
    d = regression()
    roh = aggregiere(von=_monat_minus(START, VORLAUF_MONATE), bis=ENDE)
    nachschlag = roh.set_index(["stadtteil", "jahr_monat"])["anzahl_einsaetze"]

    for _, r in d.sample(200, random_state=42).iterrows():
        st, jm = r["stadtteil"], int(r["jahr_monat"])
        assert r["lag_1"]  == nachschlag[(st, _monat_minus(jm, 1))]
        assert r["lag_12"] == nachschlag[(st, _monat_minus(jm, 12))]
        erwartet = np.mean([nachschlag[(st, _monat_minus(jm, k))] for k in (1, 2, 3)])
        assert np.isclose(r["rolling_mean_3"], erwartet)


def test_lags_nicht_gegenwartsbezogen():
    """Gegenprobe gegen ein vergessenes shift(): lag_1 darf nicht der Istwert sein."""
    d = regression()
    anteil = (d["lag_1"] == d["anzahl_einsaetze"]).mean()
    assert anteil < 0.10, \
        f"lag_1 stimmt in {anteil:.1%} der Fälle mit dem Istwert überein"


def test_vorlauf_ohne_eigene_zeilen():
    """Die Vorlaufmonate liefern Lag-Werte, aber keine eigenen Beobachtungen.

    Sonst enthielte der Datensatz Zeilen ohne gültige Strukturmerkmale – der
    Kriminalitätsindex beginnt erst 2015-01 (Decision Log #23).
    """
    d = regression()
    assert (d["jahr_monat"] >= START).all()
    assert d["lag_12"].notna().all(), "Anlaufmonate ohne lag_12 im Datensatz"


# ---------------------------------------------------------------------------
# 4. Klassifikation
# ---------------------------------------------------------------------------
def test_keine_ergebnisvariablen():
    """Wichtigster Test des Klassifikationsteils.

    Sachschaden, Löschfahrzeuge, Alarmstufe und Antwortzeit stehen erst nach dem
    Einsatz fest. Rutscht eine dieser Spalten in den Merkmalssatz, springt der
    AUROC auf über 0,9 und sieht nach einem guten Ergebnis aus.
    """
    gefunden = [c for c in ERGEBNISVARIABLEN if c in klassifikation().columns]
    assert not gefunden, f"Ergebnisvariable(n) im Datensatz: {gefunden}"


def test_klassifikation_gleiche_abgrenzung_wie_regression():
    """Beide Teile der Arbeit müssen auf demselben Datenbestand beruhen."""
    k, d = klassifikation(), regression()
    assert set(k["stadtteil"]) == set(d["stadtteil"])
    assert k["jahr_monat"].min() == d["jahr_monat"].min() == START
    assert k["jahr_monat"].max() == d["jahr_monat"].max() == ENDE
    assert len(k) == N_EINSAETZE, f"{len(k):,} statt {N_EINSAETZE:,} Einsätze"


def test_zielgroessen_konsistent():
    """Mehrklassige und binäre Zielgröße dürfen sich nicht widersprechen."""
    d = klassifikation()
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
              f"= {N_MODELL:,} Beobachtungen ({START}-{ENDE}); "
              f"Klassifikation {N_EINSAETZE:,} Einsätze.")
    return 1 if fehler else 0


if __name__ == "__main__":
    sys.exit(main())
