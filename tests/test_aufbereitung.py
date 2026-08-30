"""
Prüfungen der Datenaufbereitung – gesammelt an einer Stelle.

Diese Datei ist bewusst vom Analysecode getrennt: Die Module unter `prep/`
bleiben dadurch lesbar und eignen sich als Code-Beleg im Anhang der Arbeit,
während die Absicherung hier vollständig nachvollziehbar bleibt.

Geprüft werden die fertigen Datensätze in `data/processed/`, nicht der Code, der
sie erzeugt. Damit fällt auch auf, wenn jemand eine Datei von Hand ändert.

Gegenstand:

  1. Analysedatensatz  rechteckig, vollständig, fester Zeitraum
  2. Stadtteil-Split   kein Stadtteil zugleich Trainings- und Testfall,
                       unberührtes Hold-out, Aufteilungsspalten konsistent
  3. Merkmale          Lags gegen die Rohdaten verifiziert, kein Leakage
  4. Struktur          keine Ergebnisvariablen, Anteile konsistent

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

from config import (ANTEILE, ANZAHLEN, ENDE, ERGEBNISVARIABLEN,  # noqa: E402
                    FEATURE_SETS, MERKMALE_STRUKTUR, N_FOLDS,
                    PFAD_KLASSIFIKATION, PFAD_REGRESSION, PRAEDIKTOREN,
                    SAISON, START, VORLAUF_MONATE)
from s2_datensaetze import (RATE, ZIELGROESSE, ZIELKLASSE,  # noqa: E402
                            _monat_minus, aggregiere, ergaenze_aufteilung,
                            fold_masken)

# Erwartungswerte des festgesetzten Analysedatensatzes
# (Decision Log #15, #18, #19, #23)
N_STADTTEILE = 35
N_MONATE     = 132                              # 2015-01 bis 2025-12
N_MODELL     = N_STADTTEILE * N_MONATE          # 4.620
N_STRUKTUR   = 4_619                            # ein Monat ohne Einsatz fällt weg

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


def test_datentypen_modelltauglich():
    """Kein Merkmal darf einen pandas-eigenen (nullable) Typ haben.

    Die ACS-Aggregation liefert `median_haushaltseinkommen` und `median_miete`
    als `Int64`. Solange nur scikit-learn rechnet, fällt das nicht auf – der
    StandardScaler wandelt still um. Sobald aber eine einzige Int64-Spalte im
    Merkmalssatz steht, liefert `X.to_numpy()` ein **object**-Array statt
    float64, und XGBoost lehnt den DataFrame ab ("dtypes for data must be int,
    float, bool or category"). Der Fehler träte also erst beim dritten der drei
    zu vergleichenden Verfahren auf – und säße dann im Preprocessing.
    """
    erlaubt = {"float64", "int64"}
    for name, d, feats in [
        ("menge", regression(), FEATURE_SETS["S"]),
        ("struktur", klassifikation(), MERKMALE_STRUKTUR + SAISON),
    ]:
        schlecht = {c: str(t) for c, t in d[feats].dtypes.items()
                    if str(t) not in erlaubt}
        assert not schlecht, f"{name}: untaugliche Merkmals-dtypes {schlecht}"
        matrix = d[feats].to_numpy()
        assert matrix.dtype != object, \
            f"{name}: Designmatrix wird zu object statt float"


def test_exposure_und_kriminalitaetsindex_vorhanden():
    """Die log-Transformationen sind gebildet und vollständig."""
    d = regression()
    assert d["log_bevoelkerung"].notna().all()
    assert d["log_kriminalitaetsindex"].notna().all()
    # Rohwerte bleiben erhalten (Poisson-Offset, Raten-Sensitivität, Kap. 5.1)
    assert {"gesamtbevoelkerung", "kriminalitaetsindex"} <= set(d.columns)


# ---------------------------------------------------------------------------
# 2. Zeitschnitte
# ---------------------------------------------------------------------------
def test_folds_ordnung_und_holdout():
    """Kein Stadtteil ist zugleich Trainings- und Testfall.

    Das ist der zentrale Punkt des Stadtteil-Splits: Sobald ein Stadtteil in
    beiden Mengen steht, kennt das Modell sein Niveau bereits und die
    Strukturmerkmale müssen nichts mehr erklären – genau die Frage, die die
    Arbeit stellt, bliebe dann unbeantwortet.
    """
    d = regression()
    holdout = set(d.loc[d["ist_holdout"] == 1, "stadtteil"])
    gesehen = set()
    for k in range(1, N_FOLDS + 1):
        train, test = fold_masken(d, k)
        st_train = set(d.loc[train, "stadtteil"])
        st_test = set(d.loc[test, "stadtteil"])
        assert st_train.isdisjoint(st_test), \
            f"Fold {k}: Stadtteil in Training UND Test: {st_train & st_test}"
        assert st_test.isdisjoint(holdout), f"Fold {k} greift ins Hold-out"
        assert st_train.isdisjoint(holdout), f"Fold {k} trainiert auf Hold-out"
        assert gesehen.isdisjoint(st_test), "Stadtteil in zwei Folds im Test"
        gesehen |= st_test
    assert gesehen | holdout == set(d["stadtteil"]), \
        "Nicht jeder Stadtteil ist genau einmal Testfall oder im Hold-out"


def test_jeder_fold_deckt_den_vollen_zeitraum():
    """Ein Teststadtteil wird mit allen seinen Monaten getestet.

    Andernfalls vermischten sich Stadtteil- und Zeitschnitt, und die Fold-
    Streuung wäre nicht mehr interpretierbar.
    """
    d = regression()
    n_monate = d.groupby("jahr_monat").ngroups
    for k in range(1, N_FOLDS + 1):
        _, test = fold_masken(d, k)
        assert d.loc[test].groupby("jahr_monat").ngroups == n_monate


def test_aufteilungsspalten_konsistent():
    """`fold` und `ist_holdout` in der Datei müssen zu prep/s2_datensaetze.py passen.

    Die Spalten sind der Grund, warum die Fairness-Regel nachzählbar ist. Wären
    sie veraltet – etwa weil jemand die Stadtteilliste geändert, aber den
    Datensatz nicht neu gebaut hat –, liefen alle drei Verfahren auf falschen,
    aber untereinander identischen Splits: der Vergleich bliebe fair, das
    Ergebnis wäre trotzdem falsch.
    """
    d, k = regression(), klassifikation()
    selten = (k[k[ZIELKLASSE] == "brand"].groupby("stadtteil").size()
                .reindex(sorted(d["stadtteil"].unique()), fill_value=0))
    neu = ergaenze_aufteilung(d.drop(columns=["fold", "ist_holdout"]),
                              selten=selten)
    assert (neu["fold"].to_numpy() == d["fold"].to_numpy()).all()
    assert (neu["ist_holdout"].to_numpy() == d["ist_holdout"].to_numpy()).all()
    # Die Aufteilung gilt je Stadtteil, nicht je Zeile.
    assert (d.groupby("stadtteil")["fold"].nunique() == 1).all()


def test_folds_decken_die_groessenspanne_ab():
    """Stratifizierung nach Bevölkerung: Kein Fold besteht nur aus Großstadtteilen.

    Sonst wäre die Streuung über die Folds ein Größeneffekt und kein
    Modellunterschied.
    """
    d = regression()
    gross = d.groupby("stadtteil")["gesamtbevoelkerung"].mean()
    median = gross.median()
    for k in range(1, N_FOLDS + 1):
        st = d.loc[d["fold"] == k, "stadtteil"].unique()
        werte = gross[st]
        assert (werte > median).any() and (werte <= median).any(), \
            f"Fold {k} enthält nur eine Größenklasse"


# ---------------------------------------------------------------------------
# 3. Merkmale
# ---------------------------------------------------------------------------
def test_merkmale_vollstaendig():
    """Der Merkmalssatz ist vollständig, die Rate ist gebildet."""
    d = regression()
    for name, spalten in FEATURE_SETS.items():
        assert d[spalten].notna().all().all(), f"NaN im Merkmalssatz {name}"
    assert d[RATE].notna().all(), "NaN in der Rate"
    assert np.isfinite(d[RATE]).all(), "Rate mit Nenner null"
    # Nullmonate sind erlaubt (Anteil 0,02 %), negative Raten nicht.
    assert (d[RATE] >= 0).all()
    assert np.allclose(d[RATE], d[ZIELGROESSE] / d["gesamtbevoelkerung"] * 1000)
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
# 4. Struktur der Einsatzlast
# ---------------------------------------------------------------------------
def test_keine_ergebnisvariablen():
    """Wichtigster Test des Strukturteils.

    Sachschaden, Löschfahrzeuge, Alarmstufe und Antwortzeit stehen erst nach dem
    Einsatz fest. Rutscht eine dieser Spalten in den Merkmalssatz, sieht das
    Ergebnis gut aus und ist wertlos.
    """
    gefunden = [c for c in ERGEBNISVARIABLEN if c in klassifikation().columns]
    assert not gefunden, f"Ergebnisvariable(n) im Datensatz: {gefunden}"


def test_struktur_gleiche_abgrenzung_wie_regression():
    """Beide Teile der Arbeit beruhen auf demselben Datenbestand.

    Gleiche Analyseeinheit, gleiche Stadtteile, gleicher Zeitraum und – der
    entscheidende Punkt – dieselbe Fold-Zuordnung. Nur dann ist der Vergleich
    zwischen Menge und Struktur überhaupt zulässig (Gutachten R1).
    """
    k, d = klassifikation(), regression()
    assert set(k["stadtteil"]) == set(d["stadtteil"])
    assert k["jahr_monat"].min() == d["jahr_monat"].min() == START
    assert k["jahr_monat"].max() == d["jahr_monat"].max() == ENDE
    assert len(k) == N_STRUKTUR, f"{len(k):,} statt {N_STRUKTUR:,} Zeilen"

    zuordnung = d.groupby("stadtteil")["fold"].first()
    assert (k.groupby("stadtteil")["fold"].first() == zuordnung).all(), \
        "Struktur- und Mengendatensatz nutzen verschiedene Folds"


def test_anteile_konsistent():
    """Die vier Anteile summieren sich je Zeile auf 1 und passen zu den Zählungen."""
    k = klassifikation()
    assert np.allclose(k[ANTEILE].sum(axis=1), 1.0), \
        "Anteile summieren sich nicht auf 1 – eine NFIRS-Gruppe fehlt"
    assert (k[ANZAHLEN].sum(axis=1) == k[ZIELGROESSE]).all(), \
        "Zählungen je Gruppe summieren sich nicht auf die Gesamtzahl"
    assert (k[ANTEILE] >= 0).all().all() and (k[ANTEILE] <= 1).all().all()
    assert (k[ZIELGROESSE] > 0).all(), "Zeile ohne Einsatz – Anteil wäre 0/0"


def test_zielklasse_konsistent():
    """Die dominante Einsatzart ist argmax ueber die vier Anteile.

    Eine echte Klasse, kein gesetzter Schwellwert - deshalb entfaellt die
    Begruendungslast einer kuenstlichen Einteilung (Altman & Royston 2006).
    """
    k = klassifikation()
    erwartet = k[ANTEILE].idxmax(axis=1).str.replace("anteil_", "", regex=False)
    assert (k[ZIELKLASSE] == erwartet).all(), \
        "dominante_einsatzart passt nicht zu den Anteilen"
    assert k[ZIELKLASSE].nunique() > 1, "Zielklasse ist konstant"


def test_seltene_klasse_in_jedem_fold():
    """Brand muss in jedem Test-Fold vorkommen.

    Von 70 brand-dominierten Monaten liegen 35 allein in Bayview Hunters Point.
    Ohne Stratifizierung nach der seltenen Klasse hatte in drei von vier
    Aufteilungen ein Fold null Brand-Testfaelle - Macro-F1 mittelt dann ueber
    eine Klasse, die gar nicht vorkommt, und springt zwischen den Folds.
    """
    k = klassifikation()
    for f in range(1, N_FOLDS + 1):
        _, test = fold_masken(k, f)
        n = int((k.loc[test, ZIELKLASSE] == "brand").sum())
        assert n >= 2, f"Fold {f} hat nur {n} brand-dominierte Monate im Test"


def test_struktur_hat_signal():
    """Die Anteile variieren zwischen Stadtteilen – sonst gäbe es nichts zu erklären.

    Der Brandanteil schwankt zwischen den Stadtteilen um mehr als den Faktor 2;
    genau diese Variation soll durch Strukturmerkmale erklärt werden.
    """
    k = klassifikation()
    je_stadtteil = k.groupby("stadtteil")["anteil_brand"].mean()
    assert je_stadtteil.max() / je_stadtteil.min() > 2.0, \
        "Brandanteil variiert kaum zwischen Stadtteilen"
    assert k[MERKMALE_STRUKTUR + SAISON].notna().all().all()


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
              f"Struktur {N_STRUKTUR:,} Zeilen; {N_FOLDS} Stadtteil-Folds.")
    return 1 if fehler else 0


if __name__ == "__main__":
    sys.exit(main())
