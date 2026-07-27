"""
Schritt 3a: Regressionsdatensatz - Zielgroesse Einsatzhaeufigkeit.

Eingang:  data/processed/einsaetze.parquet   (ein Einsatz je Zeile)
Ausgang:  data/processed/regression.parquet  (ein Stadtteil-Monat je Zeile)

Das ist der Datensatz, den Ridge, Random Forest und XGBoost fuer die Regression
lesen - vollstaendig, fertig aggregiert, mit allen Merkmalen und der
CV-Aufteilung. Kein Modellskript rechnet daran noch etwas.

Vier Arbeitsschritte:

  1. AGGREGATION   Einsaetze zaehlen je Stadtteil x Monat, vollstaendiges Raster
                   (Monate ohne Einsatz = echte Null), Exposure und
                   Kriminalitaetsindex logarithmieren
  2. MERKMALE      Saison als Sinus/Kosinus, Lags der Zielgroesse
  3. ABGRENZUNG    Vorlaufmonate abschneiden, balanciertes Panel herstellen
  4. AUFTEILUNG    fold und ist_holdout als Spalten

Warum liegt die Aggregation hier und nicht in join.py? Weil sie eine
Entscheidung ueber die ANALYSEEINHEIT ist, keine Datenzusammenfuehrung. Der
Klassifikationsteil nutzt dieselben Rohdaten auf einer anderen Ebene.

Ausfuehren:
  python prep/regression_datensatz.py
"""
import numpy as np
import pandas as pd

from config import (CRIME_ROH, ENDE, EXPOSURE_ROH, FEATURE_SETS, LAGS,
                    PARKGEBIETE, PFAD_EINSAETZE, PFAD_REGRESSION, PRAEDIKTOREN,
                    ROOT, SAISON, START, VOLLSTAENDIGKEITS_SCHWELLE,
                    VORLAUF_MONATE)
from cv import ergaenze_aufteilung

# Aus der Einsatz-Tabelle je Stadtteil-Monat uebernommene Spalten. Die
# log-Merkmale entstehen erst danach.
_ABGELEITET  = ["log_bevoelkerung", "log_kriminalitaetsindex"]
_UEBERNOMMEN = ([c for c in PRAEDIKTOREN if c not in _ABGELEITET]
                + [EXPOSURE_ROH, CRIME_ROH])

# Endgueltige Spaltenreihenfolge des Datensatzes.
SCHLUESSEL = ["stadtteil", "jahr", "monat", "jahr_monat"]
ZIELGROESSE = "anzahl_einsaetze"
NEBENRECHNUNG = [EXPOSURE_ROH, CRIME_ROH]
AUFTEILUNG = ["fold", "ist_holdout"]


def _monat_minus(jahr_monat: int, monate: int) -> int:
    """Verschiebt einen jahr_monat-Schluessel um n Monate zurueck."""
    jahr, monat = divmod(jahr_monat, 100)
    gesamt = jahr * 12 + (monat - 1) - monate
    return (gesamt // 12) * 100 + (gesamt % 12) + 1


def pruefe_randmonate(df: pd.DataFrame) -> None:
    """Warnt, wenn Monate im Rohdatenbestand unvollstaendig wirken.

    Reine Diagnose - massgeblich ist die Konstante ENDE in config.py. Diese
    Pruefung verhindert, dass nach einem Neu-Download erneut ein angebrochener
    Randmonat unbemerkt ins Panel laeuft (Decision Log #12): Frueher blieb
    2026-01 mit 258 statt ~3.300 Einsaetzen als scheinbar vollstaendiger Monat
    im Testfenster des letzten Folds stehen und drueckte die naive Baseline dort
    von R2 0,955 auf 0,740.
    """
    je_monat = df.groupby("jahr_monat").size()
    median = je_monat.median()
    verdaechtig = je_monat[je_monat < VOLLSTAENDIGKEITS_SCHWELLE * median]
    verdaechtig = verdaechtig[verdaechtig.index <= ENDE]
    if len(verdaechtig):
        print(f"  WARNUNG: {len(verdaechtig)} Monat(e) <= ENDE={ENDE} wirken "
              f"unvollstaendig (< {VOLLSTAENDIGKEITS_SCHWELLE:.0%} des "
              f"Median-Monats von {median:,.0f} Einsaetzen):")
        for jm, n in verdaechtig.items():
            print(f"    {jm}: {n:,} Einsaetze -> ENDE in config.py pruefen!")


# ==========================================================================
# 1  Aggregation auf Stadtteil x Monat
# ==========================================================================
def aggregiere(von: int, bis: int, mit_parkgebieten: bool = False,
               verbose: bool = False) -> pd.DataFrame:
    """Einsatz-Ebene -> Stadtteil x Monat, vollstaendiges Raster.

    Parameter `von`/`bis` sind jahr_monat-Schluessel. Der Aufrufer uebergibt
    hier den Zeitraum INKLUSIVE Lag-Vorlauf.
    """
    df = pd.read_parquet(PFAD_EINSAETZE)
    if not mit_parkgebieten:
        df = df[~df["stadtteil"].isin(PARKGEBIETE)]

    # Sicherheits-Dedup (idempotent; die Bereinigung erfolgt in join.py).
    df = df.drop_duplicates(subset=["einsatz_nummer"], keep="first")

    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]
    if verbose:
        pruefe_randmonate(df)

    # Zuschnitt VOR der Aggregation, damit das Raster unten genau den
    # gewuenschten Zeitraum aufspannt.
    df = df[df["jahr_monat"].between(von, bis)]
    if df.empty:
        raise ValueError(f"Keine Einsaetze im Zeitraum {von}-{bis}.")

    agg = (df.groupby(["stadtteil", "jahr", "monat"])
             .agg(anzahl_einsaetze=("einsatz_nummer", "count"),
                  **{c: (c, "first") for c in _UEBERNOMMEN})
             .reset_index())

    # Vollstaendiges Raster: Monate ohne Einsatz sind echte Nullen.
    idx = pd.MultiIndex.from_product(
        [agg["stadtteil"].unique(),
         range(int(agg["jahr"].min()), int(agg["jahr"].max()) + 1),
         range(1, 13)],
        names=["stadtteil", "jahr", "monat"])
    raster = agg.set_index(["stadtteil", "jahr", "monat"]).reindex(idx).reset_index()
    raster[ZIELGROESSE] = raster[ZIELGROESSE].fillna(0).astype(int)

    # Nur VORWAERTS fuellen. KEIN bfill: Rueckwaertsfuellen wuerde fehlende
    # Werte (z. B. akademikerquote vor ACS 2014) still mit ZUKUNFTSWERTEN
    # imputieren - Leakage (Decision Log #10). Echte NaN bleiben sichtbar.
    raster[_UEBERNOMMEN] = (raster.groupby("stadtteil")[_UEBERNOMMEN]
                                  .transform(lambda s: s.ffill()))

    raster["jahr_monat"] = raster["jahr"] * 100 + raster["monat"]
    raster = raster[raster["jahr_monat"].between(von, bis)]

    # Exposure (Decision Log #13). log1p statt log nur zur Absicherung gegen
    # Datenartefakte mit Bevoelkerung 0.
    raster["log_bevoelkerung"] = np.log1p(raster[EXPOSURE_ROH].astype(float))
    # Kriminalitaetsindex logarithmieren (Decision Log #17/#19): 0 entspricht
    # dem Stadtdurchschnitt. Nullwerte wuerden -inf erzeugen und werden auf NaN
    # gesetzt, damit sie sichtbar bleiben statt still zum Extremwert zu werden.
    index_roh = raster[CRIME_ROH].astype(float)
    raster["log_kriminalitaetsindex"] = np.log(index_roh.where(index_roh > 0))

    ergebnis = (raster.sort_values(["jahr", "monat", "stadtteil"])
                      .reset_index(drop=True))
    if verbose:
        print(f"  Panel inkl. Vorlauf: {len(ergebnis):,} Zeilen | "
              f"{ergebnis['stadtteil'].nunique()} Stadtteile | "
              f"{ergebnis['jahr_monat'].min()}-{ergebnis['jahr_monat'].max()}")
    return ergebnis


# ==========================================================================
# 2  Merkmale: Saison und Lags
# ==========================================================================
def baue_merkmale(panel: pd.DataFrame) -> pd.DataFrame:
    """Ergaenzt Saison- und Lag-Merkmale.

    SAISON - Der Monat als Zahl 1-12 waere eine schlechte Kodierung: Dezember
    und Januar haetten den Abstand 11, obwohl sie benachbart sind, und ein
    linearer Koeffizient koennte ein U-foermiges Jahresmuster grundsaetzlich
    nicht abbilden. sin/cos legen die Monate auf ein Zifferblatt.

    LAGS - Strikt rueckwaertsgerichtet und je Stadtteil gebildet (`groupby`),
    nie ueber Stadtteilgrenzen hinweg. Beim gleitenden Mittel steht `shift(1)`
    VOR `rolling(3)`: der Wert fuer Monat t verwendet t-1, t-2, t-3, aber
    niemals t selbst.
    """
    d = panel.copy()
    d["monat_sin"] = np.sin(2 * np.pi * d["monat"] / 12)
    d["monat_cos"] = np.cos(2 * np.pi * d["monat"] / 12)

    d = d.sort_values(["stadtteil", "jahr_monat"]).reset_index(drop=True)
    g = d.groupby("stadtteil")[ZIELGROESSE]
    d["lag_1"]          = g.shift(1)
    d["lag_12"]         = g.shift(12)
    d["rolling_mean_3"] = g.transform(lambda s: s.shift(1).rolling(3).mean())
    return d


# ==========================================================================
# 3  Abgrenzung
# ==========================================================================
def balanciertes_panel(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Reduziert das Panel auf ein rechteckiges Stadtteil x Monat-Gitter.

    Nicht alle Stadtteile sind in allen ACS-Jahrgaengen enthalten:
      Treasure Island, Lakeshore  in KEINEM Jahrgang
      Mission Bay                 erst ab ACS 2021

    Zeilenweises `dropna` erzeugte ein UNBALANCIERTES Panel - Mission Bay taucht
    mitten in der Zeitreihe auf, die Folds enthalten unterschiedlich viele
    Stadtteile, und die Einsatzsumme im Testfenster springt allein durch den
    Zutritt eines Stadtteils (Decision Log #15).
    """
    unvollstaendig = (df.groupby("stadtteil")[PRAEDIKTOREN]
                        .apply(lambda g: g.isna().any().any()))
    raus = sorted(unvollstaendig[unvollstaendig].index)
    aus = df[~df["stadtteil"].isin(raus)].reset_index(drop=True)
    if verbose and raus:
        print(f"  Balanciertes Panel: {len(raus)} Stadtteil(e) ohne "
              f"durchgaengige Abdeckung ausgeschlossen -> {raus}")
    return aus


# ==========================================================================
# Ablauf
# ==========================================================================
def baue_datensatz(vorlauf: int = VORLAUF_MONATE,
                   verbose: bool = False) -> pd.DataFrame:
    """Erzeugt den vollstaendigen Regressionsdatensatz.

    LAG-VORLAUF (Decision Log #23): Aggregiert wird ab START minus `vorlauf`
    Monaten, damit lag_12 schon fuer den ersten Analysemonat definiert ist.
    Danach wird auf START zugeschnitten - die Vorlaufmonate gehen ausschliesslich
    ueber shift() ein, nie als eigene Zeile.

    Ohne Vorlauf (vorlauf=0) fiel das erste Jahr je Stadtteil weg und die
    Regression begann 2016-01, waehrend die Klassifikation ab 2015-01 lief.
    """
    von = _monat_minus(START, vorlauf)
    panel = aggregiere(von=von, bis=ENDE, verbose=verbose)
    d = baue_merkmale(panel)

    # Vorlauf abschneiden. Erst danach greift die NaN-Pruefung des balancierten
    # Panels - die Vorlaufmonate haben absichtlich keine Strukturmerkmale
    # (der Kriminalitaetsindex beginnt erst 2015-01) und duerfen die
    # Stadtteilauswahl nicht beeinflussen.
    vor_schnitt = len(d)
    d = d[d["jahr_monat"] >= START].reset_index(drop=True)
    if verbose and vorlauf:
        print(f"  Lag-Vorlauf: {vor_schnitt - len(d):,} Vorlaufzeilen "
              f"({von}-{_monat_minus(START, 1)}) nach der Lag-Bildung entfernt")

    d = balanciertes_panel(d, verbose=verbose)

    # Sicherheitsnetz: ohne ausreichenden Vorlauf bleiben Anlaufmonate ohne
    # lag_12 uebrig. Sie muessen fuer ALLE Modelle und beide Merkmalssaetze
    # gleichermassen entfallen - auch fuer Set S, das die Lags gar nicht nutzt.
    # Sonst liefen die Verfahren auf unterschiedlichen Zeilen (Fairness-Regel).
    vor_dropna = len(d)
    d = d.dropna(subset=LAGS).reset_index(drop=True)
    if verbose and vor_dropna - len(d):
        print(f"  {vor_dropna - len(d):,} Anlaufmonate ohne lag_12 entfernt")

    # Feste, dokumentierte Zeilenreihenfolge (Zeit, dann Stadtteil).
    # REPRODUZIERBARKEITSVERTRAG: Random Forest und XGBoost ziehen ihre
    # Bootstrap- bzw. Subsample-Stichproben ueber Zeilenpositionen. Eine andere
    # Sortierung liefert trotz identischem random_state leicht andere Baeume -
    # empirisch 17,2587 statt 17,2974 RMSE in Fold 1. Ridge ist dagegen
    # reihenfolgeinvariant. Diese Sortierung darf nicht veraendert werden.
    d = d.sort_values(["jahr_monat", "stadtteil"]).reset_index(drop=True)

    d = ergaenze_aufteilung(d)

    spalten = (SCHLUESSEL + [ZIELGROESSE] + PRAEDIKTOREN + SAISON + LAGS
               + AUFTEILUNG + NEBENRECHNUNG)
    return d[spalten]


def run(verbose: bool = True) -> pd.DataFrame:
    d = baue_datensatz(verbose=verbose)
    d.to_parquet(PFAD_REGRESSION, index=False)
    if verbose:
        print(f"\n  => {PFAD_REGRESSION.relative_to(ROOT)}  "
              f"({len(d):,} Zeilen | {len(d.columns)} Spalten)")
        print(f"  Zeitraum {d['jahr_monat'].min()}-{d['jahr_monat'].max()} | "
              f"{d['stadtteil'].nunique()} Stadtteile | "
              f"{d.groupby(['jahr', 'monat']).ngroups} Monate")
        print(f"  Merkmalssaetze: S = {len(FEATURE_SETS['S'])}, "
              f"S+L = {len(FEATURE_SETS['S+L'])}")
        for k in sorted(x for x in d["fold"].unique() if x > 0):
            te = d[d["fold"] == k]
            print(f"    Fold {k}: Test {te['jahr_monat'].min()}-"
                  f"{te['jahr_monat'].max()} ({len(te):,} Zeilen)")
        ho = d[d["ist_holdout"] == 1]
        print(f"    Hold-out: {ho['jahr_monat'].min()}-{ho['jahr_monat'].max()} "
              f"({len(ho):,} Zeilen)")
    return d


def ridge_sicht(d: pd.DataFrame, spalten: list[str]) -> pd.DataFrame:
    """Modellspezifische Aufbereitung der Lags fuer Ridge.

    Ridge wird auf log(1+y) geschaetzt. Damit die Beziehung zwischen Lags und
    log-Zielgroesse linear ist, muessen auch die Lags logarithmiert werden
    (log-AR-Spezifikation). Rohe Lags in einem log-Modell sind fehlspezifiziert -
    empirisch ergab das R2 < 0.

    Das ist eine modellinterne Transformation wie die Standardisierung und KEINE
    Verletzung der Fairness-Regel: identische Zeilen, identische Information,
    nur eine andere Darstellung (Decision Log #9).
    """
    x = d[spalten].copy()
    for c in LAGS:
        if c in x.columns:
            x[c] = np.log1p(x[c])
    return x


if __name__ == "__main__":
    run()
    print("\n  Pruefungen: python tests/test_aufbereitung.py")
