"""
Schritt 3: die beiden finalen Datensaetze.

Eingang:  data/processed/einsaetze.parquet        (ein Einsatz je Zeile)
Ausgang:  data/processed/regression.parquet       Stadtteil x Monat
          data/processed/klassifikation.parquet   Einzeleinsatz

Beide entstehen in EINER Datei, weil sie zwingend dieselbe Abgrenzung teilen
muessen: Zeitraum und Stadtteilliste werden einmal bestimmt und an beide
weitergereicht. Frueher lagen sie getrennt und die Klassifikation las den
Regressionsdatensatz von der Platte - eine Reihenfolgeabhaengigkeit, die man
falsch machen konnte.

  TEIL 1  Regression      Aggregation, Exposure, Saison, Lags, Panel
  TEIL 2  Klassifikation  Zielgroessen aus NFIRS, Zeitmerkmale
  TEIL 3  Datentypen      float64 fuer alle Merkmale (Modelltauglichkeit)

Ausfuehren:
  python prep/s03_s03_datensaetze.py
"""
import numpy as np
import pandas as pd

from config import (CRIME_ROH, ENDE, ERGEBNISVARIABLEN, EXPOSURE_ROH,
                    FEATURE_SETS, KLASSEN, LAGS, MERKMALE_KATEGORIAL,
                    MERKMALE_ORT, MERKMALE_STRUKTUR, MERKMALE_ZEIT,
                    MIT_BATAILLON, NFIRS_GRUPPEN, PARKGEBIETE, PFAD_EINSAETZE,
                    PFAD_KLASSIFIKATION, PFAD_REGRESSION, PRAEDIKTOREN,
                    RESTKLASSE, ROOT, SAISON, START,
                    VOLLSTAENDIGKEITS_SCHWELLE, VORLAUF_MONATE, merkmalslisten)
from cv import ergaenze_aufteilung

# --------------------------------------------------------------------------
# Spaltenordnung der beiden Ausgaben
# --------------------------------------------------------------------------
ZIELGROESSE   = "anzahl_einsaetze"
R_SCHLUESSEL  = ["stadtteil", "jahr", "monat", "jahr_monat"]
R_NEBEN       = [EXPOSURE_ROH, CRIME_ROH]   # NegBin-Offset, Deskription Kap. 5.1

K_SCHLUESSEL  = ["einsatz_nummer", "stadtteil", "jahr", "monat", "jahr_monat"]
K_ZIELGROESSEN = ["einsatzart_gruppe", "ist_brand"]

AUFTEILUNG = ["fold", "ist_holdout"]

# Aus der Einsatz-Tabelle je Stadtteil-Monat uebernommene Spalten. Die
# log-Merkmale entstehen erst danach.
_ABGELEITET  = ["log_bevoelkerung", "log_kriminalitaetsindex"]
_UEBERNOMMEN = ([c for c in PRAEDIKTOREN if c not in _ABGELEITET]
                + [EXPOSURE_ROH, CRIME_ROH])


def _monat_minus(jahr_monat: int, monate: int) -> int:
    """Verschiebt einen jahr_monat-Schluessel um n Monate zurueck."""
    jahr, monat = divmod(jahr_monat, 100)
    gesamt = jahr * 12 + (monat - 1) - monate
    return (gesamt // 12) * 100 + (gesamt % 12) + 1


# ==========================================================================
# TEIL 3 (vorgezogen, weil beide Teile es nutzen)  DATENTYPEN
# ==========================================================================
def _setze_datentypen(d: pd.DataFrame, merkmale: list[str]) -> pd.DataFrame:
    """Vereinheitlicht die Datentypen auf modelltaugliche NumPy-Typen.

    WARUM DAS NOETIG IST: Die ACS-Aggregation liefert `median_haushaltseinkommen`
    und `median_miete` als pandas-eigenen Typ `Int64` (nullable). Solange nur
    scikit-learn im Spiel ist, faellt das nicht auf - der StandardScaler wandelt
    still um. Sobald aber EINE Int64-Spalte im Merkmalssatz steht, liefert
    `X.to_numpy()` ein **object**-Array statt float64, und XGBoost lehnt den
    DataFrame mit "dtypes for data must be int, float, bool or category"
    rundweg ab. Der Fehler traete also erst beim dritten der drei zu
    vergleichenden Verfahren auf.

    Regel:
      Merkmale                float64  (einheitliche Designmatrix)
      wochentag               int64    (kategorial, One-Hot im ColumnTransformer)
      Schluessel/Zaehlgroesse int64
      Steuerspalten           int64
      stadtteil, Zielklasse   str
    """
    d = d.copy()
    for c in merkmale:
        if c in MERKMALE_KATEGORIAL:
            d[c] = d[c].astype("int64")
        else:
            d[c] = pd.to_numeric(d[c], errors="coerce").astype("float64")
    for c in ["jahr", "monat", "jahr_monat", ZIELGROESSE, "ist_brand",
              "fold", "ist_holdout", EXPOSURE_ROH]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce").astype("int64")
    if CRIME_ROH in d.columns:
        d[CRIME_ROH] = d[CRIME_ROH].astype("float64")
    for c in ["stadtteil", "einsatz_nummer", "einsatzart_gruppe"]:
        if c in d.columns:
            d[c] = d[c].astype(str)
    return d


# ==========================================================================
# TEIL 1  REGRESSION
# ==========================================================================
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


def aggregiere(von: int, bis: int, mit_parkgebieten: bool = False,
               verbose: bool = False) -> pd.DataFrame:
    """Einsatz-Ebene -> Stadtteil x Monat, vollstaendiges Raster.

    `von`/`bis` sind jahr_monat-Schluessel INKLUSIVE Lag-Vorlauf.
    """
    df = pd.read_parquet(PFAD_EINSAETZE)
    if not mit_parkgebieten:
        df = df[~df["stadtteil"].isin(PARKGEBIETE)]

    # Sicherheits-Dedup (idempotent; die Bereinigung erfolgt in s02_einsaetze.py).
    df = df.drop_duplicates(subset=["einsatz_nummer"], keep="first")

    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]
    if verbose:
        pruefe_randmonate(df)

    # Zuschnitt VOR der Aggregation, damit das Raster genau den gewuenschten
    # Zeitraum aufspannt.
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

    # Nur VORWAERTS fuellen. KEIN bfill: Rueckwaertsfuellen wuerde fehlende Werte
    # (z. B. akademikerquote vor ACS 2014) still mit ZUKUNFTSWERTEN imputieren -
    # Leakage (Decision Log #10). Echte NaN bleiben sichtbar.
    raster[_UEBERNOMMEN] = (raster.groupby("stadtteil")[_UEBERNOMMEN]
                                  .transform(lambda s: s.ffill()))

    raster["jahr_monat"] = raster["jahr"] * 100 + raster["monat"]
    raster = raster[raster["jahr_monat"].between(von, bis)]

    # Exposure (Decision Log #13). log1p statt log nur zur Absicherung gegen
    # Datenartefakte mit Bevoelkerung 0.
    raster["log_bevoelkerung"] = np.log1p(raster[EXPOSURE_ROH].astype(float))
    # Kriminalitaetsindex logarithmieren (Decision Log #17/#19): 0 entspricht dem
    # Stadtdurchschnitt. Nullwerte wuerden -inf erzeugen und werden auf NaN
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


def baue_merkmale(panel: pd.DataFrame) -> pd.DataFrame:
    """Ergaenzt Saison- und Lag-Merkmale.

    SAISON - Der Monat als Zahl 1-12 waere eine schlechte Kodierung: Dezember und
    Januar haetten den Abstand 11, obwohl sie benachbart sind, und ein linearer
    Koeffizient koennte ein U-foermiges Jahresmuster grundsaetzlich nicht
    abbilden. sin/cos legen die Monate auf ein Zifferblatt.

    LAGS - Strikt rueckwaertsgerichtet und je Stadtteil gebildet (`groupby`), nie
    ueber Stadtteilgrenzen hinweg. Beim gleitenden Mittel steht `shift(1)` VOR
    `rolling(3)`: der Wert fuer Monat t verwendet t-1, t-2, t-3, nie t selbst.
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
        print(f"  Balanciertes Panel: {len(raus)} Stadtteil(e) ohne durchgaengige "
              f"Abdeckung ausgeschlossen -> {raus}")
    return aus


def baue_regression(vorlauf: int = VORLAUF_MONATE,
                    verbose: bool = False) -> pd.DataFrame:
    """Der vollstaendige Regressionsdatensatz.

    LAG-VORLAUF (Decision Log #23): Aggregiert wird ab START minus `vorlauf`
    Monaten, damit lag_12 schon fuer den ersten Analysemonat definiert ist.
    Danach wird auf START zugeschnitten - die Vorlaufmonate gehen ausschliesslich
    ueber shift() ein, nie als eigene Zeile. Ohne Vorlauf (vorlauf=0) fiel das
    erste Jahr je Stadtteil weg und die Regression begann 2016-01, waehrend die
    Klassifikation ab 2015-01 lief.
    """
    von = _monat_minus(START, vorlauf)
    d = baue_merkmale(aggregiere(von=von, bis=ENDE, verbose=verbose))

    # Vorlauf abschneiden. Erst danach greift die NaN-Pruefung des balancierten
    # Panels - die Vorlaufmonate haben absichtlich keine Strukturmerkmale (der
    # Kriminalitaetsindex beginnt erst 2015-01) und duerfen die Stadtteilauswahl
    # nicht beeinflussen.
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

    spalten = (R_SCHLUESSEL + [ZIELGROESSE] + FEATURE_SETS["S+L"]
               + AUFTEILUNG + R_NEBEN)
    return _setze_datentypen(d[spalten], FEATURE_SETS["S+L"])


# ==========================================================================
# TEIL 2  KLASSIFIKATION
# ==========================================================================
def baue_klassifikation(regression: pd.DataFrame,
                        mit_ort: bool = MIT_BATAILLON,
                        verbose: bool = False) -> pd.DataFrame:
    """Der vollstaendige Klassifikationsdatensatz.

    Zeitraum und Stadtteilliste werden dem Regressionsdatensatz ENTNOMMEN, nicht
    neu bestimmt. Damit beziehen sich beide Teile der Arbeit zwingend auf
    denselben Datenbestand.
    """
    von, bis = int(regression["jahr_monat"].min()), int(regression["jahr_monat"].max())
    stadtteile = sorted(regression["stadtteil"].unique())

    df = pd.read_parquet(PFAD_EINSAETZE)
    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]
    df = df[df["jahr_monat"].between(von, bis)]
    df = df[df["stadtteil"].isin(stadtteile)].copy()
    df = df.drop_duplicates(subset=["einsatz_nummer"], keep="first")

    # --- Zielgroessen ------------------------------------------------------
    # NFIRS-Codes sind hierarchisch, die fuehrende Ziffer bezeichnet die Serie.
    serie = df["einsatzart"].astype(str).str.extract(r"^(\d)")[0]
    df["einsatzart_gruppe"] = serie.map(NFIRS_GRUPPEN).fillna(RESTKLASSE)
    df["ist_brand"] = (serie == "1").astype(int)

    # --- Block A: Stadtteilstruktur ----------------------------------------
    # Dieselben Transformationen wie im Regressionsdatensatz, hier auf
    # Einsatz-Ebene.
    df["log_bevoelkerung"] = np.log1p(df[EXPOSURE_ROH].astype(float))
    index_roh = df[CRIME_ROH].astype(float)
    df["log_kriminalitaetsindex"] = np.log(index_roh.where(index_roh > 0))

    # --- Block B: Zeitpunkt des Alarms -------------------------------------
    # Zyklisch kodiert, weil der Zusammenhang periodisch und nicht monoton ist:
    # Der Brandanteil schwankt ueber den Tag zwischen 8,5 % und 20,5 %, die
    # lineare Korrelation mit `stunde` betraegt aber nur -0,006.
    df["stunde_sin"] = np.sin(2 * np.pi * df["stunde"] / 24)
    df["stunde_cos"] = np.cos(2 * np.pi * df["stunde"] / 24)
    df["monat_sin"]  = np.sin(2 * np.pi * df["monat"] / 12)
    df["monat_cos"]  = np.cos(2 * np.pi * df["monat"] / 12)

    merkmale = (MERKMALE_STRUKTUR + MERKMALE_ZEIT + MERKMALE_KATEGORIAL
                + (MERKMALE_ORT if mit_ort else []))
    d = df.dropna(subset=MERKMALE_STRUKTUR).reset_index(drop=True)
    d = ergaenze_aufteilung(d)
    # Reihenfolge wie im Regressionsdatensatz; die Einsatznummer macht die
    # Sortierung innerhalb eines Monats eindeutig (Reproduzierbarkeitsvertrag).
    d = (d.sort_values(["jahr_monat", "stadtteil", "einsatz_nummer"])
           .reset_index(drop=True))

    ergebnis = _setze_datentypen(
        d[K_SCHLUESSEL + K_ZIELGROESSEN + merkmale + AUFTEILUNG], merkmale)

    # Harte Zusicherung: keine Ergebnisvariable darf im Datensatz landen. Diese
    # Spalten stehen erst nach dem Einsatz fest oder sind eine Folge der
    # Einsatzart - ihre Verwendung waere Leakage im engeren Sinn (#20).
    verboten = [c for c in ERGEBNISVARIABLEN if c in ergebnis.columns]
    assert not verboten, f"Ergebnisvariablen im Datensatz: {verboten}"

    if verbose:
        n_raus = len(df) - len(ergebnis)
        print(f"  Klassifikationsdaten: {len(ergebnis):,} Einsaetze | "
              f"{ergebnis['stadtteil'].nunique()} Stadtteile | {von}-{bis}")
        if n_raus:
            print(f"  {n_raus:,} Zeilen ohne vollstaendige Strukturmerkmale entfernt")
        v = ergebnis["einsatzart_gruppe"].value_counts()
        print("  Klassen:", " | ".join(
            f"{k} {int(v.get(k, 0)) / len(ergebnis) * 100:.1f} %" for k in KLASSEN))
        print(f"  Ungleichgewicht groesste/kleinste: {v.max() / v.min():.1f}:1 | "
              f"ist_brand {ergebnis['ist_brand'].mean() * 100:.1f} %")
    return ergebnis


# ==========================================================================
# Ablauf
# ==========================================================================
def run(verbose: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    r = baue_regression(verbose=verbose)
    r.to_parquet(PFAD_REGRESSION, index=False)
    if verbose:
        print(f"\n  => {PFAD_REGRESSION.relative_to(ROOT)}  "
              f"({len(r):,} Zeilen | {len(r.columns)} Spalten)")
        print(f"  Zeitraum {r['jahr_monat'].min()}-{r['jahr_monat'].max()} | "
              f"{r['stadtteil'].nunique()} Stadtteile | "
              f"{r.groupby(['jahr', 'monat']).ngroups} Monate | "
              f"Merkmale S={len(FEATURE_SETS['S'])}, S+L={len(FEATURE_SETS['S+L'])}")
        for j in sorted(x for x in r["fold"].unique() if x > 0):
            te = r[r["fold"] == j]
            print(f"    Fold {j}: Test {te['jahr_monat'].min()}-"
                  f"{te['jahr_monat'].max()} ({len(te):,} Zeilen)")
        ho = r[r["ist_holdout"] == 1]
        print(f"    Hold-out: {ho['jahr_monat'].min()}-{ho['jahr_monat'].max()} "
              f"({len(ho):,} Zeilen)")

    print()
    k = baue_klassifikation(r, verbose=verbose)
    k.to_parquet(PFAD_KLASSIFIKATION, index=False)
    if verbose:
        print(f"\n  => {PFAD_KLASSIFIKATION.relative_to(ROOT)}  "
              f"({len(k):,} Zeilen | {len(k.columns)} Spalten)")
        print("  Merkmalssaetze: "
              + ", ".join(f"{a} ({len(b)})" for a, b in merkmalslisten().items()))
    return r, k


if __name__ == "__main__":
    run()
    print("\n  Pruefungen: python tests/test_aufbereitung.py")
