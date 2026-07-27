"""
Schritt 2 der Aufbereitung: die beiden finalen Datensaetze samt Validierungsrahmen.

Eingang:  data/processed/einsaetze.parquet        (ein Einsatz je Zeile)
Ausgang:  data/processed/regression.parquet       Stadtteil x Monat
          data/processed/klassifikation.parquet   Einzeleinsatz

Beide Datensaetze entstehen in EINER Datei, weil sie zwingend dieselbe
Abgrenzung teilen muessen: Zeitraum und Stadtteilliste werden einmal bestimmt
und an beide weitergereicht.

Der Validierungsrahmen steht ebenfalls hier, weil die Aufteilung als SPALTEN in
die Parquet-Dateien geschrieben wird (`fold`, `ist_holdout`). Sie ist damit
nachzaehlbar und haengt nicht davon ab, dass jedes Modellskript die richtige
Funktion aufruft. "Alle drei Verfahren sehen identische Folds" ist eine Zusage
ueber den DATENSATZ, nicht ueber die Algorithmen (Fairness-Regel, CLAUDE.md).

  TEIL A  Zeitschnitte, Folds, End-Hold-out
  TEIL B  Guetemasse
  TEIL C  Regression      Aggregation, Exposure, Saison, Lags, Panel
  TEIL D  Klassifikation  Zielgroessen aus NFIRS, Zeitmerkmale

Ausfuehren:
  python prep/s2_datensaetze.py          # beide Datensaetze bauen
  python prep/s2_datensaetze.py splits   # nur die Zeitschnitte anzeigen
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from sklearn.metrics import (average_precision_score, f1_score,
                             mean_absolute_error, mean_squared_error,
                             r2_score, roc_auc_score)

from config import (CRIME_ROH, ENDE, ERGEBNISVARIABLEN, EXPOSURE_ROH,
                    FEATURE_SETS, KLASSEN, LAGS, MERKMALE_KATEGORIAL,
                    MERKMALE_ORT, MERKMALE_STRUKTUR, MERKMALE_ZEIT,
                    MIT_BATAILLON, N_FOLDS, N_HOLDOUT, N_TEST_MONATE,
                    N_VAL_MONATE, NFIRS_GRUPPEN, PARKGEBIETE, PFAD_EINSAETZE,
                    PFAD_KLASSIFIKATION, PFAD_REGRESSION, PRAEDIKTOREN,
                    RESTKLASSE, ROOT, SAISON, START,
                    VOLLSTAENDIGKEITS_SCHWELLE, VORLAUF_MONATE, merkmalslisten)

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


# ==========================================================================
# TEIL A  ZEITSCHNITTE, FOLDS, END-HOLD-OUT
# ==========================================================================
# Aufbau der Zeitachse (Decision Log #14):
#
#     |<---------- Entwicklungsdaten ---------->|<--- HOLD-OUT (12 M) --->|
#     |  Fold 1: Train .......... | Test (12 M) |                         |
#     |  Fold 2: Train ......................... | Test (12 M) |          |
#                                                               ^
#                                         wird beim Tuning NIE beruehrt
#
# Blockiertes Forward Chaining ueber GLOBALE Zeitschnitte: alle Stadtteile
# teilen dieselbe Trennlinie. Kein Gap zwischen Train und Test noetig, weil
# saemtliche Lag- und Rolling-Features strikt rueckwaertsgerichtet sind (shift
# vor rolling) - ein Testmonat greift nie auf Werte nach seinem eigenen
# Zeitpunkt zu. Getunt wird auf dem INNEREN Fenster (letzte val_monate des
# Trainings), nie auf Testmonaten und nie auf dem Hold-out.
# ==========================================================================
def zeitachse(daten: pd.DataFrame, spalte: str = "jahr_monat") -> list[int]:
    """Sortierte, eindeutige Monatsschluessel des Datensatzes."""
    return sorted(int(m) for m in daten[spalte].unique())


def split_holdout(monate: list[int],
                  n_holdout: int = N_HOLDOUT) -> tuple[list[int], list[int]]:
    """Trennt die Zeitachse in Entwicklungsdaten und End-Hold-out.

    Das Hold-out umfasst die letzten `n_holdout` Monate und wird waehrend
    Modellauswahl und Tuning NICHT verwendet - nur fuer die abschliessende,
    einmalige Bewertung.
    """
    if len(monate) <= n_holdout:
        raise ValueError(f"Zeitachse zu kurz ({len(monate)} Monate) "
                         f"fuer ein Hold-out von {n_holdout} Monaten.")
    return monate[:-n_holdout], monate[-n_holdout:]


def zeit_folds(monate: list[int],
               n_folds: int = N_FOLDS,
               test_monate: int = N_TEST_MONATE) -> list[tuple[list[int], list[int]]]:
    """Expanding-Window-Folds ueber sortierte Jahr-Monats-Schluessel.

    `monate` sollte das Hold-out bereits ausschliessen (siehe split_holdout).
    """
    benoetigt = n_folds * test_monate + test_monate
    if len(monate) < benoetigt:
        raise ValueError(f"Zeitachse zu kurz: {len(monate)} Monate, "
                         f"mindestens {benoetigt} noetig fuer {n_folds} Folds "
                         f"a {test_monate} Testmonate.")
    folds = []
    for i in range(n_folds):
        ende_test  = len(monate) - (n_folds - 1 - i) * test_monate
        start_test = ende_test - test_monate
        folds.append((monate[:start_test], monate[start_test:ende_test]))
    return folds


def inneres_fenster(train_monate: list[int],
                    val_monate: int = N_VAL_MONATE) -> tuple[list[int], list[int]]:
    """Zerlegt ein Trainingsfenster in Sub-Training und Validierung.

    Fuer die Hyperparameter-Suche: die letzten `val_monate` dienen als
    Validierung, der Rest als Sub-Training. Damit wird nie auf Testmonaten
    getunt.
    """
    if len(train_monate) <= val_monate:
        raise ValueError(f"Trainingsfenster zu kurz ({len(train_monate)} Monate) "
                         f"fuer ein inneres Fenster von {val_monate} Monaten.")
    return train_monate[:-val_monate], train_monate[-val_monate:]


def maske(daten: pd.DataFrame, monate: list[int],
          spalte: str = "jahr_monat") -> pd.Series:
    """Boolesche Maske fuer eine Monatsliste."""
    return daten[spalte].isin(monate)


def ergaenze_aufteilung(daten: pd.DataFrame) -> pd.DataFrame:
    """Schreibt die Spalten `fold` und `ist_holdout` in den Datensatz.

    `fold`         Nummer des Folds, in dessen TESTfenster der Monat liegt.
                   0 = der Monat dient ausschliesslich als Trainingsmaterial.
    `ist_holdout`  1 = Monat gehoert zum unberuehrten End-Hold-out.

    Die Trainingsfenster sind daraus vollstaendig ableitbar (siehe
    `fold_masken`), weil das Training bei Forward Chaining immer aus allen
    Monaten VOR dem Testfenster besteht.
    """
    d = daten.copy()
    monate = zeitachse(d)
    entwicklung, holdout = split_holdout(monate)

    d["fold"] = 0
    for i, (_, test) in enumerate(zeit_folds(entwicklung), start=1):
        d.loc[d["jahr_monat"].isin(test), "fold"] = i
    d["ist_holdout"] = d["jahr_monat"].isin(holdout).astype(int)
    d["fold"] = d["fold"].astype(int)
    return d


def fold_masken(daten: pd.DataFrame, k: int) -> tuple[pd.Series, pd.Series]:
    """Trainings- und Testmaske des Folds k - allein aus den Spalten der Datei.

    Training = alle Monate vor dem Testfenster, ohne das Hold-out.
    """
    test = daten["fold"] == k
    if not test.any():
        raise ValueError(f"Kein Fold {k} im Datensatz (vorhanden: "
                         f"{sorted(daten['fold'].unique())}).")
    start_test = daten.loc[test, "jahr_monat"].min()
    train = (daten["jahr_monat"] < start_test) & (daten["ist_holdout"] == 0)
    return train, test


def inneres_fenster_masken(daten: pd.DataFrame, k: int) -> tuple[pd.Series, pd.Series]:
    """Sub-Training und inneres Validierungsfenster des Folds k (fuer die Suche)."""
    train, _ = fold_masken(daten, k)
    sub, val = inneres_fenster(sorted(daten.loc[train, "jahr_monat"].unique()))
    return daten["jahr_monat"].isin(sub), daten["jahr_monat"].isin(val)


def beschreibe_splits(monate: list[int]) -> str:
    """Menschenlesbare Zusammenfassung der Zeitschnitte (fuer Kap. 5.2/5.4)."""
    entwicklung, holdout = split_holdout(monate)
    zeilen = [f"Zeitachse gesamt: {monate[0]}-{monate[-1]} ({len(monate)} Monate)",
              f"  Entwicklungsdaten: {entwicklung[0]}-{entwicklung[-1]} "
              f"({len(entwicklung)} Monate)",
              f"  End-Hold-out:      {holdout[0]}-{holdout[-1]} "
              f"({len(holdout)} Monate, beim Tuning unberuehrt)"]
    for i, (tr, te) in enumerate(zeit_folds(entwicklung), 1):
        _, val = inneres_fenster(tr)
        zeilen.append(f"  Fold {i}: Train {tr[0]}-{tr[-1]} ({len(tr)} M) "
                      f"[inneres Val {val[0]}-{val[-1]}] -> "
                      f"Test {te[0]}-{te[-1]} ({len(te)} M)")
    return "\n".join(zeilen)


# ==========================================================================
# TEIL B  GUETEMASSE
# ==========================================================================
def bewerte_regression(y_true, y_pred) -> dict:
    """RMSE, MAE, R2 - immer auf der ORIGINALSKALA der Zaehlgroesse.

    Modelle, die auf log(1+y) trainiert werden (Ridge), muessen ihre
    Vorhersagen vorher per expm1 zuruecktransformieren.
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return {
        "RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "MAE":  float(mean_absolute_error(y_true, y_pred)),
        "R2":   float(r2_score(y_true, y_pred)),
    }


def bewerte_klassifikation(y_true, p_hat, schwelle: float = 0.5) -> dict:
    """Binaer: F1 (positive Klasse = Brand), AUROC, Average Precision.

    AUROC ist schwellenunabhaengig, F1 nicht - die Schwelle ist daher auf dem
    inneren Validierungsfenster zu waehlen, nicht blind auf 0,5.
    """
    y_true = np.asarray(y_true).astype(int)
    p_hat  = np.asarray(p_hat, dtype=float)
    return {
        "F1":    float(f1_score(y_true, (p_hat >= schwelle).astype(int),
                                zero_division=0)),
        "AUROC": float(roc_auc_score(y_true, p_hat)),
        "AP":    float(average_precision_score(y_true, p_hat)),
        "schwelle": float(schwelle),
    }


def bewerte_mehrklassig(y_true, p_hat, klassen: list[str]) -> dict:
    """Mehrklassig: Macro-F1 und Macro-AUROC (One-vs-Rest).

    Zuordnung ueber argmax statt Schwellenwert (Decision Log #21). Macro
    gewichtet alle vier Klassen gleich - sonst dominiert Fehlalarm/Good Intent
    mit 48 % das Ergebnis.
    """
    p_hat  = np.asarray(p_hat, dtype=float)
    y_true = np.asarray(y_true)
    y_pred = np.asarray(klassen)[p_hat.argmax(axis=1)]
    # roc_auc_score verlangt sortierte Labels; die Wahrscheinlichkeitsspalten
    # muessen in derselben Reihenfolge stehen wie `labels`.
    ordnung = sorted(range(len(klassen)), key=lambda i: klassen[i])
    return {
        "Macro-F1":    float(f1_score(y_true, y_pred, average="macro",
                                      labels=klassen, zero_division=0)),
        "Macro-AUROC": float(roc_auc_score(y_true, p_hat[:, ordnung],
                                           multi_class="ovr", average="macro",
                                           labels=[klassen[i] for i in ordnung])),
    }


def beste_schwelle(y_true, p_hat, raster: np.ndarray | None = None) -> float:
    """F1-optimale Schwelle auf dem inneren Validierungsfenster (nur binaer)."""
    y_true = np.asarray(y_true).astype(int)
    p_hat  = np.asarray(p_hat, dtype=float)
    raster = np.arange(0.05, 0.96, 0.01) if raster is None else raster
    werte = [f1_score(y_true, (p_hat >= s).astype(int), zero_division=0)
             for s in raster]
    return float(raster[int(np.argmax(werte))])


# ==========================================================================
# Datentypen (von beiden Datensaetzen genutzt)
# ==========================================================================
def _monat_minus(jahr_monat: int, monate: int) -> int:
    """Verschiebt einen jahr_monat-Schluessel um n Monate zurueck."""
    jahr, monat = divmod(jahr_monat, 100)
    gesamt = jahr * 12 + (monat - 1) - monate
    return (gesamt // 12) * 100 + (gesamt % 12) + 1


def _setze_datentypen(d: pd.DataFrame, merkmale: list[str]) -> pd.DataFrame:
    """Vereinheitlicht die Datentypen auf modelltaugliche NumPy-Typen.

    WARUM DAS NOETIG IST: Die ACS-Aggregation liefert `median_haushaltseinkommen`
    und `median_miete` als pandas-eigenen Typ `Int64` (nullable). Solange nur
    scikit-learn im Spiel ist, faellt das nicht auf - der StandardScaler wandelt
    still um. Sobald aber EINE Int64-Spalte im Merkmalssatz steht, liefert
    `X.to_numpy()` ein object-Array statt float64, und XGBoost lehnt den
    DataFrame mit "dtypes for data must be int, float, bool or category" ab.
    Der Fehler traete also erst beim dritten der drei zu vergleichenden
    Verfahren auf.

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
# TEIL C  REGRESSION
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

    # Sicherheits-Dedup (idempotent; die Bereinigung erfolgt in s1_daten.py).
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
    Koeffizient koennte ein U-foermiges Jahresmuster nicht abbilden. sin/cos
    legen die Monate auf ein Zifferblatt.

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
    ueber shift() ein, nie als eigene Zeile. Ohne Vorlauf fiel das erste Jahr je
    Stadtteil weg und die Regression begann 2016-01, waehrend die Klassifikation
    ab 2015-01 lief.
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
# TEIL D  KLASSIFIKATION
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


def zeige_splits() -> None:
    """Selbsttest: Zeitschnitte des fertigen Regressionsdatensatzes."""
    if not PFAD_REGRESSION.exists():
        raise SystemExit("regression.parquet fehlt - erst 'python prep/build.py'.")
    d = pd.read_parquet(PFAD_REGRESSION)
    print(beschreibe_splits(zeitachse(d)))
    print("\n  Aufteilung wie im Datensatz gespeichert:")
    for k in sorted(x for x in d["fold"].unique() if x > 0):
        tr, te = fold_masken(d, k)
        print(f"    Fold {k}: Train {tr.sum():>5,} Zeilen | Test {te.sum():>5,} Zeilen")
    print(f"    Hold-out: {int(d['ist_holdout'].sum()):,} Zeilen")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "splits":
        zeige_splits()
    else:
        run()
        print("\n  Pruefungen: python tests/test_aufbereitung.py")
