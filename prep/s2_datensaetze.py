"""
Schritt 2: die beiden finalen Datensaetze samt Validierungsrahmen.

Eingang:  data/processed/einsaetze.parquet        (ein Einsatz je Zeile)
Ausgang:  data/processed/regression.parquet       Stadtteil x Monat, Menge
          data/processed/klassifikation.parquet   Stadtteil x Monat, Struktur

BEIDE liegen auf derselben Analyseeinheit - Stadtteil x Monat. Die eine misst
die MENGE der Einsatzlast (Anzahl und Rate), die andere ihre ZUSAMMENSETZUNG
(Anteile der vier NFIRS-Gruppen). Damit laufen beide Teile der Arbeit durch
denselben Rahmen: gleiche Zeilen, gleiche Merkmale, gleiche Folds
(Gutachten R1, Decision Log #29).

Der Validierungsrahmen steht ebenfalls hier, weil die Aufteilung als SPALTEN in
die Parquet-Dateien geschrieben wird (`fold`, `ist_holdout`). Sie ist damit
nachzaehlbar und haengt nicht davon ab, dass jedes Modellskript die richtige
Funktion aufruft. "Alle drei Verfahren sehen identische Folds" ist eine Zusage
ueber den DATENSATZ, nicht ueber die Algorithmen (Fairness-Regel, CLAUDE.md).

  TEIL A  Stadtteil-Split: Folds und Hold-out
  TEIL B  Menge        Aggregation, Exposure, Rate, Saison, Lags
  TEIL C  Struktur     Anteile der vier NFIRS-Gruppen

Ausfuehren:
  python prep/s2_datensaetze.py          # beide Datensaetze bauen
  python prep/s2_datensaetze.py splits   # nur die Aufteilung anzeigen
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from config import (ANTEILE, ANZAHLEN, CRIME_ROH, ENDE, ERGEBNISVARIABLEN,
                    EXPOSURE_ROH, FEATURE_SETS, KLASSEN, LAGS,
                    MERKMALE_STRUKTUR, N_FOLDS, NFIRS_GRUPPEN, PARKGEBIETE,
                    PFAD_EINSAETZE, PFAD_KLASSIFIKATION, PFAD_REGRESSION,
                    PRAEDIKTOREN, RESTKLASSE, ROOT, SAISON, START,
                    VOLLSTAENDIGKEITS_SCHWELLE, VORLAUF_MONATE)

ZIELGROESSE  = "anzahl_einsaetze"
RATE         = "einsaetze_je_1000_ew"   # zweite Zielgroesse der Menge
ZIELKLASSE   = "dominante_einsatzart"   # Zielgroesse der Klassifikation
SCHLUESSEL   = ["stadtteil", "jahr", "monat", "jahr_monat"]
R_NEBEN      = [EXPOSURE_ROH, CRIME_ROH]   # NegBin-Offset, Deskription 5.1
AUFTEILUNG   = ["fold", "ist_holdout"]

# NFIRS-Gruppe -> Spaltensuffix. Die Reihenfolge folgt KLASSEN aus config.py.
SUFFIX = dict(zip(KLASSEN, ["brand", "rettung_ems", "technische_hilfe",
                            "fehlalarm"]))

# Je Stadtteil-Monat aus der Einsatz-Tabelle uebernommen; die log-Merkmale
# entstehen erst danach.
_ABGELEITET  = ["log_bevoelkerung", "log_kriminalitaetsindex"]
_UEBERNOMMEN = ([c for c in PRAEDIKTOREN if c not in _ABGELEITET]
                + [EXPOSURE_ROH, CRIME_ROH])


# ==========================================================================
# TEIL A  STADTTEIL-SPLIT
# ==========================================================================
# Die Forschungsfrage lautet: Laesst sich aus Strukturmerkmalen vorhersagen,
# wie viele und welche Einsaetze ein Stadtteil hat? Geprueft wird das, indem
# ganze Stadtteile zurueckgehalten werden:
#
#     Stadtteile 1-6    Fold 1        \
#     Stadtteile 7-12   Fold 2         |  30 Entwicklungs-Stadtteile,
#     ...                              |  jeder genau einmal Testfall
#     Stadtteile 25-30  Fold 5        /
#     Stadtteile 31-35  Hold-out         beim Tuning nie beruehrt
#
# Ein Zeitschnitt wuerde die Frage nicht pruefen: Dort steht jeder Stadtteil in
# Training UND Test, das Modell kennt sein Niveau bereits, und die
# Strukturmerkmale muessen nichts erklaeren (Decision Log #29).
#
# Die Aufteilung steht als Spalten `fold` und `ist_holdout` in beiden
# Parquet-Dateien. "Alle drei Verfahren sehen identische Folds" ist damit eine
# Zusage ueber den DATENSATZ, nicht ueber die Algorithmen (Fairness-Regel).
# ==========================================================================
def zeitachse(daten: pd.DataFrame, spalte: str = "jahr_monat") -> list[int]:
    """Sortierte, eindeutige Monatsschluessel des Datensatzes."""
    return sorted(int(m) for m in daten[spalte].unique())


def ergaenze_aufteilung(daten: pd.DataFrame, versatz: int = 0,
                        selten: pd.Series | None = None) -> pd.DataFrame:
    """Schreibt `fold` (0..N_FOLDS) und `ist_holdout` in den Datensatz.

    Die Stadtteile werden reihum auf N_FOLDS + 1 Gruppen verteilt; Gruppe 0 ist
    das Hold-out. Wer wohin kommt, haengt allein von der Sortierreihenfolge ab -
    und die stratifiziert nach zwei Kriterien:

      1. `selten`  Anzahl der Monate, in denen die seltenste Klasse dominiert.
                   Brand dominiert nur 70 von 4.619 Stadtteil-Monaten, davon 35
                   allein in Bayview Hunters Point. Ohne diese Stratifizierung
                   hatte in drei von vier Aufteilungen ein Fold KEINEN einzigen
                   Brand-Testfall - Macro-F1 mittelt dann ueber eine Klasse, die
                   im Test gar nicht vorkommt, und springt zwischen den Folds.
      2. Bevoelkerung bei Gleichstand. 25 der 35 Stadtteile haben ueberhaupt
                   keine Brand-Monate, fuer sie bleibt die Groessenstratifizierung
                   damit erhalten: Kein Fold besteht nur aus Grossstadtteilen,
                   sonst waere die Fold-Streuung ein Groesseneffekt statt eines
                   Modellunterschieds.

    Beides sind Vorgaben ueber die Gruppenbildung, kein Leakage: Das Modell
    bekommt keine zusaetzliche Information, es wird nur festgelegt, welche
    Stadtteile gemeinsam getestet werden - wie bei `StratifiedGroupKFold`.

    `versatz` verschiebt den Startpunkt der Austeilung, fuer WIEDERHOLTE Splits.
    Bei nur 30 Entwicklungsstadtteilen schwankt ein einzelner Fold stark
    (Extrapolationsanteil 0 bis 53 %); ueber Wiederholungen gemittelt ist die
    Schaetzung stabil.
    """
    bev = daten.groupby("stadtteil")[EXPOSURE_ROH].mean()
    if selten is None:
        selten = pd.Series(0, index=bev.index)
    ordnung = (pd.DataFrame({"selten": selten.reindex(bev.index).fillna(0),
                             "bev": bev})
                 .sort_values(["selten", "bev"], ascending=False).index)
    gruppe = {st: (i + versatz) % (N_FOLDS + 1) for i, st in enumerate(ordnung)}

    d = daten.copy()
    d["fold"] = d["stadtteil"].map(gruppe).astype(int)
    d["ist_holdout"] = (d["fold"] == 0).astype(int)
    return d


def fold_masken(daten: pd.DataFrame, k: int) -> tuple[pd.Series, pd.Series]:
    """Trainings- und Testmaske des Folds k - allein aus den Spalten der Datei.

    Test  = die Stadtteile dieses Folds, mit allen ihren Monaten.
    Train = alle uebrigen Entwicklungs-Stadtteile, ohne das Hold-out.
    Kein Stadtteil ist je zugleich Trainings- und Testfall.
    """
    test = daten["fold"] == k
    assert test.any(), (f"Kein Fold {k} im Datensatz "
                        f"(vorhanden: {sorted(daten['fold'].unique())}).")
    train = (daten["fold"] != k) & (daten["ist_holdout"] == 0)
    return train, test


def beschreibe_splits(daten: pd.DataFrame) -> str:
    """Menschenlesbare Zusammenfassung der Aufteilung (fuer Kap. 5.2/5.4)."""
    monate = zeitachse(daten)
    zeilen = [f"Zeitraum (in jedem Fold vollstaendig): {monate[0]}-{monate[-1]} "
              f"({len(monate)} Monate)",
              f"Stadtteile gesamt: {daten['stadtteil'].nunique()}"]
    for k in range(1, N_FOLDS + 1):
        st = sorted(daten.loc[daten["fold"] == k, "stadtteil"].unique())
        zeilen.append(f"  Fold {k}: Test auf {len(st)} Stadtteilen -> "
                      + ", ".join(st))
    ho = sorted(daten.loc[daten["ist_holdout"] == 1, "stadtteil"].unique())
    zeilen.append(f"  Hold-out: {len(ho)} Stadtteile (unberuehrt) -> "
                  + ", ".join(ho))
    return "\n".join(zeilen)


# ==========================================================================
# Von beiden Datensaetzen genutzt
# ==========================================================================
def _monat_minus(jahr_monat: int, monate: int) -> int:
    """Verschiebt einen jahr_monat-Schluessel um n Monate zurueck."""
    jahr, monat = divmod(jahr_monat, 100)
    gesamt = jahr * 12 + (monat - 1) - monate
    return (gesamt // 12) * 100 + (gesamt % 12) + 1


def _setze_datentypen(d: pd.DataFrame, merkmale: list[str]) -> pd.DataFrame:
    """Vereinheitlicht die Datentypen auf modelltaugliche NumPy-Typen.

    WARUM: Die ACS-Aggregation liefert `median_haushaltseinkommen` und
    `median_miete` als pandas-eigenen Typ `Int64` (nullable). Solange nur
    scikit-learn im Spiel ist, faellt das nicht auf. Sobald aber EINE
    Int64-Spalte im Merkmalssatz steht, liefert `X.to_numpy()` ein object-Array
    statt float64, und XGBoost lehnt den DataFrame ab - der Fehler traete erst
    beim dritten der drei zu vergleichenden Verfahren auf (Decision Log #24).

    Merkmale float64 · Schluessel, Zaehlgroessen und Steuerspalten int64 ·
    stadtteil str. Anteile bleiben float64.
    """
    d = d.copy()
    for c in merkmale:
        d[c] = pd.to_numeric(d[c], errors="coerce").astype("float64")
    for c in ["jahr", "monat", "jahr_monat", ZIELGROESSE,
              "fold", "ist_holdout", EXPOSURE_ROH] + ANZAHLEN:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce").astype("int64")
    if CRIME_ROH in d.columns:
        d[CRIME_ROH] = d[CRIME_ROH].astype("float64")
    for c in ("stadtteil", ZIELKLASSE):
        if c in d.columns:
            d[c] = d[c].astype(str)
    return d


# ==========================================================================
# TEIL B  REGRESSION
# ==========================================================================
def aggregiere(von: int, bis: int, mit_parkgebieten: bool = False,
               verbose: bool = False) -> pd.DataFrame:
    """Einsatz-Ebene -> Stadtteil x Monat, vollstaendiges Raster.

    `von`/`bis` sind jahr_monat-Schluessel INKLUSIVE Lag-Vorlauf.
    """
    df = pd.read_parquet(PFAD_EINSAETZE)
    if not mit_parkgebieten:
        df = df[~df["stadtteil"].isin(PARKGEBIETE)]
    # Sicherheits-Dedup (idempotent; bereinigt wird in s1_daten.py).
    df = df.drop_duplicates(subset=["einsatz_nummer"], keep="first")
    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]

    if verbose:
        # Diagnose gegen angebrochene Randmonate (Decision Log #12): 2026-01
        # blieb frueher mit 258 statt ~3.300 Einsaetzen als scheinbar
        # vollstaendiger Monat im letzten Testfenster stehen und drueckte die
        # naive Baseline dort von R2 0,955 auf 0,740. Massgeblich bleibt ENDE.
        je_monat = df.groupby("jahr_monat").size()
        median = je_monat.median()
        duenn = je_monat[(je_monat < VOLLSTAENDIGKEITS_SCHWELLE * median)
                         & (je_monat.index <= ENDE)]
        for jm, n in duenn.items():
            print(f"  WARNUNG: {jm} hat nur {n:,} Einsaetze "
                  f"(Median {median:,.0f}) -> ENDE in config.py pruefen!")

    # Zuschnitt VOR der Aggregation, damit das Raster genau den gewuenschten
    # Zeitraum aufspannt.
    df = df[df["jahr_monat"].between(von, bis)]
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

    # Exposure (Decision Log #13); log1p sichert gegen Bevoelkerung 0 ab.
    raster["log_bevoelkerung"] = np.log1p(raster[EXPOSURE_ROH].astype(float))
    # Kriminalitaetsindex logarithmieren (#17/#19): 0 = Stadtdurchschnitt.
    # Nullwerte wuerden -inf erzeugen und werden zu NaN, damit sie sichtbar
    # bleiben statt still zum Extremwert zu werden.
    index_roh = raster[CRIME_ROH].astype(float)
    raster["log_kriminalitaetsindex"] = np.log(index_roh.where(index_roh > 0))

    ergebnis = (raster.sort_values(["jahr", "monat", "stadtteil"])
                      .reset_index(drop=True))
    if verbose:
        print(f"  Panel inkl. Vorlauf: {len(ergebnis):,} Zeilen | "
              f"{ergebnis['stadtteil'].nunique()} Stadtteile | "
              f"{ergebnis['jahr_monat'].min()}-{ergebnis['jahr_monat'].max()}")
    return ergebnis


def baue_regression(vorlauf: int = VORLAUF_MONATE,
                    verbose: bool = False) -> pd.DataFrame:
    """Der vollstaendige Regressionsdatensatz.

    LAG-VORLAUF (Decision Log #23): Aggregiert wird ab START minus `vorlauf`
    Monaten, damit lag_12 schon fuer den ersten Analysemonat definiert ist.
    Danach Zuschnitt auf START - die Vorlaufmonate gehen ausschliesslich ueber
    shift() ein, nie als eigene Zeile.
    """
    von = _monat_minus(START, vorlauf)
    d = aggregiere(von=von, bis=ENDE, verbose=verbose)

    # SAISON - Der Monat als Zahl 1-12 waere eine schlechte Kodierung: Dezember
    # und Januar haetten den Abstand 11, obwohl sie benachbart sind, und ein
    # linearer Koeffizient koennte ein U-foermiges Jahresmuster nicht abbilden.
    # sin/cos legen die Monate auf ein Zifferblatt.
    d["monat_sin"] = np.sin(2 * np.pi * d["monat"] / 12)
    d["monat_cos"] = np.cos(2 * np.pi * d["monat"] / 12)

    # LAGS - Strikt rueckwaertsgerichtet und je Stadtteil gebildet, nie ueber
    # Stadtteilgrenzen hinweg. Beim gleitenden Mittel steht `shift(1)` VOR
    # `rolling(3)`: der Wert fuer Monat t verwendet t-1, t-2, t-3, nie t selbst.
    d = d.sort_values(["stadtteil", "jahr_monat"]).reset_index(drop=True)
    g = d.groupby("stadtteil")[ZIELGROESSE]
    d["lag_1"]          = g.shift(1)
    d["lag_12"]         = g.shift(12)
    d["rolling_mean_3"] = g.transform(lambda s: s.shift(1).rolling(3).mean())

    # Vorlauf abschneiden. Erst danach greift die NaN-Pruefung des balancierten
    # Panels - die Vorlaufmonate haben absichtlich keine Strukturmerkmale (der
    # Kriminalitaetsindex beginnt erst 2015-01) und duerfen die Stadtteilauswahl
    # nicht beeinflussen.
    vor_schnitt = len(d)
    d = d[d["jahr_monat"] >= START].reset_index(drop=True)
    if verbose and vorlauf:
        print(f"  Lag-Vorlauf: {vor_schnitt - len(d):,} Vorlaufzeilen "
              f"({von}-{_monat_minus(START, 1)}) nach der Lag-Bildung entfernt")

    # Balanciertes Panel (Decision Log #15): Treasure Island und Lakeshore
    # fehlen in JEDEM ACS-Jahrgang, Mission Bay erst ab 2021. Zeilenweises
    # dropna erzeugte ein unbalanciertes Panel - Mission Bay taucht mitten in
    # der Zeitreihe auf, die Folds enthalten unterschiedlich viele Stadtteile,
    # und die Testfenster-Summe springt allein durch diesen Zutritt.
    luecken = (d.groupby("stadtteil")[PRAEDIKTOREN]
                .apply(lambda g: g.isna().any().any()))
    raus = sorted(luecken[luecken].index)
    d = d[~d["stadtteil"].isin(raus)].reset_index(drop=True)
    if verbose and raus:
        print(f"  Balanciertes Panel: {len(raus)} Stadtteil(e) ohne durchgaengige "
              f"Abdeckung ausgeschlossen -> {raus}")

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
    # Zweite Zielgroesse: Einsaetze je 1.000 Einwohner. Fuer den Vergleich
    # zwischen unterschiedlich grossen Stadtteilen ist die Rate die
    # aussagekraeftigere Groesse - die absolute Zahl bildet vor allem die
    # Einwohnerzahl ab (Decision Log #29).
    d[RATE] = d[ZIELGROESSE] / d[EXPOSURE_ROH].astype(float) * 1000

    d = d.sort_values(["jahr_monat", "stadtteil"]).reset_index(drop=True)
    merkmale = FEATURE_SETS["S"] + LAGS
    spalten = SCHLUESSEL + [ZIELGROESSE, RATE] + merkmale + R_NEBEN
    return _setze_datentypen(d[spalten], merkmale)


# ==========================================================================
# TEIL C  STRUKTUR DER EINSATZLAST
# ==========================================================================
def baue_klassifikation(regression: pd.DataFrame,
                        verbose: bool = False) -> pd.DataFrame:
    """Anteile der vier NFIRS-Gruppen je Stadtteil und Monat.

    Zielgroesse ist die ZUSAMMENSETZUNG der Einsatzlast, nicht die Art des
    einzelnen Einsatzes (Decision Log #29). Innerhalb eines Stadtteil-Monats
    tragen alle Einsaetze identische Strukturmerkmale; auf Einzeleinsatz-Ebene
    war deshalb nichts zu holen - ein perfektes Modell haette 49,9 % Treffer
    erreicht gegenueber 48,2 % fuer blosses Raten. Auf dieser Ebene ist die
    Frage beantwortbar.

    Zeilen, Zeitraum, Stadtteile, Merkmale und Folds werden dem
    Regressionsdatensatz ENTNOMMEN. Beide Teile der Arbeit beruhen damit
    zwingend auf demselben Datenbestand und derselben Aufteilung.
    """
    von, bis = int(regression["jahr_monat"].min()), int(regression["jahr_monat"].max())
    stadtteile = set(regression["stadtteil"])

    df = pd.read_parquet(PFAD_EINSAETZE)
    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]
    df = df[df["jahr_monat"].between(von, bis) & df["stadtteil"].isin(stadtteile)]
    df = df.drop_duplicates(subset=["einsatz_nummer"], keep="first").copy()

    # NFIRS-Codes sind hierarchisch, die fuehrende Ziffer bezeichnet die Serie.
    serie = df["einsatzart"].astype(str).str.extract(r"^(\d)")[0]
    df["gruppe"] = serie.map(NFIRS_GRUPPEN).fillna(RESTKLASSE)

    zaehlung = (df.groupby(["stadtteil", "jahr_monat", "gruppe"]).size()
                  .unstack(fill_value=0).reindex(columns=KLASSEN, fill_value=0)
                  .rename(columns={k: f"anzahl_{SUFFIX[k]}" for k in KLASSEN})
                  .reset_index())

    # gesamtbevoelkerung bleibt: die Fold-Zuteilung stratifiziert danach.
    d = regression.drop(columns=[RATE, CRIME_ROH] + LAGS).merge(
        zaehlung, on=["stadtteil", "jahr_monat"], how="left")
    d[ANZAHLEN] = d[ANZAHLEN].fillna(0).astype("int64")

    # Monate ganz ohne Einsatz haben keine Zusammensetzung - der Anteil waere
    # 0/0. Sie fallen heraus und werden gemeldet, statt still zu NaN zu werden.
    ohne = d[ZIELGROESSE] == 0
    if ohne.any():
        d = d[~ohne].reset_index(drop=True)

    for k in KLASSEN:
        d[f"anteil_{SUFFIX[k]}"] = d[f"anzahl_{SUFFIX[k]}"] / d[ZIELGROESSE]

    # Zielgroesse der Klassifikation: die haeufigste Einsatzart des Monats.
    # Eine echte Klasse, kein gesetzter Schwellwert - argmax ueber die vier
    # NFIRS-Gruppen. Damit entfaellt die Begruendungslast, die eine kuenstliche
    # Einteilung einer Zaehlgroesse mit sich braechte (Altman & Royston 2006).
    d[ZIELKLASSE] = d[ANTEILE].idxmax(axis=1).str.replace("anteil_", "",
                                                          regex=False)

    # Keine Ergebnisvariable darf im Datensatz landen - diese Spalten stehen
    # erst nach dem Einsatz fest (Decision Log #20).
    verboten = [c for c in ERGEBNISVARIABLEN if c in d.columns]
    assert not verboten, f"Ergebnisvariablen im Datensatz: {verboten}"

    spalten = (SCHLUESSEL + [ZIELGROESSE, ZIELKLASSE] + ANZAHLEN + ANTEILE
               + MERKMALE_STRUKTUR + SAISON + [EXPOSURE_ROH])
    ergebnis = _setze_datentypen(d[spalten], MERKMALE_STRUKTUR + SAISON)

    if verbose:
        if ohne.any():
            print(f"  {int(ohne.sum())} Stadtteil-Monat(e) ohne Einsatz entfernt "
                  f"(keine Zusammensetzung definierbar)")
        print(f"  Strukturdaten: {len(ergebnis):,} Stadtteil-Monate | "
              f"{ergebnis['stadtteil'].nunique()} Stadtteile | {von}-{bis}")
        print("  Anteile im Mittel:", " | ".join(
            f"{k} {ergebnis[f'anteil_{SUFFIX[k]}'].mean() * 100:.1f} %"
            for k in KLASSEN))
        spanne = ergebnis.groupby("stadtteil")["anteil_brand"].mean()
        print(f"  Brandanteil je Stadtteil: {spanne.min() * 100:.1f} % bis "
              f"{spanne.max() * 100:.1f} % (Faktor {spanne.max() / spanne.min():.1f})")
    return ergebnis


# ==========================================================================
# Ablauf
# ==========================================================================
def run(verbose: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    r = baue_regression(verbose=verbose)
    print()
    k = baue_klassifikation(r, verbose=verbose)

    # Fold-Zuteilung EINMAL fuer beide Datensaetze, stratifiziert nach der
    # seltensten Klasse und der Bevoelkerung. Sie muss identisch sein, sonst
    # waeren die Ergebnisse der beiden Straenge nicht vergleichbar.
    selten = (k[k[ZIELKLASSE] == "brand"].groupby("stadtteil").size()
                .reindex(sorted(r["stadtteil"].unique()), fill_value=0))
    r = ergaenze_aufteilung(r, selten=selten)
    k = ergaenze_aufteilung(k, selten=selten)

    r.to_parquet(PFAD_REGRESSION, index=False)
    if verbose:
        print(f"\n  => {PFAD_REGRESSION.relative_to(ROOT)}  "
              f"({len(r):,} Zeilen | {len(r.columns)} Spalten)")
        print(f"  Zeitraum {r['jahr_monat'].min()}-{r['jahr_monat'].max()} | "
              f"{r['stadtteil'].nunique()} Stadtteile | "
              f"{r.groupby(['jahr', 'monat']).ngroups} Monate | "
              f"{len(FEATURE_SETS['S'])} Merkmale")
        for j in range(1, N_FOLDS + 1):
            n_brand = int((k.loc[k["fold"] == j, ZIELKLASSE] == "brand").sum())
            print(f"    Fold {j}: {r.loc[r['fold'] == j, 'stadtteil'].nunique()} "
                  f"Stadtteile, davon {n_brand} brand-dominierte Monate im Test")
        ho = r["ist_holdout"] == 1
        print(f"    Hold-out: {r.loc[ho, 'stadtteil'].nunique()} Stadtteile "
              f"({ho.sum():,} Zeilen, beim Tuning unberuehrt)")

    k.to_parquet(PFAD_KLASSIFIKATION, index=False)
    if verbose:
        print(f"\n  => {PFAD_KLASSIFIKATION.relative_to(ROOT)}  "
              f"({len(k):,} Zeilen | {len(k.columns)} Spalten)")
        v = k[ZIELKLASSE].value_counts(normalize=True)
        print(f"  Zielgroesse Klassifikation ({ZIELKLASSE}): "
              + " | ".join(f"{a} {n * 100:.1f} %" for a, n in v.items()))
    return r, k


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "splits":
        d = pd.read_parquet(PFAD_REGRESSION)
        print(beschreibe_splits(d))
        print("\n  Zeilen je Fold:")
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            print(f"    Fold {k}: Train {tr.sum():>5,} | Test {te.sum():>5,}")
        print(f"    Hold-out: {int(d['ist_holdout'].sum()):,} Zeilen")
    else:
        run()
        print("\n  Pruefungen: python tests/test_aufbereitung.py")
