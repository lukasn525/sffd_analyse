"""
Schritt 2: die beiden finalen Datensaetze samt Validierungsrahmen.

    python prep/s2_datensaetze.py          beide Datensaetze bauen
    python prep/s2_datensaetze.py splits   nur die Aufteilung anzeigen

Eingang: data/processed/einsaetze.parquet        ein Einsatz je Zeile
Ausgang: data/processed/regression.parquet       Stadtteil x Monat, Menge
         data/processed/klassifikation.parquet   Stadtteil x Monat, Struktur

- Beide Dateien liegen auf derselben Analyseeinheit. Die eine misst die MENGE
  der Einsatzlast, die andere ihre ZUSAMMENSETZUNG (Decision Log #29).
- Der Validierungsrahmen steht hier, weil die Aufteilung als SPALTEN in die
  Dateien geht. "Alle Verfahren sehen identische Folds" ist damit eine Zusage
  ueber den DATENSATZ, nicht ueber die Algorithmen.
- Drei Teile: A Stadtteil-Split, B Menge, C Struktur.
- Kennzahlen der erzeugten Dateien: docs/03_STAND.md

Ausfuehrlich: docs/08_FUNKTIONSDOKUMENTATION.md
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from config import (ANTEILE, ANZAHLEN, BEV_PLAUSIBEL, CRIME_ROH, ENDE,
                    ERGEBNISVARIABLEN,
                    EXPOSURE_ROH, FEATURE_SETS, KLASSEN, LAGS,
                    MERKMALE_STRUKTUR, N_FOLDS, N_STADTTEILE_ERWARTET,
                    NFIRS_GRUPPEN, PARKGEBIETE,
                    PFAD_EINSAETZE, PFAD_KLASSIFIKATION, PFAD_REGRESSION,
                    PRAEDIKTOREN, RESTKLASSE, ROOT, SAISON, START,
                    VOLLSTAENDIGKEITS_SCHWELLE, VORLAUF_MONATE)

ZIELGROESSE  = "anzahl_einsaetze"
RATE         = "einsaetze_je_1000_ew"   # zweite Zielgroesse der Menge
ZIELKLASSE   = "dominante_einsatzart"   # Zielgroesse der Klassifikation
SCHLUESSEL   = ["stadtteil", "jahr", "monat", "jahr_monat"]
R_NEBEN      = [EXPOSURE_ROH, CRIME_ROH]   # Poisson-Offset, Deskription 5.1
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
# Geprueft wird, indem ganze Stadtteile zurueckgehalten werden: 6 ins Hold-out,
# die uebrigen 30 auf 5 Folds (6/6/6/6/6), jeder genau einmal Testfall. Ein
# Zeitschnitt wuerde die Forschungsfrage nicht pruefen - dort steht jeder
# Stadtteil in Training UND Test (Decision Log #29).
# ==========================================================================
def ergaenze_aufteilung(daten: pd.DataFrame, versatz: int = 0,
                        selten: pd.Series | None = None) -> pd.DataFrame:
    """Schreibt `fold` (0..N_FOLDS) und `ist_holdout` in den Datensatz.

    Ein:  Datensatz, `versatz` fuer wiederholte Splits, `selten` als Zahl
          brand-dominierter Monate je Stadtteil
    Aus:  derselbe Datensatz mit zwei zusaetzlichen Spalten

    - die Stadtteile werden reihum auf N_FOLDS + 1 Gruppen verteilt; Gruppe 0 ist
      das Hold-out
    - doppelte Stratifizierung (#30): erst nach `selten`, sonst hat ein Fold
      keinen Brand-Testfall und Macro-F1 mittelt ueber eine fehlende Klasse
    - bei Gleichstand nach Bevoelkerung, sonst waere die Fold-Streuung ein
      Groesseneffekt
    - kein Leakage: festgelegt wird nur, welche Stadtteile gemeinsam getestet
      werden, wie bei StratifiedGroupKFold
    """
    # Doppelte Stratifizierung (#30): zuerst nach brand-dominierten Monaten,
    # bei Gleichstand nach Bevoelkerung.
    bev = daten.groupby("stadtteil")[EXPOSURE_ROH].mean()
    if selten is None:
        selten = pd.Series(0, index=bev.index)
    ordnung = (pd.DataFrame({"selten": selten.reindex(bev.index).fillna(0),
                             "bev": bev})
                 .sort_values(["selten", "bev"], ascending=False).index)

    # Reihum auf N_FOLDS + 1 Gruppen; Gruppe 0 ist das Hold-out.
    gruppe = {st: (i + versatz) % (N_FOLDS + 1) for i, st in enumerate(ordnung)}

    d = daten.copy()
    d["fold"] = d["stadtteil"].map(gruppe).astype(int)
    d["ist_holdout"] = (d["fold"] == 0).astype(int)
    return d


def fold_masken(daten: pd.DataFrame, k: int) -> tuple[pd.Series, pd.Series]:
    """Liefert Trainings- und Testmaske des Folds k aus den Spalten der Datei.

    Ein:  Datensatz mit fold-Spalte, Foldnummer k
    Aus:  zwei boolesche Masken (Training, Test)

    - Test sind die Stadtteile dieses Folds mit allen Monaten
    - Training sind alle uebrigen Entwicklungsstadtteile, ohne das Hold-out
    - kein Stadtteil ist je zugleich Trainings- und Testfall
    """
    test = daten["fold"] == k
    assert test.any(), (f"Kein Fold {k} im Datensatz "
                        f"(vorhanden: {sorted(daten['fold'].unique())}).")
    train = (daten["fold"] != k) & (daten["ist_holdout"] == 0)
    return train, test


def beschreibe_splits(daten: pd.DataFrame) -> str:
    """Fasst die Aufteilung lesbar zusammen, fuer Kapitel 5.2 und 5.4.

    Ein:  Datensatz mit Fold-Spalten
    Aus:  nichts, reine Konsolenausgabe

    - zeigt, welcher Stadtteil in welchem Fold getestet wird
    - zeigt, dass jeder Fold den vollen Zeitraum abdeckt: der Unterschied zum
      Zeitschnitt
    """
    monate = sorted(int(m) for m in daten["jahr_monat"].unique())
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
    """Verschiebt einen jahr_monat-Schluessel um n Monate zurueck.

    Ein:  Schluessel wie 202403, Zahl der Monate
    Aus:  verschobener Schluessel
    """
    jahr, monat = divmod(jahr_monat, 100)
    gesamt = jahr * 12 + (monat - 1) - monate
    return (gesamt // 12) * 100 + (gesamt % 12) + 1


def _setze_datentypen(d: pd.DataFrame, merkmale: list[str]) -> pd.DataFrame:
    """Vereinheitlicht die Datentypen auf modelltaugliche NumPy-Typen.

    Ein:  Datensatz, Merkmalsliste
    Aus:  derselbe Datensatz - Merkmale float64, Schluessel und Zaehlgroessen
          int64, stadtteil str

    - notwendig, weil EINE nullable Int64-Spalte genuegt, damit X.to_numpy() ein
      object-Array liefert
    - sklearn faengt das still ab, XGBoost lehnt es ab (#24)
    """
    # Eine einzige nullable Int64-Spalte genuegt, damit X.to_numpy() ein
    # object-Array liefert: sklearn faengt das still ab, XGBoost nicht (#24).
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
    """Verdichtet die Einsatz-Ebene zu Stadtteil x Monat, vollstaendiges Raster.

    Ein:  Zeitgrenzen als jahr_monat-Schluessel, inklusive Lag-Vorlauf
    Aus:  Panel mit einer Zeile je Stadtteil und Monat

    - vollstaendig heisst: auch ein Monat ohne Einsaetze bekommt eine Zeile mit
      Null
    - ohne dieses Raster verrutschen die Lags, weil ein ruhiger Monat
      stillschweigend fehlte
    """
    df = pd.read_parquet(PFAD_EINSAETZE)
    if not mit_parkgebieten:
        df = df[~df["stadtteil"].isin(PARKGEBIETE)]
    # Sicherheits-Dedup (idempotent; bereinigt wird in s1_daten.py).
    df = df.drop_duplicates(subset=["einsatz_nummer"], keep="first")
    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]

    if verbose:
        # Warnung vor angebrochenen Randmonaten (Decision Log #12). Massgeblich
        # bleibt ENDE aus config.py - die Warnung korrigiert nichts, sie meldet.
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
    """Baut den vollstaendigen Regressionsdatensatz.

    Ein:  einsaetze.parquet, Zahl der Vorlaufmonate
    Aus:  4.752 Zeilen x 25 Spalten - Merkmale, beide Mengen-Zielgroessen,
          Exposition, Saison, Lags

    - Lag-Vorlauf (#23): aggregiert wird ab START minus `vorlauf` Monaten, damit
      lag_12 schon fuer den ersten Analysemonat definiert ist
    - danach Zuschnitt auf START
    - die Vorlaufmonate gehen ausschliesslich ueber shift() ein, nie als eigene
      Zeile
    """
    von = _monat_minus(START, vorlauf)
    d = aggregiere(von=von, bis=ENDE, verbose=verbose)

    # SAISON als sin/cos statt Monat 1-12 (Begruendung in config.py, SAISON).
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

    # Balanciertes Panel (Decision Log #15): Stadtteile ohne durchgaengige
    # ACS-Abdeckung fliegen GANZ raus. Zeilenweises dropna erzeugte sonst ein
    # unbalanciertes Panel - ein Stadtteil tritt mitten in der Zeitreihe hinzu.
    luecken = (d.groupby("stadtteil")[PRAEDIKTOREN]
                .apply(lambda g: g.isna().any().any()))
    raus = sorted(luecken[luecken].index)
    d = d[~d["stadtteil"].isin(raus)].reset_index(drop=True)
    if verbose and raus:
        print(f"  Balanciertes Panel: {len(raus)} Stadtteil(e) ohne durchgaengige "
              f"Abdeckung ausgeschlossen -> {raus}")

    # Sicherheitsnetz bei zu kurzem Vorlauf: Anlaufmonate ohne lag_12 entfallen
    # fuer ALLE Verfahren gleichermassen, sonst liefen sie auf unterschiedlichen
    # Zeilen (Fairness-Regel).
    vor_dropna = len(d)
    d = d.dropna(subset=LAGS).reset_index(drop=True)
    if verbose and vor_dropna - len(d):
        print(f"  {vor_dropna - len(d):,} Anlaufmonate ohne lag_12 entfernt")

    # Zweite Zielgroesse: Einsaetze je 1.000 Einwohner. Fuer den Vergleich
    # zwischen unterschiedlich grossen Stadtteilen ist die Rate die
    # aussagekraeftigere Groesse - die absolute Zahl bildet vor allem die
    # Einwohnerzahl ab (Decision Log #29).
    d[RATE] = d[ZIELGROESSE] / d[EXPOSURE_ROH].astype(float) * 1000

    # REPRODUZIERBARKEITSVERTRAG - diese Sortierung darf nicht veraendert
    # werden: Random Forest und XGBoost ziehen ihre Bootstrap- bzw.
    # Subsample-Stichproben ueber Zeilenpositionen. Eine andere Reihenfolge
    # liefert trotz identischem random_state leicht andere Baeume (empirisch
    # 17,2587 statt 17,2974 RMSE in Fold 1). Ridge ist reihenfolgeinvariant.
    d = d.sort_values(["jahr_monat", "stadtteil"]).reset_index(drop=True)
    merkmale = FEATURE_SETS["S"] + LAGS
    spalten = SCHLUESSEL + [ZIELGROESSE, RATE] + merkmale + R_NEBEN
    return _setze_datentypen(d[spalten], merkmale)


# ==========================================================================
# TEIL C  STRUKTUR DER EINSATZLAST
# ==========================================================================
def baue_klassifikation(regression: pd.DataFrame,
                        verbose: bool = False) -> pd.DataFrame:
    """Baut die Anteile der vier NFIRS-Gruppen je Stadtteil und Monat.

    Ein:  der fertige Regressionsdatensatz
    Aus:  4.751 Zeilen x 29 Spalten mit `dominante_einsatzart` als argmax ueber
          die vier Anteile

    - Zielgroesse ist die ZUSAMMENSETZUNG der Einsatzlast, nicht die Art des
      einzelnen Einsatzes (#29)
    - Grund: Innerhalb eines Stadtteil-Monats tragen alle Einsaetze identische
      Strukturmerkmale; auf Einzeleinsatz-Ebene war nichts zu holen (49,9 % gegen
      48,2 % fuer blosses Raten)
    - Zeilen, Zeitraum, Merkmale und Folds werden dem Regressionsdatensatz
      entnommen; beide Straenge beruhen zwingend auf derselben Aufteilung
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
def pruefe_zuschnitt(r: pd.DataFrame) -> None:
    """Prueft, ob der Analysezuschnitt der Festlegung entspricht.

    Ein:  fertiger Regressionsdatensatz
    Aus:  nichts; bricht ab, wenn Zuschnitt oder Exposition unplausibel sind

    - ein Verbund, der nicht matchende Zeilen verwirft, verliert Analyseeinheiten
      und Bevoelkerung, ohne dass etwas abbricht; alle Folgezahlen sehen
      weiterhin plausibel aus. Die 19 Pruefungen in tests/ sichern die Struktur
      der erzeugten Dateien, nicht die Plausibilitaet ihrer Werte
    - deshalb hier zwei Groessen, die ein solcher Verlust zwangslaeufig bewegt:
      die Zahl der Analyseeinheiten und die stadtweite Wohnbevoelkerung
    """
    ist = sorted(r["stadtteil"].unique())
    alle = sorted(pd.read_parquet(PFAD_EINSAETZE, columns=["stadtteil"])
                    ["stadtteil"].dropna().unique())
    assert len(ist) == N_STADTTEILE_ERWARTET, (
        f"{len(ist)} Analyseeinheiten statt {N_STADTTEILE_ERWARTET}. "
        f"Nicht enthalten: {sorted(set(alle) - set(ist))}")

    bev = (r.groupby(["jahr", "stadtteil"])[EXPOSURE_ROH].first()
             .groupby("jahr").sum())
    unten, oben = BEV_PLAUSIBEL
    assert unten <= bev.min() and bev.max() <= oben, (
        f"Stadtweite Wohnbevoelkerung {bev.min():,.0f} bis {bev.max():,.0f} "
        f"ausserhalb {unten:,}-{oben:,} - ACS-Zuordnung pruefen "
        f"(Tract-Grenzen der Jahrgaenge gegen den Crosswalk).")


def run(verbose: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Baut beide finalen Datensaetze, traegt die Folds ein und schreibt sie.

    Ein:  einsaetze.parquet
    Aus:  regression.parquet und klassifikation.parquet auf der Platte

    - die Fold-Zuteilung erfolgt EINMAL und wird auf beide Datensaetze angewandt
    - nur so sehen Menge und Struktur dieselben Stadtteile im Test
      (Fairness-Regel)
    """
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

    pruefe_zuschnitt(r)

    r.to_parquet(PFAD_REGRESSION, index=False)
    k.to_parquet(PFAD_KLASSIFIKATION, index=False)

    if verbose:
        for pfad, d in ((PFAD_REGRESSION, r), (PFAD_KLASSIFIKATION, k)):
            print(f"\n  => {pfad.relative_to(ROOT)}  "
                  f"({len(d):,} Zeilen | {len(d.columns)} Spalten)")
        # Einzige Kennzahl, die hier gedruckt wird: die Brand-Testfaelle je Fold.
        # Sie ist der Grund fuer die doppelte Stratifizierung (#30) und der
        # einzige Wert, der beim Lauf tatsaechlich kontrolliert werden muss.
        # Alles Weitere steht in docs/03_STAND.md und wird von den Tests geprueft.
        brand = [int((k.loc[k["fold"] == j, ZIELKLASSE] == "brand").sum())
                 for j in range(1, N_FOLDS + 1)]
        print(f"\n  Brand-Testfaelle je Fold: {' | '.join(map(str, brand))}"
              + ("   ACHTUNG: Fold ohne Brand!" if 0 in brand else ""))
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
