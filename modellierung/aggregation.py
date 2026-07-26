"""
Aggregation des Prep-Pipeline-Outputs (Einsatz-Ebene) auf Stadtteil x Monat.

Dieser Schritt fehlt in der bestehenden Prep-Pipeline (deren Output ist
Einsatz-Ebene) und wird hier ergaenzt, OHNE die Pipeline zu veraendern
(vgl. CLAUDE.md, Abschnitt Preprocessing / Decision Log).

Entwurfsentscheidungen (Bezug: Expose, Kap. 3):
- Zielgroesse Regression: anzahl_einsaetze pro Stadtteil und Monat.
- Stadtteil-Merkmale (ACS/Crime/Land Use) sind je Stadtteil bzw.
  Stadtteil x ACS-Jahr konstant -> "first" bei der Aggregation.
- Monate ohne Einsatz sind echte Nullen -> vollstaendiges Raster
  Stadtteil x Monat, fehlende Kombinationen mit 0 aufgefuellt.
- Der Analysezeitraum endet mit dem letzten VOLLSTAENDIGEN Kalenderjahr
  (Konstante ENDE, s. u. / Decision Log #12).
- Exposure: log(Bevoelkerung) statt roher Bevoelkerungszahl
  (Decision Log #13).

Alle drei Modelle (Ridge, RF, XGBoost) erhalten exakt diesen Datensatz.
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
FEATURES_PARQUET = ROOT / "data" / "processed" / "sf_fire_risk_features.parquet"

# --------------------------------------------------------------------------
# Analysezeitraum - FESTGESETZT am 2026-07-26 (Decision Log #12, #18)
# --------------------------------------------------------------------------
# Der Analysezeitraum ist bewusst hart fixiert und wird nicht mehr aus den
# Daten abgeleitet. Damit ist jeder Lauf reproduzierbar, unabhaengig davon,
# wie weit der jeweils letzte DataSF-Download reicht.
#
# START = 2015-01: frueheste Periode mit vollstaendigen ACS-Merkmalen unter
#   Beruecksichtigung des Publikationsversatzes von einem Jahr
#   (akademikerquote_pct existiert erst ab ACS-Jahrgang 2014, der ab 2015
#   nutzbar ist; Decision Log #5 und #11).
# ENDE  = 2025-12: letztes vollstaendiges Kalenderjahr.
START = 201501

# --------------------------------------------------------------------------
# Ausgeschlossene Analyseeinheiten (Decision Log #19, 2026-07-26)
# --------------------------------------------------------------------------
# Stadtteile ohne nennenswerte Wohnbevoelkerung sind fuer ein
# bevoelkerungsbezogenes Risikomodell keine sinnvolle Analyseeinheit: Jede
# Pro-Kopf-Groesse (Exposure, Kriminalitaetsindex, Einsatzrate) wird dort
# beliebig gross, weil der Nenner gegen null geht.
#   Golden Gate Park:  45 Einwohner, Kriminalitaetsindex im Median 186 (!),
#                      7.394 Einsaetze je 1.000 Einwohner und Jahr
#   Lincoln Park:     299 Einwohner
#   McLaren Park:     507 Einwohner, zusaetzlich bekanntes Census-Artefakt
#                      (Armutsquote 0,90)
# Zum Vergleich: Der Median aller uebrigen Stadtteile liegt bei 14.435
# Einwohnern. Der Ausschluss ist eine bewusste, dokumentierte Entscheidung
# ueber die Analyseeinheit - keine Ausreisserbereinigung nach Zielgroesse.
PARKGEBIETE = ["Golden Gate Park", "Lincoln Park", "Mclaren Park"]

# --------------------------------------------------------------------------
# Randmonat-Bugfix (Decision Log #12, 2026-07-26)
# --------------------------------------------------------------------------
# Bis 2026-07-26 wurde lediglich der MAXIMALE jahr_monat abgeschnitten. Die
# Rohdaten enthalten 2026-02 mit 1 Einsatz und 2026-01 mit 258 Einsaetzen
# (Normalwert ~3.300). Abgeschnitten wurde nur 2026-02; Januar 2026 blieb als
# scheinbar vollstaendiger Monat im Panel und lag im Testfenster des letzten
# CV-Folds. Wirkung: naive Baseline im letzten Fold R2 0,740 statt 0,955.
# Die im Umsetzungsleitfaden als "Nichtstationaritaet" (A2) gedeutete
# Fold-Instabilitaet war damit ein Datenartefakt.
#
# Fix: harter, dokumentierter Schnitt am letzten vollstaendigen Kalenderjahr.
# Reproduzierbar und im Methodenkapitel in einem Satz begruendbar. Bei einem
# kuenftigen Neu-Download der Rohdaten muss diese Konstante nachgezogen werden;
# pruefe_randmonate() warnt automatisch, falls das vergessen wurde.
ENDE = 202512

# Ein Monat gilt als unvollstaendig, wenn seine stadtweite Einsatzzahl unter
# diesem Anteil des Median-Monats liegt (nur Warnung, kein automatischer Filter).
VOLLSTAENDIGKEITS_SCHWELLE = 0.5

# --------------------------------------------------------------------------
# Praediktoren gemaess Expose: soziooekonomisch, kriminalitaetsbezogen, baulich
# --------------------------------------------------------------------------
# Exposure (Decision Log #13, 2026-07-26): Statt der rohen Einwohnerzahl geht
# log(Bevoelkerung) in die Modelle ein. Begruendung: Der Zusammenhang zwischen
# Bevoelkerung und Einsatzzahl ist multiplikativ, nicht additiv; ohne
# Exposure-Kontrolle sagt das Modell im Kern die Stadtteilgroesse vorher.
# Empirisch (Stand 2026-07-26): armutsquote_pct korreliert +0,20 mit der
# absoluten Einsatzzahl, aber -0,13 mit Einsaetzen je 1.000 Einwohner - das
# Vorzeichen des zentralen Struktur-Befundes haengt an dieser Entscheidung.
# Die Zielgroesse bleibt eine Zaehlgroesse (NegBin-Baseline und die
# Overdispersion-Argumentation bleiben damit gueltig); fuer die NegBin-Baseline
# ist log_bevoelkerung als echter Offset zu verwenden (Koeffizient auf 1
# fixiert), fuer Ridge/RF/XGBoost als regulaeres Feature.
#
# Kriminalitaet (Decision Log #17, 2026-07-26): `kriminalitaetsindex` ist der
# relative Location Quotient je Stadtteil und Monat (1,0 = Stadtdurchschnitt),
# gebildet in 02_join.py aus einem rollierenden 12-Monats-Fenster, das im
# Vormonat endet. Er ersetzt die frueheren statischen Anteilsmerkmale
# `anteil_gewaltdelikte_pct` / `anteil_eigentumsdelikte_pct`, die weder
# Zeitvarianz besassen noch leakage-frei waren.
#
# Als Modellmerkmal geht der Index LOGARITHMIERT ein
# (`log_kriminalitaetsindex`), analog zu log_bevoelkerung. Ein Quotient ist
# multiplikativ und rechtsschief; der Logarithmus macht ihn symmetrisch um 0
# (0 = Stadtdurchschnitt, +0,69 = doppelt so hoch, -0,69 = halb so hoch) und
# damit fuer ein lineares Modell brauchbar. Fuer Baumverfahren ist die
# monotone Transformation wirkungsneutral, deshalb wird sie einheitlich fuer
# ALLE Modelle angewandt und nicht modellspezifisch (Fairness-Regel).
PRAEDIKTOREN = [
    "median_haushaltseinkommen", "armutsquote_pct", "akademikerquote_pct",
    "median_miete", "leerstandsquote_pct", "log_bevoelkerung",
    "log_kriminalitaetsindex",
    "anteil_altbau_vor_1940_pct", "anteil_wohngebaeude_pct",
    "anteil_risikogewerbe_pct",
]

# Rohe Bevoelkerungszahl: kein Modell-Praediktor, wird aber im Datensatz
# mitgefuehrt fuer (a) den NegBin-Offset und (b) die Sensitivitaetsanalyse mit
# der Zielgroesse "Einsaetze je 1.000 Einwohner".
EXPOSURE_ROH = "gesamtbevoelkerung"

# Roher Kriminalitaetsindex: Grundlage der log-Transformation und zugleich die
# interpretierbare Groesse fuer Kapitel 5.1 (1,0 = Stadtdurchschnitt).
CRIME_ROH = "kriminalitaetsindex"

# Spalten, die aus der Einsatz-Tabelle je Stadtteil-Monat uebernommen werden.
# Die log-Merkmale werden erst danach berechnet.
_ABGELEITET  = ["log_bevoelkerung", "log_kriminalitaetsindex"]
_UEBERNOMMEN = ([c for c in PRAEDIKTOREN if c not in _ABGELEITET]
                + [EXPOSURE_ROH, CRIME_ROH])


def pruefe_randmonate(df: pd.DataFrame) -> None:
    """Warnt, wenn Monate im Rohdatenbestand unvollstaendig wirken.

    Reine Diagnose: Der massgebliche Schnitt ist die Konstante ENDE. Diese
    Pruefung stellt sicher, dass ein unvollstaendiger Randmonat nach einem
    Neu-Download nicht erneut unbemerkt ins Panel laeuft.
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
            print(f"    {jm}: {n:,} Einsaetze -> ENDE in aggregation.py pruefen!")


def lade_stadtteil_monat(pfad: Path = FEATURES_PARQUET,
                         von: int = START,
                         bis: int = ENDE,
                         mit_parkgebieten: bool = False,
                         verbose: bool = False) -> pd.DataFrame:
    """Laedt den Pipeline-Output und aggregiert auf Stadtteil x Monat.

    Parameter:
      von / bis        - Analysezeitraum als jahr_monat-Schluessel. Defaults sind
                         die festgesetzten Konstanten START/ENDE (201501/202512).
                         Abweichende Werte nur fuer Sensitivitaetsanalysen.
      mit_parkgebieten - True behaelt Golden Gate Park, Lincoln Park und
                         McLaren Park im Panel (nur fuer den Robustheitslauf,
                         s. PARKGEBIETE / Decision Log #19).
      verbose          - Diagnoseausgaben (Randmonatspruefung, Kennzahlen).

    Rueckgabe enthaelt neben PRAEDIKTOREN auch `gesamtbevoelkerung` (roh) fuer
    den NegBin-Offset und die Raten-Sensitivitaetsanalyse.
    """
    df = pd.read_parquet(pfad)
    if not mit_parkgebieten:
        df = df[~df["stadtteil"].isin(PARKGEBIETE)]

    # Sicherheits-Dedup (idempotent). Die eigentliche Bereinigung erfolgt seit
    # 2026-07-18 in der Prep-Pipeline (02_join, Decision Log #7).
    df = df.drop_duplicates(subset=["einsatz_nummer"], keep="first")

    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]
    if verbose:
        pruefe_randmonate(df)

    # Harter, festgesetzter Analysezeitraum (Decision Log #12, #18).
    # Das Zuschneiden erfolgt VOR der Aggregation, damit das Raster unten genau
    # den gewuenschten Zeitraum aufspannt.
    df = df[(df["jahr_monat"] >= von) & (df["jahr_monat"] <= bis)]
    if df.empty:
        raise ValueError(f"Keine Einsaetze im Zeitraum {von}-{bis}.")

    agg = (df.groupby(["stadtteil", "jahr", "monat"])
             .agg(anzahl_einsaetze=("einsatz_nummer", "count"),
                  **{c: (c, "first") for c in _UEBERNOMMEN})
             .reset_index())

    # Vollstaendiges Raster: Monate ohne Einsatz = 0
    idx = pd.MultiIndex.from_product(
        [agg["stadtteil"].unique(),
         range(int(agg["jahr"].min()), int(agg["jahr"].max()) + 1),
         range(1, 13)],
        names=["stadtteil", "jahr", "monat"])
    raster = agg.set_index(["stadtteil", "jahr", "monat"]).reindex(idx).reset_index()
    raster["anzahl_einsaetze"] = raster["anzahl_einsaetze"].fillna(0).astype(int)
    # Nur VORWAERTS fuellen (ffill): schliesst die durch das Raster erzeugten
    # einsatzfreien Monate mit dem letzten bekannten Stand desselben Stadtteils.
    # KEIN bfill: Rueckwaertsfuellen wuerde fehlende Werte (z. B.
    # akademikerquote vor ACS 2014) stillschweigend mit ZUKUNFTSWERTEN
    # imputieren (Leakage; Audit-Befund 2026-07-18, Decision Log #10). Echte
    # NaN bleiben sichtbar und werden bewusst in der Modellierungsschicht
    # behandelt (Zeitraumfilter).
    raster[_UEBERNOMMEN] = (raster.groupby("stadtteil")[_UEBERNOMMEN]
                                  .transform(lambda s: s.ffill()))

    raster["jahr_monat"] = raster["jahr"] * 100 + raster["monat"]
    raster = raster[(raster["jahr_monat"] >= von) & (raster["jahr_monat"] <= bis)]

    # Exposure-Transformation (Decision Log #13). log1p ist hier faktisch log,
    # da jeder bewohnte Stadtteil > 0 Einwohner hat; log1p vermeidet lediglich
    # eine Division durch Null bei Datenartefakten.
    raster["log_bevoelkerung"] = np.log1p(raster[EXPOSURE_ROH].astype(float))
    # Kriminalitaetsindex logarithmieren (Decision Log #17/#19): 0 entspricht
    # dem Stadtdurchschnitt. Nullwerte (Monate ohne jedes Delikt) wuerden
    # -inf erzeugen und werden auf NaN gesetzt, damit sie sichtbar bleiben
    # statt still zu einem Extremwert zu werden.
    idx = raster[CRIME_ROH].astype(float)
    raster["log_kriminalitaetsindex"] = np.log(idx.where(idx > 0))

    ergebnis = (raster.drop(columns="jahr_monat")
                      .sort_values(["jahr", "monat", "stadtteil"])
                      .reset_index(drop=True))
    if verbose:
        vollstaendig = ergebnis.dropna(subset=PRAEDIKTOREN)
        print(f"  Panel: {len(ergebnis):,} Zeilen | "
              f"{ergebnis['stadtteil'].nunique()} Stadtteile | "
              f"{ergebnis['jahr'].min()}-{ergebnis['jahr'].max()}")
        print(f"  Ohne NaN in Praediktoren: {len(vollstaendig):,} Zeilen | "
              f"{vollstaendig['stadtteil'].nunique()} Stadtteile | "
              f"{vollstaendig['jahr'].min()}-{vollstaendig['jahr'].max()}")
    return ergebnis


def balanciertes_panel(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Reduziert das Panel auf ein rechteckiges Stadtteil x Monat-Gitter.

    Hintergrund (Befund 2026-07-26): Nicht alle Stadtteile sind in allen
    ACS-Jahrgaengen enthalten. Konkret im Analysezeitraum ab 2015:
      - Treasure Island, Lakeshore: in KEINEM Jahrgang -> ohnehin ohne Werte
      - Mission Bay: erst ab ACS 2021 (die Jahrgaenge 2009/2014/2019 kennen
        den Stadtteil noch nicht als eigene Analyseeinheit)

    Wuerde man nur zeilenweise `dropna` anwenden, entstuende ein
    UNBALANCIERTES Panel: Mission Bay taucht mitten in der Zeitreihe auf. Das
    verzerrt den Verfahrensvergleich, weil die Folds dann unterschiedlich viele
    Stadtteile enthalten (Fold 1 mit 38, spaetere Folds mit 39) und die
    Gesamtzahl der Einsaetze im Testfenster allein durch den Zutritt eines
    Stadtteils springt.

    Daher: Stadtteile ohne DURCHGAENGIGE Merkmalsabdeckung im Analysezeitraum
    werden vollstaendig ausgeschlossen. Ergebnis ist ein rechteckiges Panel,
    identisch fuer alle drei Verfahren (Fairness-Regel).
    """
    unvollstaendig = (df.groupby("stadtteil")[PRAEDIKTOREN]
                        .apply(lambda g: g.isna().any().any()))
    raus = sorted(unvollstaendig[unvollstaendig].index)
    aus = df[~df["stadtteil"].isin(raus)].reset_index(drop=True)
    if verbose:
        print(f"  Balanciertes Panel: {len(raus)} Stadtteil(e) ohne "
              f"durchgaengige Abdeckung ausgeschlossen -> {raus}")
        print(f"  Ergebnis: {len(aus):,} Zeilen | "
              f"{aus['stadtteil'].nunique()} Stadtteile | "
              f"{aus['jahr'].min()}-{aus['jahr'].max()}")
    return aus


if __name__ == "__main__":
    # Hauptanalyse-Konfiguration (Decision Log #5, #11-#18)
    d = lade_stadtteil_monat(verbose=True)
    voll = balanciertes_panel(d, verbose=True)

    # Selbsttests (Done-Kriterien, Leitfaden Schritt 1)
    jm = voll["jahr"] * 100 + voll["monat"]
    n_monate = voll.groupby(["jahr", "monat"]).ngroups
    assert len(voll) == voll["stadtteil"].nunique() * n_monate, \
        "Panel ist nicht das vollstaendige Kreuzprodukt Stadtteil x Monat."
    assert voll[PRAEDIKTOREN].notna().all().all(), "NaN im balancierten Panel."
    assert jm.min() == START and jm.max() == ENDE, \
        f"Zeitraum {jm.min()}-{jm.max()} weicht von {START}-{ENDE} ab."
    assert n_monate == 132, f"Erwartet 132 Monate, gefunden {n_monate}."
    assert voll["log_bevoelkerung"].notna().all(), "Exposure fehlt."
    print("\n  Selbsttests bestanden (rechteckiges Panel, keine NaN, "
          f"Zeitraum {START}-{ENDE}, Exposure).")
