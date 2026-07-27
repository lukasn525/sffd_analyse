"""
Schritt 3b: Klassifikationsdatensatz - Zielgroesse Einsatzart.

Eingang:  data/processed/einsaetze.parquet      (ein Einsatz je Zeile)
          data/processed/regression.parquet     (nur fuer die Abgrenzung)
Ausgang:  data/processed/klassifikation.parquet (ein Einsatz je Zeile, gefiltert)

Gegenstueck zu regression_datensatz.py: dort Stadtteil-Monats-Ebene, hier
Einzeleinsatz-Ebene. Beide teilen zwingend dieselbe Abgrenzung - Zeitraum und
Stadtteilliste werden AUS DEM REGRESSIONSDATENSATZ uebernommen, nicht neu
definiert. Sonst beziehen sich die beiden Teile der Arbeit auf unterschiedliche
Datenbestaende.

ZWEI ZIELGROESSEN, damit die Entscheidung zwischen mehrklassiger und binaerer
Variante ohne erneute Aufbereitung revidierbar bleibt (Zeilen und Merkmale sind
identisch):

  einsatzart_gruppe  4 zusammengefasste NFIRS-Serien  -> Hauptanalyse (#21)
  ist_brand          Brand vs. Nicht-Brand            -> Robustheitslauf

Grundlage: docs/KLASSIFIKATION_DESIGN.md, Decision Log #6, #20, #21.

Ausfuehren:
  python prep/klassifikation_datensatz.py
"""
import numpy as np
import pandas as pd

from config import (CRIME_ROH, ERGEBNISVARIABLEN, EXPOSURE_ROH, KLASSEN,
                    MERKMALE_KATEGORIAL, MERKMALE_ORT, MERKMALE_STRUKTUR,
                    MERKMALE_ZEIT, MIT_BATAILLON, NFIRS_GRUPPEN,
                    PFAD_EINSAETZE, PFAD_KLASSIFIKATION, PFAD_REGRESSION,
                    RESTKLASSE, ROOT)
from cv import ergaenze_aufteilung

SCHLUESSEL  = ["einsatz_nummer", "stadtteil", "jahr", "monat", "jahr_monat"]
ZIELGROESSEN = ["einsatzart_gruppe", "ist_brand"]
AUFTEILUNG  = ["fold", "ist_holdout"]


def _abgrenzung() -> tuple[int, int, list[str]]:
    """Zeitraum und Stadtteilliste aus dem Regressionsdatensatz uebernehmen."""
    if not PFAD_REGRESSION.exists():
        raise FileNotFoundError(
            f"{PFAD_REGRESSION.relative_to(ROOT)} fehlt. Der "
            f"Klassifikationsdatensatz uebernimmt seine Abgrenzung von dort - "
            f"erst 'python prep/regression_datensatz.py' ausfuehren.")
    r = pd.read_parquet(PFAD_REGRESSION, columns=["jahr_monat", "stadtteil"])
    return (int(r["jahr_monat"].min()), int(r["jahr_monat"].max()),
            sorted(r["stadtteil"].unique()))


def merkmalslisten(mit_ort: bool = MIT_BATAILLON) -> dict[str, list[str]]:
    """Merkmalssaetze der geplanten Laeufe (docs/KLASSIFIKATION_DESIGN.md).

      A+B  Stadtteilstruktur + Zeitpunkt -> Hauptmodell
      B    nur Zeitpunkt                 -> zeigt den Beitrag der Struktur
      A    nur Stadtteilstruktur         -> Gegenprobe
    """
    saetze = {
        "A+B": MERKMALE_STRUKTUR + MERKMALE_ZEIT + MERKMALE_KATEGORIAL,
        "B":   MERKMALE_ZEIT + MERKMALE_KATEGORIAL,
        "A":   list(MERKMALE_STRUKTUR),
    }
    if mit_ort:
        saetze["A+B+Ort"] = saetze["A+B"] + MERKMALE_ORT
    return saetze


def baue_datensatz(mit_ort: bool = MIT_BATAILLON,
                   verbose: bool = False) -> pd.DataFrame:
    von, bis, stadtteile = _abgrenzung()

    df = pd.read_parquet(PFAD_EINSAETZE)
    df["jahr_monat"] = df["jahr"] * 100 + df["monat"]
    df = df[df["jahr_monat"].between(von, bis)]
    df = df[df["stadtteil"].isin(stadtteile)].copy()
    # Sicherheits-Dedup (idempotent; die Bereinigung erfolgt in join.py).
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
    # lineare Korrelation mit `stunde` betraegt aber nur -0,006. Als Zahl laegen
    # Stunde 23 und Stunde 0 maximal weit auseinander.
    df["stunde_sin"] = np.sin(2 * np.pi * df["stunde"] / 24)
    df["stunde_cos"] = np.cos(2 * np.pi * df["stunde"] / 24)
    df["monat_sin"]  = np.sin(2 * np.pi * df["monat"] / 12)
    df["monat_cos"]  = np.cos(2 * np.pi * df["monat"] / 12)

    merkmale = (MERKMALE_STRUKTUR + MERKMALE_ZEIT + MERKMALE_KATEGORIAL
                + (MERKMALE_ORT if mit_ort else []))
    d = df.dropna(subset=MERKMALE_STRUKTUR).reset_index(drop=True)
    d = ergaenze_aufteilung(d)

    # Reihenfolge wie im Regressionsdatensatz: Zeit, dann Stadtteil. Zusaetzlich
    # nach Einsatznummer, damit die Sortierung innerhalb eines Monats eindeutig
    # ist (Reproduzierbarkeitsvertrag, vgl. regression_datensatz.py).
    d = (d.sort_values(["jahr_monat", "stadtteil", "einsatz_nummer"])
           .reset_index(drop=True))

    ergebnis = d[SCHLUESSEL + ZIELGROESSEN + merkmale + AUFTEILUNG]

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
        print("\n  Zielgroesse A - einsatzart_gruppe (4 Klassen):")
        v = ergebnis["einsatzart_gruppe"].value_counts()
        for k in KLASSEN:
            n = int(v.get(k, 0))
            print(f"    {k:<26} {n:>7,}  ({n / len(ergebnis) * 100:5.1f} %)")
        print(f"    Ungleichgewicht groesste/kleinste: {v.max() / v.min():.1f}:1")
        b = ergebnis["ist_brand"].mean()
        print(f"\n  Zielgroesse B - ist_brand (binaer): {b * 100:.1f} % Brand, "
              f"Ungleichgewicht {(1 - b) / b:.1f}:1")
        print(f"    -> scale_pos_weight = {(1 - b) / b:.2f} bzw. class_weight='balanced'")
    return ergebnis


def run(verbose: bool = True) -> pd.DataFrame:
    d = baue_datensatz(verbose=verbose)
    d.to_parquet(PFAD_KLASSIFIKATION, index=False)
    if verbose:
        print(f"\n  => {PFAD_KLASSIFIKATION.relative_to(ROOT)}  "
              f"({len(d):,} Zeilen | {len(d.columns)} Spalten)")
        print("  Merkmalssaetze: "
              + ", ".join(f"{k} ({len(v)})" for k, v in merkmalslisten().items()))
    return d


if __name__ == "__main__":
    run()
    print("\n  Pruefungen: python tests/test_aufbereitung.py")
