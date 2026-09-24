"""
Rohdatenbefunde.

    python prep/rohbefunde.py

Ausgang: results/deskriptiv/rohbefunde.md

Gegenstueck zu deskriptiv.py: Jenes beschreibt den AUFBEREITETEN Datensatz,
dieses die ROHQUELLEN, so wie sie vom Portal kommen.

--------------------------------------------------------------------------
WAS HIER STEHT UND WAS NICHT
--------------------------------------------------------------------------
Aufgenommen ist nur, was in der Aufbereitung eine FOLGE hat. Jede der sieben
Groessen traegt einen Eingriff:

  Meldungen gesamt / im Analysezeitraum -> Umfang, Zeitraumwahl
  Dubletten nach Einsatznummer          -> Dedup
  Parzellen ohne Baujahr                -> Nenner yrbuilt_count statt
                                           parcel_count
  ACS-Jahrgang 2009 ohne B15003         -> Analysebeginn 2015
  Tracts je Jahrgang gegen Crosswalk    -> Trefferquoten
  Einwohner der Parkgebiete             -> Ausschluss der drei Parks
  erster Jahrgang mit Mission Bay       -> Ausschluss der drei Stadtteile
                                           ohne durchgaengige Abdeckung

NICHT aufgenommen, bewusst:
  - Die Antwortzeit. Sie ist eine ERGEBNISvariable und faellt erst nach dem
    Einsatz an; sie ist weder Merkmal noch Zielgroesse. Ein Qualitaetsbefund
    ueber eine Spalte, die nie in die Analyse eingeht, ist Ballast.
  - Fehlende Medianwerte in einzelnen Tracts. Daraus folgt kein Eingriff.

--------------------------------------------------------------------------
WARUM DIE PARKGEBIETE ALS SPANNE BERICHTET WERDEN
--------------------------------------------------------------------------
Die Einwohnerzahl des Golden Gate Park haengt am ACS-Jahrgang. Eine einzelne
Zahl waere nicht reproduzierbar - wer nachrechnet, bekommt je nach Jahrgang
etwas anderes. Berichtet wird deshalb das MAXIMUM ueber die
genutzten Jahrgaenge gegen das MINIMUM des Medians der uebrigen Stadtteile.
Diese Aussage gilt in jedem genutzten Jahrgang und traegt das Argument
staerker als eine Einzelzahl.
"""


























from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

WURZEL = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WURZEL))

from prep.config import ACS_YEARS, START, ENDE  # noqa: E402

ROH = WURZEL / "data" / "raw"
PROC = WURZEL / "data" / "processed"
ZIEL = WURZEL / "results" / "deskriptiv"
PARKS = ["Golden Gate Park", "Lincoln Park", "McLaren Park", "Mclaren Park"]

# Der aelteste Jahrgang taucht im Analysezeitraum NICHT auf: Die Regel
# acs_jahr <= Einsatzjahr - 1 waehlt fuer 2015 den Jahrgang 2014, fuer 2020
# den von 2019 und so fort. 2009 ist nur der Rueckfall fuer Jahre vor 2015
# und damit fuer diese Arbeit ohne Belang - er faellt aus den Spannen heraus,
# sonst zoege ein nie benutzter Jahrgang die Aussage nach unten.
GENUTZTE_JAHRGAENGE = ACS_YEARS[1:]


def z(wert: float, n: int = 1) -> str:
    """Deutsche Schreibweise: Punkt als Tausender-, Komma als Dezimaltrenner."""
    return f"{wert:,.{n}f}".replace(",", "#").replace(".", ",").replace("#", ".")


def main() -> int:
    ZIEL.mkdir(parents=True, exist_ok=True)
    t: list[str] = ["# Rohdatenbefunde", "",
                    "Erzeugt von `prep/rohbefunde.py` aus `data/raw/`.", ""]

    # ---- 1  Einsatzmeldungen ---------------------------------------------
    f = pd.read_parquet(ROH / "fire_incidents.parquet",
                        columns=["incident_number", "alarm_dttm"])
    alarm = pd.to_datetime(f["alarm_dttm"])
    monat = alarm.dt.year * 100 + alarm.dt.month
    im_fenster = int(((monat >= START) & (monat <= ENDE)).sum())
    dubletten = int(f["incident_number"].duplicated().sum())
    t += ["## Einsatzmeldungen", "",
          f"- Meldungen gesamt: **{z(len(f), 0)}**, "
          f"Zeitraum {alarm.min():%Y-%m} bis {alarm.max():%Y-%m}",
          f"- im Analysezeitraum {START} bis {ENDE}: **{z(im_fenster, 0)}**",
          f"- doppelte Einsatznummern: **{z(dubletten, 0)}** "
          f"= {z(100 * dubletten / len(f), 2)} %", ""]

    # ---- 2  Parzellen ----------------------------------------------------
    lu = pd.read_parquet(ROH / "land_use_2020_raw.parquet", columns=["yrbuilt"])
    jahr = pd.to_numeric(lu["yrbuilt"], errors="coerce")
    ohne = int(jahr.where(jahr.between(1800, 2025)).isna().sum())
    t += ["## Parzellenverzeichnis", "",
          f"- Eintraege: **{z(len(lu), 0)}**",
          f"- ohne plausibles Baujahr: **{z(ohne, 0)}** "
          f"= {z(100 * ohne / len(lu), 1)} %", ""]

    # ---- 3  ACS-Jahrgaenge -----------------------------------------------
    zeilen = []
    for jg in ACS_YEARS:
        tr = pd.read_csv(ROH / f"acs_tracts_{jg}.csv")
        nb = pd.read_csv(PROC / f"acs_neighborhoods_{jg}.csv")
        uebrige = nb[~nb["neighborhood"].isin(PARKS)]["total_population"]
        park = nb[nb["neighborhood"].isin(PARKS)]["total_population"]
        ggp = nb[nb["neighborhood"] == "Golden Gate Park"]["total_population"]
        zeilen.append({
            "jahrgang": jg,
            "genutzt": jg in GENUTZTE_JAHRGAENGE,
            "tracts": len(tr),
            "mit_bildungsangabe": int(tr["bachelor_degree_count"].notna().sum()),
            "stadtteile": len(nb),
            "golden_gate_park": int(ggp.iloc[0]) if len(ggp) else None,
            "park_max": int(park.max()) if len(park) else None,
            "median_uebrige": int(uebrige.median()),
            "mission_bay": "Mission Bay" in set(nb["neighborhood"]),
        })
    acs = pd.DataFrame(zeilen)
    crosswalk = pd.read_csv(ROH / "crosswalk.csv")["geoid"].nunique()
    erste_mb = acs.loc[acs["mission_bay"], "jahrgang"].min()
    g = acs[acs["genutzt"]]
    t += ["## ACS-Jahrgaenge", "",
          "| Jahrgang | genutzt | Tracts | mit Bildungsangabe | Stadtteile | "
          "Golden Gate Park | groesstes Parkgebiet | Median der uebrigen |",
          "|---|:--:|---:|---:|---:|---:|---:|---:|"]
    for r in zeilen:
        strich = "--"
        t.append(f"| {r['jahrgang']} | {'ja' if r['genutzt'] else 'nein'} | "
                 f"{r['tracts']} | {r['mit_bildungsangabe']} | "
                 f"{r['stadtteile']} | "
                 f"{z(r['golden_gate_park'], 0) if r['golden_gate_park'] is not None else strich} | "
                 f"{z(r['park_max'], 0) if r['park_max'] is not None else strich} | "
                 f"{z(r['median_uebrige'], 0)} |")
    t += ["",
          f"- Zuordnungstabelle (Zensus 2020): **{crosswalk} Tracts**",
          f"- Mission Bay erscheint erstmals im Jahrgang **{erste_mb}**",
          "",
          "Die folgenden beiden Aussagen gelten in JEDEM genutzten Jahrgang "
          "und sind deshalb unabhaengig davon, welcher Jahrgang gerade "
          "angejoint ist:",
          "",
          f"- Der Golden Gate Park zaehlt nie mehr als "
          f"**{z(g['golden_gate_park'].max(), 0)}** Einwohner "
          f"(Spanne {z(g['golden_gate_park'].min(), 0)} bis "
          f"{z(g['golden_gate_park'].max(), 0)}).",
          f"- Der Median der uebrigen Stadtteile liegt nie unter "
          f"**{z(g['median_uebrige'].min(), 0)}**.", ""]

    (ZIEL / "rohbefunde.md").write_text("\n".join(t), encoding="utf-8")
    acs.to_csv(ZIEL / "rohbefunde_acs.csv", index=False)

    print("geschrieben nach results/deskriptiv/")
    print("  rohbefunde.md")
    print("  rohbefunde_acs.csv")
    print()
    print(f"Meldungen gesamt / im Fenster : {z(len(f), 0)} / {z(im_fenster, 0)}")
    print(f"Dubletten                     : {z(dubletten, 0)} "
          f"({z(100 * dubletten / len(f), 2)} %)")
    print(f"Parzellen ohne Baujahr        : {z(ohne, 0)} "
          f"({z(100 * ohne / len(lu), 1)} %)")
    print(f"Tracts je Jahrgang            : "
          f"{' -> '.join(str(x) for x in acs['tracts'])}")
    print(f"Golden Gate Park, genutzte Jg.: "
          f"{z(g['golden_gate_park'].min(), 0)} bis "
          f"{z(g['golden_gate_park'].max(), 0)} Einwohner")
    print()
    print("Fertig.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
