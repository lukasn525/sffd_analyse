"""
Rohdatenbefunde der Quellen, wie sie vom Portal kommen.

    python prep/rohbefunde.py

Output:  results/deskriptiv/rohbefunde.md

  - Gegenstueck zu deskriptiv.py (aufbereiteter Datensatz)
  - nur Befunde mit Folge fuer die Aufbereitung:
      Meldungen gesamt / im Analysezeitraum -> Umfang, Zeitraumwahl
      Dubletten nach Einsatznummer          -> Dedup
      Parzellen ohne Baujahr                -> Nenner yrbuilt_count statt
                                               parcel_count
      ACS-Jahrgang 2009 ohne B15003         -> Analysebeginn 2015
      Tracts je Jahrgang gegen Crosswalk    -> Trefferquoten, Rueckfall ueber
                                               den Basiscode der Tract-Nummer
      Einwohner der Parkgebiete             -> Ausschluss der drei Parks
      erster Jahrgang mit Mission Bay       -> Ausschluss der drei Stadtteile
                                               ohne durchgaengige Abdeckung
  - bewusst nicht: Antwortzeit (Ergebnisvariable, geht nie in die Analyse),
    fehlende Medianwerte einzelner Tracts (kein Eingriff)
  - Parkgebiete als Spanne: Maximum ueber die genutzten Jahrgaenge gegen das
    Minimum des Medians der uebrigen Stadtteile (gilt in jedem Jahrgang)
"""


from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

WURZEL = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WURZEL))
sys.path.insert(0, str(WURZEL / "prep"))

from prep.config import ACS_YEARS, START, ENDE  # noqa: E402
from s1_daten import tract_zu_stadtteil  # noqa: E402

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
    # Zuordnung der Tracts wie in der Aufbereitung: erst ueber die volle
    # Kennung, sonst ueber den vierstelligen Basiscode. Mehrdeutig ist ein
    # Basiscode, dessen Nachfolger in verschiedenen Stadtteilen liegen.
    cw = pd.read_csv(ROH / "crosswalk.csv", dtype={"geoid": str})
    cw_kennungen = set(cw["geoid"])
    stadtteile_je_basis = (cw.assign(basis=cw["geoid"].str[5:9])
                             .groupby("basis")["neighborhood"].nunique())
    zeilen = []
    for jg in ACS_YEARS:
        tr = pd.read_csv(ROH / f"acs_tracts_{jg}.csv")
        nb = pd.read_csv(PROC / f"acs_neighborhoods_{jg}.csv")
        uebrige = nb[~nb["neighborhood"].isin(PARKS)]["total_population"]
        park = nb[nb["neighborhood"].isin(PARKS)]["total_population"]
        ggp = nb[nb["neighborhood"] == "Golden Gate Park"]["total_population"]
        kennung = tr["geoid"].astype(str).str.zfill(11)
        direkt = kennung.isin(cw_kennungen)
        zugeordnet = tract_zu_stadtteil(tr["geoid"], cw).notna()
        bev = pd.to_numeric(tr["total_population"], errors="coerce").fillna(0)
        n_je_basis = stadtteile_je_basis.reindex(kennung[~direkt].str[5:9])
        zuordnung = {
            "tracts_direkt": int(direkt.sum()),
            "tracts_basiscode": int((zugeordnet & ~direkt).sum()),
            "tracts_basiscode_mehrdeutig": int((n_je_basis > 1).sum()),
            "tracts_offen": int((~zugeordnet).sum()),
            "einwohner_offen": int(bev[~zugeordnet].sum()),
            "bev_anteil_ohne_basiscode": round(float(bev[~direkt].sum()
                                                     / bev.sum()), 6),
        }
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
            **zuordnung,
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

    t += ["## Zuordnung der Tracts zu Stadtteilen", "",
          "Erst ueber die volle Kennung, sonst ueber den vierstelligen "
          "Basiscode der Tract-Nummer (`prep/s1_daten.py`, "
          "`tract_zu_stadtteil`). Die letzte Spalte nennt den Anteil der "
          "Wohnbevoelkerung, der ohne den Basiscode keinen Stadtteil erhielte.",
          "",
          "| Jahrgang | Tracts | direkt | ueber Basiscode | Basiscode "
          "mehrdeutig | offen | Einwohner offen | zugeordnet | ohne Basiscode "
          "fehlte |",
          "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in zeilen:
        n_zugeordnet = r["tracts_direkt"] + r["tracts_basiscode"]
        t.append(f"| {r['jahrgang']} | {r['tracts']} | {r['tracts_direkt']} "
                 f"({z(100 * r['tracts_direkt'] / r['tracts'], 1)} %) | "
                 f"{r['tracts_basiscode']} | "
                 f"{r['tracts_basiscode_mehrdeutig']} | {r['tracts_offen']} | "
                 f"{z(r['einwohner_offen'], 0)} | "
                 f"{z(100 * n_zugeordnet / r['tracts'], 1)} % | "
                 f"{z(100 * r['bev_anteil_ohne_basiscode'], 1)} % |")
    t.append("")

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
    print(f"davon direkt zugeordnet       : "
          f"{' -> '.join(str(x) for x in acs['tracts_direkt'])}")
    print(f"davon ueber den Basiscode     : "
          f"{' -> '.join(str(x) for x in acs['tracts_basiscode'])}")
    print(f"Golden Gate Park, genutzte Jg.: "
          f"{z(g['golden_gate_park'].min(), 0)} bis "
          f"{z(g['golden_gate_park'].max(), 0)} Einwohner")
    print()
    print("Fertig.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
