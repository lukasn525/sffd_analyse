"""
Wer sind die 30 und wer sind die 6? - Profil beider Panelhaelften.

    python vorpruefung/panelprofil.py

Eingang: data/processed/regression.parquet, data/processed/klassifikation.parquet,
         data/raw/neighborhoods.geojson
Ausgang: results/panelprofil/stadtteile.csv, klassenverteilung.csv,
         zielgroessen.csv, suchmenge.csv, nachbarschaft.csv, panelprofil.md

  - Der Validierungsrahmen haelt ganze Stadtteile zurueck. Eine
    Schlussbewertung auf sechs Einheiten ist ohne diese sechs Einheiten keine
    Aussage, sondern eine Zahl ohne Bezugsmenge. Deshalb steht die
    Zusammensetzung beider Haelften in einer eigenen Datei, erzeugt VOR der
    Auswertung und unabhaengig von jedem Modellergebnis
  - Die Zuteilung ist deterministisch: absteigend nach brand-dominierten
    Monaten, bei Gleichstand nach Bevoelkerung, dann reihum auf N_FOLDS + 1
    Gruppen; Gruppe 0 ist das Hold-out. Der auf dem
    Stratifizierungskriterium rangerste Stadtteil liegt damit ZWANGSLAEUFIG
    im Hold-out - keine Zufallsziehung, sondern eine Eigenschaft der Regel,
    und als solche zu berichten
  - Zwei Groessen entscheiden darueber, wie die Schlussbewertung zu lesen
    ist: die Klassenverteilung, weil Macro-F1 ueber vier Klassen mittelt
    von denen eine selten ist - und die Verteilung der Zielgroessen auf der
    RATE, weil das die Skala ist, auf der alle drei Verfahren angepasst
    werden
  - Zwei weitere Eigenschaften der Aufteilung stehen hier, weil sie nicht
    vom Modell abhaengen: wie viele Teststadtteile der Wiederholungen 1 bis 9
    schon in der Suchmenge von Wiederholung 0 lagen, und wie viele
    Trainingsstadtteile keine gemeinsame Grenze mit einem Teststadtteil haben
  - Rein deskriptiv. Kein Modell, kein Test, kein Zufall: zwei Laeufe
    liefern dieselbe Datei. Die Wiederholungen kommen aus v0_aufteilung.py
    mit festem Startwert. Haengt bewusst nicht an vorpruefung/run.py,
    weil es keine Voraussetzung fuer die Baselines ist

FALLSTRICKE
  1  Die Klassenverteilung kommt aus klassifikation.parquet (4.751 Zeilen),
     die Zielgroessen aus regression.parquet (4.752). Der Unterschied ist
     der eine Monat ohne Einsatz, der keine Zusammensetzung hat. Beide
     Zahlen nebeneinanderzustellen ohne das zu sagen waere ein stiller
     Bezugsmengenwechsel
  2  Anteile IMMER innerhalb der jeweiligen Haelfte bilden, nie am
     Gesamtpanel. Sonst liest sich der Brandanteil als Aussage ueber die
     Stadt statt ueber die Testmenge
  3  Der Dispersionsindex gehoert zur Anzahl, nicht zur Rate - er ist auf
     Zaehldaten definiert. Fuer die Rate steht die Standardabweichung da
  4  Modalklasse je Stadtteil ueber alle 132 Monate, nicht je Fold. Sonst
     haengt die Groesse, aus der v4_decke Decke B bildet, an der
     Fold-Zuteilung
  5  Die Spalte ist_holdout kommt aus der DATEI, nicht aus einer Rechnung
     in diesem Skript. Wer sie hier neu bestimmte, koennte still von der
     Aufteilung abweichen, gegen die m02 und m03 gesperrt sind
"""



from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "prep"))
sys.path.insert(0, str(ROOT / "modelle"))

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION)
from config_modelle import WIEDERHOLUNGEN  # noqa: E402
from s1_daten import neighborhoods_gdf  # noqa: E402
from v0_aufteilung import selten_je_stadtteil, wiederholte_aufteilung  # noqa: E402

OUT = ROOT / "results" / "panelprofil"

ZIELGROESSE = "anzahl_einsaetze"
RATE        = "einsaetze_je_1000_ew"
ZIELKLASSE  = "dominante_einsatzart"
KLASSEN     = ["brand", "rettung_ems", "technische_hilfe", "fehlalarm"]

HAELFTEN = {0: "Entwicklung", 1: "Hold-out"}


def lade() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Liest beide Datensaetze und prueft die gemeinsame Aufteilung.

    Ein:  nichts
    Aus:  (regression, klassifikation)

    - beide Dateien tragen fold und ist_holdout aus EINEM Aufruf von
      ergaenze_aufteilung(). Weichen sie ab, ist die Aufbereitung
      neu zu laufen - dieses Skript darf das nicht stillschweigend heilen
    """
    reg = pd.read_parquet(PFAD_REGRESSION)
    kl = pd.read_parquet(PFAD_KLASSIFIKATION)

    a = reg.groupby("stadtteil")["ist_holdout"].first()
    b = kl.groupby("stadtteil")["ist_holdout"].first()
    if not a.equals(b.reindex(a.index)):
        raise SystemExit("Die beiden Dateien tragen verschiedene "
                         "Hold-out-Zuteilungen - prep/build.py neu laufen.")
    return reg, kl


def stadtteile(reg: pd.DataFrame, kl: pd.DataFrame) -> pd.DataFrame:
    """Eine Zeile je Stadtteil: Zuteilung, Groesse, Einsatzlast, Klassenlage.

    Ein:  beide Datensaetze
    Aus:  Datenrahmen, absteigend nach brand-dominierten Monaten sortiert

    - die Sortierung bildet die Zuteilungsregel nach. Wer die Datei
      liest, sieht der Reihenfolge an, warum welcher Stadtteil in Gruppe 0
      steht
    - FALLSTRICK 4: Modalklasse ueber alle Monate des Stadtteils
    """
    g = reg.groupby("stadtteil")
    tab = pd.DataFrame({
        "fold": g["fold"].first(),
        "ist_holdout": g["ist_holdout"].first(),
        "n_monate": g.size(),
        "bevoelkerung_mittel": g[EXPOSURE_ROH].mean().round(0),
        "einsaetze_mittel": g[ZIELGROESSE].mean().round(1),
        "einsaetze_max": g[ZIELGROESSE].max(),
        "rate_mittel": g[RATE].mean().round(2),
    })

    k = kl.groupby("stadtteil")
    tab["monate_brand_dominiert"] = (
        kl.assign(_b=(kl[ZIELKLASSE] == "brand").astype(int))
          .groupby("stadtteil")["_b"].sum())
    tab["modalklasse"] = k[ZIELKLASSE].agg(lambda s: s.mode().iat[0])

    tab = tab.sort_values(["monate_brand_dominiert", "bevoelkerung_mittel"],
                          ascending=False)
    tab.insert(0, "rang_zuteilung", range(1, len(tab) + 1))
    return tab.reset_index()


def klassenverteilung(kl: pd.DataFrame) -> pd.DataFrame:
    """Klassenanteile je Panelhaelfte.

    Ein:  klassifikation.parquet
    Aus:  Datenrahmen mit einer Zeile je Haelfte und Klasse

    - FALLSTRICK 2: Anteil am Nenner der eigenen Haelfte
    """
    zeilen = []
    for flag, name in HAELFTEN.items():
        teil = kl[kl["ist_holdout"] == flag]
        n = len(teil)
        for klasse in KLASSEN:
            k = int((teil[ZIELKLASSE] == klasse).sum())
            zeilen.append({"haelfte": name, "klasse": klasse, "n_monate": k,
                           "anteil": round(k / n, 4) if n else float("nan"),
                           "n_haelfte": n})
    return pd.DataFrame(zeilen)


def zielgroessen(reg: pd.DataFrame) -> pd.DataFrame:
    """Verteilung beider Zielgroessen je Panelhaelfte.

    Ein:  regression.parquet
    Aus:  Datenrahmen mit einer Zeile je Haelfte und Zielgroesse

    - FALLSTRICK 3: Dispersionsindex nur fuer die Anzahl
    - die Rate steht mit dabei, weil auf ihr angepasst wird: eine
      Testmenge ohne die Extremwerte des Trainings ist eine andere Aufgabe
    """
    zeilen = []
    for flag, name in HAELFTEN.items():
        teil = reg[reg["ist_holdout"] == flag]
        for ziel in (ZIELGROESSE, RATE):
            s = teil[ziel].astype(float)
            zeilen.append({
                "haelfte": name, "zielgroesse": ziel,
                "n_zeilen": len(s), "n_stadtteile": teil["stadtteil"].nunique(),
                "mittel": round(s.mean(), 3), "sd": round(s.std(), 3),
                "min": round(s.min(), 3), "max": round(s.max(), 3),
                "dispersionsindex": (round(s.var() / s.mean(), 1)
                                     if ziel == ZIELGROESSE else ""),
            })
    return pd.DataFrame(zeilen)


def suchmenge(reg: pd.DataFrame, kl: pd.DataFrame) -> pd.DataFrame:
    """Anteil der Teststadtteile, die schon in der Suchmenge lagen.

    Ein:  beide Datensaetze
    Aus:  Datenrahmen mit einer Zeile je Wiederholung 1 bis 9 und Fold

    - die Parameter fuer Fold k werden in Wiederholung 0 auf dessen
      Trainingsstadtteilen gesucht und in allen Wiederholungen fuer Fold k
      verwendet
    - gezaehlt wird, wie viele Teststadtteile von Fold k einer spaeteren
      Wiederholung zu dieser Suchmenge gehoerten
    """
    selten = selten_je_stadtteil(kl)

    def zuteilung(w: int) -> pd.Series:
        d = wiederholte_aufteilung(reg, wiederholung=w, selten=selten)
        return d[d["ist_holdout"] == 0].groupby("stadtteil")["fold"].first()

    basis = zuteilung(0)
    zeilen = []
    for w in range(1, WIEDERHOLUNGEN):
        jetzt = zuteilung(w)
        for k in range(1, N_FOLDS + 1):
            test = set(jetzt[jetzt == k].index)
            gesucht = set(basis[basis != k].index)
            zeilen.append({"wiederholung": w, "fold": k,
                           "teststadtteile": len(test),
                           "davon_in_suchmenge": len(test & gesucht),
                           "anteil": round(len(test & gesucht) / len(test), 4)})
    return pd.DataFrame(zeilen)


def nachbarschaft(reg: pd.DataFrame, kl: pd.DataFrame) -> pd.DataFrame:
    """Trainingsstadtteile ohne gemeinsame Grenze mit einem Teststadtteil.

    Ein:  beide Datensaetze, dazu die Stadtteilgeometrie aus data/raw
    Aus:  Datenrahmen mit einer Zeile je Wiederholung und Fold

    - zeigt, wie viele Trainingsstadtteile ein Puffer um die Teststadtteile
      uebrig liesse: ausgeschlossen wird jeder Trainingsstadtteil, der einen
      Teststadtteil beruehrt
    - dieselbe Geometrie wie in der Aufbereitung (s1_daten.neighborhoods_gdf)
    """
    geo = neighborhoods_gdf().set_index("neighborhood")["geometry"]
    selten = selten_je_stadtteil(kl)
    zeilen = []
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(reg, wiederholung=w, selten=selten)
        f = d[d["ist_holdout"] == 0].groupby("stadtteil")["fold"].first()
        for k in range(1, N_FOLDS + 1):
            test = geo.loc[f[f == k].index].union_all()
            train = f[f != k].index
            frei = [s for s in train if not geo.loc[s].intersects(test)]
            zeilen.append({"wiederholung": w, "fold": k,
                           "trainingsstadtteile": len(train),
                           "ohne_gemeinsame_grenze": len(frei)})
    return pd.DataFrame(zeilen)


def _md(df: pd.DataFrame) -> str:
    """Datenrahmen als Markdown-Tabelle. Reine Formatierung."""
    kopf = "| " + " | ".join(df.columns) + " |"
    linie = "|" + "|".join(["---"] * len(df.columns)) + "|"
    zeilen = ["| " + " | ".join(str(v) for v in r) + " |"
              for r in df.itertuples(index=False)]
    return "\n".join([kopf, linie, *zeilen])


def bericht(st: pd.DataFrame, kv: pd.DataFrame, zg: pd.DataFrame,
            sm: pd.DataFrame, nb: pd.DataFrame) -> str:
    """Setzt die Tabellen zu panelprofil.md zusammen.

    Ein:  Stadtteilprofil, Klassenverteilung, Zielgroessenverteilung,
          Suchmenge, Nachbarschaft
    Aus:  Markdown-Text

    - reine Formatierung, hier wird nichts gerechnet
    - die Zuteilungsregel steht im Kopf, damit die Datei ohne den Code
      lesbar bleibt
    """
    hold = st[st["ist_holdout"] == 1]["stadtteil"].tolist()
    b_hold = int(st[st["ist_holdout"] == 1]["monate_brand_dominiert"].sum())
    b_dev = int(st[st["ist_holdout"] == 0]["monate_brand_dominiert"].sum())
    erster = st.iloc[0]
    nb0 = nb[nb["wiederholung"] == 0]

    return "\n".join([
        "# Profil der beiden Panelhaelften",
        "",
        "Erzeugt von `vorpruefung/panelprofil.py`. Rein deskriptiv, kein Modell.",
        "",
        "Zuteilung: absteigend nach brand-dominierten Monaten, bei",
        "Gleichstand nach Bevoelkerung, dann reihum auf sechs Gruppen; Gruppe 0",
        "ist das Hold-out.",
        "",
        "## Zurueckgehaltene Stadtteile",
        "",
        f"{', '.join(hold)}.",
        "",
        f"Rang 1 der Zuteilungsordnung ist {erster['stadtteil']} mit "
        f"{int(erster['monate_brand_dominiert'])} brand-dominierten Monaten. "
        "Rang 1 faellt nach der Regel immer in Gruppe 0 und liegt damit "
        "konstruktionsbedingt im Hold-out.",
        "",
        f"Von {b_dev + b_hold} brand-dominierten Monaten des Panels liegen "
        f"{b_hold} im Hold-out und {b_dev} in der Entwicklung.",
        "",
        "## Klassenverteilung",
        "",
        _md(kv),
        "",
        "## Zielgroessen",
        "",
        _md(zg),
        "",
        "## Stadtteile",
        "",
        _md(st),
        "",
        "## Suchmenge und spaetere Wiederholungen",
        "",
        "Die Parameter fuer Fold k werden in Wiederholung 0 auf dessen "
        "Trainingsstadtteilen gesucht. In den Wiederholungen 1 bis 9 lagen im "
        f"Mittel {sm['anteil'].mean():.1%} der Teststadtteile eines Folds in "
        f"dieser Suchmenge (Spanne {sm['anteil'].min():.1%} bis "
        f"{sm['anteil'].max():.1%} je Fold). Einzelwerte in `suchmenge.csv`.",
        "",
        "## Raeumliche Nachbarschaft der Testfolds",
        "",
        "Trainingsstadtteile ohne gemeinsame Grenze mit einem Teststadtteil, "
        "also was ein Puffer um die Teststadtteile uebrig liesse. Im Mittel "
        f"{nb0['ohne_gemeinsame_grenze'].mean():.1f} statt "
        f"{nb0['trainingsstadtteile'].mean():.0f} in Wiederholung 0, "
        f"{nb['ohne_gemeinsame_grenze'].mean():.1f} ueber alle "
        f"{len(nb)} Laeufe. Einzelwerte in `nachbarschaft.csv`.",
        "",
        _md(nb0),
        "",
    ])


def main(argv: list[str]) -> int:
    """Schreibt fuenf CSV-Dateien und die Lesefassung.

    Ein:  keine Argumente
    Aus:  Exitcode

    - anders als m02/m03/v4 gibt es hier KEINE Hold-out-Sperre: das Skript
      beschreibt die Aufteilung, es bewertet kein Modell auf ihr. Genau
      deshalb darf und muss es beide Haelften sehen
    """
    OUT.mkdir(parents=True, exist_ok=True)
    reg, kl = lade()

    st = stadtteile(reg, kl)
    kv = klassenverteilung(kl)
    zg = zielgroessen(reg)
    sm = suchmenge(reg, kl)
    nb = nachbarschaft(reg, kl)

    print(st.to_string(index=False), "\n")
    print(kv.to_string(index=False), "\n")
    print(zg.to_string(index=False), "\n")
    print(f"  Teststadtteile in der Suchmenge von Wiederholung 0: "
          f"{sm['anteil'].mean():.1%} im Mittel")
    print(f"  Trainingsstadtteile ohne Grenze zum Testfold: "
          f"{nb.loc[nb['wiederholung'] == 0, 'ohne_gemeinsame_grenze'].mean():.1f} "
          f"(Wiederholung 0), {nb['ohne_gemeinsame_grenze'].mean():.1f} "
          f"(alle Laeufe)\n")

    st.to_csv(OUT / "stadtteile.csv", index=False)
    kv.to_csv(OUT / "klassenverteilung.csv", index=False)
    zg.to_csv(OUT / "zielgroessen.csv", index=False)
    sm.to_csv(OUT / "suchmenge.csv", index=False)
    nb.to_csv(OUT / "nachbarschaft.csv", index=False)
    (OUT / "panelprofil.md").write_text(bericht(st, kv, zg, sm, nb),
                                        encoding="utf-8")

    # Kontrollen:
    #   1  6 Stadtteile im Hold-out, 30 in der Entwicklung
    #   2  Klassenanteile summieren je Haelfte auf 1
    #   3  Zeilenzahlen 4.752 (Regression) und 4.751 (Klassifikation)
    #   4  brand-dominierte Monate beider Haelften ergeben die Gesamtzahl
    n_hold = int((st["ist_holdout"] == 1).sum())
    n_dev = int((st["ist_holdout"] == 0).sum())
    if (n_hold, n_dev) != (6, 30):
        print(f"  WARNUNG: {n_hold} Hold-out- und {n_dev} "
              f"Entwicklungsstadtteile, erwartet 6 und 30.")
    for name, teil in kv.groupby("haelfte"):
        if abs(teil["anteil"].sum() - 1.0) > 0.001:
            print(f"  WARNUNG: Klassenanteile {name} summieren auf "
                  f"{teil['anteil'].sum():.4f}, erwartet 1.")
    if (len(reg), len(kl)) != (4752, 4751):
        print(f"  WARNUNG: {len(reg)} und {len(kl)} Zeilen, "
              f"erwartet 4752 und 4751.")
    gesamt = int((kl[ZIELKLASSE] == "brand").sum())
    if int(st["monate_brand_dominiert"].sum()) != gesamt:
        print("  WARNUNG: brand-dominierte Monate je Stadtteil summieren "
              "nicht auf die Gesamtzahl.")

    print("  Geschrieben: results/panelprofil/stadtteile.csv, "
          "klassenverteilung.csv, zielgroessen.csv, suchmenge.csv, "
          "nachbarschaft.csv, panelprofil.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
