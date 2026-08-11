"""
Codebook - die eine grosse Merkmalstabelle fuer Kapitel 4.

    python tools/codebook.py            erzeugt die Tabelle
    python tools/codebook.py -v         zusaetzlich die Spalten je Datensatz

Ausgang: results/codebook/merkmale.csv · merkmale.md

NICHT TEIL DER ABGABE - das SKRIPT. Die erzeugte Tabelle schon: Sie gehoert in
Kapitel 4. Der Ordner `tools/` wird vor dem Packen geloescht, `results/` nicht;
deshalb ist die Ausgabe bewusst selbsttragend und enthaelt alles, was die
Tabelle im Text braucht.

Auflage Schroeter vom 10.08.2026, woertlich: "Codebook und Variablenbuch:
Skalenniveau ... Jedes Merkmal in einer Tabelle auflisten (Wertebereich etc.)
eine grosse Tabelle. Was, wie, wofuer" - und ausdruecklich: NICHT fuer jedes
Merkmal eine eigene deskriptive Statistik.

--------------------------------------------------------------------------
DIE AUFTEILUNG, AUF DER DAS SKRIPT BERUHT
--------------------------------------------------------------------------
Eine Haelfte der Tabelle ist GEMESSEN, die andere BEHAUPTET:

  gemessen     Wertebereich, Zeilenzahl, fehlende Werte, Zahl der
               Auspraegungen, in welchem Datensatz die Spalte steht. Entsteht
               bei jedem Lauf neu aus den Parquet-Dateien und kann deshalb
               nicht veralten.

  behauptet    Skalenniveau, Einheit, Quelle, Was/Wie/Wofuer. Das steht so in
               keiner Datei und muss von Hand gepflegt werden - unten in META.

Die Trennung ist der Zweck der Uebung. Waeren die Wertebereiche abgeschrieben,
waeren sie beim naechsten Pipeline-Lauf still falsch.

--------------------------------------------------------------------------
DIE WAECHTERFUNKTION
--------------------------------------------------------------------------
Das Skript bricht mit Exit-Code 1 ab, wenn

  - eine Spalte in den Parquet-Dateien steht, aber nicht in META
    -> ein neues Merkmal waere sonst stillschweigend undokumentiert
  - ein META-Eintrag auf keine Spalte passt
    -> ein entferntes Merkmal wuerde sonst weiter in Kapitel 4 stehen

Damit ist die Tabelle nicht nur einmal richtig, sondern bleibt es.

--------------------------------------------------------------------------
EIN BEFUND, DER IN DIE TABELLE GEHOERT
--------------------------------------------------------------------------
Fuenf Spalten tragen die Endung `_pct`, enthalten aber ANTEILE von 0 bis 1,
keine Prozentwerte: `armutsquote_pct` steht auf 0,36 und meint 36 %. Wer den
Namen liest statt den Wertebereich, berichtet den Faktor 100 falsch. Die
Spalte "Einheit" weist das deshalb ausdruecklich aus.

Umbenannt wird nichts - die Namen stehen in den fertigen Parquet-Dateien und
in allen bisherigen Ergebnissen. Dokumentiert wird es.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "prep"))

from config import PFAD_KLASSIFIKATION, PFAD_REGRESSION, RESULTS_DIR  # noqa: E402

OUT = RESULTS_DIR / "codebook"

ANTEIL = "Anteil 0–1"      # NICHT Prozent - siehe Kopf
ROLLE_KEIN = "kein Modellmerkmal"


@dataclass(frozen=True)
class Meta:
    """Was nicht in den Daten steht und von Hand gepflegt wird.

    `schluessel` markiert Spalten, deren Zahl eine KENNUNG ist und keine Menge:
    Jahre und Zeitschluessel bekommen keinen Tausenderpunkt, sonst stuende dort
    "2.015 bis 2.025" statt "2015 bis 2025".
    """
    skala: str
    einheit: str
    quelle: str
    was: str
    wie: str
    wofuer: str
    schluessel: bool = False


def M(skala, einheit, quelle, was, wie, wofuer, schluessel=False) -> Meta:
    return Meta(skala, einheit, quelle, was, wie, wofuer, schluessel)


# ==========================================================================
# META - die behauptete Haelfte. Reihenfolge = Reihenfolge in der Tabelle.
# ==========================================================================
META: dict[str, Meta] = {
    # ---- Schluessel der Analyseeinheit ------------------------------------
    "stadtteil": M(
        "nominal", "–", "DataSF, Analysis Neighborhoods",
        "Stadtteil nach der offiziellen Abgrenzung San Franciscos",
        "Punktverortung jedes Einsatzes im Stadtteilpolygon",
        f"Analyseeinheit und Gruppierungsvariable des Splits; {ROLLE_KEIN}"),
    "jahr": M(
        "intervall", "Kalenderjahr", "abgeleitet aus dem Alarmzeitpunkt",
        "Kalenderjahr der Zeile",
        "Jahresanteil des Alarmzeitpunkts",
        f"Zuordnung des ACS-Jahrgangs mit Publikationsversatz; {ROLLE_KEIN}",
        schluessel=True),
    "monat": M(
        "ordinal, zyklisch", "1–12", "abgeleitet aus dem Alarmzeitpunkt",
        "Kalendermonat der Zeile",
        "Monatsanteil des Alarmzeitpunkts",
        f"Grundlage von monat_sin und monat_cos; {ROLLE_KEIN}"),
    "jahr_monat": M(
        "intervall", "JJJJMM", "abgeleitet",
        "Zeitschluessel der Analyseeinheit",
        "jahr · 100 + monat",
        f"Sortierung, Lag-Bildung, Zuschnitt des Analysezeitraums; {ROLLE_KEIN}",
        schluessel=True),

    # ---- Zielgroessen -----------------------------------------------------
    "anzahl_einsaetze": M(
        "absolut, Zaehldaten", "Einsaetze je Monat", "SFFD Fire Incidents",
        "Zahl der Feuerwehreinsaetze im Stadtteil-Monat",
        "Zaehlung der verorteten Einsatzsaetze je Stadtteil und Monat",
        "ZIELGROESSE des Mengenstrangs"),
    "einsaetze_je_1000_ew": M(
        "verhaeltnis", "Einsaetze je 1.000 Einwohner", "abgeleitet",
        "auf die Wohnbevoelkerung normierte Einsatzlast",
        "anzahl_einsaetze / gesamtbevoelkerung · 1.000",
        "ZIELGROESSE des Mengenstrangs"),
    "dominante_einsatzart": M(
        "nominal, 4 Klassen", "–", "abgeleitet aus den NFIRS-Codes",
        "haeufigste Einsatzart des Stadtteil-Monats",
        "Maximum ueber die vier Anteilsspalten (argmax)",
        "ZIELGROESSE des Strukturstrangs"),

    # ---- Praediktoren, soziooekonomisch -----------------------------------
    "median_haushaltseinkommen": M(
        "verhaeltnis", "USD je Jahr", "ACS 5-Jahres, B19013_001E",
        "Medianeinkommen der Haushalte",
        "flaechengewichtete Aggregation der Census Tracts auf Stadtteile",
        "Praediktor, soziooekonomisch"),
    "armutsquote_pct": M(
        "verhaeltnis", ANTEIL, "ACS 5-Jahres, B17001",
        "Anteil der Bevoelkerung unter der Armutsgrenze",
        "poverty_below / poverty_universe_total",
        "Praediktor, soziooekonomisch"),
    "akademikerquote_pct": M(
        "verhaeltnis", ANTEIL, "ACS 5-Jahres, B15003",
        "Anteil mit mindestens Bachelor-Abschluss",
        "bachelor_degree_count / education_universe_total",
        "Praediktor, soziooekonomisch"),
    "median_miete": M(
        "verhaeltnis", "USD je Monat", "ACS 5-Jahres, B25064_001E",
        "Median der Bruttomiete",
        "flaechengewichtete Aggregation der Census Tracts",
        "Praediktor, soziooekonomisch"),
    "leerstandsquote_pct": M(
        "verhaeltnis", ANTEIL, "ACS 5-Jahres, B25002",
        "Anteil leerstehender Wohneinheiten",
        "vacant_housing_units / total_housing_units",
        "Praediktor, soziooekonomisch"),

    # ---- Praediktoren, Groessenkontrolle und Kriminalitaet ----------------
    "log_bevoelkerung": M(
        "intervall (logarithmiert)", "ln(Personen)", "ACS 5-Jahres, B01003_001E",
        "logarithmierte Wohnbevoelkerung des Stadtteils",
        "ln(gesamtbevoelkerung)",
        "Praediktor, Groessenkontrolle (Decision Log #13)"),
    "log_kriminalitaetsindex": M(
        "intervall (logarithmiert)", "ln(Index), 0 = Stadtdurchschnitt",
        "SFPD Incident Reports",
        "relative Kriminalitaetsbelastung des Stadtteils",
        "ln(kriminalitaetsindex); der Index ist ein Location Quotient ueber ein "
        "rollierendes 12-Monats-Fenster, das im VORMONAT endet",
        "Praediktor, kriminalitaetsbezogen"),

    # ---- Praediktoren, baulich -------------------------------------------
    "anteil_altbau_vor_1940_pct": M(
        "verhaeltnis", ANTEIL, "DataSF Land Use 2020",
        "Anteil der Parzellen mit Baujahr vor 1940",
        "pre1940_count / yrbuilt_count",
        "Praediktor, baulich"),
    "anteil_wohngebaeude_pct": M(
        "verhaeltnis", ANTEIL, "DataSF Land Use 2020",
        "Anteil der Wohnparzellen",
        "residential_count / parcel_count",
        "Praediktor, baulich"),
    "anteil_risikogewerbe_pct": M(
        "verhaeltnis", ANTEIL, "DataSF Land Use 2020",
        "Flaechenanteil brandlastreicher Gewerbenutzung",
        "Flaeche der Kategorien RETAIL/ENT und PDR / Gesamtflaeche",
        "Praediktor, baulich"),

    # ---- Praediktoren, Saison --------------------------------------------
    "monat_sin": M(
        "intervall", "−1 bis 1", "abgeleitet",
        "Sinuskomponente des Kalendermonats",
        "sin(2π · monat / 12)",
        "Praediktor, Saison; legt die Monate auf ein Zifferblatt"),
    "monat_cos": M(
        "intervall", "−1 bis 1", "abgeleitet",
        "Kosinuskomponente des Kalendermonats",
        "cos(2π · monat / 12)",
        "Praediktor, Saison; legt die Monate auf ein Zifferblatt"),

    # ---- Exposition und Rohwerte -----------------------------------------
    "gesamtbevoelkerung": M(
        "absolut", "Personen", "ACS 5-Jahres, B01003_001E",
        "Wohnbevoelkerung des Stadtteils",
        "flaechengewichtete Aggregation der Census Tracts",
        "EXPOSITION: Offset des Poisson-GLM und Ruecktransformation der Rate; "
        f"{ROLLE_KEIN}"),
    "kriminalitaetsindex": M(
        "verhaeltnis", "Location Quotient, 1 = Stadtdurchschnitt",
        "SFPD Incident Reports",
        "Rohwert zu log_kriminalitaetsindex",
        "Delikte je Einwohner im Stadtteil / Delikte je Einwohner der Stadt, "
        "rollierendes 12-Monats-Fenster endend im Vormonat",
        f"Deskription in Kapitel 4; {ROLLE_KEIN}"),

    # ---- Vergangenheitswerte, bewusst NICHT im Modell ---------------------
    "lag_1": M(
        "absolut, Zaehldaten", "Einsaetze je Monat", "abgeleitet",
        "Einsatzzahl des Vormonats im selben Stadtteil",
        "shift(1) je Stadtteil, strikt rueckwaertsgerichtet",
        f"Deskription der zeitlichen Struktur; {ROLLE_KEIN} (Decision Log #29)"),
    "lag_12": M(
        "absolut, Zaehldaten", "Einsaetze je Monat", "abgeleitet",
        "Einsatzzahl des Vorjahresmonats im selben Stadtteil",
        "shift(12) je Stadtteil, strikt rueckwaertsgerichtet",
        f"Deskription der zeitlichen Struktur; {ROLLE_KEIN} (Decision Log #29)"),
    "rolling_mean_3": M(
        "verhaeltnis", "Einsaetze je Monat", "abgeleitet",
        "Mittel der drei Vormonate im selben Stadtteil",
        "shift(1) VOR rolling(3).mean() - nie der eigene Monat",
        f"Deskription der zeitlichen Struktur; {ROLLE_KEIN} (Decision Log #29)"),

    # ---- Aufteilung -------------------------------------------------------
    "fold": M(
        "nominal", "0–5", "prep/s2_datensaetze.py",
        "Fold-Zuteilung des Stadtteils; 0 sind die Hold-out-Stadtteile",
        "Sortierung nach brand-dominierten Monaten, bei Gleichstand nach "
        "Bevoelkerung, danach reihum auf sechs Gruppen",
        "VALIDIERUNGSRAHMEN: steht als Spalte in der Datei, damit alle "
        "Verfahren zwingend dieselben Folds sehen"),
    "ist_holdout": M(
        "nominal, binaer", "0 / 1", "prep/s2_datensaetze.py",
        "Gehoert der Stadtteil zum Hold-out?",
        "1 fuer die sechs Stadtteile der Gruppe 0",
        "Sperrt sechs Stadtteile bis zur einmaligen Schlussbewertung"),

    # ---- Nur im Klassifikationsdatensatz ----------------------------------
    "anzahl_brand": M(
        "absolut, Zaehldaten", "Einsaetze je Monat", "SFFD, NFIRS-Serie 100",
        "Zahl der Brandeinsaetze im Stadtteil-Monat",
        "Zaehlung nach fuehrender Ziffer des NFIRS-Codes",
        f"Nenner der Anteilsbildung und Deskription; {ROLLE_KEIN}"),
    "anzahl_rettung_ems": M(
        "absolut, Zaehldaten", "Einsaetze je Monat", "SFFD, NFIRS-Serie 300",
        "Zahl der Rettungsdiensteinsaetze im Stadtteil-Monat",
        "Zaehlung nach fuehrender Ziffer des NFIRS-Codes",
        f"Nenner der Anteilsbildung und Deskription; {ROLLE_KEIN}"),
    "anzahl_technische_hilfe": M(
        "absolut, Zaehldaten", "Einsaetze je Monat",
        "SFFD, NFIRS-Serien 200/400/500/800/900",
        "Zahl der technischen Hilfeleistungen und Gefahrenlagen",
        "Zaehlung nach fuehrender Ziffer des NFIRS-Codes",
        f"Nenner der Anteilsbildung und Deskription; {ROLLE_KEIN}"),
    "anzahl_fehlalarm": M(
        "absolut, Zaehldaten", "Einsaetze je Monat",
        "SFFD, NFIRS-Serien 600/700",
        "Zahl der Fehlalarme und Good-Intent-Einsaetze",
        "Zaehlung nach fuehrender Ziffer des NFIRS-Codes",
        f"Nenner der Anteilsbildung und Deskription; {ROLLE_KEIN}"),
    "anteil_brand": M(
        "verhaeltnis", ANTEIL, "abgeleitet",
        "Anteil der Brandeinsaetze an allen Einsaetzen des Stadtteil-Monats",
        "anzahl_brand / anzahl_einsaetze",
        f"Grundlage der Zielgroesse (argmax); {ROLLE_KEIN}"),
    "anteil_rettung_ems": M(
        "verhaeltnis", ANTEIL, "abgeleitet",
        "Anteil der Rettungsdiensteinsaetze am Stadtteil-Monat",
        "anzahl_rettung_ems / anzahl_einsaetze",
        f"Grundlage der Zielgroesse (argmax); {ROLLE_KEIN}"),
    "anteil_technische_hilfe": M(
        "verhaeltnis", ANTEIL, "abgeleitet",
        "Anteil der technischen Hilfeleistungen am Stadtteil-Monat",
        "anzahl_technische_hilfe / anzahl_einsaetze",
        f"Grundlage der Zielgroesse (argmax); {ROLLE_KEIN}"),
    "anteil_fehlalarm": M(
        "verhaeltnis", ANTEIL, "abgeleitet",
        "Anteil der Fehlalarme am Stadtteil-Monat",
        "anzahl_fehlalarm / anzahl_einsaetze",
        f"Grundlage der Zielgroesse (argmax); {ROLLE_KEIN}"),
}


# ==========================================================================
# Die gemessene Haelfte
# ==========================================================================
def _de(x: float, stellen: int = 2) -> str:
    """Deutsche Zahlformatierung mit Tausenderpunkt."""
    return f"{x:,.{stellen}f}".replace(",", " ").replace(".", ",").replace(" ", ".")


def spanne(s: pd.Series, schluessel: bool = False) -> str:
    """Wertebereich - bei Zahlen min bis max, sonst die Auspraegungen."""
    if pd.api.types.is_numeric_dtype(s):
        stellen = 0 if (s.dropna() % 1 == 0).all() else (
            4 if s.abs().max() < 10 else 2)
        if schluessel:
            return f"{s.min():.0f} bis {s.max():.0f}"
        return f"{_de(s.min(), stellen)} bis {_de(s.max(), stellen)}"
    werte = sorted(map(str, s.dropna().unique()))
    if len(werte) <= 4:
        return " · ".join(werte)
    return f"{len(werte)} Auspraegungen"


def gemessen(datensaetze: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Je Spalte: Wertebereich, Zeilen, fehlende Werte, Auspraegungen, Herkunft.

    Steht eine Spalte in beiden Dateien, wird der REGRESSIONS-Datensatz
    ausgewiesen - er ist die Obermenge (die Klassifikation ist eine echte
    Teilmenge, Decision Log #31). Die Spalte "Datensatz" haelt fest, wo sie
    ueberhaupt vorkommt, damit der Unterschied sichtbar bleibt.
    """
    zeilen = []
    for name in META:
        vorkommen = [k for k, d in datensaetze.items() if name in d.columns]
        if not vorkommen:
            continue
        d = datensaetze[vorkommen[0]]
        s = d[name]
        zeilen.append({
            "merkmal": name,
            "datensatz": " + ".join(vorkommen),
            "typ": str(s.dtype),
            "wertebereich": spanne(s, META[name].schluessel),
            "n": len(s),
            "fehlend": int(s.isna().sum()),
            "auspraegungen": int(s.nunique()),
        })
    return pd.DataFrame(zeilen)


# ==========================================================================
def waechter(datensaetze: dict[str, pd.DataFrame]) -> list[str]:
    """Jede Spalte dokumentiert, jeder Eintrag belegt - sonst Abbruch."""
    vorhanden = {c for d in datensaetze.values() for c in d.columns}
    fehlt = [c for c in sorted(vorhanden) if c not in META]
    verwaist = [k for k in META if k not in vorhanden]

    meldungen = []
    for c in fehlt:
        meldungen.append(f"Spalte '{c}' steht in den Daten, aber nicht in META "
                         f"- undokumentiertes Merkmal")
    for k in verwaist:
        meldungen.append(f"META-Eintrag '{k}' passt auf keine Spalte - "
                         f"Merkmal entfernt oder umbenannt?")
    return meldungen


def baue() -> pd.DataFrame:
    datensaetze = {
        "Regression": pd.read_parquet(PFAD_REGRESSION),
        "Klassifikation": pd.read_parquet(PFAD_KLASSIFIKATION),
    }
    meldungen = waechter(datensaetze)
    if meldungen:
        print("ABBRUCH - Tabelle und Daten passen nicht zusammen:\n")
        for m in meldungen:
            print(f"  x {m}")
        raise SystemExit(1)

    mess = gemessen(datensaetze)
    fest = pd.DataFrame([{"merkmal": k, "skalenniveau": v.skala,
                          "einheit": v.einheit, "quelle": v.quelle,
                          "was": v.was, "wie": v.wie, "wofuer": v.wofuer}
                         for k, v in META.items()])
    df = mess.merge(fest, on="merkmal", how="left")
    return df[["merkmal", "skalenniveau", "einheit", "wertebereich",
               "auspraegungen", "fehlend", "quelle", "was", "wie", "wofuer",
               "datensatz", "typ", "n"]]


def als_markdown(df: pd.DataFrame) -> str:
    """Eine grosse Tabelle, wie verlangt - plus zwei Saetze Lesehilfe."""
    kopf = ["Merkmal", "Skalenniveau", "Einheit", "Wertebereich", "Quelle",
            "Was", "Wie", "Wofuer"]
    spalten = ["merkmal", "skalenniveau", "einheit", "wertebereich", "quelle",
               "was", "wie", "wofuer"]

    zeilen = [f"# Codebook - Merkmalsuebersicht", "",
              f"Stand {pd.Timestamp.today():%Y-%m-%d}. Erzeugt aus "
              f"`data/processed/regression.parquet` und "
              f"`klassifikation.parquet`.", "",
              f"{len(df)} Spalten, davon "
              f"{int((df['wofuer'].str.startswith('Praediktor')).sum())} "
              f"Praediktoren und "
              f"{int((df['wofuer'].str.startswith('ZIELGROESSE')).sum())} "
              f"Zielgroessen. Fehlende Werte: "
              f"{int(df['fehlend'].sum())} in der gesamten Tabelle.", "",
              "**Zur Einheit:** Fuenf Spalten tragen die Endung `_pct`, "
              "enthalten aber Anteile von 0 bis 1 und keine Prozentwerte. "
              "`armutsquote_pct` = 0,36 bedeutet 36 %. Die Spalte Einheit ist "
              "massgeblich, nicht der Name.", "",
              "| " + " | ".join(kopf) + " |",
              "|" + "---|" * len(kopf)]
    for _, z in df.iterrows():
        felder = [str(z[s]).replace("|", "/") for s in spalten]
        felder[0] = f"`{felder[0]}`"
        zeilen.append("| " + " | ".join(felder) + " |")
    return "\n".join(zeilen) + "\n"


def main(argv: list[str]) -> int:
    for pfad in (PFAD_REGRESSION, PFAD_KLASSIFIKATION):
        if not pfad.exists():
            raise SystemExit(f"{pfad.relative_to(ROOT)} fehlt - erst "
                             f"'python prep/build.py' ausfuehren.")
    OUT.mkdir(parents=True, exist_ok=True)
    df = baue()

    df.to_csv(OUT / "merkmale.csv", index=False)
    (OUT / "merkmale.md").write_text(als_markdown(df), encoding="utf-8")

    print(f"\n  {len(df)} Merkmale dokumentiert, "
          f"{int(df['fehlend'].sum())} fehlende Werte insgesamt.\n")
    rollen = df["wofuer"].str.split(",").str[0].str.split(";").str[0]
    for rolle, n in rollen.value_counts().items():
        print(f"    {rolle:<48}{n:>3}")

    if "-v" in argv:
        print("\n  Je Datensatz:")
        for datensatz, g in df.groupby("datensatz"):
            print(f"    {datensatz:<28}{len(g):>3} Spalten")

    print(f"\n  => {(OUT / 'merkmale.md').relative_to(ROOT)}")
    print(f"  => {(OUT / 'merkmale.csv').relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
