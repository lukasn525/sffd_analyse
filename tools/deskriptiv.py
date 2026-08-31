"""
Deskriptive Attributuebersicht - der Befundteil von Kapitel 4.

    python tools/deskriptiv.py          erzeugt alle Tabellen
    python tools/deskriptiv.py -v       zusaetzlich die Rohwerte je Stadtteil

Ausgang: results/deskriptiv/verteilung.csv
         results/deskriptiv/varianzzerlegung.csv
         results/deskriptiv/aufloesung.csv
         results/deskriptiv/korrelation_zielgroesse.csv
         results/deskriptiv/korrelation_pearson.csv · _spearman.csv
         results/deskriptiv/stadtteilprofil.csv  <- Datenquelle von A16
         results/deskriptiv/befunde.md           <- die Lesefassung fuer 4.2
         results/deskriptiv/je_stadtteil.csv     nur mit -v

NICHT TEIL DER ABGABE - das SKRIPT. Die erzeugten Tabellen schon.
Wie bei `codebook.py` ist die Ausgabe selbsttragend geschrieben, weil `tools/`
vor dem Packen geloescht wird und `results/` nicht.

--------------------------------------------------------------------------
ABGRENZUNG ZU codebook.py - DIE BEIDEN TUN VERSCHIEDENES
--------------------------------------------------------------------------
  codebook.py    WAS ist ein Merkmal?   Skalenniveau, Einheit, Quelle,
                 Was/Wie/Wofuer. Eine grosse Tabelle, Auflage Schroeter vom
                 10.08.2026 - und dort ausdruecklich OHNE deskriptive
                 Statistik je Merkmal.

  deskriptiv.py  WIE SIEHT es aus?      Lage, Streuung, Form, Varianzanteile,
                 zeitliche Aufloesung, Zusammenhaenge.

Die Auflage vom 10.08. verbietet die deskriptive Statistik IM CODEBOOK, nicht
in der Arbeit. Kapitel 4 ist die Phase Data Understanding und ohne
Verteilungsbefunde leer. Getrennte Skripte, getrennte Ausgaben, kein Wert an
zwei Stellen.

--------------------------------------------------------------------------
ABGRENZUNG ZU vorpruefung/v2_eignung.py - DIE WICHTIGERE GRENZE
--------------------------------------------------------------------------
Der Abgrenzungsblock in `main.tex` vor Kapitel 5 regelt den Grenzfall (a):

  4.2  WIE DIE DATEN BESCHAFFEN SIND    "rechtsschief, Dispersionsindex 62,8"
  6.2  OB EIN VERFAHREN DAZU PASST      "deshalb Ridge auf log(1+y);
                                         der RESET-Test verwirft die lineare
                                         Spezifikation"

Dieses Skript rechnet AUSSCHLIESSLICH die linke Spalte. Kein RESET-Test, kein
VIF, keine Residuenanalyse, keine Breusch-Pagan- oder Jarque-Bera-Statistik -
die stehen in `v2_eignung.py` und gehoeren nach 6.2. Wer sie hier ergaenzt,
erzeugt genau die Doppelung, die Schroeter am 27.07.2026 angemerkt hat.

Eine Ausnahme mit Absicht: Pearson UND Spearman werden beide berechnet. Ihr
ABSTAND ist ein Befund ueber die Daten (monotoner, aber gekruemmter
Zusammenhang) und gehoert nach 4.2. Die Schlussfolgerung daraus - dass ein
lineares Modell die Kruemmung nicht abbildet - gehoert nach 6.2.

--------------------------------------------------------------------------
DIE GESAMTMENGE, AUF DER GERECHNET WIRD
--------------------------------------------------------------------------
ALLE 36 Stadtteile, Entwicklung UND Hold-out. Das ist hier richtig und in der
Eignungspruefung falsch:

  Kapitel 4 beschreibt den DATENBESTAND. Wer ihn nur auf 30 Stadtteilen
  beschriebe, beschriebe nicht den Datensatz, sondern eine Teilmenge davon.
  Es wird nichts geschaetzt und nichts entschieden - eine Verteilungsangabe
  ueber den vollen Bestand kann kein Leakage erzeugen.

  Die Eignungspruefung dagegen ENTSCHEIDET ueber Verfahren. Sie rechnet
  deshalb auf den 24 Trainingsstadtteilen von Fold 1.

Beide Bezugsmengen kommen in der Arbeit vor. Wo eine Zahl steht, ist sie zu
nennen - deshalb traegt jede erzeugte Tabelle ihre Bezugsmenge in der
Kopfzeile. Der Dispersionsindex ist das stehende Beispiel: 62,8 auf dem vollen
Datensatz, 54,2 auf den Trainingsstadtteilen von Fold 1. Beide korrekt.

--------------------------------------------------------------------------
WARUM DIE VARIANZZERLEGUNG DER KERN DIESES SKRIPTS IST
--------------------------------------------------------------------------
Der Datensatz hat 4.752 Zeilen, aber nur 36 Stadtteile. Fast alle Merkmale
sind innerhalb eines Stadtteils nahezu konstant. Die Zerlegung zwischen /
innerhalb macht das messbar statt behauptet und traegt drei Stellen der
Arbeit gleichzeitig:

  4.2   Befund ueber die Daten
  5.4   Begruendung des Stadtteil-Splits (ein Zeitschnitt pruefte nichts)
  8.3   effektive Stichprobe, Designeffekt, warum mehr Jahre nicht helfen

--------------------------------------------------------------------------
PRUEFAUFTRAEGE - nach jedem Lauf abzuarbeiten
--------------------------------------------------------------------------
  1  Stimmt der Dispersionsindex von `anzahl_einsaetze` mit dem Wert in
     docs/03_STAND.md Abschnitt 2 ueberein (62,8)? Wenn nicht, hat sich die
     Aufbereitung geaendert und 03_STAND.md ist nachzuziehen, nicht diese
     Datei.
  2  Liegt der Zwischen-Varianzanteil von `anzahl_einsaetze` bei 92,5 %?
     Diese Zahl traegt die Begruendung des Stadtteil-Splits in Kapitel 5.4.
  3  Zeigt die Aufloesungstabelle weiterhin 1 eindeutigen Wert je Stadtteil
     fuer die drei baulichen Merkmale und rund 128 fuer den
     Kriminalitaetsindex? (Genau: 128,3 auf allen 36 Stadtteilen, 128,6 auf
     den 30 Entwicklungsstadtteilen - so steht es in 03_STAND.md. Der
     Unterschied ist die Bezugsmenge, kein Fehler.) Das ist Mechanismus 1 aus 07_BEFUNDE.md B-47 und
     erklaert den Widerspruch zwischen Attribution und Ablation in 7.4.
  4  Haben alle Merkmale weiterhin null fehlende Werte? Sonst ist die Aussage
     "keine fehlenden Werte" in Kapitel 5 falsch.
  5  Liegt log_kriminalitaetsindex gegen anteil_risikogewerbe_pct auf den 29
     ENTWICKLUNGSSTADTTEILEN noch bei +0,739? Dieser Wert traegt Mechanismus 2
     in 7.4 und steht so in 03_STAND.md Abschnitt 5.6. Auf den 36 Stadtteilen
     dieser Datei sind es +0,730, auf den 24 Trainingsstadtteilen von Fold 1
     +0,656 - drei Bezugsmengen, drei Werte, alle drei richtig.
     KORRIGIERT AM 22.08.2026: Hier stand zuvor, +0,739 sei der hoechste
     Betrag der Korrelationsmatrix. Das ist er nicht. Hoechster Betrag ist
     median_haushaltseinkommen gegen median_miete mit +0,913 (36 Stadtteile)
     bzw. +0,918 (29). 03_STAND.md war nie falsch - dort steht die Aussage
     "hoechster Betrag" nicht; sie stand nur in diesem Pruefauftrag.
  6  Enthaelt stadtteilprofil.csv weiterhin genau 36 Zeilen, und liegen die
     Extremwerte des Mittels bei 6,4 (Seacliff) und 279,7 (Tenderloin)?
     Die Datei ist die Eingangsgroesse von Abbildung A16; aendert sie sich,
     aendert sich die Abbildung stillschweigend mit.
  7  Stehen in verteilung.csv die beiden ZIELGROESSEN als eigene Zeilen, und
     stimmen ihre Werte mit Tabelle 2 in Kapitel 4 ueberein?
     anzahl_einsaetze     75,9 | 53   | 451   | Schiefe 1,89 | Woelbung 3,43
     einsaetze_je_1000_ew  5,71 | 3,54 | 67,66 | Schiefe 4,16 | Woelbung 20,72
     Sie sind am 24.08.2026 dazugekommen. Vorher standen Schiefe und Woelbung
     der Rate NIRGENDS in einer Ausgabedatei, obwohl Kapitel 4 sie berichtet.
     Gegenprobe: aufloesung.csv hat weiterhin 17 Zeilen und
     varianzzerlegung.csv 19 ohne Dubletten - beide speisen A17 und duerfen
     sich durch die Ergaenzung NICHT veraendert haben.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

WURZEL = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WURZEL))

from prep.config import (  # noqa: E402
    PRAEDIKTOREN, SAISON, LAGS, EXPOSURE_ROH, CRIME_ROH, ANTEILE, ANZAHLEN,
)

DATEN = WURZEL / "data" / "processed"
ZIEL = WURZEL / "results" / "deskriptiv"

# Faktorgruppen wie in modelle/m04_shap.py - dieselbe Einteilung, damit die
# Gruppenaussagen in Kapitel 4 und 7 dieselbe Bedeutung haben.
GRUPPEN = {
    "soziooekonomisch": ["median_haushaltseinkommen", "armutsquote_pct",
                         "akademikerquote_pct", "median_miete",
                         "leerstandsquote_pct"],
    "kriminalitaetsbezogen": ["log_kriminalitaetsindex"],
    "baulich": ["anteil_altbau_vor_1940_pct", "anteil_wohngebaeude_pct",
                "anteil_risikogewerbe_pct"],
    "groessenkontrolle": ["log_bevoelkerung"],
    "saison": list(SAISON),
}


def gruppe_von(spalte: str) -> str:
    for name, spalten in GRUPPEN.items():
        if spalte in spalten:
            return name
    return "-"


# ==========================================================================
# 1  VERTEILUNG JE MERKMAL
# ==========================================================================
def verteilung(d: pd.DataFrame, spalten: list[str]) -> pd.DataFrame:
    """Lage, Streuung und Form je Merkmal.

    Ein:  Datensatz, Spaltenliste
    Aus:  eine Zeile je Merkmal

    - Variationskoeffizient nur bei durchgehend positiven Merkmalen; bei
      Groessen um null herum ist er nicht interpretierbar und bleibt leer.
      Betrifft log_kriminalitaetsindex und die beiden Saisonmerkmale.
    - Schiefe und Woelbung als Befund ueber die FORM, nicht als Test. Ein
      formaler Normalitaetstest gehoert nach 6.2 (Jarque-Bera).
    """
    zeilen = []
    for c in spalten:
        s = pd.to_numeric(d[c], errors="coerce")
        beschreibbar = bool((s > 0).all())
        zeilen.append({
            "merkmal": c,
            "gruppe": gruppe_von(c),
            "n": int(s.notna().sum()),
            "fehlend": int(s.isna().sum()),
            "mittel": s.mean(),
            "sd": s.std(ddof=1),
            "vk": (s.std(ddof=1) / s.mean()) if beschreibbar else np.nan,
            "min": s.min(),
            "q25": s.quantile(0.25),
            "median": s.median(),
            "q75": s.quantile(0.75),
            "max": s.max(),
            "schiefe": s.skew(),
            "woelbung": s.kurtosis(),
        })
    return pd.DataFrame(zeilen)


# ==========================================================================
# 2  VARIANZZERLEGUNG  zwischen / innerhalb der Stadtteile
# ==========================================================================
def varianzzerlegung(d: pd.DataFrame, spalten: list[str]) -> pd.DataFrame:
    """Wie viel Streuung liegt ZWISCHEN, wie viel INNERHALB der Stadtteile?

    Ein:  Datensatz mit Spalte `stadtteil`, Spaltenliste
    Aus:  je Merkmal Zwischen-Anteil, ICC und Designeffekt

    - Der Zwischen-Anteil ist die Varianz der Stadtteilmittelwerte, gewichtet
      mit der Zahl ihrer Monate, geteilt durch die Gesamtvarianz.
    - Die ICC hier ist die Einweg-Zufallseffektform ICC(1). Bei balanciertem
      Panel (jeder Stadtteil 132 Monate) faellt sie mit dem Zwischen-Anteil
      praktisch zusammen; berechnet wird sie trotzdem eigenstaendig, weil der
      Klassifikationsdatensatz einen Monat weniger hat.
    - Der DESIGNEFFEKT 1 + (m - 1) * ICC beziffert, um welchen Faktor die
      effektive Stichprobe unter der Zeilenzahl liegt. n_eff = n / Designeffekt.
      Das ist die Zahl, die in Kapitel 8.3 steht - hier wird sie erzeugt, nicht
      abgeschrieben.
    """
    zeilen = []
    for c in spalten:
        s = pd.to_numeric(d[c], errors="coerce")
        g = s.groupby(d["stadtteil"])
        k = g.ngroups
        n = int(s.notna().sum())
        if k < 2 or n <= k:
            continue
        m = n / k                                   # mittlere Gruppengroesse
        mittel = g.mean()
        groesse = g.count()
        gesamt = s.mean()

        ss_zwischen = float((groesse * (mittel - gesamt) ** 2).sum())
        ss_gesamt = float(((s - gesamt) ** 2).sum())
        ss_innerhalb = ss_gesamt - ss_zwischen

        ms_zwischen = ss_zwischen / (k - 1)
        ms_innerhalb = ss_innerhalb / (n - k)
        # ICC(1) nach Shrout & Fleiss; negative Werte auf 0 gekappt, weil eine
        # negative Varianzkomponente keine Bedeutung hat.
        nenner = ms_zwischen + (m - 1) * ms_innerhalb
        icc = max(0.0, (ms_zwischen - ms_innerhalb) / nenner) if nenner else 0.0
        deff = 1 + (m - 1) * icc

        zeilen.append({
            "merkmal": c,
            "gruppe": gruppe_von(c),
            "anteil_zwischen": ss_zwischen / ss_gesamt if ss_gesamt else np.nan,
            "anteil_innerhalb": ss_innerhalb / ss_gesamt if ss_gesamt else np.nan,
            "icc": icc,
            "designeffekt": deff,
            "n_effektiv": n / deff if deff else np.nan,
        })
    return pd.DataFrame(zeilen)


# ==========================================================================
# 3  ZEITLICHE AUFLOESUNG
# ==========================================================================
def aufloesung(d: pd.DataFrame, spalten: list[str]) -> pd.DataFrame:
    """Wie viele verschiedene Werte nimmt ein Merkmal je Stadtteil an?

    Ein:  Datensatz, Spaltenliste
    Aus:  je Merkmal der Mittelwert der eindeutigen Werte je Stadtteil

    Die vielleicht aussagekraeftigste Tabelle des Kapitels. Ein Merkmal mit
    einem einzigen Wert je Stadtteil traegt ueber 132 Monate keine einzige
    zusaetzliche Information - es ist ein Stadtteilmerkmal, das 132-mal
    wiederholt in der Tabelle steht. Genau das gilt fuer die drei baulichen
    Merkmale (Land-Use-Snapshot 2020) und naeherungsweise fuer die
    ACS-Merkmale (fuenf Jahrgaenge mit Publikationsversatz).

    Der Befund gehoert nach 4.2; seine Konsequenz - dass Ablation und
    Attribution deshalb auseinanderfallen - nach 7.4 und 8.2.
    """
    zeilen = []
    for c in spalten:
        je_stadtteil = d.groupby("stadtteil")[c].nunique()
        zeilen.append({
            "merkmal": c,
            "gruppe": gruppe_von(c),
            "eindeutig_je_stadtteil_mittel": je_stadtteil.mean(),
            "eindeutig_je_stadtteil_min": int(je_stadtteil.min()),
            "eindeutig_je_stadtteil_max": int(je_stadtteil.max()),
            "eindeutig_gesamt": int(d[c].nunique()),
            "zeitkonstant": bool(je_stadtteil.max() == 1),
        })
    return (pd.DataFrame(zeilen)
            .sort_values("eindeutig_je_stadtteil_mittel", ascending=False))


# ==========================================================================
# 3b  STADTTEILPROFIL - die Eingangsgroesse von Abbildung A16
# ==========================================================================
def stadtteilprofil(d: pd.DataFrame) -> pd.DataFrame:
    """Lage und Streuung der Einsatzlast je Stadtteil ueber alle Monate.

    Ein:  Regressionsdatensatz
    Aus:  eine Zeile je Stadtteil mit Median, Quartilen, Mittel, Maximum,
          Rate, Bevoelkerung und Hold-out-Kennzeichen

    Warum als eigene Datei und nicht nur als Zahl in befunde.md:
    `modelle/m05_abbildungen.py` rechnet nichts, es liest ausschliesslich
    CSV-Dateien aus results/. Abbildung A16 braucht die Verteilung je
    Stadtteil, also muss sie hier entstehen und nicht dort. Dieselbe
    Arbeitsteilung wie bei allen uebrigen Abbildungen.

    Abgegrenzt von `je_stadtteil.csv` (Schalter -v): Das ist eine
    Diagnoseausgabe zum Nachschauen, diese Datei ist ein Artefakt der Arbeit
    und wird immer geschrieben.
    """
    g = d.groupby("stadtteil")
    profil = pd.DataFrame({
        "monate":        g["jahr_monat"].count(),
        "mittel":        g["anzahl_einsaetze"].mean(),
        "median":        g["anzahl_einsaetze"].median(),
        "q25":           g["anzahl_einsaetze"].quantile(0.25),
        "q75":           g["anzahl_einsaetze"].quantile(0.75),
        "min":           g["anzahl_einsaetze"].min(),
        "max":           g["anzahl_einsaetze"].max(),
        "rate_mittel":   g["einsaetze_je_1000_ew"].mean(),
        "bevoelkerung":  g[EXPOSURE_ROH].mean(),
        "fold":          g["fold"].first(),
        "ist_holdout":   g["ist_holdout"].first(),
    })
    return profil.sort_values("mittel", ascending=False).reset_index()


# ==========================================================================
# 4  ZIELGROESSEN
# ==========================================================================
def zielgroessen(reg: pd.DataFrame, kls: pd.DataFrame) -> list[str]:
    """Steckbrief beider Zielgroessen als Markdown-Bloecke.

    Ein:  beide Datensaetze
    Aus:  Liste von Textzeilen

    - Der DISPERSIONSINDEX Var/Mean ist der zentrale Befund des Mengenstrangs:
      Bei einer Poisson-verteilten Groesse waere er 1. Er begruendet in
      Kapitel 4 NICHTS - er ist ein Befund. Die Wahl der Baseline gehoert
      nach 5.4, die Frage der Verteilungsannahme nach 6.2.
    - Der Nullanteil ist bei Zaehldaten mitzuberichten, weil ein hoher Anteil
      eine andere Modellklasse verlangte (Hurdle, Zero-Inflated). Hier ist er
      praktisch null - und genau das ist der Grund, warum diese Modellklassen
      in der Arbeit nicht vorkommen. Das gehoert in einen Satz.
    """
    t: list[str] = []
    y = reg["anzahl_einsaetze"]
    disp = y.var(ddof=1) / y.mean()
    st_mittel = reg.groupby("stadtteil")["anzahl_einsaetze"].mean()
    t += [
        "## Zielgroesse 1 - anzahl_einsaetze (Mengenstrang)", "",
        f"- Zeilen: {z(len(y), 0)}",
        f"- Mittel {z(y.mean())} | Median {z(y.median(), 0)} "
        f"| SD {z(y.std(ddof=1))} | Min {z(y.min(), 0)} | Max {z(y.max(), 0)}",
        f"- Schiefe {z(y.skew(), 2)} | Woelbung {z(y.kurtosis(), 2)} "
        "-> deutlich rechtsschief",
        f"- **Dispersionsindex Var/Mean = {z(disp)}** "
        "(bei Poisson waere er 1,0) - Ueberdispersion",
        f"- Nullanteil {z(100 * (y == 0).mean(), 2)} % "
        "-> keine Nullinflation, deshalb kein Hurdle- oder "
        "Zero-Inflated-Modell",
        f"- Spannweite der Stadtteilmittel: {z(st_mittel.min())} bis "
        f"{z(st_mittel.max())} -> Faktor "
        f"{z(st_mittel.max() / st_mittel.min(), 0)}",
        "",
    ]

    r = reg["einsaetze_je_1000_ew"]
    je_st = reg.groupby("stadtteil")["einsaetze_je_1000_ew"].mean()
    t += [
        "## Zielgroesse 2 - einsaetze_je_1000_ew (Robustheitspruefung)", "",
        f"- Mittel {z(r.mean(), 2)} | Median {z(r.median(), 2)} "
        f"| Min {z(r.min(), 2)} | Max {z(r.max(), 2)}",
        f"- Schiefe {z(r.skew(), 2)} | Woelbung {z(r.kurtosis(), 2)} "
        "-> die Normierung glaettet die Verteilung NICHT, sie verschaerft "
        "sie (Vergleichswerte oben: 1,89 und 3,43)",
        f"- Dispersionsindex Var/Mean = {z(r.var(ddof=1) / r.mean(), 2)} "
        "-> nachrichtlich; Var/Mean ist bei einer Rate keine Kenngroesse "
        "der Verteilungsannahme, die Modellwahl haengt an Zielgroesse 1",
        f"- Stadtteilmittel {z(je_st.min(), 2)} bis {z(je_st.max(), 2)} "
        f"-> Faktor {z(je_st.max() / je_st.min(), 0)}",
        "- Der Faktor ist der Grund, warum R2 auf der Rate kein tragfaehiges "
        "Hauptmass ist: R2 misst gegen den Mittelwert der Testdaten, und der "
        "liegt je nach Fold weit vom Trainingsmittelwert entfernt "
        "(Begruendung in 5.4, Zahlen in 03_STAND.md Abschnitt 4).",
        "",
    ]

    v = kls["dominante_einsatzart"].value_counts(normalize=True)
    t += ["## Zielgroesse 3 - dominante_einsatzart (Strukturstrang)", "",
          "| Klasse | Anteil | Zeilen |", "|---|---:|---:|"]
    for klasse, anteil in v.items():
        n = int((kls["dominante_einsatzart"] == klasse).sum())
        t.append(f"| {klasse} | {z(100 * anteil)} % | {n} |")
    t += [
        "",
        f"- Mehrheitsklasse `{v.index[0]}` mit {z(100 * v.iloc[0])} % "
        "-> Accuracy ist als Hauptmass wertlos, ein Modell das immer die "
        "Mehrheitsklasse sagt erreicht diesen Wert ohne jede Leistung.",
        "- Die vier Klassen entstehen als argmax ueber die vier "
        "NFIRS-Anteilsspalten desselben Monats. Es ist eine echte Klasse, "
        "kein gesetzter Schwellwert und keine Einteilung einer stetigen "
        "Groesse.",
        "",
    ]

    # Marge zwischen Platz eins und zwei - der Befund, der Decke A in 7.2
    # spaeter erst lesbar macht. Hier NUR als Eigenschaft der Zielgroesse.
    anteile = kls[ANTEILE].to_numpy()
    sortiert = np.sort(anteile, axis=1)
    marge = sortiert[:, -1] - sortiert[:, -2]
    t += [
        "### Wie eindeutig ist die dominante Klasse?", "",
        f"- Mittlerer Siegeranteil {z(sortiert[:, -1].mean(), 3)}",
        f"- Mittlere Marge zu Platz zwei {z(marge.mean(), 3)} "
        f"| Median {z(float(np.median(marge)), 3)}",
        f"- Bei {z(100 * (marge < 0.20).mean())} % der Stadtteil-Monate "
        "liegt die Marge unter 0,20.",
        "- BEFUND, kein Argument: Die Zielgroesse ist an vielen Stellen knapp "
        "entschieden. Was daraus folgt - die Obergrenze des Strukturstrangs -, "
        "wird in `vorpruefung/v4_decke.py` gerechnet und gehoert vor die "
        "Ergebnistabelle in 7.2.",
        "- BEZUGSMENGE BEACHTEN: `v4_decke.py` rechnet auf dem "
        "Entwicklungspanel (30 Stadtteile) und berichtet deshalb einen "
        "mittleren Siegeranteil von 0,509 und einen Margenanteil unter 0,20 "
        "von 41,6 %. Die Werte hier gelten fuer alle 36 Stadtteile. Beide "
        "sind korrekt - in der Arbeit ist die Zahl aus `v4_decke.py` zu "
        "verwenden, weil sie zur Ergebnistabelle in 7.2 gehoert.",
        "",
    ]
    return t


# ==========================================================================
# 5  ZUSAMMENHAENGE
# ==========================================================================
def zusammenhaenge(d: pd.DataFrame, spalten: list[str],
                   ziel: str) -> pd.DataFrame:
    """Pearson und Spearman gegen die Zielgroesse - und ihr Abstand.

    Ein:  Datensatz, Merkmalsliste, Name der Zielgroesse
    Aus:  je Merkmal beide Korrelationen und ihre Differenz

    Der ABSTAND ist der Befund: Ein grosser Unterschied bei gleichzeitig
    substanzieller Korrelation zeigt einen monotonen, aber gekruemmten
    Zusammenhang. Wichtig fuer den Text - und leicht falsch zu machen: Liegt
    die Korrelation nahe null, ist der Abstand Rauschen und KEIN Befund.
    Die Spalte `belastbar` markiert das, damit die Zeile nicht mitgelesen wird.
    """
    zeilen = []
    for c in spalten:
        p = d[c].corr(d[ziel], method="pearson")
        s = d[c].corr(d[ziel], method="spearman")
        zeilen.append({
            "merkmal": c,
            "gruppe": gruppe_von(c),
            "pearson": p,
            "spearman": s,
            "abstand": s - p,
            "belastbar": abs(p) > 0.20 or abs(s) > 0.20,
        })
    return (pd.DataFrame(zeilen)
            .sort_values("pearson", key=abs, ascending=False))


# ==========================================================================
# 6  AUSGABE
# ==========================================================================
def z(wert: float, n: int = 1) -> str:
    """Zahl in deutscher Schreibweise: Komma als Dezimaltrenner, Punkt als
    Tausendertrenner. Eigene Funktion, weil ein globales replace(",", ".")
    auch die Satzkommas der Fliesstexte trifft - genau der Fehler, der in der
    ersten Fassung dieses Skripts drin war."""
    if pd.isna(wert):
        return ""
    return f"{wert:,.{n}f}".replace(",", "#").replace(".", ",").replace("#", ".")


def als_markdown(df: pd.DataFrame, nachkomma: dict | None = None) -> str:
    d = df.copy()
    nachkomma = nachkomma or {}
    for c in d.columns:
        if pd.api.types.is_float_dtype(d[c]):
            n = nachkomma.get(c, 3)
            d[c] = d[c].map(lambda v, n=n: z(v, n))
    kopf = "| " + " | ".join(d.columns) + " |"
    trenn = "|" + "|".join("---" for _ in d.columns) + "|"
    return "\n".join([kopf, trenn] + [
        "| " + " | ".join(str(v) for v in zeile) + " |"
        for zeile in d.itertuples(index=False)
    ])


def main(argv: list[str]) -> int:
    ausfuehrlich = "-v" in argv
    reg = pd.read_parquet(DATEN / "regression.parquet")
    kls = pd.read_parquet(DATEN / "klassifikation.parquet")
    ZIEL.mkdir(parents=True, exist_ok=True)

    merkmale = list(PRAEDIKTOREN) + list(SAISON)
    beschreibend = merkmale + [EXPOSURE_ROH, CRIME_ROH] + list(LAGS)
    vorhanden = [c for c in beschreibend if c in reg.columns]

    # Die beiden Mengenzielgroessen kommen NUR in verteilung.csv dazu
    # (24.08.2026). Vorher liefen sie allein durch zielgroessen() in die
    # Lesefassung; ihre Schiefe und Woelbung standen damit in keiner
    # maschinenlesbaren Datei, obwohl Tabelle 2 in Kapitel 4 beide
    # gegenueberstellt. NICHT in `vorhanden` aufnehmen: varianzzerlegung()
    # bekommt sie unten ohnehin explizit, sie stuenden sonst doppelt, und
    # aufloesung() speist A17, das ausschliesslich die zwoelf Merkmale zeigt.
    mit_zielen = vorhanden + [c for c in ("anzahl_einsaetze",
                                          "einsaetze_je_1000_ew")
                              if c in reg.columns]

    vert = verteilung(reg, mit_zielen)
    varz = varianzzerlegung(reg, vorhanden + ["anzahl_einsaetze",
                                              "einsaetze_je_1000_ew"])
    aufl = aufloesung(reg, vorhanden)
    korr = zusammenhaenge(reg, merkmale, "anzahl_einsaetze")
    prof = stadtteilprofil(reg)

    vert.to_csv(ZIEL / "verteilung.csv", index=False)
    varz.to_csv(ZIEL / "varianzzerlegung.csv", index=False)
    aufl.to_csv(ZIEL / "aufloesung.csv", index=False)
    korr.to_csv(ZIEL / "korrelation_zielgroesse.csv", index=False)
    prof.to_csv(ZIEL / "stadtteilprofil.csv", index=False)
    reg[merkmale].corr(method="pearson").to_csv(ZIEL / "korrelation_pearson.csv")
    reg[merkmale].corr(method="spearman").to_csv(ZIEL / "korrelation_spearman.csv")

    # ---- die Lesefassung -------------------------------------------------
    n_st = reg["stadtteil"].nunique()
    zeitraum = f"{reg['jahr_monat'].min()} bis {reg['jahr_monat'].max()}"
    y_zwischen = float(varz.loc[varz["merkmal"] == "anzahl_einsaetze",
                                "anteil_zwischen"].iloc[0])
    y_deff = float(varz.loc[varz["merkmal"] == "anzahl_einsaetze",
                            "designeffekt"].iloc[0])
    y_neff = float(varz.loc[varz["merkmal"] == "anzahl_einsaetze",
                            "n_effektiv"].iloc[0])

    t = [
        "# Deskriptive Befunde - Grundlage von Kapitel 4",
        "",
        f"Erzeugt von `tools/deskriptiv.py` aus "
        f"`data/processed/regression.parquet` und `klassifikation.parquet`.",
        "",
        f"**Bezugsmenge: alle {n_st} Stadtteile**, Entwicklung und Hold-out, "
        f"{zeitraum}, {z(len(reg), 0)} Zeilen.",
        "",
        "Die Eignungspruefung rechnet auf den 24 Trainingsstadtteilen von "
        "Fold 1, `03_STAND.md` teilweise auf den 30 Entwicklungsstadtteilen. "
        "Bei denselben Groessen entstehen dadurch leicht abweichende Werte - "
        "alle drei sind korrekt. Beispiel: eindeutige Werte des "
        "Kriminalitaetsindex je Stadtteil betragen 128,3 hier (36), 128,6 in "
        "`03_STAND.md` (29) und 128,2 in der Eignungspruefung (23). **Wo eine "
        "Zahl in der Arbeit steht, ist ihre Bezugsmenge zu nennen.**",
        "",
        "---", "",
        "## 1  Verteilung je Merkmal", "",
        als_markdown(vert, {"mittel": 2, "sd": 2, "vk": 2, "min": 2, "q25": 2,
                            "median": 2, "q75": 2, "max": 2, "schiefe": 2,
                            "woelbung": 2}),
        "",
        f"Fehlende Werte insgesamt: **{int(vert['fehlend'].sum())}**.",
        "",
        "---", "",
        "## 2  Varianzzerlegung zwischen und innerhalb der Stadtteile", "",
        als_markdown(varz, {"anteil_zwischen": 3, "anteil_innerhalb": 3,
                            "icc": 3, "designeffekt": 1, "n_effektiv": 1}),
        "",
        f"**Der tragende Befund:** {z(100 * y_zwischen)} % der Varianz von "
        "`anzahl_einsaetze` liegen ZWISCHEN den Stadtteilen. Der Designeffekt "
        f"betraegt {z(y_deff, 0)}, die effektive Stichprobe damit rund "
        f"{z(y_neff, 0)} Einheiten bei {z(len(reg), 0)} Zeilen.",
        "",
        "Daraus folgen drei Stellen der Arbeit:",
        "",
        "- **5.4** Ein Zeitschnitt pruefte die Forschungsfrage nicht - jeder "
        "Stadtteil stuende in Training und Test, das Modell kennte sein "
        "Niveau bereits. Deshalb der Stadtteil-Split.",
        "- **8.3** Die Beschraenkung liegt bei der Zahl der Stadtteile, nicht "
        "bei der Zahl der Beobachtungen. Ein zusaetzliches Jahr braechte "
        f"{n_st} x 12 Zeilen und null zusaetzliche Stadtteile.",
        "- **7.1** Getestet wird auf den zehn Wiederholungsmitteln, nicht auf "
        "den 50 Einzellaeufen - letztere waeren Pseudoreplikation.",
        "",
        "---", "",
        "## 3  Zeitliche Aufloesung", "",
        "Eindeutige Werte je Stadtteil ueber den gesamten Zeitraum.",
        "",
        als_markdown(aufl, {"eindeutig_je_stadtteil_mittel": 1}),
        "",
        "Merkmale mit `zeitkonstant = True` tragen ueber die Monate keine "
        "zusaetzliche Information; sie sind Stadtteilmerkmale, die in jeder "
        "Zeile des Stadtteils wiederholt stehen. Bei drei Merkmalen ist das "
        "eine Eigenschaft der Datenlage - der Land-Use-Datensatz existiert "
        "nur im Jahrgang 2020.",
        "",
        "---", "",
        "## 4  Zusammenhaenge mit der Zielgroesse `anzahl_einsaetze`", "",
        als_markdown(korr, {"pearson": 3, "spearman": 3, "abstand": 3}),
        "",
        "Der Abstand zwischen Spearman und Pearson zeigt einen monotonen, "
        "aber gekruemmten Zusammenhang - allerdings NUR in den Zeilen mit "
        "`belastbar = True`. Wo beide Korrelationen nahe null liegen, ist der "
        "Abstand Rauschen und kein Befund.",
        "",
        "Was daraus fuer die Verfahrenswahl folgt, steht in 6.2 und wird dort "
        "mit dem RESET-Test formal geprueft. Hier steht nur, wie die Daten "
        "beschaffen sind.",
        "",
        "---", "",
    ] + zielgroessen(reg, kls)

    (ZIEL / "befunde.md").write_text("\n".join(t), encoding="utf-8")

    if ausfuehrlich:
        (reg.groupby("stadtteil")
            .agg(monate=("jahr_monat", "count"),
                 einsaetze_mittel=("anzahl_einsaetze", "mean"),
                 einsaetze_max=("anzahl_einsaetze", "max"),
                 bevoelkerung=(EXPOSURE_ROH, "mean"),
                 fold=("fold", "first"),
                 holdout=("ist_holdout", "first"))
            .sort_values("einsaetze_mittel", ascending=False)
            .to_csv(ZIEL / "je_stadtteil.csv"))

    print(f"geschrieben nach {ZIEL.relative_to(WURZEL)}/")
    for p in sorted(ZIEL.glob("*")):
        print(f"  {p.name}")
    print()
    print(f"Zwischen-Varianzanteil anzahl_einsaetze : {100 * y_zwischen:.1f} %")
    print(f"Designeffekt / n_effektiv               : {y_deff:.0f} / {y_neff:.0f}")
    print(f"Dispersionsindex anzahl_einsaetze       : "
          f"{reg['anzahl_einsaetze'].var(ddof=1) / reg['anzahl_einsaetze'].mean():.1f}")
    print()
    print("Pruefauftraege im Docstring abarbeiten.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
