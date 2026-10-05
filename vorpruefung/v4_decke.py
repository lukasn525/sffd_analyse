"""
Wie gut KANN die Einsatzart mit diesen Merkmalen ueberhaupt vorhergesagt werden?

    python vorpruefung/v4_decke.py            Entwicklungspanel, 30 Stadtteile
    python vorpruefung/v4_decke.py holdout    zusaetzlich die 6 gesperrten

Input:   data/processed/klassifikation.parquet
         results/klassifikation/struktur_mittel.csv, holdout.csv,
         struktur_vorhersagen.parquet (je optional: Quoten, F1 je Klasse)
Output:  results/klassifikation/decke.csv, decke_marge.csv,
         decke_ausschoepfung.csv, decke.md (mit "holdout": Endung _holdout)
         results/klassifikation/klassen_f1.csv - nur ohne Argument

  - Decke A, Label-Rauschen: dominante_einsatzart ist der argmax ueber vier
    Anteile; parametrischer Bootstrap aus Multinomial(N, p_beobachtet) =
    Guete eines Modells, das die wahren Wahrscheinlichkeiten kennt
  - Decke B, Stadtteilwissen: alle Praediktoren sind stadtteilgebunden ->
    hoechstens die Modalklasse des Stadtteils; liegt deutlich unter A
  - berichtet: (Modell - Mehrheitsklasse) / (Decke - Mehrheitsklasse)
  - beide Decken entstehen vor jeder Modellwahl
  - F1 je Klasse zeigt, worauf der Macro-F1 ruht

FALLSTRICKE
  1  ohne "holdout" Filter auf ist_holdout == 0 wie in m02 und m03
  2  Zeilen mit N = 0 werden geprueft (multinomial liefert sonst einen
     Nullvektor, argmax = erste Klasse)
  3  Bootstrap mit RANDOM_STATE, sonst schwankt Decke A zwischen Laeufen
  4  Decke A ist Obergrenze, kein Zielwert; bindend ist Decke B
  5  Modellwerte und Decken aus derselben Bewertung; _modellwerte() waehlt
     die Quelle, decke.md nennt sie
"""


from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "modelle"))

from config_modelle import RANDOM_STATE  # noqa: E402

PFAD = ROOT / "data" / "processed" / "klassifikation.parquet"
OUT = ROOT / "results" / "klassifikation"

ZIEL = "dominante_einsatzart"
KLASSEN = ["brand", "rettung_ems", "technische_hilfe", "fehlalarm"]
ZAEHLER = [f"anzahl_{k}" for k in KLASSEN]

ZIEHUNGEN = 200


def _macro_f1(a, b) -> float:
    """Macro-F1 zweier Klassenreihen; fehlende Klassen zaehlen als 0.

    Input:  zwei Reihen von Klassenlabels
    Output: Zahl
    """
    return float(f1_score(a, b, average="macro", zero_division=0))


def decke_a(panel: pd.DataFrame) -> tuple[float, float, float]:
    """Decke A: Label-Rauschen des argmax, parametrischer Bootstrap.

    Input:  Panel mit den vier Anteilsspalten und der Einsatzzahl N
    Output: (mittlerer Macro-F1, Streuung ueber die Ziehungen, Kippanteil)

    - jeder Stadtteil-Monat wird aus Multinomial(N, p_beobachtet) neu gezogen
    - der Macro-F1 zwischen beobachtetem und neu gezogenem Label ist die Guete
      eines Modells mit exakter Kenntnis der Klassenwahrscheinlichkeiten
    - kein Verfahren kann darueber hinaus
    - der Kippanteil gibt an, wie viele Zeilen ihren argmax mindestens einmal
      wechseln
    - Fallstrick 2: Zeilen mit N = 0 werden ausgeschlossen, nicht auf eine Klasse
      gesetzt
    """
    zaehler = panel[ZAEHLER].to_numpy(dtype=float)
    n = zaehler.sum(axis=1).astype(int)
    if (n == 0).any():
        zaehler, n = zaehler[n > 0], n[n > 0]
    p = zaehler / n[:, None]
    beobachtet = zaehler.argmax(axis=1)

    rng = np.random.default_rng(RANDOM_STATE)
    werte, kipp = [], np.zeros(len(n))
    for _ in range(ZIEHUNGEN):
        gezogen = np.array([rng.multinomial(k, q) for k, q in zip(n, p)]).argmax(axis=1)
        werte.append(_macro_f1(beobachtet, gezogen))
        kipp += gezogen != beobachtet
    return float(np.mean(werte)), float(np.std(werte, ddof=1)), float((kipp / ZIEHUNGEN).mean())


def decke_b(panel: pd.DataFrame) -> tuple[float, float, pd.Series]:
    """Decke B: Modalklasse je Stadtteil, Obergrenze des Stadtteilwissens.

    Input:  Panel mit Stadtteil- und Klassenspalte
    Output: (Macro-F1 der Zuweisung, Trefferanteil, Verteilung der Modalklassen)

    - aus stadtteilgebundenen Merkmalen kann ein Modell nicht mehr ableiten als
      die haeufigste Klasse seines Stadtteils
    - die Verteilung der Modalklassen ist die inhaltliche Begruendung, warum
      diese Decke tief liegt
    """
    modal = panel.groupby("stadtteil")[ZIEL].agg(lambda s: s.mode().iloc[0])
    vorhersage = panel["stadtteil"].map(modal)
    treffer = float((vorhersage == panel[ZIEL]).mean())
    return _macro_f1(panel[ZIEL], vorhersage), treffer, modal.value_counts()


def marge(panel: pd.DataFrame) -> pd.DataFrame:
    """Abstand zwischen groesstem und zweitgroesstem Klassenanteil.

    Input:  Panel mit den vier Anteilsspalten
    Output: Datenrahmen mit der Verteilung des Abstands

    - ein kleiner Abstand heisst: das Label haette bei einer anderen
      Monatsziehung anders gelautet
    - die Tabelle zeigt, wie gross der Anteil solcher Zeilen ist
    """
    anteile = np.sort(panel[[f"anteil_{k}" for k in KLASSEN]].to_numpy(), axis=1)
    d = anteile[:, -1] - anteile[:, -2]
    return pd.DataFrame([
        {"kennzahl": "Median des Abstands", "wert": round(float(np.median(d)), 4)},
        {"kennzahl": "10%-Quantil", "wert": round(float(np.quantile(d, 0.10)), 4)},
        {"kennzahl": "Anteil Zeilen mit Abstand < 0,05", "wert": round(float((d < 0.05).mean()), 4)},
        {"kennzahl": "Anteil Zeilen mit Abstand < 0,10", "wert": round(float((d < 0.10).mean()), 4)},
        {"kennzahl": "Anteil Zeilen mit Abstand < 0,20", "wert": round(float((d < 0.20).mean()), 4)},
        {"kennzahl": "mittlerer Siegeranteil", "wert": round(float(anteile[:, -1].mean()), 4)},
    ])


def _modellwerte(mit_holdout: bool) -> tuple[dict[str, float], str]:
    """Macro-F1 je Verfahren aus derselben Bewertung, aus der die Decken stammen.

    Input:  Schalter, ob der Hold-out-Lauf gefahren wird
    Output: Zuordnung Verfahren -> Macro-F1 und der Dateiname als Herkunftsnachweis

    - ohne "holdout": struktur_mittel.csv, also die Mittel ueber die 50 Laeufe
      auf den 30 Entwicklungsstadtteilen. Dazu passen die Decken aus
      demselben Panel
    - mit "holdout": holdout.csv, die einmalige Schlussbewertung auf den sechs
      gesperrten Stadtteilen. Dazu passen die Decken aus dem vollen Panel
    - die Stufe-1-Zeile bleibt draussen: die Mehrheitsklasse IST die Basis,
      gegen die korrigiert wird, ihre Quote waere per Konstruktion 0
    - fehlt die Datei, bleibt die Zuordnung leer und die Ausschoepfung
      entfaellt; die beiden Decken haengen nicht von Modellergebnissen ab
    """
    if mit_holdout:
        pfad = OUT / "holdout.csv"
        if not pfad.exists():
            return {}, pfad.name
        h = pd.read_csv(pfad)
        h = h[h["stufe"] >= 2]
        return dict(zip(h["verfahren"], h["macro_f1"])), pfad.name

    pfad = OUT / "struktur_mittel.csv"
    if not pfad.exists():
        return {}, pfad.name
    m = pd.read_csv(pfad)
    return dict(zip(m["verfahren"], m["macro_f1_mean"])), pfad.name


def ausschoepfung(modelle: dict[str, float], basis: float,
                  a: float, b: float) -> pd.DataFrame:
    """Baselinekorrigierte Quote je Verfahren gegen beide Decken.

    Input:  Modellwerte aus _modellwerte(), Mehrheitsklassen-Basis, Decke A,
            Decke B
    Output: Datenrahmen mit einer Quote je Verfahren und Decke

    - Formel: (Modell - Mehrheitsklasse) / (Decke - Mehrheitsklasse)
    - der Rohquotient Modell/Decke waere geschoent: der Sockel der
      Mehrheitsklasse ist keine Leistung des Modells
    - die Funktion prueft NICHT, ob Modellwerte und Decken zueinander passen -
      das entscheidet _modellwerte() (Fallstrick 5)
    """
    zeilen = []
    for name, wert in modelle.items():
        zeilen.append({
            "verfahren": name,
            "macro_f1": round(wert, 4),
            "ueber_mehrheitsklasse": round(wert - basis, 4),
            "quote_decke_a": round((wert - basis) / (a - basis), 4),
            "quote_decke_b": round((wert - basis) / (b - basis), 4),
        })
    return pd.DataFrame(zeilen)


def klassen_f1() -> pd.DataFrame:
    """F1 je Klasse und Verfahren aus den Vorhersagen der Kreuzvalidierung.

    Input:  struktur_vorhersagen.parquet aus m03_struktur.py
    Output: Datenrahmen mit einer Zeile je Verfahren und Klasse; leer, wenn die
            Datei fehlt

    - F1 je Lauf und Klasse, dann gemittelt ueber die 50 Laeufe. Das Mittel
      der vier Klassen ist der Macro-F1 aus struktur_mittel.csv
    - eine Klasse, die ein Lauf nie vorhersagt, zaehlt dort mit F1 = 0
    - dazu, wie oft eine Klasse vorhergesagt und dabei getroffen wird und in
      wie vielen Laeufen sie gar nicht vorkommt
    - die Kodierung 0 bis 3 folgt KLASSEN, wie in m03_struktur.kodiere()
    """
    pfad = OUT / "struktur_vorhersagen.parquet"
    if not pfad.exists():
        return pd.DataFrame()
    v = pd.read_parquet(pfad)
    codes = list(range(len(KLASSEN)))
    zeilen = []
    for name, g in v.groupby("verfahren", sort=False):
        laeufe = g.groupby(["wiederholung", "fold"])
        f1 = np.array([f1_score(h["y"], h["y_hat"], labels=codes,
                                average=None, zero_division=0)
                       for _, h in laeufe])
        for i, klasse in enumerate(KLASSEN):
            vorhergesagt = g["y_hat"] == i
            zeilen.append({
                "verfahren": name, "klasse": klasse,
                "f1_mittel": round(float(f1[:, i].mean()), 4),
                "vorhergesagt": int(vorhergesagt.sum()),
                "davon_richtig": int((vorhergesagt & (g["y"] == i)).sum()),
                "testzeilen": len(g),
                "laeufe_ohne_vorhersage": int(laeufe["y_hat"]
                                              .apply(lambda s: (s == i).sum() == 0)
                                              .sum()),
                "laeufe": laeufe.ngroups,
            })
    return pd.DataFrame(zeilen)


def _md(df: pd.DataFrame) -> str:
    """Markdown-Tabelle von Hand.

    NICHT `DataFrame.to_markdown()`: Das braucht `tabulate`, und das steht
    nicht in `requirements.txt`.
    Hier waere der Aufruf besonders tueckisch, weil er ganz am Ende steht - die
    CSV-Dateien sind dann schon geschrieben, nur decke.md fehlt, und der Lauf
    endet mit einem Traceback statt mit einem Ergebnis. Gleiche Loesung wie in
    `modelle/suchdiagnose.py`, mit einem Zusatz: Gleitkommazahlen werden auf vier
    Nachkommastellen ausgeschrieben. `str(0.26)` ergaebe "0.26", und diese
    Tabelle wird abgeschrieben - eine verschluckte Null ist genau die Sorte
    Fehler, die dabei entsteht.
    """
    def zelle(x) -> str:
        return f"{x:.4f}" if isinstance(x, float) else str(x)

    kopf = list(df.columns)
    zeilen = ["| " + " | ".join(kopf) + " |", "|" + "---|" * len(kopf)]
    for _, z in df.iterrows():
        zeilen.append("| " + " | ".join(zelle(z[s]) for s in kopf) + " |")
    return "\n".join(zeilen)


def bericht(tab: pd.DataFrame, aus: pd.DataFrame, mrg: pd.DataFrame,
            kipp: float, treffer: float, modal: pd.Series, n_stadtteile: int,
            quelle: str = "", kf: pd.DataFrame | None = None) -> str:
    """Setzt die Ergebnistabellen zu decke.md zusammen.

    Input:  Deckentabelle, Ausschoepfung, Margenverteilung, Kipp- und
            Trefferanteil, Modalklassen, Zahl der Stadtteile, Herkunft der
            Modellwerte, optional F1 je Klasse
    Output: Markdown-Text

    - reine Formatierung, hier wird nichts gerechnet
    - Ziehungszahl und RANDOM_STATE stehen im Kopf, damit die Datei ohne den Code
      lesbar bleibt
    """
    teil_klassen = ([
        "## F1 je Klasse (Kreuzvalidierung)",
        "",
        "Mittel ueber die 50 Laeufe aus `struktur_vorhersagen.parquet`. Das "
        "Mittel der vier Klassen ist der Macro-F1 des Verfahrens.",
        "",
        _md(kf),
        "",
    ] if kf is not None and not kf.empty else [])
    z = "\n".join([
        "# Obergrenzen des Strukturstrangs",
        "",
        f"Erzeugt von `vorpruefung/v4_decke.py`, {ZIEHUNGEN} Ziehungen, Seed {RANDOM_STATE}.",
        "",
        "## Die beiden Decken",
        "",
        _md(tab),
        "",
        "## Ausschoepfung",
        "",
        # Die Herkunft gehoert in die Datei: Nur so ist ohne den Code zu
        # sehen, aus welcher Bewertung die Modellwerte stammen - und dass sie
        # zu den Decken darueber passen.
        f"Modellwerte aus `{quelle}`.\n" if quelle else "",
        _md(aus),
        "",
        *teil_klassen,
        "## Wie knapp faellt der argmax aus?",
        "",
        _md(mrg),
        "",
        "## Zu lesen",
        "",
        f"Bei einer Neuziehung derselben Monatsverteilung kippt der argmax in "
        f"{kipp:.1%} der Stadtteil-Monate. Ein Modell, das die wahren "
        f"Klassenwahrscheinlichkeiten exakt kennt, erreicht deshalb nur Decke A.",
        "",
        f"{treffer:.1%} der Zeilen tragen die Modalklasse ihres eigenen Stadtteils - "
        f"das Label ist fast vollstaendig stadtteilgebunden. Von den "
        f"{n_stadtteile} Stadtteilen haben jedoch "
        + ", ".join(f"{v} die Modalklasse {k}" for k, v in modal.items())
        + ". Weil die Stadtteile sich in ihrer Modalklasse kaum unterscheiden, "
          "liegt Decke B unter Decke A: Nicht das Label-Rauschen bindet, "
          "sondern die Armut des Stadtteilwissens.",
        "",
        "Decke B ist damit die massgebliche Obergrenze. Der Abstand zwischen "
        "dem besten Verfahren und Decke B beziffert, was Verfahrenswahl und "
        "Hyperparametersuche ueberhaupt noch holen koennen.",
    ])
    return z + "\n"


def main(argv: list[str]) -> int:
    """Rechnet beide Decken, Marge, Ausschoepfung und F1 je Klasse.

    Input:  klassifikation.parquet; Argument "holdout" oeffnet die 6 gesperrten
            Stadtteile; struktur_mittel.csv bzw. holdout.csv optional fuer die
            Quoten
    Output: decke.csv, decke_marge.csv, decke_ausschoepfung.csv, decke.md
            (mit Endung _holdout, wenn das Argument gesetzt ist), ohne Argument
            zusaetzlich klassen_f1.csv; Exitcode

    - ohne das Argument wird auf ist_holdout == 0 gefiltert (Fallstrick 1)
    - die Quelle der Modellwerte richtet sich nach dem Lauf (Fallstrick 5);
      fehlt sie, entfaellt nur die Ausschoepfungstabelle, die beiden Decken
      haengen nicht von Modellergebnissen ab
    """
    if not PFAD.exists():
        raise SystemExit(f"{PFAD.relative_to(ROOT)} fehlt - erst 'python prep/build.py'.")
    OUT.mkdir(parents=True, exist_ok=True)

    voll = pd.read_parquet(PFAD)
    # FALLSTRICK 1: ohne Argument sind die Hold-out-Zeilen ab hier gesperrt.
    mit_holdout = "holdout" in argv
    panel = voll if mit_holdout else voll[voll["ist_holdout"] == 0]
    panel = panel.reset_index(drop=True)
    print(f"  Grundlage: {len(panel):,} Zeilen | "
          f"{panel['stadtteil'].nunique()} Stadtteile | "
          f"{'inklusive' if mit_holdout else 'ohne'} Hold-out\n")

    a, a_sd, kipp = decke_a(panel)
    b, treffer, modal = decke_b(panel)
    basis = _macro_f1(panel[ZIEL], np.full(len(panel), panel[ZIEL].mode().iloc[0]))

    tab = pd.DataFrame([
        {"grenze": "Mehrheitsklasse (Stufe 1)", "macro_f1": round(basis, 4),
         "streuung": "", "bedeutung": "triviale Baseline, kein Modellwissen"},
        {"grenze": "Decke B - Stadtteilwissen", "macro_f1": round(b, 4),
         "streuung": "", "bedeutung": "Modalklasse je Stadtteil perfekt bekannt"},
        {"grenze": "Decke A - Label-Rauschen", "macro_f1": round(a, 4),
         "streuung": round(a_sd, 4), "bedeutung": "Klassenwahrscheinlichkeiten exakt bekannt"},
        {"grenze": "fehlerfreie Vorhersage", "macro_f1": 1.0,
         "streuung": "", "bedeutung": "bei dieser Zielgroesse nicht erreichbar"},
    ])
    print(tab.to_string(index=False), "\n")

    # FALLSTRICK 5: Die Modellwerte muessen aus derselben Bewertung stammen
    # wie die Decken darueber - sonst steht in der Quote eine Guete aus 50
    # Kreuzvalidierungslaeufen gegen eine Decke aus sechs Hold-out-Stadtteilen.
    modelle, quelle_modelle = _modellwerte(mit_holdout)
    if not modelle:
        print(f"  HINWEIS: {quelle_modelle} fehlt - Ausschoepfung wird "
              f"uebersprungen. Erst 'python modelle/m03_struktur.py"
              f"{' holdout' if mit_holdout else ''}'.\n")

    aus = ausschoepfung(modelle, basis, a, b) if modelle else pd.DataFrame()
    if not aus.empty:
        print(f"  Modellwerte aus {quelle_modelle}")
        print(aus.to_string(index=False), "\n")

    mrg = marge(panel)
    print(mrg.to_string(index=False), "\n")

    # Nur fuer die Kreuzvalidierung: Fuer das Hold-out gibt es keine
    # gespeicherten Vorhersagen (Fallstrick 5).
    kf = pd.DataFrame() if mit_holdout else klassen_f1()
    if not kf.empty:
        print(kf.to_string(index=False), "\n")

    # Der Hold-out-Lauf schreibt in EIGENE Dateien. Sonst ueberschriebe die
    # Schlussbewertung die Zahlen des Entwicklungspanels - derselbe Fehler,
    # den m02 und m03 mit einer getrennten holdout.csv vermeiden.
    endung = "_holdout" if mit_holdout else ""
    tab.to_csv(OUT / f"decke{endung}.csv", index=False)
    mrg.to_csv(OUT / f"decke_marge{endung}.csv", index=False)
    if not aus.empty:
        aus.to_csv(OUT / f"decke_ausschoepfung{endung}.csv", index=False)
    if not kf.empty:
        kf.to_csv(OUT / "klassen_f1.csv", index=False)
    (OUT / f"decke{endung}.md").write_text(
        bericht(tab, aus, mrg, kipp, treffer, modal,
                panel["stadtteil"].nunique(), quelle_modelle if modelle else "",
                kf),
        encoding="utf-8")

    # Kontrolle: Mehrheitsklasse < Decke B < Decke A.
    if not basis < b < a:
        print("  WARNUNG: Erwartete Ordnung Mehrheitsklasse < Decke B < Decke A "
              "verletzt - Rechenweg pruefen.")
    print(f"  Geschrieben: results/klassifikation/decke{endung}.csv, "
          f"decke_marge{endung}.csv, decke{endung}.md"
          f"{'' if kf.empty else ', klassen_f1.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
