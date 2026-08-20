"""
Wie gut KANN die Einsatzart mit diesen Merkmalen ueberhaupt vorhergesagt werden?

    python vorpruefung/v4_decke.py            Entwicklungspanel, 29 Stadtteile
    python vorpruefung/v4_decke.py holdout    zusaetzlich die 6 gesperrten

Eingang: data/processed/klassifikation.parquet
         results/klassifikation/struktur_mittel.csv (optional, fuer die Quoten
         des Entwicklungspanels)
         results/klassifikation/holdout.csv (optional, fuer die Quoten des
         Hold-out-Laufs)
Ausgang: results/klassifikation/decke.csv, decke_marge.csv,
         decke_ausschoepfung.csv, decke.md - mit Argument "holdout" dieselben
         Dateien mit Endung _holdout

  - Der Strukturstrang erreicht Macro-F1 um 0,33. Gegen 1,0 gehalten sieht
    das misslungen aus; diese Lesart vergleicht mit einer Obergrenze, die
    bei DIESER Zielgroesse und DIESEM Merkmalssatz nicht erreichbar ist
  - DECKE A, Label-Rauschen: `dominante_einsatzart` ist kein beobachtetes
    Merkmal, sondern der argmax ueber vier Anteilsspalten. Liegen zwei
    Anteile dicht beieinander, entscheidet die Monatsziehung. Gemessen per
    parametrischem Bootstrap aus Multinomial(N, p_beobachtet) - das ist die
    Guete eines Modells, das die wahren Wahrscheinlichkeiten exakt kennt
  - DECKE B, Grenze des Stadtteilwissens: Alle Praediktoren sind
    stadtteilgebunden (baulich konstant, sozial je Stadtteil-Jahr,
    Kriminalitaet zu 90 % zwischen den Stadtteilen). Mehr als die
    Modalklasse SEINES Stadtteils kann ein Modell daraus nicht ableiten.
    Diese Decke liegt deutlich unter A, weil fast alle Stadtteile dieselbe
    Modalklasse haben
  - Berichtet wird die baselinekorrigierte Ausschoepfung
    (Modell - Mehrheitsklasse) / (Decke - Mehrheitsklasse). Der
    Rohquotient waere geschoent: Der Sockel von Macro-F1 0,22 ist keine
    Leistung des Modells
  - Beide Decken entstehen VOR jeder Modellwahl. Sie zu beziffern ist keine
    nachtraegliche Entlastung, sondern die Voraussetzung dafuer, 0,33
    ueberhaupt einordnen zu koennen (B-48)

FALLSTRICKE
  1  Ohne Argument "holdout" wird auf ist_holdout == 0 gefiltert wie in m02
     und m03. Die Decke ist zwar eine Eigenschaft der ZIELGROESSE und
     beruehrt keinen Praediktor - die Sperre gilt trotzdem konstruktiv
  2  Zeilen mit N = 0 gibt es hier nicht, das Skript prueft es trotzdem:
     rng.multinomial lieferte stumm einen Nullvektor, dessen argmax immer
     auf die erste Klasse zeigt - eine erfundene Beobachtung
  3  Der Bootstrap braucht RANDOM_STATE aus config_modelle.py, sonst
     schwankt Decke A zwischen zwei Laeufen und die Zahl in der Arbeit passt
     nicht mehr zur Zahl in der CSV
  4  Decke A ist eine Obergrenze, kein Zielwert. Bindend ist Decke B
  5  Modellwerte und Decken muessen aus DERSELBEN Bewertung stammen. Bis zum
     20.08.2026 las auch der Hold-out-Lauf die Quoten aus
     struktur_mittel.csv - also Kreuzvalidierungsmittel gegen
     Hold-out-Decken. Die Zahlen in decke_ausschoepfung_holdout.csv waren
     dadurch nicht interpretierbar und wichen weit von den richtigen ab
     (Random Forest 44,7 % statt 18,1 %). Seither waehlt _modellwerte() die
     Quelle anhand des Laufs und decke.md nennt sie

Ausfuehrliche Fassung: docs/08_FUNKTIONSDOKUMENTATION.md
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

    Ein:  zwei Reihen von Klassenlabels
    Aus:  Zahl
    """
    return float(f1_score(a, b, average="macro", zero_division=0))


def decke_a(panel: pd.DataFrame) -> tuple[float, float, float]:
    """Decke A: Label-Rauschen des argmax, parametrischer Bootstrap.

    Ein:  Panel mit den vier Anteilsspalten und der Einsatzzahl N
    Aus:  (mittlerer Macro-F1, Streuung ueber die Ziehungen, Kippanteil)

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

    Ein:  Panel mit Stadtteil- und Klassenspalte
    Aus:  (Macro-F1 der Zuweisung, Trefferanteil, Verteilung der Modalklassen)

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

    Ein:  Panel mit den vier Anteilsspalten
    Aus:  Datenrahmen mit der Verteilung des Abstands

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

    Ein:  Schalter, ob der Hold-out-Lauf gefahren wird
    Aus:  Zuordnung Verfahren -> Macro-F1 und der Dateiname als Herkunftsnachweis

    - ohne "holdout": struktur_mittel.csv, also die Mittel ueber die 50 Laeufe
      auf den 29 Entwicklungsstadtteilen. Dazu passen die Decken aus
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

    Ein:  Modellwerte aus _modellwerte(), Mehrheitsklassen-Basis, Decke A,
          Decke B
    Aus:  Datenrahmen mit einer Quote je Verfahren und Decke

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


def _md(df: pd.DataFrame) -> str:
    """Markdown-Tabelle von Hand.

    NICHT `DataFrame.to_markdown()`: Das braucht `tabulate`, und das steht
    weder in `requirements.txt` noch im gemessenen `requirements_lauf.txt`.
    Hier war der Aufruf besonders tueckisch, weil er ganz am Ende steht - die
    CSV-Dateien sind dann schon geschrieben, nur decke.md fehlt, und der Lauf
    endet mit einem Traceback statt mit einem Ergebnis. Gleiche Loesung wie in
    `tools/suchdiagnose.py`, mit einem Zusatz: Gleitkommazahlen werden auf vier
    Nachkommastellen ausgeschrieben. `str(0.26)` ergaebe "0.26", und diese
    Tabelle wird abgeschrieben - eine verschluckte Null ist genau die Sorte
    Fehler, gegen die `tools/pruefe_zahlen.py` antritt.
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
            quelle: str = "") -> str:
    """Setzt die Ergebnistabellen zu decke.md zusammen.

    Ein:  Deckentabelle, Ausschoepfung, Margenverteilung, Kipp- und
          Trefferanteil, Modalklassen, Zahl der Stadtteile, Herkunft der
          Modellwerte
    Aus:  Markdown-Text

    - reine Formatierung, hier wird nichts gerechnet
    - Ziehungszahl und RANDOM_STATE stehen im Kopf, damit die Datei ohne den Code
      lesbar bleibt
    """
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
    """Rechnet beide Decken, Marge und Ausschoepfung; schreibt vier Dateien.

    Ein:  klassifikation.parquet; Argument "holdout" oeffnet die 6 gesperrten
          Stadtteile; struktur_mittel.csv bzw. holdout.csv optional fuer die
          Quoten
    Aus:  decke.csv, decke_marge.csv, decke_ausschoepfung.csv, decke.md
          (mit Endung _holdout, wenn das Argument gesetzt ist); Exitcode

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

    # Der Hold-out-Lauf schreibt in EIGENE Dateien. Sonst ueberschriebe die
    # Schlussbewertung die Zahlen des Entwicklungspanels, auf die sich
    # Kapitel 7.2 bezieht - derselbe Fehler, den m02 und m03 mit einer
    # getrennten holdout.csv vermeiden.
    endung = "_holdout" if mit_holdout else ""
    tab.to_csv(OUT / f"decke{endung}.csv", index=False)
    mrg.to_csv(OUT / f"decke_marge{endung}.csv", index=False)
    if not aus.empty:
        aus.to_csv(OUT / f"decke_ausschoepfung{endung}.csv", index=False)
    (OUT / f"decke{endung}.md").write_text(
        bericht(tab, aus, mrg, kipp, treffer, modal,
                panel["stadtteil"].nunique(), quelle_modelle if modelle else ""),
        encoding="utf-8")

    # PRUEFAUFTRAG 1 und 2 maschinell.
    if not basis < b < a:
        print("  WARNUNG: Erwartete Ordnung Mehrheitsklasse < Decke B < Decke A "
              "verletzt - Rechenweg pruefen.")
    print(f"  Geschrieben: results/klassifikation/decke{endung}.csv, "
          f"decke_marge{endung}.csv, decke{endung}.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
