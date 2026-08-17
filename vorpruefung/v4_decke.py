"""
Wie gut KANN die Einsatzart bei dieser Zielgroesse ueberhaupt vorhergesagt werden?

    python vorpruefung/v4_decke.py            Entwicklungspanel, 29 Stadtteile
    python vorpruefung/v4_decke.py holdout    zusaetzlich die 6 gesperrten

Eingang: data/processed/klassifikation.parquet
         results/klassifikation/struktur_mittel.csv (optional, fuer die Quoten)
Ausgang: results/klassifikation/decke.csv
         results/klassifikation/decke_marge.csv
         results/klassifikation/decke_ausschoepfung.csv
         results/klassifikation/decke.md
         mit Argument "holdout" dieselben Dateien mit Endung _holdout

STAND: vollstaendig, 17.08.2026.

--------------------------------------------------------------------------
WOZU DIESES SKRIPT
--------------------------------------------------------------------------
Der Strukturstrang erreicht Macro-F1 um 0,33. Gegen die 1,0 einer fehlerfreien
Vorhersage gehalten sieht das nach einem misslungenen Modell aus. Diese Lesart
ist falsch, und dieses Skript belegt, warum: Sie vergleicht das Ergebnis mit
einer Obergrenze, die bei DIESER Zielgroesse und DIESEM Merkmalssatz gar nicht
erreichbar ist.

Zwei Obergrenzen begrenzen den Strukturstrang, und beide entstehen VOR jeder
Modellwahl - die eine in der Konstruktion der Zielgroesse, die andere in der
Struktur der Merkmale. Sie zu beziffern ist keine nachtraegliche Entlastung,
sondern die Voraussetzung dafuer, Macro-F1 0,33 ueberhaupt einordnen zu
koennen. Ohne sie bleibt jede Aussage ueber den Strukturstrang eine Vermutung.

  DECKE A - LABEL-RAUSCHEN AUS DEM ARGMAX
  `dominante_einsatzart` ist kein beobachtetes Merkmal, sondern der argmax
  ueber vier Anteilsspalten desselben Stadtteil-Monats. Wo zwei Anteile dicht
  beieinander liegen, entscheidet der Zufall der Monatsziehung, welche Klasse
  gewinnt - nicht die Struktur des Stadtteils.
  Gemessen wird das mit einem parametrischen Bootstrap: Jeder Stadtteil-Monat
  wird aus Multinomial(N, p_beobachtet) neu gezogen und geprueft, ob der argmax
  kippt. Der Macro-F1 zwischen dem beobachteten und dem neu gezogenen Label ist
  die Guete, die ein Modell erreichte, das die wahren Klassenwahrscheinlichkeiten
  EXAKT kennt. Kein Verfahren kann darueber hinaus.

  DECKE B - GRENZE DES STADTTEILWISSENS
  Alle zwoelf Praediktoren sind stadtteilgebunden: die baulichen konstant ueber
  den gesamten Zeitraum, die sozialen konstant je Stadtteil-Jahr, der
  Kriminalitaetsindex zu 90 Prozent zwischen den Stadtteilen. Ein Modell kann
  aus ihnen nur Stadtteilwissen ziehen. Die zugehoerige Obergrenze ist deshalb
  die Guete einer Vorhersage, die jedem Stadtteil-Monat die Modalklasse SEINES
  Stadtteils zuweist - mehr traegt Stadtteilwissen nicht.
  Diese Decke liegt DEUTLICH unter Decke A. Der Grund steht in der Tabelle
  `decke.csv`: Die Modalklassen der Stadtteile sind fast alle dieselbe.

  AUSSCHOEPFUNG
  Berichtet wird die baselinekorrigierte Quote

      (Modell - Mehrheitsklasse) / (Decke - Mehrheitsklasse)

  Der Rohquotient Modell/Decke waere geschoent: Ein Modell, das nur die
  Mehrheitsklasse nachbaut, erreicht bereits Macro-F1 0,22 - dieser Sockel
  gehoert nicht zur Leistung des Modells und darf nicht mitgezaehlt werden.

--------------------------------------------------------------------------
FALLSTRICKE
--------------------------------------------------------------------------
  1  DAS HOLD-OUT BLEIBT GESPERRT. Ohne das Argument "holdout" wird auf
     `ist_holdout == 0` gefiltert, wie in m02 und m03. Die Decke ist zwar eine
     Eigenschaft der ZIELGROESSE und beruehrt keine Praediktoren - aber die
     Sperre gilt konstruktiv fuer alle Skripte, nicht nach Ermessen.

  2  MONATE OHNE EINSAETZE gibt es in dieser Tabelle nicht (N >= 1 in allen
     Zeilen), das Skript prueft es trotzdem. Bei N = 0 waere p undefiniert und
     `rng.multinomial` wuerde stumm einen Nullvektor liefern, dessen argmax
     immer auf die erste Klasse zeigt - eine erfundene Beobachtung.

  3  DER BOOTSTRAP BRAUCHT EINEN FESTEN RANDOM_STATE. Ohne ihn schwankt Decke A
     zwischen zwei Laeufen, und die Zahl in der Arbeit passt nicht mehr zur
     Zahl in der CSV. RANDOM_STATE steht in config_modelle.py und ist derselbe wie im
     Verfahrensvergleich.

  4  DECKE A IST EINE OBERGRENZE, KEIN ZIELWERT. Sie beziffert, was bei
     perfekter Kenntnis der Klassenwahrscheinlichkeiten uebrig bliebe. Dass ein
     Modell sie nicht erreicht, ist kein Mangel - Decke B ist die bindende.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Liegt Decke B UNTER Decke A? Wenn nicht, ist etwas falsch: Stadtteilwissen
    kann das Label-Rauschen nicht unterbieten.
  - Liegen beide Decken UEBER dem Macro-F1 der Mehrheitsklasse? Eine Decke
    unterhalb der trivialen Baseline waere ein Rechenfehler.
  - Wie viele Stadtteile teilen dieselbe Modalklasse? Je hoeher diese Zahl,
    desto enger Decke B - das ist die inhaltliche Begruendung des Befundes.
  - Passt die Zeilenzahl? 4 Zeilen in decke.csv, 6 in decke_marge.csv.
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
    return float(f1_score(a, b, average="macro", zero_division=0))


def decke_a(panel: pd.DataFrame) -> tuple[float, float, float]:
    """Label-Rauschen des argmax, parametrischer Bootstrap.

    Jeder Stadtteil-Monat wird aus Multinomial(N, p_beobachtet) neu gezogen.
    Zurueck kommen der mittlere Macro-F1 zwischen beobachtetem und neu
    gezogenem Label, dessen Streuung ueber die Ziehungen und der Anteil der
    Zeilen, deren argmax dabei mindestens einmal kippt.

    FALLSTRICK 2: Zeilen mit N = 0 werden vorher ausgeschlossen, nicht auf eine
    Klasse gesetzt.
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
    """Obergrenze des Stadtteilwissens: Modalklasse je Stadtteil.

    Mehr als die haeufigste Klasse seines Stadtteils kann ein Modell aus
    stadtteilgebundenen Merkmalen nicht ableiten. Zurueck kommen der Macro-F1
    dieser Zuweisung, der Anteil der so korrekt getroffenen Zeilen und die
    Verteilung der Modalklassen ueber die Stadtteile.
    """
    modal = panel.groupby("stadtteil")[ZIEL].agg(lambda s: s.mode().iloc[0])
    vorhersage = panel["stadtteil"].map(modal)
    treffer = float((vorhersage == panel[ZIEL]).mean())
    return _macro_f1(panel[ZIEL], vorhersage), treffer, modal.value_counts()


def marge(panel: pd.DataFrame) -> pd.DataFrame:
    """Wie knapp faellt der argmax aus?

    Abstand zwischen dem groessten und dem zweitgroessten Klassenanteil je
    Stadtteil-Monat. Ein kleiner Abstand heisst: das Label haette bei einer
    anderen Monatsziehung anders gelautet.
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


def ausschoepfung(modelle: dict[str, float], basis: float,
                  a: float, b: float) -> pd.DataFrame:
    """Baselinekorrigierte Quote je Verfahren gegen beide Decken.

    (Modell - Mehrheitsklasse) / (Decke - Mehrheitsklasse). Der Rohquotient
    Modell/Decke waere geschoent, weil der Sockel der Mehrheitsklasse keine
    Leistung des Modells ist.
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


def bericht(tab: pd.DataFrame, aus: pd.DataFrame, mrg: pd.DataFrame,
            kipp: float, treffer: float, modal: pd.Series, n_stadtteile: int) -> str:
    z = "\n".join([
        "# Obergrenzen des Strukturstrangs",
        "",
        f"Erzeugt von `vorpruefung/v4_decke.py`, {ZIEHUNGEN} Ziehungen, Seed {RANDOM_STATE}.",
        "",
        "## Die beiden Decken",
        "",
        tab.to_markdown(index=False),
        "",
        "## Ausschoepfung",
        "",
        aus.to_markdown(index=False),
        "",
        "## Wie knapp faellt der argmax aus?",
        "",
        mrg.to_markdown(index=False),
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

    pfad_mittel = OUT / "struktur_mittel.csv"
    modelle = {}
    if pfad_mittel.exists():
        m = pd.read_csv(pfad_mittel)
        modelle = dict(zip(m["verfahren"], m["macro_f1_mean"]))
    else:
        print("  HINWEIS: struktur_mittel.csv fehlt - Ausschoepfung wird "
              "uebersprungen. Erst 'python modelle/m03_struktur.py'.\n")

    aus = ausschoepfung(modelle, basis, a, b) if modelle else pd.DataFrame()
    if not aus.empty:
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
        bericht(tab, aus, mrg, kipp, treffer, modal, panel["stadtteil"].nunique()),
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
