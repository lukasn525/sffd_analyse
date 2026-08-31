"""
Zahlenwaechter - prueft die Dokumentation gegen die Ergebnisdateien.

NICHT TEIL DER ABGABE. Dieser Ordner ist ein Arbeitswerkzeug und kann vor dem
Packen des Abgabe-ZIP geloescht werden. Er erzeugt keine Ergebnisse und wird
von keinem Skript in prep/, vorpruefung/ oder modelle/ importiert.

WOZU
--------------------------------------------------------------------------
`CLAUDE.md` legt fest: jede Ergebniszahl steht in `docs/03_STAND.md` und nur
dort. Diese Regel haelt genau so lange, wie jemand sie nach jedem Lauf von
Hand nachzieht. Am 07.08.2026 hat sie nicht gehalten - Abschnitt 4 berichtete
die Negative Binomial (RMSE 37,27), waehrend Abschnitt 5 derselben Datei das
Poisson-GLM (33,98) auswies, und `06_RISIKEN.md` empfahl in R-9 das Gegenteil
dessen, was Decision Log #43 umgesetzt hatte.

Kein Rechenfehler, sondern Drift: Eine Zahl lebte an zwei Orten, und nur einer
wurde gepflegt. Dagegen hilft keine Sorgfalt, sondern ein Exit-Code.

WIE
--------------------------------------------------------------------------
Jede Pruefung sagt: "Wert X aus Datei Y muss in Abschnitt Z von Dokument D
vorkommen." Der Sollwert wird bei jedem Lauf NEU aus results/ gelesen, nie aus
einem Dokument uebernommen. Gesucht wird abschnittsweise, damit eine Zahl, die
zufaellig anderswo steht, nicht als Treffer durchgeht.

Drei Arten von Befund:
  FEHLER    Der Sollwert steht nicht im geforderten Abschnitt.
  ALTLAST   Ein frueher gueltiger Wert steht noch da. Warnung, kein Fehler -
            historische Verweise ("Bis zum 06.08. stand hier ...") sind
            erwuenscht und werden erkannt.
  HINWEIS   Struktur stimmt nicht (Abschnitt umbenannt, Datei fehlt).

Exit-Code 0 = sauber, 1 = mindestens ein FEHLER.

AUSFUEHREN
--------------------------------------------------------------------------
  python tools/pruefe_zahlen.py            alle Pruefungen
  python tools/pruefe_zahlen.py -v         zusaetzlich die bestandenen zeigen

Nach jedem Lauf von m02/m03/m04/v1/v3 aufrufen. Schlaegt etwas fehl, ist die
DOKUMENTATION nachzuziehen - nicht die Pruefung anzupassen.
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
DOCS = ROOT / "docs"


# ==========================================================================
# 1  Werte aus den Ergebnisdateien holen
# ==========================================================================
_cache: dict[str, pd.DataFrame] = {}


def tab(pfad: str) -> pd.DataFrame:
    """CSV aus results/, einmal gelesen und gemerkt."""
    if pfad not in _cache:
        p = RES / pfad
        if not p.exists():
            raise FileNotFoundError(pfad)
        _cache[pfad] = pd.read_csv(p)
    return _cache[pfad]


def wert(pfad: str, spalte: str, **filter_) -> float:
    """Ein einzelner Wert. Bricht ab, wenn der Filter nicht genau eine Zeile trifft.

    Das ist Absicht: Trifft er mehrere, hat sich das Format der Datei geaendert
    und die Pruefung waere ab da stillschweigend falsch.
    """
    d = tab(pfad)
    for k, v in filter_.items():
        d = d[d[k] == v]
    if len(d) != 1:
        raise LookupError(f"{pfad} {filter_} -> {len(d)} Zeilen, erwartet 1")
    return float(d.iloc[0][spalte])


def mittel(pfad: str, spalte: str, **filter_) -> float:
    """Mittelwert ueber alle Zeilen, die der Filter trifft."""
    d = tab(pfad)
    for k, v in filter_.items():
        d = d[d[k] == v]
    if d.empty:
        raise LookupError(f"{pfad} {filter_} -> keine Zeile")
    return float(d[spalte].mean())


def summe(pfad: str, spalte: str, **filter_) -> float:
    d = tab(pfad)
    for k, v in filter_.items():
        d = d[d[k] == v]
    return float(d[spalte].sum())


# ==========================================================================
# 2  Dokumente in Abschnitte zerlegen
# ==========================================================================
@dataclass
class Abschnitt:
    nummer: str
    titel: str
    zeile_von: int
    text: str


def abschnitte(datei: Path) -> dict[str, Abschnitt]:
    """Zerlegt ein Markdown-Dokument an den nummerierten Ueberschriften.

    Es entstehen zwei Ebenen von Schluesseln:
      "4"    aus `## 4. Die Baselines`      - das ganze Kapitel
      "5.1"  aus `### 5.1 Menge ...`        - nur dieser Unterabschnitt

    Die feine Ebene ist wichtig: Ein Wert, der im Kapitel noch einmal
    legitim vorkommt (etwa 35,88 in der Ergebnistabelle UND in der Ablation),
    wuerde eine Pruefung auf Kapitelebene bestehen lassen, obwohl die
    eigentliche Zeile falsch ist.
    """
    zeilen = datei.read_text(encoding="utf-8").splitlines()
    aus: dict[str, Abschnitt] = {}

    for muster in (r"^## +(\d+)\.?\s+(.*)$", r"^### +(\d+\.\d+)\.?\s+(.*)$"):
        treffer = []
        for i, z in enumerate(zeilen):
            m = re.match(muster, z)
            if m:
                treffer.append((i, m.group(1), m.group(2).strip()))
        for j, (i, nr, titel) in enumerate(treffer):
            ende = treffer[j + 1][0] if j + 1 < len(treffer) else len(zeilen)
            aus[nr] = Abschnitt(nr, titel, i + 1, "\n".join(zeilen[i:ende]))
    return aus


_ZAHL = re.compile(r"−?-?\d{1,3}(?:\.\d{3})*,\d+|−?-?\d+,\d+|−?-?\d+")


def zahlen_in(text: str) -> set[float]:
    """Alle deutsch formatierten Zahlen eines Textes als float.

    Erkennt Minuszeichen (U+2212) ebenso wie Bindestriche und den Punkt als
    Tausendertrenner. `1.234,5` -> 1234.5
    """
    aus = set()
    for roh in _ZAHL.findall(text):
        s = roh.replace("−", "-").replace(".", "").replace(",", ".")
        try:
            aus.add(float(s))
        except ValueError:
            pass
    return aus


# ==========================================================================
# 3  Die Pruefungen
# ==========================================================================
@dataclass
class Pruefung:
    name: str
    soll: float
    dok: str                      # Dateiname in docs/
    abschnitt: str | None = None  # None = ganzes Dokument
    stellen: int = 2
    quelle: str = ""
    anker: str | None = None      # nur Zeilen durchsuchen, die hierauf passen

    def gerundet(self) -> float:
        return round(self.soll, self.stellen)


def suchraum(text: str, anker: str | None) -> str:
    """Der Text, in dem der Sollwert stehen muss.

    Mit Anker sind das nur die Zeilen, die ihn enthalten - also in aller Regel
    die eine Tabellenzeile, um die es geht. Ohne Anker der ganze Abschnitt.
    """
    if anker is None:
        return text
    return "\n".join(z for z in text.splitlines() if re.search(anker, z))


@dataclass
class Ergebnis:
    fehler: list[str] = field(default_factory=list)
    altlasten: list[str] = field(default_factory=list)
    hinweise: list[str] = field(default_factory=list)
    bestanden: list[str] = field(default_factory=list)


def baue_pruefungen() -> list[Pruefung]:
    """Alle tragenden Zahlen der Arbeit, je mit ihrer Quelle.

    Aufgenommen wird, was in Kapitel 5 bis 9 als Zahl auftaucht oder eine
    Aussage traegt. Nicht aufgenommen wird, was nur beschreibend ist.
    """
    P: list[Pruefung] = []
    S, R = "03_STAND.md", "06_RISIKEN.md"

    # So heissen die Verfahren im Fliesstext - der Anker, mit dem die
    # zugehoerige Tabellenzeile gefunden wird.
    A = {"ridge": "Ridge",
         "random_forest": "Random Forest",
         "xgboost": "XGBoost",
         "Poisson-GLM": "Poisson-GLM",
         "Gesamtmittelwert": "Gesamtmittelwert",
         "Multinomiale logistische Regression": "Logit",
         "Mehrheitsklasse (fehlalarm)": "Mehrheitsklasse"}

    def add(name, soll, dok, absch=None, stellen=2, quelle="", anker=None):
        P.append(Pruefung(name, soll, dok, absch, stellen, quelle, anker))

    # ---- Baselines Regression (§4) --------------------------------------
    bm = "regression/baselines_mittel.csv"
    for ziel, zeile in (("anzahl_einsaetze", r"anzahl_einsaetze"),
                        ("einsaetze_je_1000_ew", r"einsaetze_je_1000_ew")):
        for stufe, modell in ((2, "Poisson-GLM"), (1, "Gesamtmittelwert")):
            for sp, st in (("RMSE_mean", 2), ("R2_mean", 3), ("MAE_mean", 2)):
                add(f"Baseline {ziel} Stufe {stufe} {sp}",
                    wert(bm, sp, zielgroesse=ziel, stufe=stufe, modell=modell),
                    S, "4", st, bm, anker=f"{zeile}.*{A[modell]}")

    # ---- Modellergebnisse Menge (§5.1) ----------------------------------
    mm = "regression/menge_mittel.csv"
    add("Ergebnistabelle Menge: Zielgroesse anzahl",
        wert(mm, "RMSE_mean", zielgroesse="anzahl_einsaetze",
             verfahren="ridge"), S, "5.1", 2, mm, anker=A["ridge"])
    for ziel, absch in (("anzahl_einsaetze", "5.1"),
                        ("einsaetze_je_1000_ew", "5.1")):
        for v in ("ridge", "random_forest", "xgboost"):
            add(f"Menge {ziel} {v} RMSE",
                wert(mm, "RMSE_mean", zielgroesse=ziel, verfahren=v),
                S, absch, 2, mm, anker=A[v])
            add(f"Menge {ziel} {v} R2",
                wert(mm, "R2_mean", zielgroesse=ziel, verfahren=v),
                S, absch, 3, mm, anker=A[v])

    # ---- Aufwand (§5.4) --------------------------------------------------
    for v in ("ridge", "random_forest", "xgboost"):
        add(f"Laufzeit Menge {v} Training",
            wert(mm, "train_sekunden_mean", zielgroesse="anzahl_einsaetze",
                 verfahren=v), S, "5.4", 3 if v == "ridge" else 2, mm,
            anker=r"anzahl_einsaetze|Trainingszeit|Menge,")
        add(f"Laufzeit Menge {v} Inferenz",
            wert(mm, "inferenz_sekunden_mean", zielgroesse="anzahl_einsaetze",
                 verfahren=v), S, "5.4", 3, mm, anker=r"Inferenz")
        add(f"Parallelisierungsgewinn Menge {v}",
            wert(mm, "parallel_gewinn", zielgroesse="anzahl_einsaetze",
                 verfahren=v), S, "5.4", 2, mm, anker=r"^\| Menge")

    # ---- Primaertests Menge (§5.1) --------------------------------------
    vg = "regression/vergleich.csv"
    for ziel in ("anzahl_einsaetze", "einsaetze_je_1000_ew"):
        for v in ("ridge", "random_forest", "xgboost"):
            paar = f"{v} vs Poisson-GLM"
            add(f"Test {ziel} {v} Differenz",
                wert(vg, "differenz_mittel", teststufe="wiederholung",
                     zielgroesse=ziel, paarung=paar), S, "5.1", 2, vg,
                anker=A[v])
            add(f"Test {ziel} {v} p",
                wert(vg, "wilcoxon_p", teststufe="wiederholung",
                     zielgroesse=ziel, paarung=paar), S, "5.1", 3, vg,
                anker=A[v])

    # ---- Baseline Klassifikation (§4) -----------------------------------
    bk = "klassifikation/baselines_klasse_mittel.csv"
    for stufe, ank in ((1, "Mehrheitsklasse"), (2, "Logit")):
        for sp in ("Macro-F1_mean", "Accuracy_mean", "Macro-AUROC_mean"):
            if stufe == 1 and sp == "Macro-AUROC_mean":
                continue          # fuer die Mehrheitsklasse nicht definiert
            add(f"Klass. Stufe {stufe} {sp}",
                wert(bk, sp, stufe=stufe), S, "4", 3, bk, anker=ank)

    # ---- Modellergebnisse Struktur (§5.2) -------------------------------
    sm = "klassifikation/struktur_mittel.csv"
    for v in ("random_forest", "xgboost"):
        add(f"Struktur {v} Macro-F1",
            wert(sm, "macro_f1_mean", verfahren=v), S, "5.2", 4, sm, anker=A[v])
        add(f"Struktur {v} Macro-AUROC",
            wert(sm, "macro_auroc_mean", verfahren=v), S, "5.2", 3, sm, anker=A[v])
        add(f"Struktur {v} Accuracy",
            wert(sm, "accuracy_mean", verfahren=v), S, "5.2", 3, sm, anker=A[v])
        add(f"Laufzeit Struktur {v} Training",
            wert(sm, "train_sekunden_mean", verfahren=v), S, "5.4", 2, sm,
            anker=r"^\| Struktur")
        add(f"Parallelisierungsgewinn Struktur {v}",
            wert(sm, "parallel_gewinn", verfahren=v), S, "5.4", 2, sm,
            anker=r"^\| Struktur")
    add("Struktur Stufe 2 Macro-F1 (Ergebnistabelle)",
        wert(bk, "Macro-F1_mean", stufe=2), S, "5.2", 3, bk, anker="Logit")

    vk = "klassifikation/vergleich.csv"
    for v in ("random_forest", "xgboost"):
        add(f"Struktur Test {v} Differenz",
            wert(vk, "differenz_mittel", teststufe="wiederholung",
                 paarung=f"{v} vs Multinomiale logistische Regression"),
            S, "5.2", 4, vk, anker=A[v])

    # ---- Hold-out (§5.7) -------------------------------------------------
    # GESCHLOSSENE LUECKE, 20.08.2026: Bis hierher deckte dieser Block nur
    # einen Teil der beiden Tabellen in 5.7 ab - in der Menge nur
    # `anzahl_einsaetze`, in der Struktur nur Macro-F1. Die uebrigen Spalten
    # standen ungeprueft in der Dokumentation. Aufgefallen ist es an der
    # Accuracy von Random Forest und XGBoost: Sie stand nach dem Lauf vom
    # 16.08.2026 mit 0,570 und 0,553 in 5.7, waehrend holdout.csv 0,561 und
    # 0,532 auswies - und keine Pruefung schlug an. Regel dieses Skripts ist,
    # dass jede BERICHTETE Zahl eine Pruefung hat; eine Tabellenspalte ohne
    # Pruefung ist eine Zahl, die still veraltet.
    hr = "regression/holdout.csv"
    for v in ("Poisson-GLM", "Gesamtmittelwert", "ridge", "random_forest",
              "xgboost"):
        # Beide Zielgroessen: die Tabelle in 5.7 fuehrt sie in einer Zeile,
        # der Anker trifft also dieselbe Zeile fuer beide.
        for ziel in ("anzahl_einsaetze", "einsaetze_je_1000_ew"):
            for sp, st in (("RMSE", 2), ("R2", 3)):
                add(f"Hold-out Menge {v} {sp} ({ziel})",
                    wert(hr, sp, verfahren=v, zielgroesse=ziel),
                    S, "5.7", st, hr, anker=A[v])
    hk = "klassifikation/holdout.csv"
    for v in ("Multinomiale logistische Regression", "random_forest",
              "xgboost", "Mehrheitsklasse (fehlalarm)"):
        add(f"Hold-out Struktur {v} Macro-F1",
            wert(hk, "macro_f1", verfahren=v), S, "5.7", 3, hk, anker=A[v])
        add(f"Hold-out Struktur {v} Accuracy",
            wert(hk, "accuracy", verfahren=v), S, "5.7", 3, hk, anker=A[v])
        if v != "Mehrheitsklasse (fehlalarm)":
            # Fuer die Mehrheitsklasse ist die AUROC nicht definiert; in der
            # Tabelle steht dort ein Strich.
            add(f"Hold-out Struktur {v} Macro-AUROC",
                wert(hk, "macro_auroc", verfahren=v), S, "5.7", 3, hk,
                anker=A[v])

    # ---- Ablation und Spezifikation (§5.5) -------------------------------
    ab = "shap/ablation_exposition.csv"
    for v in ("random_forest", "xgboost"):
        add(f"Ablation {v} ohne Exposition RMSE",
            mittel(ab, "RMSE", verfahren=v, spezifikation="ohne_exposition"),
            S, "5.5", 2, ab, anker=rf"{A[v]}, ohne")
        add(f"Ablation {v} mit Exposition RMSE",
            mittel(ab, "RMSE", verfahren=v, spezifikation="mit_exposition"),
            S, "5.5", 2, ab, anker=rf"{A[v]}, mit")
    sp = "spezifikation/spezifikation_mittel.csv"
    for s, ank in (("linear", "linear"), ("quadrate", "quadratische"),
                   ("interaktionen", r"Interaktionen"), ("beides", "beides")):
        add(f"Spezifikation {s} RMSE",
            wert(sp, "RMSE_mean", spezifikation=s), S, "5.5", 2, sp, anker=ank)
        add(f"Spezifikation {s} R2",
            wert(sp, "R2_mean", spezifikation=s), S, "5.5", 3, sp, anker=ank)

    # ---- Extrapolation (§3) ---------------------------------------------
    ez = "shap/extrapolation_zusammenhang.csv"
    for v in ("ridge", "random_forest", "xgboost"):
        add(f"Extrapolation rho {v} (anzahl)",
            wert(ez, "spearman_rho", zielgroesse="anzahl_einsaetze",
                 verfahren=v), S, "3", 3, ez)
    add("Extrapolationsanteil ueber 50 Laeufe",
        wert(mm, "extrapolationsanteil", zielgroesse="anzahl_einsaetze",
             verfahren="ridge") * 100, S, "3", 1, mm)

    # ---- Faktorgruppen und VIF (§5.6) ------------------------------------
    fg = "shap/faktorgruppen_menge.csv"
    for g in ("kriminalitaetsbezogen", "baulich", "soziooekonomisch",
              "groessenkontrolle", "saison"):
        add(f"Faktorgruppe Menge {g}",
            summe(fg, "anteil", gruppe=g) * 100, S, "5.6", 1, fg)
    gr = "shap/gruppen.csv"
    # SEIT 31.08.2026 NICHT MEHR FEST: m04_shap.py rechnet SHAP nur fuer
    # Verfahren, die die Stufe-2-Baseline schlagen (uebersprungen.csv haelt
    # die uebrigen fest). Im finalen Lauf ist das im Strukturstrang allein der
    # Random Forest. Eine feste Liste liess den Waechter hier ABBRECHEN - und
    # mit ihm alle nachfolgenden Pruefungen, auch die voellig intakten. Die
    # Liste kommt deshalb aus der Datei: Geprueft wird, was gerechnet wurde.
    for v in sorted(tab(gr)["verfahren"].unique()):
        for g in ("soziooekonomisch", "baulich", "kriminalitaetsbezogen"):
            add(f"Faktorgruppe Struktur {v} {g}",
                wert(gr, "anteil", verfahren=v, gruppe=g) * 100,
                S, "5.6", 1, gr)
    add("Hoechster VIF",
        tab("shap/vif.csv").query("basis == 'stadtteil_jahr'")["vif"].max(),
        S, "5.6", 2, "shap/vif.csv")

    # ---- Risikoblatt: die Zahlen, die dort eine Einstufung tragen --------
    add("R-2 Struktur RF Macro-F1 (Risikoblatt)",
        wert(sm, "macro_f1_mean", verfahren="random_forest"), R, "2", 4, sm)
    add("R-2 Struktur XGB Macro-F1 (Risikoblatt)",
        wert(sm, "macro_f1_mean", verfahren="xgboost"), R, "2", 4, sm)
    add("R-2 Logit Hold-out (Risikoblatt)",
        wert(hk, "macro_f1", verfahren="Multinomiale logistische Regression"),
        R, "2", 3, hk)
    add("R-2 Logit Kreuzvalidierung (Risikoblatt)",
        wert(bk, "Macro-F1_mean", stufe=2), R, "2", 3, bk)
    add("R-14 Baselinelatte Regression (Risikoblatt)",
        wert(bm, "RMSE_mean", zielgroesse="anzahl_einsaetze", stufe=2,
             modell="Poisson-GLM"), R, "2", 2, bm)
    add("R-3 Extrapolation rho Ridge (Risikoblatt)",
        wert(ez, "spearman_rho", zielgroesse="anzahl_einsaetze",
             verfahren="ridge"), R, "2", 3, ez)
    add("R-3 Extrapolationsanteil (Risikoblatt)",
        wert(mm, "extrapolationsanteil", zielgroesse="anzahl_einsaetze",
             verfahren="ridge") * 100, R, "2", 1, mm)

    # ---- Anforderungen je Verfahren (§7, Auflage 10.08.2026) -------------
    # Die drei formalen Tests aus `v2_eignung.annahmen()`. Sollwert ist die
    # NUMERISCHE Spalte `statistik_wert`, nicht die lesbare `statistik` - eine
    # Zeichenkette, die man parst, parst sich beim naechsten Formatwechsel
    # anders.
    an = "eignungspruefung/annahmen.csv"
    for name, anforderung, anker in (
            ("Cameron & Trivedi t (Ueberdispersion)",
             "Equidispersion, Var = mu", "Cameron"),
            ("Breusch-Pagan LM (Homoskedastizitaet)",
             "Homoskedastizitaet der Residuen", "Breusch"),
            ("Jarque-Bera JB (Normalitaet)",
             "normalverteilte Residuen", "Jarque")):
        add(name, wert(an, "statistik_wert", anforderung=anforderung),
            S, "7", 1, an, anker=anker)
    return P


# ==========================================================================
# 4  Abgeleitete Behauptungen - die Klasse Fehler, die niemand nachrechnet
# ==========================================================================
def pruefe_verhaeltnisse(erg: Ergebnis) -> None:
    """Saetze der Form "X-mal schneller". Sie altern unbemerkt mit."""
    mm = "regression/menge_mittel.csv"
    t = {v: wert(mm, "train_sekunden_mean", zielgroesse="anzahl_einsaetze",
                 verfahren=v) for v in ("ridge", "random_forest", "xgboost")}
    abs_ = abschnitte(DOCS / "03_STAND.md").get("5")
    if abs_ is None:
        erg.hinweise.append("03_STAND.md: Abschnitt 5 nicht gefunden")
        return
    vorhanden = zahlen_in(abs_.text)
    for name, faktor in (("XGBoost", t["xgboost"] / t["ridge"]),
                         ("Random Forest", t["random_forest"] / t["ridge"])):
        soll = round(faktor)
        if soll not in vorhanden:
            erg.fehler.append(
                f"03_STAND.md §5: 'Ridge ist N-mal schneller als {name}' - "
                f"N muss {soll} sein ({faktor:.1f}), steht nicht im Abschnitt")
        else:
            erg.bestanden.append(f"Verhaeltnis Ridge/{name} = {soll}")


def pruefe_negative_vorhersagen(erg: Ergebnis) -> None:
    """Die Aussage 'keine negativen Vorhersagen' muss gelten, nicht gehofft sein."""
    n = tab("regression/menge_mittel.csv")["n_negativ_gesamt"].sum()
    if n:
        erg.fehler.append(
            f"menge_mittel.csv: {int(n)} negative Vorhersagen - die Aussage "
            f"'keine negativen Vorhersagen' in 03_STAND.md §5 ist falsch")
    else:
        erg.bestanden.append("Negative Vorhersagen: keine (Aussage gedeckt)")


def pruefe_signifikanzen(erg: Ergebnis) -> None:
    """Welche Paarungen sind signifikant? Aendert sich das, aendert sich Kapitel 7."""
    for datei, erwartet in (
            # Stand nach dem finalen Lauf vom 31.08.2026 - dem Lauf NACH der
            # Korrektur der Wohnbevoelkerung (Crosswalk-Rueckfallebene) und mit
            # Mission Bay, also 36 statt 35 Stadtteilen.
            #
            # DAS MUSTER HAT SICH UMGEDREHT, und zwar in beiden Straengen:
            #   MENGE     Kein Verfahren ist mehr von der Baseline zu trennen -
            #             weder nach oben noch nach unten. Dafuer ist RIDGE
            #             erstmals signifikant besser als BEIDE Baumverfahren
            #             (10/10 gegen XGBoost, 9/10 gegen Random Forest, in
            #             beiden Zielgroessen dieselbe Richtung). R-1 ist damit
            #             im Verfahrensvergleich NICHT eingetreten.
            #   STRUKTUR  XGBoost schlaegt die Baseline nicht mehr (-0,0003,
            #             p = 1,000); der Random Forest schlaegt sie noch, aber
            #             nur halb so deutlich, und er schlaegt nun auch XGBoost.
            #
            # Wer diese Menge aendert, aendert Kapitel 7 der Arbeit. Vorher
            # 03_STAND.md 5.1/5.2 und 06_RISIKEN.md R-1/R-2 nachziehen.
            ("regression/vergleich.csv",
             {"ridge vs random_forest|anzahl_einsaetze",
              "ridge vs xgboost|anzahl_einsaetze",
              "ridge vs xgboost|einsaetze_je_1000_ew"}),
            ("klassifikation/vergleich.csv",
             {"random_forest vs Multinomiale logistische Regression|dominante_einsatzart",
              "random_forest vs xgboost|dominante_einsatzart"})):
        d = tab(datei)
        d = d[(d["teststufe"] == "wiederholung") & (d["signifikant"] == True)]  # noqa: E712
        ist = {f"{r.paarung}|{r.zielgroesse}" for r in d.itertuples()}
        if ist != erwartet:
            neu = ist - erwartet
            weg = erwartet - ist
            erg.fehler.append(
                f"{datei}: Signifikanzmuster hat sich geaendert. "
                + (f"NEU signifikant: {sorted(neu)}. " if neu else "")
                + (f"NICHT MEHR: {sorted(weg)}. " if weg else "")
                + "Kapitel 7 und 06_RISIKEN.md R-1/R-2 pruefen.")
        else:
            erg.bestanden.append(f"Signifikanzmuster {datei} unveraendert")


def pruefe_holdout_unberuehrt(erg: Ergebnis) -> None:
    """Das Hold-out darf genau eine Auswertung haben - je Verfahren eine Zeile."""
    for datei, spalte in (("regression/holdout.csv", "zielgroesse"),
                          ("klassifikation/holdout.csv", "zielgroesse")):
        d = tab(datei)
        dopp = d.groupby(["verfahren", spalte]).size()
        if (dopp > 1).any():
            erg.fehler.append(
                f"{datei}: mehrfache Hold-out-Zeilen je Verfahren - "
                f"das Hold-out wurde mehr als einmal ausgewertet")
        else:
            erg.bestanden.append(f"{datei}: eine Auswertung je Verfahren")


# Modelle, die einmal Stufe 2 waren und es nicht mehr sind. Steht ein solcher
# Name unmarkiert im erzeugten Bericht, ist er stehen geblieben.
VERWORFENE_BASELINES = ("Negative Binomial", "Negativ-Binomial",
                        "Negative-Binomial")

# Berichte, die die Wahl der Baseline BEGRUENDEN - erzeugt, nicht von Hand
# geschrieben. Nur diese werden geprueft: In docs/ und in 07_BEFUNDE.md sind
# Rueckblicke auf die Negative Binomial erwuenscht, dort greift ALTLASTEN.
BEGRUENDENDE_BERICHTE = ("eignungspruefung/eignungspruefung.md",)


def pruefe_baselinename(erg: Ergebnis) -> None:
    """Nennt der erzeugte Bericht dieselbe Stufe-2-Baseline, die gerechnet wurde?

    ANLASS, 10.08.2026. `v2_eignung.py` schloss aus der Overdispersion, Poisson
    scheide aus und die Negative Binomial sei die passende Count-Baseline -
    waehrend `v1_baselines.py` seit Decision Log #45 ein Poisson-GLM anpasst.
    Der Bericht argumentierte damit gegen die eigene Umsetzung, und zwar in
    genau dem Dokument, das die Wahl der Baseline belegen soll.

    WARUM DIE VIER UEBRIGEN PRUEFUNGEN DAS NICHT GEFUNDEN HABEN - zwei Gruende,
    beide behebbar nur durch diese Pruefung:

      1  Sie lesen `docs/`. Dies hier ist eine ERZEUGTE Datei unter `results/`,
         die bis heute von keiner Pruefung angefasst wurde.
      2  Es ist keine Zahl, sondern ein NAME. Der Zahlenwaechter sucht
         ausschliesslich nach deutsch formatierten Zahlen.

    Sollwert ist die Spalte `modell` der Baseline-Dateien, Stufe 2. Sie
    entsteht bei jedem Lauf neu aus dem, was tatsaechlich angepasst wurde - der
    Name kann also nicht veralten, ohne dass diese Pruefung es merkt.
    """
    gerechnet = {}
    for datei, strang in (("regression/baselines_folds.csv", "Menge"),
                          ("klassifikation/baselines_klasse.csv", "Struktur")):
        d = tab(datei)
        namen = sorted({str(n) for n in d[d["stufe"] == 2]["modell"]})
        if len(namen) != 1:
            erg.hinweise.append(
                f"{datei}: {len(namen)} verschiedene Stufe-2-Modelle "
                f"({namen}) - erwartet genau eines, Pruefung uebersprungen")
            continue
        gerechnet[strang] = namen[0]

    for rel in BEGRUENDENDE_BERICHTE:
        pfad = RES / rel
        if not pfad.exists():
            erg.hinweise.append(
                f"results/{rel} fehlt - Baselinename ungeprueft. "
                f"Erst 'python vorpruefung/v2_eignung.py' ausfuehren.")
            continue
        zeilen = pfad.read_text(encoding="utf-8").splitlines()
        text = "\n".join(zeilen)

        # a) Das gerechnete Modell muss vorkommen.
        for strang, name in gerechnet.items():
            if name in text:
                erg.bestanden.append(f"{rel}: nennt '{name}' ({strang})")
            else:
                erg.fehler.append(
                    f"results/{rel}: die Stufe-2-Baseline des Strangs "
                    f"{strang} heisst laut results/ '{name}', kommt im "
                    f"Bericht aber nicht vor - der Bericht belegt eine "
                    f"andere Wahl, als gerechnet wurde")

        # b) Ein verworfenes Modell darf genannt werden - aber nur MARKIERT.
        #    Die Abgrenzung "warum nicht die Negative Binomial" gehoert in den
        #    Bericht; sie muss nur als Abgrenzung erkennbar sein und nicht wie
        #    eine Setzung dastehen. Massstab ist dieselbe HISTORIE-Regel, die
        #    auch fuer Altlasten in docs/ gilt.
        for i, zeile in enumerate(zeilen):
            umfeld = "\n".join(zeilen[max(0, i - UMFELD):i + UMFELD + 1])
            for alt in VERWORFENE_BASELINES:
                if alt in zeile and not HISTORIE.search(umfeld):
                    erg.fehler.append(
                        f"results/{rel}:{i + 1}  '{alt}' ohne Rueckblick-"
                        f"markierung - dieses Modell ist seit #45 nicht mehr "
                        f"Stufe 2\n      {zeile.strip()[:110]}")
                    break


# ==========================================================================
# 5  Altlasten - frueher gueltige Werte, die noch herumstehen
#    HISTORIE und UMFELD stehen hier und werden von pruefe_baselinename
#    mitbenutzt - dieselbe Regel, was ein zulaessiger Rueckblick ist.
# ==========================================================================
@dataclass
class Altlast:
    muster: str            # der alte Wert, deutsch formatiert
    partner: str | None    # der heute gueltige Wert
    warum: str
    ausser: str | None = None   # Zeilen, auf die dieses Muster nicht zielt


ALTLASTEN = [
    Altlast("37,27", "32,99", "Negative-Binomial-Baseline, ersetzt (#45)"),
    Altlast("37,44", "32,99", "Negative Binomial auf Wiederholung 0 (#45)"),
    Altlast("0,477", "0,607", "R2 der Negative Binomial, ersetzt (#45)"),
    Altlast("0,314", "0,301", "getunte Logit-Baseline, ersetzt (#45)",
            ausser=r"je Fold|Fold \d"),
    Altlast("0,290", "0,301", "Logit-Vortest, ersetzt durch den vollen Lauf"),
    # `ausser` seit 31.08.2026: 0,298 ist im finalen Lauf das relative Rho der
    # Fairnesspruefung fuer Ridge (results/fairness/fairness.csv).
    Altlast("0,298", "0,301", "Logit mit C = 1,0, ersetzt (#45)",
            ausser=r"rho|Rho|Fairness|Armut"),
    # `ausser` seit 16.08.2026: Im finalen Lauf ist 0,020 der p-Wert von
    # XGBoost gegen die Baseline (§5.1). Ohne die Ausnahme meldet der Waechter
    # eine echte Zahl als Altlast - ein Fehlalarm, der die uebrigen Meldungen
    # entwertet, weil man aufhoert hinzusehen.
    Altlast("0,020", None, "rho aus dem Lauf vor #43 (B-31), neu gemessen",
            ausser=r"signifikant|unterscheidbar"),
    Altlast("α/7", None, "eine Testfamilie mit 7 Tests, ersetzt (#38)"),
    Altlast("550", None, "altes Laufzeitverhaeltnis Ridge/Random Forest"),
    # Werte aus dem Lauf mit Budget 50 (07.08.2026). Nach Decision Log #52 wird
    # ausschliesslich der finale Lauf berichtet - kein Vorher-Nachher, keine
    # zweite Ergebnisreihe. Taucht eine dieser Zahlen in docs/ auf, ist sie
    # entweder ein Rueckblick (dann markiert) oder ein Verstoss gegen #52.
    Altlast("35,88", "37,39", "XGBoost RMSE, Lauf mit Budget 50 (#52)"),
    Altlast("35,63", "34,96", "Random Forest RMSE, Budget 50 (#52)"),
    Altlast("0,3343", "0,3008", "XGBoost Macro-F1, Budget 50 (#52)"),
    Altlast("0,3276", "0,3184", "Random Forest Macro-F1, Budget 50 (#52)"),
    Altlast("57,86", "51,81", "XGBoost ohne Exposition, Budget 50 (#52)"),
    Altlast("526", "326", "Laufzeitfaktor Ridge/Random Forest, Budget 50 (#52)"),

    # ----------------------------------------------------------------------
    # Lauf vom 16.08.2026 - VOR der Korrektur der Wohnbevoelkerung. Der
    # Crosswalk ordnete 40 von 197 Census Tracts nicht zu, weil er auf
    # Zensusgrenzen 2020 beruht und die ACS-Jahrgaenge 2014/2019 auf denen von
    # 2010; rund 27 % der Einwohner fehlten, konzentriert in Downtown. Nach
    # der Reparatur (Basiscode-Rueckfallebene) und mit Mission Bay sind es 36
    # statt 35 Stadtteile. Diese Werte sind damit vollstaendig ersetzt.
    # ----------------------------------------------------------------------
    Altlast("33,98", "32,99", "Poisson-GLM RMSE, vor der Bevoelkerungskorrektur"),
    Altlast("36,51", "30,93", "Ridge RMSE, vor der Bevoelkerungskorrektur"),
    Altlast("35,61", "34,96", "Random Forest RMSE, vor der Korrektur"),
    Altlast("37,77", "37,39", "XGBoost RMSE, vor der Korrektur"),
    Altlast("69,93", "68,09", "Gesamtmittelwert RMSE, vor der Korrektur"),
    Altlast("0,297", "0,301", "Logit Macro-F1, vor der Korrektur"),
    Altlast("0,3278", "0,3184", "Random Forest Macro-F1, vor der Korrektur"),
    Altlast("0,3322", "0,3008", "XGBoost Macro-F1, vor der Korrektur"),
    Altlast("0,223", "0,221", "Mehrheitsklasse Macro-F1, vor der Korrektur"),
    Altlast("0,806", "0,795", "Mehrheitsklasse Accuracy, vor der Korrektur"),
    Altlast("0,457", "0,4147", "Decke B Stadtteilwissen, vor der Korrektur"),
    Altlast("12,29", "10,64", "hoechster VIF, vor der Korrektur"),
    Altlast("861", "326", "Laufzeitfaktor Ridge/Random Forest, vor der Korrektur"),
    Altlast("56,90", "51,81", "XGBoost ohne Exposition, vor der Korrektur"),
    Altlast("3.828", "3.960", "Zeilen Entwicklungspanel, 29 statt 30 Stadtteile"),
    Altlast("4.620", "4.752", "Zeilen Regression, 35 statt 36 Stadtteile"),
    Altlast("0,739", "0,779", "Kriminalitaet gegen Risikogewerbe, Entwicklungspanel"),
    Altlast("128,6", "127,9", "eindeutige Kriminalitaetswerte je Stadtteil"),
]

# Zeilen mit diesen Markern sind bewusste Rueckblicke und keine Altlast.
HISTORIE = re.compile(
    r"bis zum|stand hier|stand zuvor|ersetzt|historie|früher|frueher|"
    r"überholt|ueberholt|vorher|alte fassung|erste fassung|entfällt|"
    r"entfaellt|nicht mehr|revidiert|zuvor|ehemals|damals", re.I)

# Der Wert muss als eigenstaendige Zahl dastehen: '0,298' in '+0,298' oder
# '10,2980' ist ein anderer Wert, kein Fund.
def _als_zahl(muster: str) -> re.Pattern:
    return re.compile(r"(?<![\d.,+\-−])" + re.escape(muster) + r"(?![\d])")


UMFELD = 2   # Zeilen vor und nach, in denen Markierung oder Partner zaehlen


def pruefe_altlasten(erg: Ergebnis) -> None:
    """Findet frueher gueltige Werte, die noch unmarkiert herumstehen.

    Ein Rueckblick ist erlaubt und erwuenscht - er ist daran zu erkennen, dass
    er entweder eine Markierung traegt ("bis zum 06.08. stand hier ...") oder
    den heutigen Wert danebenstellt ("von 37,27 auf 33,98"). Fehlt beides, ist
    die Zahl vermutlich stehen geblieben.
    """
    for name in ("03_STAND.md", "06_RISIKEN.md", "04_MODELLIERUNG.md",
                 "01_VORGABEN.md", "../CLAUDE.md"):
        p = (DOCS / name).resolve()
        if not p.exists():
            continue
        zeilen = p.read_text(encoding="utf-8").splitlines()
        for i, zeile in enumerate(zeilen):
            umfeld = "\n".join(zeilen[max(0, i - UMFELD):i + UMFELD + 1])
            for alt in ALTLASTEN:
                if alt.ausser and re.search(alt.ausser, zeile):
                    continue
                if not _als_zahl(alt.muster).search(zeile):
                    continue
                if HISTORIE.search(umfeld):
                    continue
                if alt.partner and _als_zahl(alt.partner).search(umfeld):
                    continue
                erg.altlasten.append(
                    f"{p.name}:{i + 1}  '{alt.muster}' ohne Rueckblick und "
                    f"ohne heutigen Wert daneben - {alt.warum}\n"
                    f"      {zeile.strip()[:110]}")


# ==========================================================================
# 6  Lauf
# ==========================================================================
def laufe(ausfuehrlich: bool = False) -> int:
    erg = Ergebnis()
    dok_cache: dict[str, dict[str, Abschnitt]] = {}

    try:
        pruefungen = baue_pruefungen()
    except (FileNotFoundError, LookupError, KeyError) as e:
        print(f"ABBRUCH: Ergebnisdatei fehlt oder hat ein anderes Format - {e}")
        print("Erst die Modellskripte laufen lassen.")
        return 1

    for pr in pruefungen:
        p = DOCS / pr.dok
        if not p.exists():
            erg.hinweise.append(f"{pr.dok} fehlt")
            continue
        if pr.dok not in dok_cache:
            dok_cache[pr.dok] = abschnitte(p)
        if pr.abschnitt is None:
            text = p.read_text(encoding="utf-8")
        else:
            a = dok_cache[pr.dok].get(pr.abschnitt)
            if a is None:
                erg.hinweise.append(
                    f"{pr.dok}: Abschnitt {pr.abschnitt} nicht gefunden - "
                    f"Ueberschrift umbenannt? Pruefung '{pr.name}' uebersprungen")
                continue
            text = a.text
        raum = suchraum(text, pr.anker)
        if pr.anker and not raum.strip():
            erg.hinweise.append(
                f"{pr.dok} §{pr.abschnitt}: keine Zeile passt auf den Anker "
                f"'{pr.anker}' - Tabelle umgebaut? Pruefung '{pr.name}' "
                f"uebersprungen")
            continue
        soll = pr.gerundet()
        if soll in zahlen_in(raum):
            erg.bestanden.append(f"{pr.name} = {soll}")
        else:
            wo = f"§{pr.abschnitt}" + (f" / Zeilen mit '{pr.anker}'"
                                       if pr.anker else "")
            erg.fehler.append(
                f"{pr.dok} {wo}: {pr.name} - erwartet {_de(soll)}, "
                f"steht dort nicht   [{pr.quelle}]")

    for f in (pruefe_verhaeltnisse, pruefe_negative_vorhersagen,
              pruefe_signifikanzen, pruefe_holdout_unberuehrt,
              pruefe_baselinename):
        try:
            f(erg)
        except (FileNotFoundError, LookupError, KeyError) as e:
            erg.hinweise.append(f"{f.__name__}: {e}")
    pruefe_altlasten(erg)

    # ---- Bericht --------------------------------------------------------
    breit = "=" * 74
    print(breit)
    print(f"ZAHLENWAECHTER  -  {len(pruefungen)} Wertpruefungen + 5 Strukturpruefungen")
    print(breit)
    if erg.fehler:
        print(f"\nFEHLER  ({len(erg.fehler)})  - Dokumentation nachziehen\n")
        for f in erg.fehler:
            print(f"  x {f}")
    if erg.altlasten:
        print(f"\nALTLAST ({len(erg.altlasten)})  - pruefen, ob als Rueckblick gemeint\n")
        for a in erg.altlasten:
            print(f"  ! {a}")
    if erg.hinweise:
        print(f"\nHINWEIS ({len(erg.hinweise)})\n")
        for h in erg.hinweise:
            print(f"  i {h}")
    if ausfuehrlich:
        print(f"\nBESTANDEN ({len(erg.bestanden)})\n")
        for b in erg.bestanden:
            print(f"  . {b}")

    print("\n" + breit)
    print(f"bestanden {len(erg.bestanden)}  |  Fehler {len(erg.fehler)}  |  "
          f"Altlasten {len(erg.altlasten)}  |  Hinweise {len(erg.hinweise)}")
    print(breit)
    if erg.fehler:
        print("\nNicht die Pruefung anpassen. Die Dokumentation nachziehen -")
        print("die Ergebnisdateien sind die Wahrheit.\n")
        return 1
    print("\nAlle tragenden Zahlen stimmen mit results/ ueberein.\n")
    return 0


def _de(x: float) -> str:
    return f"{x}".replace(".", ",")


if __name__ == "__main__":
    sys.exit(laufe(ausfuehrlich="-v" in sys.argv))
