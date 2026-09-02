"""
Alle Abbildungen der Kapitel 4 und 7 - aus den CSV-Dateien, nicht von Hand.

    python modelle/m05_abbildungen.py

Eingang: results/regression/*.csv, results/klassifikation/*.csv,
         results/spezifikation/*.csv, results/eignungspruefung/qq_residuen.csv,
         results/shap/{ablation_exposition,gruppen,faktorgruppen_menge,
         extrapolation_zusammenhang}.csv,
         results/deskriptiv/{stadtteilprofil,varianzzerlegung,aufloesung}.csv
Ausgang: results/abbildungen/a1..a17.pdf

  - Dieses Skript RECHNET NICHTS, es liest nur. Dadurch laesst sich eine
    Darstellung aendern, ohne die Modelle neu zu rechnen
  - A16 und A17 sind am 22.08.2026 fuer KAPITEL 4 ergaenzt worden - die
    einzigen beiden Abbildungen, die nicht zu Kapitel 7 gehoeren. Sie lesen
    aus results/deskriptiv/, erzeugt von tools/deskriptiv.py, und zeigen
    ausschliesslich BEFUNDE ueber die Daten. Keine Verfahrensaussage, keine
    Fold-Zuordnung, kein Hold-out: beides waere ein Vorgriff auf Kapitel 5
    und verstiesse gegen den Abgrenzungsblock in main.tex (steht seit dem
    22.08.2026 vor Kapitel 5, nicht mehr vor Kapitel 4)
  - A11 bis A15 sind am 19.08.2026 fuer Kapitel 7 ergaenzt worden: A11
    Forest-Plot der gepaarten Differenzen (ersetzt A1), A12 Deckenleiter
    des Strukturstrangs, A13 Kreuzvalidierung gegen Hold-out (ersetzt A5),
    A14 Ueberanpassung, A15 Attribution gegen Ablation (ersetzt A6)
  - A1 gegen Baseline (Primaeraussage nach #34), A2 Foldstruktur (warum
    gepaart wird), A3 Spezifikation gegen Verfahren (UF4, B-41), A4 Laufzeit
    gegen Guete (UF3), A5 Hold-out, A6 Faktorgruppen (UF1), A7 Extrapolation
    (R-3), A8 Hyperparameterstabilitaet, A9 Parallelisierung (zweite Haelfte
    UF3), A10 QQ-Diagramm der Residuen (Auflage 10.08.2026)
  - WARUM GEPAART (Neuschnitt 07.08.2026): Die 50 Laeufe unterscheiden sich
    darin, welche Stadtteile im Testfold liegen - Bayview hat ein Vielfaches
    der Einsaetze von Seacliff, also schwankt der RMSE zwischen 13 und 76
    unabhaengig vom Verfahren. Rohwert-Streuung 12,4 bis 15,5 RMSE,
    Verfahrensunterschied rund 2. Jedes Verfahren sieht aber DIESELBEN Folds:
    Ueber die gepaarte Differenz kuerzt sich die Fold-Streuung heraus (2,4
    bis 4,3). Es ist dieselbe Paarung, auf der der Wilcoxon-Test beruht

ANFORDERUNGEN AN DIE DARSTELLUNG - Gestaltung war im Gutachten ein eigenes
Bewertungskriterium
  Format     PDF, nicht PNG. Rasterbilder werden im Druck unscharf
  Groesse    in der ENDGROESSE erzeugen, nicht in LaTeX schrumpfen, sonst
             steht dort 5-pt-Schrift. Mindestens 9 pt
  Titel      KEINE in der Abbildung - die Bildunterschrift ist der Titel
  Graustufen Verfahren zusaetzlich ueber Schraffur und Marker unterscheiden
  Achsen     Beschriftung mit Einheit, deutsches Dezimalkomma
  Nulllinie  bei Differenzen und R2 einzeichnen - das Vorzeichen ist die
             Aussage
  Richtung   an jeder Differenzachse muss stehen, welche Seite besser ist.
             Bei RMSE links, bei Macro-F1 rechts - wer das verwechselt, liest
             das Ergebnis genau falsch herum
  Streuung   immer benennen, worueber sie gebildet ist: ueber die 10
             Wiederholungsmittel, nicht ueber die 50 Einzellaeufe (R-5)

PRUEFAUFTRAEGE
  - Sind alle zehn PDF entstanden und in LaTeX einbindbar?
  - Schneidet in A1 die Nulllinie eine der Boxen? Dann darf im Text kein
    Unterschied behauptet werden, den der Test nicht deckt (R-6)
  - Traegt jede Differenzachse die Richtungsangabe, bei Macro-F1
    entgegengesetzt zu RMSE?
  - Stimmt der Referenzwert in A3 mit `linear` aus v3 und der Stufe-2-
    Baseline aus v1 ueberein? Alle drei muessen dieselbe Zahl sein
  - In Graustufen ausdrucken: sind die Verfahren noch unterscheidbar?
  - A6: Summiert sich jeder Balken auf 100 %, und steht in der Fusszeile,
    dass der Mengenbalken KOEFFIZIENTEN und die Strukturbalken SHAP-Werte
    zeigt? Die beiden Groessen sind nicht dasselbe
  - A7: Liegen die drei Verfahren bei gleichem x uebereinander? Muessen sie -
    der Extrapolationsanteil ist eine Eigenschaft des Folds
  - A8: Klebt ein Parameter am Rand seines Suchraums? Dann war der Raum zu
    eng, und das gehoert in die Limitationen
  - A9: Steht die Linie bei 1,0 und ist sie beschriftet? Werte darunter
    heissen "parallel langsamer"
  - A16: Sind es 36 Stadtteile, und stehen Tenderloin (279,7) oben und
    Seacliff (6,4) unten? Taucht irgendwo "Fold" oder "Hold-out" auf? Dann
    raus damit - Kapitel 4 kennt die Aufteilung noch nicht
  - A17: Stehen die drei baulichen Merkmale links bei 100 % und rechts bei 1,
    und die beiden Saisonterme links bei 0 %? Diese vier Punkte sind die
    Aussage der Abbildung. Liegt die gestrichelte Linie bei 92,5 %?

Setzt m02, m03, m04, v1 und v3 voraus. Ausfuehrliche Fassung:
docs/08_FUNKTIONSDOKUMENTATION.md
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "prep"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import RESULTS_DIR, ROOT  # noqa: E402
from config_modelle import SUCHRAEUME  # noqa: E402

OUT = RESULTS_DIR / "abbildungen"
REG = RESULTS_DIR / "regression"
KLA = RESULTS_DIR / "klassifikation"
SPEZ = RESULTS_DIR / "spezifikation"
SHAP = RESULTS_DIR / "shap"
DESK = RESULTS_DIR / "deskriptiv"

# Textbreite einer FOM-Arbeit bei A4 und 2,5 cm Raendern: rund 16 cm = 6,3 Zoll.
BREITE = 6.3
SCHRIFT = 9

# Graustufentauglich: Grauwert, Schraffur und Marker tragen die Unterscheidung
# gemeinsam. Wer die Arbeit schwarzweiss ausdruckt, sieht dasselbe.
STIL = {
    "ridge":         {"grau": "0.78", "schraffur": "//",   "marker": "o"},
    "random_forest": {"grau": "0.52", "schraffur": "\\\\", "marker": "s"},
    "xgboost":       {"grau": "0.26", "schraffur": "xx",   "marker": "^"},
}
LABEL = {"ridge": "Ridge", "random_forest": "Random Forest",
         "xgboost": "XGBoost",
         "anzahl_einsaetze": "Anzahl Einsätze",
         "einsaetze_je_1000_ew": "Einsätze je 1.000 Ew.",
         "dominante_einsatzart": "Dominante Einsatzart"}

POISSON = "Poisson-GLM"

# --------------------------------------------------------------------------
# Nur fuer A6 bis A9
# --------------------------------------------------------------------------
# Reihenfolge der Faktorgruppen: die drei des Exposes zuerst, danach die zwei
# getrennt gefuehrten Groessen. Identisch zu GRUPPEN in m04_shap.py - stuenden
# sie in anderer Reihenfolge, waeren Abbildung und Tabelle nicht vergleichbar.
GRUPPEN_ORDNUNG = ["soziooekonomisch", "kriminalitaetsbezogen", "baulich",
                   "groessenkontrolle", "saison"]

# Helle Fuellung plus eigene Schraffur je Gruppe. In gestapelten Balken traegt
# die Schraffur die Unterscheidung, nicht der Grauwert - fuenf Grautoene sind
# im Druck nicht mehr sicher auseinanderzuhalten.
GRUPPEN_STIL = {
    "soziooekonomisch":      ("0.92", ""),
    "kriminalitaetsbezogen": ("0.74", "//"),
    "baulich":               ("0.56", "xx"),
    "groessenkontrolle":     ("0.38", "\\\\"),
    "saison":                ("0.20", ".."),
}

LABEL_GRUPPE = {"soziooekonomisch": "sozioökonomisch",
                "kriminalitaetsbezogen": "kriminalitätsbezogen",
                "baulich": "baulich",
                "groessenkontrolle": "Größenkontrolle",
                "saison": "Saison"}

# Kurzformen fuer A8. Die Rohnamen sind sklearn-/XGBoost-Bezeichner und als
# Achsenbeschriftung zu lang; die Bildunterschrift nennt sie einmal vollstaendig.
LABEL_PARAMETER = {
    "alpha": "alpha", "n_estimators": "n_estimators", "max_depth": "max_depth",
    "min_samples_leaf": "min_samples_leaf", "max_features": "max_features",
    "learning_rate": "learning_rate", "subsample": "subsample",
    "colsample_bytree": "colsample_bytree", "reg_lambda": "reg_lambda",
    "tweedie_variance_power": "tweedie_power",
}

# Reihenfolge der Verfahren in A8 und A9 - die der Arbeit, nicht die
# alphabetische, in der Ridge zwischen den beiden Ensembles stuende.
RANG_VERFAHREN = {"ridge": 0, "random_forest": 1, "xgboost": 2}


def _sekunden(wert: float) -> str:
    """Beschriftet Sekundenwerte lesbar.

    Ein:  Zahl in Sekunden
    Aus:  Zeichenkette mit Dezimalkomma

    - zwei Nachkommastellen reichen fuer die Ensembles (5,83 s), nicht fuer Ridge
      (0,011 s); dort stuende sonst zweimal "0,01 s" und die Abbildung
      behauptete, der parallele Fit sei gleich schnell gewesen
    """
    return (f"{wert:.2f}" if wert >= 1 else f"{wert:.3f}").replace(".", ",") + " s"


# Von _matplotlib() belegt. Vorher absichtlich None: Wer eine
# Abbildungsfunktion ohne vorheriges _matplotlib() ruft, soll sofort scheitern
# statt mit halb gesetzten rcParams zu zeichnen.
plt = None
FuncFormatter = None


def _matplotlib() -> None:
    """Setzt Schriftgroessen, Schrift und Rahmen fuer alle Abbildungen.

    Ein:  nichts
    Aus:  nichts; belegt die Modulglobalen plt und FuncFormatter und wirkt
          auf die globalen rcParams
    """
    global plt, FuncFormatter
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    plt.rcParams.update({
        "font.size": SCHRIFT, "axes.titlesize": SCHRIFT,
        "axes.labelsize": SCHRIFT, "xtick.labelsize": SCHRIFT - 1,
        "ytick.labelsize": SCHRIFT - 1, "legend.fontsize": SCHRIFT - 1,
        "figure.constrained_layout.use": True, "pdf.fonttype": 42,
        "axes.spines.top": False, "axes.spines.right": False,
    })


def _komma(stellen: int = 2, vorzeichen: bool = False):
    """Deutsches Dezimalkomma auf den Achsen.

    Ein:  Zahl, Nachkommastellen, Schalter fuer explizites Vorzeichen
    Aus:  Zeichenkette

    - `stellen` ist nicht kosmetisch: Macro-F1 liegt zwischen 0,301 und 0,318;
      mit zwei Nachkommastellen stuende an allen Achsenmarken "0,33"
    - `vorzeichen` setzt auf Differenzachsen ein explizites Plus, sonst liest sich
      "2,5" wie ein Absolutwert statt wie ein Abstand
    """
    fmt = "{:+,." + str(stellen) + "f}" if vorzeichen else "{:,." + str(stellen) + "f}"
    return FuncFormatter(lambda x, _: fmt.format(x).replace(",", " ")
                         .replace(".", ",").replace(" ", "."))


def _prozent(stellen: int = 0):
    """Prozentwert mit deutschem Dezimalkomma.

    Ein:  Anteil zwischen 0 und 1
    Aus:  Zeichenkette
    """
    fmt = "{:." + str(stellen) + "f}"
    return FuncFormatter(lambda x, _: fmt.format(x * 100).replace(".", ",") + " %")


def _text(pfad: Path) -> pd.DataFrame | None:
    """Setzt einen Textblock unter die Abbildung.

    Ein:  Figur, Text
    Aus:  nichts
    """
    return pd.read_csv(pfad) if pfad.exists() else None


# ===========================================================================
def _speichere(fig, datei: str) -> list:
    """Legt eine Abbildung in results/abbildungen ab und schliesst sie.

    Ein:  die Figur und ihr Dateiname
    Aus:  einstellige Liste mit dem Pfad - so, wie main() sie erwartet

    - `bbox_inches="tight"` gilt fuer alle Abbildungen dieser Datei bis auf
      eine: A2 speichert ohne, weil dort constrained_layout die Breite schon
      exakt setzt. Deshalb ruft A2 diesen Helfer nicht und speichert selbst -
      die drei Zeilen dort sind Absicht, keine vergessene Umstellung.
    """
    fig.savefig(OUT / datei, bbox_inches="tight")
    plt.close(fig)
    return [OUT / datei]


def _gepaarte_differenz() -> list[dict]:
    """Je Verfahren die 10 Wiederholungsmittel der Differenz zur Baseline.

    Ein:  Laufdatei des Strangs, Baseline-Laeufe
    Aus:  dict Verfahren -> zehn Differenzen

    - gepaart wird auf (wiederholung, fold), also auf identischen Testzeilen
    - dieselbe Paarung liegt dem Wilcoxon-Test in m02/m03 zugrunde; die Abbildung
      zeigt damit die getestete Groesse und nicht eine aehnlich aussehende andere
    """
    reihen = []

    f, b = _text(REG / "menge_folds.csv"), _text(REG / "baselines_folds.csv")
    if f is not None and b is not None:
        basis = (b[b["stufe"] == 2][["wiederholung", "fold", "zielgroesse", "RMSE"]]
                 .rename(columns={"RMSE": "basis"}))
        m = f.merge(basis, on=["wiederholung", "fold", "zielgroesse"], how="inner")
        assert len(m) == len(f), "Zu einem Lauf fehlt der Baseline-Gegenwert."
        m["differenz"] = m["RMSE"] - m["basis"]
        for ziel in dict.fromkeys(m["zielgroesse"]):
            reihen.append({"ziel": ziel, "achse": "Δ RMSE",
                           "besser": "links", "stellen": 1,
                           "daten": m[m["zielgroesse"] == ziel]})

    k, kb = _text(KLA / "struktur_folds.csv"), _text(KLA / "baselines_klasse.csv")
    if k is not None and kb is not None:
        basis = (kb[kb["stufe"] == 2][["wiederholung", "fold", "Macro-F1"]]
                 .rename(columns={"Macro-F1": "basis"}))
        m = k.merge(basis, on=["wiederholung", "fold"], how="inner")
        assert len(m) == len(k), "Zu einem Lauf fehlt der Baseline-Gegenwert."
        m["differenz"] = m["macro_f1"] - m["basis"]
        reihen.append({"ziel": "dominante_einsatzart", "achse": "Δ Macro-F1",
                       "besser": "rechts", "stellen": 2, "daten": m})
    return reihen


def a1_gegen_baseline() -> list:
    """A1: jedes Verfahren gegen seine Stufe-2-Baseline (Primaeraussage).

    Ein:  menge_folds.csv, struktur_folds.csv, beide Baseline-Dateien
    Aus:  a1_gegen_baseline.pdf

    - dargestellt sind die 10 Wiederholungsmittel als Punkte und ihre Verteilung
      als Kasten
    - ein Fehlerbalken waere schlechter: bei zehn Werten zeigt der Punktschwarm
      die Verteilung selbst, statt Symmetrie zu unterstellen
    - die Nulllinie ist die Baseline; wo der Kasten sie schneidet, ist der
      Unterschied nicht gesichert - unabhaengig vom Mittelwert
    """
    reihen = _gepaarte_differenz()
    if not reihen:
        return []

    fig, achsen = plt.subplots(1, len(reihen), figsize=(BREITE, 2.9))
    achsen = np.atleast_1d(achsen)
    streu = np.random.default_rng(20260807)

    for ax, reihe in zip(achsen, reihen):
        d = reihe["daten"]
        verf = [v for v in ("ridge", "random_forest", "xgboost")
                if v in set(d["verfahren"])]
        ypos = np.arange(len(verf))[::-1]
        for y, v in zip(ypos, verf):
            wdh = (d[d["verfahren"] == v]
                   .groupby("wiederholung")["differenz"].mean().to_numpy())
            kasten = ax.boxplot([wdh], positions=[y], vert=False, widths=0.52,
                                patch_artist=True, showfliers=False,
                                medianprops={"color": "black", "linewidth": 1.5})
            kasten["boxes"][0].set_facecolor(STIL[v]["grau"])
            kasten["boxes"][0].set_hatch(STIL[v]["schraffur"])
            kasten["boxes"][0].set_edgecolor("black")
            ax.plot(wdh, np.full(len(wdh), y) + streu.uniform(-0.14, 0.14, len(wdh)),
                    linestyle="none", marker="o", markersize=2.8,
                    markerfacecolor="white", markeredgecolor="black",
                    markeredgewidth=0.6, zorder=3)
        ax.axvline(0, color="black", linewidth=1.3, linestyle="--", zorder=1)
        ax.set_yticks(ypos)
        ax.set_yticklabels([LABEL[v] for v in verf])
        ax.set_ylim(-0.62, len(verf) - 0.38)
        ax.set_xlabel(reihe["achse"])
        ax.xaxis.set_major_formatter(
            _komma(reihe["stellen"], vorzeichen=True))
        pfeil = ("← besser" if reihe["besser"] == "links" else "besser →")
        ax.set_title(f"{LABEL.get(reihe['ziel'], reihe['ziel'])}\n{pfeil}",
                     fontsize=SCHRIFT - 1)

    fig.supxlabel("Abstand zur Stufe-2-Baseline. Ein Punkt je Wiederholung (10), "
                  "gepaart je Fold — die Fold-Streuung kürzt sich heraus.",
                  fontsize=SCHRIFT - 2)
    return _speichere(fig, "a1_gegen_baseline.pdf")


# ===========================================================================
def a2_foldstruktur() -> list:
    """A2: Rohwerte je Fold - Begruendung fuer die Paarung in A1.

    Ein:  menge_folds.csv
    Aus:  a2_foldstruktur.pdf

    - die Folds bewegen alle Verfahren gemeinsam; die Streuung stammt aus dem
      Fold, nicht aus dem Verfahren
    - gezeigt wird eine einzelne Wiederholung, sonst waeren es 50 Linien
    - die Aussage haengt nicht an der Auswahl; die Streuungszerlegung in der
      Fusszeile belegt das
    """
    f, b = _text(REG / "menge_folds.csv"), _text(REG / "baselines_folds.csv")
    if f is None or b is None:
        return []
    ziel = "anzahl_einsaetze"
    f = f[(f["zielgroesse"] == ziel) & (f["wiederholung"] == 0)]
    b = b[(b["zielgroesse"] == ziel) & (b["wiederholung"] == 0)
          & (b["stufe"] == 2)].sort_values("fold")

    fig, ax = plt.subplots(figsize=(BREITE, 2.7))
    x = b["fold"].to_numpy()
    ax.plot(x, b["RMSE"], color="black", linestyle="--", marker="D",
            markersize=5, linewidth=1.5, label="Poisson-GLM (Baseline)", zorder=4)
    for v in ("ridge", "random_forest", "xgboost"):
        g = f[f["verfahren"] == v].sort_values("fold")
        if not len(g):
            continue
        ax.plot(g["fold"], g["RMSE"], marker=STIL[v]["marker"], markersize=5,
                linewidth=1.0, color=STIL[v]["grau"], markeredgecolor="black",
                markeredgewidth=0.6, label=LABEL[v])
    ax.set_xticks(x)
    ax.set_xlabel("Fold (Wiederholung 1)")
    ax.set_ylabel("RMSE")
    ax.yaxis.set_major_formatter(_komma(0))
    ax.legend(frameon=False, ncol=2, loc="upper left")
    unten, oben = ax.get_ylim()
    ax.set_ylim(unten, oben + (oben - unten) * 0.30)

    # Die Zerlegung als Zahl dazu - sie ist der eigentliche Grund fuer A1.
    roh = f.groupby("verfahren")["RMSE"].std().mean()
    fig.supxlabel(f"Streuung der Rohwerte über die 50 Läufe: {roh:.1f} RMSE. "
                  f"Streuung der gepaarten Differenz: 2,4 bis 4,3."
                  .replace(".", ",", 1),
                  fontsize=SCHRIFT - 2)
    fig.savefig(OUT / "a2_foldstruktur.pdf")   # ohne bbox_inches, s. _speichere
    plt.close(fig)
    return [OUT / "a2_foldstruktur.pdf"]


# ===========================================================================
def _spezifikationszeilen() -> list[tuple[str, float, str]]:
    """Sammelt die Balkenwerte fuer A3 aus drei Ergebnisdateien.

    Ein:  spezifikation_mittel.csv, menge_mittel.csv, baselines_mittel.csv
    Aus:  Liste (Gruppe, Beschriftung, RMSE)
    """
    zeilen = []
    b = _text(REG / "baselines_mittel.csv")
    if b is None:
        return []
    ref = b[(b["stufe"] == 2) & (b["zielgroesse"] == "anzahl_einsaetze")]
    if not len(ref):
        return []
    zeilen.append(("Poisson-GLM (Referenz)", float(ref["RMSE_mean"].iloc[0]),
                   "referenz"))

    m = _text(REG / "menge_mittel.csv")
    if m is not None:
        for _, z in m[m["zielgroesse"] == "anzahl_einsaetze"].iterrows():
            zeilen.append((LABEL[z["verfahren"]], float(z["RMSE_mean"]),
                           "verfahren"))

    # Die Expositions-Ablation stand hier frueher als zwei weitere Balken.
    # Sie beantwortet aber eine andere Frage als die Spezifikationsgegenprobe
    # und gehoert deshalb nach 8.1; die Werte liegen in
    # results/shap/ablation_exposition.csv bereit.

    s = _text(SPEZ / "spezifikation_mittel.csv")
    if s is not None:
        namen = {"quadrate": "Poisson-GLM + quadratische Terme",
                 "interaktionen": "Poisson-GLM + Interaktionen",
                 "beides": "Poisson-GLM + beides"}
        for _, z in s.iterrows():
            if z["spezifikation"] in namen:
                zeilen.append((namen[z["spezifikation"]], float(z["RMSE_mean"]),
                               "spezifikation"))
    return zeilen


def a3_spezifikation() -> list:
    """A3: Verfahren gegen Spezifikation (Unterfrage 4).

    Ein:  spezifikation_mittel.csv, menge_mittel.csv, baselines_mittel.csv
    Aus:  a3_spezifikation.pdf

    - alle Werte sind RMSE auf anzahl_einsaetze, gemittelt ueber dieselben 50
      Laeufe und damit unmittelbar vergleichbar
    - die Balken sind nach Gruppen sortiert, nicht global; sonst stuende die
      Referenz mitten zwischen den Verfahren
    """
    zeilen = _spezifikationszeilen()
    if len(zeilen) < 4:
        return []

    rang = {"referenz": 0, "verfahren": 1, "spezifikation": 2}
    zeilen.sort(key=lambda z: (rang[z[2]], z[1]))
    fuellung = {"referenz": "0.15", "verfahren": "0.55", "spezifikation": "0.88"}
    schraffur = {"referenz": None, "verfahren": None, "spezifikation": "///"}

    fig, ax = plt.subplots(figsize=(BREITE, 3.4))
    y = np.arange(len(zeilen))[::-1]
    referenz = zeilen[0][1]
    for yy, (name, wert, gruppe) in zip(y, zeilen):
        ax.barh(yy, wert, height=0.64, color=fuellung[gruppe],
                edgecolor="black", linewidth=0.8, hatch=schraffur[gruppe])
        ax.text(wert + max(z[1] for z in zeilen) * 0.015, yy,
                f"{wert:.1f}".replace(".", ","),
                va="center", fontsize=SCHRIFT - 1.5)
    ax.axvline(referenz, color="black", linewidth=1.0, linestyle="--", zorder=0)
    ax.set_yticks(y)
    ax.set_yticklabels([z[0] for z in zeilen], fontsize=SCHRIFT - 1)
    ax.set_xlabel("RMSE (Anzahl Einsätze), Mittel über 50 Läufe")
    ax.set_xlim(0, max(z[1] for z in zeilen) * 1.16)
    ax.xaxis.set_major_formatter(_komma(0))

    # Gruppenklammern am rechten Rand - ausserhalb der Balken, damit sie
    # nichts ueberdecken. Die Spannweite je Gruppe ist die eigentliche Aussage.
    for gruppe, beschriftung in (("verfahren", "Wahl des Verfahrens"),
                                 ("spezifikation", "Wahl der Spezifikation")):
        idx = [i for i, z in enumerate(zeilen) if z[2] == gruppe]
        if not idx:
            continue
        oben, unten = y[idx[0]], y[idx[-1]]
        spanne = max(zeilen[i][1] for i in idx) - referenz
        ax.annotate("", xy=(1.005, (unten - 0.4 + 0.62) / (len(zeilen))),
                    xytext=(1.005, (oben + 0.4 + 0.62) / (len(zeilen))),
                    xycoords="axes fraction",
                    arrowprops={"arrowstyle": "-", "linewidth": 1.0})
        ax.text(1.02, (oben + unten + 1.24) / (2 * len(zeilen)),
                f"{beschriftung}\nbis {spanne:.1f} RMSE".replace(".", ","),
                transform=ax.transAxes, va="center", fontsize=SCHRIFT - 2)
    return _speichere(fig, "a3_spezifikation.pdf")


# ===========================================================================
def a4_laufzeit_guete() -> list:
    """A4: Aufwand gegen Guete, ein Punkt je Verfahren (Unterfrage 3).

    Ein:  menge_folds.csv, struktur_folds.csv
    Aus:  a4_laufzeit_guete.pdf

    - die Zeitachse ist logarithmisch, weil zwischen Ridge und den Ensembles
      Groessenordnungen liegen; linear waere Ridge ein Punkt auf der Null
    - aufgetragen ist die einkernige Zeit; was Kerne bringen, zeigt A9
    """
    from matplotlib.lines import Line2D
    from matplotlib.ticker import LogLocator, NullFormatter

    punkte, latten = [], {}
    m = _text(REG / "menge_mittel.csv")
    if m is not None:
        for _, z in m.iterrows():
            punkte.append((z["zielgroesse"], z["verfahren"],
                           z["train_sekunden_mean"], z["RMSE_mean"], "RMSE", 2))
        b = _text(REG / "baselines_mittel.csv")
        if b is not None:
            for _, z in b[b["stufe"] == 2].iterrows():
                latten[z["zielgroesse"]] = z["RMSE_mean"]
    k = _text(KLA / "struktur_mittel.csv")
    if k is not None:
        for _, z in k.iterrows():
            punkte.append((z["zielgroesse"], z["verfahren"],
                           z["train_sekunden_mean"], z["macro_f1_mean"],
                           "Macro-F1", 3))
        b = _text(KLA / "baselines_klasse_mittel.csv")
        if b is not None and len(b[b["stufe"] == 2]):
            latten[k["zielgroesse"].iloc[0]] = \
                b[b["stufe"] == 2]["Macro-F1_mean"].iloc[0]
    if not punkte:
        return []

    df = pd.DataFrame(punkte, columns=["ziel", "verfahren", "zeit", "guete",
                                       "mass", "stellen"])
    ziele = list(dict.fromkeys(df["ziel"]))
    fig, achsen = plt.subplots(1, len(ziele), figsize=(BREITE, 2.9))
    achsen = np.atleast_1d(achsen)
    for ax, ziel in zip(achsen, ziele):
        g = df[df["ziel"] == ziel]
        # Die Stufe-2-Baseline als Linie. Ohne sie ist der sehr enge
        # Wertebereich nicht einzuordnen - 0,9 RMSE Unterschied saehen aus wie
        # ein Abgrund, obwohl alle drei Verfahren dicht an der Latte liegen.
        if ziel in latten:
            ax.axhline(latten[ziel], color="black", linewidth=1.0,
                       linestyle="--", zorder=1)
        for _, z in g.iterrows():
            ax.scatter(z["zeit"], z["guete"], s=45,
                       marker=STIL[z["verfahren"]]["marker"],
                       facecolor=STIL[z["verfahren"]]["grau"],
                       edgecolor="black", label=LABEL[z["verfahren"]], zorder=3)
        ax.set_xscale("log")
        # Mindestens eine Dekade Spannweite, sonst stehen nur Nebenmarken da
        # und ihre Beschriftungen ueberlappen sich unlesbar.
        ax.set_xlim(g["zeit"].min() / 4, g["zeit"].max() * 4)
        ax.xaxis.set_major_locator(LogLocator(base=10))
        ax.xaxis.set_minor_formatter(NullFormatter())
        # Kurz halten: bei drei Feldern auf Textbreite bleibt je Feld rund
        # 2 Zoll, eine laengere Beschriftung wird am rechten Rand abgeschnitten.
        ax.set_xlabel("Trainingszeit (s, log)")
        ax.set_ylabel(g["mass"].iloc[0])
        ax.yaxis.set_major_formatter(
            _komma(int(g["stellen"].iloc[0])))
        unten, oben = ax.get_ylim()
        ax.set_ylim(unten, oben + (oben - unten) * 0.22)
        ax.text(0.03, 0.96, LABEL.get(ziel, ziel), transform=ax.transAxes,
                fontsize=SCHRIFT - 1, va="top")
        ax.grid(True, which="major", linewidth=0.3, alpha=0.5)

    kennzeichen, namen = [], []
    for ax in achsen:
        for h, l in zip(*ax.get_legend_handles_labels()):
            if l not in namen:
                kennzeichen.append(h); namen.append(l)
    kennzeichen.append(Line2D([], [], color="black", linestyle="--"))
    namen.append("Stufe-2-Baseline")
    fig.legend(kennzeichen, namen, loc="lower center", ncol=len(namen),
               frameon=False, bbox_to_anchor=(0.5, -0.08))
    return _speichere(fig, "a4_laufzeit_guete.pdf")


# ===========================================================================
def a5_holdout() -> list:
    """A5: die einmalige Auswertung auf den sechs zurueckgehaltenen Stadtteilen.

    Ein:  holdout.csv beider Straenge
    Aus:  a5_holdout.pdf

    - keine Streuung, anders als in A1: das Hold-out wird genau einmal
      ausgewertet, Fehlerbalken waeren falsch
    - alle drei Stufen stehen nebeneinander, damit sichtbar bleibt, wovon der
      Abstand gemessen wird
    """
    aufgaben = []
    h = _text(REG / "holdout.csv")
    if h is not None:
        for ziel in dict.fromkeys(h["zielgroesse"]):
            aufgaben.append((h[h["zielgroesse"] == ziel], "RMSE", "RMSE",
                             LABEL.get(ziel, ziel), 1, False))
    hk = _text(KLA / "holdout.csv")
    if hk is not None:
        aufgaben.append((hk, "macro_f1", "Macro-F1",
                         LABEL["dominante_einsatzart"], 2, True))
    if not aufgaben:
        return []

    fig, achsen = plt.subplots(1, len(aufgaben), figsize=(BREITE, 3.0))
    achsen = np.atleast_1d(achsen)
    for ax, (d, spalte, einheit, titel, stellen, hoch_gut) in zip(achsen, aufgaben):
        d = d.sort_values("stufe")
        # Die Baselines heissen in der CSV so, wie sie in Kapitel 5 heissen -
        # ausgeschrieben. Als Achsenbeschriftung sind sie zu lang und
        # ueberdecken die Nachbarfelder; hier stehen die Kurzformen.
        kurz = {"Gesamtmittelwert": "Mittelwert",
                "Mehrheitsklasse": "Mehrheitsklasse",
                "Multinomiale logistische Regression": "Logit"}
        namen, werte, stufen = [], [], []
        for _, z in d.iterrows():
            v = str(z["verfahren"]).split(" (")[0]
            namen.append(LABEL.get(str(z["verfahren"]), kurz.get(v, v)))
            werte.append(float(z[spalte]))
            stufen.append(int(z["stufe"]))
        grau = {1: "0.92", 2: "0.15", 3: "0.55"}
        x = np.arange(len(namen))
        for i, (w, s) in enumerate(zip(werte, stufen)):
            ax.bar(i, w, width=0.64, color=grau[s], edgecolor="black",
                   linewidth=0.8, hatch="///" if s == 1 else None)
        # Die Stufe-2-Linie durchgezogen: Sie ist die Messlatte, an der die
        # Stufe-3-Balken abzulesen sind.
        if 2 in stufen:
            ax.axhline(werte[stufen.index(2)], color="black", linewidth=1.0,
                       linestyle="--", zorder=0)
        ax.set_xticks(x)
        ax.set_xticklabels(namen, rotation=28, ha="right",
                           fontsize=SCHRIFT - 2)
        ax.set_ylabel(einheit)
        ax.yaxis.set_major_formatter(_komma(stellen))
        ax.set_title(titel, fontsize=SCHRIFT - 1)
    fig.supxlabel("Sechs zurückgehaltene Stadtteile, einmalig ausgewertet — "
                  "hell: Stufe 1, schwarz: Stufe 2, grau: Stufe 3.",
                  fontsize=SCHRIFT - 2)
    return _speichere(fig, "a5_holdout.pdf")


# ===========================================================================
def _faktorgruppen_balken() -> list[tuple[str, str, pd.Series]]:
    """Anteile je Faktorgruppe fuer einen Strang.

    Ein:  gruppen.csv bzw. faktorgruppen_menge.csv
    Aus:  (Strang, Beschriftung, Anteile je Gruppe)

    - zwei Quellen, zwei Groessen: der Mengenbalken zeigt standardisierte
      KOEFFIZIENTEN des Poisson-GLM, die Strukturbalken zeigen SHAP-BEITRAEGE
    - beide sind auf Summe 1 normiert und nebeneinander lesbar, aber nicht
      dieselbe Groesse - daher die Fusszeile der Abbildung
    - dass die Menge aus der Baseline kommt, ist kein Notbehelf: m04 ueberspringt
      dort jedes Vergleichsverfahren, weil keines seine Baseline schlaegt
    """
    balken = []

    m = _text(SHAP / "faktorgruppen_menge.csv")
    if m is not None and len(m):
        balken.append(("Menge", POISSON, m.groupby("gruppe")["anteil"].sum()))

    g = _text(SHAP / "gruppen.csv")
    if g is not None and len(g):
        for strang, titel in (("menge", "Menge"), ("struktur", "Struktur")):
            teil = g[g["strang"] == strang]
            for verf in dict.fromkeys(teil["verfahren"]):
                z = teil[teil["verfahren"] == verf]
                balken.append((titel, LABEL.get(verf, verf),
                               z.set_index("gruppe")["anteil"]))
    return balken


def a6_faktorgruppen() -> list:
    """A6: Welche Faktorgruppe traegt wie viel? (Unterfrage 1)

    Ein:  gruppen.csv, faktorgruppen_menge.csv
    Aus:  a6_faktorgruppen.pdf

    - gestapelte statt gruppierter Balken: die Anteile summieren sich je Modell
      auf 100 %, und diese Aufteilung ist die Aussage
    - gruppierte Balken luden zum Vergleich einer Gruppe zwischen den Modellen
      ein; das traegt nicht, weil die Werte aus verschiedenen Groessen stammen
    - die Segmente sind ueber Schraffur unterschieden: fuenf Grautoene in einem
      Balken sind im Schwarzweissdruck nicht sicher zu trennen
    """
    balken = _faktorgruppen_balken()
    if not balken:
        return []

    from matplotlib.patches import Patch

    fig, ax = plt.subplots(figsize=(BREITE, 1.5 + 0.46 * len(balken)))
    y = np.arange(len(balken))[::-1]

    for yy, (_, _, anteile) in zip(y, balken):
        links = 0.0
        for gruppe in GRUPPEN_ORDNUNG:
            wert = float(anteile.get(gruppe, 0.0))
            if wert <= 0:
                continue
            fuellung, schraffur = GRUPPEN_STIL[gruppe]
            ax.barh(yy, wert, left=links, height=0.58, color=fuellung,
                    edgecolor="black", linewidth=0.7, hatch=schraffur or None)
            # Zahl nur, wo das Segment sie traegt. Ein Label, das in den
            # Nachbarn ragt, ist schlimmer als kein Label. Der weisse Kasten
            # dahinter ist noetig, weil die Schraffur sonst durch die Ziffern
            # laeuft - im Druck sind sie dann nicht mehr sicher lesbar.
            if wert >= 0.10:
                ax.text(links + wert / 2, yy, f"{wert * 100:.0f} %",
                        ha="center", va="center", fontsize=SCHRIFT - 2,
                        color="black",
                        bbox={"facecolor": "white", "edgecolor": "none",
                              "pad": 1.2, "alpha": 0.9})
            links += wert

    ax.set_yticks(y)
    ax.set_yticklabels([f"{name}\n({strang})" for strang, name, _ in balken],
                       fontsize=SCHRIFT - 1)
    ax.set_xlim(0, 1)
    ax.set_xlabel("Anteil am erklärten Beitrag")
    ax.xaxis.set_major_formatter(_prozent())
    ax.set_ylim(-0.6, len(balken) - 0.4)

    felder = [Patch(facecolor=GRUPPEN_STIL[g][0], edgecolor="black",
                    hatch=GRUPPEN_STIL[g][1] or None, label=LABEL_GRUPPE[g])
              for g in GRUPPEN_ORDNUNG]
    ax.legend(handles=felder, frameon=False, ncol=3, loc="upper center",
              bbox_to_anchor=(0.5, -0.28), fontsize=SCHRIFT - 2)

    fig.supxlabel("Menge: standardisierte Koeffizienten des Poisson-GLM. "
                  "Struktur: SHAP-Beiträge. Beide auf Summe 100 % normiert, "
                  "aber nicht dieselbe Größe.", fontsize=SCHRIFT - 2)
    return _speichere(fig, "a6_faktorgruppen.pdf")


# ===========================================================================
def a7_extrapolation() -> list:
    """A7: Extrapolationsanteil gegen Fehler, 50 Punkte je Verfahren.

    Ein:  extrapolation_zusammenhang.csv, menge_folds.csv
    Aus:  a7_extrapolation.pdf

    - macht R-3 sichtbar und liefert die Begruendung, die A2 nur behauptet
    - die drei Verfahren liegen bei gleichem x uebereinander, weil der
      Extrapolationsanteil eine Eigenschaft des FOLDS ist; das ist kein
      Darstellungsfehler, sondern die halbe Aussage
    - Spearman-rho steht in der Abbildung
    - Abgrenzung zu #34 wie in m04.extrapolation_aufschluesseln: Die Testmenge
      wird nicht nach Extrapolationsgrad geschnitten, die Einheit bleibt der Lauf
    """
    f = _text(REG / "menge_folds.csv")
    if f is None:
        return []
    rho = _text(SHAP / "extrapolation_zusammenhang.csv")

    ziele = list(dict.fromkeys(f["zielgroesse"]))
    fig, achsen = plt.subplots(1, len(ziele), figsize=(BREITE, 3.2))
    achsen = np.atleast_1d(achsen)

    for ax, ziel in zip(achsen, ziele):
        d = f[f["zielgroesse"] == ziel]
        for v in ("ridge", "random_forest", "xgboost"):
            g = d[d["verfahren"] == v]
            if not len(g):
                continue
            ax.scatter(g["extrapolationsanteil"], g["RMSE"], s=15,
                       marker=STIL[v]["marker"], facecolor=STIL[v]["grau"],
                       edgecolor="black", linewidth=0.45, alpha=0.85,
                       label=LABEL[v], zorder=3)
        ax.set_xlabel("Anteil extrapolierter Testzeilen")
        ax.set_ylabel("RMSE")
        ax.xaxis.set_major_formatter(_prozent())
        ax.yaxis.set_major_formatter(
            _komma(0 if d["RMSE"].max() > 10 else 1))
        ax.grid(True, which="major", linewidth=0.3, alpha=0.5)
        ax.set_title(LABEL.get(ziel, ziel), fontsize=SCHRIFT - 1)

        # Spearman statt Pearson: Der Zusammenhang muss nicht linear sein, und
        # die Rangkorrelation ist gegen die wenigen sehr schweren Folds robust.
        if rho is not None:
            texte = [f"{LABEL.get(z['verfahren'], z['verfahren'])}  "
                     f"ρ = {z['spearman_rho']:.2f}".replace(".", ",")
                     for _, z in rho[rho["zielgroesse"] == ziel].iterrows()]
            if texte:
                ax.text(0.03, 0.97, "\n".join(texte), transform=ax.transAxes,
                        va="top", ha="left", fontsize=SCHRIFT - 3,
                        bbox={"facecolor": "white", "edgecolor": "0.7",
                              "linewidth": 0.5, "pad": 2.5})
        unten, oben = ax.get_ylim()
        ax.set_ylim(unten, oben + (oben - unten) * 0.18)

    kennzeichen, namen = achsen[0].get_legend_handles_labels()
    fig.legend(kennzeichen, namen, loc="lower center", ncol=len(namen),
               frameon=False, bbox_to_anchor=(0.5, -0.10))
    fig.supxlabel("Ein Punkt je Lauf (50 je Verfahren). Die Verfahren liegen "
                  "bei gleichem x übereinander — der Extrapolationsanteil "
                  "gehört zum Fold, nicht zum Verfahren.",
                  fontsize=SCHRIFT - 2)
    return _speichere(fig, "a7_extrapolation.pdf")


# ===========================================================================
def _lage_im_suchraum(name: str, parameter: str, wert) -> float | None:
    """Relative Lage eines gefundenen Wertes in seinem Suchraum, 0 bis 1.

    Ein:  Parametername, gefundener Wert
    Aus:  Zahl zwischen 0 und 1, oder None

    - ohne Normierung liessen sich die Parameter nicht gemeinsam darstellen:
      alpha laeuft ueber sechs Zehnerpotenzen, subsample ueber 0,4 Einheiten
    - die Frage lautet nicht "welcher Wert", sondern "wie weit streuen die fuenf
      Folds im verfuegbaren Raum"
    - die Umrechnung spiegelt m02.suchraum(): loguniform logarithmisch, int und
      uniform linear, choice ueber die Listenposition
    - faellt ein Wert aus seinem Raum, gibt es None; dann hat sich der Suchraum
      seit dem Lauf geaendert und die Zeile fehlt, statt eine falsche Lage
      vorzutaeuschen
    """
    spez = SUCHRAEUME.get(name, {}).get(parameter)
    if spez is None or wert is None:
        return None
    art, *werte = spez
    if art == "loguniform":
        a, b = np.log(float(werte[0])), np.log(float(werte[1]))
        return float((np.log(float(wert)) - a) / (b - a))
    if art in ("int", "uniform"):
        a, b = float(werte[0]), float(werte[1])
        return float((float(wert) - a) / (b - a))
    if art == "choice":
        moeglich = list(werte[0])
        if wert not in moeglich:
            return None
        return float(moeglich.index(wert) / max(len(moeglich) - 1, 1))
    return None


def _hyperparameter_lagen() -> pd.DataFrame:
    """Bereitet die Fold-Parametersaetze fuer A8 auf.

    Ein:  tuning.csv eines Strangs
    Aus:  Liste (Verfahren, Parameter, Lagen der fuenf Folds)

    - in der Regression steht jeder Suchlauf zweimal in der Datei, einmal je
      Zielgroesse; gesucht wurde aber nur einmal auf der Rate (#43)
    - ohne Entdopplung stuenden zehn statt fuenf Punkte je Parameter und die
      Streuung saehe halb so gross aus
    """
    zeilen = []
    for strang, pfad in (("Menge", REG / "tuning.csv"),
                         ("Struktur", KLA / "tuning.csv")):
        t = _text(pfad)
        if t is None or "parameter_json" not in t.columns:
            continue
        # In der Regression steht jeder Suchlauf ZWEIMAL in der Datei, einmal
        # je Zielgroesse - gesucht wurde aber nur einmal, auf der Rate (#43).
        # Ohne diese Entdopplung stuenden hier zehn statt fuenf Punkte je
        # Parameter, und die Streuung saehe nur halb so gross aus.
        t = t.drop_duplicates(subset=["verfahren", "fold"])
        for _, z in t.iterrows():
            for schluessel, wert in json.loads(z["parameter_json"]).items():
                # Bei Ridge liegt der Schaetzer zwei Ebenen tief in der
                # Pipeline; der Praefix gehoert nicht in die Beschriftung.
                p = schluessel.split("__")[-1]
                lage = _lage_im_suchraum(z["verfahren"], p, wert)
                if lage is None:
                    continue
                zeilen.append({"strang": strang, "verfahren": z["verfahren"],
                               "parameter": p, "fold": int(z["fold"]),
                               "lage": lage})
    return pd.DataFrame(zeilen)


def a8_hyperparameter() -> list:
    """A8: Stabilitaet der Modellwahl bei 30 Entwicklungsstadtteilen.

    Ein:  tuning.csv beider Straenge
    Aus:  a8_hyperparameter.pdf

    - jede Zeile ist ein Hyperparameter, die fuenf Punkte sind die fuenf Folds,
      die graue Strecke ist der volle Suchraum
    - streuen die Punkte ueber die ganze Strecke, hat die Kreuzvalidierung diesen
      Parameter nicht bestimmt; das Tuning waehlt dann faktisch zufaellig
    - das ist eine Aussage fuer Kapitel 8 und keine Fehlermeldung: bei 24
      Trainingsstadtteilen je Fold ist es zu erwarten
    - die Spannweite rechts ist die Kennzahl dazu; 1,00 heisst "von Rand zu Rand"
    """
    d = _hyperparameter_lagen()
    if not len(d):
        return []

    # Menge vor Struktur, innerhalb dessen die Verfahrensreihenfolge der Arbeit
    # (Ridge, Random Forest, XGBoost) - nicht die alphabetische, in der Ridge
    # zwischen den Ensembles stuende.
    d = d.assign(_s=d["strang"].map({"Menge": 0, "Struktur": 1}),
                 _v=d["verfahren"].map(RANG_VERFAHREN).fillna(9))
    d = d.sort_values(["_s", "_v", "parameter"])
    reihen = list(dict.fromkeys(zip(d["strang"], d["verfahren"], d["parameter"])))

    # Y-Positionen mit LUECKE zwischen den Straengen. Ohne die Luecke bliebe
    # kein Platz fuer die Blockueberschriften, und ohne die Ueberschriften ist
    # nicht ablesbar, welcher Block welcher ist - die Verfahrensnamen
    # wiederholen sich in beiden.
    LUECKE = 1.15
    y, versatz, vorher = [], 0.0, None
    for i, (strang, _, _) in enumerate(reihen):
        if vorher is not None and strang != vorher:
            versatz += LUECKE
        y.append(-(i + versatz))
        vorher = strang
    y = np.array(y)

    fig, ax = plt.subplots(figsize=(BREITE, 1.5 + 0.235 * (len(reihen) + LUECKE)))

    for yy, (strang, verf, param) in zip(y, reihen):
        g = d[(d["strang"] == strang) & (d["verfahren"] == verf)
              & (d["parameter"] == param)]
        ax.plot([0, 1], [yy, yy], color="0.85", linewidth=3.2,
                solid_capstyle="butt", zorder=1)
        ax.scatter(g["lage"], np.full(len(g), yy), s=26,
                   marker=STIL[verf]["marker"], facecolor=STIL[verf]["grau"],
                   edgecolor="black", linewidth=0.5, zorder=3)
        spanne = float(g["lage"].max() - g["lage"].min())
        ax.text(1.03, yy, f"{spanne:.2f}".replace(".", ","), va="center",
                fontsize=SCHRIFT - 2.5, transform=ax.get_yaxis_transform())

    straenge = [s for s, _, _ in reihen]
    for strang in dict.fromkeys(straenge):
        erste = straenge.index(strang)
        ax.text(0.5, y[erste] + 0.68, f"Strang: {strang}", ha="center",
                va="bottom", fontsize=SCHRIFT - 2, color="0.30")
        if erste:
            ax.axhline((y[erste - 1] + y[erste]) / 2, color="black",
                       linewidth=0.6, zorder=2)

    ax.set_yticks(y)
    ax.set_yticklabels([f"{LABEL.get(v, v)} · {LABEL_PARAMETER.get(p, p)}"
                        for _, v, p in reihen], fontsize=SCHRIFT - 2)
    ax.set_xlim(-0.03, 1.03)
    ax.set_ylim(y.min() - 0.7, y.max() + 1.25)
    ax.set_xticks([0, 0.5, 1])
    ax.set_xticklabels(["untere Grenze", "Mitte", "obere Grenze"],
                       fontsize=SCHRIFT - 2)
    ax.set_xlabel("Lage des gewählten Werts im eigenen Suchraum")
    ax.text(1.03, 1.005, "Spannweite", transform=ax.transAxes,
            fontsize=SCHRIFT - 2.5, va="bottom")
    ax.grid(True, axis="x", linewidth=0.3, alpha=0.5)

    fig.supxlabel("Fünf Punkte je Zeile — ein Fold je Punkt, getunt auf "
                  "Wiederholung 0. Kategoriale Räume sind über die Position "
                  "in der Werteliste normiert.", fontsize=SCHRIFT - 2)
    return _speichere(fig, "a8_hyperparameter.pdf")


# ===========================================================================
def a9_parallelisierung() -> list:
    """A9: Parallelisierungsgewinn je Verfahren (Unterfrage 3, zweite Haelfte).

    Ein:  menge_folds.csv, struktur_folds.csv
    Aus:  a9_parallelisierung.pdf

    - A4 traegt die einkernige Trainingszeit auf; hier steht der Faktor, um den
      derselbe Fit ueber alle Kerne schneller wird
    - die Linie bei 1,0 ist keine Dekoration: Werte darunter heissen, dass der
      parallele Fit langsamer war - der Verwaltungsaufwand der Threads uebersteigt
      den Gewinn
    - bei Ridge ist ein Wert um 1 zu erwarten; eine geschlossene Loesung hat nichts
      zu verteilen. Auch das ist ein Ergebnis
    """
    zeilen = []
    for strang, pfad in (("Menge", REG / "menge_mittel.csv"),
                         ("Struktur", KLA / "struktur_mittel.csv")):
        t = _text(pfad)
        if t is None or "parallel_gewinn" not in t.columns:
            continue
        # Im Mengenstrang stammen beide Zielgroessen aus DERSELBEN Anpassung
        # (#43) - fuer `anzahl_einsaetze` wird die Ratenvorhersage nur
        # zurueckmultipliziert. Die Zeiten sind deshalb bis auf Messrauschen
        # identisch; zwei Balken taeuschten zwei Messungen vor.
        for verf, g in t.groupby("verfahren", sort=False):
            zeilen.append({"strang": strang, "verfahren": verf,
                           "gewinn": float(g["parallel_gewinn"].mean()),
                           "ein": float(g["train_sekunden_mean"].mean()),
                           "par": float(g["train_sekunden_parallel_mean"].mean())})
    if not zeilen:
        return []

    d = pd.DataFrame(zeilen)
    d = (d.assign(_s=d["strang"].map({"Menge": 0, "Struktur": 1}),
                  _v=d["verfahren"].map(RANG_VERFAHREN).fillna(9))
          .sort_values(["_s", "_v"]).drop(columns=["_s", "_v"])
          .reset_index(drop=True))

    fig, ax = plt.subplots(figsize=(BREITE, 1.2 + 0.44 * len(d)))
    y = np.arange(len(d))[::-1]

    # Der Bereich unter 1 wird hinterlegt - "langsamer" soll man sehen, bevor
    # man die Zahl liest.
    ax.axvspan(0, 1, color="0.94", zorder=0)
    for yy, (_, z) in zip(y, d.iterrows()):
        ax.barh(yy, z["gewinn"], height=0.58,
                color=STIL[z["verfahren"]]["grau"], edgecolor="black",
                linewidth=0.8, hatch=STIL[z["verfahren"]]["schraffur"], zorder=2)
        ax.text(z["gewinn"] + d["gewinn"].max() * 0.02, yy,
                f"{_sekunden(z['ein'])} → {_sekunden(z['par'])}",
                va="center", fontsize=SCHRIFT - 2.5)
    ax.axvline(1.0, color="black", linewidth=1.2, linestyle="--", zorder=3)

    ax.set_yticks(y)
    ax.set_yticklabels([f"{LABEL.get(z['verfahren'], z['verfahren'])}\n"
                        f"({z['strang']})" for _, z in d.iterrows()],
                       fontsize=SCHRIFT - 1)
    ax.set_xlim(0, max(d["gewinn"].max() * 1.42, 1.35))
    ax.set_ylim(-0.6, len(d) - 0.4)
    ax.set_xlabel("Parallelisierungsgewinn (einkernige Zeit ÷ parallele Zeit)")
    ax.xaxis.set_major_formatter(_komma(1))
    ax.annotate("kein Gewinn", xy=(1.0, len(d) - 0.45), xytext=(0, 3),
                textcoords="offset points", ha="center",
                fontsize=SCHRIFT - 2.5)

    fig.supxlabel("Mittel über 50 Läufe. Werte unter 1 bedeuten: der parallele "
                  "Fit war langsamer. Beschriftung: einkernig → parallel.",
                  fontsize=SCHRIFT - 2)
    return _speichere(fig, "a9_parallelisierung.pdf")


# ===========================================================================
def a10_qq_residuen() -> list:
    """A10: QQ-Diagramm der Residuen der linearen Spezifikation.

    Ein:  qq_residuen.csv aus v2_eignung.annahmen()
    Aus:  a10_qq_residuen.pdf

    - gezeichnet, nicht gerechnet: die Quantile stehen fertig in der CSV
    - beantwortet, WO die Abweichung liegt; Jarque-Bera sagt nur, DASS die
      Verteilung nicht normal ist, und bei n = 3.036 sagt er das fast immer
    - zu lesen an den Enden: Punkte in der Mitte auf der Geraden und nur aussen
      abbiegend heisst im Kern normal mit schweren Raendern - bei Einsatzzahlen zu
      erwarten
    - eine Kruemmung ueber die ganze Laenge waere schwerer wiegend
    - gezeigt wird die Abbildung, obwohl Normalitaet fuer die Punktprognose nicht
      erforderlich ist: Die Anforderung wird geprueft, die Antwort lautet "besteht
      hier nicht". Sie ungeprueft zu lassen waere keine Aussage
    """
    d = _text(RESULTS_DIR / "eignungspruefung" / "qq_residuen.csv")
    if d is None:
        return []

    ziele = list(dict.fromkeys(d["zielgroesse"]))
    fig, achsen = plt.subplots(1, len(ziele), figsize=(BREITE, 3.0))
    achsen = np.atleast_1d(achsen)

    for ax, ziel in zip(achsen, ziele):
        g = d[d["zielgroesse"] == ziel]
        grenze = float(max(abs(g["theoretisch"]).max(),
                           abs(g["beobachtet"]).max())) * 1.05
        # Die Winkelhalbierende ZUERST, damit die Punkte darauf liegen und
        # nicht darunter verschwinden.
        ax.plot([-grenze, grenze], [-grenze, grenze], color="black",
                linewidth=1.0, linestyle="--", zorder=1)
        ax.scatter(g["theoretisch"], g["beobachtet"], s=4, color="0.35",
                   linewidth=0, alpha=0.55, zorder=2)
        ax.set_xlim(-grenze, grenze)
        ax.set_ylim(-grenze, grenze)
        ax.set_aspect("equal", adjustable="box")
        ax.set_xlabel("theoretisches Quantil")
        ax.set_ylabel("beobachtetes Quantil")
        ax.xaxis.set_major_formatter(_komma(0))
        ax.yaxis.set_major_formatter(_komma(0))
        ax.set_title(LABEL.get(ziel, ziel), fontsize=SCHRIFT - 1)
        ax.grid(True, linewidth=0.3, alpha=0.5)

    fig.supxlabel("Standardisierte Residuen der linearen Spezifikation auf "
                  "log(1+y), Trainingsstadtteile von Fold 1. Die Gerade ist "
                  "die Normalverteilung.", fontsize=SCHRIFT - 2)
    return _speichere(fig, "a10_qq_residuen.pdf")


# ===========================================================================
# A11 bis A15 -- Kapitel 7, ergaenzt am 19.08.2026
# ---------------------------------------------------------------------------
# Alle fuenf lesen ausschliesslich CSV und rechnen nichts. Sie ersetzen bzw.
# ergaenzen A1, A5 und A6, ohne sie zu loeschen - welche Fassung in die Arbeit
# geht, entscheidet der Text, nicht dieses Skript.
#   A11 Forest-Plot der gepaarten Differenzen   ersetzt A1   -> 7.1
#   A12 Deckenleiter des Strukturstrangs        neu          -> 7.2, vor T2
#   A13 Kreuzvalidierung gegen Hold-out         ersetzt A5   -> 7.2
#   A14 Ueberanpassung Training gegen CV        neu          -> 7.3
#   A15 Attribution gegen Ablation              ersetzt A6   -> 7.4
# ===========================================================================
def _dez(wert: float, stellen: int = 3) -> str:
    """Deutsches Dezimalkomma fuer Beschriftungen im Bild.

    Ein:  Zahl, Nachkommastellen
    Aus:  Zeichenkette

    - die Achsen benutzen _komma ueber den FuncFormatter; fuer Text IM Bild
      braucht es dieselbe Schreibweise ohne Formatter
    """
    return f"{wert:.{stellen}f}".replace(".", ",")


def a11_differenzen() -> list:
    """Gepaarte Differenzen mit Konfidenzintervall, je Strang eine Abbildung.

    Ein:  results/regression/vergleich.csv, results/klassifikation/vergleich.csv
    Aus:  results/abbildungen/a11_differenzen.pdf          (Menge,    7.1)
          results/abbildungen/a11b_differenzen_struktur.pdf (Struktur, 7.2)

    - beide Straenge standen frueher untereinander in EINER Abbildung. In 7.1
      erschien dadurch ein Block mit Klassifikationsergebnissen, der dort
      inhaltlich nichts zu suchen hat. Jetzt je Strang eine eigene Datei
    - A1 zeigt dieselbe Groesse als Boxplot ueber die zehn Wiederholungsmittel.
      Der Boxplot traegt aber weder das Intervall noch die Testentscheidung -
      der Leser sieht nicht, welche Differenz gedeckt ist. Hier steht beides
    - berichtet wird nur anzahl_einsaetze (#B10); die Rate laeuft als
      Anhangstabelle mit
    - die Testkennzahlen stehen als eigene Spalte auf einer zweiten y-Achse.
      Innerhalb der Achse ueberdeckten sie im Strukturstrang die Whisker
    - gefuellter Marker heisst signifikant. Die Fuellung, nicht die Farbe,
      traegt die Aussage - im Graustufendruck bleibt sie erhalten
    """
    dateien = [REG / "vergleich.csv", KLA / "vergleich.csv"]
    if not all(p.exists() for p in dateien):
        return []
    vr = pd.read_csv(dateien[0])
    vk = pd.read_csv(dateien[1])
    vr = vr[(vr.teststufe == "wiederholung")
            & (vr.zielgroesse == "anzahl_einsaetze")]
    vk = vk[vk.teststufe == "wiederholung"]

    def _zeilen(df: pd.DataFrame, ersatz: str) -> list[dict]:
        out = []
        for rolle in ("primaer", "sekundaer"):
            for _, r in df[df.rolle == rolle].iterrows():
                a, b = [t.strip() for t in r.paarung.split(" vs ")]
                holm = rolle == "sekundaer" and pd.notna(r.p_holm)
                out.append(dict(
                    name=f"{LABEL.get(a, a)} gegen {LABEL.get(b, ersatz)}",
                    rolle=rolle, d=r.differenz_mittel, lo=r.ci_unten,
                    hi=r.ci_oben, gew=int(r.gewonnene),
                    p=r.p_holm if holm else r.wilcoxon_p,
                    marke="Holm" if holm else "p",
                    sig=bool(r.signifikant), verf=a))
        return out

    fussnote = ("Gepaarter Wilcoxon-Test auf den zehn Wiederholungsmitteln, "
                "95-%-Intervall. Oberer Block: gegen die Stufe-2-Baseline, "
                "unkorrigiert.\nUnterer Block: paarweise Verfahrensvergleiche, "
                "p nach Holm über die Familie. Gefüllter Marker bedeutet "
                "signifikant, x/10 die Zahl der gewonnenen Wiederholungen.")

    pfade = []
    for daten, titel, stellen, datei in (
            (_zeilen(vr, POISSON), "Menge: Anzahl Einsätze — Δ RMSE", 2,
             "a11_differenzen.pdf"),
            (_zeilen(vk, "Logit"),
             "Struktur: dominante Einsatzart — Δ Macro-F1", 3,
             "a11b_differenzen_struktur.pdf")):
        if not daten:
            continue
        n = len(daten)
        # Zeilenhoehe konstant halten, damit beide Abbildungen im Druck
        # gleich dicht wirken - der Mengenstrang hat sechs Zeilen, der
        # Strukturstrang drei.
        fig, ax = plt.subplots(figsize=(BREITE, 1.35 + 0.40 * n))
        ypos = list(range(n))[::-1]
        for y, d in zip(ypos, daten):
            marker = STIL.get(d["verf"], {}).get("marker", "D")
            ax.plot([d["lo"], d["hi"]], [y, y], color="0.25", lw=1.1, zorder=2)
            for x in (d["lo"], d["hi"]):
                ax.plot([x, x], [y - 0.13, y + 0.13], color="0.25", lw=1.1,
                        zorder=2)
            ax.plot(d["d"], y, marker=marker, ms=6, ls="none", mew=1.0,
                    mfc="black" if d["sig"] else "white", mec="black", zorder=3)
        ax.axvline(0, color="0.25", ls="--", lw=0.9, zorder=1)
        primaer = sum(1 for d in daten if d["rolle"] == "primaer")
        ax.axhline(n - primaer - 0.5, color="0.75", lw=0.8)
        ax.set_yticks(ypos)
        ax.set_yticklabels([d["name"] for d in daten])
        ax.set_ylim(-0.7, n - 0.3)
        ax.set_title(titel, fontsize=SCHRIFT)
        ax.set_xlabel("← schlechter          besser →")
        ax.xaxis.set_major_formatter(_komma(stellen, True))
        lo = min(d["lo"] for d in daten)
        hi = max(d["hi"] for d in daten)
        ax.set_xlim(lo - 0.10 * (hi - lo), hi + 0.10 * (hi - lo))
        rechts = ax.twinx()
        rechts.set_ylim(ax.get_ylim())
        rechts.set_yticks(ypos)
        rechts.set_yticklabels(
            [f"{d['gew']}/10 · {d['marke']} {_dez(d['p'])}" for d in daten],
            fontsize=SCHRIFT - 2.5, color="0.30")
        rechts.tick_params(axis="y", length=0, pad=3)
        for rand in ("top", "right", "left", "bottom"):
            rechts.spines[rand].set_visible(False)
        fig.supxlabel(fussnote, fontsize=SCHRIFT - 2)
        pfade += _speichere(fig, datei)
    return pfade


def a12_decken() -> list:
    """Die beiden Obergrenzen des Strukturstrangs mit den erreichten Werten.

    Ein:  results/klassifikation/decke.csv, decke_ausschoepfung.csv,
          baselines_klasse_mittel.csv, struktur_mittel.csv
    Aus:  results/abbildungen/a12_decken.pdf

    - Macro-F1 0,33 gegen die 1,0 einer fehlerfreien Vorhersage zu halten ist
      der falsche Massstab. Die Abbildung setzt den richtigen: beide Decken
      entstehen VOR jeder Modellwahl (v4_decke.py)
    - der bemasste Pfeil zwischen dem besten Verfahren und Decke B ist die
      eigentliche Aussage - mehr als diesen Abstand koennen Verfahrenswahl und
      Hyperparametersuche zusammen nicht mehr holen
    - der schraffierte Bereich rechts von Decke A ist bei dieser Zielgroesse
      nicht erreichbar; ohne ihn liest sich die Skala als offen
    """
    noetig = [KLA / "decke.csv", KLA / "decke_ausschoepfung.csv",
              KLA / "baselines_klasse_mittel.csv", KLA / "struktur_mittel.csv"]
    if not all(p.exists() for p in noetig):
        return []
    dk = pd.read_csv(noetig[0]).set_index("grenze").macro_f1
    aus = pd.read_csv(noetig[1]).set_index("verfahren")
    bl = pd.read_csv(noetig[2])
    st = pd.read_csv(noetig[3])

    reihen = [
        ("Mehrheitsklasse (Stufe 1)",
         float(bl.loc[bl.stufe == 1, "Macro-F1_mean"].iloc[0]), "0.92", ""),
        ("Multinomiales Logit (Stufe 2)",
         float(bl.loc[bl.stufe == 2, "Macro-F1_mean"].iloc[0]), "0.72", ".."),
        ("Random Forest",
         float(st.loc[st.verfahren == "random_forest", "macro_f1_mean"].iloc[0]),
         STIL["random_forest"]["grau"], STIL["random_forest"]["schraffur"]),
        ("XGBoost",
         float(st.loc[st.verfahren == "xgboost", "macro_f1_mean"].iloc[0]),
         STIL["xgboost"]["grau"], STIL["xgboost"]["schraffur"]),
    ]
    bester = max(r[1] for r in reihen)
    decke_b = float(dk["Decke B - Stadtteilwissen"])
    decke_a = float(dk["Decke A - Label-Rauschen"])

    fig, ax = plt.subplots(figsize=(BREITE, 2.9))
    ypos = list(range(len(reihen)))[::-1]
    ax.axvspan(decke_a, 1.0, facecolor="0.94", edgecolor="0.80",
               hatch="///", lw=0, zorder=0)
    for y, (name, wert, grau, hatch) in zip(ypos, reihen):
        ax.barh(y, wert, height=0.55, color=grau, edgecolor="black",
                hatch=hatch, lw=0.7, zorder=2)
        ax.annotate(_dez(wert), xy=(wert, y), xytext=(3, 0),
                    textcoords="offset points", va="center",
                    fontsize=SCHRIFT - 1)
    for x, txt in ((decke_b, "Decke B\nStadtteilwissen"),
                   (decke_a, "Decke A\nLabel-Rauschen")):
        ax.axvline(x, color="black", ls="--", lw=1.0, zorder=3)
        ax.annotate(f"{txt}\n{_dez(x)}", xy=(x, len(reihen) - 0.35),
                    xytext=(3, 0), textcoords="offset points",
                    fontsize=SCHRIFT - 2, va="top", ha="left")
    ax.annotate("", xy=(decke_b, -0.72), xytext=(bester, -0.72),
                arrowprops=dict(arrowstyle="<->", lw=0.9, color="black"))
    ax.annotate(f"Restpotenzial {_dez(decke_b - bester)}",
                xy=((bester + decke_b) / 2, -0.72), xytext=(0, 4),
                textcoords="offset points", ha="center", fontsize=SCHRIFT - 2)
    ax.annotate("nicht erreichbar", xy=((decke_a + 1) / 2, 0.6), ha="center",
                va="center", fontsize=SCHRIFT - 2, color="0.35")
    ax.set_yticks(ypos)
    ax.set_yticklabels([r[0] for r in reihen])
    ax.set_ylim(-1.15, len(reihen) - 0.25)
    ax.set_xlim(0, 1.0)
    ax.set_xlabel("Macro-F1 — 1,0 wäre die fehlerfreie Vorhersage")
    ax.xaxis.set_major_formatter(_komma(1))
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)

    quote_x = float(aus.loc["xgboost", "quote_decke_b"]) * 100
    quote_r = float(aus.loc["random_forest", "quote_decke_b"]) * 100
    fig.supxlabel("Kreuzvalidierung, Entwicklungspanel. Beide Decken entstehen "
                  "vor jeder Modellwahl (vorpruefung/v4_decke.py). "
                  "Ausschöpfung, baselinekorrigiert:\n"
                  f"XGBoost {_dez(quote_x, 1)} %, Random Forest "
                  f"{_dez(quote_r, 1)} % von Decke B. Auf dem Hold-out liegen "
                  "die Decken bei 0,422 (B) und 0,679 (A).",
                  fontsize=SCHRIFT - 2)
    return _speichere(fig, "a12_decken.pdf")


def a13_umschlag() -> list:
    """Kreuzvalidierung gegen Hold-out im Strukturstrang.

    Ein:  results/klassifikation/struktur_folds.csv, baselines_klasse.csv,
          holdout.csv
    Aus:  results/abbildungen/a13_umschlag.pdf

    - A5 zeigt die Hold-out-Werte als Balken. Der Befund ist aber nicht ihre
      Hoehe, sondern die UMKEHR der Rangfolge gegenueber der Kreuzvalidierung.
      Die sieht man nur, wenn beide Seiten in einem Bild stehen
    - die 50 Einzellaeufe stehen als Punktwolke daneben. Sie belegen, dass der
      Hold-out-Wert innerhalb der eigenen CV-Spannweite liegt - die Umkehr ist
      eine Ziehung aus einer breiten Verteilung, keine Anomalie
    - der Zufallsversatz der Punkte ist mit RANDOM_STATE gezogen; die Abbildung
      sieht bei jedem Lauf gleich aus
    """
    noetig = [KLA / "struktur_folds.csv", KLA / "baselines_klasse.csv",
              KLA / "holdout.csv"]
    if not all(p.exists() for p in noetig):
        return []
    fo = pd.read_csv(noetig[0])
    bl = pd.read_csv(noetig[1])
    ho = pd.read_csv(noetig[2])

    serien = [
        ("Multinomiales Logit", bl.loc[bl.stufe == 2, "Macro-F1"].to_numpy(),
         float(ho.loc[ho.stufe == 2, "macro_f1"].iloc[0]), "0.72", "D"),
        ("Random Forest",
         fo.loc[fo.verfahren == "random_forest", "macro_f1"].to_numpy(),
         float(ho.loc[ho.verfahren == "random_forest", "macro_f1"].iloc[0]),
         STIL["random_forest"]["grau"], STIL["random_forest"]["marker"]),
        ("XGBoost", fo.loc[fo.verfahren == "xgboost", "macro_f1"].to_numpy(),
         float(ho.loc[ho.verfahren == "xgboost", "macro_f1"].iloc[0]),
         STIL["xgboost"]["grau"], STIL["xgboost"]["marker"]),
    ]
    zufall = np.random.default_rng(42)
    fig, ax = plt.subplots(figsize=(BREITE, 3.4))
    for i, (name, cv, hold, grau, marker) in enumerate(serien):
        x0 = (i - 1) * 0.19
        ax.scatter(x0 + zufall.uniform(-0.055, 0.055, cv.size), cv, s=7,
                   facecolor=grau, edgecolor="none", alpha=0.55, zorder=1)
        ax.plot([x0 - 0.085, x0 + 0.085], [cv.mean()] * 2, color="black",
                lw=1.4, zorder=3)
        ax.plot([x0, 1.0], [cv.mean(), hold], color="black", lw=1.0,
                ls="-" if hold > 0.3 else "--", zorder=2)
        ax.plot(1.0, hold, marker=marker, ms=7, mfc=grau, mec="black",
                mew=0.9, zorder=4)
        ax.annotate(f"{name}  {_dez(hold)}", xy=(1.0, hold),
                    xytext=(8, {0: 0, 1: -9, 2: 9}[i]),
                    textcoords="offset points", va="center",
                    fontsize=SCHRIFT - 1)
        ax.annotate(_dez(cv.mean()), xy=(x0, cv.mean()), xytext=(0, 5),
                    textcoords="offset points", va="bottom", ha="center",
                    fontsize=SCHRIFT - 2)
    ax.set_xticks([0.0, 1.0])
    ax.set_xticklabels(["Kreuzvalidierung\n50 Läufe, 30 Stadtteile",
                        "Hold-out\neine Messung, 6 Stadtteile"])
    ax.set_xlim(-0.40, 1.72)
    ax.set_ylabel("Macro-F1")
    ax.yaxis.set_major_formatter(_komma(2))
    fig.supxlabel("Linke Seite: jeder Punkt ein Fold-Lauf, der waagerechte "
                  "Strich das Mittel über die 50 Läufe. Rechte Seite: die "
                  "einmalige Hold-out-Messung.\nDie Hold-out-Werte der "
                  "Baumverfahren liegen innerhalb der Spannweite ihrer eigenen "
                  "Kreuzvalidierung — die Umkehr ist keine Anomalie.",
                  fontsize=SCHRIFT - 2)
    return _speichere(fig, "a13_umschlag.pdf")


def a14_ueberanpassung() -> list:
    """Trainingsguete gegen Kreuzvalidierungsguete je Verfahren.

    Ein:  results/regression/menge_mittel.csv, baselines_mittel.csv,
          results/klassifikation/struktur_mittel.csv,
          baselines_klasse_mittel.csv
    Aus:  results/abbildungen/a14_ueberanpassung.pdf

    - B6: Random Forest braucht Faktor 326 mehr Trainingszeit als Ridge. Am
      schlechtesten generalisiert im finalen Lauf XGBoost - Ueberanpassung
      27,0 RMSE gegen 21,9 beim Random Forest und 3,0 bei Ridge. Die Zeit
      steht in A4, der Abstand Training-CV bisher nirgends
    - die Stufe-2-Baselines haben keinen gespeicherten Trainingswert; sie
      stehen als waagerechte Referenz der Kreuzvalidierungsguete. Ein Punkt
      waere dort eine erfundene Zahl
    - beide Straenge haben eigene Achsen: R2 und Macro-F1 sind nicht dieselbe
      Groesse und duerfen nicht auf eine Skala
    """
    noetig = [REG / "menge_mittel.csv", REG / "baselines_mittel.csv",
              KLA / "struktur_mittel.csv", KLA / "baselines_klasse_mittel.csv"]
    if not all(p.exists() for p in noetig):
        return []
    me = pd.read_csv(noetig[0])
    me = me[me.zielgroesse == "anzahl_einsaetze"].set_index("verfahren")
    br = pd.read_csv(noetig[1])
    st = pd.read_csv(noetig[2]).set_index("verfahren")
    bk = pd.read_csv(noetig[3])
    glm = float(br[(br.stufe == 2)
                   & (br.zielgroesse == "anzahl_einsaetze")].R2_mean.iloc[0])
    logit = float(bk.loc[bk.stufe == 2, "Macro-F1_mean"].iloc[0])

    fig, axes = plt.subplots(1, 2, figsize=(BREITE, 3.1))
    for ax, tab, sp_tr, sp_cv, verf, titel, ylab, ref, refname in (
            (axes[0], me, "R2_train", "R2_mean",
             ["ridge", "random_forest", "xgboost"], "Menge: Anzahl Einsätze",
             "R²", glm, POISSON),
            (axes[1], st, "macro_f1_train", "macro_f1_mean",
             ["random_forest", "xgboost"], "Struktur: dominante Einsatzart",
             "Macro-F1", logit, "Logit")):
        for i, v in enumerate(verf):
            tr, cv = float(tab.loc[v, sp_tr]), float(tab.loc[v, sp_cv])
            ax.plot([i, i], [tr, cv], color="0.35", lw=1.2, zorder=1)
            ax.plot(i, tr, marker="o", ms=6, mfc="white", mec="black",
                    mew=1.1, zorder=3)
            ax.plot(i, cv, marker=STIL[v]["marker"], ms=6, mfc="black",
                    mec="black", zorder=3)
            ax.annotate(f"−{_dez(tr - cv)}", xy=(i, (tr + cv) / 2),
                        xytext=(6, 0), textcoords="offset points",
                        va="center", fontsize=SCHRIFT - 2)
            ax.annotate(_dez(tr), xy=(i, tr), xytext=(0, 6),
                        textcoords="offset points", ha="center",
                        fontsize=SCHRIFT - 2)
            ax.annotate(_dez(cv), xy=(i, cv), xytext=(0, -13),
                        textcoords="offset points", ha="center",
                        fontsize=SCHRIFT - 2)
        ax.axhline(ref, color="black", ls="--", lw=0.9)
        ax.annotate(refname, xy=(len(verf) - 0.47, ref), xytext=(-2, 3),
                    textcoords="offset points", ha="right", va="bottom",
                    fontsize=SCHRIFT - 2.5)
        ax.set_xticks(range(len(verf)))
        ax.set_xticklabels([LABEL[v] for v in verf])
        ax.set_xlim(-0.55, len(verf) - 0.45)
        ax.set_title(titel, fontsize=SCHRIFT)
        ax.set_ylabel(ylab)
        ax.yaxis.set_major_formatter(_komma(1))
        ax.set_ylim(min(0.2, ax.get_ylim()[0]), 1.12)
    trainmarke, = axes[0].plot([], [], marker="o", ms=6, mfc="white",
                               mec="black", ls="none", label="Training")
    cvmarke, = axes[0].plot([], [], marker="s", ms=6, mfc="black",
                            mec="black", ls="none", label="Kreuzvalidierung")
    axes[0].legend(handles=[trainmarke, cvmarke], loc="upper left",
                   frameon=False, handletextpad=0.3, borderaxespad=0.1)
    fig.supxlabel("Mittel über die zehn Wiederholungen. Die Zahl an der "
                  "Verbindungslinie ist der Abstand zwischen Trainings- und "
                  "Kreuzvalidierungsgüte.\nFür die Stufe-2-Baselines liegt "
                  "kein Trainingswert vor; sie stehen als waagerechte Referenz "
                  "der Kreuzvalidierungsgüte.", fontsize=SCHRIFT - 2)
    return _speichere(fig, "a14_ueberanpassung.pdf")


def a15_attribution_ablation() -> list:
    """Attribution und Ablation je Faktorgruppe, nebeneinander.

    Ein:  results/shap/gruppen.csv, faktorgruppen_menge.csv,
          ablation_faktorgruppen_mittel.csv
    Aus:  results/abbildungen/a15_attribution_ablation.pdf

    - A6 zeigt nur die linke Haelfte. Der Befund von B-47 ist aber der
      UNTERSCHIED zwischen beiden Spalten: Attribution sagt, wie ein Modell
      schaut, Ablation sagt, was fehlt, wenn man die Gruppe wegnimmt
    - gleiche y-Ordnung in allen vier Feldern - nur dann vergleicht das Auge
      Zeile gegen Zeile statt Bild gegen Bild
    - Menge und Struktur haben eigene x-Achsen: RMSE und Macro-F1 sind nicht
      dieselbe Groesse. Die linke Spalte ist zudem in der Menge ein
      standardisierter Koeffizient und in der Struktur ein SHAP-Wert; das steht
      in der Fusszeile, sonst behauptet die Abbildung eine Vergleichbarkeit,
      die nicht besteht
    """
    noetig = [SHAP / "gruppen.csv", SHAP / "faktorgruppen_menge.csv",
              SHAP / "ablation_faktorgruppen_mittel.csv"]
    if not all(p.exists() for p in noetig):
        return []
    gr = pd.read_csv(noetig[0])
    menge_attr = pd.read_csv(noetig[1]).groupby("gruppe").anteil.sum()
    ab = pd.read_csv(noetig[2])

    y = np.arange(len(GRUPPEN_ORDNUNG))[::-1]
    fig, axes = plt.subplots(2, 2, figsize=(BREITE, 4.6),
                             gridspec_kw={"width_ratios": [1, 1.15]})

    ax = axes[0][0]
    ax.barh(y, [menge_attr.get(g, 0) for g in GRUPPEN_ORDNUNG], height=0.6,
            color="0.72", edgecolor="black", hatch="//", lw=0.7)
    for yy, g in zip(y, GRUPPEN_ORDNUNG):
        ax.annotate(f"{_dez(menge_attr.get(g, 0) * 100, 1)} %",
                    xy=(menge_attr.get(g, 0), yy), xytext=(3, 0),
                    textcoords="offset points", va="center",
                    fontsize=SCHRIFT - 2)
    ax.set_xlim(0, 0.52)
    ax.set_title("Attribution", fontsize=SCHRIFT)
    ax.xaxis.set_major_formatter(_prozent(0))
    ax.set_ylabel(f"Menge\n{POISSON}", fontsize=SCHRIFT)

    ax = axes[0][1]
    am = ab[ab.strang == "menge"].set_index("weggelassen")
    werte = [float(am.loc[g, "verschlechterung_mittel"]) for g in GRUPPEN_ORDNUNG]
    wdh = [int(am.loc[g, "wdh_mit_verschlechterung"]) for g in GRUPPEN_ORDNUNG]
    ax.barh(y, werte, height=0.6, color="0.72", edgecolor="black",
            hatch="//", lw=0.7)
    ax.axvline(0, color="0.25", ls="--", lw=0.9)
    for yy, v, w in zip(y, werte, wdh):
        ax.annotate(f"{_dez(v, 2)}  ({w}/10)", xy=(v, yy),
                    xytext=(4 if v >= 0 else -4, 0),
                    textcoords="offset points", va="center",
                    ha="left" if v >= 0 else "right", fontsize=SCHRIFT - 2)
    ax.set_xlim(-16, 38)
    ax.set_title("Ablation", fontsize=SCHRIFT)
    ax.set_xlabel("Δ RMSE ohne die Gruppe", fontsize=SCHRIFT - 1)
    ax.xaxis.set_major_formatter(_komma(0, True))

    ax = axes[1][0]
    hoehe = 0.34
    for k, v in enumerate(["random_forest", "xgboost"]):
        teil = gr[gr.verfahren == v].set_index("gruppe").anteil
        ax.barh(y + (0.5 - k) * hoehe,
                [teil.get(g, 0) for g in GRUPPEN_ORDNUNG], height=hoehe,
                color=STIL[v]["grau"], edgecolor="black",
                hatch=STIL[v]["schraffur"], lw=0.6, label=LABEL[v])
    ax.set_xlim(0, 0.62)
    ax.xaxis.set_major_formatter(_prozent(0))
    ax.set_ylabel("Struktur\nSHAP-Beiträge", fontsize=SCHRIFT)
    ax.set_xlabel("Anteil am erklärten Beitrag", fontsize=SCHRIFT - 1)
    ax.legend(loc="lower right", frameon=False, fontsize=SCHRIFT - 2.5,
              handletextpad=0.3, borderpad=0.1)

    ax = axes[1][1]
    hoehe = 0.24
    for k, (v, beschriftung) in enumerate([("Logit", "Logit"),
                                           ("random_forest", "Random Forest"),
                                           ("xgboost", "XGBoost")]):
        teil = ab[(ab.strang == "struktur")
                  & (ab.verfahren == v)].set_index("weggelassen")
        stil = STIL.get(v, {"grau": "0.85", "schraffur": ".."})
        ax.barh(y + (1 - k) * hoehe,
                [float(teil.loc[g, "verschlechterung_mittel"])
                 for g in GRUPPEN_ORDNUNG], height=hoehe, color=stil["grau"],
                edgecolor="black", hatch=stil["schraffur"], lw=0.5,
                label=beschriftung)
    ax.axvline(0, color="0.25", ls="--", lw=0.9)
    ax.set_xlim(-0.031, 0.032)
    ax.set_xlabel("Δ Macro-F1 ohne die Gruppe", fontsize=SCHRIFT - 1)
    ax.xaxis.set_major_formatter(_komma(2, True))
    ax.legend(loc="center right", frameon=False, fontsize=SCHRIFT - 2.5,
              handletextpad=0.3, borderpad=0.1)

    for zeile in axes:
        for feld in zeile:
            feld.set_yticks(y)
            feld.set_ylim(-0.7, len(GRUPPEN_ORDNUNG) - 0.3)
            feld.spines["left"].set_visible(False)
            feld.tick_params(axis="y", length=0)
        zeile[0].set_yticklabels([LABEL_GRUPPE[g] for g in GRUPPEN_ORDNUNG])
        zeile[1].set_yticklabels([])
    fig.supxlabel("Links: Anteil am erklärten Beitrag — Menge über "
                  "standardisierte Koeffizienten, Struktur über SHAP-Werte. "
                  "Zwei verschiedene Größen, beide auf 100 % normiert.\n"
                  "Rechts: Güteänderung, wenn die Gruppe weggelassen wird; "
                  "positiv heißt schlechter ohne sie. In Klammern die Zahl der "
                  "Wiederholungen mit Verschlechterung.", fontsize=SCHRIFT - 2)
    return _speichere(fig, "a15_attribution_ablation.pdf")


# ===========================================================================
# KAPITEL 4 - Data Understanding. Nur Befunde ueber die Daten.
# ===========================================================================
# Kurzformen fuer A17. Die Rohnamen sind Spaltenbezeichner und als
# Achsenbeschriftung zu lang; das Codebook nennt sie einmal vollstaendig.
LABEL_MERKMAL = {
    "median_haushaltseinkommen":  "Medianeinkommen",
    "armutsquote_pct":            "Armutsquote",
    "akademikerquote_pct":        "Akademikerquote",
    "median_miete":               "Medianmiete",
    "leerstandsquote_pct":        "Leerstandsquote",
    "log_bevoelkerung":           "log(Bevölkerung)",
    "log_kriminalitaetsindex":    "log(Kriminalitätsindex)",
    "anteil_altbau_vor_1940_pct": "Altbau vor 1940",
    "anteil_wohngebaeude_pct":    "Wohngebäudeanteil",
    "anteil_risikogewerbe_pct":   "Risikogewerbeanteil",
    "monat_sin":                  "Saison (Sinus)",
    "monat_cos":                  "Saison (Kosinus)",
}


def a16_einsatzlast() -> list:
    """A16: Einsatzlast je Stadtteil - Lage und Streuung ueber 132 Monate.

    Ein:  results/deskriptiv/stadtteilprofil.csv
    Aus:  a16_einsatzlast.pdf

    - Punkt ist der Median des Stadtteils, der Balken der Interquartilsabstand,
      die Antenne die Spannweite ueber alle Monate
    - ein Histogramm ueber alle 4.752 Zeilen waere die naheliegende, aber
      schwaechere Wahl: Es zeigt die Rechtsschiefe und verschweigt, woher sie
      kommt. Hier ist beides in einem Bild - das Niveaugefaelle ZWISCHEN den
      Stadtteilen (Faktor 44) und die vergleichsweise enge Streuung INNERHALB
      eines Stadtteils. Genau diese Zerlegung traegt spaeter 5.4 und 8.3
    - bewusst OHNE Fold- oder Hold-out-Kennzeichnung: die Aufteilung entsteht
      erst in Kapitel 5, eine Einfaerbung hier waere ein Vorgriff
    """
    p = _text(DESK / "stadtteilprofil.csv")
    if p is None:
        return []
    p = p.sort_values("mittel")           # unten der kleinste, oben der groesste
    y = np.arange(len(p))

    fig, ax = plt.subplots(figsize=(BREITE, 0.155 * len(p) + 1.15))
    ax.hlines(y, p["min"], p["max"], color="0.80", lw=0.8, zorder=1)
    ax.hlines(y, p["q25"], p["q75"], color="0.45", lw=3.4, zorder=2)
    ax.plot(p["median"], y, "o", markersize=4.2, color="white",
            markeredgecolor="black", markeredgewidth=0.9, zorder=3)

    ax.set_yticks(y)
    ax.set_yticklabels(p["stadtteil"], fontsize=SCHRIFT - 2)
    ax.set_ylim(-0.8, len(p) - 0.2)
    ax.set_xlim(left=0)
    ax.set_xlabel("Einsätze je Monat")
    ax.xaxis.set_major_formatter(_komma(0))
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.grid(axis="x", color="0.90", lw=0.6, zorder=0)
    ax.set_axisbelow(True)

    klein, gross = p["mittel"].min(), p["mittel"].max()
    monate = int(p["monate"].max())
    # Dezimalkomma je Zahl setzen, nicht ueber den ganzen Satz: ein
    # str.replace(".", ",") trifft sonst die Satzzeichen mit.
    fig.supxlabel(
        f"Punkt: Median über {monate} Monate. Balken: Interquartilsabstand. "
        f"Feine Linie: Spannweite.\n"
        f"Stadtteilmittel von {_dez(klein, 1)} bis {_dez(gross, 1)} Einsätzen "
        f"je Monat — Faktor {gross / klein:.0f}. "
        f"Bezugsmenge: alle {len(p)} Stadtteile, 2015-01 bis 2025-12.",
        fontsize=SCHRIFT - 2)
    return _speichere(fig, "a16_einsatzlast.pdf")
# ===========================================================================
def a18_foldstruktur() -> list:
    """A18: Struktur der Aufteilung - Beleg fuer Kapitel 5.4.

    Ein:  data/processed/regression.parquet und klassifikation.parquet
    Aus:  a18_foldstruktur.pdf

    - zeigt die Aufteilung als EIGENSCHAFT DES DATENSATZES, nicht Ergebnisse.
      Nicht mit A2 verwechseln: A2 traegt den RMSE je Fold und gehoert zu
      Kapitel 7
    - drei Aussagen aus 5.4 in einem Bild: Foldgroessen 6/6/6/6/6 plus 6 im
      Hold-out, Verteilung der brand-dominierten Monate, und dass jede Gruppe
      die Groessenspanne abdeckt (Punkte ober- und unterhalb des Medians)
    - Punktflaeche = Zahl brand-dominierter Monate; leere Punkte haben keinen
    """
    pfad_r = ROOT / "data" / "processed" / "regression.parquet"
    pfad_k = ROOT / "data" / "processed" / "klassifikation.parquet"
    if not pfad_r.exists() or not pfad_k.exists():
        return []
    d, k = pd.read_parquet(pfad_r), pd.read_parquet(pfad_k)

    g = d.groupby("stadtteil").agg(fold=("fold", "first"),
                                   bev=("gesamtbevoelkerung", "mean"))
    brand = k[k["dominante_einsatzart"] == "brand"].groupby("stadtteil").size()
    g["brand"] = brand.reindex(g.index).fillna(0).astype(int)

    fig, ax = plt.subplots(figsize=(BREITE, 3.1))
    rng = np.random.default_rng(7)
    spalten = [0, 1, 2, 3, 4, 5]
    for s in spalten:
        teil = g[g["fold"] == s]
        x = np.full(len(teil), s, float) + rng.uniform(-0.17, 0.17, len(teil))
        ohne = (teil["brand"] == 0).to_numpy()
        ax.scatter(x[ohne], teil.loc[ohne, "bev"] / 1000, s=16,
                   facecolor="white", edgecolor="0.35", linewidth=0.8, zorder=3)
        ax.scatter(x[~ohne], teil.loc[~ohne, "bev"] / 1000,
                   s=22 + teil.loc[~ohne, "brand"] * 7, facecolor="0.45",
                   edgecolor="black", linewidth=0.7, alpha=0.85, zorder=4)

    median = g["bev"].median() / 1000
    ax.axhline(median, color="black", linewidth=0.8, linestyle=":", zorder=2)
    ax.text(5.55, median * 1.06, "Median", va="bottom", ha="right",
            fontsize=SCHRIFT - 2)
    ax.axvline(0.5, color="0.6", linewidth=0.8, zorder=1)

    ax.set_xticks(spalten)
    ax.set_xticklabels([f"{'Hold-out' if s == 0 else f'Fold {s}'}\n"
                        f"({int((g['fold'] == s).sum())})" for s in spalten])
    ax.set_xlim(-0.6, 5.6)
    ax.set_yscale("log")
    ax.set_yticks([2.5, 5, 10, 20, 40, 80])
    ax.get_yaxis().set_major_formatter(_komma(1))
    ax.set_ylabel("Wohnbevölkerung in Tausend")
    for rand in ("top", "right"):
        ax.spines[rand].set_visible(False)

    je = g.groupby("fold")["brand"].sum()
    fig.supxlabel(
        "Ein Punkt ist ein Stadtteil; die Fläche zeigt die Zahl "
        "brand-dominierter Monate (leer: keiner). Brand-dominierte Monate je "
        "Gruppe: " + ", ".join(
            f"{'Hold-out' if s == 0 else f'Fold {s}'} {int(je[s])}"
            for s in spalten) + ".", fontsize=SCHRIFT - 2, wrap=True)
    fig.tight_layout()
    return _speichere(fig, "a18_foldstruktur.pdf")





def a17_panelstruktur() -> list:
    """A17: Varianzanteile und zeitliche Aufloesung der zwoelf Modellmerkmale.

    Ein:  results/deskriptiv/varianzzerlegung.csv, aufloesung.csv
    Aus:  a17_panelstruktur.pdf

    - links: wieviel Varianz eines Merkmals ZWISCHEN den Stadtteilen liegt und
      wieviel INNERHALB. Die gestrichelte Linie ist der Wert der Zielgroesse
      selbst (92,5 %) - sie macht sichtbar, dass Zielgroesse und Praediktoren
      dieselbe Struktur haben
    - rechts: wieviele verschiedene Werte ein Merkmal je Stadtteil ueber 132
      Monate annimmt, logarithmisch, weil zwischen 1 und 128 sonst nichts zu
      sehen waere
    - die beiden Panels sind zwei Blicke auf denselben Befund: Die Merkmale
      sind fast durchweg Stadtteilmerkmale, nicht Stadtteil-Monats-Merkmale.
      Zusammen sind sie die quantifizierte Antwort auf die Gutachten-Regeln
      R2 (Panelabhaengigkeit) und R3 (jaehrlich wiederholte ACS-Werte)
    - was daraus folgt, steht in 5.4, 7.4 und 8.3 - hier steht nur der Befund
    """
    v, a = _text(DESK / "varianzzerlegung.csv"), _text(DESK / "aufloesung.csv")
    if v is None or a is None:
        return []
    merkmale = [m for m in LABEL_MERKMAL if m in set(v["merkmal"])]
    if not merkmale:
        return []

    v = v.set_index("merkmal").loc[merkmale]
    a = a.set_index("merkmal").loc[merkmale]
    reihen = v.sort_values("anteil_zwischen").index.tolist()
    y = np.arange(len(reihen))

    fig, axes = plt.subplots(1, 2, figsize=(BREITE, 0.30 * len(reihen) + 1.9),
                             gridspec_kw={"width_ratios": [2.05, 1]})

    # ---- links: Varianzzerlegung -----------------------------------------
    ax = axes[0]
    zwischen = v.loc[reihen, "anteil_zwischen"].to_numpy()
    ax.barh(y, zwischen, color="0.45", edgecolor="black", lw=0.5,
            label="zwischen den Stadtteilen")
    ax.barh(y, 1 - zwischen, left=zwischen, color="0.90", edgecolor="black",
            lw=0.5, hatch="//", label="innerhalb eines Stadtteils")

    # Der Referenzwert der Zielgroesse steht in derselben Datei, wurde oben
    # aber durch die Beschraenkung auf die Modellmerkmale herausgefiltert.
    alle = _text(DESK / "varianzzerlegung.csv")
    zielwert = alle.loc[alle["merkmal"] == "anzahl_einsaetze", "anteil_zwischen"]
    if len(zielwert):
        w = float(zielwert.iloc[0])
        ax.axvline(w, color="black", ls="--", lw=1.0, zorder=5)
        # Unten links von der Linie: dort liegt der helle Saison-Balken, der
        # Text bleibt lesbar. Oben stuende er auf einem 100-%-Balken.
        ax.annotate(f"Zielgröße {_dez(w * 100, 1)} %",
                    xy=(w, 0.55), xytext=(-5, 0), textcoords="offset points",
                    ha="right", va="center", fontsize=SCHRIFT - 2.5,
                    bbox={"facecolor": "white", "edgecolor": "none",
                          "pad": 1.4, "alpha": 0.85})
    ax.set_xlim(0, 1)
    ax.set_xlabel("Anteil an der Gesamtvarianz")
    ax.xaxis.set_major_formatter(_prozent())
    ax.legend(loc="lower left", bbox_to_anchor=(0.0, 1.01), frameon=False,
              ncol=2, fontsize=SCHRIFT - 2.5, handletextpad=0.4)

    # ---- rechts: zeitliche Aufloesung ------------------------------------
    ax = axes[1]
    werte = a.loc[reihen, "eindeutig_je_stadtteil_mittel"].to_numpy()
    ax.barh(y, werte, color="0.72", edgecolor="black", lw=0.5, zorder=3)
    ax.set_xscale("log")
    ax.set_xlim(0.85, 420)
    ax.set_xticks([1, 4, 12, 132])
    ax.set_xticklabels(["1", "4", "12", "132"], fontsize=SCHRIFT - 2)
    ax.set_xlabel("Werte je Stadtteil (log.)")
    for yi, wert in zip(y, werte):
        ax.annotate(_dez(wert, 1), xy=(wert, yi), xytext=(3, 0),
                    textcoords="offset points", va="center",
                    fontsize=SCHRIFT - 2.5)
    ax.grid(axis="x", color="0.90", lw=0.6, zorder=0)
    ax.set_axisbelow(True)

    for feld in axes:
        feld.set_yticks(y)
        feld.set_ylim(-0.7, len(reihen) - 0.3)
        feld.spines["left"].set_visible(False)
        feld.tick_params(axis="y", length=0)
    axes[0].set_yticklabels([LABEL_MERKMAL[m] for m in reihen],
                            fontsize=SCHRIFT - 2)
    axes[1].set_yticklabels([])

    fig.supxlabel(
        "Links: Zerlegung der Varianz jedes Merkmals. Die gestrichelte Linie "
        "ist der entsprechende Anteil der Zielgröße `anzahl_einsaetze`.\n"
        "Rechts: mittlere Zahl verschiedener Werte je Stadtteil über die 132 "
        "Monate. 132 hieße monatlich neu, 1 heißt zeitkonstant.\n"
        "Bezugsmenge: alle 36 Stadtteile.", fontsize=SCHRIFT - 2)
    return _speichere(fig, "a17_panelstruktur.pdf")


# ===========================================================================
def main() -> int:
    """Erzeugt alle siebzehn Abbildungen nacheinander.

    Ein:  die CSV-Dateien aus m02, m03, m04, v1, v2, v3 und tools/deskriptiv.py
    Aus:  results/abbildungen/a1..a17.pdf; Exitcode

    - fehlt eine Eingangsdatei, wird die betroffene Abbildung uebersprungen und
      gemeldet; ein fehlender Lauf soll die uebrigen nicht verhindern
    """
    if not (REG / "menge_mittel.csv").exists() and \
       not (KLA / "struktur_mittel.csv").exists():
        raise SystemExit("Keine Ergebnisdateien - erst m02 und m03 ausfuehren.")
    if not (SPEZ / "spezifikation_mittel.csv").exists():
        print("  Hinweis: results/spezifikation/ fehlt - A3 zeigt die "
              "Spezifikationsvarianten nicht. Erst v3_spezifikation.py laufen "
              "lassen.")
    if not (SHAP / "gruppen.csv").exists():
        print("  Hinweis: results/shap/ fehlt - A6 und A7 entfallen. "
              "Erst m04_shap.py laufen lassen.")
    if not (DESK / "stadtteilprofil.csv").exists():
        print("  Hinweis: results/deskriptiv/ fehlt - A16 und A17 entfallen. "
              "Erst tools/deskriptiv.py laufen lassen.")
    OUT.mkdir(parents=True, exist_ok=True)
    _matplotlib()

    # Die Reihenfolge ist die der Arbeit, nicht die der Nummern: A17 steht
    # inhaltlich vor A18, deshalb stand es schon vorher so in der Summe.
    erzeugt = []
    for zeichne in (a1_gegen_baseline, a2_foldstruktur, a3_spezifikation,
                    a4_laufzeit_guete, a5_holdout, a6_faktorgruppen,
                    a7_extrapolation, a8_hyperparameter, a9_parallelisierung,
                    a10_qq_residuen, a11_differenzen, a12_decken,
                    a13_umschlag, a14_ueberanpassung, a15_attribution_ablation,
                    a16_einsatzlast, a17_panelstruktur, a18_foldstruktur):
        erzeugt += zeichne()
    for pfad in erzeugt:
        try:
            zeigen = pfad.relative_to(ROOT)
        except ValueError:
            zeigen = pfad
        print(f"  => {zeigen}  ({pfad.stat().st_size / 1024:.0f} kB)")
    print(f"\n  {len(erzeugt)} Abbildung(en). Einbinden mit "
          f"\\includegraphics[width=\\textwidth]{{...}} - die Dateien sind "
          f"bereits in Endgroesse ({BREITE}\" breit, {SCHRIFT} pt).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
