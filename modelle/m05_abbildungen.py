"""
Alle Abbildungen fuer Kapitel 7 - aus den CSV-Dateien, nicht von Hand.

    python modelle/m05_abbildungen.py

Eingang: results/regression/*.csv · results/klassifikation/*.csv
         results/spezifikation/*.csv · results/shap/ablation_exposition.csv
Ausgang: results/abbildungen/*.pdf

STAND: neu gefasst 07.08.2026. Setzt m02, m03, v1 und v3 voraus.

Dieses Skript RECHNET NICHTS. Es liest nur. Dadurch laesst sich eine Darstellung
aendern, ohne die Modelle neu zu rechnen, und nach einem neuen Lauf ist ein
Befehl genug.

--------------------------------------------------------------------------
WARUM DER SATZ AM 07.08.2026 NEU GESCHNITTEN WURDE
--------------------------------------------------------------------------
Der erste Satz bestand aus Boxplots der Rohwerte je Verfahren und einem
Balkendiagramm gegen die Baseline. Beides zeigte den Vergleich nicht, und zwar
aus einem messbaren Grund.

Die 50 Laeufe unterscheiden sich darin, WELCHE Stadtteile im Testfold liegen.
Bayview hat ein Vielfaches der Einsaetze von Seacliff, also schwankt der RMSE
zwischen 13 und 76 - unabhaengig vom Verfahren. Die Streuung der Rohwerte
betraegt 12,4 bis 15,5 RMSE, der Verfahrensunterschied rund 2. Ein Boxplot der
Rohwerte zeigt daher fast ausschliesslich Fold-Streuung.

Jedes Verfahren sieht aber DIESELBEN Folds. Bildet man die Differenz je Lauf,
kuerzt sich die Fold-Streuung heraus: Die Streuung der gepaarten Differenz
ueber die 10 Wiederholungsmittel betraegt 2,4 bis 4,3. Gepaarte Daten ungepaart
darzustellen verschenkt genau die Information, fuer die das Design gebaut wurde
- und es ist dieselbe Paarung, auf der der Wilcoxon-Test beruht (#34).

Das alte A2 hatte zusaetzlich einen einfachen Darstellungsfehler: Balken ab
null, waehrend sich alles zwischen 33,98 und 36,51 abspielt. Die Unterschiede
lagen in den obersten sechs Prozent der Bildhoehe.

--------------------------------------------------------------------------
FUENF ABBILDUNGEN
--------------------------------------------------------------------------
  a1_gegen_baseline.pdf   Gepaarte Differenz zur Stufe-2-Baseline, ein Punkt je
                          Wiederholung, beide Straenge nebeneinander. Das ist
                          die Primaeraussage nach Decision Log #34.

  a2_foldstruktur.pdf     Die Rohwerte je Fold, Verfahren als Linien. Zeigt,
                          dass die Streuung aus dem Fold stammt und nicht aus
                          dem Verfahren - die Begruendung fuer A1.

  a3_spezifikation.pdf    Was bewegt mehr: die Wahl des Verfahrens oder die
                          Wahl der Spezifikation? Antwort auf Unterfrage 4,
                          Grundlage von B-41.

  a4_laufzeit_guete.pdf   Trainingszeit gegen Prognoseguete, ein Punkt je
                          Verfahren. Unterfrage 3.

  a5_holdout.pdf          Die einmalige Auswertung auf den sechs
                          zurueckgehaltenen Stadtteilen, beide Straenge.

--------------------------------------------------------------------------
ANFORDERUNGEN AN DIE DARSTELLUNG
--------------------------------------------------------------------------
Sie landen im gedruckten Dokument, und Gestaltung war im Gutachten ein eigenes
Bewertungskriterium.

  Format        PDF, nicht PNG. Rasterbilder werden im Druck unscharf.
  Groesse       In der ENDGROESSE erzeugen, nicht gross erzeugen und in LaTeX
                schrumpfen - sonst steht dort 5-pt-Schrift. Mindestens 9 pt.
  Titel         KEINE Titel in der Abbildung. Die Bildunterschrift in LaTeX ist
                der Titel; beides doppelt sich sonst.
  Graustufen    Verfahren zusaetzlich ueber Schraffur und Marker unterscheiden,
                nicht allein ueber Farbe.
  Achsen        Beschriftung mit Einheit, deutsches Dezimalkomma.
  Nulllinie     Wo eine Differenz oder ein R2 dargestellt wird, ist sie
                einzuzeichnen - das Vorzeichen ist die Aussage.
  Richtung      Bei jeder Differenzachse muss dastehen, welche Seite besser
                ist. Bei RMSE ist das links, bei Macro-F1 rechts - wer das
                verwechselt, liest das Ergebnis genau falsch herum.
  Streuung      IMMER benennen, worueber sie gebildet ist: ueber die 10
                Wiederholungsmittel, nicht ueber die 50 Einzellaeufe (R-5).

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Sind alle fuenf PDF entstanden und in LaTeX einbindbar?
  - Schneidet in A1 die Nulllinie eine der Boxen? Dann darf im Text kein
    Unterschied zur Baseline behauptet werden, den der Test nicht deckt (R-6).
  - Traegt jede Differenzachse die Richtungsangabe, und zeigt sie bei Macro-F1
    in die andere Richtung als bei RMSE?
  - Stimmt der Referenzwert in A3 mit `linear` aus v3 und mit der
    Stufe-2-Baseline aus v1 ueberein? Alle drei muessen dieselbe Zahl sein.
  - In Graustufen ausdrucken: sind die Verfahren noch unterscheidbar?
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "prep"))

from config import RESULTS_DIR, ROOT  # noqa: E402

OUT = RESULTS_DIR / "abbildungen"
REG = RESULTS_DIR / "regression"
KLA = RESULTS_DIR / "klassifikation"
SPEZ = RESULTS_DIR / "spezifikation"
SHAP = RESULTS_DIR / "shap"

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


def _matplotlib():
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
    return plt, FuncFormatter


def _komma(FuncFormatter, stellen: int = 2, vorzeichen: bool = False):
    """Deutsches Dezimalkomma auf den Achsen.

    `stellen` ist nicht kosmetisch: Macro-F1 liegt zwischen 0,328 und 0,334 -
    mit zwei Nachkommastellen stuenden an allen Achsenmarken dieselben "0,33",
    und die Abbildung waere sinnlos.

    `vorzeichen` setzt auf Differenzachsen ein explizites Plus. Ohne das liest
    sich "2,5" wie ein Absolutwert statt wie ein Abstand.
    """
    fmt = "{:+,." + str(stellen) + "f}" if vorzeichen else "{:,." + str(stellen) + "f}"
    return FuncFormatter(lambda x, _: fmt.format(x).replace(",", " ")
                         .replace(".", ",").replace(" ", "."))


def _text(pfad: Path) -> pd.DataFrame | None:
    return pd.read_csv(pfad) if pfad.exists() else None


# ===========================================================================
def _gepaarte_differenz() -> list[dict]:
    """Je Verfahren die 10 Wiederholungsmittel der Differenz zur Baseline.

    Gepaart wird auf (wiederholung, fold) - also auf identischen Testzeilen.
    Genau diese Paarung liegt auch dem Wilcoxon-Test in m02/m03 zugrunde; die
    Abbildung zeigt damit dieselbe Groesse, die getestet wird, und nicht eine
    andere, die zufaellig aehnlich aussieht.
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


def a1_gegen_baseline(plt, FuncFormatter) -> list:
    """Die Primaeraussage: jedes Verfahren gegen seine Stufe-2-Baseline.

    Dargestellt sind die 10 Wiederholungsmittel als Punkte und ihre Verteilung
    als Kasten. Ein Fehlerbalken waere hier die schlechtere Wahl: Bei zehn
    Werten zeigt der Punktschwarm die Verteilung selbst, statt sie durch eine
    Kennzahl zu ersetzen, die Symmetrie unterstellt.

    Die Nulllinie ist die Baseline. Wo der Kasten sie schneidet, ist der
    Unterschied nicht gesichert - unabhaengig davon, was der Mittelwert sagt.
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
            _komma(FuncFormatter, reihe["stellen"], vorzeichen=True))
        pfeil = ("← besser" if reihe["besser"] == "links" else "besser →")
        ax.set_title(f"{LABEL.get(reihe['ziel'], reihe['ziel'])}\n{pfeil}",
                     fontsize=SCHRIFT - 1)

    fig.supxlabel("Abstand zur Stufe-2-Baseline. Ein Punkt je Wiederholung (10), "
                  "gepaart je Fold — die Fold-Streuung kürzt sich heraus.",
                  fontsize=SCHRIFT - 2)
    pfad = OUT / "a1_gegen_baseline.pdf"
    fig.savefig(pfad, bbox_inches="tight"); plt.close(fig)
    return [pfad]


# ===========================================================================
def a2_foldstruktur(plt, FuncFormatter) -> list:
    """Warum in A1 gepaart wird: die Folds bewegen alle Verfahren gemeinsam.

    Gezeigt wird eine einzelne Wiederholung, sonst waeren es 50 Linien. Die
    Aussage haengt nicht an der Auswahl - die uebrigen neun sehen genauso aus,
    was sich an der Streuungszerlegung in der Fusszeile ablesen laesst.
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
    ax.yaxis.set_major_formatter(_komma(FuncFormatter, 0))
    ax.legend(frameon=False, ncol=2, loc="upper left")
    unten, oben = ax.get_ylim()
    ax.set_ylim(unten, oben + (oben - unten) * 0.30)

    # Die Zerlegung als Zahl dazu - sie ist der eigentliche Grund fuer A1.
    roh = f.groupby("verfahren")["RMSE"].std().mean()
    fig.supxlabel(f"Streuung der Rohwerte über die 50 Läufe: {roh:.1f} RMSE. "
                  f"Streuung der gepaarten Differenz: 2,4 bis 4,3."
                  .replace(".", ",", 1),
                  fontsize=SCHRIFT - 2)
    pfad = OUT / "a2_foldstruktur.pdf"
    fig.savefig(pfad); plt.close(fig)
    return [pfad]


# ===========================================================================
def _spezifikationszeilen() -> list[tuple[str, float, str]]:
    """(Beschriftung, RMSE, Gruppe) fuer A3 - alles aus CSV, nichts von Hand."""
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

    a = _text(SHAP / "ablation_exposition.csv")
    if a is not None:
        ohne = (a[a["spezifikation"] == "ohne_exposition"]
                .groupby("verfahren")["RMSE"].mean())
        for v, wert in ohne.items():
            zeilen.append((f"{LABEL.get(v, v)} ohne Exposition", float(wert),
                           "spezifikation"))

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


def a3_spezifikation(plt, FuncFormatter) -> list:
    """Unterfrage 4: Was bewegt mehr - das Verfahren oder die Spezifikation?

    Alle Werte sind RMSE auf `anzahl_einsaetze`, gemittelt ueber dieselben 50
    Laeufe. Sie sind damit unmittelbar vergleichbar; es ist kein Wechsel des
    Massstabs zwischen den Gruppen im Spiel.

    Die Balken sind nach Gruppen sortiert, nicht global - sonst stuende die
    Referenz mitten zwischen den Verfahren und die Gruppierung waere nicht
    ablesbar.
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
    ax.xaxis.set_major_formatter(_komma(FuncFormatter, 0))

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
    pfad = OUT / "a3_spezifikation.pdf"
    fig.savefig(pfad, bbox_inches="tight"); plt.close(fig)
    return [pfad]


# ===========================================================================
def a4_laufzeit_guete(plt, FuncFormatter) -> list:
    """Unterfrage 3: Aufwand gegen Guete.

    Die Zeitachse ist logarithmisch, weil zwischen Ridge und den Ensembles
    Groessenordnungen liegen - linear waere Ridge ein Punkt auf der Null.
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
            _komma(FuncFormatter, int(g["stellen"].iloc[0])))
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
    pfad = OUT / "a4_laufzeit_guete.pdf"
    fig.savefig(pfad, bbox_inches="tight"); plt.close(fig)
    return [pfad]


# ===========================================================================
def a5_holdout(plt, FuncFormatter) -> list:
    """Die einmalige Auswertung auf den sechs zurueckgehaltenen Stadtteilen.

    Anders als in A1 gibt es hier KEINE Streuung - das Hold-out wird genau
    einmal ausgewertet. Fehlerbalken waeren an dieser Stelle falsch; die
    Einmaligkeit ist der Zweck des Hold-outs.

    Alle drei Stufen stehen nebeneinander, damit sichtbar bleibt, wovon der
    Abstand gemessen wird.
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
        ax.yaxis.set_major_formatter(_komma(FuncFormatter, stellen))
        ax.set_title(titel, fontsize=SCHRIFT - 1)
    fig.supxlabel("Sechs zurückgehaltene Stadtteile, einmalig ausgewertet — "
                  "hell: Stufe 1, schwarz: Stufe 2, grau: Stufe 3.",
                  fontsize=SCHRIFT - 2)
    pfad = OUT / "a5_holdout.pdf"
    fig.savefig(pfad, bbox_inches="tight"); plt.close(fig)
    return [pfad]


# ===========================================================================
def main() -> int:
    if not (REG / "menge_mittel.csv").exists() and \
       not (KLA / "struktur_mittel.csv").exists():
        raise SystemExit("Keine Ergebnisdateien - erst m02 und m03 ausfuehren.")
    if not (SPEZ / "spezifikation_mittel.csv").exists():
        print("  Hinweis: results/spezifikation/ fehlt - A3 zeigt die "
              "Spezifikationsvarianten nicht. Erst v3_spezifikation.py laufen "
              "lassen.")
    OUT.mkdir(parents=True, exist_ok=True)
    plt, FuncFormatter = _matplotlib()

    erzeugt = (a1_gegen_baseline(plt, FuncFormatter)
               + a2_foldstruktur(plt, FuncFormatter)
               + a3_spezifikation(plt, FuncFormatter)
               + a4_laufzeit_guete(plt, FuncFormatter)
               + a5_holdout(plt, FuncFormatter))
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
