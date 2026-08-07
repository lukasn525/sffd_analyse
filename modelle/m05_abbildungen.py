"""
Alle Abbildungen fuer Kapitel 7 - aus den CSV-Dateien, nicht von Hand.

    python modelle/m05_abbildungen.py

Eingang: results/regression/*.csv · results/klassifikation/*.csv
Ausgang: results/abbildungen/*.pdf

STAND: vollstaendig, 05.08.2026. Setzt m02 und m03 voraus.

Dieses Skript RECHNET NICHTS. Es liest nur. Dadurch laesst sich eine Darstellung
aendern, ohne die Modelle neu zu rechnen, und nach einem neuen Lauf ist ein
Befehl genug.

--------------------------------------------------------------------------
DREI ABBILDUNGEN
--------------------------------------------------------------------------
  a1_boxplot_menge.pdf        Boxplot je Verfahren ueber die 50 Laeufe,
  a1_boxplot_struktur.pdf     je Zielgroesse. Zeigt die Streuung ehrlich,
                              statt sie zu mitteln.

  a2_gegen_baseline.pdf       Balken: jedes Verfahren gegen seine
                              Stufe-2-Baseline, Fehlerbalken aus
                              std_wiederholungen. Das ist die Primaeraussage
                              nach Decision Log #34.

  a3_laufzeit_guete.pdf       Streudiagramm: Trainingszeit (log-Achse) gegen
                              Prognoseguete, ein Punkt je Verfahren.
                              Beantwortet Unterfrage 3 und 4 in einem Bild.

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
  Nulllinie     Wo R2 dargestellt wird, ist sie einzuzeichnen - negative Werte
                sind hier normal und muessen erkennbar sein.
  Fehlerbalken  IMMER beschriften: std_wiederholungen, nicht std_folds. Ohne
                Angabe ist ein Fehlerbalken bedeutungslos.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Sind alle drei PDF entstanden und in LaTeX einbindbar?
  - Ueberlappen sich in A1 die Boxen zweier Verfahren? Dann darf im Text keine
    Rangfolge stehen (R-6).
  - Traegt A2 die Beschriftung "std_wiederholungen"?
  - Ist in A1 die Nulllinie sichtbar, wo R2 dargestellt wird?
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

# Textbreite einer FOM-Arbeit bei A4 und 2,5 cm Raendern: rund 16 cm = 6,3 Zoll.
BREITE = 6.3
SCHRIFT = 9

# Graustufentauglich: Grauwert, Schraffur und Marker tragen die Unterscheidung
# gemeinsam. Wer die Arbeit schwarzweiss ausdruckt, sieht dasselbe.
STIL = {
    "ridge":         {"grau": "0.75", "schraffur": "//",  "marker": "o"},
    "random_forest": {"grau": "0.50", "schraffur": "\\\\", "marker": "s"},
    "xgboost":       {"grau": "0.25", "schraffur": "xx",  "marker": "^"},
}
LABEL = {"ridge": "Ridge", "random_forest": "Random Forest",
         "xgboost": "XGBoost",
         "anzahl_einsaetze": "Anzahl Einsätze",
         "einsaetze_je_1000_ew": "Einsätze je 1.000 Ew.",
         "dominante_einsatzart": "Dominante Einsatzart"}


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
    })
    return plt, FuncFormatter


def _komma(FuncFormatter):
    """Deutsches Dezimalkomma auf den Achsen."""
    return FuncFormatter(lambda x, _: f"{x:,.2f}".replace(",", " ")
                         .replace(".", ",").replace(" ", "."))


def a1_boxplots(plt, FuncFormatter) -> list:
    """Boxplot ueber alle Einzellaeufe - die Streuung ehrlich gezeigt."""
    erzeugt = []
    aufgaben = []
    if (REG / "menge_folds.csv").exists():
        f = pd.read_csv(REG / "menge_folds.csv")
        for mass, einheit in (("RMSE", "RMSE"), ("R2", "R²")):
            aufgaben.append(("menge", f, mass, einheit))
    if (KLA / "struktur_folds.csv").exists():
        f = pd.read_csv(KLA / "struktur_folds.csv")
        aufgaben.append(("struktur", f, "macro_f1", "Macro-F1"))

    for strang, folds, mass, einheit in aufgaben:
        ziele = list(dict.fromkeys(folds["zielgroesse"]))
        verf = list(dict.fromkeys(folds["verfahren"]))
        fig, achsen = plt.subplots(1, len(ziele),
                                   figsize=(BREITE, 2.6), sharey=False)
        achsen = np.atleast_1d(achsen)
        for ax, ziel in zip(achsen, ziele):
            daten = [folds[(folds["zielgroesse"] == ziel)
                           & (folds["verfahren"] == v)][mass].dropna()
                     for v in verf]
            kasten = ax.boxplot(daten, patch_artist=True, widths=0.55,
                                medianprops={"color": "black"})
            for stueck, v in zip(kasten["boxes"], verf):
                stueck.set_facecolor(STIL[v]["grau"])
                stueck.set_hatch(STIL[v]["schraffur"])
                stueck.set_edgecolor("black")
            ax.set_xticks(range(1, len(verf) + 1))
            ax.set_xticklabels([LABEL[v] for v in verf], rotation=12)
            ax.set_ylabel(einheit)
            ax.set_xlabel(LABEL.get(ziel, ziel))
            ax.yaxis.set_major_formatter(_komma(FuncFormatter))
            if mass == "R2":
                # Nulllinie: negative R2 sind hier normal und muessen als
                # solche erkennbar sein.
                ax.axhline(0, color="black", linewidth=0.8, linestyle=":")
        pfad = OUT / f"a1_boxplot_{strang}{'_r2' if mass == 'R2' else ''}.pdf"
        fig.savefig(pfad); plt.close(fig); erzeugt.append(pfad)
    return erzeugt


def a2_gegen_baseline(plt, FuncFormatter) -> list:
    """Die Primaeraussage nach #34: jedes Verfahren gegen seine Stufe 2.

    Fehlerbalken aus `std_wiederholungen` - der Wert ueber die 10
    Wiederholungsmittel, nicht ueber die 50 Einzellaeufe. Die Beschriftung sagt
    das ausdruecklich; ohne sie waere der Balken bedeutungslos.
    """
    if not (REG / "menge_mittel.csv").exists():
        return []
    mittel = pd.read_csv(REG / "menge_mittel.csv")
    basis = pd.read_csv(REG / "baselines_mittel.csv")
    basis = basis[basis["modell"] == "Poisson-GLM"]

    ziele = list(dict.fromkeys(mittel["zielgroesse"]))
    fig, achsen = plt.subplots(1, len(ziele), figsize=(BREITE, 2.8))
    achsen = np.atleast_1d(achsen)
    for ax, ziel in zip(achsen, ziele):
        g = mittel[mittel["zielgroesse"] == ziel]
        b = basis[basis["zielgroesse"] == ziel].iloc[0]
        for i, (_, z) in enumerate(g.iterrows()):
            ax.bar(i, z["RMSE_mean"], yerr=z["RMSE_std_wiederholungen"],
                   capsize=3, color=STIL[z["verfahren"]]["grau"],
                   hatch=STIL[z["verfahren"]]["schraffur"],
                   edgecolor="black", width=0.6)
        ax.axhline(b["RMSE_mean"], color="black", linewidth=1.1,
                   linestyle="--", label="Stufe-2-Baseline (Poisson-GLM)")
        ax.set_xticks(range(len(g)))
        ax.set_xticklabels([LABEL[v] for v in g["verfahren"]], rotation=12)
        ax.set_ylabel("RMSE")
        ax.set_xlabel(LABEL.get(ziel, ziel))
        ax.yaxis.set_major_formatter(_komma(FuncFormatter))
        # Kopfraum, damit die Legende die Baseline-Linie nicht ueberdeckt.
        ax.set_ylim(0, ax.get_ylim()[1] * 1.28)
    achsen[0].legend(loc="upper left", frameon=False)
    fig.supxlabel("Fehlerbalken: Standardabweichung über die 10 "
                  "Wiederholungsmittel (std_wiederholungen)",
                  fontsize=SCHRIFT - 1)
    pfad = OUT / "a2_gegen_baseline.pdf"
    fig.savefig(pfad); plt.close(fig)
    return [pfad]


def a3_laufzeit_guete(plt, FuncFormatter) -> list:
    """Unterfrage 3 und 4 in einem Bild: Aufwand gegen Guete.

    Die Zeitachse ist logarithmisch, weil zwischen Ridge und den Ensembles
    Groessenordnungen liegen - linear waere Ridge ein Punkt auf der Null.
    """
    punkte = []
    if (REG / "menge_mittel.csv").exists():
        m = pd.read_csv(REG / "menge_mittel.csv")
        for _, z in m.iterrows():
            punkte.append((z["zielgroesse"], z["verfahren"],
                           z["train_sekunden_mean"], z["RMSE_mean"], "RMSE"))
    if (KLA / "struktur_mittel.csv").exists():
        m = pd.read_csv(KLA / "struktur_mittel.csv")
        for _, z in m.iterrows():
            punkte.append((z["zielgroesse"], z["verfahren"],
                           z["train_sekunden_mean"], z["macro_f1_mean"],
                           "Macro-F1"))
    if not punkte:
        return []

    df = pd.DataFrame(punkte, columns=["ziel", "verfahren", "zeit", "guete", "mass"])
    ziele = list(dict.fromkeys(df["ziel"]))
    fig, achsen = plt.subplots(1, len(ziele), figsize=(BREITE, 2.6))
    achsen = np.atleast_1d(achsen)
    for ax, ziel in zip(achsen, ziele):
        g = df[df["ziel"] == ziel]
        for _, z in g.iterrows():
            ax.scatter(z["zeit"], z["guete"], s=45, marker=STIL[z["verfahren"]]["marker"],
                       facecolor=STIL[z["verfahren"]]["grau"], edgecolor="black",
                       label=LABEL[z["verfahren"]], zorder=3)
        ax.set_xscale("log")
        ax.set_xlabel("Trainingszeit je Fold in s (log)")
        ax.set_ylabel(g["mass"].iloc[0])
        ax.set_title("")
        ax.text(0.02, 0.94, LABEL.get(ziel, ziel), transform=ax.transAxes,
                fontsize=SCHRIFT - 1, va="top")
        ax.yaxis.set_major_formatter(_komma(FuncFormatter))
        ax.grid(True, which="both", linewidth=0.3, alpha=0.5)
    achsen[-1].legend(loc="best", frameon=False)
    pfad = OUT / "a3_laufzeit_guete.pdf"
    fig.savefig(pfad); plt.close(fig)
    return [pfad]


def main() -> int:
    if not (REG / "menge_mittel.csv").exists() and not (KLA / "struktur_mittel.csv").exists():
        raise SystemExit("Keine Ergebnisdateien - erst m02 und m03 ausfuehren.")
    OUT.mkdir(parents=True, exist_ok=True)
    plt, FuncFormatter = _matplotlib()

    erzeugt = (a1_boxplots(plt, FuncFormatter)
               + a2_gegen_baseline(plt, FuncFormatter)
               + a3_laufzeit_guete(plt, FuncFormatter))
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
