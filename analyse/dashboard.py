"""
Dashboard: Deskriptive Uebersicht – sf_fire_risk_features_cleaned

Liest:   data/processed/sf_fire_risk_features_cleaned.parquet
Schreibt: results/dashboard.png

Ausfuehren:
  python analyse/dashboard.py
"""
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import pandas as pd

ROOT      = Path(__file__).parent.parent
DATA_PATH = ROOT / "data" / "processed" / "sf_fire_risk_features_cleaned.parquet"
OUT_PATH  = ROOT / "results" / "dashboard.png"

GRUEN  = "#2ecc71"
BLAU   = "#3498db"
ROT    = "#e74c3c"
GRAU   = "#95a5a6"
DUNKEL = "#2c3e50"
BG     = "#f8f9fa"


def _balken(ax, labels, werte, farbe, titel, xlabel=""):
    ax.barh(labels, werte, color=farbe, edgecolor="white", height=0.7)
    ax.set_title(titel, fontsize=11, fontweight="bold", color=DUNKEL, pad=8)
    ax.set_xlabel(xlabel, fontsize=9, color=DUNKEL)
    ax.invert_yaxis()
    ax.tick_params(labelsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor(BG)
    for i, v in enumerate(werte):
        ax.text(v * 1.01, i, f"{v:,.0f}", va="center", fontsize=7.5, color=DUNKEL)


def _metric_table(ax, df):
    COLS = [
        "antwortzeit_min", "schaetzung_sachschaden_usd",
        "armutsquote_pct", "akademikerquote_pct",
        "median_haushaltseinkommen", "median_miete",
        "leerstandsquote_pct", "anteil_altbau_vor_1940_pct",
        "anteil_wohngebaeude_pct", "anteil_risikogewerbe_pct",
    ]
    LABELS = [
        "Antwortzeit (min)", "Sachschaden (USD)",
        "Armutsquote", "Akademikerquote",
        "Median Einkommen", "Median Miete",
        "Leerstandsquote", "Altbauanteil <1940",
        "Wohngebäudeanteil", "Hochrisiko-Gewerbe",
    ]
    rows = []
    for col, label in zip(COLS, LABELS):
        s = df[col].dropna()
        rows.append([label, f"{s.mean():.2f}", f"{s.min():.2f}", f"{s.max():.2f}",
                     f"{df[col].isna().mean()*100:.1f}%"])

    ax.axis("off")
    tbl = ax.table(
        cellText=rows,
        colLabels=["Variable", "Mittelwert", "Min", "Max", "NaN%"],
        cellLoc="right", colLoc="center", loc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8.5)
    tbl.scale(1, 1.55)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#dee2e6")
        if r == 0:
            cell.set_facecolor(DUNKEL)
            cell.set_text_props(color="white", fontweight="bold")
        elif r % 2 == 0:
            cell.set_facecolor("#e9ecef")
        else:
            cell.set_facecolor("white")
        if c == 0:
            cell.set_text_props(ha="left")
    ax.set_title("Numerische Variablen – Kernindikatoren", fontsize=11,
                 fontweight="bold", color=DUNKEL, pad=10)


def _jahresverlauf(ax, df):
    jahres = df[df["jahr"].between(2003, 2025)].groupby("jahr").size()
    ax.fill_between(jahres.index, jahres.values, alpha=0.25, color=BLAU)
    ax.plot(jahres.index, jahres.values, color=BLAU, linewidth=2, marker="o",
            markersize=4, markerfacecolor="white", markeredgewidth=1.5)
    ax.set_title("Einsätze pro Jahr (2003–2025)", fontsize=11,
                 fontweight="bold", color=DUNKEL, pad=8)
    ax.set_xlabel("Jahr", fontsize=9, color=DUNKEL)
    ax.set_ylabel("Einsätze", fontsize=9, color=DUNKEL)
    ax.tick_params(labelsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_facecolor(BG)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x/1000:.0f}k"))


def build(df: pd.DataFrame) -> None:
    fig = plt.figure(figsize=(18, 13), facecolor=BG)
    fig.suptitle(
        f"SFFD Fire Risk Dashboard  •  {len(df):,} Einsätze  •  41 Stadtteile",
        fontsize=15, fontweight="bold", color=DUNKEL, y=0.98,
    )

    gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.45, wspace=0.35,
                           left=0.06, right=0.97, top=0.93, bottom=0.04)

    # Row 0 – top neighborhoods / battalions / year trend
    ax_nb  = fig.add_subplot(gs[0, 0])
    ax_bat = fig.add_subplot(gs[0, 1])
    ax_yr  = fig.add_subplot(gs[0, 2])

    top_nb = df["stadtteil"].value_counts().head(10)
    _balken(ax_nb, top_nb.index[::-1], top_nb.values[::-1], BLAU,
            "Top-10 Stadtteile nach Einsatzzahl", "Einsätze")

    bat = df["bataillon"].value_counts().sort_index()
    _balken(ax_bat, bat.index, bat.values, GRUEN,
            "Einsätze nach Bataillon", "Einsätze")

    _jahresverlauf(ax_yr, df)

    # Row 1 – incident types + response time dist
    ax_art = fig.add_subplot(gs[1, :2])
    ax_rt  = fig.add_subplot(gs[1, 2])

    top_art = df["einsatzart"].value_counts().head(8)
    labels_art = [s[:55] for s in top_art.index[::-1]]
    _balken(ax_art, labels_art, top_art.values[::-1], ROT,
            "Top-8 Einsatzarten", "Einsätze")

    ax_rt.hist(df["antwortzeit_min"].dropna(), bins=60, color=BLAU,
               edgecolor="white", linewidth=0.4)
    ax_rt.axvline(df["antwortzeit_min"].mean(), color=ROT, linewidth=1.8,
                  linestyle="--", label=f"Mittel {df['antwortzeit_min'].mean():.1f} min")
    ax_rt.set_title("Antwortzeit-Verteilung", fontsize=11,
                    fontweight="bold", color=DUNKEL, pad=8)
    ax_rt.set_xlabel("Minuten", fontsize=9, color=DUNKEL)
    ax_rt.legend(fontsize=8)
    ax_rt.spines[["top", "right"]].set_visible(False)
    ax_rt.set_facecolor(BG)
    ax_rt.tick_params(labelsize=8)

    # Row 2 – metric table spanning full width
    ax_tbl = fig.add_subplot(gs[2, :])
    _metric_table(ax_tbl, df)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_PATH, dpi=150, bbox_inches="tight", facecolor=BG)
    print(f"=> {OUT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    try:
        df = pd.read_parquet(DATA_PATH)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Datei nicht gefunden: {DATA_PATH.relative_to(ROOT)}\n"
            "Erst 'python pipeline/run_pipeline.py' ausfuehren.")
    build(df)
