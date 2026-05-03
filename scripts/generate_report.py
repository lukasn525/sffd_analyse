"""
Generates a 10-page PDF analysis report for SFFD Fire Incidents (FIR-0001).
Output: results/sffd_fire_incidents_report.pdf

Run:
    .\\venv\\Scripts\\python.exe scripts\\generate_report.py
"""

import warnings
warnings.filterwarnings("ignore")

import polars as pl
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Patch
from pathlib import Path
from datetime import datetime

ROOT      = Path(__file__).parent.parent
DATA_PATH = ROOT / "data" / "raw" / "fire_incidents.parquet"
OUT_PDF   = ROOT / "results" / "sffd_fire_incidents_report.pdf"

# ── Palette ────────────────────────────────────────────────────────────────────
C_BLUE   = "#2563EB"
C_RED    = "#DC2626"
C_GREEN  = "#16A34A"
C_ORANGE = "#EA580C"
C_PURPLE = "#7C3AED"
C_GRAY   = "#6B7280"
C_DARK   = "#1E3A5F"

NUM_COLS = [
    "exposure_number", "suppression_units", "suppression_personnel",
    "ems_units", "ems_personnel", "other_units", "other_personnel",
    "fire_fatalities", "fire_injuries", "civilian_fatalities", "civilian_injuries",
    "number_of_alarms", "floor_of_fire_origin",
    "number_of_floors_with_minimum_damage", "number_of_floors_with_significant_damage",
    "number_of_floors_with_heavy_damage", "number_of_floors_with_extreme_damage",
    "number_of_sprinkler_heads_operating",
    "estimated_property_loss", "estimated_contents_loss",
]
DT_FMT = "%Y-%m-%dT%H:%M:%S%.3f"


# ── Data loading ───────────────────────────────────────────────────────────────

def load_and_prepare() -> pl.DataFrame:
    print("Loading data ...")
    df = pl.read_parquet(DATA_PATH)

    for col in NUM_COLS:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    for col in ["incident_date", "alarm_dttm", "arrival_dttm", "close_dttm"]:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).str.to_datetime(format=DT_FMT, strict=False)
            )

    if "alarm_dttm" in df.columns:
        df = df.with_columns([
            pl.col("alarm_dttm").dt.year().alias("year"),
            pl.col("alarm_dttm").dt.strftime("%b").alias("month_name"),
            pl.col("alarm_dttm").dt.hour().alias("hour"),
            pl.col("alarm_dttm").dt.strftime("%a").alias("weekday"),
        ])

    if "alarm_dttm" in df.columns and "arrival_dttm" in df.columns:
        df = df.with_columns(
            ((pl.col("arrival_dttm") - pl.col("alarm_dttm"))
             .dt.total_seconds() / 60.0).alias("response_min")
        )
    if "alarm_dttm" in df.columns and "close_dttm" in df.columns:
        df = df.with_columns(
            ((pl.col("close_dttm") - pl.col("alarm_dttm"))
             .dt.total_seconds() / 60.0).alias("duration_min")
        )

    print(f"Loaded: {df.shape[0]:,} rows x {df.shape[1]} columns")
    return df


# ── Chart helpers ──────────────────────────────────────────────────────────────

def style_ax(ax, title=None, xlabel=None, ylabel=None):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(labelsize=8)
    if title:
        ax.set_title(title, fontsize=10, fontweight="bold", pad=8, color=C_DARK)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=8)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=8)


def fmt_k(x, _):
    return f"{int(x):,}"


# ── Pages ──────────────────────────────────────────────────────────────────────

def page_cover(df: pl.DataFrame) -> plt.Figure:
    fig = plt.figure(figsize=(8.5, 11))
    fig.patch.set_facecolor("#F8FAFC")

    fig.text(0.5, 0.87, "SFFD Fire Incidents",
             ha="center", fontsize=30, fontweight="bold", color=C_DARK)
    fig.text(0.5, 0.82, "Dataset Analysis Report",
             ha="center", fontsize=16, color=C_GRAY)
    fig.text(0.5, 0.78, f"Generated: {datetime.now().strftime('%B %d, %Y')}",
             ha="center", fontsize=10, color=C_GRAY)

    axl = fig.add_axes([0.1, 0.755, 0.8, 0.003])
    axl.axhline(0.5, color=C_DARK, linewidth=1.5)
    axl.axis("off")

    min_d = df["incident_date"].drop_nulls().min()
    max_d = df["incident_date"].drop_nulls().max()
    n_unique = df["incident_number"].n_unique()
    rt_valid = df.filter(pl.col("response_min").is_between(0, 120))["response_min"]
    avg_rt = rt_valid.mean()

    metrics = [
        ("Total Records",            f"{df.shape[0]:,}"),
        ("Unique Incidents",         f"{n_unique:,}"),
        ("Date Range",               f"{min_d.strftime('%b %Y')} – {max_d.strftime('%b %Y')}"),
        ("Avg Response Time",        f"{avg_rt:.1f} min"),
        ("Fire Fatalities (total)",  f"{int(df['fire_fatalities'].sum()):,}"),
        ("Civilian Fatalities",      f"{int(df['civilian_fatalities'].sum()):,}"),
        ("Fire Injuries (total)",    f"{int(df['fire_injuries'].sum()):,}"),
        ("Civilian Injuries",        f"{int(df['civilian_injuries'].sum()):,}"),
        ("Total Est. Property Loss", f"${df['estimated_property_loss'].drop_nulls().sum():,.0f}"),
        ("Unique Neighborhoods",     f"{df['neighborhood_district'].n_unique()}"),
    ]

    xs = [0.08, 0.55]
    y = 0.71
    bw, bh = 0.38, 0.054
    for i, (label, value) in enumerate(metrics):
        col_i = i % 2
        if col_i == 0 and i > 0:
            y -= bh + 0.012
        ax = fig.add_axes([xs[col_i], y - bh, bw, bh])
        ax.set_facecolor("white")
        for sp in ax.spines.values():
            sp.set_edgecolor("#CBD5E1")
            sp.set_linewidth(0.7)
        ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
        ax.text(0.04, 0.82, label.upper(), fontsize=6, color=C_GRAY,
                fontweight="bold", va="top", transform=ax.transAxes)
        ax.text(0.04, 0.32, value, fontsize=14, color=C_DARK,
                fontweight="bold", va="center", transform=ax.transAxes)

    fig.text(0.5, 0.04,
             "Source: SF Open Data · data.sfgov.org/resource/wr8u-xric · FIR-0001 Data Dictionary",
             ha="center", fontsize=8, color=C_GRAY)
    return fig


def page_temporal(df: pl.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Temporal Analysis", fontsize=14, fontweight="bold", color=C_DARK, y=0.98)
    fig.patch.set_facecolor("white")

    # Incidents per year
    ax = axes[0, 0]
    yr = (df.filter(pl.col("year").is_not_null() & (pl.col("year") >= 2003))
            .group_by("year").len().sort("year"))
    ax.bar(yr["year"].to_list(), yr["len"].to_list(),
           color=C_BLUE, edgecolor="white", width=0.7)
    style_ax(ax, "Incidents per Year", "Year", "Count")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))

    # Incidents by month
    ax = axes[0, 1]
    month_order = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    mo = df.filter(pl.col("month_name").is_not_null()).group_by("month_name").len()
    mo_d = dict(zip(mo["month_name"].to_list(), mo["len"].to_list()))
    ax.bar(month_order, [mo_d.get(m, 0) for m in month_order],
           color=C_ORANGE, edgecolor="white", width=0.7)
    ax.set_xticks(range(12))
    ax.set_xticklabels(month_order, fontsize=7)
    style_ax(ax, "Incidents by Month (all years)", "Month", "Count")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))

    # Incidents by hour of day
    ax = axes[1, 0]
    hr = (df.filter(pl.col("hour").is_not_null())
            .group_by("hour").len().sort("hour"))
    ax.bar(hr["hour"].to_list(), hr["len"].to_list(),
           color=C_GREEN, edgecolor="white", width=0.8)
    style_ax(ax, "Incidents by Hour of Day", "Hour (0–23)", "Count")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))

    # Incidents by weekday
    ax = axes[1, 1]
    day_order = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    wd = df.filter(pl.col("weekday").is_not_null()).group_by("weekday").len()
    wd_d = dict(zip(wd["weekday"].to_list(), wd["len"].to_list()))
    ax.bar(day_order, [wd_d.get(d, 0) for d in day_order],
           color=C_PURPLE, edgecolor="white", width=0.7)
    style_ax(ax, "Incidents by Day of Week", "Day", "Count")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def page_incident_types(df: pl.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(1, 2, figsize=(11, 8.5))
    fig.suptitle("Incident Types & Property Use", fontsize=14, fontweight="bold", color=C_DARK, y=0.98)
    fig.patch.set_facecolor("white")

    ax = axes[0]
    top = (df.filter(pl.col("primary_situation").is_not_null())
             .group_by("primary_situation").len()
             .sort("len", descending=True).head(20).reverse())
    ax.barh([str(s)[:52] for s in top["primary_situation"].to_list()],
            top["len"].to_list(), color=C_BLUE, edgecolor="white", height=0.7)
    style_ax(ax, "Top 20 Primary Situations", "Count", "")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))
    ax.tick_params(axis="y", labelsize=6.5)

    ax = axes[1]
    pu = (df.filter(pl.col("property_use").is_not_null())
            .group_by("property_use").len()
            .sort("len", descending=True).head(15).reverse())
    ax.barh([str(s)[:52] for s in pu["property_use"].to_list()],
            pu["len"].to_list(), color=C_ORANGE, edgecolor="white", height=0.7)
    style_ax(ax, "Top 15 Property Uses", "Count", "")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))
    ax.tick_params(axis="y", labelsize=6.5)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def page_response_times(df: pl.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(1, 2, figsize=(11, 8.5))
    fig.suptitle("Response Times", fontsize=14, fontweight="bold", color=C_DARK, y=0.98)
    fig.patch.set_facecolor("white")

    rt = df.filter(pl.col("response_min").is_between(0, 60))["response_min"].to_numpy()

    ax = axes[0]
    ax.hist(rt, bins=60, color=C_BLUE, edgecolor="white", linewidth=0.3)
    med = float(np.median(rt))
    p90 = float(np.percentile(rt, 90))
    ax.axvline(med, color=C_RED,    linestyle="--", linewidth=1.5, label=f"Median: {med:.1f} min")
    ax.axvline(p90, color=C_ORANGE, linestyle="--", linewidth=1.5, label=f"P90: {p90:.1f} min")
    ax.legend(fontsize=8)
    style_ax(ax, f"Alarm to Arrival Distribution (0-60 min, n={len(rt):,})", "Minutes", "Count")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))

    ax = axes[1]
    bat_rt = (
        df.filter(
            pl.col("response_min").is_between(0, 60) &
            pl.col("battalion").is_not_null()
        )
        .group_by("battalion")
        .agg(pl.col("response_min").median().alias("med_rt"))
        .sort("med_rt")
    )
    ax.barh(bat_rt["battalion"].to_list(), bat_rt["med_rt"].to_list(),
            color=C_GREEN, edgecolor="white", height=0.7)
    style_ax(ax, "Median Response Time by Battalion", "Minutes", "Battalion")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def page_resources(df: pl.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Resource Deployment", fontsize=14, fontweight="bold", color=C_DARK, y=0.98)
    fig.patch.set_facecolor("white")

    configs = [
        (axes[0, 0], "suppression_units",    "Suppression Units per Incident", C_BLUE),
        (axes[0, 1], "ems_units",             "EMS Units per Incident",         C_GREEN),
        (axes[1, 0], "suppression_personnel", "Suppression Personnel",          C_ORANGE),
        (axes[1, 1], "number_of_alarms",      "Number of Alarms",               C_PURPLE),
    ]
    for ax, col, title, color in configs:
        sub = df.filter(pl.col(col).is_not_null())
        cap = float(sub[col].quantile(0.99))
        vc = (
            sub.filter(pl.col(col) <= cap)
               .with_columns(pl.col(col).cast(pl.Int64, strict=False))
               .group_by(col).len().sort(col)
        )
        ax.bar([str(v) for v in vc[col].to_list()], vc["len"].to_list(),
               color=color, edgecolor="white", width=0.7)
        style_ax(ax, title, "Value", "Count")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))
        ax.tick_params(axis="x", labelsize=7)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def page_geography(df: pl.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(1, 2, figsize=(11, 8.5))
    fig.suptitle("Geographic Distribution", fontsize=14, fontweight="bold", color=C_DARK, y=0.98)
    fig.patch.set_facecolor("white")

    ax = axes[0]
    bat = df.group_by("battalion").len().sort("len", descending=True)
    ax.barh(bat["battalion"].to_list()[::-1], bat["len"].to_list()[::-1],
            color=C_BLUE, edgecolor="white", height=0.7)
    style_ax(ax, "Incidents by Battalion", "Count", "")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))

    ax = axes[1]
    nb = (
        df.filter(pl.col("neighborhood_district").is_not_null())
          .group_by("neighborhood_district").len()
          .sort("len", descending=True).head(20).reverse()
    )
    ax.barh([str(s)[:35] for s in nb["neighborhood_district"].to_list()],
            nb["len"].to_list(), color=C_PURPLE, edgecolor="white", height=0.7)
    style_ax(ax, "Top 20 Neighborhoods", "Count", "")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))
    ax.tick_params(axis="y", labelsize=7)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def page_casualties(df: pl.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(1, 2, figsize=(11, 8.5))
    fig.suptitle("Life Safety Impact", fontsize=14, fontweight="bold", color=C_DARK, y=0.98)
    fig.patch.set_facecolor("white")

    cas = (
        df.filter(pl.col("year").is_not_null() & (pl.col("year") >= 2003))
          .group_by("year")
          .agg([
              pl.col("fire_fatalities").sum(),
              pl.col("fire_injuries").sum(),
              pl.col("civilian_fatalities").sum(),
              pl.col("civilian_injuries").sum(),
          ])
          .sort("year")
    )
    years = cas["year"].to_list()
    x = np.arange(len(years))
    w = 0.2

    ax = axes[0]
    ax.bar(x - 1.5*w, cas["fire_fatalities"].to_list(),     w, label="Fire Fatalities",    color=C_RED)
    ax.bar(x - 0.5*w, cas["civilian_fatalities"].to_list(), w, label="Civilian Fatalities", color=C_ORANGE)
    ax.bar(x + 0.5*w, cas["fire_injuries"].to_list(),       w, label="Fire Injuries",       color="#FCA5A5")
    ax.bar(x + 1.5*w, cas["civilian_injuries"].to_list(),   w, label="Civilian Injuries",   color="#FED7AA")
    ax.set_xticks(x)
    ax.set_xticklabels([str(int(y)) for y in years], rotation=45, fontsize=6)
    ax.legend(fontsize=7)
    style_ax(ax, "Fatalities & Injuries by Year", "Year", "Count")

    ax = axes[1]
    totals = {
        "Fire\nFatalities":    int(df["fire_fatalities"].sum()),
        "Fire\nInjuries":      int(df["fire_injuries"].sum()),
        "Civilian\nFatalities":int(df["civilian_fatalities"].sum()),
        "Civilian\nInjuries":  int(df["civilian_injuries"].sum()),
    }
    bars = ax.bar(list(totals.keys()), list(totals.values()),
                  color=[C_RED, "#FCA5A5", C_ORANGE, "#FED7AA"],
                  edgecolor="white", width=0.5)
    for bar, val in zip(bars, totals.values()):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{val:,}", ha="center", fontsize=9, fontweight="bold", color=C_DARK)
    style_ax(ax, "Total Casualties (All Time)", "", "Count")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def page_fire_specifics(df: pl.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Fire-Specific Analysis", fontsize=14, fontweight="bold", color=C_DARK, y=0.98)
    fig.patch.set_facecolor("white")

    fire_df = df.filter(pl.col("ignition_cause").is_not_null())

    ax = axes[0, 0]
    ic = (fire_df.group_by("ignition_cause").len()
                 .sort("len", descending=True).head(10).reverse())
    ax.barh([str(s)[:48] for s in ic["ignition_cause"].to_list()],
            ic["len"].to_list(), color=C_RED, edgecolor="white", height=0.7)
    style_ax(ax, "Ignition Cause", "Count", "")
    ax.tick_params(axis="y", labelsize=7)

    ax = axes[0, 1]
    hs = (df.filter(pl.col("heat_source").is_not_null())
            .group_by("heat_source").len()
            .sort("len", descending=True).head(10).reverse())
    ax.barh([str(s)[:48] for s in hs["heat_source"].to_list()],
            hs["len"].to_list(), color=C_ORANGE, edgecolor="white", height=0.7)
    style_ax(ax, "Heat Source (Top 10)", "Count", "")
    ax.tick_params(axis="y", labelsize=7)

    ax = axes[1, 0]
    fs = (df.filter(pl.col("fire_spread").is_not_null())
            .group_by("fire_spread").len()
            .sort("len", descending=True).reverse())
    ax.barh([str(s)[:48] for s in fs["fire_spread"].to_list()],
            fs["len"].to_list(), color=C_PURPLE, edgecolor="white", height=0.7)
    style_ax(ax, "Fire Spread", "Count", "")
    ax.tick_params(axis="y", labelsize=7)

    ax = axes[1, 1]
    losses = df.filter(
        pl.col("estimated_property_loss").is_not_null() &
        (pl.col("estimated_property_loss") > 0)
    )["estimated_property_loss"].to_numpy()
    if len(losses) > 0:
        ax.hist(np.log10(losses), bins=40, color=C_RED, edgecolor="white", linewidth=0.3)
        style_ax(ax, f"Property Loss Distribution (n={len(losses):,})", "log10($ Loss)", "Count")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def page_detection(df: pl.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Detection & Suppression Systems", fontsize=14, fontweight="bold", color=C_DARK, y=0.98)
    fig.patch.set_facecolor("white")

    configs = [
        (axes[0, 0], "detectors_present",                     "Detectors Present",     C_BLUE),
        (axes[0, 1], "detector_effectiveness",                 "Detector Effectiveness", C_GREEN),
        (axes[1, 0], "automatic_extinguishing_system_present", "AES Present",            C_ORANGE),
        (axes[1, 1], "automatic_extinguishing_sytem_perfomance","AES Performance",       C_PURPLE),
    ]
    for ax, col, title, color in configs:
        sub = df.filter(pl.col(col).is_not_null())
        if sub.is_empty():
            ax.text(0.5, 0.5, "No data available", ha="center", va="center",
                    transform=ax.transAxes, color=C_GRAY, fontsize=10)
            ax.set_title(title, fontsize=10, fontweight="bold", color=C_DARK)
            for sp in ax.spines.values():
                sp.set_visible(False)
            ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
            continue
        vc = sub.group_by(col).len().sort("len", descending=True).head(8).reverse()
        ax.barh([str(s)[:48] for s in vc[col].to_list()],
                vc["len"].to_list(), color=color, edgecolor="white", height=0.7)
        style_ax(ax, title, "Count", "")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_k))
        ax.tick_params(axis="y", labelsize=7)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def page_data_quality(df: pl.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8.5, 11))
    fig.suptitle("Data Quality — Column Completeness", fontsize=14,
                 fontweight="bold", color=C_DARK, y=0.99)
    fig.patch.set_facecolor("white")

    skip = {"year", "month_name", "hour", "weekday", "response_min", "duration_min",
            "point", "data_as_of", "data_loaded_at", "id"}
    rows = df.shape[0]
    null_pct = {
        col: df[col].null_count() / rows * 100
        for col in df.columns if col not in skip
    }
    items  = sorted(null_pct.items(), key=lambda x: x[1])
    labels = [c[:52] for c, _ in items]
    pcts   = [v for _, v in items]
    colors = [C_RED if p > 50 else C_ORANGE if p > 10 else C_GREEN for p in pcts]

    ax.barh(range(len(labels)), pcts, color=colors, edgecolor="white", height=0.75)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=6.5)
    ax.set_xlim(0, 105)
    ax.axvline(10, color=C_GRAY, linestyle=":",  linewidth=0.7, alpha=0.6)
    ax.axvline(50, color=C_GRAY, linestyle="--", linewidth=0.8, alpha=0.6)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    style_ax(ax, "", "% Missing", "")

    legend_handles = [
        Patch(color=C_GREEN,  label="< 10% missing  (complete)"),
        Patch(color=C_ORANGE, label="10–50% missing (partial)"),
        Patch(color=C_RED,    label="> 50% missing  (sparse — fire-only fields)"),
    ]
    ax.legend(handles=legend_handles, fontsize=8, loc="lower right")

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    return fig


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    df = load_and_prepare()
    OUT_PDF.parent.mkdir(exist_ok=True)

    print("\nBuilding pages ...")
    pages = [
        ("Cover",             page_cover(df)),
        ("Temporal",          page_temporal(df)),
        ("Incident Types",    page_incident_types(df)),
        ("Response Times",    page_response_times(df)),
        ("Resources",         page_resources(df)),
        ("Geography",         page_geography(df)),
        ("Life Safety",       page_casualties(df)),
        ("Fire Specifics",    page_fire_specifics(df)),
        ("Detection Systems", page_detection(df)),
        ("Data Quality",      page_data_quality(df)),
    ]

    print("Writing PDF ...")
    with PdfPages(OUT_PDF) as pdf:
        for name, fig in pages:
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)
            print(f"  [OK] {name}")

        info = pdf.infodict()
        info["Title"]   = "SFFD Fire Incidents — Analysis Report"
        info["Author"]  = "sffd_analyse"
        info["Subject"] = "Statistical analysis of SFFD fire incidents (FIR-0001)"

    print(f"\nReport saved: {OUT_PDF}")


if __name__ == "__main__":
    main()
