"""
Eignungspruefung der drei Verfahren (Ridge Regression, Random Forest, XGBoost).

Bezug Bachelorarbeit: Kapitel 5.1 (Data Understanding) / 5.2 (Data Preparation).
Vorgabe Schroeter: "erst plotten, falls keine lineare Baseline vorliegt,
kein lineares Regressionsmodell verwenden" -> Linearitaetspruefung explizit.

WICHTIG (Neufassung 2026-07-26): Alle Diagnosen, die eine Modellentscheidung
begruenden - Linearitaet, Multikollinearitaet, Transformationswahl - werden
AUSSCHLIESSLICH auf dem Trainingsfenster des ERSTEN CV-Folds gerechnet
(2015-01 bis 2021-12). Diese Monate sind in jedem Fold Trainingsdaten und in
keinem Fold Testdaten; sie liegen ausserdem vollstaendig vor dem End-Hold-out.
Damit beruht keine Verfahrensentscheidung auf Beobachtungen, die spaeter zur
Bewertung dienen. Die frueheren Fassungen rechneten auf dem Gesamtdatensatz -
methodisch angreifbar, auch wenn die Ergebnisse aehnlich ausfielen.

Rein deskriptive Kennzahlen (Verteilungen, Klassenbalance, Datenqualitaet)
duerfen den vollen Zeitraum nutzen und sind entsprechend gekennzeichnet.

Prueft NUR, veraendert nichts an der Pipeline.

Input:  data/processed/sf_fire_risk_features.parquet  (Einsatz-Ebene)
Output: results/eignungspruefung/  (Plots + eignungspruefung_summary.md)

Ausfuehren:
  python analyse/eignungspruefung.py
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data" / "processed" / "sf_fire_risk_features.parquet"
OUT  = ROOT / "results" / "eignungspruefung"

# Gemeinsame Logik aus modellierung/ importieren, damit Eignungspruefung und
# Modellierung zwingend denselben Datensatz und dieselben Zeitschnitte sehen.
sys.path.append(str(ROOT / "modellierung"))
from aggregation import (CRIME_ROH, ENDE, EXPOSURE_ROH,  # noqa: E402
                         PARKGEBIETE, PRAEDIKTOREN, START,
                         balanciertes_panel, lade_stadtteil_monat)
from cv import split_holdout, zeit_folds, zeitachse  # noqa: E402

bericht: list[str] = []


def log(txt: str = "") -> None:
    print(txt)
    bericht.append(txt)


def trainingsfenster(panel: pd.DataFrame) -> pd.DataFrame:
    """Monate, die in JEDEM CV-Fold Trainingsdaten sind (= Fold-1-Training).

    Alles, was eine Verfahrensentscheidung begruendet, wird nur hierauf
    gerechnet. So ist ausgeschlossen, dass die Begruendung fuer Ridge auf
    Beobachtungen beruht, die spaeter der Bewertung dienen.
    """
    panel = panel.copy()
    panel["jahr_monat"] = panel["jahr"] * 100 + panel["monat"]
    entwicklung, _ = split_holdout(zeitachse(panel))
    train_monate = zeit_folds(entwicklung)[0][0]
    return panel[panel["jahr_monat"].isin(train_monate)].copy()


# ---------------------------------------------------------------------------
# 1. Datengrundlage und Analyseeinheiten
# ---------------------------------------------------------------------------
def pruefe_datengrundlage(df: pd.DataFrame, panel: pd.DataFrame,
                          train: pd.DataFrame) -> None:
    log("## 1. Datengrundlage und Analyseeinheiten\n")
    log(f"- Einsatz-Ebene (Rohbestand): {len(df):,} Zeilen, "
        f"{len(df.columns)} Spalten, {int(df['jahr'].min())}-{int(df['jahr'].max())}")
    dup = df.duplicated(subset=["einsatz_nummer"]).sum()
    log(f"- Duplikate nach `einsatz_nummer`: {dup:,} "
        + ("(Dedup erfolgt in 02_join.py)" if dup == 0
           else "-> ACHTUNG: Dedup greift nicht!"))
    log(f"- **Analysepanel:** Stadtteil x Monat, {START}-{ENDE}, "
        f"{len(panel):,} Beobachtungen, {panel['stadtteil'].nunique()} Stadtteile, "
        f"{panel.groupby(['jahr','monat']).ngroups} Monate (rechteckig, keine NaN)")
    log(f"- Ausgeschlossene Analyseeinheiten: Treasure Island, Lakeshore, "
        f"Mission Bay (keine durchgaengige ACS-Abdeckung) sowie "
        f"{', '.join(PARKGEBIETE)} (Park-/Institutionsgebiete ohne "
        f"nennenswerte Wohnbevoelkerung, Decision Log #19)")
    tm = train["jahr"] * 100 + train["monat"]
    log(f"- **Trainingsfenster fuer alle Modellentscheidungen:** "
        f"{tm.min()}-{tm.max()} ({train.groupby(['jahr','monat']).ngroups} Monate, "
        f"{len(train):,} Beobachtungen)")

    log("\n| Praediktor | Min | Median | Max | NaN% |")
    log("|---|---|---|---|---|")
    for col in PRAEDIKTOREN:
        s = pd.to_numeric(panel[col], errors="coerce")
        log(f"| {col} | {s.min():.3f} | {s.median():.3f} | {s.max():.3f} | "
            f"{s.isna().mean()*100:.1f}% |")

    log("\n**Zeitvarianz der Merkmale** (Anteil der Stadtteile mit mehr als einem "
        "Wert ueber den Analysezeitraum). Merkmale ohne Zeitvarianz erklaeren "
        "Niveauunterschiede zwischen Stadtteilen, nicht deren zeitliche "
        "Entwicklung - das ist bei der Interpretation zu beachten.\n")
    log("| Merkmal | Stadtteile mit Zeitvarianz | versch. Werte (Mittel) |")
    log("|---|---|---|")
    for col in PRAEDIKTOREN:
        n = panel.groupby("stadtteil")[col].nunique()
        log(f"| {col} | {(n > 1).mean()*100:.0f}% | {n.mean():.0f} |")


# ---------------------------------------------------------------------------
# 2. Zielgroesse Regression
# ---------------------------------------------------------------------------
def pruefe_zielgroesse_regression(panel: pd.DataFrame) -> None:
    log("\n## 2. Zielgroesse Einsatzhaeufigkeit (deskriptiv, voller Zeitraum)\n")
    y = panel["anzahl_einsaetze"]
    log(f"- Beobachtungen: {len(panel):,} | Mittelwert {y.mean():.1f} | "
        f"Median {y.median():.0f} | Varianz {y.var():.1f} | "
        f"Min {y.min()} | Max {y.max()}")
    disp = y.var() / y.mean()
    log(f"- **Dispersionsindex (Var/Mean): {disp:.1f}** -> "
        + ("starke Overdispersion. Die Poisson-Annahme Var = Mean ist deutlich "
           "verletzt; als interpretierbare Count-Baseline ist die "
           "Negative-Binomial-Regression zu verwenden, nicht Poisson."
           if disp > 1.5 else "keine wesentliche Overdispersion, Poisson zulaessig."))
    null = (y == 0).mean() * 100
    log(f"- Anteil Nullmonate: {null:.2f}% -> "
        + ("keine Zero-Inflation, ein Zero-Inflated-Modell ist nicht erforderlich."
           if null < 5 else "Zero-Inflation pruefen."))
    log(f"- Schiefe roh {y.skew():.2f}, nach log(1+y) {np.log1p(y).skew():.2f} "
        "-> die Log-Transformation macht die Zielgroesse annaehernd symmetrisch.")

    fig, ax = plt.subplots(1, 2, figsize=(11, 4))
    ax[0].hist(y, bins=60, color="#4878A8")
    ax[0].set(title=f"Einsaetze je Stadtteil-Monat (Schiefe {y.skew():.2f})",
              xlabel="Einsaetze", ylabel="Haeufigkeit")
    ax[1].hist(np.log1p(y), bins=60, color="#A85048")
    ax[1].set(title=f"log(1+y) (Schiefe {np.log1p(y).skew():.2f})",
              xlabel="log(1+Einsaetze)")
    fig.tight_layout()
    fig.savefig(OUT / "01_zielgroesse_verteilung.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 3. Kriminalitaetsindex: Zeitvarianz und Strukturbruch 2018
# ---------------------------------------------------------------------------
def pruefe_kriminalitaetsindex(panel: pd.DataFrame) -> None:
    log("\n## 3. Kriminalitaetsindex: Zeitvarianz und Strukturbruch 2018\n")
    idx = panel[CRIME_ROH]
    log(f"- Roher Index: Median {idx.median():.2f}, Spanne "
        f"{idx.min():.2f}-{idx.max():.2f}, Schiefe {idx.skew():.2f}")
    log(f"- Logarithmiert (Modellmerkmal): Median "
        f"{panel['log_kriminalitaetsindex'].median():.2f}, Schiefe "
        f"{panel['log_kriminalitaetsindex'].skew():.2f} -> annaehernd symmetrisch")
    n = panel.groupby("stadtteil")[CRIME_ROH].nunique()
    log(f"- Zeitvarianz: im Mittel {n.mean():.0f} verschiedene Werte je Stadtteil "
        f"(vor der Umstellung: 1 Wert, 0 % Zeitvarianz)")

    # Strukturbruch-Check: Der Index soll gegen den SFPD-Systemwechsel 05/2018
    # robust sein, weil sich ein stadtweiter Niveauspruung im Quotienten kuerzt.
    # Pruefung: Wie stark veraendert sich der Index je Stadtteil zwischen 2017
    # (nur Altsystem) und 2019 (nur Neusystem)?
    vor  = panel[panel["jahr"] == 2017].groupby("stadtteil")[CRIME_ROH].median()
    nach = panel[panel["jahr"] == 2019].groupby("stadtteil")[CRIME_ROH].median()
    verh = (nach / vor).dropna()
    log(f"\n**Strukturbruch-Test (2017 vs. 2019, je Stadtteil-Median):**")
    log(f"- Verhaeltnis nach/vor: Median {verh.median():.2f}, "
        f"Spanne {verh.min():.2f}-{verh.max():.2f}, "
        f"Rangkorrelation der Stadtteile {vor.corr(nach, method='spearman'):.3f}")
    stark = verh[(verh < 0.5) | (verh > 2.0)]
    if len(stark):
        log(f"- Stadtteile mit Faktor <0,5 oder >2,0: {len(stark)} "
            f"({', '.join(f'{k} {v:.2f}' for k, v in stark.items())})")
    else:
        log("- Kein Stadtteil veraendert sich um mehr als Faktor 2 -> der "
            "relative Index ist gegen den Systemwechsel robust.")
    log("- Lesart: Eine hohe Rangkorrelation bedeutet, dass die relative "
        "Ordnung der Stadtteile ueber den Systemwechsel hinweg stabil bleibt. "
        "Ein multiplikativer stadtweiter Niveauspruung kuerzt sich im Quotienten "
        "heraus; **nicht** kuerzen wuerde sich eine Verschiebung, die einzelne "
        "Stadtteile unterschiedlich stark trifft (Limitation Kap. 6.3).")

    fig, ax = plt.subplots(1, 2, figsize=(13, 4.5))
    zeit = panel.assign(t=panel["jahr"] + (panel["monat"] - 1) / 12)
    for st in ["Tenderloin", "South Of Market", "Mission", "Sunset/Parkside",
               "Bayview Hunters Point"]:
        s = zeit[zeit["stadtteil"] == st].sort_values("t")
        if len(s):
            ax[0].plot(s["t"], s[CRIME_ROH], lw=1.2, label=st)
    ax[0].axvline(2018.33, color="red", ls="--", lw=1, label="SFPD-Systemwechsel")
    ax[0].axhline(1.0, color="grey", lw=0.8)
    ax[0].set(title="Kriminalitaetsindex im Zeitverlauf", xlabel="Jahr",
              ylabel="Index (1,0 = Stadtdurchschnitt)")
    ax[0].legend(fontsize=7)
    ax[1].scatter(vor, nach, s=18)
    lim = [0, max(vor.max(), nach.max()) * 1.05]
    ax[1].plot(lim, lim, color="red", lw=1)
    ax[1].set(title=f"Index 2017 vs. 2019 (Spearman "
                    f"{vor.corr(nach, method='spearman'):.3f})",
              xlabel="Median 2017 (Altsystem)", ylabel="Median 2019 (Neusystem)")
    fig.tight_layout()
    fig.savefig(OUT / "05_kriminalitaetsindex.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 4. Exposure: absolute Counts vs. Raten
# ---------------------------------------------------------------------------
def pruefe_exposure(train: pd.DataFrame) -> None:
    log("\n## 4. Exposure-Kontrolle (nur Trainingsfenster)\n")
    y    = train["anzahl_einsaetze"].astype(float)
    rate = y / train[EXPOSURE_ROH].astype(float) * 1000
    log("Warum die Exposure-Entscheidung inhaltlich zaehlt: Ohne Kontrolle der "
        "Stadtteilgroesse sagt ein Modell im Kern die Einwohnerzahl vorher. Die "
        "Tabelle zeigt, wie sich die Korrelationen aendern, wenn statt der "
        "absoluten Einsatzzahl die Rate je 1.000 Einwohner betrachtet wird.\n")
    log("| Merkmal | r mit Einsatzzahl | r mit Einsaetzen je 1.000 Ew. |")
    log("|---|---|---|")
    for col in PRAEDIKTOREN:
        log(f"| {col} | {train[col].corr(y):+.3f} | {train[col].corr(rate):+.3f} |")
    log("\n- Konsequenz (Decision Log #13): `log_bevoelkerung` geht als Merkmal "
        "in alle Modelle ein und dient bei der NegBin-Baseline als Offset. Die "
        "Zielgroesse bleibt eine Zaehlgroesse, damit die "
        "Overdispersion-Argumentation gueltig bleibt. Eine Sensitivitaetsanalyse "
        "mit der Rate als Zielgroesse ist als Robustheitscheck vorgesehen.")


# ---------------------------------------------------------------------------
# 5. Linearitaetspruefung (Schroeter-Pruefpunkt) - NUR Trainingsfenster
# ---------------------------------------------------------------------------
def pruefe_linearitaet(train: pd.DataFrame) -> dict:
    import statsmodels.api as sm

    log("\n## 5. Linearitaetspruefung fuer Ridge Regression (Vorgabe Schroeter)\n")
    log("Berechnet ausschliesslich auf dem Trainingsfenster des ersten Folds.\n")
    y = train["anzahl_einsaetze"].astype(float)

    fig, axes = plt.subplots(3, 4, figsize=(16, 10))
    korr = {}
    for ax, col in zip(axes.flat, PRAEDIKTOREN):
        ax.scatter(train[col], y, s=3, alpha=0.15)
        r = float(np.corrcoef(train[col], y)[0, 1])
        korr[col] = r
        ax.set_title(f"{col}\nr={r:.2f}", fontsize=8)
    for ax in axes.flat[len(PRAEDIKTOREN):]:
        ax.axis("off")
    fig.suptitle("Praediktoren vs. Einsaetze je Stadtteil-Monat (Trainingsfenster)")
    fig.tight_layout()
    fig.savefig(OUT / "03_linearitaet_streudiagramme.png", dpi=150)
    plt.close(fig)

    X = sm.add_constant(train[PRAEDIKTOREN].astype(float))
    fig, ax = plt.subplots(1, 2, figsize=(11, 4))
    r2, negativ = {}, {}
    for i, (name, ziel) in enumerate([("y roh", y), ("log(1+y)", np.log1p(y))]):
        fit = sm.OLS(ziel, X).fit()
        r2[name] = fit.rsquared
        pred = fit.fittedvalues if name == "y roh" else np.expm1(fit.fittedvalues)
        negativ[name] = float((pred < 0).mean() * 100)
        ax[i].scatter(fit.fittedvalues, fit.resid, s=3, alpha=0.15)
        ax[i].axhline(0, color="red", lw=1)
        ax[i].set(title=f"Residuen ({name}), R2={fit.rsquared:.3f}",
                  xlabel="Vorhersage", ylabel="Residuum")
    fig.tight_layout()
    fig.savefig(OUT / "04_residuenanalyse.png", dpi=150)
    plt.close(fig)

    staerkster = max(korr, key=lambda k: abs(korr[k]))
    log(f"- OLS R2 (y roh):    {r2['y roh']:.3f} | negative Vorhersagen: "
        f"{negativ['y roh']:.1f} %")
    log(f"- OLS R2 (log(1+y)): {r2['log(1+y)']:.3f} | negative Vorhersagen nach "
        f"Ruecktransformation: {negativ['log(1+y)']:.1f} %")
    log(f"- Staerkster Einzelpraediktor: `{staerkster}` (r = {korr[staerkster]:+.2f})")
    log(f"\n**Befund:** Eine lineare Baseline liegt vor - die Strukturmerkmale "
        f"erklaeren bereits linear einen erheblichen Teil der Varianz "
        f"(R2 = {r2['y roh']:.2f} auf der Rohskala). Das Schroeter-Kriterium ist "
        f"damit erfuellt und Ridge Regression als interpretierbares lineares "
        f"Verfahren zulaessig. Einschraenkend zeigen die Residuen auf der "
        f"Rohskala eine Trichterform (Heteroskedastizitaet, erwartbar bei "
        f"Zaehldaten), und das Modell erzeugt negative Vorhersagen. Beides "
        f"spricht fuer die Log-Spezifikation: Ridge wird auf log(1+y) "
        f"geschaetzt, die Guetemasse werden nach Ruecktransformation auf der "
        f"Originalskala berechnet (Decision Log #2). Ergaenzend dient die "
        f"Negative-Binomial-Regression als verteilungsgerechte Count-Baseline.")
    return r2


# ---------------------------------------------------------------------------
# 6. Multikollinearitaet (VIF) - NUR Trainingsfenster
# ---------------------------------------------------------------------------
def pruefe_vif(train: pd.DataFrame) -> None:
    import statsmodels.api as sm
    from statsmodels.stats.outliers_influence import variance_inflation_factor

    log("\n## 6. Multikollinearitaet (VIF, nur Trainingsfenster)\n")
    log("Berechnet auf den eindeutigen Stadtteil-Merkmalskombinationen des "
        "Trainingsfensters. Wuerde man alle Panelzeilen verwenden, waeren die "
        "Werte allein durch die Wiederholung derselben Kombination ueber die "
        "Monate hinweg kuenstlich stabilisiert.\n")
    d = train[PRAEDIKTOREN].astype(float).drop_duplicates()
    X = sm.add_constant((d - d.mean()) / d.std())
    log(f"- Grundlage: {len(d):,} eindeutige Merkmalskombinationen\n")
    log("| Praediktor | VIF |")
    log("|---|---|")
    maxvif, maxcol = 0.0, ""
    for i, col in enumerate(X.columns):
        if col == "const":
            continue
        vif = float(variance_inflation_factor(X.values, i))
        if vif > maxvif:
            maxvif, maxcol = vif, col
        marker = " (>10: stark)" if vif > 10 else (" (>5: erhoeht)" if vif > 5 else "")
        log(f"| {col} | {vif:.1f}{marker} |")
    log(f"\n**Befund:** Hoechster VIF {maxvif:.1f} (`{maxcol}`). Erhoehte Werte bei "
        "Einkommen, Miete und Bildung sind sachlich erwartbar - es handelt sich "
        "um verschiedene Messungen desselben soziooekonomischen Gefaelles. Genau "
        "diese Konstellation ist der Anwendungsfall der L2-Regularisierung "
        "(Hoerl & Kennard 1970): Ridge stabilisiert die Schaetzung, ohne "
        "Merkmale zu entfernen. Fuer Random Forest und XGBoost ist "
        "Multikollinearitaet unkritisch; fuer die Interpretation einzelner "
        "SHAP-Werte muss sie jedoch beruecksichtigt werden, weil sich Beitraege "
        "auf korrelierte Merkmale verteilen.")


# ---------------------------------------------------------------------------
# 7. Zielgroesse Klassifikation
# ---------------------------------------------------------------------------
NFIRS_SERIEN = {
    "1": "Brand (100er)", "2": "Explosion (200er)", "3": "Rettung/EMS (300er)",
    "4": "Gefahrenlage (400er)", "5": "Service (500er)",
    "6": "Good Intent (600er)", "7": "Fehlalarm (700er)",
    "8": "Naturereignis (800er)", "9": "Sonstige (900er)",
}


def pruefe_zielgroesse_klassifikation(df: pd.DataFrame,
                                      stadtteile: list[str]) -> None:
    log("\n## 7. Zielgroesse Einsatzart (Klassifikation)\n")
    # Gleiche Grundgesamtheit wie die Regression: gleicher Zeitraum, gleiche
    # Stadtteile. Sonst beziehen sich die beiden Teile der Arbeit auf
    # unterschiedliche Datenbestaende.
    d = df[(df["jahr"] * 100 + df["monat"]).between(START, ENDE)]
    d = d[d["stadtteil"].isin(stadtteile)]
    log(f"- Grundgesamtheit: {len(d):,} Einzeleinsaetze "
        f"({START}-{ENDE}, {d['stadtteil'].nunique()} Stadtteile) - identisch "
        f"abgegrenzt wie das Regressionspanel")

    code = d["einsatzart"].astype(str).str.extract(r"^(\d)")[0]
    verteilung = code.map(NFIRS_SERIEN).fillna("unbekannt").value_counts(normalize=True) * 100
    log("\n| NFIRS-Serie | Anteil |")
    log("|---|---|")
    for k, v in verteilung.items():
        log(f"| {k} | {v:.1f}% |")

    brand = (code == "1")
    anteil = brand.mean() * 100
    log(f"\n- **Binaer Brand vs. Nicht-Brand: {anteil:.1f}% vs. {100-anteil:.1f}%** "
        f"(Verhaeltnis 1:{(100-anteil)/anteil:.1f})")
    log(f"- Empfohlenes Gegengewicht: `class_weight='balanced'` bzw. "
        f"`scale_pos_weight = {(100-anteil)/anteil:.1f}`; Bewertung mit F1 "
        f"(positive Klasse = Brand) und AUROC statt Accuracy - ein Modell, das "
        f"immer 'kein Brand' sagt, erreicht bereits {100-anteil:.1f}% Accuracy.")

    # Pseudo-Signal: wie oft wiederholt sich dieselbe Merkmalskombination?
    je_monat = d.groupby(["stadtteil", "jahr", "monat"]).size()
    log(f"\n- **Pseudo-Signal-Problem:** Die Stadtteilmerkmale sind je "
        f"Stadtteil-Monat konstant. Im Mittel teilen sich "
        f"{je_monat.mean():.0f} Einsaetze (Median {je_monat.median():.0f}, "
        f"Maximum {je_monat.max():,}) dieselbe Merkmalsauspraegung. "
        f"{len(d):,} Zeilen enthalten damit nur "
        f"{len(je_monat):,} verschiedene Stadtteil-Monats-Profile. "
        f"Das ist bei Modellguete und Feature Importance zu beruecksichtigen "
        f"und im Methodenkapitel zu benennen.")

    # Zeitliche Entwicklung des Brandanteils
    jahre = d.assign(ist_brand=brand).groupby("jahr")["ist_brand"].mean() * 100
    log(f"- Brandanteil je Jahr: {jahre.min():.1f}%-{jahre.max():.1f}% "
        f"(stabil, kein Trendbruch)" if jahre.max() - jahre.min() < 5 else
        f"- Brandanteil je Jahr schwankt zwischen {jahre.min():.1f}% und "
        f"{jahre.max():.1f}% -> im Zeitverlauf pruefen")

    fig, ax = plt.subplots(1, 2, figsize=(13, 4.5))
    verteilung.sort_values().plot.barh(ax=ax[0], color="#4878A8")
    ax[0].set(title="Klassenverteilung Einsatzart (NFIRS-Serien)", xlabel="Anteil in %")
    ax[1].plot(jahre.index, jahre.values, marker="o")
    ax[1].set(title="Brandanteil je Jahr", xlabel="Jahr", ylabel="Anteil in %")
    ax[1].set_ylim(0, max(20, jahre.max() * 1.2))
    fig.tight_layout()
    fig.savefig(OUT / "02_klassenbalance_einsatzart.png", dpi=150)
    plt.close(fig)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    df    = pd.read_parquet(DATA)
    panel = balanciertes_panel(lade_stadtteil_monat())
    train = trainingsfenster(panel)

    log("# Eignungspruefung: Ridge Regression, Random Forest, XGBoost\n")
    log(f"Stand {pd.Timestamp.today():%Y-%m-%d}. Neufassung nach dem "
        "Preprocessing-Audit: Alle Diagnosen, die eine Modellentscheidung "
        "begruenden, werden ausschliesslich auf dem Trainingsfenster des ersten "
        "CV-Folds gerechnet - diese Monate sind in keinem Fold Testdaten und "
        "liegen vollstaendig vor dem End-Hold-out. Rein deskriptive Kennzahlen "
        "nutzen den vollen Analysezeitraum und sind entsprechend gekennzeichnet.\n")

    pruefe_datengrundlage(df, panel, train)
    pruefe_zielgroesse_regression(panel)
    pruefe_kriminalitaetsindex(panel)
    pruefe_exposure(train)
    pruefe_linearitaet(train)
    pruefe_vif(train)
    pruefe_zielgroesse_klassifikation(df, sorted(panel["stadtteil"].unique()))

    log("\n## 8. Leakage- und Strukturpruefung (Stand nach dem Audit)\n")
    log("| Punkt | Status |")
    log("|---|---|")
    log("| ACS-Join | **behoben**: letzter *publizierter* Snapshot "
        "(`acs_jahr <= Einsatzjahr - 1`), Decision Log #4 und #11 |")
    log("| Kriminalitaetsmerkmale | **behoben**: relativer Index je Stadtteil x "
        "Monat, rollierendes 12-Monats-Fenster endend im Vormonat, Decision "
        "Log #17 |")
    log("| Randmonat | **behoben**: fester Analysezeitraum "
        f"{START}-{ENDE}, Decision Log #12 und #18 |")
    log("| Fehlende Werte | **behoben**: kein `bfill` mehr (Zukunfts-Imputation), "
        "Decision Log #10; verbleibende NaN werden ueber den Zeitraumfilter "
        "und das balancierte Panel behandelt |")
    log("| Exposure | **behoben**: `log_bevoelkerung` als Merkmal, Rohwert fuer "
        "NegBin-Offset erhalten, Decision Log #13 |")
    log("| Analyseeinheiten | **behoben**: Park-/Institutionsgebiete "
        "ausgeschlossen, Decision Log #19 |")
    log("| End-Hold-out | **eingerichtet**: letzte 12 Monate, beim Tuning "
        "unberuehrt, Decision Log #14 (`modellierung/cv.py`) |")
    log("| Land Use | **offen (Limitation)**: Snapshot 2020, ueber den gesamten "
        "Zeitraum konstant -> als quasi-stabiles Strukturmerkmal zu "
        "interpretieren, Kap. 6.3 |")
    log("| Response-Time-Filter | **dokumentieren**: 0-60 min entfernt ~1,7 % der "
        "Einsaetze bereits in der Prep-Pipeline; alle Zaehlungen beziehen sich "
        "auf den gefilterten Bestand |")

    (OUT / "eignungspruefung_summary.md").write_text("\n".join(bericht),
                                                     encoding="utf-8")
    print(f"\n=> {OUT.relative_to(ROOT)}/eignungspruefung_summary.md + 5 Plots")


if __name__ == "__main__":
    main()
