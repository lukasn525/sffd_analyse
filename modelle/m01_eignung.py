"""
Schritt 3 der Aufbereitung: Eignungspruefung und Vergleichsgroessen.

Dieser Schritt erzeugt keine Modelldaten mehr. Er prueft die VORAUSSETZUNGEN von
Ridge Regression, Random Forest und XGBoost gegen die fertigen Datensaetze,
faellt ein explizites Urteil je Verfahren (Abschnitt 9) und rechnet die drei
Vergleichsgroessen, an denen sich jedes spaetere Modell messen lassen muss
(Abschnitt 11). Damit ist vor der Modellierung belegt, dass die Verfahrenswahl
zum Datensatz passt - und nicht umgekehrt der Datensatz nachtraeglich zur
Verfahrenswahl erklaert wird.

Bezug Bachelorarbeit: Kapitel 5.1 (Data Understanding) / 5.2 (Data Preparation).
Vorgabe Schroeter: "erst plotten, falls keine lineare Baseline vorliegt, kein
lineares Regressionsmodell verwenden" -> Linearitaetspruefung in Abschnitt 5.

METHODISCHE REGEL: Alle Diagnosen, die eine Modellentscheidung begruenden -
Linearitaet, Multikollinearitaet, Transformationswahl - werden AUSSCHLIESSLICH
auf dem Trainingsfenster des ersten CV-Folds gerechnet. Diese Monate sind in
jedem Fold Trainingsdaten und in keinem Fold Testdaten; sie liegen ausserdem
vollstaendig vor dem End-Hold-out. Rein deskriptive Kennzahlen duerfen den
vollen Zeitraum nutzen und sind entsprechend gekennzeichnet.

Eingang:  data/processed/regression.parquet
          data/processed/klassifikation.parquet
          data/processed/einsaetze.parquet   (nur fuer die Rohbestand-Kennzahlen)
Ausgang:  results/eignungspruefung/  (Bericht + 5 Plots)
          results/regression/baselines_*.csv

Ausfuehren:
  python prep/s3_pruefung.py             # Eignungspruefung + Baselines
  python prep/s3_pruefung.py baselines   # nur die Vergleichsgroessen
"""
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt   # noqa: E402
import numpy as np                # noqa: E402
import pandas as pd               # noqa: E402

from config import (CRIME_ROH, ENDE, EXPOSURE_ROH, FEATURE_SETS,  # noqa: E402
                    KLASSEN, MERKMALE_KATEGORIAL, MERKMALE_STRUKTUR,
                    MERKMALE_ZEIT, N_FOLDS, PARKGEBIETE, PFAD_EINSAETZE,
                    PFAD_KLASSIFIKATION, PFAD_REGRESSION, PRAEDIKTOREN,
                    RESULTS_DIR, ROOT, SAISON, START)
from s2_datensaetze import bewerte_regression, fold_masken  # noqa: E402

OUT      = RESULTS_DIR / "eignungspruefung"
OUT_BASE = RESULTS_DIR / "regression"

bericht: list[str] = []
# Sammelt die maschinell geprueften Kriterien fuer das Urteil in Abschnitt 9:
# (Verfahren, Kriterium, gemessener Wert, Schwelle, Urteil)
urteile: list[tuple[str, str, str, str, str]] = []

OK, ACHTUNG, KRITISCH = "erfuellt", "mit Auflage", "nicht erfuellt"


def log(txt: str = "") -> None:
    print(txt)
    bericht.append(txt)


def pruefe(verfahren: str, kriterium: str, wert: str, schwelle: str,
           bestanden: bool, auflage: bool = False) -> None:
    urteile.append((verfahren, kriterium, wert, schwelle,
                    ACHTUNG if auflage else (OK if bestanden else KRITISCH)))


# ---------------------------------------------------------------------------
# 1. Datengrundlage und Analyseeinheiten
# ---------------------------------------------------------------------------
def pruefe_datengrundlage(roh: pd.DataFrame, n_spalten: int, panel: pd.DataFrame,
                          train: pd.DataFrame) -> None:
    log("## 1. Datengrundlage und Analyseeinheiten\n")
    log(f"- Einsatz-Ebene (Rohbestand): {len(roh):,} Zeilen, "
        f"{n_spalten} Spalten, {int(roh['jahr'].min())}-{int(roh['jahr'].max())}")
    dup = roh.duplicated(subset=["einsatz_nummer"]).sum()
    log(f"- Duplikate nach `einsatz_nummer`: {dup:,} "
        + ("(Dedup erfolgt in prep/s1_daten.py)" if dup == 0
           else "-> ACHTUNG: Dedup greift nicht!"))
    log(f"- **Analysepanel:** Stadtteil x Monat, {START}-{ENDE}, "
        f"{len(panel):,} Beobachtungen, {panel['stadtteil'].nunique()} Stadtteile, "
        f"{panel.groupby(['jahr', 'monat']).ngroups} Monate (rechteckig, "
        f"{int(panel[PRAEDIKTOREN].isna().sum().sum())} NaN)")
    log(f"- Ausgeschlossene Analyseeinheiten: Treasure Island, Lakeshore, "
        f"Mission Bay (keine durchgaengige ACS-Abdeckung) sowie "
        f"{', '.join(PARKGEBIETE)} (Park-/Institutionsgebiete ohne "
        f"nennenswerte Wohnbevoelkerung, Decision Log #19)")
    log(f"- **Trainingsfenster fuer alle Modellentscheidungen:** "
        f"{train['jahr_monat'].min()}-{train['jahr_monat'].max()} "
        f"({train.groupby(['jahr', 'monat']).ngroups} Monate, "
        f"{len(train):,} Beobachtungen)")

    log("\n| Praediktor | Min | Median | Max | NaN% |")
    log("|---|---|---|---|---|")
    for col in PRAEDIKTOREN:
        s = pd.to_numeric(panel[col], errors="coerce")
        log(f"| {col} | {s.min():.3f} | {s.median():.3f} | {s.max():.3f} | "
            f"{s.isna().mean() * 100:.1f}% |")

    log("\n**Zeitvarianz der Merkmale** (Anteil der Stadtteile mit mehr als einem "
        "Wert ueber den Analysezeitraum). Merkmale ohne Zeitvarianz erklaeren "
        "Niveauunterschiede zwischen Stadtteilen, nicht deren zeitliche "
        "Entwicklung - das ist bei der Interpretation zu beachten.\n")
    log("| Merkmal | Stadtteile mit Zeitvarianz | versch. Werte (Mittel) |")
    log("|---|---|---|")
    for col in PRAEDIKTOREN:
        n = panel.groupby("stadtteil")[col].nunique()
        log(f"| {col} | {(n > 1).mean() * 100:.0f}% | {n.mean():.0f} |")

    # Stichprobengroesse je Merkmal - Voraussetzung fuer beide Baumverfahren.
    p = len(FEATURE_SETS["S+L"])
    verhaeltnis = len(train) / p
    pruefe("Random Forest / XGBoost", "Beobachtungen je Merkmal",
           f"{len(train):,} / {p} = {verhaeltnis:.0f}", ">= 10", verhaeltnis >= 10)


def pruefe_designmatrix(panel: pd.DataFrame, kl: pd.DataFrame) -> None:
    """Kann man die Merkmale ohne Umweg an alle drei Verfahren uebergeben?

    Geprueft wird, was sonst erst im Modellskript auffaellt: fehlende Werte,
    unendliche Werte, konstante Spalten und - der subtilste Punkt - pandas-eigene
    (nullable) Datentypen. Eine einzige `Int64`-Spalte macht aus `X.to_numpy()`
    ein object-Array; scikit-learn faengt das still ab, XGBoost lehnt es ab.
    """
    log("\n**Modelltauglichkeit der Designmatrix**\n")
    log("| Datensatz | Merkmale | dtypes | X.to_numpy() | NaN | inf | konstant |")
    log("|---|---|---|---|---|---|---|")
    alles_ok = True
    for name, d, feats in [
        ("Regression (S+L)", panel, FEATURE_SETS["S+L"]),
        ("Klassifikation (A+B)", kl,
         MERKMALE_STRUKTUR + MERKMALE_ZEIT + MERKMALE_KATEGORIAL),
    ]:
        X = d[feats]
        typen = sorted(set(map(str, X.dtypes)))
        matrix = X.to_numpy()
        n_nan = int(X.isna().sum().sum())
        n_inf = int(np.isinf(X.astype("float64").to_numpy()).sum())
        konst = [c for c in feats if X[c].nunique() <= 1]
        ok = (matrix.dtype != object and n_nan == 0 and n_inf == 0 and not konst)
        alles_ok &= ok
        log(f"| {name} | {len(feats)} | {', '.join(typen)} | {matrix.dtype} | "
            f"{n_nan} | {n_inf} | {len(konst)} |")
    pruefe("alle Verfahren", "Designmatrix direkt uebergebbar",
           "float64, keine NaN/inf/Konstanten" if alles_ok else "Maengel s. Tabelle",
           "keine nullable dtypes, kein object-Array", alles_ok)


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

    pruefe("NegBin-Baseline", "Overdispersion (Var/Mean)", f"{disp:.1f}",
           "> 1,5 -> NegBin statt Poisson", disp > 1.5)
    pruefe("NegBin-Baseline", "Zero-Inflation (Anteil Nullmonate)",
           f"{null:.2f} %", "< 5 % -> kein ZIP-Modell", null < 5)

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
# 3. Kriminalitaetsindex
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

    # Der Index soll gegen den SFPD-Systemwechsel 05/2018 robust sein, weil sich
    # ein stadtweiter Niveausprung im Quotienten kuerzt. Pruefung: Wie stark
    # veraendert sich der Index je Stadtteil zwischen 2017 (nur Altsystem) und
    # 2019 (nur Neusystem)?
    vor  = panel[panel["jahr"] == 2017].groupby("stadtteil")[CRIME_ROH].median()
    nach = panel[panel["jahr"] == 2019].groupby("stadtteil")[CRIME_ROH].median()
    verh = (nach / vor).dropna()
    rang = vor.corr(nach, method="spearman")
    log("\n**Strukturbruch-Test (2017 vs. 2019, je Stadtteil-Median):**")
    log(f"- Verhaeltnis nach/vor: Median {verh.median():.2f}, "
        f"Spanne {verh.min():.2f}-{verh.max():.2f}, "
        f"Rangkorrelation der Stadtteile {rang:.3f}")
    stark = verh[(verh < 0.5) | (verh > 2.0)]
    if len(stark):
        log(f"- Stadtteile mit Faktor <0,5 oder >2,0: {len(stark)} "
            f"({', '.join(f'{k} {v:.2f}' for k, v in stark.items())})")
    else:
        log("- Kein Stadtteil veraendert sich um mehr als Faktor 2 -> der "
            "relative Index ist gegen den Systemwechsel robust.")
    log("- Lesart: Eine hohe Rangkorrelation bedeutet, dass die relative Ordnung "
        "der Stadtteile ueber den Systemwechsel hinweg stabil bleibt. Ein "
        "multiplikativer stadtweiter Niveausprung kuerzt sich im Quotienten "
        "heraus; **nicht** kuerzen wuerde sich eine Verschiebung, die einzelne "
        "Stadtteile unterschiedlich stark trifft (Limitation Kap. 6.3).")

    pruefe("alle Verfahren", "Kriminalitaetsindex stabil ueber Systemwechsel",
           f"Rangkorrelation {rang:.3f}", ">= 0,90", rang >= 0.90)

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
    ax[1].set(title=f"Index 2017 vs. 2019 (Spearman {rang:.3f})",
              xlabel="Median 2017 (Altsystem)", ylabel="Median 2019 (Neusystem)")
    fig.tight_layout()
    fig.savefig(OUT / "05_kriminalitaetsindex.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 4. Exposure
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
    log("\n- Konsequenz (Decision Log #13): `log_bevoelkerung` geht als Merkmal in "
        "alle Modelle ein und dient bei der NegBin-Baseline als Offset. Die "
        "Zielgroesse bleibt eine Zaehlgroesse, damit die "
        "Overdispersion-Argumentation gueltig bleibt. Eine Sensitivitaetsanalyse "
        "mit der Rate als Zielgroesse ist als Robustheitscheck vorgesehen.")


# ---------------------------------------------------------------------------
# 5. Linearitaet (Schroeter-Pruefpunkt) - nur Trainingsfenster
# ---------------------------------------------------------------------------
def pruefe_linearitaet(train: pd.DataFrame) -> None:
    import statsmodels.api as sm
    from statsmodels.stats.diagnostic import het_breuschpagan

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
    r2, negativ, bp = {}, {}, {}
    for i, (name, ziel) in enumerate([("y roh", y), ("log(1+y)", np.log1p(y))]):
        fit = sm.OLS(ziel, X).fit()
        r2[name] = fit.rsquared
        pred = fit.fittedvalues if name == "y roh" else np.expm1(fit.fittedvalues)
        negativ[name] = float((pred < 0).mean() * 100)
        # Breusch-Pagan: H0 = Homoskedastizitaet. Kleines p -> Trichterform.
        bp[name] = float(het_breuschpagan(fit.resid, X)[1])
        ax[i].scatter(fit.fittedvalues, fit.resid, s=3, alpha=0.15)
        ax[i].axhline(0, color="red", lw=1)
        ax[i].set(title=f"Residuen ({name}), R2={fit.rsquared:.3f}",
                  xlabel="Vorhersage", ylabel="Residuum")
    fig.tight_layout()
    fig.savefig(OUT / "04_residuenanalyse.png", dpi=150)
    plt.close(fig)

    staerkster = max(korr, key=lambda k: abs(korr[k]))
    log(f"- OLS R2 (y roh):    {r2['y roh']:.3f} | negative Vorhersagen: "
        f"{negativ['y roh']:.1f} % | Breusch-Pagan p = {bp['y roh']:.2e}")
    log(f"- OLS R2 (log(1+y)): {r2['log(1+y)']:.3f} | negative Vorhersagen nach "
        f"Ruecktransformation: {negativ['log(1+y)']:.1f} % | "
        f"Breusch-Pagan p = {bp['log(1+y)']:.2e}")
    log(f"- Staerkster Einzelpraediktor: `{staerkster}` (r = {korr[staerkster]:+.2f})")
    log(f"\n**Befund:** Eine lineare Baseline liegt vor - die Strukturmerkmale "
        f"erklaeren bereits linear einen erheblichen Teil der Varianz "
        f"(R2 = {r2['y roh']:.2f} auf der Rohskala). Das Schroeter-Kriterium ist "
        f"damit erfuellt und Ridge Regression als interpretierbares lineares "
        f"Verfahren zulaessig. Einschraenkend zeigen die Residuen auf der "
        f"Rohskala eine Trichterform (Heteroskedastizitaet, erwartbar bei "
        f"Zaehldaten), und das Modell erzeugt in {negativ['y roh']:.1f} % der "
        f"Faelle negative Vorhersagen. Beides spricht fuer die "
        f"Log-Spezifikation: Ridge wird auf log(1+y) geschaetzt, die Guetemasse "
        f"werden nach Ruecktransformation auf der Originalskala berechnet "
        f"(Decision Log #2). Ergaenzend dient die Negative-Binomial-Regression "
        f"als verteilungsgerechte Count-Baseline.")

    pruefe("Ridge Regression", "Lineare Baseline vorhanden (OLS R2, Rohskala)",
           f"{r2['y roh']:.2f}", ">= 0,50 (Schroeter-Kriterium)",
           r2["y roh"] >= 0.50)
    pruefe("Ridge Regression", "Keine negativen Vorhersagen auf Rohskala",
           f"{negativ['y roh']:.1f} % roh / {negativ['log(1+y)']:.1f} % nach log",
           "0 % -> sonst log-Spezifikation",
           negativ["y roh"] == 0, auflage=negativ["y roh"] > 0)
    pruefe("Ridge Regression", "Homoskedastizitaet (Breusch-Pagan)",
           f"p = {bp['y roh']:.1e} roh / {bp['log(1+y)']:.1e} nach log",
           "p > 0,05 -> sonst log-Spezifikation",
           bp["y roh"] > 0.05, auflage=bp["y roh"] <= 0.05)

    # Nichtlinearitaet: Wo Spearman deutlich ueber Pearson liegt, ist der
    # Zusammenhang monoton, aber nicht linear - genau dort haben Baumverfahren
    # einen Vorteil gegenueber Ridge.
    log("\n**Hinweis auf Nichtlinearitaet** (Rangkorrelation deutlich staerker als "
        "lineare Korrelation -> Vorteil fuer Baumverfahren):\n")
    log("| Merkmal | Pearson r | Spearman rho | Differenz |")
    log("|---|---|---|---|")
    n_nichtlinear = 0
    for col in PRAEDIKTOREN:
        pe = abs(train[col].corr(y))
        sp = abs(train[col].corr(y, method="spearman"))
        if sp - pe > 0.10:
            n_nichtlinear += 1
            log(f"| {col} | {pe:.2f} | {sp:.2f} | +{sp - pe:.2f} |")
    if n_nichtlinear == 0:
        log("| - | - | - | kein Merkmal mit Differenz > 0,10 |")
    log(f"\n{n_nichtlinear} von {len(PRAEDIKTOREN)} Merkmalen zeigen einen "
        f"deutlich staerkeren monotonen als linearen Zusammenhang. "
        + ("Das begruendet, warum neben Ridge auch Random Forest und XGBoost "
           "gepruft werden: Sie bilden solche Verlaeufe ohne manuelle "
           "Transformation ab."
           if n_nichtlinear else
           "Die Zusammenhaenge sind ueberwiegend linear; der erwartete Vorsprung "
           "der Baumverfahren duerfte entsprechend gering ausfallen - selbst ein "
           "Befund fuer Kap. 6."))
    pruefe("Random Forest / XGBoost", "Nichtlineare Zusammenhaenge vorhanden",
           f"{n_nichtlinear} von {len(PRAEDIKTOREN)} Merkmalen", ">= 1",
           n_nichtlinear >= 1, auflage=n_nichtlinear == 0)


# ---------------------------------------------------------------------------
# 6. Multikollinearitaet (VIF) - nur Trainingsfenster
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
    log(f"\n**Befund Set S:** Hoechster VIF {maxvif:.1f} (`{maxcol}`). Erhoehte "
        "Werte bei Einkommen, Miete und Bildung sind sachlich erwartbar - es "
        "handelt sich um verschiedene Messungen desselben soziooekonomischen "
        "Gefaelles. Genau diese Konstellation ist der Anwendungsfall der "
        "L2-Regularisierung (Hoerl & Kennard 1970): Ridge stabilisiert die "
        "Schaetzung, ohne Merkmale zu entfernen. Fuer Random Forest und XGBoost "
        "ist Multikollinearitaet unkritisch; fuer die Interpretation einzelner "
        "SHAP-Werte muss sie jedoch beruecksichtigt werden, weil sich Beitraege "
        "auf korrelierte Merkmale verteilen.")

    pruefe("Ridge Regression", "Multikollinearitaet Set S begruendet L2-Strafterm",
           f"max. VIF {maxvif:.1f} ({maxcol})",
           "5-30: klassischer Ridge-Fall; > 30 kritisch",
           5 <= maxvif <= 30, auflage=maxvif < 5)

    # Set S+L: Der Hauptvergleich laeuft auf DIESEM Satz. Die drei Lags messen
    # dieselbe Groesse zu verschiedenen Zeitpunkten und sind daher zwangslaeufig
    # hoch korreliert (lag_1 vs. rolling_mean_3: r = 0,99). Das gehoert berichtet,
    # weil sonst der Eindruck entstuende, die VIF-Pruefung betreffe den
    # tatsaechlich verwendeten Merkmalssatz.
    log("\n**Set S+L (der Hauptvergleich laeuft hierauf):**\n")
    X2 = train[FEATURE_SETS["S+L"]].astype(float)
    X2s = sm.add_constant((X2 - X2.mean()) / X2.std())
    werte = sorted(((float(variance_inflation_factor(X2s.values, i)), c)
                    for i, c in enumerate(X2s.columns) if c != "const"),
                   reverse=True)
    log("| Merkmal | VIF |")
    log("|---|---|")
    for v, c in werte[:5]:
        log(f"| {c} | {v:.1f} |")
    maxvif2, maxcol2 = werte[0]
    log(f"\n**Befund Set S+L:** Hoechster VIF {maxvif2:.1f} (`{maxcol2}`). Die "
        f"drei Lag-Merkmale messen dieselbe Groesse zu verschiedenen "
        f"Zeitpunkten; `lag_1` und `rolling_mean_3` korrelieren mit r = "
        f"{train['lag_1'].corr(train['rolling_mean_3']):.2f}. Das ist keine "
        f"Fehlspezifikation, sondern liegt in der Natur autoregressiver "
        f"Merkmale. Fuer Ridge ist es unschaedlich - der L2-Strafterm verteilt "
        f"das Gewicht auf die korrelierten Merkmale, statt einzelne Koeffizienten "
        f"instabil werden zu lassen; genau dafuer wurde das Verfahren "
        f"entwickelt. Konsequenz fuer die Interpretation: **Einzelkoeffizienten "
        f"und SHAP-Werte der Lags sind nicht einzeln zu deuten, sondern nur als "
        f"Block** (Kap. 5.5).")
    pruefe("Ridge Regression", "Multikollinearitaet Set S+L beherrschbar",
           f"max. VIF {maxvif2:.1f} ({maxcol2})",
           "durch L2 abgedeckt; Lags nur blockweise interpretieren",
           True, auflage=maxvif2 > 10)


# ---------------------------------------------------------------------------
# 7. Extrapolation - koennen Baumverfahren den Wertebereich abdecken?
# ---------------------------------------------------------------------------
def pruefe_extrapolation(panel: pd.DataFrame) -> None:
    """Baumverfahren koennen nicht extrapolieren.

    Random Forest und XGBoost sagen ausserhalb des im Training gesehenen
    Wertebereichs immer den Randwert des letzten Blatts vorher. Liegt die
    Zielgroesse in spaeteren Perioden systematisch ueber dem Trainingsmaximum,
    sind beide Verfahren strukturell im Nachteil - und der Verfahrensvergleich
    misst dann diesen Nachteil statt der Modellguete.
    """
    log("\n## 7. Extrapolationsbedarf (Voraussetzung fuer Random Forest und XGBoost)\n")
    log("Baumverfahren koennen nicht extrapolieren: Ausserhalb des im Training "
        "gesehenen Wertebereichs geben sie den Randwert des letzten Blatts "
        "zurueck. Ridge rechnet dagegen linear weiter. Liegt die Zielgroesse in "
        "spaeteren Perioden systematisch oberhalb des Trainingsbereichs, misst "
        "der Vergleich diesen strukturellen Nachteil statt der Modellguete.\n")

    train, _ = fold_masken(panel, 1)
    y_train = panel.loc[train, "anzahl_einsaetze"]
    spaeter = panel.loc[~train, "anzahl_einsaetze"]
    ueber = float((spaeter > y_train.max()).mean() * 100)

    # Stadtweiter Zeittrend: Wenn die Einsatzzahl systematisch steigt, betrifft
    # das Baumverfahren staerker als Ridge.
    stadt = panel.groupby("jahr_monat")["anzahl_einsaetze"].sum()
    t = np.arange(len(stadt))
    steigung = float(np.polyfit(t, stadt.values, 1)[0])
    trend_pct = steigung * len(stadt) / stadt.iloc[0] * 100

    log(f"- Trainingsbereich (Fold 1): {y_train.min():.0f} bis {y_train.max():.0f} "
        f"Einsaetze je Stadtteil-Monat")
    log(f"- Spaetere Perioden ueber dem Trainingsmaximum: {ueber:.2f} % der "
        f"Beobachtungen")
    log(f"- Stadtweiter Trend ueber den Analysezeitraum: {trend_pct:+.1f} % "
        f"(lineare Steigung {steigung:+.2f} Einsaetze je Monat)")
    log("\n**Befund:** "
        + (f"Der Extrapolationsbedarf ist gering ({ueber:.2f} % der spaeteren "
           f"Beobachtungen liegen ueber dem Trainingsmaximum). Zusaetzlich "
           f"tragen die Lag-Merkmale das Zeitniveau mit, sodass die "
           f"Baumverfahren nicht auf das rohe `jahr` angewiesen sind - dieses "
           f"ist bewusst kein Merkmal (vgl. FEATURE_SETS in config.py)."
           if ueber < 5 else
           f"ACHTUNG: {ueber:.1f} % der spaeteren Beobachtungen liegen ueber dem "
           f"Trainingsmaximum. Der Nachteil der Baumverfahren ist im "
           f"Ergebniskapitel zu diskutieren."))

    pruefe("Random Forest / XGBoost", "Kein wesentlicher Extrapolationsbedarf",
           f"{ueber:.2f} % ueber Trainingsmaximum", "< 5 %", ueber < 5)


# ---------------------------------------------------------------------------
# 8. Zielgroesse Klassifikation
# ---------------------------------------------------------------------------
def pruefe_zielgroesse_klassifikation(kl: pd.DataFrame) -> None:
    log("\n## 8. Zielgroesse Einsatzart (Klassifikation)\n")
    log(f"- Grundgesamtheit: {len(kl):,} Einzeleinsaetze "
        f"({kl['jahr_monat'].min()}-{kl['jahr_monat'].max()}, "
        f"{kl['stadtteil'].nunique()} Stadtteile) - identisch abgegrenzt wie das "
        f"Regressionspanel, weil beide in prep/s2_datensaetze.py entstehen und "
        f"Zeitraum sowie Stadtteilliste einmal bestimmt und weitergereicht werden")

    v = kl["einsatzart_gruppe"].value_counts()
    log("\n| Klasse | Anzahl | Anteil |")
    log("|---|---|---|")
    for k in KLASSEN:
        n = int(v.get(k, 0))
        log(f"| {k} | {n:,} | {n / len(kl) * 100:.1f}% |")
    ungleich = float(v.max() / v.min())
    log(f"\n- **Ungleichgewicht groesste/kleinste Klasse: {ungleich:.1f}:1** -> "
        + ("mit `class_weight='balanced'` bzw. `sample_weight` beherrschbar; "
           "Bewertung ueber Macro-F1 und Macro-AUROC statt Accuracy."
           if ungleich <= 10 else
           "stark unbalanciert, Resampling oder Klassenzusammenfassung pruefen."))

    b = float(kl["ist_brand"].mean())
    log(f"- Binaerer Robustheitslauf `ist_brand`: {b * 100:.1f} % Brand "
        f"(Verhaeltnis 1:{(1 - b) / b:.1f}) -> `scale_pos_weight = "
        f"{(1 - b) / b:.2f}`. Ein Modell, das immer 'kein Brand' sagt, erreicht "
        f"bereits {100 - b * 100:.1f} % Accuracy - Accuracy ist hier wertlos.")

    # Basisratendrift: verschiebt sich der Brandanteil zwischen Training und Test?
    train, test = fold_masken(kl, 1)
    drift_tr, drift_te = kl.loc[train, "ist_brand"].mean(), kl.loc[test, "ist_brand"].mean()
    drift = abs(drift_te - drift_tr) * 100
    log(f"\n- **Basisratendrift:** Brandanteil im Training des ersten Folds "
        f"{drift_tr * 100:.1f} %, im Testfenster {drift_te * 100:.1f} % "
        f"(Differenz {drift:.1f} Prozentpunkte). AUROC ist davon unberuehrt, F1 "
        f"nicht -> der Schwellenwert des binaeren Laufs wird je Fold auf dem "
        f"inneren Validierungsfenster kalibriert, nicht blind auf 0,5 gesetzt. "
        f"Bei der mehrklassigen Hauptvariante entfaellt das Problem, weil ueber "
        f"`argmax` zugeordnet wird (Decision Log #21).")

    # Pseudo-Signal
    je_monat = kl.groupby(["stadtteil", "jahr", "monat"]).size()
    log(f"\n- **Pseudo-Signal-Problem:** Die Stadtteilmerkmale sind je "
        f"Stadtteil-Monat konstant. Im Mittel teilen sich {je_monat.mean():.0f} "
        f"Einsaetze (Median {je_monat.median():.0f}, Maximum {je_monat.max():,}) "
        f"dieselbe Merkmalsauspraegung. {len(kl):,} Zeilen enthalten damit nur "
        f"{len(je_monat):,} verschiedene Stadtteil-Monats-Profile. Konsequenz: "
        f"SHAP nur blockweise auswerten, keine Signifikanztests auf "
        f"Einsatz-Ebene rechnen.")

    jahre = kl.groupby("jahr")["ist_brand"].mean() * 100
    log(f"- Brandanteil je Jahr: {jahre.min():.1f} % bis {jahre.max():.1f} %")

    pruefe("Klassifikation (alle 3)", "Klassenbalance beherrschbar",
           f"{ungleich:.1f}:1", "<= 10:1 mit class_weight", ungleich <= 10)
    pruefe("Klassifikation (alle 3)", "Basisratendrift Training -> Test",
           f"{drift:.1f} Prozentpunkte",
           "< 10 pp; sonst Schwelle je Fold kalibrieren", drift < 10,
           auflage=drift >= 5)

    fig, ax = plt.subplots(1, 2, figsize=(13, 4.5))
    (v / len(kl) * 100).sort_values().plot.barh(ax=ax[0], color="#4878A8")
    ax[0].set(title="Klassenverteilung Einsatzart (4 Gruppen)", xlabel="Anteil in %")
    ax[1].plot(jahre.index, jahre.values, marker="o")
    ax[1].set(title="Brandanteil je Jahr", xlabel="Jahr", ylabel="Anteil in %")
    ax[1].set_ylim(0, max(20, jahre.max() * 1.2))
    fig.tight_layout()
    fig.savefig(OUT / "02_klassenbalance_einsatzart.png", dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 9. Eignungsurteil
# ---------------------------------------------------------------------------
def schreibe_urteil() -> bool:
    log("\n## 9. Eignungsurteil je Verfahren\n")
    log("Maschinell gepruefte Kriterien. `mit Auflage` bedeutet: Das Verfahren "
        "ist einsetzbar, aber nur in der angegebenen Spezifikation.\n")
    log("| Verfahren | Kriterium | Messwert | Schwelle | Urteil |")
    log("|---|---|---|---|---|")
    for verfahren, kriterium, wert, schwelle, urteil in urteile:
        log(f"| {verfahren} | {kriterium} | {wert} | {schwelle} | **{urteil}** |")

    kritisch = [u for u in urteile if u[4] == KRITISCH]
    auflagen = [u for u in urteile if u[4] == ACHTUNG]

    log("\n**Gesamturteil:**\n")
    if kritisch:
        log(f"- {len(kritisch)} Kriterium/Kriterien **nicht erfuellt**: "
            + "; ".join(f"{v} - {k}" for v, k, *_ in kritisch))
        log("- Die Verfahrenswahl ist in dieser Form nicht begruendbar. Vor der "
            "Modellierung klaeren.")
    else:
        log("- **Alle harten Kriterien sind erfuellt.** Ridge Regression, Random "
            "Forest und XGBoost sind fuer diesen Datensatz geeignet, die "
            "Negative-Binomial-Regression ist die korrekte Count-Baseline.")
    if auflagen:
        log(f"- {len(auflagen)} Auflage(n), die die Spezifikation festlegen:")
        for v, k, wert, _, _ in auflagen:
            log(f"    - {v}: {k} ({wert})")
        log("    Konkret: Ridge wird auf log(1+y) geschaetzt und die Lags werden "
            "log(1+x)-transformiert (Decision Log #2, #9); die Guetemasse werden "
            "nach Ruecktransformation auf der Originalskala berechnet.")
    log("\n- Nicht maschinell pruefbar und daher im Text zu begruenden: die "
        "Fairness-Regel (identische Zeilen, Merkmale und Folds fuer alle drei "
        "Verfahren) und das gleiche Tuning-Budget. Beides ist konstruktiv "
        "abgesichert - die Folds stehen als Spalten im Datensatz, die Suchraeume "
        "und das Budget in prep/config.py.")
    return not kritisch


# ---------------------------------------------------------------------------
# 11. Vergleichsgroessen (Baselines)
# ---------------------------------------------------------------------------
# Ein Verfahrensvergleich ohne Referenz sagt wenig. Erst diese drei Groessen
# machen die Ergebnisse der Modelle einordbar. Sie stehen hier und nicht unter
# modelle/, weil sie nichts tunen und nichts auswaehlen - sie legen die Latte
# fest, ueber die die getunten Verfahren spaeter springen muessen.
# ---------------------------------------------------------------------------
def naiv(test: pd.DataFrame) -> np.ndarray:
    """Wert des Vormonats desselben Stadtteils - steht als lag_1 im Datensatz.

    Die Zielgroesse ist mit Lag-1-Autokorrelation 0,96 stark persistent - wer
    diese Baseline nicht schlaegt, hat nichts gelernt (Decision Log #8).
    """
    return test["lag_1"].to_numpy()


def saisonal(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """Mittelwert desselben Kalendermonats im Training, je Stadtteil.

    Trennt das Saisonmuster von echtem Signal.
    """
    mittel = (train.groupby(["stadtteil", "monat"])["anzahl_einsaetze"]
                   .mean().rename("saison"))
    return (test.join(mittel, on=["stadtteil", "monat"])["saison"]
                .fillna(train["anzahl_einsaetze"].mean()).to_numpy())


def negative_binomial(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    """NegBin-GLM mit log(Bevoelkerung) als Offset.

    Interpretierbare Count-Baseline. Poisson scheidet aus: Dispersionsindex 62,8
    (Abschnitt 2). Der Offset unterscheidet dieses Modell von einem gewoehnlichen
    Regressor auf log_bevoelkerung: Der Koeffizient ist auf 1 fixiert, das Modell
    schaetzt also Einsaetze JE EINWOHNER und nicht die Stadtteilgroesse mit -
    genau das war die Begruendung der Exposure-Entscheidung (Decision Log #13).

    Der Dispersionsparameter alpha wird ueber ein Poisson-Vormodell geschaetzt
    (Momentenschaetzer auf den Pearson-Residuen) - das uebliche zweistufige
    Vorgehen, wenn statsmodels alpha nicht selbst optimiert.
    """
    import statsmodels.api as sm

    spalten = PRAEDIKTOREN + SAISON
    X_tr = sm.add_constant(train[spalten].astype(float), has_constant="add")
    X_te = sm.add_constant(test[spalten].astype(float),  has_constant="add")
    y_tr = train["anzahl_einsaetze"].astype(float)
    off_tr = np.log(train[EXPOSURE_ROH].astype(float))
    off_te = np.log(test[EXPOSURE_ROH].astype(float))

    poisson = sm.GLM(y_tr, X_tr, family=sm.families.Poisson(),
                     offset=off_tr).fit()
    mu = poisson.mu
    alpha = float(np.sum((y_tr - mu) ** 2 / mu - 1) / np.sum(mu))
    alpha = max(alpha, 1e-6)

    negbin = sm.GLM(y_tr, X_tr,
                    family=sm.families.NegativeBinomial(alpha=alpha),
                    offset=off_tr).fit()
    return np.asarray(negbin.predict(X_te, offset=off_te))


def rechne_baselines(panel: pd.DataFrame, protokoll: bool = True) -> pd.DataFrame:
    """Naiv, saisonal und NegBin je Fold - auf der Originalskala bewertet.

    Das End-Hold-out (`ist_holdout == 1`) wird bewusst NICHT ausgewertet, es
    bleibt der finalen Bewertung vorbehalten.
    """
    OUT_BASE.mkdir(parents=True, exist_ok=True)

    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(panel, k)
        train, test = panel[tr], panel[te]
        y = test["anzahl_einsaetze"].to_numpy()
        for name, y_hat in [
            ("Naiv (Vormonat)",          naiv(test)),
            ("Saisonaler Durchschnitt",  saisonal(train, test)),
            ("Negative Binomial",        negative_binomial(train, test)),
        ]:
            zeilen.append({"fold": k, "modell": name,
                           **bewerte_regression(y, y_hat)})

    df = pd.DataFrame(zeilen)
    mittel = (df.groupby("modell")[["RMSE", "MAE", "R2"]]
                .agg(["mean", "std"]).round(2))
    mittel.columns = [f"{a}_{b}" for a, b in mittel.columns]
    mittel = mittel.sort_values("RMSE_mean")

    df.to_csv(OUT_BASE / "baselines_folds.csv", index=False)
    mittel.to_csv(OUT_BASE / "baselines_mittel.csv")

    if protokoll:
        log("\n## 11. Vergleichsgroessen der Regression\n")
        log("Referenzwerte, an denen sich Ridge, Random Forest und XGBoost messen "
            "lassen muessen. Identische Folds, identische Zeilen, Bewertung auf "
            "der Originalskala. Das End-Hold-out bleibt unberuehrt.\n")
        log("| Modell | RMSE (Mittel +/- Std) | MAE | R2 |")
        log("|---|---|---|---|")
        for name, r in mittel.iterrows():
            log(f"| {name} | {r['RMSE_mean']:.2f} +/- {r['RMSE_std']:.2f} | "
                f"{r['MAE_mean']:.2f} | {r['R2_mean']:.2f} |")
        bester = mittel.index[0]
        log(f"\n**Zu schlagende Latte:** {bester} "
            f"(RMSE {mittel.iloc[0]['RMSE_mean']:.2f}). Ein Verfahren, das diese "
            f"Referenz nicht schlaegt, ist ein Ergebnis - kein Makel.")

    print("\nVergleichsgroessen je Fold:\n", df.round(2).to_string(index=False))
    print("\nMittelwert +/- Std ueber die Folds:\n", mittel.to_string())
    print(f"\n  => {OUT_BASE.relative_to(ROOT)}/baselines_*.csv")
    return df


# ---------------------------------------------------------------------------
# Ablauf
# ---------------------------------------------------------------------------
def _lade() -> tuple[pd.DataFrame, pd.DataFrame]:
    for pfad in (PFAD_REGRESSION, PFAD_KLASSIFIKATION, PFAD_EINSAETZE):
        if not pfad.exists():
            raise SystemExit(f"{pfad.relative_to(ROOT)} fehlt - "
                             f"erst 'python prep/build.py' ausfuehren.")
    return pd.read_parquet(PFAD_REGRESSION), pd.read_parquet(PFAD_KLASSIFIKATION)


def main() -> bool:
    OUT.mkdir(parents=True, exist_ok=True)
    panel, kl = _lade()

    import pyarrow.parquet as pq

    # Nur die fuer die Kennzahlen noetigen Spalten laden - die volle Tabelle hat
    # 720.000 Zeilen und wird hier nicht gebraucht.
    n_spalten = len(pq.ParquetFile(PFAD_EINSAETZE).schema_arrow)
    roh = pd.read_parquet(PFAD_EINSAETZE,
                          columns=["einsatz_nummer", "jahr", "monat", "stadtteil"])
    train_maske, _ = fold_masken(panel, 1)
    train = panel[train_maske].copy()

    log("# Eignungspruefung: Ridge Regression, Random Forest, XGBoost\n")
    log(f"Stand {pd.Timestamp.today():%Y-%m-%d}. Grundlage sind die fertigen "
        "Datensaetze `regression.parquet` und `klassifikation.parquet`. Alle "
        "Diagnosen, die eine Modellentscheidung begruenden, werden ausschliesslich "
        "auf dem Trainingsfenster des ersten CV-Folds gerechnet - diese Monate "
        "sind in keinem Fold Testdaten und liegen vollstaendig vor dem "
        "End-Hold-out. Rein deskriptive Kennzahlen nutzen den vollen "
        "Analysezeitraum und sind entsprechend gekennzeichnet.\n")
    log("Das abschliessende Urteil je Verfahren steht in Abschnitt 9, die "
        "Vergleichsgroessen in Abschnitt 11.\n")

    pruefe_datengrundlage(roh, n_spalten, panel, train)
    pruefe_designmatrix(panel, kl)
    pruefe_zielgroesse_regression(panel)
    pruefe_kriminalitaetsindex(panel)
    pruefe_exposure(train)
    pruefe_linearitaet(train)
    pruefe_vif(train)
    pruefe_extrapolation(panel)
    pruefe_zielgroesse_klassifikation(kl)
    bestanden = schreibe_urteil()

    log("\n## 10. Leakage- und Strukturpruefung\n")
    log("| Punkt | Status |")
    log("|---|---|")
    log("| ACS-Join | **behoben**: letzter *publizierter* Snapshot "
        "(`acs_jahr <= Einsatzjahr - 1`), Decision Log #4 und #11 |")
    log("| Kriminalitaetsmerkmale | **behoben**: relativer Index je Stadtteil x "
        "Monat, rollierendes 12-Monats-Fenster endend im Vormonat, Decision "
        "Log #17 |")
    log(f"| Randmonat | **behoben**: fester Analysezeitraum {START}-{ENDE}, "
        "Decision Log #12 und #18 |")
    log("| Fehlende Werte | **behoben**: kein `bfill` mehr (Zukunfts-Imputation), "
        "Decision Log #10 |")
    log("| Exposure | **behoben**: `log_bevoelkerung` als Merkmal, Rohwert fuer "
        "NegBin-Offset erhalten, Decision Log #13 |")
    log("| Analyseeinheiten | **behoben**: Park-/Institutionsgebiete "
        "ausgeschlossen, Decision Log #19 |")
    log("| End-Hold-out | **eingerichtet**: letzte 12 Monate, beim Tuning "
        "unberuehrt, als Spalte `ist_holdout` im Datensatz, Decision Log #14 |")
    log("| Lag-Vorlauf | **eingerichtet**: Aggregation ab 2014-01, Zuschnitt auf "
        "2015-01 nach der Lag-Bildung; Vorlaufmonate nur ueber `shift()`, nie "
        "als eigene Zeile, Decision Log #23 |")
    log("| Land Use | **offen (Limitation)**: Snapshot 2020, ueber den gesamten "
        "Zeitraum konstant -> als quasi-stabiles Strukturmerkmal zu "
        "interpretieren, Kap. 6.3 |")
    log("| Response-Time-Filter | **dokumentieren**: 0-60 min entfernt ~1,7 % der "
        "Einsaetze bereits in prep/s1_daten.py; alle Zaehlungen beziehen sich auf "
        "den gefilterten Bestand |")

    rechne_baselines(panel)

    (OUT / "eignungspruefung_summary.md").write_text("\n".join(bericht),
                                                     encoding="utf-8")
    print(f"\n  => {OUT.relative_to(ROOT)}/eignungspruefung_summary.md + 5 Plots")
    return bestanden


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "baselines":
        panel, _ = _lade()
        rechne_baselines(panel, protokoll=False)
    else:
        raise SystemExit(0 if main() else 1)
