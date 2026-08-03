"""
Schritt 1 der Modellierung: Passen die Verfahren ueberhaupt zu diesem Datensatz?

Dieses Skript erzeugt KEINE Daten. Es liest die beiden fertigen Datensaetze und
prueft die Voraussetzungen von Ridge, Random Forest und XGBoost, bevor
verglichen wird. Damit ist die Verfahrenswahl belegt statt behauptet - und nicht
umgekehrt der Datensatz nachtraeglich zur Verfahrenswahl passend erklaert.

  ABSCHNITT 1  Zielgroessen: Verteilung und Overdispersion
  ABSCHNITT 2  Linearitaet und Residuen          <- Auflage Schroeter (R7)
  ABSCHNITT 3  Formaler Spezifikationstest       <- RESET, Interaktionen
  ABSCHNITT 4  Multikollinearitaet (VIF)
  ABSCHNITT 5  Extrapolation je Fold
  ABSCHNITT 6  Klassenbalance der Einsatzart
  ABSCHNITT 7  Urteil je Verfahren

METHODISCHE REGEL: Jede Diagnose, die eine MODELLENTSCHEIDUNG begruendet, wird
ausschliesslich auf den TRAININGSSTADTTEILEN DES ERSTEN FOLDS gerechnet. Diese
Stadtteile sind in Fold 1 nie Testfall und liegen nie im Hold-out. Rein
deskriptive Kennzahlen duerfen den vollen Datensatz nutzen und sind als
"(deskriptiv)" gekennzeichnet.

Eingang:  data/processed/{regression,klassifikation}.parquet
Ausgang:  results/eignungspruefung/eignungspruefung.md  + 4 Abbildungen

Ausfuehren:
  python modelle/m01_eignung.py
"""
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt   # noqa: E402
import numpy as np                # noqa: E402
import pandas as pd               # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep"))

from config import (N_FOLDS, PFAD_KLASSIFIKATION, PFAD_REGRESSION,  # noqa: E402
                    PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from s2_datensaetze import (RATE, ZIELGROESSE, ZIELKLASSE,  # noqa: E402
                            fold_masken)

OUT = RESULTS_DIR / "eignungspruefung"
MERKMALE = PRAEDIKTOREN + SAISON

bericht: list[str] = []
# (Verfahren, Kriterium, Messwert, Schwelle, Urteil) - Grundlage fuer Abschnitt 7
urteile: list[tuple[str, str, str, str, str]] = []


def log(txt: str = "") -> None:
    """Eine Zeile gleichzeitig auf die Konsole und in den Bericht schreiben."""
    print(txt)
    bericht.append(txt)


def pruefe(verfahren: str, kriterium: str, wert: str, schwelle: str,
           bestanden: bool) -> None:
    """Ein Einzelurteil festhalten, das spaeter in die Tabelle wandert."""
    urteile.append((verfahren, kriterium, wert, schwelle,
                    "erfuellt" if bestanden else "VERLETZT"))


def speichere(fig, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUT / name, dpi=140)
    plt.close(fig)
    log(f"    -> {name}")


# ---------------------------------------------------------------------------
# ABSCHNITT 1  Zielgroessen
# ---------------------------------------------------------------------------
def zielgroessen(panel: pd.DataFrame, train: pd.DataFrame) -> None:
    """Verteilung beider Mengen-Zielgroessen und der Dispersionsindex."""
    log("\n## 1  Zielgroessen\n")

    y = train[ZIELGROESSE].astype(float)
    dispersion = y.var() / y.mean()

    log(f"`{ZIELGROESSE}` auf den Trainingsstadtteilen von Fold 1:")
    log(f"  Mittel {y.mean():.1f} | Median {y.median():.0f} | Max {y.max():.0f}")
    log(f"  Schiefe {y.skew():.2f} | Nullanteil {(y == 0).mean() * 100:.2f} %")
    log(f"  **Dispersionsindex Var/Mean = {dispersion:.1f}**")
    log("")
    log("Bei einer Poisson-Verteilung waere Var/Mean = 1. Der Wert liegt weit")
    log("darueber - die Zaehldaten sind ueberdispers, Poisson scheidet als")
    log("Verteilungsannahme aus, und die Negative Binomial ist die richtige")
    log("Count-Baseline (Decision Log #32).")
    pruefe("Negative Binomial", "Overdispersion vorhanden",
           f"Var/Mean = {dispersion:.1f}", "> 1", dispersion > 1)

    log("")
    log(f"`{RATE}` (deskriptiv, voller Datensatz):")
    log(f"  Mittel {panel[RATE].mean():.2f} | Median {panel[RATE].median():.2f} "
        f"| Max {panel[RATE].max():.2f}")
    st = panel.groupby("stadtteil")[RATE].mean()
    log(f"  Stadtteil-Mittelwerte: {st.min():.2f} ({st.idxmin()}) bis "
        f"{st.max():.2f} ({st.idxmax()}) - Faktor {st.max() / st.min():.0f}")
    log("")
    log("Diese Spreizung ist der Grund, warum R2 auf der Rate kein tragfaehiges")
    log("Hauptmass ist: R2 misst gegen den Mittelwert der TESTdaten, und der")
    log("liegt je nach Fold-Zusammensetzung weit vom Trainingsmittelwert weg.")
    log("Bei der Rate ist RMSE zu berichten, R2 nur nachrichtlich.")

    fig, ax = plt.subplots(2, 2, figsize=(11, 7))
    for j, (spalte, titel) in enumerate([(ZIELGROESSE, "Anzahl Einsaetze"),
                                         (RATE, "Einsaetze je 1.000 Ew.")]):
        ax[j, 0].hist(train[spalte], bins=50, color="steelblue")
        ax[j, 0].set_title(f"{titel} - Rohskala")
        ax[j, 1].hist(np.log1p(train[spalte]), bins=50, color="darkseagreen")
        ax[j, 1].set_title(f"{titel} - log(1+y)")
    speichere(fig, "01_zielgroessen.png")


# ---------------------------------------------------------------------------
# ABSCHNITT 2  Linearitaet - die harte Auflage (R7)
# ---------------------------------------------------------------------------
def linearitaet(train: pd.DataFrame) -> None:
    """Streudiagramme und Residuen, ausschliesslich auf Trainingsstadtteilen.

    Schroeter woertlich: "erstmal plotten, falls keine lineare Baseline, KEIN
    lineares Regressionsmodell." Geprueft wird zweierlei - ob die Zusammenhaenge
    der EINZELNEN Merkmale linear sind (Pearson gegen Spearman), und ob die
    Residuen der Gesamtregression strukturlos sind.
    """
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    log("\n## 2  Linearitaet und Residuen (Auflage Schroeter, R7)\n")
    log("Gerechnet auf den Trainingsstadtteilen von Fold 1.\n")

    log("| Merkmal | Pearson | Spearman | Abstand |")
    log("|---|---|---|---|")
    max_abstand, schlimmstes = 0.0, ""
    for m in PRAEDIKTOREN:
        p = train[m].corr(train[ZIELGROESSE])
        s = train[m].corr(train[ZIELGROESSE], method="spearman")
        if abs(p - s) > max_abstand:
            max_abstand, schlimmstes = abs(p - s), m
        log(f"| `{m}` | {p:+.3f} | {s:+.3f} | {abs(p - s):.3f} |")

    log("")
    log(f"Groesster Abstand: {max_abstand:.3f} bei `{schlimmstes}`.")
    log("Ein grosser Abstand zwischen Pearson und Spearman zeigt einen")
    log("monotonen, aber gekruemmten Zusammenhang. Bleiben alle Abstaende klein,")
    log("sind die EINZELNEN Effekte praktisch linear - eine etwaige")
    log("Fehlspezifikation liegt dann nicht an der Kruemmung.")
    pruefe("Ridge", "Einzeleffekte linear (Pearson vs Spearman)",
           f"max {max_abstand:.3f}", "< 0,05", max_abstand < 0.05)

    fig, achsen = plt.subplots(2, 5, figsize=(16, 6.5))
    for ax, m in zip(achsen.ravel(), PRAEDIKTOREN):
        ax.scatter(train[m], np.log1p(train[ZIELGROESSE]), s=4, alpha=0.25)
        ax.set_xlabel(m, fontsize=8)
        ax.set_ylabel("log(1+y)", fontsize=8)
        ax.tick_params(labelsize=7)
    fig.suptitle("Strukturmerkmale gegen log(1 + Anzahl Einsaetze), "
                 "Trainingsstadtteile Fold 1")
    speichere(fig, "02_linearitaet.png")

    X = train[MERKMALE].astype(float)
    guete = {}
    fig, achsen = plt.subplots(1, 2, figsize=(12, 4.5))
    for ax, (name, y) in zip(achsen, [
            ("Rohskala", train[ZIELGROESSE].astype(float)),
            ("log(1+y)", np.log1p(train[ZIELGROESSE].astype(float)))]):
        modell = make_pipeline(StandardScaler(), Ridge(alpha=1.0)).fit(X, y)
        residuen = y - modell.predict(X)
        guete[name] = modell.score(X, y)
        ax.scatter(modell.predict(X), residuen, s=4, alpha=0.25)
        ax.axhline(0, color="black", lw=0.8)
        ax.set_title(f"Residuen, Ridge auf {name}  "
                     f"(R2 im Training {guete[name]:.3f})")
        ax.set_xlabel("Vorhersage")
        ax.set_ylabel("Residuum")
    speichere(fig, "03_residuen.png")

    log("")
    log(f"Ridge im Training: Rohskala R2 {guete['Rohskala']:.3f}, "
        f"log(1+y) R2 {guete['log(1+y)']:.3f}.")
    log("Massgeblich ist das Residuenbild: Ein Trichter auf der Rohskala zeigt")
    log("Heteroskedastizitaet - die Streuung waechst mit dem Niveau, was bei")
    log("Zaehldaten zu erwarten ist. Die log-Spezifikation macht das Modell")
    log("multiplikativ und begrenzt den Schaden. **Ridge wird deshalb auf")
    log("log(1+y) geschaetzt**, Guetemasse nach expm1-Ruecktransformation.")
    pruefe("Ridge", "log-Transformation verbessert die Anpassung",
           f"R2 {guete['Rohskala']:.3f} -> {guete['log(1+y)']:.3f}",
           "log besser", guete["log(1+y)"] > guete["Rohskala"])


# ---------------------------------------------------------------------------
# ABSCHNITT 3  Formaler Spezifikationstest
# ---------------------------------------------------------------------------
def spezifikation(train: pd.DataFrame) -> None:
    """RESET-Test nach Ramsey (1969) plus Interaktionsterme.

    Der RESET-Test prueft die Nullhypothese, dass die lineare Spezifikation
    adaequat ist. Wird sie verworfen, ist der Schritt zu flexibleren Verfahren
    methodisch begruendet und nicht bloss behauptet. Die Interaktionsterme
    zeigen anschliessend, WORAN es liegt - das ist der Unterschied zwischen
    "es ist nichtlinear" und einer belastbaren Aussage.
    """
    import statsmodels.api as sm
    from statsmodels.stats.diagnostic import linear_reset

    log("\n## 3  Formaler Spezifikationstest\n")

    y = np.log1p(train[ZIELGROESSE].astype(float))
    X = sm.add_constant(train[MERKMALE].astype(float), has_constant="add")
    ols = sm.OLS(y, X).fit()

    log("| Test | F | p | Urteil |")
    log("|---|---|---|---|")
    verworfen = False
    for potenz in (2, 3):
        r = linear_reset(ols, power=potenz, test_type="fitted", use_f=True)
        verworfen |= r.pvalue < 0.05
        log(f"| RESET, Potenzen bis {potenz} | {r.fvalue:.2f} | {r.pvalue:.2e} | "
            f"{'**H0 verworfen**' if r.pvalue < 0.05 else 'H0 nicht verworfen'} |")

    log("")
    log("H0: Die lineare Spezifikation ist adaequat.")
    pruefe("Ridge", "lineare Spezifikation ausreichend (RESET)",
           "H0 verworfen" if verworfen else "H0 gehalten", "p >= 0,05",
           not verworfen)

    inter = train[MERKMALE].astype(float).copy()
    for i, a in enumerate(PRAEDIKTOREN):
        for b in PRAEDIKTOREN[i + 1:]:
            inter[f"{a}_x_{b}"] = train[a].astype(float) * train[b].astype(float)
    ols_inter = sm.OLS(y, sm.add_constant(inter, has_constant="add")).fit()
    zusatz = len(inter.columns) - len(MERKMALE)

    log("")
    log(f"Adjustiertes R2 ohne Interaktionen: {ols.rsquared_adj:.3f}")
    log(f"Adjustiertes R2 mit  Interaktionen: {ols_inter.rsquared_adj:.3f} "
        f"({zusatz} zusaetzliche Terme)")
    log("")
    log("Steigt das adjustierte R2 durch Interaktionen deutlich, waehrend die")
    log("Einzeleffekte laut Abschnitt 2 linear sind, liegt die Fehlspezifikation")
    log("an WECHSELWIRKUNGEN zwischen Merkmalen. Ein lineares Modell bildet die")
    log(f"nur ab, wenn man sie von Hand angibt - hier waeren es {zusatz} Terme,")
    log("deren Auswahl willkuerlich waere und die bei 29 Trainingsstadtteilen")
    log("ueberanpassen. Genau diese Luecke schliessen Baumverfahren")
    log("konstruktionsbedingt: Jeder Split bedingt auf die vorherigen.")
    log("")
    log("**Das ist die Begruendungskette fuer Random Forest und XGBoost.** Ob")
    log("sich der theoretische Vorteil in Prognosegute uebersetzt, ist die")
    log("empirische Frage der Arbeit und wird in m02/m03 beantwortet.")


# ---------------------------------------------------------------------------
# ABSCHNITT 4  Multikollinearitaet
# ---------------------------------------------------------------------------
def vif(train: pd.DataFrame) -> None:
    """VIF auf den EINDEUTIGEN Stadtteil-Merkmalskombinationen.

    Die Strukturmerkmale sind innerhalb eines Jahres konstant; ueber alle Zeilen
    gerechnet zaehlt jede Kombination bis zu zwoelfmal, und der VIF waere
    kuenstlich stabilisiert. Massgeblich sind die tatsaechlich verschiedenen
    Merkmalsprofile.
    """
    import statsmodels.api as sm
    from statsmodels.stats.outliers_influence import variance_inflation_factor

    log("\n## 4  Multikollinearitaet (VIF)\n")

    eindeutig = train[PRAEDIKTOREN].astype(float).drop_duplicates()
    X = sm.add_constant(eindeutig, has_constant="add")
    werte = {PRAEDIKTOREN[i - 1]: variance_inflation_factor(X.values, i)
             for i in range(1, X.shape[1])}

    log(f"{len(eindeutig):,} eindeutige Merkmalskombinationen "
        f"(aus {len(train):,} Zeilen).\n")
    log("| Merkmal | VIF |")
    log("|---|---|")
    for m, v in sorted(werte.items(), key=lambda x: -x[1]):
        log(f"| `{m}` | {v:.1f} |")

    hoechster = max(werte.values())
    log("")
    log("Faustregel: VIF > 10 gilt als kritisch. Ridge ist gegen")
    log("Multikollinearitaet durch den L2-Strafterm robust - der VIF ist hier")
    log("vor allem fuer die INTERPRETATION relevant: Bei hohen Werten verteilen")
    log("sich Koeffizienten und SHAP-Beitraege auf korrelierte Merkmale und sind")
    log("einzeln nicht mehr sinnvoll deutbar (blockweise interpretieren).")
    pruefe("Ridge", "Multikollinearitaet beherrschbar",
           f"max VIF {hoechster:.1f}", "< 10", hoechster < 10)


# ---------------------------------------------------------------------------
# ABSCHNITT 5  Extrapolation
# ---------------------------------------------------------------------------
def extrapolation(panel: pd.DataFrame) -> None:
    """Wie viele Testzeilen liegen ausserhalb des Trainings-Wertebereichs?

    Unter einem Stadtteil-Split ist das keine Randerscheinung: Ein unbekannter
    Stadtteil kann in jedem Merkmal ausserhalb liegen. Ridge rechnet dort linear
    weiter, Baumverfahren ordnen dem letzten Blatt zu - die Verfahren sind also
    unterschiedlich betroffen, und das gehoert in die Limitationen.
    """
    log("\n## 5  Extrapolation je Fold\n")
    log("| Fold | Trainingszeilen | Testzeilen | ausserhalb des Wertebereichs |")
    log("|---|---|---|---|")

    anteile = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(panel, k)
        lo, hi = panel.loc[tr, MERKMALE].min(), panel.loc[tr, MERKMALE].max()
        aussen = ((panel.loc[te, MERKMALE] < lo)
                  | (panel.loc[te, MERKMALE] > hi)).any(axis=1)
        anteile.append(aussen.mean())
        log(f"| {k} | {tr.sum():,} | {te.sum():,} | {aussen.mean() * 100:.1f} % |")

    log("")
    log(f"Im Mittel {np.mean(anteile) * 100:.1f} %, Spanne "
        f"{min(anteile) * 100:.1f} bis {max(anteile) * 100:.1f} %.")
    log("Diese Spanne erklaert einen erheblichen Teil der Fold-Streuung und ist")
    log("der Grund fuer die wiederholten Splits mit unterschiedlichem Versatz.")
    for verfahren in ("Ridge", "Random Forest / XGBoost"):
        pruefe(verfahren, "Extrapolationsanteil begrenzt",
               f"{np.mean(anteile) * 100:.1f} % im Mittel", "< 20 %",
               np.mean(anteile) < 0.20)


# ---------------------------------------------------------------------------
# ABSCHNITT 6  Klassenbalance
# ---------------------------------------------------------------------------
def klassenbalance(kl: pd.DataFrame) -> None:
    """Traegt die Zielgroesse der Klassifikation vier Klassen?"""
    log("\n## 6  Klassenbalance der Einsatzart\n")

    v = kl[ZIELKLASSE].value_counts()
    anteil = kl[ZIELKLASSE].value_counts(normalize=True)
    log("| Klasse | Stadtteil-Monate | Anteil |")
    log("|---|---|---|")
    for klasse in v.index:
        log(f"| {klasse} | {v[klasse]:,} | {anteil[klasse] * 100:.1f} % |")

    seltenste = anteil.idxmin()
    n_stadtteile = kl[kl[ZIELKLASSE] == seltenste]["stadtteil"].nunique()
    log("")
    log(f"Seltenste Klasse: **{seltenste}** mit {anteil.min() * 100:.1f} % "
        f"({v[seltenste]} Stadtteil-Monate in {n_stadtteile} Stadtteilen).")
    log("")
    log("| Fold | Testfaelle der seltensten Klasse |")
    log("|---|---|")
    je_fold = []
    for k in range(1, N_FOLDS + 1):
        n = int((kl.loc[kl["fold"] == k, ZIELKLASSE] == seltenste).sum())
        je_fold.append(n)
        log(f"| {k} | {n} |")

    log("")
    log("Kein Fold darf null Testfaelle der seltensten Klasse haben - Macro-F1")
    log("mittelt sonst ueber eine Klasse, die im Test gar nicht vorkommt. Genau")
    log("dafuer wird die Fold-Zuteilung doppelt stratifiziert (Decision Log #30).")
    log("")
    log(f"Die Mehrheitsklasse allein erreicht Accuracy {anteil.max():.3f} - ")
    log("**Accuracy ist als Hauptmass wertlos**, massgeblich ist Macro-F1.")
    pruefe("Random Forest / XGBoost", "seltenste Klasse in jedem Fold vertreten",
           f"min {min(je_fold)} Testfaelle", "> 0", min(je_fold) > 0)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(anteil.index, anteil.values * 100, color="steelblue")
    ax.set_ylabel("Anteil der Stadtteil-Monate in %")
    ax.set_title("Verteilung der dominanten Einsatzart")
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")
    speichere(fig, "04_klassenbalance.png")


# ---------------------------------------------------------------------------
# ABSCHNITT 7  Urteil
# ---------------------------------------------------------------------------
def urteil() -> None:
    """Alle Einzelbefunde als Tabelle, plus die Konsequenzen fuer m02/m03."""
    log("\n## 7  Urteil je Verfahren\n")
    log("| Verfahren | Kriterium | Messwert | Schwelle | Urteil |")
    log("|---|---|---|---|---|")
    for zeile in urteile:
        log("| " + " | ".join(zeile) + " |")

    verletzt = [u for u in urteile if u[4] == "VERLETZT"]
    log("")
    if verletzt:
        log(f"**{len(verletzt)} Kriterium/Kriterien verletzt.** Eine Verletzung")
        log("ist kein Ausschluss, sondern eine Begruendungspflicht: Sie muss in")
        log("Kapitel 6.2 benannt und beantwortet werden. Betroffen sind:")
        for v in verletzt:
            log(f"  - {v[0]}: {v[1]} ({v[2]}, erwartet {v[3]})")
    else:
        log("Alle geprueften Kriterien erfuellt.")

    log("")
    log("**Konsequenzen fuer die Modellierung:**")
    log("- Ridge wird auf `log(1+y)` geschaetzt, nicht auf der Rohskala.")
    log("- Der RESET-Test begruendet den Schritt zu Baumverfahren formal.")
    log("- Der Extrapolationsanteil gehoert in die Limitationen (Kap. 8.3).")
    log("- Auf der Rate ist RMSE das Hauptmass, R2 nur nachrichtlich.")
    log("- SHAP nur blockweise interpretieren (Multikollinearitaet).")


# ---------------------------------------------------------------------------
def main() -> None:
    for pfad in (PFAD_REGRESSION, PFAD_KLASSIFIKATION):
        if not pfad.exists():
            raise SystemExit(f"{pfad.relative_to(ROOT)} fehlt - "
                             f"erst 'python prep/build.py' ausfuehren.")

    panel = pd.read_parquet(PFAD_REGRESSION)
    kl = pd.read_parquet(PFAD_KLASSIFIKATION)
    train = panel[fold_masken(panel, 1)[0]]

    log("# Eignungspruefung")
    log("")
    log(f"Stand {pd.Timestamp.today():%Y-%m-%d}. Datensatz: {len(panel):,} "
        f"Zeilen, {panel['stadtteil'].nunique()} Stadtteile, "
        f"{panel['jahr_monat'].min()}-{panel['jahr_monat'].max()}.")
    log(f"Diagnosen auf den {train['stadtteil'].nunique()} Trainingsstadtteilen "
        f"von Fold 1 ({len(train):,} Zeilen).")

    zielgroessen(panel, train)
    linearitaet(train)
    spezifikation(train)
    vif(train)
    extrapolation(panel)
    klassenbalance(kl)
    urteil()

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "eignungspruefung.md").write_text("\n".join(bericht), encoding="utf-8")
    print(f"\n  => {(OUT / 'eignungspruefung.md').relative_to(ROOT)}")
    print("  Naechster Schritt: modelle/m02_menge.py")


if __name__ == "__main__":
    main()
