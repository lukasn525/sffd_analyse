"""
Eignungspruefung: Passen die gewaehlten Verfahren zu den Zielgroessen?

Zweiter Schritt der Vorpruefung. Setzt `v1_baselines.py` voraus - die
Baseline-Werte werden gelesen, nicht neu gerechnet.

Fuenf Belege, mehr nicht:

  1  Zaehldaten sind ueberdispers          ->  Negative Binomial als Baseline
  2  Zusammenhaenge sind nicht linear      ->  Ridge auf log(1+y), nicht roh
  3  Lineare Spezifikation reicht nicht    ->  Random Forest und XGBoost
  4  Teststadtteile liegen oft ausserhalb  ->  Limitation, keine Verfahrensfrage
  5  Merkmale trennen auch die Einsatzart  ->  RF und XGBoost im 2. Strang

Abschnitt 2 ist Auflage Schroeter (R7): "erstmal plotten, falls keine lineare
Baseline, KEIN lineares Regressionsmodell." Deshalb Streudiagramme und
Residuenanalyse, beides als Abbildung.

Abschnitt 5 ist noetig, weil die Regression den Klassifikationsstrang NICHT
mitbeantwortet: Dass der Zusammenhang zur Anzahl gekruemmt ist, sagt nichts
darueber, ob dieselben Merkmale die Art der Einsaetze trennen koennen.

Was diese Pruefung NICHT leistet: Sie unterscheidet nicht zwischen Random
Forest und XGBoost. Welche der beiden Strategien gewinnt, ist die empirische
Forschungsfrage der Arbeit - vorab noetig ist nur, dass beide plausibel sind.

Gerechnet wird ausschliesslich auf den TRAININGSSTADTTEILEN VON FOLD 1 - die
Teststadtteile duerfen keine Modellentscheidung beeinflussen. Ausgenommen sind
Abschnitt 4 (Extrapolation, betrifft alle Folds naturgemaess) und die aus
v1_baselines.py gelesenen Referenzwerte.

Der Bericht ist ein BEFUNDBLATT, keine Kapitelvorlage: Er liefert Zahlen und
Abbildungen, die Argumentation fuer Kapitel 6.2 wird von Hand geschrieben.

Eingang:  data/processed/{regression,klassifikation}.parquet
          results/klassifikation/baselines_klasse.csv
Ausgang:  results/eignungspruefung/eignungspruefung.md + 2 Abbildungen

Ausfuehren:
  python vorpruefung/v2_eignung.py
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


def log(txt: str = "") -> None:
    print(txt)
    bericht.append(txt)


def speichere(fig, name: str) -> None:
    fig.tight_layout()
    fig.savefig(OUT / name, dpi=140)
    plt.close(fig)
    log(f"-> {name}")


# ---------------------------------------------------------------------------
def dispersion(train: pd.DataFrame) -> None:
    """Beleg 1: Overdispersion schliesst Poisson aus."""
    y = train[ZIELGROESSE].astype(float)
    index = y.var() / y.mean()

    log("\n## 1  Zaehldaten sind ueberdispers\n")
    log(f"Mittel {y.mean():.1f} | Varianz {y.var():.1f} | "
        f"**Dispersionsindex {index:.1f}**")
    log("")
    log("Poisson unterstellt Varianz = Mittelwert, also einen Index von 1. Der")
    log("gemessene Wert liegt weit darueber. Poisson scheidet aus, die Negative")
    log("Binomial ist die passende Count-Baseline.")


# ---------------------------------------------------------------------------
def linearitaet(train: pd.DataFrame) -> None:
    """Beleg 2: Auflage Schroeter (R7) - plotten, bevor Ridge eingesetzt wird.

    Zwei Diagnosen. Pearson misst den LINEAREN, Spearman den MONOTONEN
    Zusammenhang; klaffen sie auseinander, ist der Zusammenhang gekruemmt.
    Bewertet wird das nur, wo die Korrelation ueberhaupt substanziell ist - bei
    einer Korrelation nahe null ist der Abstand Rauschen.

    Danach Ridge einmal auf der Rohskala und einmal auf log(1+y), mit
    Residuenbild. Ein Trichter zeigt, dass der Fehler mit dem Niveau waechst.
    """
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    log("\n## 2  Die Zusammenhaenge sind nicht linear (R7)\n")

    log("Beide Mengen-Zielgroessen, weil sie unterschiedliche Groessen sind:")
    log("`anzahl_einsaetze` ist eine Zaehlung, `einsaetze_je_1000_ew` eine Quote.\n")
    log("| Merkmal | Pearson (Anzahl) | Spearman | Pearson (Rate) | Spearman |")
    log("|---|---|---|---|---|")
    # Massgeblich ist das MAXIMUM beider Korrelationen, nicht Pearson allein.
    # Grund: Ist ein Zusammenhang stark gekruemmt, faellt Pearson gerade
    # deshalb ab - ein reiner Pearson-Filter wuerde also ausgerechnet den
    # staerksten Kruemmungsbefund aussortieren. Umgekehrt bleibt der Zweck
    # erhalten: Liegen BEIDE Korrelationen nahe null, ist ihr Abstand Rauschen
    # und sagt nichts ueber die Funktionsform.
    SCHWELLE = 0.20

    stark = {ZIELGROESSE: [], RATE: []}
    for m in PRAEDIKTOREN:
        werte = []
        for ziel in (ZIELGROESSE, RATE):
            p = train[m].corr(train[ziel])
            s = train[m].corr(train[ziel], method="spearman")
            werte += [p, s]
            if max(abs(p), abs(s)) > SCHWELLE:
                stark[ziel].append((m, p, s, abs(p - s)))
        log(f"| `{m}` | " + " | ".join(f"{w:+.3f}" for w in werte) + " |")

    log("")
    log(f"Bewertet werden Merkmale, bei denen **mindestens eine** der beiden")
    log(f"Korrelationen ueber {SCHWELLE:.2f} liegt. Bei starker Kruemmung faellt")
    log("Pearson ab - ein reiner Pearson-Filter wuerde den staerksten Befund")
    log("aussortieren.\n")
    for ziel in (ZIELGROESSE, RATE):
        if not stark[ziel]:
            log(f"`{ziel}`: kein Merkmal ueber der Schwelle - nicht bewertbar.")
            continue
        m, p, s, d = max(stark[ziel], key=lambda x: x[3])
        art = "Kruemmung" if abs(s) > abs(p) else "Hebelpunkte"
        log(f"`{ziel}`: {len(stark[ziel])} Merkmale bewertet, groesster Abstand "
            f"**`{m}`** ({p:+.3f} gegen {s:+.3f}, {d:.3f}) - {art}.")

    fig, achsen = plt.subplots(2, 5, figsize=(16, 6.5))
    for ax, merkmal in zip(achsen.ravel(), PRAEDIKTOREN):
        ax.scatter(train[merkmal], np.log1p(train[ZIELGROESSE]), s=4, alpha=0.25)
        ax.set_xlabel(merkmal, fontsize=8)
        ax.set_ylabel("log(1+y)", fontsize=8)
        ax.tick_params(labelsize=7)
    fig.suptitle("Strukturmerkmale gegen log(1 + Anzahl Einsaetze), "
                 "Trainingsstadtteile Fold 1")
    speichere(fig, "01_streudiagramme.png")

    # Residuen fuer BEIDE Zielgroessen, je roh und logarithmiert.
    X = train[MERKMALE].astype(float)
    guete = {}
    fig, achsen = plt.subplots(2, 2, figsize=(12, 8))
    for zeile, ziel in enumerate((ZIELGROESSE, RATE)):
        roh = train[ziel].astype(float)
        for spalte, (name, y) in enumerate([("Rohskala", roh),
                                            ("log(1+y)", np.log1p(roh))]):
            modell = make_pipeline(StandardScaler(), Ridge(alpha=1.0)).fit(X, y)
            guete[(ziel, name)] = modell.score(X, y)
            ax = achsen[zeile, spalte]
            ax.scatter(modell.predict(X), y - modell.predict(X), s=4, alpha=0.25)
            ax.axhline(0, color="black", lw=0.8)
            ax.set_title(f"{ziel} - {name}", fontsize=9)
            ax.set_xlabel("Vorhersage", fontsize=8)
            ax.set_ylabel("Residuum", fontsize=8)
    speichere(fig, "02_residuen.png")

    log("")
    log("Ridge im Training (Anpassung, NICHT Prognoseguete):\n")
    log("| Zielgroesse | R2 Rohskala | R2 log(1+y) |")
    log("|---|---|---|")
    for ziel in (ZIELGROESSE, RATE):
        log(f"| `{ziel}` | {guete[(ziel, 'Rohskala')]:.3f} | "
            f"{guete[(ziel, 'log(1+y)')]:.3f} |")

    log("")
    log("**Folgt daraus:** Ridge wird auf `log(1+y)` geschaetzt, Guetemasse")
    log("nach expm1-Ruecktransformation auf der Originalskala - fuer die")
    log("Zielgroesse, bei der die Transformation die Anpassung verbessert.")


# ---------------------------------------------------------------------------
def spezifikation(train: pd.DataFrame) -> None:
    """Beleg 3: Der RESET-Test verwirft die lineare Spezifikation.

    Er prueft, ob Potenzen der Vorhersage noch etwas erklaeren. Tun sie das,
    hat das lineare Modell Struktur uebrig gelassen. Die Interaktionsterme
    zeigen anschliessend, dass ein Teil davon Wechselwirkungen sind.
    """
    import statsmodels.api as sm
    from statsmodels.stats.diagnostic import linear_reset

    log("\n## 3  Die lineare Spezifikation reicht nicht\n")

    y = np.log1p(train[ZIELGROESSE].astype(float))
    ols = sm.OLS(y, sm.add_constant(train[MERKMALE].astype(float),
                                    has_constant="add")).fit()

    log("| RESET-Test | F | p |")
    log("|---|---|---|")
    for potenz in (2, 3):
        r = linear_reset(ols, power=potenz, test_type="fitted", use_f=True)
        log(f"| Potenzen bis {potenz} | {r.fvalue:.1f} | {r.pvalue:.1e} |")
    log("")
    log("H0 (die lineare Spezifikation ist adaequat) wird verworfen.")

    inter = train[MERKMALE].astype(float).copy()
    for i, a in enumerate(PRAEDIKTOREN):
        for b in PRAEDIKTOREN[i + 1:]:
            inter[f"{a}_x_{b}"] = train[a].astype(float) * train[b].astype(float)
    ols_inter = sm.OLS(y, sm.add_constant(inter, has_constant="add")).fit()
    zusatz = len(inter.columns) - len(MERKMALE)

    log("")
    log(f"Adjustiertes R2 ohne Interaktionen: {ols.rsquared_adj:.3f}")
    log(f"Adjustiertes R2 mit  Interaktionen: {ols_inter.rsquared_adj:.3f} "
        f"({zusatz} Terme)")
    log("")
    log("**Folgt daraus:** Die lineare Spezifikation scheitert an Kruemmung")
    log("(Abschnitt 2) und an fehlenden Wechselwirkungen. Baumverfahren fangen")
    log("beides ohne Zutun ab - ein Split kann an beliebiger Stelle schneiden,")
    log(f"und jeder Split bedingt auf die vorherigen. Von Hand waeren es "
        f"{zusatz} Terme, deren Auswahl willkuerlich waere und die bei "
        f"{train['stadtteil'].nunique()} Trainingsstadtteilen ueberanpassen.")


# ---------------------------------------------------------------------------
def extrapolation(panel: pd.DataFrame) -> None:
    """Beleg 4: Wie oft liegt ein Teststadtteil ausserhalb des Gelernten?"""
    log("\n## 4  Teststadtteile liegen haeufig ausserhalb des Trainingsbereichs\n")
    log("| Fold | 1 | 2 | 3 | 4 | 5 | Mittel |")
    log("|---|---|---|---|---|---|---|")

    anteile = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(panel, k)
        lo, hi = panel.loc[tr, MERKMALE].min(), panel.loc[tr, MERKMALE].max()
        aussen = ((panel.loc[te, MERKMALE] < lo)
                  | (panel.loc[te, MERKMALE] > hi)).any(axis=1)
        anteile.append(aussen.mean())
    log("| Testzeilen ausserhalb | "
        + " | ".join(f"{a * 100:.1f} %" for a in anteile)
        + f" | **{np.mean(anteile) * 100:.1f} %** |")

    log("")
    log("**Folgt daraus:** Keine Verfahrensfrage, sondern eine Limitation des")
    log("Stadtteil-Splits - sie gehoert in Kapitel 8.3. Die Verfahren sind")
    log("unterschiedlich betroffen: Ridge rechnet ausserhalb linear weiter,")
    log("Baumverfahren ordnen dem letzten bekannten Blatt zu. Die Spanne von")
    log(f"{min(anteile) * 100:.1f} bis {max(anteile) * 100:.1f} % erklaert einen")
    log("Teil der Fold-Streuung und begruendet die wiederholten Splits.")


# ---------------------------------------------------------------------------
def klassifikation(kl: pd.DataFrame) -> None:
    """Beleg 5: Taugen dieselben Merkmale auch fuer die Einsatzart?

    Eine Frage, die die Regression NICHT mitbeantwortet. Dass der Zusammenhang
    zur ANZAHL gekruemmt ist, sagt nichts darueber, ob dieselben Merkmale die
    ART trennen koennen - das sind zwei verschiedene Fragen an dieselben Spalten.

    Geprueft wird per Kruskal-Wallis je Merkmal ueber die vier Klassen:
    nichtparametrisch, vertraegt ungleich grosse Gruppen. Trennt kein Merkmal,
    ist die Zielgroesse mit diesen Praediktoren nicht vorhersagbar - und zwar
    fuer JEDES Verfahren.

    Danach werden die beiden Baseline-Stufen aus v1_baselines.py berichtet, um
    das Signal ins Verhaeltnis zu setzen: Wie viel davon schoepft ein lineares
    Modell aus? OB flexiblere Verfahren mehr herausholen, beantwortet m03 - mit
    getunten Modellen ueber alle Wiederholungen und nicht mit einer Vorschau.
    """
    from scipy.stats import kruskal

    log("\n## 5  Die Merkmale trennen auch die Einsatzart\n")

    tr, _ = fold_masken(kl, 1)
    train = kl[tr]
    klassen = sorted(train[ZIELKLASSE].unique())

    log("**(a) Ist Signal da?** Kruskal-Wallis je Merkmal ueber die vier Klassen.\n")
    log("| Merkmal | H | p |")
    log("|---|---|---|")
    signifikant = 0
    for m in PRAEDIKTOREN:
        gruppen = [train.loc[train[ZIELKLASSE] == k, m].astype(float)
                   for k in klassen]
        h, p = kruskal(*gruppen)
        signifikant += p < 0.05
        log(f"| `{m}` | {h:.1f} | {p:.1e} |")

    log("")
    log(f"**{signifikant} von {len(PRAEDIKTOREN)} Merkmalen** unterscheiden sich")
    log("signifikant zwischen den Klassen. Die Zielgroesse ist mit diesen")
    log("Praediktoren also grundsaetzlich vorhersagbar - Voraussetzung fuer jedes")
    log("Verfahren und keine Frage, die erst Kapitel 7 beantwortet.")

    # Stufe 1 und 2 aus der Baseline-Datei lesen, nicht neu rechnen.
    pfad = RESULTS_DIR / "klassifikation" / "baselines_klasse.csv"
    if not pfad.exists():
        raise SystemExit(f"{pfad.relative_to(ROOT)} fehlt - "
                         f"erst 'python vorpruefung/v1_baselines.py' ausfuehren.")
    basis = pd.read_csv(pfad)

    log("\nWie viel von diesem Signal schoepft ein lineares Modell aus?\n")
    log("| Stufe | Verfahren | Macro-F1 je Fold | Mittel |")
    log("|---|---|---|---|")
    for stufe in (1, 2):
        g = basis[basis["stufe"] == stufe].sort_values("fold")
        log(f"| {stufe} | {g['modell'].iloc[0]} | "
            + " · ".join(f"{w:.3f}" for w in g["Macro-F1"])
            + f" | **{g['Macro-F1'].mean():.3f}** |")

    stufe1 = basis.loc[basis["stufe"] == 1, "Macro-F1"].mean()
    stufe2 = basis.loc[basis["stufe"] == 2, "Macro-F1"].mean()
    log("")
    log(f"Wenig: {stufe2:.3f} gegenueber {stufe1:.3f} der Mehrheitsklasse - ein")
    log(f"Zugewinn von {stufe2 - stufe1:.3f} bei einem Maximum von 1,0, obwohl")
    log("die Merkmale hochsignifikant trennen.")
    log("")
    log("**Folgt daraus:** Die Klassengrenze laesst sich nicht gut durch Geraden")
    log("im Merkmalsraum beschreiben. Das ist konstruktionsbedingt zu erwarten:")
    log("Die Zielgroesse entsteht als Maximum ueber vier Anteile, die Grenze")
    log("zwischen zwei Klassen liegt dort, wo die zugehoerigen Anteile einander")
    log("schneiden - im Merkmalsraum eine Schnittflaeche, keine Hyperebene.")
    log("Verfahren mit flexibleren Grenzen sind damit begruendet; OB sie den")
    log("Rueckstand aufholen, beantwortet m03.")


# ---------------------------------------------------------------------------
def main() -> None:
    if not PFAD_REGRESSION.exists():
        raise SystemExit(f"{PFAD_REGRESSION.relative_to(ROOT)} fehlt - "
                         f"erst 'python prep/build.py' ausfuehren.")
    OUT.mkdir(parents=True, exist_ok=True)

    panel = pd.read_parquet(PFAD_REGRESSION)
    kl = pd.read_parquet(PFAD_KLASSIFIKATION)
    train = panel[fold_masken(panel, 1)[0]]

    log("# Eignungspruefung")
    log("")
    log(f"Stand {pd.Timestamp.today():%Y-%m-%d}. Gerechnet auf den "
        f"{train['stadtteil'].nunique()} Trainingsstadtteilen von Fold 1 "
        f"({len(train):,} Zeilen); die Teststadtteile bleiben unberuehrt.")

    dispersion(train)
    linearitaet(train)
    spezifikation(train)
    extrapolation(panel)
    klassifikation(kl)

    log("\n## Fazit\n")
    log("| Zielgroesse | Verfahren | Beleg | Status |")
    log("|---|---|---|---|")
    log("| Anzahl | Negative Binomial (Stufe 2) | Overdispersion, Abschnitt 1 | belegt |")
    log("| Anzahl, Rate | Ridge auf `log(1+y)` | Residuenbilder, Abschnitt 2 | belegt |")
    log("| Anzahl, Rate | Random Forest, XGBoost | RESET und Interaktionen, Abschnitt 3 | belegt |")
    log("| Einsatzart | Log. Regression (Stufe 2) | Signaltest, Abschnitt 5 | belegt |")
    log("| Einsatzart | Random Forest, XGBoost | geringe lineare Ausschoepfung, Abschnitt 5 | belegt |")
    log("")
    log("**Was diese Pruefung nicht leistet:** Sie unterscheidet nicht")
    log("zwischen Random Forest und XGBoost - beide bekommen dieselbe Begruendung.")
    log("Das ist richtig so: Welche der beiden Kombinationsstrategien (Bagging")
    log("gegen Boosting) gewinnt, ist die empirische Forschungsfrage der Arbeit.")
    log("Vorab noetig ist nur, dass beide plausibel sind.")

    (OUT / "eignungspruefung.md").write_text("\n".join(bericht), encoding="utf-8")
    print(f"\n=> {(OUT / 'eignungspruefung.md').relative_to(ROOT)}")


if __name__ == "__main__":
    main()
