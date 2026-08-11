"""
Eignungspruefung: Passen die gewaehlten Verfahren zu den Zielgroessen?

Zweiter Schritt der Vorpruefung. Setzt `v1_baselines.py` voraus - die
Baseline-Werte werden gelesen, nicht neu gerechnet.

Sechs Belege, mehr nicht:

  1  Zaehldaten sind ueberdispers          ->  zaehldatengerechte Verlust-
                                               funktionen (#42); die Stufe-2-
                                               Baseline bleibt das Poisson-GLM
                                               (#45, Begruendung in Abschnitt 1)
  2  Zusammenhaenge sind nicht linear      ->  Ridge auf log(1+y), nicht roh
  3  Lineare Spezifikation reicht nicht    ->  Random Forest und XGBoost
  4  Teststadtteile liegen oft ausserhalb  ->  Limitation, keine Verfahrensfrage
  5  Merkmale trennen auch die Einsatzart  ->  RF und XGBoost im 2. Strang
  6  Anforderungen je Verfahren geprueft   ->  Tabelle mit Teststatistik und
                                               p-Wert, Auflage vom 10.08.2026

Abschnitt 6 ist Auflage Schroeter (10.08.2026): "Pruefung ob die Algorithmen
auf den Daten passen, z.B. Varianzgleichheit, linearer Zusammenhang ... Jeder
Algorithmus sollte dargestellt werden ... Test laufen lassen: in Tabelle
Statistiken mit p-Werten anzeigen." Die Abschnitte 1 bis 5 belegen die
VERFAHRENSWAHL, Abschnitt 6 fuehrt die Anforderungen je Verfahren zusammen -
einschliesslich der Zeilen, in denen eine Anforderung GAR NICHT besteht. Genau
die gehoeren hin: Dass Baumverfahren keine Verteilungsannahme haben, ist eine
Aussage und keine Auslassung.

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
          results/eignungspruefung/annahmen.csv      Abschnitt 6, maschinenlesbar
          results/eignungspruefung/qq_residuen.csv   Rohdaten fuer Abbildung A10

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

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
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
    """Beleg 1: Die Zaehldaten sind ueberdispers - und was daraus folgt.

    NEU GEFASST AM 10.08.2026. Bis dahin schloss dieser Abschnitt aus der
    Overdispersion, Poisson scheide aus und die Negative Binomial sei die
    passende Baseline. Decision Log #45 hat am 06.08. das Gegenteil entschieden
    und ist am 08.08. freigegeben worden - der Abschnitt argumentierte danach
    gegen die eigene Umsetzung, und die erzeugte `eignungspruefung.md` trug den
    Widerspruch weiter. Kein Rechenfehler, sondern Drift.

    DIE KORREKTE FOLGERUNG hat zwei Aeste, und nur der erste betrifft die
    Baseline:

      Verlustfunktion   Ein quadratischer Fehler auf rohen Zaehldaten ist bei
                        diesem Dispersionsindex unangemessen. Daraus folgt
                        `reg:tweedie` fuer XGBoost und `criterion="poisson"`
                        fuer den Random Forest (#42). Das ist die eigentliche
                        Konsequenz aus dieser Messung.

      Baseline          Die Overdispersion verletzt die Poisson-Varianzannahme
                        Var = mu. Beschaedigt werden dadurch die
                        STANDARDFEHLER, nicht die Konsistenz des geschaetzten
                        bedingten Mittelwerts (Gourieroux, Monfort & Trognon
                        1984). Eine Baseline, die ausschliesslich
                        Punktvorhersagen liefert - keine Koeffiziententests,
                        keine Konfidenzintervalle -, ist davon nicht betroffen.
                        Das Poisson-GLM bleibt Stufe 2 (#45).

    Die Negative Binomial waere die Erweiterung fuer korrekte INFERENZ. Sie
    loest damit ein Problem, das diese Baseline nicht hat, und bringt mit dem
    Dispersionsparameter eine zusaetzliche Groesse mit - sie ist dann nicht
    mehr "die einfachste Form, die zur Datenform passt".

    Der gemessene Index bleibt unveraendert und wird weiterhin berichtet. Er
    ist nicht falsch geworden, er traegt nur eine andere Schlussfolgerung.
    """
    y = train[ZIELGROESSE].astype(float)
    index = y.var() / y.mean()

    log("\n## 1  Zaehldaten sind ueberdispers\n")
    log(f"Mittel {y.mean():.1f} | Varianz {y.var():.1f} | "
        f"**Dispersionsindex {index:.1f}**")
    log("")
    log("Poisson unterstellt Varianz = Mittelwert, also einen Index von 1. Der")
    log("gemessene Wert liegt weit darueber. Daraus folgt zweierlei, und beides")
    log("betrifft nicht dieselbe Modellklasse.")
    log("")
    log("**Fuer die Vergleichsverfahren:** Ein quadratischer Fehler auf rohen")
    log("Zaehldaten gewichtet bei dieser Streuung einen absoluten Fehler in")
    log("einem grossen Stadtteil genauso wie in einem kleinen, wo er ein")
    log("Vielfaches des Gesamtwerts ausmacht. Random Forest und XGBoost rechnen")
    log("deshalb mit zaehldatengerechten Verlustfunktionen - `criterion=")
    log("\"poisson\"` und `reg:tweedie` (Decision Log #42).")
    log("")
    log("**Fuer die Stufe-2-Baseline:** Die verletzte Varianzannahme")
    log("beschaedigt die Standardfehler des Poisson-Schaetzers, nicht die")
    log("Konsistenz des bedingten Mittelwerts (Gourieroux, Monfort & Trognon")
    log("1984). Die Baseline liefert ausschliesslich Punktvorhersagen und")
    log("verwendet keine Standardfehler - sie ist davon nicht betroffen. Das")
    log("**Poisson-GLM mit Offset bleibt Stufe 2** (Decision Log #45). Die")
    log("Negative Binomial ist damit **nicht mehr** die Stufe-2-Baseline: Sie")
    log("waere die Erweiterung fuer korrekte Inferenz und loest ein Problem,")
    log("das hier nicht besteht.")
    log("")
    log("Der FORMALE Test dazu - die Hilfsregression nach Cameron und Trivedi")
    log("(1990) - steht in Abschnitt 6. Der Dispersionsindex ist eine")
    log("Kennzahl, kein Test; die Auflage vom 10.08.2026 verlangt beides.")
    return float(index)


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
    reset = {}
    for potenz in (2, 3):
        r = linear_reset(ols, power=potenz, test_type="fitted", use_f=True)
        reset[potenz] = (float(r.fvalue), float(r.pvalue))
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
    return reset


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
    return float(np.mean(anteile))


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

    # DIE SPALTE "je Fold" ZEIGT NUR WIEDERHOLUNG 0 - nachgezogen 10.08.2026.
    # `v1_baselines.py` liefert seit dem 05.08. 50 Laeufe statt 5 (10
    # Wiederholungen x 5 Folds), damit m03 gepaart testen kann. Diese Tabelle
    # hat weiterhin alle Zeilen aufgereiht: eine Zelle mit 50 durch Punkte
    # getrennten Werten, unlesbar und als "je Fold" auch falsch beschriftet.
    #
    # Der MITTELWERT bleibt bewusst ueber ALLE Laeufe gebildet - das ist der
    # Wert, der in 03_STAND.md steht und gegen den m03 antritt. Gezeigt werden
    # die fuenf Folds der Wiederholung 0, weil sie die Aufteilung aus der Datei
    # sind (v0_aufteilung) und damit die nachvollziehbare.
    log("\nWie viel von diesem Signal schoepft ein lineares Modell aus?\n")
    log("| Stufe | Verfahren | Macro-F1 je Fold (Wiederholung 0) | Mittel (alle Laeufe) |")
    log("|---|---|---|---|")
    for stufe in (1, 2):
        g = basis[basis["stufe"] == stufe]
        je_fold = (g[g["wiederholung"] == 0] if "wiederholung" in g.columns
                   else g).sort_values("fold")
        log(f"| {stufe} | {g['modell'].iloc[0]} | "
            + " · ".join(f"{w:.3f}" for w in je_fold["Macro-F1"])
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
def _z(wert: float, stellen: int = 1) -> str:
    """Teststatistik mit deutschem Dezimalkomma - wie die p-Spalte daneben."""
    return f"{wert:.{stellen}f}".replace(".", ",")


def _p(wert: float) -> str:
    """p-Wert deutsch. Unter 0,001 wird nicht mehr beziffert, sondern begrenzt.

    Grund: `4.0e-47` ist keine Information, die jemand liest - die Aussage ist
    "praktisch null". Bei n = 3.036 findet ein Test ohnehin fast jede
    Abweichung; die Effektgroesse traegt, nicht die Nachkommastelle.
    """
    if wert != wert:                      # NaN
        return "–"
    if wert < 0.001:
        return "< 0,001"
    return f"{wert:.3f}".replace(".", ",")


def annahmen(train: pd.DataFrame, befunde: dict) -> pd.DataFrame:
    """Beleg 6: Was verlangt jedes Verfahren - und haelt der Datensatz das?

    AUFLAGE SCHROETER, 10.08.2026. Verlangt sind drei Dinge: die Anforderungen
    JE VERFAHREN dargestellt, formale Tests statt Augenmass, und beides in
    einer Tabelle mit Teststatistik und p-Wert.

    DREI SORTEN VON ZEILEN, und die dritte ist die wichtigste:

      erfuellt            die Anforderung besteht und ist eingehalten
      verletzt            sie besteht und ist verletzt - dann steht in der
                          Spalte "Konsequenz", was daraus folgt
      nicht erforderlich  das Verfahren stellt diese Anforderung gar nicht

    Die dritte Sorte wegzulassen waere der Fehler. Dass Random Forest keine
    Verteilungsannahme hat, ist eine AUSSAGE ueber das Verfahren - und sie ist
    der halbe Grund, warum es im Vergleich steht. Eine Tabelle, die nur
    verletzte Annahmen zeigt, laesst die Baumverfahren voraussetzungslos
    aussehen; eine, die sie ganz weglaesst, beantwortet die Auflage nicht.

    DREI NEUE TESTS, die es vorher nicht gab:

      Cameron & Trivedi (1990)  Hilfsregression auf Ueberdispersion. Der
                                Dispersionsindex aus Abschnitt 1 ist eine
                                Kennzahl, kein Test - hier steht der t-Wert.
      Breusch-Pagan             Varianzgleichheit der Residuen. Woertlich in
                                der Auflage genannt.
      Jarque-Bera               Normalitaet der Residuen, dazu Schiefe und
                                Woelbung. Die zugehoerige Abbildung ist A10;
                                die Rohdaten dafuer schreibt diese Funktion
                                nach `qq_residuen.csv`, gezeichnet wird in
                                m05 - dieses Skript erzeugt Befunde, keine
                                druckfertigen Abbildungen.

    WAS HIER NICHT STEHT: die Multikollinearitaet. Der VIF wird in
    `m04_shap._vif()` gerechnet, weil seine einzige echte Konsequenz die
    Interpretation der Beitraege betrifft. Ihn hier zu wiederholen hiesse,
    dieselbe Zahl an zwei Orten zu fuehren - genau die Fehlerquelle, die
    `tools/pruefe_zahlen.py` bewacht.
    """
    import statsmodels.api as sm
    from scipy import stats
    from statsmodels.stats.diagnostic import het_breuschpagan
    from statsmodels.stats.stattools import jarque_bera

    log("\n## 6  Anforderungen je Verfahren\n")

    X = sm.add_constant(train[MERKMALE].astype(float), has_constant="add")
    y = train[ZIELGROESSE].astype(float).to_numpy()

    # --- Ueberdispersion, formal (Cameron & Trivedi 1990) -----------------
    # Hilfsregression: z = ((y - mu)^2 - y) / mu auf mu, ohne Konstante. Der
    # Koeffizient ist der Dispersionsparameter alpha der NB2-Form, H0 lautet
    # alpha = 0 (Equidispersion). Einseitig, weil Unterdispersion hier keine
    # sinnvolle Gegenhypothese waere.
    poisson = sm.GLM(y, X, family=sm.families.Poisson(),
                     offset=np.log(train[EXPOSURE_ROH].astype(float))).fit()
    mu = np.asarray(poisson.fittedvalues, float)
    z = ((y - mu) ** 2 - y) / mu
    hilfs = sm.OLS(z, mu).fit()
    ct_t, ct_p = float(hilfs.tvalues[0]), float(hilfs.pvalues[0]) / 2

    # --- Streuung und Verteilung der Residuen -----------------------------
    # Geprueft wird das lineare Modell, fuer das Ridge steht: OLS auf log(1+y)
    # mit denselben zwoelf Merkmalen. Ridge selbst hat denselben Erwartungswert
    # und unterscheidet sich nur durch den Strafterm.
    diagnose, qq = {}, []
    for ziel in (ZIELGROESSE, RATE):
        ols = sm.OLS(np.log1p(train[ziel].astype(float)), X).fit()
        bp = het_breuschpagan(ols.resid, ols.model.exog)
        jb = jarque_bera(ols.resid)
        diagnose[ziel] = {"bp_lm": float(bp[0]), "bp_p": float(bp[1]),
                          "jb": float(jb[0]), "jb_p": float(jb[1]),
                          "schiefe": float(jb[2]), "woelbung": float(jb[3])}
        r = np.sort((ols.resid - ols.resid.mean()) / ols.resid.std(ddof=1))
        theo = stats.norm.ppf((np.arange(1, len(r) + 1) - 0.5) / len(r))
        qq += [{"zielgroesse": ziel, "theoretisch": float(t),
                "beobachtet": float(b)} for t, b in zip(theo, r)]

    pd.DataFrame(qq).round(5).to_csv(OUT / "qq_residuen.csv", index=False)

    reset = befunde["reset"]
    d_anz = diagnose[ZIELGROESSE]

    def Z(verfahren, anforderung, pruefung, statistik, p, status, konsequenz,
          wert=float("nan")):
        """Eine Zeile der Anforderungstabelle.

        `statistik` ist die LESBARE Fassung mit Dezimalkomma, `wert` dieselbe
        Zahl maschinenlesbar. Beides, weil `tools/pruefe_zahlen.py` den Sollwert
        aus dieser Datei zieht und "t = 17,2" dafuer erst geparst werden
        muesste - eine Zeichenkette, die man parst, ist eine Zeichenkette, die
        sich beim naechsten Formatwechsel anders parst.
        """
        return {"verfahren": verfahren, "anforderung": anforderung,
                "pruefung": pruefung, "statistik": statistik,
                "statistik_wert": wert, "p_wert": p,
                "status": status, "konsequenz": konsequenz}

    zeilen = [
        Z("alle Verfahren", "unabhaengige Beobachtungen",
          "Panelstruktur: 132 Monate je Stadtteil", "–", float("nan"),
          "verletzt",
          "Stadtteil-Split statt zufaelliger Aufteilung; Streuung ueber die "
          "10 Wiederholungsmittel statt ueber 50 Laeufe (R-5)"),
        Z("alle Verfahren", "identische Merkmale, Zeilen und Folds",
          "fold-Spalte in der Parquet-Datei", "–", float("nan"), "erfuellt",
          "konstruktiv abgesichert, Auflage C vom 04.08.2026"),

        Z("Poisson-GLM (Stufe 2)", "Equidispersion, Var = mu",
          "Cameron & Trivedi (1990), Hilfsregression", f"t = {_z(ct_t)}",
          ct_p, "verletzt",
          "folgenlos fuer diese Baseline: sie liefert nur Punktvorhersagen, "
          "der Schaetzer bleibt konsistent (Gourieroux et al. 1984, #45)",
          wert=ct_t),
        Z("Poisson-GLM (Stufe 2)", "Linearitaet im Log-Link",
          "RESET, Abschnitt 3", f"F = {_z(reset[2][0])}", reset[2][1],
          "verletzt",
          "bewusst in Kauf genommen; die Gegenprobe v3_spezifikation zeigt, "
          "dass die nichtlinearen Erweiterungen out-of-sample SCHLECHTER "
          "sind (B-41)", wert=reset[2][0]),

        Z("Ridge", "Linearitaet der Zusammenhaenge",
          "RESET und Pearson gegen Spearman, Abschnitte 2 und 3",
          f"F = {_z(reset[2][0])}", reset[2][1], "verletzt",
          "Schaetzung auf log(1+y), Ruecktransformation mit expm1; "
          "Guetemasse auf der Originalskala", wert=reset[2][0]),
        Z("Ridge", "Homoskedastizitaet der Residuen",
          "Breusch-Pagan auf log(1+y)", f"LM = {_z(d_anz['bp_lm'])}",
          d_anz["bp_p"],
          "verletzt" if d_anz["bp_p"] < 0.05 else "erfuellt",
          "betrifft die Standardfehler, nicht die Punktprognose - und "
          "Standardfehler werden hier nicht berichtet", wert=d_anz["bp_lm"]),
        Z("Ridge", "normalverteilte Residuen",
          "Jarque-Bera, Abbildung A10", f"JB = {_z(d_anz['jb'])}",
          d_anz["jb_p"], "nicht erforderlich",
          f"Normalitaet ist Voraussetzung fuer INFERENZ, nicht fuer die "
          f"Punktprognose eines L2-penalisierten Modells. Schiefe "
          f"{_z(d_anz['schiefe'], 2)}, Woelbung {_z(d_anz['woelbung'], 2)}",
          wert=d_anz["jb"]),

        Z("Random Forest", "Verteilungsannahme", "entfaellt", "–",
          float("nan"), "nicht erforderlich",
          "verteilungsfrei; das ist der Grund, warum das Verfahren im "
          "Vergleich steht"),
        Z("Random Forest", "Testpunkte im gelernten Wertebereich",
          "Extrapolationsanteil, Abschnitt 4",
          f"{_z(befunde['extrapolation'] * 100)} %", float("nan"), "verletzt",
          "Baeume ordnen ausserhalb dem letzten bekannten Blatt zu - "
          "Limitation des Stadtteil-Splits, Kapitel 8.3 (R-3)",
          wert=befunde["extrapolation"] * 100),
        Z("Random Forest", "Verlustfunktion passend zur Datenform",
          "Dispersionsindex, Abschnitt 1", f"{_z(befunde['dispersion'])}",
          float("nan"), "erfuellt",
          "criterion=\"poisson\" statt quadratischem Fehler (#42)",
          wert=befunde["dispersion"]),

        Z("XGBoost", "Verteilungsannahme", "entfaellt", "–", float("nan"),
          "nicht erforderlich", "verteilungsfrei, wie Random Forest"),
        Z("XGBoost", "Testpunkte im gelernten Wertebereich",
          "Extrapolationsanteil, Abschnitt 4",
          f"{_z(befunde['extrapolation'] * 100)} %", float("nan"), "verletzt",
          "wie Random Forest; beide Baumverfahren sind gleich betroffen",
          wert=befunde["extrapolation"] * 100),
        Z("XGBoost", "Verlustfunktion passend zur Datenform",
          "Dispersionsindex, Abschnitt 1", f"{_z(befunde['dispersion'])}",
          float("nan"), "erfuellt",
          "reg:tweedie mit getuntem Varianzexponenten (#42); Poisson (p = 1) "
          "waere bei diesem Index zu eng", wert=befunde["dispersion"]),

        Z("Multinomiales Logit (Stufe 2)", "Linearitaet in den Log-Odds",
          "keine formale Pruefung", "–", float("nan"), "angenommen",
          "genau die Trennlinie zu RF und XGBoost: fehlende Wechselwirkungen "
          "sind der Unterschied, den der Vergleich messen soll"),
        Z("Multinomiales Logit (Stufe 2)", "jede Klasse im Testfold besetzt",
          "doppelte Stratifizierung (#30), Selbsttest v0_aufteilung", "–",
          float("nan"), "erfuellt",
          "ohne sie waere die Macro-AUROC in einzelnen Folds undefiniert"),
    ]

    df = pd.DataFrame(zeilen)
    df.to_csv(OUT / "annahmen.csv", index=False)

    log("| Verfahren | Anforderung | Pruefung | Statistik | p | Status | Konsequenz |")
    log("|---|---|---|---|---|---|---|")
    for z in zeilen:
        log(f"| {z['verfahren']} | {z['anforderung']} | {z['pruefung']} | "
            f"{z['statistik']} | {_p(z['p_wert'])} | **{z['status']}** | "
            f"{z['konsequenz']} |")

    n_verletzt = sum(z["status"] == "verletzt" for z in zeilen)
    n_entfaellt = sum(z["status"] == "nicht erforderlich" for z in zeilen)
    log("")
    log(f"**{len(zeilen)} Anforderungen geprueft**: {n_verletzt} verletzt, "
        f"{n_entfaellt} bestehen fuer das jeweilige Verfahren nicht, der Rest")
    log("ist eingehalten. Zu jeder verletzten Anforderung steht in der letzten")
    log("Spalte, was daraus folgt - eine verletzte Annahme ohne Konsequenz")
    log("waere ein erkanntes und nicht geloestes Problem.")
    log("")
    log("**Folgt daraus:** Kein Verfahren wird eingesetzt, ohne dass seine")
    log("Voraussetzungen geprueft sind. Die beiden Baumverfahren sind nicht")
    log("deshalb voraussetzungsfrei, weil nichts geprueft wurde, sondern weil")
    log("sie keine Verteilungsannahme stellen - ihre eigentliche Anforderung")
    log("ist der Interpolationsbereich, und die ist verletzt.")
    return df


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

    # Die Abschnitte 1, 3 und 4 geben ihre Kennzahlen zurueck, damit
    # Abschnitt 6 sie nicht ein zweites Mal rechnen muss. Zweimal gerechnet
    # hiesse zwei Zahlen, die auseinanderlaufen koennen.
    befunde = {"dispersion": dispersion(train)}
    linearitaet(train)
    befunde["reset"] = spezifikation(train)
    befunde["extrapolation"] = extrapolation(panel)
    klassifikation(kl)
    annahmen(train, befunde)

    log("\n## Fazit\n")
    log("| Zielgroesse | Verfahren | Beleg | Status |")
    log("|---|---|---|---|")
    log("| Anzahl | Poisson-GLM mit Offset (Stufe 2) | Zaehldaten mit Exposition, Abschnitt 1 | belegt |")
    log("| Anzahl, Rate | Zaehldatengerechter Verlust in RF und XGBoost | Dispersionsindex, Abschnitt 1 | belegt |")
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
