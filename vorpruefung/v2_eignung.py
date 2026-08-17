"""
Eignungspruefung: Passen die gewaehlten Verfahren zu den Zielgroessen?

    python vorpruefung/v2_eignung.py

Eingang: data/processed/{regression,klassifikation}.parquet
         results/klassifikation/baselines_klasse.csv (aus v1_baselines.py)
Ausgang: results/eignungspruefung/eignungspruefung.md + 2 Abbildungen
         results/eignungspruefung/annahmen.csv     Abschnitt 6, maschinenlesbar
         results/eignungspruefung/qq_residuen.csv  Rohdaten fuer Abbildung A10

  - Sechs Belege, mehr nicht: (1) Zaehldaten ueberdispers -> zaehldaten-
    gerechte Verlustfunktionen (#42), (2) Zusammenhaenge nicht linear ->
    Ridge auf log(1+y), (3) lineare Spezifikation reicht nicht -> RF und
    XGBoost, (4) Teststadtteile liegen oft ausserhalb -> Limitation, keine
    Verfahrensfrage, (5) Merkmale trennen auch die Einsatzart -> zweiter
    Strang, (6) Anforderungen je Verfahren mit Teststatistik und p-Wert
  - Abschnitt 6 ist Auflage Schroeter (10.08.2026) und fuehrt auch die
    Zeilen, in denen eine Anforderung GAR NICHT besteht: Dass Baumverfahren
    keine Verteilungsannahme haben, ist eine Aussage und keine Auslassung
  - Abschnitt 2 ist Auflage R7 - erst plotten, dann ueber lineare Modelle
    reden. Deshalb Streudiagramme und Residuenanalyse als Abbildung
  - Abschnitt 5 ist noetig, weil die Regression den Klassifikationsstrang
    nicht mitbeantwortet: Kruemmung bei der ANZAHL sagt nichts darueber, ob
    dieselben Merkmale die ART trennen
  - Was die Pruefung NICHT leistet: Sie unterscheidet nicht zwischen Random
    Forest und XGBoost - das ist die empirische Forschungsfrage der Arbeit
  - Gerechnet wird nur auf den TRAININGSSTADTTEILEN VON FOLD 1; ausgenommen
    sind Abschnitt 4 und die aus v1 gelesenen Referenzwerte
  - Der Bericht ist ein Befundblatt, keine Kapitelvorlage

Setzt v1_baselines.py voraus. Ausfuehrliche Fassung:
docs/08_FUNKTIONSDOKUMENTATION.md
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
    """Gibt eine Zeile aus und haengt sie an den Berichtstext an.

    Ein:  Textzeile (leer = Leerzeile)
    Aus:  nichts; wirkt auf die Liste `bericht`
    """
    print(txt)
    bericht.append(txt)


def speichere(fig, name: str) -> None:
    """Legt eine Abbildung im Ergebnisordner ab und vermerkt sie im Bericht.

    Ein:  Matplotlib-Figur, Dateiname
    Aus:  Datei in results/eignungspruefung/; die Figur wird geschlossen
    """
    fig.tight_layout()
    fig.savefig(OUT / name, dpi=140)
    plt.close(fig)
    log(f"-> {name}")


# ---------------------------------------------------------------------------
def dispersion(train: pd.DataFrame) -> None:
    """Beleg 1: Dispersionsindex der beiden Zaehl-Zielgroessen.

    Ein:  Trainingszeilen von Fold 1
    Aus:  Textabschnitt im Bericht, Dispersionsindex je Zielgroesse

    - der Index Varianz/Mittelwert misst die Verletzung der Poisson-Annahme
      Var = mu
    - Folge 1, Verlustfunktion: ein quadratischer Fehler auf rohen Zaehldaten ist
      bei diesem Index unangemessen -> reg:tweedie fuer XGBoost,
      criterion="poisson" fuer den Random Forest (#42)
    - Folge 2, Baseline: beschaedigt sind die Standardfehler, nicht die
      Konsistenz des bedingten Mittelwerts (Gourieroux u.a. 1984). Das
      Poisson-GLM bleibt Stufe 2 (#45)
    - bis 10.08.2026 schloss dieser Abschnitt das Gegenteil und widersprach der
      eigenen Umsetzung; der gemessene Index ist unveraendert geblieben
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
    """Beleg 2: Korrelationen und Residuenbild (Auflage R7).

    Ein:  Trainingszeilen von Fold 1
    Aus:  Textabschnitt, Abbildungen 01_streudiagramme.png und 02_residuen.png

    - Pearson misst den linearen, Spearman den monotonen Zusammenhang; ein
      Auseinanderklaffen zeigt Kruemmung an
    - bewertet wird nur, wo die Korrelation substanziell ist - nahe null ist der
      Abstand Rauschen
    - Ridge einmal auf der Rohskala, einmal auf log(1+y), mit Residuenbild
    - ein Trichter im Residuenbild zeigt, dass der Fehler mit dem Niveau waechst
    """
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    log("\n## 2  Die Zusammenhaenge sind nicht linear (R7)\n")

    log("Beide Mengen-Zielgroessen, weil sie unterschiedliche Groessen sind:")
    log("`anzahl_einsaetze` ist eine Zaehlung, `einsaetze_je_1000_ew` eine Quote.\n")
    log("| Merkmal | Pearson (Anzahl) | Spearman | Pearson (Rate) | Spearman |")
    log("|---|---|---|---|---|")
    # Massgeblich ist das MAXIMUM beider Korrelationen: Ein reiner
    # Pearson-Filter sortierte gerade den staerksten Kruemmungsbefund aus.
    # Liegen beide nahe null, ist ihr Abstand Rauschen.
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
    """Beleg 3: RESET-Test und Interaktionsterme.

    Ein:  Trainingszeilen von Fold 1
    Aus:  Textabschnitt mit F-Wert, p-Wert und adjustiertem R2

    - der RESET-Test prueft, ob Potenzen der Vorhersage noch etwas erklaeren
    - tun sie das, hat das lineare Modell Struktur uebrig gelassen
    - die 45 Interaktionsterme zeigen, dass ein Teil davon Wechselwirkungen sind
    - beide Kennzahlen sind In-Sample-Groessen; die Uebertragung auf unbekannte
      Stadtteile prueft v3_spezifikation.py
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
    """Beleg 4: Anteil der Teststadtteile ausserhalb des Gelernten.

    Ein:  vollstaendiges Panel (alle Folds, Extrapolation betrifft jeden)
    Aus:  Textabschnitt mit dem Anteil ausserhalb der Trainingsspanne

    - ein hoher Anteil ist eine Limitation der Datenlage, keine Verfahrensfrage
    - Baumverfahren extrapolieren grundsaetzlich nicht ueber den gesehenen
      Wertebereich hinaus
    """
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
    """Beleg 5: Trennen dieselben Merkmale auch die Einsatzart?

    Ein:  klassifikation.parquet, baselines_klasse.csv
    Aus:  Textabschnitt mit Kruskal-Wallis je Merkmal und den Baseline-Werten

    - eigene Frage: Kruemmung bei der ANZAHL sagt nichts darueber, ob dieselben
      Merkmale die ART trennen
    - Kruskal-Wallis ist nichtparametrisch und vertraegt ungleich grosse Gruppen
    - trennt kein Merkmal, ist die Zielgroesse fuer JEDES Verfahren nicht
      vorhersagbar
    - die Baseline-Stufen werden nur gelesen, um das Signal einzuordnen; ob
      flexiblere Verfahren mehr herausholen, beantwortet m03
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

    # Die Spalte "je Fold" zeigt nur Wiederholung 0 - die Aufteilung aus der
    # Datei und damit die nachvollziehbare. Der MITTELWERT bleibt ueber alle
    # 50 Laeufe gebildet; das ist der Wert, gegen den m03 antritt.
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
    """Teststatistik mit deutschem Dezimalkomma.

    Ein:  Zahl, Nachkommastellen
    Aus:  Zeichenkette
    """
    return f"{wert:.{stellen}f}".replace(".", ",")


def _p(wert: float) -> str:
    """p-Wert deutsch; unter 0,001 wird begrenzt statt beziffert.

    Ein:  p-Wert
    Aus:  Zeichenkette, ggf. "< 0,001"

    - "4.0e-47" ist keine lesbare Information; die Aussage lautet "praktisch null"
    - bei n = 3.036 findet ein Test fast jede Abweichung; die Effektgroesse
      traegt, nicht die Nachkommastelle
    """
    if wert != wert:                      # NaN
        return "–"
    if wert < 0.001:
        return "< 0,001"
    return f"{wert:.3f}".replace(".", ",")


def annahmen(train: pd.DataFrame, befunde: dict) -> pd.DataFrame:
    """Beleg 6: Anforderungen je Verfahren mit formalen Tests.

    Ein:  Trainingszeilen von Fold 1, Klassifikationspanel
    Aus:  Textabschnitt, annahmen.csv, qq_residuen.csv

    - Auflage Schroeter vom 10.08.2026: Anforderungen je Verfahren, formale Tests
      statt Augenmass, Tabelle mit Teststatistik und p-Wert
    - drei Sorten von Zeilen: erfuellt, verletzt (mit Spalte "Konsequenz") und
      NICHT ERFORDERLICH
    - die dritte Sorte ist die wichtigste: Dass Baumverfahren keine
      Verteilungsannahme haben, ist eine Aussage ueber das Verfahren
    - drei Tests kommen hier neu hinzu: Cameron & Trivedi (1990) auf
      Ueberdispersion, Breusch-Pagan auf Varianzgleichheit, Jarque-Bera auf
      Normalitaet samt Schiefe und Woelbung
    - die Rohdaten fuer Abbildung A10 gehen nach qq_residuen.csv; gezeichnet wird
      in m05
    - der VIF steht bewusst nicht hier, sondern in m04_shap._vif(): dieselbe Zahl
      an zwei Orten ist die Fehlerquelle, die tools/pruefe_zahlen.py bewacht
    """
    import statsmodels.api as sm
    from scipy import stats
    from statsmodels.stats.diagnostic import het_breuschpagan
    from statsmodels.stats.stattools import jarque_bera

    log("\n## 6  Anforderungen je Verfahren\n")

    X = sm.add_constant(train[MERKMALE].astype(float), has_constant="add")
    y = train[ZIELGROESSE].astype(float).to_numpy()

    # --- Ueberdispersion, formal (Cameron & Trivedi 1990) -----------------
    # Hilfsregression z = ((y - mu)^2 - y) / mu auf mu, ohne Konstante. Der
    # Koeffizient ist alpha der NB2-Form, H0: alpha = 0. Einseitig, weil
    # Unterdispersion keine sinnvolle Gegenhypothese waere.
    poisson = sm.GLM(y, X, family=sm.families.Poisson(),
                     offset=np.log(train[EXPOSURE_ROH].astype(float))).fit()
    mu = np.asarray(poisson.fittedvalues, float)
    z = ((y - mu) ** 2 - y) / mu
    hilfs = sm.OLS(z, mu).fit()
    ct_t, ct_p = float(hilfs.tvalues[0]), float(hilfs.pvalues[0]) / 2

    # --- Streuung und Verteilung der Residuen -----------------------------
    # Geprueft wird das lineare Modell, fuer das Ridge steht: OLS auf log(1+y)
    # mit denselben zwoelf Merkmalen - gleicher Erwartungswert, nur ohne
    # Strafterm.
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
        """Baut eine Zeile der Anforderungstabelle.

        Ein:  Verfahren, Anforderung, Status, Test, Statistik, p-Wert, Konsequenz
        Aus:  dict fuer den Tabellenaufbau

        - `statistik` ist die lesbare Fassung mit Dezimalkomma, `wert` dieselbe Zahl
          maschinenlesbar
        - beides, weil tools/pruefe_zahlen.py den Sollwert aus dieser Datei zieht und
          eine geparste Zeichenkette beim naechsten Formatwechsel anders parst
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
    """Rechnet die sechs Belege und schreibt Bericht, Tabellen und Abbildungen.

    Ein:  beide Parquet-Dateien, baselines_klasse.csv
    Aus:  eignungspruefung.md, annahmen.csv, qq_residuen.csv, 2 Abbildungen

    - Schritt 2 von vorpruefung/run.py
    - Grundlage sind die Trainingsstadtteile von Fold 1; die Teststadtteile
      duerfen keine Modellentscheidung beeinflussen
    - Abschnitt 6 uebernimmt die Kennzahlen aus 1, 3 und 4, statt sie neu zu
      rechnen
    """
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

    # Abschnitt 6 uebernimmt die Kennzahlen aus 1, 3 und 4 - zweimal
    # gerechnet hiesse zwei Zahlen, die auseinanderlaufen koennen.
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
