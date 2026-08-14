"""
Suchdiagnose - war die Hyperparametersuche am Limit?

    python tools/suchdiagnose.py            beide Straenge, alle Verfahren
    python tools/suchdiagnose.py menge      nur die Regression
    python tools/suchdiagnose.py struktur   nur die Klassifikation
    python tools/suchdiagnose.py --nur-xgboost   das billigste sinnvolle Mass
    python tools/suchdiagnose.py --test     Rauchtest, Budget 6, ~3 min

`--test` schreibt nach `results/suchdiagnose_test/` und laesst die echte
Ausgabe unberuehrt. Vor einem zweistuendigen Lauf einmal ausfuehren - er
prueft beide Straenge einmal durch, damit ein Fehler nicht erst nach der
ganzen Rechenzeit auffaellt.

Ausgang: results/suchdiagnose/kurve.csv · raender.csv · zusammenfassung.md

NICHT TEIL DER ABGABE als Skript. Die Befunde schon - sie beantworten eine
Auflage aus der Sprechstunde vom 10.08.2026 mit einer Messung statt mit einem
Argument.

--------------------------------------------------------------------------
DIE ZWEI FRAGEN
--------------------------------------------------------------------------
  1  WAR DAS BUDGET ZU KLEIN?   `tuning.csv` haelt nur den Gewinner fest, nicht
     den Weg dorthin. Diese Diagnose schreibt jede einzelne Ziehung mit ihrem
     inneren Guetewert mit und bildet daraus die SUCHKURVE: bester Wert nach
     n Ziehungen. Steigt sie nach Ziehung 50 noch, war Budget 50 zu klein.
     Laeuft sie flach aus, war sie es nicht.

  2  STAND DER ZAUN AN DER FALSCHEN STELLE?  Ein Suchraum ist eine Festlegung,
     keine Naturkonstante. Liegt der beste gefundene Wert AM RAND, liegt das
     Optimum vermutlich dahinter - und die Suche durfte nie hin.

--------------------------------------------------------------------------
ZWEI SORTEN RAND - der Unterschied entscheidet
--------------------------------------------------------------------------
Gemessen am Lauf vom 07.08. (Budget 50, fuenf Folds):

  NATUERLICH, nichts zu tun
    random_forest max_features       4/5 waehlen 1,0 - das sind ALLE Merkmale
    random_forest min_samples_leaf   3/5 waehlen 1  - weniger als eine
                                     Beobachtung je Blatt gibt es nicht
    Das ist ein BEFUND, kein Mangel: Der Wald will maximale Flexibilitaet.

  WILLKUERLICH, hier kann etwas fehlen
    xgboost max_depth (Struktur)     4/5 waehlen 3 - die UNTERGRENZE
    xgboost max_depth (Menge)        3/5 am Rand, beide Enden getroffen
    ridge alpha, xgboost reg_lambda  je 1/5 am Rand

Der erste Fall ist der wichtigste des Projekts: In der Klassifikation will
XGBoost den flachsten Baum, den es darf. Genau dort widersprechen sich
Kreuzvalidierung und Hold-out (R-2, B-42) - das Muster von Ueberanpassung.
Wird die Untergrenze geoeffnet und XGBoost waehlt dann Tiefe 1 oder 2, war der
Suchraum die Ursache.

--------------------------------------------------------------------------
WIE VERGLICHEN WIRD - ein Lauf statt zwei
--------------------------------------------------------------------------
Naheliegend waeren zwei Durchgaenge, einer je Suchraum. Das kostet doppelt.
Stattdessen laeuft NUR der erweiterte Raum, und bei jeder Ziehung wird
vermerkt, ob sie auch im ALTEN Raum gelegen haette. Daraus entstehen zwei
Kurven aus denselben Ziehungen, denselben Folds und demselben Startwert:

    bester Wert ueber alle Ziehungen          -> erweiterter Raum
    bester Wert ueber die Teilmenge "alt"     -> urspruenglicher Raum

Ehrlich dazu: Die Teilmenge ist kleiner als 100, der Vergleich also nicht bei
gleichem Budget. Die Zahl der Ziehungen je Teilmenge wird deshalb mitberichtet.

--------------------------------------------------------------------------
WAS DIESE DIAGNOSE NICHT LEISTET
--------------------------------------------------------------------------
Der innere Guetewert ist NICHT die Testleistung. Ein besserer innerer Wert
garantiert kein besseres Ergebnis auf unbekannten Stadtteilen - er sagt nur,
dass die Suche noch etwas gefunden hat. Ob sich das auf die Prognose
uebertraegt, zeigt erst der Hauptlauf.

Die Diagnose beantwortet also: War die Suche am Limit? Nicht: Wird das
Ergebnis besser?

--------------------------------------------------------------------------
WAS SIE NICHT ANFASST
--------------------------------------------------------------------------
  - `results/regression/` und `results/klassifikation/` bleiben unberuehrt
  - das HOLD-OUT wird nicht gelesen; gefiltert wird wie in m02/m03, bevor
    irgendetwas rechnet
  - `config_modelle.SUCHRAEUME` wird nicht veraendert, nur lokal ueberlagert
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for teil in ("prep", "vorpruefung", "modelle"):
    sys.path.insert(0, str(ROOT / teil))

from config import (N_FOLDS, PFAD_KLASSIFIKATION, PFAD_REGRESSION,  # noqa: E402
                    PRAEDIKTOREN, RESULTS_DIR, SAISON)
from config_modelle import RANDOM_STATE, SUCHRAEUME  # noqa: E402
from s2_datensaetze import RATE, ZIELKLASSE, fold_masken  # noqa: E402
from v0_aufteilung import (selten_je_stadtteil,  # noqa: E402
                           wiederholte_aufteilung)

OUT = RESULTS_DIR / "suchdiagnose"
MERKMALE = PRAEDIKTOREN + SAISON
BUDGET = 100

# ==========================================================================
# Die erweiterten Suchraeume - NUR wo der Rand willkuerlich ist
# ==========================================================================
# Nicht erweitert werden max_features, min_samples_leaf, subsample und
# colsample_bytree: Deren Grenzen sind natuerlich (alle Merkmale, eine
# Beobachtung, der ganze Datensatz). Dahinter existiert nichts.
#
# Ebenfalls NICHT erweitert: n_estimators. Nur ein Fold lag nahe der Grenze,
# und mehr Baeume sind der groesste Laufzeittreiber. Bewusste Auslassung.
WEITER = {
    "ridge": {
        "alpha": ("loguniform", 1e-5, 1e5),          # war 1e-3 bis 1e3
    },
    "random_forest": {
        # ZWEI Aenderungen. Erstens nach oben erweitert. Zweitens die
        # REIHENFOLGE korrigiert: `None` heisst unbegrenzte Tiefe, ist also
        # faktisch der TIEFSTE Wert - stand in der alten Liste aber an erster
        # Stelle. Jede Auswertung, die die Listenposition als Tiefe liest,
        # bekam damit ein verdrehtes Bild (betrifft auch Abbildung A8).
        "max_depth": ("choice", [8, 12, 16, 24, 32, 48, None]),
    },
    "xgboost": {
        "max_depth": ("int", 1, 14),                 # war 3 bis 10
        "reg_lambda": ("loguniform", 1e-4, 1e4),     # war 1e-2 bis 1e2
    },
}

# Laufzeit je Suchlauf bei Budget 50, gemessen am 07.08. Verdoppelt sich mit
# dem Budget. Dient nur der Vorabschaetzung.
SEKUNDEN_50 = {("menge", "ridge"): 3, ("menge", "random_forest"): 210,
               ("menge", "xgboost"): 154, ("struktur", "random_forest"): 184,
               ("struktur", "xgboost"): 233}


# Steuert die Verlustfunktion der REGRESSION und ist bei `multi:softprob`
# bedeutungslos. XGBoost nimmt den Parameter stillschweigend an und ignoriert
# ihn - ein Sechstel des Budgets ginge auf eine wirkungslose Dimension, und die
# Suchkurve des Strukturstrangs fiele dadurch zu flach aus.
# `m03_struktur.suchraum()` entfernt ihn aus demselben Grund; ohne diese Zeile
# weicht die Diagnose vom Hauptlauf ab (gefunden im Rauchtest am 13.08.2026).
NUR_REGRESSION = {"tweedie_variance_power"}


def erweitert(name: str, strang: str = "menge") -> dict:
    """Suchraum eines Verfahrens mit den Erweiterungen ueberlagert."""
    raum = dict(SUCHRAEUME[name])
    raum.update({k: v for k, v in WEITER.get(name, {}).items() if k in raum})
    if strang == "struktur":
        raum = {k: v for k, v in raum.items() if k not in NUR_REGRESSION}
    return raum


def _verteilungen(raum: dict, praefix: str = "") -> dict:
    """Spezifikation -> scipy-Verteilungen. Wortgleich zu m02.suchraum()."""
    from scipy.stats import loguniform, randint, uniform

    aus = {}
    for p, spez in raum.items():
        art, *w = spez
        if art == "loguniform":
            aus[praefix + p] = loguniform(w[0], w[1])
        elif art == "int":
            aus[praefix + p] = randint(w[0], w[1] + 1)
        elif art == "uniform":
            aus[praefix + p] = uniform(w[0], w[1] - w[0])
        elif art == "choice":
            aus[praefix + p] = w[0]
        else:
            raise ValueError(f"Unbekannte Suchraum-Art: {art}")
    return aus


def im_alten_raum(name: str, parameter: dict) -> bool:
    """Haette diese Ziehung auch im urspruenglichen Suchraum liegen koennen?

    Nur die erweiterten Parameter werden geprueft - die uebrigen sind
    unveraendert und liegen zwangslaeufig drin.
    """
    for p, spez in WEITER.get(name, {}).items():
        schluessel = next((k for k in parameter if k.split("__")[-1] == p), None)
        if schluessel is None:
            continue
        wert, alt = parameter[schluessel], SUCHRAEUME[name].get(p)
        if alt is None:
            continue
        art, *w = alt
        if art == "choice":
            if wert not in w[0]:
                return False
        elif not (w[0] <= wert <= w[1]):
            return False
    return True


# ==========================================================================
def eine_suche(strang: str, name: str, train: pd.DataFrame, fold: int) -> list:
    """Ein Suchlauf mit Budget 100. Gibt JEDE Ziehung zurueck, nicht nur den Sieger."""
    from sklearn.model_selection import GroupKFold, RandomizedSearchCV

    if strang == "menge":
        import m02_menge as modul
        praefix = ("transformedtargetregressor__regressor__"
                   if name == "ridge" else "")
        X, y, extra = (train[MERKMALE].astype(float),
                       train[RATE].astype(float), {})
        scoring = "neg_root_mean_squared_error"
    else:
        import m03_struktur as modul
        praefix = ""
        y_int = modul.kodiere(train[ZIELKLASSE])
        X, y = train[MERKMALE].astype(float), y_int
        extra = {"sample_weight": modul._gewichte(y_int)} if name == "xgboost" else {}
        scoring = "f1_macro"

    suche = RandomizedSearchCV(
        estimator=modul.verfahren(name, n_jobs=1),
        param_distributions=_verteilungen(erweitert(name, strang), praefix),
        n_iter=BUDGET, cv=GroupKFold(n_splits=4), scoring=scoring,
        random_state=RANDOM_STATE, n_jobs=-1)
    suche.fit(X, y, groups=train["stadtteil"], **extra)

    zeilen = []
    for i, (p, wert) in enumerate(zip(suche.cv_results_["params"],
                                      suche.cv_results_["mean_test_score"]), 1):
        rein = {k.split("__")[-1]: v for k, v in p.items()}
        zeilen.append({"strang": strang, "verfahren": name, "fold": fold,
                       "ziehung": i, "wert": float(wert),
                       "im_alten_raum": int(im_alten_raum(name, p)),
                       **rein})
    return zeilen


def _md(df: pd.DataFrame) -> str:
    """Markdown-Tabelle von Hand.

    NICHT `DataFrame.to_markdown()`: Das braucht `tabulate`, und das steht
    weder in `requirements.txt` noch im gemessenen `requirements_lauf.txt`.
    Der Aufruf waere erst nach ein bis zwei Stunden Rechenzeit gescheitert -
    beim Schreiben des Berichts, also nach der ganzen Arbeit.
    """
    kopf = list(df.columns)
    zeilen = ["| " + " | ".join(kopf) + " |", "|" + "---|" * len(kopf)]
    for _, z in df.iterrows():
        zeilen.append("| " + " | ".join(str(z[s]) for s in kopf) + " |")
    return "\n".join(zeilen)


def kurve(df: pd.DataFrame) -> pd.DataFrame:
    """Bester Wert nach n Ziehungen - einmal gesamt, einmal nur "alter Raum".

    Beide Guetemasse sind so gerichtet, dass GROSS besser ist
    (neg_root_mean_squared_error und f1_macro), deshalb genuegt das laufende
    Maximum.
    """
    teile = []
    for (s, v, f), g in df.groupby(["strang", "verfahren", "fold"], sort=False):
        g = g.sort_values("ziehung").copy()
        g["bester_bisher"] = g["wert"].cummax()
        nur_alt = g["wert"].where(g["im_alten_raum"] == 1)
        g["bester_bisher_alt"] = nur_alt.cummax().ffill()
        g["n_alt_bisher"] = g["im_alten_raum"].cumsum()
        teile.append(g)
    return pd.concat(teile, ignore_index=True)


def raender(df: pd.DataFrame) -> pd.DataFrame:
    """Wo liegt der SIEGER je Fold - im erweiterten Bereich oder im alten?"""
    zeilen = []
    for (s, v, f), g in df.groupby(["strang", "verfahren", "fold"], sort=False):
        sieger = g.loc[g["wert"].idxmax()]
        for p in WEITER.get(v, {}):
            if p not in g.columns:
                continue
            # `max_depth = None` heisst UNBEGRENZTE Tiefe. Ueber den DataFrame
            # wird daraus NaN, und das sieht wie ein fehlender Wert aus statt
            # wie der tiefste moegliche. Deshalb ausgeschrieben.
            wert = sieger[p]
            zeilen.append({"strang": s, "verfahren": v, "fold": f,
                           "parameter": p,
                           "gewaehlt": "None (unbegrenzt)" if pd.isna(wert)
                                       else wert,
                           "sieger_im_alten_raum": int(sieger["im_alten_raum"])})
    return pd.DataFrame(zeilen)


# ==========================================================================
def main(argv: list[str]) -> int:
    global BUDGET, OUT
    if "--test" in argv:
        BUDGET, OUT = 6, RESULTS_DIR / "suchdiagnose_test"
        print("\n  RAUCHTEST - Budget 6, Ausgabe nach results/suchdiagnose_test/")
        print("  Die Zahlen sind bedeutungslos. Geprueft wird nur, ob es laeuft.")

    straenge = [a for a in argv if a in ("menge", "struktur")] or \
               ["menge", "struktur"]
    verfahren = {"menge": ["ridge", "random_forest", "xgboost"],
                 "struktur": ["random_forest", "xgboost"]}
    if "--nur-xgboost" in argv:
        verfahren = {k: ["xgboost"] for k in verfahren}

    for pfad in (PFAD_REGRESSION, PFAD_KLASSIFIKATION):
        if not pfad.exists():
            raise SystemExit(f"{pfad.name} fehlt - erst 'python prep/build.py'.")

    schaetzung = sum(SEKUNDEN_50.get((s, v), 200) * 2 * N_FOLDS
                     for s in straenge for v in verfahren[s])
    print(f"\n{'=' * 78}\n  SUCHDIAGNOSE - Budget {BUDGET}, erweiterte Suchraeume"
          f"\n{'=' * 78}")
    print(f"  Straenge: {', '.join(straenge)}")
    print(f"  Geschaetzte Dauer: {schaetzung / 60:.0f} min")
    print("  Das Hold-out wird nicht gelesen. results/regression und")
    print("  results/klassifikation bleiben unberuehrt.\n")

    kl = pd.read_parquet(PFAD_KLASSIFIKATION)
    selten = selten_je_stadtteil(kl)
    daten = {"menge": pd.read_parquet(PFAD_REGRESSION), "struktur": kl}

    alle, t0 = [], time.perf_counter()
    for strang in straenge:
        panel = daten[strang]
        panel = panel[panel["ist_holdout"] == 0].reset_index(drop=True)
        d = wiederholte_aufteilung(panel, wiederholung=0, selten=selten)
        for name in verfahren[strang]:
            for k in range(1, N_FOLDS + 1):
                tr, _ = fold_masken(d, k)
                t = time.perf_counter()
                alle += eine_suche(strang, name, d[tr], k)
                print(f"    {strang:<9} {name:<14} Fold {k}  "
                      f"{time.perf_counter() - t:6.1f}s")

    df = pd.DataFrame(alle)
    OUT.mkdir(parents=True, exist_ok=True)
    k = kurve(df)
    k.to_csv(OUT / "kurve.csv", index=False)
    raender(df).to_csv(OUT / "raender.csv", index=False)

    # ---- Frage 1: war das Budget zu klein? -------------------------------
    halb = BUDGET // 2
    print(f"\n{'=' * 78}\n  FRAGE 1  Verbessert sich der beste Wert nach "
          f"Ziehung {halb}?\n{'=' * 78}")
    zeilen = []
    for (s, v), g in k.groupby(["strang", "verfahren"], sort=False):
        bei50 = g[g["ziehung"] == BUDGET // 2].set_index("fold")["bester_bisher"]
        bei100 = g[g["ziehung"] == BUDGET].set_index("fold")["bester_bisher"]
        spanne = g.groupby("fold")["bester_bisher"].last().std()
        gewinn = (bei100 - bei50)
        zeilen.append({"strang": s, "verfahren": v,
                       "gewinn_mittel": float(gewinn.mean()),
                       "gewinn_max": float(gewinn.max()),
                       "folds_mit_gewinn": int((gewinn > 0).sum()),
                       "streuung_zwischen_folds": float(spanne)})
        print(f"  {s:<9} {v:<14} Gewinn {halb}->{BUDGET}: "
              f"im Mittel {gewinn.mean():+.4f}, groesster {gewinn.max():+.4f}, "
              f"in {int((gewinn > 0).sum())} von {N_FOLDS} Folds")
    f1 = pd.DataFrame(zeilen)
    print("\n  Einordnung: Ist der Gewinn klein gegenueber der Streuung ZWISCHEN")
    print("  den Folds, hat sich die Suche totgelaufen - Budget 50 genuegte.")

    # ---- Frage 2: stand der Zaun falsch? ---------------------------------
    print(f"\n{'=' * 78}\n  FRAGE 2  Nutzen die Sieger den erweiterten Bereich?"
          f"\n{'=' * 78}")
    r = raender(df)
    for (s, v, p), g in r.groupby(["strang", "verfahren", "parameter"], sort=False):
        aussen = int((g["sieger_im_alten_raum"] == 0).sum())
        werte = ", ".join(str(x) for x in g["gewaehlt"])
        mark = "  <<< alter Raum war zu eng" if aussen else ""
        print(f"  {s:<9} {v:<14} {p:<18} {aussen}/{len(g)} ausserhalb   "
              f"{werte[:38]}{mark}")

    # ---- Bericht ---------------------------------------------------------
    text = ["# Suchdiagnose", "",
            f"Stand {pd.Timestamp.today():%Y-%m-%d}. Budget {BUDGET}, "
            f"erweiterte Suchraeume, Wiederholung 0, Trainingsstadtteile je Fold.",
            "Das Hold-out wurde nicht gelesen.", "",
            "## Frage 1 - war Budget 50 zu klein?", "",
            _md(f1.round(5)), "",
            "## Frage 2 - stand der Suchraum zu eng?", "",
            _md(r), "",
            "**Zu lesen:** Ein Sieger ausserhalb des alten Raums heisst, dass die",
            "urspruengliche Grenze bindend war. Der innere Guetewert ist nicht die",
            "Testleistung - ob sich der Unterschied auf die Prognose uebertraegt,",
            "zeigt erst der Hauptlauf."]
    (OUT / "zusammenfassung.md").write_text("\n".join(text), encoding="utf-8")

    print(f"\n  Gesamtdauer: {(time.perf_counter() - t0) / 60:.1f} min")
    print(f"  => {OUT / 'zusammenfassung.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
