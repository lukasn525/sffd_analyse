"""
Suchdiagnose - war das Budget der Hyperparametersuche gross genug?

    python modelle/suchdiagnose.py            beide Straenge, alle Verfahren
    python modelle/suchdiagnose.py menge      nur die Regression
    python modelle/suchdiagnose.py struktur   nur die Klassifikation
    python modelle/suchdiagnose.py --nur-xgboost   das billigste sinnvolle Mass
    python modelle/suchdiagnose.py --test     Rauchtest, Budget 6, ~3 min,
                                              schreibt nach results/suchdiagnose_test/

Output:  results/suchdiagnose/kurve.csv · zusammenfassung.md

  - wiederholt die Suche des Hauptlaufs und schreibt jede Ziehung mit
    (tuning.csv haelt nur den Gewinner) -> Suchkurve: bester innerer Wert
    nach n Ziehungen
  - Gewinn der zweiten Haelfte gegen die Streuung zwischen den Folds
  - innerer Guetewert ist nicht die Testleistung: zeigt, ob die Suche am
    Limit war, nicht ob das Ergebnis besser wird
  - unberuehrt: results/regression/, results/klassifikation/, Hold-out
    (gefiltert wie in m02/m03); Suchraeume, Folds, Startwert wie im Hauptlauf
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

# Gemessene Laufzeit je Suchlauf bei Budget 50, nur zur Vorabschaetzung.
SEKUNDEN_50 = {("menge", "ridge"): 3, ("menge", "random_forest"): 210,
               ("menge", "xgboost"): 154, ("struktur", "random_forest"): 184,
               ("struktur", "xgboost"): 233}


# Nur fuer die Regression; bei multi:softprob wirkungslos (verschwendetes
# Budget, zu flache Suchkurve). Wie in m03_struktur.suchraum() entfernt.
NUR_REGRESSION = {"tweedie_variance_power"}


def suchraum(name: str, strang: str = "menge") -> dict:
    """Suchraum eines Verfahrens, derselbe wie im Hauptlauf."""
    raum = dict(SUCHRAEUME[name])
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
        param_distributions=_verteilungen(suchraum(name, strang), praefix),
        n_iter=BUDGET, cv=GroupKFold(n_splits=4), scoring=scoring,
        random_state=RANDOM_STATE, n_jobs=-1)
    suche.fit(X, y, groups=train["stadtteil"], **extra)

    zeilen = []
    for i, (p, wert) in enumerate(zip(suche.cv_results_["params"],
                                      suche.cv_results_["mean_test_score"]), 1):
        rein = {k.split("__")[-1]: v for k, v in p.items()}
        zeilen.append({"strang": strang, "verfahren": name, "fold": fold,
                       "ziehung": i, "wert": float(wert), **rein})
    return zeilen


def _md(df: pd.DataFrame) -> str:
    """Markdown-Tabelle von Hand.

    NICHT `DataFrame.to_markdown()`: Das braucht `tabulate`, und das steht
    nicht in `requirements.txt`.
    """
    kopf = list(df.columns)
    zeilen = ["| " + " | ".join(kopf) + " |", "|" + "---|" * len(kopf)]
    for _, z in df.iterrows():
        zeilen.append("| " + " | ".join(str(z[s]) for s in kopf) + " |")
    return "\n".join(zeilen)


def kurve(df: pd.DataFrame) -> pd.DataFrame:
    """Bester Wert nach n Ziehungen.

    Beide Guetemasse sind so gerichtet, dass GROSS besser ist
    (neg_root_mean_squared_error und f1_macro), deshalb genuegt das laufende
    Maximum.
    """
    teile = []
    for (s, v, f), g in df.groupby(["strang", "verfahren", "fold"], sort=False):
        g = g.sort_values("ziehung").copy()
        g["bester_bisher"] = g["wert"].cummax()
        teile.append(g)
    return pd.concat(teile, ignore_index=True)


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
    print(f"\n{'=' * 78}\n  SUCHDIAGNOSE - Budget {BUDGET}, Suchraeume des Hauptlaufs"
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

    # ---- War das Budget zu klein? ----------------------------------------
    halb = BUDGET // 2
    print(f"\n{'=' * 78}\n  Verbessert sich der beste Wert nach "
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
    print(f"  den Folds, hat sich die Suche totgelaufen - Budget {halb} haette gereicht.")

    # ---- Bericht ---------------------------------------------------------
    text = ["# Suchdiagnose", "",
            f"Stand {pd.Timestamp.today():%Y-%m-%d}. Budget {BUDGET}, "
            f"Suchraeume des Hauptlaufs, Wiederholung 0, Trainingsstadtteile je Fold.",
            "Das Hold-out wurde nicht gelesen.", "",
            f"## Haette Budget {halb} gereicht?", "",
            _md(f1.round(5)), "",
            "**Zu lesen:** Ist der Gewinn der zweiten Haelfte klein gegenueber der",
            "Streuung zwischen den Folds, hat sich die Suche totgelaufen. Der innere",
            "Guetewert ist nicht die Testleistung."]
    (OUT / "zusammenfassung.md").write_text("\n".join(text), encoding="utf-8")

    print(f"\n  Gesamtdauer: {(time.perf_counter() - t0) / 60:.1f} min")
    print(f"  => {OUT / 'zusammenfassung.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
