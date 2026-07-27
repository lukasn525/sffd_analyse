"""
Verfahrensvergleich Klassifikation: Einsatzart in vier Gruppen.

Liest ausschliesslich data/processed/klassifikation.parquet.

Zielgroesse `einsatzart_gruppe` mit vier zusammengefassten NFIRS-Serien
(Decision Log #21). Bewertet wird mit Macro-F1 und Macro-AUROC (One-vs-Rest);
zugeordnet wird ueber `argmax`, nicht ueber einen Schwellenwert. Macro
gewichtet alle vier Klassen gleich - sonst dominiert Fehlalarm/Good Intent mit
48 % das Ergebnis, und Accuracy waere ohnehin wertlos.

Die binaere Zielgroesse `ist_brand` liegt im selben Datensatz und dient als
Robustheitslauf; dort ist der Schwellenwert je Fold auf dem inneren
Validierungsfenster zu kalibrieren (Basisratendrift, Eignungspruefung Abschnitt 8).

Verfahren:
  LogisticRegression(penalty="l2")   Ridge-Pendant fuer die Klassifikation
  RandomForestClassifier
  XGBClassifier                      falls xgboost installiert ist

FAIRNESS-REGEL: `wochentag` wird fuer ALLE Verfahren im selben
ColumnTransformer One-Hot-kodiert - auch fuer XGBoost, das kategoriale Merkmale
nativ verarbeiten koennte. Nur so ist die Designmatrix identisch und
Unterschiede bleiben rein algorithmisch.

Das End-Hold-out (`ist_holdout == 1`) wird hier NICHT ausgewertet.

Ausfuehren:
  python modelle/train_klassifikation.py
"""
import sys
import time
from pathlib import Path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep"))

from config import (KLASSEN, MERKMALE_KATEGORIAL, N_FOLDS,  # noqa: E402
                    PFAD_KLASSIFIKATION, RANDOM_STATE, RESULTS_DIR, ROOT)
from cv import bewerte_mehrklassig, fold_masken  # noqa: E402
from klassifikation_datensatz import merkmalslisten  # noqa: E402

OUT = RESULTS_DIR / "klassifikation"

# Nur die beiden zentralen Saetze rechnen: A+B ist das Hauptmodell, B zeigt,
# wie viel die Stadtteilstruktur ueberhaupt beitraegt.
SAETZE = ["A+B", "B"]


def vorverarbeitung(spalten: list[str]) -> ColumnTransformer:
    """One-Hot fuer kategoriale, Standardisierung fuer numerische Merkmale.

    Die Standardisierung braucht nur die Logistische Regression; sie den
    Baumverfahren ebenfalls zu geben ist wirkungsneutral (monotone
    Transformation) und haelt die Designmatrix identisch.
    """
    kategorial = [c for c in spalten if c in MERKMALE_KATEGORIAL]
    numerisch  = [c for c in spalten if c not in MERKMALE_KATEGORIAL]
    return ColumnTransformer([
        ("num", StandardScaler(), numerisch),
        ("kat", OneHotEncoder(handle_unknown="ignore", sparse_output=False),
         kategorial),
    ])


def modelle():
    liste = [
        ("Logistische Regression (L2)",
         lambda: LogisticRegression(penalty="l2", max_iter=1000,
                                    class_weight="balanced",
                                    random_state=RANDOM_STATE)),
        ("Random Forest",
         lambda: RandomForestClassifier(n_estimators=200, n_jobs=-1,
                                        min_samples_leaf=5,
                                        class_weight="balanced",
                                        random_state=RANDOM_STATE)),
    ]
    try:
        from xgboost import XGBClassifier
        liste.append(("XGBoost",
                      lambda: XGBClassifier(n_estimators=300, learning_rate=0.1,
                                            max_depth=6, subsample=0.8,
                                            colsample_bytree=0.8, n_jobs=-1,
                                            random_state=RANDOM_STATE,
                                            tree_method="hist")))
    except ImportError:
        print("  HINWEIS: xgboost nicht installiert - wird uebersprungen "
              "(`pip install xgboost`).\n")
    return liste


def main() -> pd.DataFrame:
    OUT.mkdir(parents=True, exist_ok=True)
    d = pd.read_parquet(PFAD_KLASSIFIKATION)
    saetze = merkmalslisten()

    print(f"Datensatz: {len(d):,} Einsaetze | {d['stadtteil'].nunique()} Stadtteile "
          f"| {d['jahr_monat'].min()}-{d['jahr_monat'].max()}")
    print("Das End-Hold-out wird hier bewusst NICHT ausgewertet.\n")

    zeilen = []
    for k in range(1, N_FOLDS + 1):
        tr, te = fold_masken(d, k)
        train, test = d[tr], d[te]
        y_train = train["einsatzart_gruppe"]
        y_test  = test["einsatzart_gruppe"]

        for satz in SAETZE:
            spalten = saetze[satz]
            for name, bauen in modelle():
                modell = bauen()
                # XGBoost braucht numerische Klassenlabels.
                ist_xgb = name == "XGBoost"
                ziel = (y_train.map({k_: i for i, k_ in enumerate(KLASSEN)})
                        if ist_xgb else y_train)

                pipe = Pipeline([("prep", vorverarbeitung(spalten)),
                                 ("modell", modell)])
                t0 = time.perf_counter()
                pipe.fit(train[spalten], ziel)
                train_s = time.perf_counter() - t0

                t0 = time.perf_counter()
                p_hat = pipe.predict_proba(test[spalten])
                inferenz_s = time.perf_counter() - t0

                # Spaltenreihenfolge der Wahrscheinlichkeiten auf KLASSEN bringen
                if ist_xgb:
                    reihenfolge = list(range(len(KLASSEN)))
                    namen = KLASSEN
                else:
                    namen = list(pipe.named_steps["modell"].classes_)
                    reihenfolge = [namen.index(k_) for k_ in KLASSEN]
                    namen = KLASSEN
                p_hat = p_hat[:, reihenfolge] if not ist_xgb else p_hat

                zeilen.append({"fold": k, "modell": f"{name} ({satz})",
                               **bewerte_mehrklassig(y_test, p_hat, KLASSEN),
                               "train_s": round(train_s, 2),
                               "inferenz_s": round(inferenz_s, 3)})
                print(f"  Fold {k} | {satz:<4} | {name:<28} "
                      f"Macro-F1 {zeilen[-1]['Macro-F1']:.3f} | "
                      f"Macro-AUROC {zeilen[-1]['Macro-AUROC']:.3f}")

    df = pd.DataFrame(zeilen)
    mittel = (df.groupby("modell")[["Macro-F1", "Macro-AUROC", "train_s"]]
                .agg(["mean", "std"]).round(3))
    mittel.columns = [f"{a}_{b}" for a, b in mittel.columns]
    mittel = mittel.sort_values("Macro-AUROC_mean", ascending=False)

    print("\nMittelwert +/- Std ueber die Folds:\n", mittel.to_string())
    df.to_csv(OUT / "klassifikation_folds.csv", index=False)
    mittel.to_csv(OUT / "klassifikation_mittel.csv")
    print(f"\n  => {OUT.relative_to(ROOT)}/klassifikation_*.csv")
    print("\nNaechster Ausbau: RandomizedSearchCV auf dem inneren "
          "Validierungsfenster, binaerer Robustheitslauf auf `ist_brand` mit "
          "Schwellenkalibrierung je Fold, danach EINMALIG das End-Hold-out.")
    return df


if __name__ == "__main__":
    main()
