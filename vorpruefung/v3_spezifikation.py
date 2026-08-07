"""
Haelt die diagnostizierte Nichtlinearitaet out-of-sample nach?

    python vorpruefung/v3_spezifikation.py

Eingang: data/processed/regression.parquet
Ausgang: results/spezifikation/spezifikation_{folds,mittel}.csv

STAND: vollstaendig, 07.08.2026.

--------------------------------------------------------------------------
WOZU DIESES SKRIPT
--------------------------------------------------------------------------
`v2_eignung.py` weist nach, dass die lineare Spezifikation nicht ausreicht:
der RESET-Test verwirft sie deutlich (F = 215,2 bei Potenzen bis 2), und 45
Interaktionsterme heben das adjustierte R2 von 0,805 auf 0,919. Daraus wurde
die Wahl der Baumverfahren begruendet - sie fangen Kruemmung und
Wechselwirkungen ohne Zutun ab.

Beide Kennzahlen sind IN-SAMPLE-Groessen, berechnet auf 3.828 Zeilen, die als
unabhaengig behandelt werden. Tatsaechlich liegen 29 unabhaengige Stadtteile
mit je 132 Monaten vor. Ein F-Test mit n = 3.828 findet praktisch jede
Abweichung signifikant, und adjustiertes R2 korrigiert fuer die Zahl der
Parameter, nicht fuer die geklumpte Struktur.

Die Diagnose beantwortet also: STECKT in diesen Daten Struktur jenseits der
Geraden? Der Verfahrensvergleich beantwortet eine andere Frage: UEBERTRAEGT
sich diese Struktur auf unbekannte Stadtteile? Dieses Skript stellt genau
diese zweite Frage - mit demselben Modell, demselben Split und denselben 50
Laeufen wie die Baseline, nur mit erweiterter Merkmalsmatrix.

Es ist damit kein Modellvorschlag. Keine der drei Erweiterungen tritt im
Verfahrensvergleich an; sie dienen ausschliesslich der Interpretation des
Hauptbefundes (docs/07_BEFUNDE.md, B-41).

--------------------------------------------------------------------------
DIE VIER SPEZIFIKATIONEN
--------------------------------------------------------------------------
Grundlage ist immer das Stufe-2-Modell aus `v1_baselines.py`: Poisson-GLM mit
log-Link und `log(Bevoelkerung)` als Offset, unpenalisiert angepasst.

  linear          12 Terme   die 10 Praediktoren + monat_sin + monat_cos
  quadrate        22 Terme   zusaetzlich die Quadrate der 10 Praediktoren
  interaktionen   57 Terme   zusaetzlich alle 45 Paarprodukte der Praediktoren
  beides          67 Terme   Quadrate und Paarprodukte

Die Saisonterme werden WEDER quadriert NOCH gekreuzt. monat_sin^2 +
monat_cos^2 = 1 ist exakt kollinear mit der Konstanten; das Modell waere nicht
identifiziert. Die 45 Paarprodukte entsprechen genau den 45 Interaktionstermen,
die `v2_eignung.py` bewertet - deshalb ist der Vergleich derselbe.

STANDARDISIERUNG. Die Merkmale werden je Fold auf den TRAININGSDATEN
z-standardisiert, bevor Quadrate und Produkte gebildet werden. Das ist keine
Modellentscheidung, sondern numerische Notwendigkeit: Das Quadrat des
Medianeinkommens liegt in der Groessenordnung 1e10, das Produkt zweier
Praediktoren bei 1e9, und die IRLS-Iteration bricht auf dieser Konditionierung
zusammen. Mathematisch ist die Standardisierung folgenlos - der aufgespannte
Raum von {1, x, x^2} ist derselbe wie der von {1, z, z^2}, die Vorhersagen sind
identisch. Die Kennzahlen der Spalte `linear` muessen deshalb exakt die
Stufe-2-Baseline aus `results/regression/baselines_mittel.csv` reproduzieren;
das prueft `_selbsttest()` und bricht sonst ab.

KONVERGENZ. Mit 67 Termen auf 3.036 Trainingszeilen konvergiert die
IRLS-Iteration nicht in jedem Fold. Nicht konvergierte Anpassungen werden
GEZAEHLT und mitberichtet, nicht stillschweigend uebergangen und nicht
entfernt - eine nicht konvergierte Anpassung ist Teil des Befundes, dass diese
Spezifikation zu den Daten nicht passt.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Reproduziert `linear` die Stufe-2-Baseline auf drei Nachkommastellen?
    Wenn nein: Abbruch, dann ist die Merkmalsmatrix nicht dieselbe.
  - Wie viele der 200 Anpassungen sind nicht konvergiert, und in welchen
    Spezifikationen? Die Zahl gehoert in den Text.
  - Ist der Abstand `linear` zu `interaktionen` groesser als der Abstand
    `linear` zu Random Forest? Nur dann traegt die Aussage "die Spezifikation
    bewegt mehr als die Verfahrenswahl" (docs/07_BEFUNDE.md, B-41).
  - Das Hold-out bleibt unberuehrt: keine Zeile mit ist_holdout == 1.
"""
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "prep"))
sys.path.insert(0, str(_ROOT / "modelle"))   # nur config_modelle.WIEDERHOLUNGEN
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from config_modelle import WIEDERHOLUNGEN  # noqa: E402
from s2_datensaetze import ZIELGROESSE, fold_masken  # noqa: E402
from v0_aufteilung import (selten_je_stadtteil,  # noqa: E402
                           wiederholte_aufteilung)
from v1_baselines import bewerte_regression  # noqa: E402

OUT = RESULTS_DIR / "spezifikation"
MERKMALE = PRAEDIKTOREN + SAISON

# Reihenfolge ist die Darstellungsreihenfolge in Abbildung und Tabelle.
SPEZIFIKATIONEN = ["linear", "quadrate", "interaktionen", "beides"]

# Nur die Praediktoren werden quadriert und gekreuzt, nie die Saisonterme.
BASIS = list(PRAEDIKTOREN)


def entwerfe(train: pd.DataFrame, test: pd.DataFrame,
             spezifikation: str) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Merkmalsmatrizen fuer Training und Test, auf Trainingsdaten zentriert.

    Gibt (X_train, X_test, Namen) zurueck, jeweils MIT Konstante an Position 0.
    Mittelwert und Streuung stammen ausschliesslich aus dem Training - der
    Teststadtteil darf die Transformation nicht mitbestimmen.
    """
    roh_tr = train[MERKMALE].astype(float).to_numpy()
    roh_te = test[MERKMALE].astype(float).to_numpy()

    mittel = roh_tr.mean(axis=0)
    streuung = roh_tr.std(axis=0)
    streuung[streuung == 0] = 1.0
    z_tr = (roh_tr - mittel) / streuung
    z_te = (roh_te - mittel) / streuung

    spalten_tr, spalten_te, namen = [z_tr], [z_te], list(MERKMALE)
    j = {m: i for i, m in enumerate(MERKMALE)}

    if spezifikation in ("quadrate", "beides"):
        idx = [j[m] for m in BASIS]
        spalten_tr.append(z_tr[:, idx] ** 2)
        spalten_te.append(z_te[:, idx] ** 2)
        namen += [f"{m}^2" for m in BASIS]

    if spezifikation in ("interaktionen", "beides"):
        paare = [(a, b) for i, a in enumerate(BASIS) for b in BASIS[i + 1:]]
        spalten_tr.append(np.column_stack(
            [z_tr[:, j[a]] * z_tr[:, j[b]] for a, b in paare]))
        spalten_te.append(np.column_stack(
            [z_te[:, j[a]] * z_te[:, j[b]] for a, b in paare]))
        namen += [f"{a}:{b}" for a, b in paare]

    X_tr = np.column_stack([np.ones(len(train))] + spalten_tr)
    X_te = np.column_stack([np.ones(len(test))] + spalten_te)
    return X_tr, X_te, ["const"] + namen


def ein_lauf(train: pd.DataFrame, test: pd.DataFrame,
             spezifikation: str) -> dict:
    """Eine Poisson-Anpassung, eine Bewertung auf der Originalskala."""
    import statsmodels.api as sm

    X_tr, X_te, namen = entwerfe(train, test, spezifikation)
    y_tr = train[ZIELGROESSE].astype(float).to_numpy()
    off_tr = np.log(train[EXPOSURE_ROH].astype(float).to_numpy())
    off_te = np.log(test[EXPOSURE_ROH].astype(float).to_numpy())

    with warnings.catch_warnings():
        # Die Warnungen interessieren nicht einzeln - massgeblich ist das
        # Konvergenzflag, das statsmodels selbst setzt.
        warnings.simplefilter("ignore")
        anpassung = sm.GLM(y_tr, X_tr, family=sm.families.Poisson(),
                           offset=off_tr).fit(maxiter=200)
        y_hat = np.asarray(anpassung.predict(X_te, offset=off_te))

    konvergiert = bool(getattr(anpassung, "converged", True))
    # Eine divergierte Anpassung kann beliebig grosse Vorhersagen liefern; die
    # Kennzahlen werden dann unbrauchbar gross. Das ist so gewollt und wird
    # nicht abgeschnitten - es IST das Ergebnis.
    return {"spezifikation": spezifikation, "terme": X_tr.shape[1] - 1,
            "konvergiert": int(konvergiert),
            **bewerte_regression(test[ZIELGROESSE].to_numpy(), y_hat)}


def alle_laeufe(panel: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """10 Wiederholungen x 5 Folds x 4 Spezifikationen = 200 Anpassungen."""
    zeilen = []
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(panel, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            for spez in SPEZIFIKATIONEN:
                zeilen.append({"wiederholung": w, "fold": k,
                               **ein_lauf(train, test, spez)})
        print(f"  Wiederholung {w + 1}/{WIEDERHOLUNGEN} fertig")
    return pd.DataFrame(zeilen)


def zweistufig(df: pd.DataFrame) -> pd.DataFrame:
    """Erst je Wiederholung ueber die Folds, dann ueber die Wiederholungen.

    Dieselbe Regel wie ueberall sonst (docs/06_RISIKEN.md, R-5): massgeblich
    ist die Streuung der 10 Wiederholungsmittel, nicht die der 50 Einzellaeufe.
    """
    masse = ["RMSE", "MAE", "R2"]
    g = df.groupby("spezifikation", sort=False)
    z = g[masse].mean().add_suffix("_mean")
    je_wdh = df.groupby(["spezifikation", "wiederholung"], sort=False)[masse].mean()
    z = z.join(je_wdh.groupby("spezifikation", sort=False).std()
                     .add_suffix("_std_wiederholungen"))
    z = z.join(g["terme"].max())
    z = z.join(g["konvergiert"].sum().rename("konvergiert_von_50"))
    return z.reset_index().round(3)


def _selbsttest(mittel: pd.DataFrame) -> None:
    """Die lineare Spezifikation MUSS die Stufe-2-Baseline reproduzieren.

    Wenn nicht, sieht das Skript andere Merkmale oder andere Folds als
    `v1_baselines.py` - dann ist jeder Vergleich in dieser Datei wertlos.
    """
    pfad = RESULTS_DIR / "regression" / "baselines_mittel.csv"
    if not pfad.exists():
        print("  Hinweis: baselines_mittel.csv fehlt, Selbsttest uebersprungen.")
        return
    b = pd.read_csv(pfad)
    b = b[(b["stufe"] == 2) & (b["zielgroesse"] == ZIELGROESSE)]
    if not len(b):
        print("  Hinweis: keine Stufe-2-Zeile gefunden, Selbsttest uebersprungen.")
        return
    soll = float(b["RMSE_mean"].iloc[0])
    ist = float(mittel.loc[mittel["spezifikation"] == "linear", "RMSE_mean"].iloc[0])
    assert abs(soll - ist) < 0.005, (
        f"'linear' ({ist:.3f}) weicht von der Stufe-2-Baseline ({soll:.3f}) ab. "
        f"Merkmalsmatrix oder Foldzuordnung stimmen nicht ueberein.")
    print(f"  Selbsttest bestanden: linear = Stufe-2-Baseline = {ist:.3f} RMSE")


def run() -> int:
    if not PFAD_REGRESSION.exists():
        raise SystemExit("regression.parquet fehlt - erst 'python prep/build.py'.")

    panel = pd.read_parquet(PFAD_REGRESSION)
    selten = selten_je_stadtteil(pd.read_parquet(PFAD_KLASSIFIKATION))

    print(f"Spezifikationstest - {WIEDERHOLUNGEN} Wiederholungen x {N_FOLDS} "
          f"Folds x {len(SPEZIFIKATIONEN)} Spezifikationen")
    df = alle_laeufe(panel, selten)

    OUT.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT / "spezifikation_folds.csv", index=False)
    mittel = zweistufig(df)
    mittel.to_csv(OUT / "spezifikation_mittel.csv", index=False)

    print()
    for _, z in mittel.iterrows():
        print(f"  {z['spezifikation']:<15} Terme {int(z['terme']):3d}   "
              f"RMSE {z['RMSE_mean']:8.2f}   MAE {z['MAE_mean']:7.2f}   "
              f"R2 {z['R2_mean']:9.3f}   "
              f"konvergiert {int(z['konvergiert_von_50'])}/"
              f"{WIEDERHOLUNGEN * N_FOLDS}")

    print()
    _selbsttest(mittel)
    print(f"  => {(OUT / 'spezifikation_mittel.csv').relative_to(ROOT)}")
    print("\n  Das Hold-out bleibt unberuehrt.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
