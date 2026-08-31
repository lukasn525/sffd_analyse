"""
Haelt die diagnostizierte Nichtlinearitaet out-of-sample nach?

    python vorpruefung/v3_spezifikation.py

Eingang: data/processed/regression.parquet
Ausgang: results/spezifikation/spezifikation_{folds,mittel}.csv

  - v2_eignung.py verwirft die lineare Spezifikation IN-SAMPLE (RESET
    F = 360,4; adjustiertes R2 von 0,859 auf 0,932 mit 45 Interaktionen).
    Beide Kennzahlen behandeln 3.960 Zeilen als unabhaengig, tatsaechlich
    sind es 30 Stadtteile mit je 132 Monaten - also stellt dieses Skript
    die zweite Frage: UEBERTRAEGT sich die Struktur auf unbekannte
    Stadtteile? Gleiches Modell, gleicher Split, gleiche 50 Laeufe
  - Vier Spezifikationen auf dem Stufe-2-Poisson-GLM: linear (12 Terme),
    quadrate (22), interaktionen (57), beides (67). Die Saisonterme werden
    weder quadriert noch gekreuzt - monat_sin^2 + monat_cos^2 = 1 waere
    exakt kollinear mit der Konstanten
  - z-Standardisierung je Fold auf den Trainingsdaten ist numerische
    Notwendigkeit, keine Modellentscheidung: Quadrate liegen bei 1e10, die
    IRLS-Iteration bricht sonst zusammen. Mathematisch folgenlos - deshalb
    MUSS `linear` die Stufe-2-Baseline reproduzieren, was _selbsttest()
    prueft und sonst abbricht
  - Nicht konvergierte Anpassungen werden GEZAEHLT und berichtet, nicht
    entfernt: Sie sind Teil des Befundes, dass die Spezifikation nicht passt
  - Kein Modellvorschlag - keine der drei Erweiterungen tritt im
    Verfahrensvergleich an, sie dienen der Interpretation von B-41
  - Das Hold-out bleibt unberuehrt

Ausfuehrliche Fassung: docs/08_FUNKTIONSDOKUMENTATION.md
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

    Ein:  Trainings- und Testrahmen, Name der Spezifikation
    Aus:  (X_train, X_test, Namen), jeweils mit Konstante an Position 0

    - Mittelwert und Streuung stammen nur aus dem Training; der Teststadtteil
      darf die Transformation nicht mitbestimmen
    - Quadrate und Paarprodukte werden erst NACH der Standardisierung gebildet
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
    """Eine Poisson-Anpassung, eine Bewertung auf der Originalskala.

    Ein:  Panel, Wiederholung, Fold, Spezifikation
    Aus:  dict mit RMSE, MAE, R2 und Konvergenzstatus
    """
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
    """10 Wiederholungen x 5 Folds x 4 Spezifikationen = 200 Anpassungen.

    Ein:  Panel, `selten` fuer die Stratifizierung
    Aus:  Datenrahmen mit einer Zeile je Lauf
    """
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

    Ein:  Datenrahmen der 200 Einzellaeufe
    Aus:  je Spezifikation eine Zeile mit Mittel und beiden Streuungen

    - dieselbe Regel wie ueberall sonst (R-5): massgeblich ist die Streuung der
      10 Wiederholungsmittel, nicht die der 50 Einzellaeufe
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
    """Prueft, ob die Spezifikation `linear` die Stufe-2-Baseline reproduziert.

    Ein:  Ergebnisse der Spalte `linear`, baselines_mittel.csv
    Aus:  Abbruch bei Abweichung ueber drei Nachkommastellen

    - weicht sie ab, sieht dieses Skript andere Merkmale oder andere Folds als
      v1_baselines.py
    - dann ist jeder Vergleich in dieser Datei wertlos
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
    """Rechnet alle 200 Anpassungen und schreibt die beiden Ergebnisdateien.

    Ein:  regression.parquet, klassifikation.parquet (nur fuer `selten`)
    Aus:  spezifikation_folds.csv, spezifikation_mittel.csv; Exitcode

    - laeuft einzeln, nicht ueber vorpruefung/run.py
    - die Zahl der nicht konvergierten Anpassungen wird ausgegeben und gehoert in
      den Text
    """
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
