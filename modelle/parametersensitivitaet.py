"""
Wie viel haengt an der Wahl des Hyperparametersatzes? - Kreuzprobe ueber die Folds.

    python modelle/parametersensitivitaet.py            beide Straenge
    python modelle/parametersensitivitaet.py menge      nur Regression
    python modelle/parametersensitivitaet.py struktur   nur Klassifikation
    python modelle/parametersensitivitaet.py spannen    nur die normierten
                                                        Spannen, ohne Kreuzprobe

Input:   data/processed/{regression,klassifikation}.parquet
         results/regression/tuning.csv, results/klassifikation/tuning.csv
         results/regression/menge_folds.csv, results/klassifikation/struktur_folds.csv
         (nur zur Selbstkontrolle der Diagonalen)
Output:  results/parametersensitivitaet/matrix.csv, zusammenfassung.csv,
         spannen.csv, bericht.md

  - die Schlussbewertung nutzt die Hyperparameter EINES Folds
    (`fold_der_parameter`); hier wird jeder Testfold mit jedem der fuenf
    Saetze bewertet (Diagonale = berichtet, 20 Zellen fremde Saetze)
  - Frage: Ist der Abstand eigener/fremder Satz klein gegen den Abstand
    zwischen den Verfahren?
  - `spannen()`: Lage der fuenf Werte im Suchraum (0 = Unter-, 1 = Obergrenze),
    Spanne = groesste minus kleinste Lage
  - nur Kreuzvalidierung auf den 30 Entwicklungsstadtteilen, kein Hold-out

FALLSTRICKE
  1  Hold-out-Sperre vor allem anderen, wie in m02, m03 und v4
  2  nur Wiederholung 0 (nur dort gehoert ein Satz zu einem Fold)
  3  tuning.csv fuehrt die Saetze je Zielgroesse -> vorher filtern
  4  ohne `auch_parallel`; Laufzeiten stammen aus dem Hauptlauf
  5  Diagonale muss die Fold-Werte des Hauptlaufs reproduzieren (geprueft)
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "prep"))
sys.path.insert(0, str(ROOT / "modelle"))
sys.path.insert(0, str(ROOT / "vorpruefung"))

from config import (N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, RESULTS_DIR)
from config_modelle import SUCHRAEUME  # noqa: E402
from s2_datensaetze import ZIELGROESSE, fold_masken  # noqa: E402
from v0_aufteilung import (selten_je_stadtteil,  # noqa: E402
                           wiederholte_aufteilung)

OUT = RESULTS_DIR / "parametersensitivitaet"

# Je Strang: Anzeigename, Zielspalte des Guetemasses, Richtung (hoeher_besser).
STRAENGE = {
    "menge":    {"mass": "RMSE",     "hoeher_besser": False},
    "struktur": {"mass": "macro_f1", "hoeher_besser": True},
}


def parametersaetze(pfad: Path, zielgroesse: str | None) -> dict:
    """Liest die getunten Saetze je Verfahren und Fold aus tuning.csv.

    Input:  Pfad zur tuning.csv, optional die Zielgroesse zum Filtern
    Output: {verfahren: {fold: parameter-dict}}

    - FALLSTRICK 3: im Mengenstrang auf eine Zielgroesse filtern, sonst
      erscheint jeder Satz zweimal
    - gelesen wird `parameter_json`, nicht die aufgefaecherten Spalten: dort
      steht der Satz genau so, wie ihn `set_params` erwartet
    """
    t = pd.read_csv(pfad)
    if zielgroesse is not None:
        t = t[t["zielgroesse"] == zielgroesse]
    saetze: dict = {}
    for _, z in t.iterrows():
        saetze.setdefault(z["verfahren"], {})[int(z["fold"])] = json.loads(
            z["parameter_json"])
    return saetze


def lage_im_suchraum(spez: tuple, wert) -> float | None:
    """Lage eines gewaehlten Wertes in seinem Suchraum.

    Input:  Suchraum-Spezifikation aus config_modelle.py, gewaehlter Wert
    Output: Zahl von 0 (Untergrenze) bis 1 (Obergrenze), oder None

    - loguniform wird logarithmisch umgerechnet, int und uniform linear,
      choice ueber die Position in der Liste
    - `None` bei max_depth heisst unbegrenzte Tiefe und steht am Ende der
      Liste, zaehlt also als tiefster Wert
    - liegt ein Wert nicht im Raum, gibt es None statt einer falschen Lage
    """
    art, *w = spez
    if art == "loguniform":
        a, b = math.log(w[0]), math.log(w[1])
        return (math.log(wert) - a) / (b - a)
    if art in ("int", "uniform"):
        return (wert - w[0]) / (w[1] - w[0])
    if art == "choice":
        liste = list(w[0])
        return liste.index(wert) / (len(liste) - 1) if wert in liste else None
    return None


def spannen() -> pd.DataFrame:
    """Wie weit liegen die fuenf gewaehlten Werte je Hyperparameter auseinander?

    Input:  tuning.csv beider Straenge, Suchraeume aus config_modelle.py
    Output: eine Zeile je Strang, Verfahren und Hyperparameter

    - normierte Spanne = groesste minus kleinste Lage ueber die fuenf Folds:
      0 heisst, alle Folds waehlen denselben Wert, 1 heisst, die Wahl reicht
      von Rand zu Rand
    - im Mengenstrang steht jeder Satz je Zielgroesse in tuning.csv, gezaehlt
      wird er nur einmal
    """
    quellen = (("menge", RESULTS_DIR / "regression" / "tuning.csv", ZIELGROESSE),
               ("struktur", RESULTS_DIR / "klassifikation" / "tuning.csv", None))
    zeilen = []
    for strang, pfad, ziel in quellen:
        for name, je_fold in parametersaetze(pfad, ziel).items():
            werte: dict = {}
            for k in sorted(je_fold):
                for schluessel, wert in je_fold[k].items():
                    werte.setdefault(schluessel.split("__")[-1], []).append(wert)
            for parameter, liste in werte.items():
                spez = SUCHRAEUME[name].get(parameter)
                if spez is None:
                    continue
                lagen = [lage_im_suchraum(spez, w) for w in liste]
                lagen = [x for x in lagen if x is not None]
                if not lagen:
                    continue
                zeilen.append({
                    "strang": strang, "verfahren": name, "parameter": parameter,
                    "gewaehlt": "; ".join(str(w) for w in liste),
                    "lage_min": round(min(lagen), 3),
                    "lage_max": round(max(lagen), 3),
                    "spanne": round(max(lagen) - min(lagen), 3),
                })
    return pd.DataFrame(zeilen).sort_values("spanne", ignore_index=True)


def kreuzprobe(strang: str) -> pd.DataFrame:
    """Bewertet jeden Testfold mit jedem Parametersatz.

    Input:  "menge" oder "struktur"
    Output: Datenrahmen mit N_FOLDS x N_FOLDS Zeilen je Verfahren

    - FALLSTRICK 1: Hold-out-Sperre in der ersten Zeile nach dem Einlesen
    - FALLSTRICK 2: ausschliesslich Wiederholung 0
    - FALLSTRICK 4: `auch_parallel` bleibt aus
    """
    if strang == "menge":
        import m02_menge as m
        voll = pd.read_parquet(PFAD_REGRESSION)
        saetze = parametersaetze(RESULTS_DIR / "regression" / "tuning.csv",
                                 ZIELGROESSE)
        mass = "RMSE"
    else:
        import m03_struktur as m
        voll = pd.read_parquet(PFAD_KLASSIFIKATION)
        saetze = parametersaetze(RESULTS_DIR / "klassifikation" / "tuning.csv",
                                 None)
        mass = "macro_f1"

    # FALLSTRICK 1 zuerst. reset_index wie in m02/m03: die Zeilenreihenfolge
    # bestimmt den Bootstrap von RF/XGBoost, sonst weicht die Diagonale ab.
    panel = voll[voll["ist_holdout"] == 0].reset_index(drop=True)

    selten = selten_je_stadtteil(pd.read_parquet(PFAD_KLASSIFIKATION))
    d = wiederholte_aufteilung(panel, wiederholung=0, selten=selten)

    zeilen = []
    for name, je_fold in saetze.items():
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            for j, parameter in sorted(je_fold.items()):
                if strang == "menge":
                    e = m.ein_lauf(name, parameter, train, test, ZIELGROESSE,
                                   auch_parallel=False)
                else:
                    e = m.ein_lauf(name, parameter, train, test,
                                   auch_parallel=False)
                zeilen.append({
                    "strang": strang, "verfahren": name,
                    "fold_test": k, "fold_der_parameter": j,
                    "eigener_satz": int(j == k), mass: e[mass],
                })
            print(f"  {strang} · {name} · Testfold {k} fertig")
    return pd.DataFrame(zeilen)


def zusammenfassung(matrix: pd.DataFrame, strang: str) -> pd.DataFrame:
    """Eigener gegen fremde Parametersaetze, je Verfahren.

    Input:  Ergebnismatrix, Strang
    Output: eine Zeile je Verfahren

    - `verschlechterung` ist immer positiv = fremder Satz ist schlechter
    - `anteil_fremd_besser` zeigt, wie oft der eigene Satz gar nicht der
      beste war - ein hoher Wert heisst, dass die Wahl Rauschen folgt
    """
    mass = STRAENGE[strang]["mass"]
    hoch = STRAENGE[strang]["hoeher_besser"]
    zeilen = []
    for name, g in matrix.groupby("verfahren"):
        eigen = g[g["eigener_satz"] == 1][mass]
        fremd = g[g["eigener_satz"] == 0][mass]
        diff = (eigen.mean() - fremd.mean()) if hoch else (fremd.mean() - eigen.mean())
        besser = 0
        for k, gk in g.groupby("fold_test"):
            e = gk[gk["eigener_satz"] == 1][mass].iat[0]
            f = gk[gk["eigener_satz"] == 0][mass]
            besser += int((f > e).sum() if hoch else (f < e).sum())
        zeilen.append({
            "strang": strang, "verfahren": name, "mass": mass,
            "eigener_satz_mittel": round(eigen.mean(), 4),
            "fremder_satz_mittel": round(fremd.mean(), 4),
            "verschlechterung": round(diff, 4),
            "fremd_spanne": f"{fremd.min():.4f}-{fremd.max():.4f}",
            "fremd_besser_von_20": besser,
        })
    return pd.DataFrame(zeilen)


def _kontrolle(matrix: pd.DataFrame, strang: str) -> None:
    """Kontrolle: Diagonale gegen den Hauptlauf.

    - weicht sie ab, hat sich Aufteilung oder Spezifikation veraendert; dann
      ist nicht diese Datei zu korrigieren, sondern die Ursache zu suchen
    """

    mass = STRAENGE[strang]["mass"]
    quelle = (RESULTS_DIR / "regression" / "menge_folds.csv" if strang == "menge"
              else RESULTS_DIR / "klassifikation" / "struktur_folds.csv")
    if not quelle.exists():
        print(f"  HINWEIS: {quelle.name} fehlt - Diagonale nicht geprueft.")
        return
    h = pd.read_csv(quelle)
    h = h[h["wiederholung"] == 0]
    if strang == "menge":
        h = h[h["zielgroesse"] == ZIELGROESSE]
    diag = matrix[matrix["eigener_satz"] == 1]
    for _, z in diag.iterrows():
        t = h[(h["verfahren"] == z["verfahren"]) & (h["fold"] == z["fold_test"])]
        if t.empty:
            continue
        if abs(float(t[mass].iat[0]) - float(z[mass])) > 1e-6:
            print(f"  WARNUNG: Diagonale weicht ab - {z['verfahren']} "
                  f"Fold {z['fold_test']}: {z[mass]:.6f} gegen "
                  f"{float(t[mass].iat[0]):.6f} im Hauptlauf.")


def bericht(teile: list[tuple[pd.DataFrame, pd.DataFrame, str]],
            sp: pd.DataFrame) -> str:
    """Setzt die Zusammenfassungen zu bericht.md zusammen. Reine Formatierung."""
    def md(df):
        kopf = "| " + " | ".join(df.columns) + " |"
        linie = "|" + "|".join(["---"] * len(df.columns)) + "|"
        return "\n".join([kopf, linie] + ["| " + " | ".join(str(v) for v in r)
                                          + " |" for r in df.itertuples(index=False)])
    z = ["# Sensitivitaet gegenueber der Wahl des Parametersatzes", "",
         "Erzeugt von `modelle/parametersensitivitaet.py`. Wiederholung 0,",
         "30 Entwicklungsstadtteile, Hold-out unberuehrt.", "",
         "Jeder Testfold wurde mit jedem der fuenf getunten Parametersaetze",
         "bewertet. Die Diagonale ist die berichtete Konfiguration.", ""]
    for _, s, strang in teile:
        z += [f"## Strang: {strang}", "", md(s), ""]
    z += ["## Normierte Spannen der gewaehlten Werte", "",
          "Lage jedes gewaehlten Wertes im Suchraum, 0 = Untergrenze, 1 =",
          "Obergrenze. Spanne = groesste minus kleinste Lage ueber die Folds.",
          "", md(sp[["strang", "verfahren", "parameter", "spanne"]]), ""]
    return "\n".join(z)


def main(argv: list[str]) -> int:
    """Rechnet Spannen und Kreuzprobe und schreibt vier Dateien."""
    gewuenscht = [a for a in argv if a in STRAENGE] or list(STRAENGE)
    OUT.mkdir(parents=True, exist_ok=True)

    sp = spannen()
    sp.to_csv(OUT / "spannen.csv", index=False)
    print("\nNormierte Spannen der gewaehlten Werte")
    print(sp[["strang", "verfahren", "parameter", "spanne"]].to_string(index=False))
    if "spannen" in argv:
        print("\n  Geschrieben: results/parametersensitivitaet/spannen.csv")
        return 0

    teile, matrizen = [], []
    for strang in gewuenscht:
        print(f"\n{strang.upper()} - {N_FOLDS} Testfolds x {N_FOLDS} Parametersaetze")
        matrix = kreuzprobe(strang)
        _kontrolle(matrix, strang)
        s = zusammenfassung(matrix, strang)
        print()
        print(s.to_string(index=False))
        matrizen.append(matrix)
        teile.append((matrix, s, strang))

    pd.concat(matrizen).to_csv(OUT / "matrix.csv", index=False)
    pd.concat([s for _, s, _ in teile]).to_csv(OUT / "zusammenfassung.csv",
                                               index=False)
    (OUT / "bericht.md").write_text(bericht(teile, sp), encoding="utf-8")

    # Kontrolle: je Verfahren N_FOLDS x N_FOLDS Zeilen
    alle = pd.concat(matrizen)
    for (strang, name), g in alle.groupby(["strang", "verfahren"]):
        if len(g) != N_FOLDS * N_FOLDS:
            print(f"  WARNUNG: {strang}/{name} hat {len(g)} Zeilen, "
                  f"erwartet {N_FOLDS * N_FOLDS}.")

    print(f"\n  Geschrieben: results/parametersensitivitaet/matrix.csv, "
          f"zusammenfassung.csv, spannen.csv, bericht.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
