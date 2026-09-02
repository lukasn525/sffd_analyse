"""
Wie viel haengt an der Wahl des Hyperparametersatzes? - Kreuzprobe ueber die Folds.

    python tools/parametersensitivitaet.py            beide Straenge
    python tools/parametersensitivitaet.py menge      nur Regression
    python tools/parametersensitivitaet.py struktur   nur Klassifikation

Eingang: data/processed/{regression,klassifikation}.parquet
         results/regression/tuning.csv, results/klassifikation/tuning.csv
         results/regression/menge_folds.csv, results/klassifikation/struktur_folds.csv
         (nur zur Selbstkontrolle der Diagonalen)
Ausgang: results/parametersensitivitaet/matrix.csv, zusammenfassung.csv,
         bericht.md

  - Die Schlussbewertung laesst die Baumverfahren mit den Hyperparametern
    EINES Folds antreten (`fold_der_parameter`), obwohl die Saetze ueber die
    Folds erheblich streuen - beim Random Forest der Struktur `max_depth`
    16/24/16/24/24, `n_estimators` 539/359/306/321/995. Die Baselines haben
    keine Hyperparameter und sind von dieser Asymmetrie nicht betroffen
  - Bislang war unbekannt, wie viel das ausmacht (R-4, B-14). Dieses Skript
    misst es: Jeder Testfold wird mit JEDEM der fuenf Parametersaetze bewertet.
    Die Diagonale ist die berichtete Konfiguration, die uebrigen 20 Zellen
    sind fremde Saetze
  - Die Frage, die damit beantwortet wird: Ist der Abstand zwischen eigenem
    und fremdem Parametersatz klein gegen den Abstand zwischen den Verfahren
    (2,2 RMSE bzw. 0,004 Macro-F1)? Dann traegt die Schlussbewertung. Ist er
    gross, ist sie zu einem erheblichen Teil eine Parameterlotterie
  - Beruehrt das Hold-out NICHT. Die Gegenprobe laeuft vollstaendig innerhalb
    der Kreuzvalidierung auf den 30 Entwicklungsstadtteilen - genau so in R-4
    entworfen. Es entsteht keine zweite Schlussbewertung

FALLSTRICKE
  1  Hold-out-Sperre steht vor allem anderen, wie in m02, m03 und v4. Dieses
     Skript hat keinen Schalter, der sie loest - es gibt hier nichts zu sehen
  2  Nur Wiederholung 0. Dort wurde getunt (#43-Rahmen, phase_tuning), also
     ist nur dort definiert, welcher Satz zu welchem Fold gehoert. In
     Wiederholung 1 bis 9 ist die Fold-Zusammensetzung eine andere, ein
     "Satz des Folds 3" existiert dort nicht
  3  Die Parametersaetze gelten fuer BEIDE Zielgroessen der Menge: getunt
     wurde einmal auf der Rate (#43). `tuning.csv` fuehrt sie trotzdem je
     Zielgroesse - vor dem Einlesen auf eine Zielgroesse filtern, sonst
     zaehlt jeder Satz doppelt
  4  Gemessen wird ohne `auch_parallel`. Die Laufzeiten aus Unterfrage 3
     stammen aus dem Hauptlauf und werden hier nicht neu erhoben; ein
     zweiter Zeitwert aus einem anderen Lauf waere eine zweite Wahrheit
  5  Die Diagonale MUSS die Fold-Werte des Hauptlaufs reproduzieren. Weicht
     sie ab, ist entweder die Aufteilung oder die Spezifikation seit dem
     16.08. veraendert worden - das Skript prueft es und warnt

Ausfuehrliche Fassung: docs/08_FUNKTIONSDOKUMENTATION.md
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "prep"))
sys.path.insert(0, str(ROOT / "modelle"))
sys.path.insert(0, str(ROOT / "vorpruefung"))

from config import (N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, RESULTS_DIR)
from s2_datensaetze import ZIELGROESSE, fold_masken  # noqa: E402
from v0_aufteilung import (selten_je_stadtteil,  # noqa: E402
                           wiederholte_aufteilung)

OUT = RESULTS_DIR / "parametersensitivitaet"

# Je Strang: Anzeigename, Zielspalte des Gueterasses, Richtung.
# hoeher_besser steuert nur das Vorzeichen der Verschlechterung im Bericht.
STRAENGE = {
    "menge":    {"mass": "RMSE",     "hoeher_besser": False},
    "struktur": {"mass": "macro_f1", "hoeher_besser": True},
}


def parametersaetze(pfad: Path, zielgroesse: str | None) -> dict:
    """Liest die getunten Saetze je Verfahren und Fold aus tuning.csv.

    Ein:  Pfad zur tuning.csv, optional die Zielgroesse zum Filtern
    Aus:  {verfahren: {fold: parameter-dict}}

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


def kreuzprobe(strang: str) -> pd.DataFrame:
    """Bewertet jeden Testfold mit jedem Parametersatz.

    Ein:  "menge" oder "struktur"
    Aus:  Datenrahmen mit N_FOLDS x N_FOLDS Zeilen je Verfahren

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

    # FALLSTRICK 1 - vor allem anderen. reset_index wie in m02/m03.main:
    # Die Zeilenreihenfolge ist ein Reproduzierbarkeitsvertrag (Bootstrap von
    # RF und XGBoost laeuft ueber Zeilenpositionen); ohne sie weicht die
    # Diagonale vom Hauptlauf ab.
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

    Ein:  Ergebnismatrix, Strang
    Aus:  eine Zeile je Verfahren

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
    """PRUEFAUFTRAG 3: Diagonale gegen den Hauptlauf.

    - weicht sie ab, hat sich Aufteilung oder Spezifikation seit dem
      16.08.2026 veraendert; dann ist nicht diese Datei zu korrigieren,
      sondern die Ursache zu suchen
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


def bericht(teile: list[tuple[pd.DataFrame, pd.DataFrame, str]]) -> str:
    """Setzt die Zusammenfassungen zu bericht.md zusammen. Reine Formatierung."""
    def md(df):
        kopf = "| " + " | ".join(df.columns) + " |"
        linie = "|" + "|".join(["---"] * len(df.columns)) + "|"
        return "\n".join([kopf, linie] + ["| " + " | ".join(str(v) for v in r)
                                          + " |" for r in df.itertuples(index=False)])
    z = ["# Sensitivitaet gegenueber der Wahl des Parametersatzes", "",
         "Erzeugt von `tools/parametersensitivitaet.py`. Wiederholung 0,",
         "30 Entwicklungsstadtteile, Hold-out unberuehrt.", "",
         "Jeder Testfold wurde mit jedem der fuenf getunten Parametersaetze",
         "bewertet. Die Diagonale ist die berichtete Konfiguration.", ""]
    for _, s, strang in teile:
        z += [f"## Strang: {strang}", "", md(s), ""]
    return "\n".join(z)


def main(argv: list[str]) -> int:
    """Rechnet die Kreuzprobe und schreibt drei Dateien."""
    gewuenscht = [a for a in argv if a in STRAENGE] or list(STRAENGE)
    OUT.mkdir(parents=True, exist_ok=True)

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
    (OUT / "bericht.md").write_text(bericht(teile), encoding="utf-8")

    # PRUEFAUFTRAEGE
    #   1  je Verfahren N_FOLDS x N_FOLDS Zeilen
    #   2  keine Zeile aus dem Hold-out (konstruktiv, hier nur bestaetigt)
    #   3  Diagonale reproduziert den Hauptlauf (siehe _kontrolle)
    alle = pd.concat(matrizen)
    for (strang, name), g in alle.groupby(["strang", "verfahren"]):
        if len(g) != N_FOLDS * N_FOLDS:
            print(f"  WARNUNG: {strang}/{name} hat {len(g)} Zeilen, "
                  f"erwartet {N_FOLDS * N_FOLDS}.")

    print(f"\n  Geschrieben: results/parametersensitivitaet/matrix.csv, "
          f"zusammenfassung.csv, bericht.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
