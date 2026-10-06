"""
Interpretation: Welche Merkmale tragen die Vorhersage?

    python modelle/m04_shap.py
    python modelle/m04_shap.py --ohne-baeume    Ablation nur fuer GLM und Logit

Input:   results/regression/{menge_folds,vergleich,tuning}.csv
         results/regression/menge_vorhersagen.parquet
         results/klassifikation/{struktur_folds,vergleich,tuning}.csv
         data/processed/{regression,klassifikation}.parquet
Output:  results/shap/beitraege.csv, gruppen.csv, uebersprungen.csv,
         faktorgruppen_menge.csv, vif.csv, extrapolation_*.csv,
         ablation_exposition.csv, ablation_faktorgruppen{,_mittel}.csv,
         verschiebung.csv

  - Attribution (Anteil der SHAP- bzw. Koeffizientenmasse je Faktorgruppe)
    und Ablation (Verlust beim Weglassen einer Gruppe = was sie wert ist)
  - SHAP nur fuer Modelle, die ihre Stufe-2-Baseline im Primaertest
    schlagen; Uebersprungenes mit Grund in uebersprungen.csv
  - ein Fold: geringster Extrapolationsanteil in Wiederholung 0
  - TreeExplainer fuer RF/XGBoost, standardisierte Koeffizienten fuer Ridge
    und GLM
  - FALLSTRICK: korrelierte Merkmale -> blockweise in Faktorgruppen lesen
  - VIF hier (betrifft nur die Interpretation), auf den eindeutigen
    Stadtteil-Jahr-Kombinationen
  - setzt m02 und m03 voraus
"""


import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "prep"))
sys.path.insert(0, str(_ROOT / "vorpruefung"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from config_modelle import WIEDERHOLUNGEN  # noqa: E402
from s2_datensaetze import RATE, ZIELGROESSE, ZIELKLASSE, fold_masken  # noqa: E402
from v0_aufteilung import (selten_je_stadtteil,  # noqa: E402
                           wiederholte_aufteilung)

OUT = RESULTS_DIR / "shap"
MERKMALE = PRAEDIKTOREN + SAISON

# Die drei Faktorgruppen, plus zwei getrennt gefuehrte Groessen.
GRUPPEN = {
    "soziooekonomisch": ["median_haushaltseinkommen", "armutsquote_pct",
                         "akademikerquote_pct", "median_miete",
                         "leerstandsquote_pct"],
    "kriminalitaetsbezogen": ["log_kriminalitaetsindex"],
    "baulich": ["anteil_altbau_vor_1940_pct", "anteil_wohngebaeude_pct",
                "anteil_risikogewerbe_pct"],
    "groessenkontrolle": ["log_bevoelkerung"],
    "saison": SAISON,
}


def schlagen_die_latte(vergleich: pd.DataFrame) -> tuple[list, pd.DataFrame]:
    """Welche (Zielgroesse, Verfahren) schlagen ihre Stufe-2-Baseline?

    Input:  vergleich.csv beider Straenge
    Output: Menge der zugelassenen Kombinationen

    - Grundlage ist der Primaertest auf den Wiederholungsmitteln
    - verlangt werden beide Bedingungen: mittlere Differenz zugunsten des
      Verfahrens UND signifikanter Test
    - ein positiver Mittelwert allein reicht nicht
    """
    p = vergleich[(vergleich["rolle"] == "primaer")
                  & (vergleich["teststufe"] == "wiederholung")].copy()
    p["verfahren"] = p["paarung"].str.split(" vs ").str[0]
    p["schlaegt"] = (p["differenz_mittel"] > 0) & p["signifikant"]
    genommen = [(z["zielgroesse"], z["verfahren"])
                for _, z in p[p["schlaegt"]].iterrows()]
    verworfen = p[~p["schlaegt"]][["zielgroesse", "verfahren",
                                   "differenz_mittel", "wilcoxon_p"]].copy()
    verworfen["grund"] = np.where(
        verworfen["differenz_mittel"] <= 0,
        "schlaegt die Stufe-2-Baseline im Mittel nicht",
        "Vorsprung nicht signifikant (alpha = 0,05)")
    return genommen, verworfen


def ruhigster_fold(folds: pd.DataFrame) -> int:
    """Fold mit dem geringsten Extrapolationsanteil in Wiederholung 0.

    Input:  Laufdatei des Strangs
    Output: Foldnummer
    """
    w0 = folds[folds["wiederholung"] == 0]
    je_fold = w0.groupby("fold")["extrapolationsanteil"].first()
    return int(je_fold.idxmin())


def _beitraege(modell, X: pd.DataFrame, name: str) -> np.ndarray:
    """Mittlerer absoluter Beitrag je Merkmal: SHAP oder Koeffizient.

    Input:  gefittetes Modell, Merkmalsmatrix, Verfahrensname
    Output: Reihe Merkmal -> Beitrag

    - Ridge und GLM: standardisierte Koeffizienten, direkter Gegenwert zu
      SHAP-Beitraegen, kein Explainer noetig
    - Baumverfahren: TreeExplainer, exakt statt approximiert
    - mehrklassige Ausgaben werden ueber die Klassen gemittelt; die Frage lautet
      "welche Faktorgruppe traegt", nicht "fuer welche Klasse"
    - XGBoost geht einen eigenen Weg: shap.TreeExplainer kann den mehrklassigen
      base_score von XGBoost 3.x nicht lesen und bricht ab. XGBoost bringt
      TreeSHAP selbst mit; pred_contribs=True liefert dieselben Werte vom selben
      Algorithmus
    """
    if name == "ridge":
        return np.abs(modell[-1].regressor_.coef_).ravel()

    if name == "xgboost":
        import xgboost as xgb
        roh = modell.get_booster().predict(
            xgb.DMatrix(X, feature_names=list(X.columns)), pred_contribs=True)
        werte = np.abs(np.asarray(roh))[..., :-1]     # letzte Spalte = Bias
    else:
        import shap
        werte = shap.TreeExplainer(modell).shap_values(X)
        if isinstance(werte, list):                   # aeltere shap-Fassungen
            werte = np.stack(werte, axis=-1)
        werte = np.abs(np.asarray(werte))

    if werte.ndim == 3:            # (n, klassen, p) oder (n, p, klassen)
        achse = 1 if werte.shape[1] != len(X.columns) else 2
        werte = werte.mean(axis=achse)
    return werte.mean(axis=0)


def extrapolation_aufschluesseln(panel: pd.DataFrame, selten: pd.Series,
                                 folds: pd.DataFrame) -> tuple:
    """Schluesselt den Extrapolationsanteil nach Merkmal und Stadtteil auf.

    Input:  Panel, menge_folds.csv
    Output: extrapolation_merkmale.csv, _stadtteile.csv, _zusammenhang.csv

    - macht aus einer Plausibilitaetsaussage eine Zahl: erklaert die Spanne
      der Extrapolationsanteile einen erheblichen Teil der Fold-Streuung?
    - drei Auswertungen: je Merkmal (wie oft liegt es allein ausserhalb), je
      Stadtteil (wie stark bricht er aus), je Verfahren (Spearman zwischen
      Extrapolationsanteil und Fehler ueber alle 50 Laeufe)
    - Abgrenzung: Verboten waere, die Testmenge nach Extrapolationsgrad zu
      schneiden und darin nach Verfahrensunterschieden zu suchen. Hier bleibt die
      Einheit der Fold, die Primaeraussage bleibt unberuehrt
    """

    from scipy.stats import spearmanr

    d = wiederholte_aufteilung(panel, wiederholung=0, selten=selten)

    je_merkmal, je_stadtteil = [], []
    for k in range(1, 6):
        tr, te = fold_masken(d, k)
        train, test = d[tr], d[te]
        lo, hi = train[MERKMALE].min(), train[MERKMALE].max()
        aussen = (test[MERKMALE] < lo) | (test[MERKMALE] > hi)
        for merkmal in MERKMALE:
            je_merkmal.append({"fold": k, "merkmal": merkmal,
                               "anteil": float(aussen[merkmal].mean())})
        st = aussen.any(axis=1).groupby(test["stadtteil"]).mean()
        je_stadtteil += [{"fold": k, "stadtteil": s, "anteil": float(a)}
                         for s, a in st.items()]

    merkmale = (pd.DataFrame(je_merkmal).groupby("merkmal")["anteil"].mean()
                  .sort_values(ascending=False).rename("anteil_testzeilen")
                  .reset_index())
    stadtteile = (pd.DataFrame(je_stadtteil).sort_values("anteil", ascending=False)
                    .reset_index(drop=True))

    # Zusammenhang Extrapolation <-> Fehler, je Verfahren und Zielgroesse.
    zeilen = []
    for (ziel, name), g in folds.groupby(["zielgroesse", "verfahren"], sort=False):
        rho, p = spearmanr(g["extrapolationsanteil"], g["RMSE"])
        zeilen.append({"zielgroesse": ziel, "verfahren": name,
                       "spearman_rho": round(float(rho), 3),
                       "p_wert": round(float(p), 5), "n_laeufe": len(g)})
    zusammenhang = pd.DataFrame(zeilen)
    return merkmale, stadtteile, zusammenhang


def ablation_exposition(panel: pd.DataFrame, selten: pd.Series,
                        parameter: pd.DataFrame) -> pd.DataFrame:
    """Ablation: Was leistet die Expositionsbehandlung?

    Input:  Panel, tuning.csv des Mengenstrangs
    Output: ablation_exposition.csv

    - der Hauptlauf modelliert die Rate und multipliziert zurueck; diese
      Ablation laesst die Baumverfahren direkt auf anzahl_einsaetze anpassen
    - alles andere bleibt gleich: dieselben Folds, Merkmale und Hyperparameter
    - die Hyperparameter werden bewusst nicht neu gesucht, sonst aenderten sich
      zwei Dinge gleichzeitig
    - Vermutung dahinter: Baeume koennen "Einsaetze = Bevoelkerung x Risiko" nicht
      nachbauen, weil sie je Blatt einen festen Wert ausgeben; RMSE auf der
      Originalskala wird von den grossen Stadtteilen dominiert
    - fuer ein Modell mit Log-Verknuepfung und freiem
      Koeffizienten auf log_bevoelkerung ist der Offset redundant, fuer einen Baum
      ohne beides nicht
    """

    import m02_menge as m02
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    param = m02._parameter_je_fold(parameter)
    baeume = [v for v in m02.VERFAHREN if v != "ridge"]
    zeilen = []

    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(panel, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            y = test[ZIELGROESSE].astype(float).to_numpy()
            for name in baeume:
                # Ohne Exposition direkt auf der Anzahl (Hauptlauf: Rate x
                # Bevoelkerung); die Differenz ist der Effekt.
                modell = m02.verfahren(name).set_params(
                    **param[(ZIELGROESSE, name, k)])
                modell.fit(train[MERKMALE].astype(float),
                           train[ZIELGROESSE].astype(float))
                anzahl = modell.predict(test[MERKMALE].astype(float))
                zeilen.append({
                    "wiederholung": w, "fold": k, "verfahren": name,
                    "spezifikation": "ohne_exposition",
                    "RMSE": float(np.sqrt(mean_squared_error(y, anzahl))),
                    "MAE": float(mean_absolute_error(y, anzahl)),
                    "R2": float(r2_score(y, anzahl))})
    return pd.DataFrame(zeilen)


def ablation_faktorgruppen(reg: pd.DataFrame, kl: pd.DataFrame,
                           selten: pd.Series, tuning_kl: pd.DataFrame | None,
                           mit_baeumen: bool = True) -> pd.DataFrame:
    """Ablation: Was ist eine Faktorgruppe wert?

    Input:  beide Panels, tuning.csv beider Straenge, Schalter --ohne-baeume
    Output: ablation_faktorgruppen.csv mit den Einzellaeufen

    - Attribution sagt, wie ein Modell seine Aufmerksamkeit verteilt, nicht was
      eine Gruppe wert ist: Ein Merkmal kann viel Masse binden und ersetzbar sein
    - die Ablation misst das Fehlende direkt: jede Gruppe wird einmal
      weggelassen, alles andere bleibt gleich
    - Mengenstrang: abladiert wird das Poisson-GLM, weil kein Vergleichsverfahren
      es schlaegt und weil es keinen Hyperparameter hat - dann aendert sich
      ausschliesslich die Merkmalsmenge
    - Strukturstrang: abladiert werden die Baumverfahren aus m03; das Logit
      laeuft zum Vergleich mit
    - Einschraenkung: Bei den Baumverfahren stammen die
      Hyperparameter aus dem vollen Merkmalssatz. Die gemessene Verschlechterung
      enthaelt einen Anteil aus einer nicht mehr passenden Einstellung
    - kein Signifikanztest: Die Testfamilien sind festgelegt, weitere
      Tests muessten in Holm eingehen. Die Ablation ist deskriptiv
    - der Offset des Poisson-GLM bleibt in jeder Variante bestehen; weggelassen
      wird nur der Praediktor log_bevoelkerung
    """

    from sklearn.metrics import f1_score

    import m03_struktur as m03
    from v1_baselines import bewerte_regression, logit_glm, poisson_glm

    varianten = ["voll"] + list(GRUPPEN)
    zeilen = []

    # ---- Mengenstrang: Poisson-GLM -------------------------------------
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(reg, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            y = test[ZIELGROESSE].astype(float).to_numpy()
            for variante in varianten:
                weg = GRUPPEN.get(variante, [])
                merkmale = [m for m in MERKMALE if m not in weg]
                y_hat = poisson_glm(train, test, merkmale)
                zeilen.append({"strang": "menge", "verfahren": "Poisson-GLM",
                               "weggelassen": variante, "n_merkmale": len(merkmale),
                               "wiederholung": w, "fold": k,
                               "mass": "RMSE",
                               "wert": bewerte_regression(y, y_hat)["RMSE"]})
        print(f"    Menge, Wiederholung {w}: {len(zeilen):>4} Anpassungen")

    # ---- Strukturstrang -------------------------------------------------
    param = ({(z["verfahren"], int(z["fold"])): json.loads(z["parameter_json"])
              for _, z in tuning_kl.iterrows()} if tuning_kl is not None else {})
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(kl, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            y_te = m03.kodiere(test[ZIELKLASSE])
            for variante in varianten:
                weg = GRUPPEN.get(variante, [])
                merkmale = [m for m in MERKMALE if m not in weg]

                logit = logit_glm(train, merkmale)
                zeilen.append({
                    "strang": "struktur", "verfahren": "Logit",
                    "weggelassen": variante, "n_merkmale": len(merkmale),
                    "wiederholung": w, "fold": k, "mass": "macro_f1",
                    "wert": float(f1_score(
                        test[ZIELKLASSE], logit.predict(test[merkmale].astype(float)),
                        average="macro", zero_division=0))})

                if not mit_baeumen:
                    continue
                y_tr = m03.kodiere(train[ZIELKLASSE])
                for name in m03.VERFAHREN:
                    modell = m03.verfahren(name).set_params(**param[(name, k)])
                    X_tr = train[merkmale].astype(float)
                    if name == "xgboost":
                        modell.fit(X_tr, y_tr, sample_weight=m03._gewichte(y_tr))
                    else:
                        modell.fit(X_tr, y_tr)
                    y_hat = modell.predict(test[merkmale].astype(float))
                    zeilen.append({
                        "strang": "struktur", "verfahren": name,
                        "weggelassen": variante, "n_merkmale": len(merkmale),
                        "wiederholung": w, "fold": k, "mass": "macro_f1",
                        "wert": float(f1_score(y_te, y_hat, average="macro",
                                               zero_division=0))})
        print(f"    Struktur, Wiederholung {w}: {len(zeilen):>4} Anpassungen")

    return pd.DataFrame(zeilen)


def _ablation_auswerten(roh: pd.DataFrame) -> pd.DataFrame:
    """Verschlechterung je Gruppe gegenueber dem vollen Merkmalssatz.

    Input:  Einzellaeufe aus ablation_faktorgruppen()
    Output: ablation_faktorgruppen_mittel.csv

    - gepaart je Lauf: Variante und voller Satz laufen auf demselben Fold
      derselben Wiederholung
    - zweistufig gemittelt, weil die 50 Laeufe nicht unabhaengig sind
    - das Vorzeichen ist so gedreht, dass ein positiver Wert immer
      "Verschlechterung durch Weglassen" heisst - bei RMSE ist klein besser, bei
      Macro-F1 gross
    """
    voll = (roh[roh["weggelassen"] == "voll"]
            .set_index(["strang", "verfahren", "wiederholung", "fold"])["wert"])
    d = roh[roh["weggelassen"] != "voll"].copy()
    d["voll"] = voll.reindex(
        pd.MultiIndex.from_frame(
            d[["strang", "verfahren", "wiederholung", "fold"]])).to_numpy()
    # RMSE: Verschlechterung = variante - voll. Macro-F1: umgekehrt.
    schlechter = np.where(d["mass"] == "RMSE", 1.0, -1.0)
    d["verschlechterung"] = (d["wert"] - d["voll"]) * schlechter

    schluessel = ["strang", "verfahren", "weggelassen", "mass"]
    je_wdh = d.groupby(schluessel + ["wiederholung"], sort=False)[
        "verschlechterung"].mean()
    aus = (je_wdh.groupby(schluessel, sort=False)
           .agg(verschlechterung_mittel="mean",
                std_wiederholungen="std",
                wdh_mit_verschlechterung=lambda s: int((s > 0).sum()))
           .reset_index())
    aus["n_merkmale_weg"] = aus["weggelassen"].map(
        {g: len(m) for g, m in GRUPPEN.items()})
    return aus.sort_values(["strang", "verfahren", "verschlechterung_mittel"],
                           ascending=[True, True, False]).round(4)


def faktorgruppen_baseline(panel: pd.DataFrame, selten: pd.Series,
                         fold: int) -> pd.DataFrame:
    """Beitrag der drei Faktorgruppen im Mengenstrang, aus der Baseline.

    Input:  Panel, ruhigster Fold
    Output: faktorgruppen_menge.csv

    - fuer die Struktur liefert SHAP den Beitrag, fuer die Menge nicht: m04
      ueberspringt dort alle Modelle, weil keines seine Baseline schlaegt
    - Beitraege eines unterlegenen Modells auszuweisen hiesse, Rauschen zu
      erklaeren
    - das beste Modell des Mengenstrangs ist das Poisson-GLM; seine Koeffizienten
      zeigen den Beitrag der Faktorgruppen direkt
    - vergleichbar gemacht ueber |Koeffizient| x Standardabweichung des Merkmals;
      sonst haengt die Groesse an der Einheit (Einkommen in Dollar bekaeme einen
      winzigen Koeffizienten)
    - gerechnet auf demselben Fold wie die SHAP-Werte
    """
    import statsmodels.api as sm

    d = wiederholte_aufteilung(panel, wiederholung=0, selten=selten)
    tr, _ = fold_masken(d, fold)
    train = d[tr]

    X = train[MERKMALE].astype(float)
    X_std = sm.add_constant((X - X.mean()) / X.std(), has_constant="add")
    y = train[ZIELGROESSE].astype(float)
    offset = np.log(train[EXPOSURE_ROH].astype(float))

    modell = sm.GLM(y, X_std, family=sm.families.Poisson(), offset=offset).fit()

    zu_gruppe = {m: g for g, ms in GRUPPEN.items() for m in ms}
    roh = modell.params.drop("const").abs()
    anteil = roh / roh.sum()
    df = pd.DataFrame({
        "merkmal": roh.index,
        "koeffizient": modell.params.drop("const").to_numpy(),
        "beitrag": roh.to_numpy(),
        "anteil": anteil.to_numpy(),
        "p_wert": modell.pvalues.drop("const").to_numpy(),
    })
    df["gruppe"] = df["merkmal"].map(zu_gruppe)
    df["fold"] = fold
    return df.sort_values("anteil", ascending=False).reset_index(drop=True)


def _vif(panel: pd.DataFrame) -> pd.DataFrame:
    """VIF auf zwei Bezugsmengen.

    Input:  Panel mit allen Praediktoren
    Output: vif.csv, je ein Wert je Merkmal und Bezugsmenge

    - Absicht: jede Merkmalskombination nur einmal zaehlen. Die Strukturmerkmale
      sind innerhalb eines Jahres konstant; ueber alle Zeilen zaehlte jede
      Kombination bis zu zwoelfmal und der VIF waere kuenstlich stabilisiert
    - drop_duplicates() auf allen Praediktoren leistet das nicht: Der
      log_kriminalitaetsindex ist monatlich rollierend, damit sind nahezu alle
      3.960 Zeilen eindeutig und die Entdopplung laeuft ins Leere
    - `stadtteil_jahr` ist die Ebene, auf der ACS- und Land-Use-Merkmale
      variieren; diese Zahl ist massgeblich
    - `alle_zeilen` steht zum Vergleich daneben
    """
    from statsmodels.stats.outliers_influence import variance_inflation_factor
    from statsmodels.tools.tools import add_constant

    mengen = {
        "stadtteil_jahr": panel.drop_duplicates(subset=["stadtteil", "jahr"]),
        "alle_zeilen": panel,
    }
    zeilen = []
    for basis, teil in mengen.items():
        X = add_constant(teil[PRAEDIKTOREN].astype(float), has_constant="add")
        for i, s in enumerate(X.columns):
            if s == "const":
                continue
            zeilen.append({"basis": basis, "merkmal": s, "n_zeilen": len(teil),
                           "vif": round(float(variance_inflation_factor(
                               X.to_numpy(), i)), 2)})
    return (pd.DataFrame(zeilen)
            .sort_values(["basis", "vif"], ascending=[True, False]))


def main() -> int:
    """Rechnet die sieben Auswertungen.

    Input:  beide Panels, Ergebnisdateien von m02 und m03; Schalter --ohne-baeume
    Output: neun CSV-Dateien unter results/shap/; Exitcode

    1. Attribution: SHAP-Beitraege auf dem ruhigsten Fold, verdichtet zu
       Faktorgruppen -> beitraege.csv, gruppen.csv, uebersprungen.csv
    2. Extrapolation nach Merkmal und Stadtteil, plus Zusammenhang zum RMSE
    3. Ablation der Exposition: Baumverfahren ohne Ruecktransformation
    4. Faktorgruppen des Mengenstrangs aus der Baseline
    5. Ablation der Faktorgruppen: was kostet das Weglassen einer Gruppe
    6. VIF als Kollinearitaetsmass
    7. Verschiebung der Mengenvorhersagen -> verschiebung.csv

    - bleibt Schritt 1 leer, ist das ein Ergebnis und kein Fehler; genau das
      ist im Mengenstrang der Fall
    - das Hold-out wird vor Schritt 1 herausgefiltert; das Skript kennt kein
      "holdout"-Argument
    - die Schritte 3 bis 5 und 7 sind Zusatzbelege und beruehren den Verfahrensvergleich
      nicht
    - --ohne-baeume beschraenkt Schritt 5 auf die GLM-Baselines und drueckt die
      Laufzeit deutlich
    """
    for pfad, wer in ((RESULTS_DIR / "regression" / "vergleich.csv", "m02"),
                      (RESULTS_DIR / "klassifikation" / "vergleich.csv", "m03")):
        if not pfad.exists():
            raise SystemExit(f"{pfad.relative_to(ROOT)} fehlt - erst {wer} "
                             f"ausfuehren.")
    OUT.mkdir(parents=True, exist_ok=True)

    import m02_menge as m02
    import m03_struktur as m03

    reg = pd.read_parquet(PFAD_REGRESSION)
    kl = pd.read_parquet(PFAD_KLASSIFIKATION)
    selten = selten_je_stadtteil(kl)
    reg = reg[reg["ist_holdout"] == 0].reset_index(drop=True)
    kl = kl[kl["ist_holdout"] == 0].reset_index(drop=True)

    beitraege, verworfen_alle = [], []

    for strang, panel, ordner, modul in (
            ("menge", reg, "regression", m02),
            ("struktur", kl, "klassifikation", m03)):
        basis = RESULTS_DIR / ordner
        folds = pd.read_csv(basis / ("menge_folds.csv" if strang == "menge"
                                     else "struktur_folds.csv"))
        vergleich = pd.read_csv(basis / "vergleich.csv")
        tuning = pd.read_csv(basis / "tuning.csv")

        genommen, verworfen = schlagen_die_latte(vergleich)
        verworfen_alle.append(verworfen.assign(strang=strang))
        k = ruhigster_fold(folds)
        d = wiederholte_aufteilung(panel, wiederholung=0, selten=selten)
        tr, te = fold_masken(d, k)
        train, test = d[tr], d[te]
        extra = float(folds[(folds["wiederholung"] == 0) & (folds["fold"] == k)]
                      ["extrapolationsanteil"].iloc[0])
        print(f"  {strang}: Fold {k} gewaehlt (Extrapolation {extra:.1%}), "
              f"{len(genommen)} Modell(e) ueber der Latte")

        for ziel, name in genommen:
            zeile = tuning[(tuning["verfahren"] == name)
                           & (tuning["fold"] == k)
                           & (tuning["zielgroesse"] == ziel)].iloc[0]
            parameter = json.loads(zeile["parameter_json"])
            X_te = test[MERKMALE].astype(float)

            if strang == "menge":

                # Exposition: erklaert wird das bewertete Ratenmodell, nicht
                # ein direkt auf der Anzahl gefittetes.
                fit_ziel = RATE if ziel == ZIELGROESSE else ziel
                modell = modul.verfahren(name).set_params(**parameter)
                modell.fit(train[MERKMALE].astype(float),
                           train[fit_ziel].astype(float))
            else:
                modell = modul.verfahren(name).set_params(**parameter)
                y_tr = modul.kodiere(train[ZIELKLASSE])
                if name == "xgboost":
                    modell.fit(train[MERKMALE].astype(float), y_tr,
                               sample_weight=modul._gewichte(y_tr))
                else:
                    modell.fit(train[MERKMALE].astype(float), y_tr)

            werte = _beitraege(modell, X_te, name)
            anteil = werte / werte.sum() if werte.sum() else werte
            for merkmal, roh, rel in zip(MERKMALE, werte, anteil):
                beitraege.append({"strang": strang, "zielgroesse": ziel,
                                  "verfahren": name, "fold": k,
                                  "merkmal": merkmal,
                                  "beitrag": float(roh),
                                  "anteil": float(rel)})
            print(f"    {name:<14} {ziel}")

    if not beitraege:
        print("\n  Kein Modell schlaegt seine Stufe-2-Baseline - es gibt nichts "
              "zu erklaeren.\n  Das ist ein Ergebnis, kein Fehler"
              ".")

    b = pd.DataFrame(beitraege)
    b.to_csv(OUT / "beitraege.csv", index=False)
    pd.concat(verworfen_alle, ignore_index=True).to_csv(
        OUT / "uebersprungen.csv", index=False)

    if len(b):
        zu_gruppe = {m: g for g, ms in GRUPPEN.items() for m in ms}
        g = (b.assign(gruppe=b["merkmal"].map(zu_gruppe))
              .groupby(["strang", "zielgroesse", "verfahren", "gruppe"],
                       sort=False)["anteil"].sum().reset_index()
              .sort_values(["zielgroesse", "verfahren", "anteil"],
                           ascending=[True, True, False]))
        g.round(4).to_csv(OUT / "gruppen.csv", index=False)
        print("\n  Beitrag je Faktorgruppe:")
        print(g.to_string(index=False))

    # Deskriptive Aufschluesselung der Extrapolation - erklaert die
    # Fold-Streuung. Beruehrt den Verfahrensvergleich nicht.
    merkmale, stadtteile, zusammenhang = extrapolation_aufschluesseln(
        reg, selten, pd.read_csv(RESULTS_DIR / "regression" / "menge_folds.csv"))
    merkmale.round(4).to_csv(OUT / "extrapolation_merkmale.csv", index=False)
    stadtteile.round(4).to_csv(OUT / "extrapolation_stadtteile.csv", index=False)
    zusammenhang.to_csv(OUT / "extrapolation_zusammenhang.csv", index=False)

    print("\n  Extrapolation, Anteil der Testzeilen je Merkmal:")
    for _, z in merkmale.head(5).iterrows():
        print(f"    {z['merkmal']:<28}{z['anteil_testzeilen']:>7.1%}")
    print("  Staerkste Stadtteile:")
    for _, z in stadtteile.head(5).iterrows():
        print(f"    Fold {z['fold']}  {z['stadtteil']:<28}{z['anteil']:>7.1%}")
    print("  Zusammenhang Extrapolationsanteil <-> RMSE (Spearman, 50 Laeufe):")
    for _, z in zusammenhang.iterrows():
        print(f"    {z['verfahren']:<14} {z['zielgroesse']:<21} "
              f"rho {z['spearman_rho']:>6.3f}  p {z['p_wert']:.4f}")

    # --- Ablation: was leistet die Expositionsbehandlung? ---
    menge_folds = pd.read_csv(RESULTS_DIR / "regression" / "menge_folds.csv")
    tuning_reg = pd.read_csv(RESULTS_DIR / "regression" / "tuning.csv")
    ohne = ablation_exposition(reg, selten, tuning_reg)
    mit = (menge_folds[menge_folds["zielgroesse"] == ZIELGROESSE]
           .assign(spezifikation="mit_exposition")
           [["wiederholung", "fold", "verfahren", "spezifikation",
             "RMSE", "MAE", "R2"]])
    basis = pd.read_csv(RESULTS_DIR / "regression" / "baselines_folds.csv")
    basis = basis[(basis["modell"] == "Poisson-GLM")
                  & (basis["zielgroesse"] == ZIELGROESSE)]

    abl = pd.concat([mit, ohne], ignore_index=True)
    abl.round(6).to_csv(OUT / "ablation_exposition.csv", index=False)
    uebersicht = (abl.groupby(["verfahren", "spezifikation"], sort=False)
                     [["RMSE", "R2"]].mean().round(3).reset_index())
    print("\n  Ablation Expositionsbehandlung, Zielgroesse anzahl_einsaetze:")
    print(f"    {'Poisson-GLM (Referenz)':<40}RMSE "
          f"{basis['RMSE'].mean():7.2f}")
    for _, z in uebersicht.iterrows():
        wie = ("mit Exposition" if z["spezifikation"] == "mit_exposition"
               else "OHNE Exposition")
        print(f"    {z['verfahren'] + ', ' + wie:<40}RMSE {z['RMSE']:7.2f}  "
              f"R2 {z['R2']:6.3f}")

    # --- Faktorgruppen des Mengenstrangs aus der Baseline ---
    k_fold = ruhigster_fold(menge_folds)
    basis_beitraege = faktorgruppen_baseline(reg, selten, k_fold)
    basis_beitraege.round(4).to_csv(OUT / "faktorgruppen_menge.csv", index=False)
    gruppiert = (basis_beitraege.groupby("gruppe")["anteil"].sum()
                 .sort_values(ascending=False))
    print(f"\n  Faktorgruppen im Mengenstrang (Poisson-GLM, Fold {k_fold}):")
    for gruppe, anteil in gruppiert.items():
        print(f"    {gruppe:<24}{anteil:>7.1%}")

    # --- Ablation der Faktorgruppen ---
    tuning_kl = pd.read_csv(RESULTS_DIR / "klassifikation" / "tuning.csv")
    roh = ablation_faktorgruppen(reg, kl, selten, tuning_kl,
                                 mit_baeumen="--ohne-baeume" not in sys.argv)
    roh.round(6).to_csv(OUT / "ablation_faktorgruppen.csv", index=False)
    abl_gruppen = _ablation_auswerten(roh)
    abl_gruppen.to_csv(OUT / "ablation_faktorgruppen_mittel.csv", index=False)

    print("\n  Ablation der Faktorgruppen - was kostet das Weglassen?")
    for (strang, verf), g in abl_gruppen.groupby(["strang", "verfahren"],
                                                 sort=False):
        mass = g["mass"].iloc[0]
        print(f"    {strang} · {verf}  ({mass}, positiv = schlechter ohne)")
        for _, z in g.iterrows():
            print(f"      {z['weggelassen']:<24}"
                  f"{z['verschlechterung_mittel']:>9.3f}  "
                  f"± {z['std_wiederholungen']:.3f}   "
                  f"{int(z['wdh_mit_verschlechterung'])}/10 Wdh. schlechter")

    vif = _vif(reg)
    vif.to_csv(OUT / "vif.csv", index=False)
    print("\n  Multikollinearitaet (hoechster VIF je Bezugsmenge):")
    for basis, g in vif.groupby("basis"):
        oben = g.iloc[0]
        print(f"    {basis:<15} {int(oben['n_zeilen']):>5} Zeilen  "
              f"{oben['merkmal']} {oben['vif']}")

    versch = verschiebung()
    versch.round(4).to_csv(OUT / "verschiebung.csv", index=False)
    print("\n  Verschiebung der Mengenvorhersagen (y_hat - y, Mittel ueber die Laeufe):")
    for _, z in versch.iterrows():
        print(f"    {z['zielgroesse']:<22}{z['verfahren']:<15}"
              f"{z['verschiebung_mittel']:>8.2f}   RMSE {z['rmse_mittel']:7.2f}"
              f" -> {z['rmse_ohne_verschiebung']:7.2f}   "
              f"{int(z['laeufe_unterschaetzt'])}/{int(z['laeufe'])} Laeufe zu tief")
    print(f"\n  => {OUT}")
    return 0


def verschiebung() -> pd.DataFrame:
    """Wie weit liegen die Mengenvorhersagen im Mittel daneben?

    Input:  results/regression/menge_vorhersagen.parquet aus m02
    Output: verschiebung.csv

    - Fehler je Testzeile: y_hat - y, negativ heisst zu tief vorhergesagt
    - je Lauf (Wiederholung x Fold) drei Werte: die Verschiebung als Mittel
      der Fehler, der RMSE und der RMSE ohne Verschiebung, also die
      Standardabweichung der Fehler; je Lauf gilt
      RMSE^2 = Verschiebung^2 + Standardabweichung^2
    - gemittelt wird ueber die Laeufe wie in m02; jeder Lauf hat gleich viele
      Testzeilen, das Laufmittel der Verschiebung ist damit zugleich das
      Mittel ueber alle Testzeilen
    - Anlass: Ridge schaetzt auf log(1+y), und expm1 fuehrt im Mittel zu
      tief; rmse_senkung zeigt fuer jedes Verfahren, was das Abziehen der
      Verschiebung braechte
    - liest nur, passt kein Modell an
    """
    vorhersagen = pd.read_parquet(RESULTS_DIR / "regression"
                                  / "menge_vorhersagen.parquet")
    vorhersagen["fehler"] = vorhersagen["y_hat"] - vorhersagen["y"]
    lauf = (vorhersagen
            .groupby(["zielgroesse", "verfahren", "wiederholung", "fold"])
            ["fehler"]
            .agg(verschiebung="mean",
                 rmse=lambda e: np.sqrt(np.mean(e ** 2)),
                 rmse_ohne_verschiebung=lambda e: e.std(ddof=0))
            .reset_index())
    lauf["zu_tief"] = lauf["verschiebung"] < 0
    tabelle = (lauf.groupby(["zielgroesse", "verfahren"])
               .agg(laeufe=("verschiebung", "size"),
                    laeufe_unterschaetzt=("zu_tief", "sum"),
                    verschiebung_mittel=("verschiebung", "mean"),
                    rmse_mittel=("rmse", "mean"),
                    rmse_ohne_verschiebung=("rmse_ohne_verschiebung", "mean"))
               .reset_index())
    tabelle["rmse_senkung"] = (tabelle["rmse_mittel"]
                               - tabelle["rmse_ohne_verschiebung"])
    return tabelle


if __name__ == "__main__":
    raise SystemExit(main())
