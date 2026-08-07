"""
Interpretation: Welche Merkmale tragen die Vorhersage?

    python modelle/m04_shap.py

Eingang: results/regression/{menge_folds,vergleich}.csv
         results/klassifikation/{struktur_folds,vergleich}.csv
         results/*/tuning.csv · data/processed/{regression,klassifikation}.parquet
Ausgang: results/shap/beitraege.csv · gruppen.csv · vif.csv · uebersprungen.csv

STAND: vollstaendig, 05.08.2026. Setzt m02 und m03 voraus.

--------------------------------------------------------------------------
DIE EINE REGEL
--------------------------------------------------------------------------
SHAP wird NUR fuer Modelle gerechnet, die ihre Stufe-2-Baseline schlagen. Fuer
alle anderen erklaert man Rauschen - und eine Abbildung, die Beitraege zeigt, wo
kein Signal ist, ist schlimmer als keine Abbildung. Das Skript prueft das selbst
und ueberspringt Modelle, die die Latte reissen; die uebersprungenen stehen mit
Begruendung in `uebersprungen.csv`, damit die Auswahl nachvollziehbar ist und
nicht wie Rosinenpicken aussieht.

Massgeblich ist der PRIMAERTEST auf den Wiederholungsmitteln (teststufe
"wiederholung"): mittlere Differenz zugunsten des Verfahrens UND signifikant.

--------------------------------------------------------------------------
WAS GERECHNET WIRD
--------------------------------------------------------------------------
  TreeExplainer   fuer Random Forest und XGBoost
  Koeffizienten   fuer Ridge - dort braucht es kein SHAP. Der StandardScaler
                  steht in der Pipeline, also sind die Koeffizienten bereits
                  standardisiert und untereinander vergleichbar.
  Fold            EIN Fold, nicht alle - der mit dem GERINGSTEN
                  Extrapolationsanteil in Wiederholung 0. Begruendung: Dort
                  liegen die wenigsten Testzeilen ausserhalb des gelernten
                  Wertebereichs, die Beitraege beruhen also am ehesten auf
                  Interpolation. Die Wahl steht in der Ausgabe und ist im Text
                  zu nennen.

--------------------------------------------------------------------------
FALLSTRICK: BLOCKWEISE INTERPRETIEREN
--------------------------------------------------------------------------
Die Strukturmerkmale sind untereinander korreliert. SHAP verteilt den Beitrag
dann auf mehrere Merkmale, und einzelne Werte sind nicht sinnvoll deutbar -
"median_haushaltseinkommen traegt 8 %" waere eine Scheinpraezision.

Deshalb zusammenfassen zu den drei Faktorgruppen des Exposes; `log_bevoelkerung`
(Groessenkontrolle) und die Saison werden getrennt ausgewiesen, weil sie in
keine der drei Gruppen gehoeren. Das beantwortet Unterfrage 1 direkt: Welche
Faktorgruppe traegt wie viel?

--------------------------------------------------------------------------
HIERHER VERSCHOBEN: DER VIF
--------------------------------------------------------------------------
Die Multikollinearitaetspruefung lag frueher in der Eignungspruefung, entschied
dort aber nichts - Ridge ist durch den L2-Strafterm robust dagegen, Baumverfahren
interessiert sie nicht. Ihre einzige echte Konsequenz betrifft genau diese
Interpretation. Deshalb steht sie hier.

Gerechnet auf den EINDEUTIGEN Stadtteil-Merkmalskombinationen, nicht auf allen
Zeilen: Die Strukturmerkmale sind innerhalb eines Jahres konstant, ueber alle
Zeilen zaehlte jede Kombination bis zu zwoelfmal und der VIF waere kuenstlich
stabilisiert.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Stimmt die Rangfolge der Faktorgruppen zwischen Random Forest und XGBoost
    ueberein? Wenn nicht, ist das ein Befund fuer Kapitel 8, kein Fehler.
  - Passt sie zu den Korrelationen aus der Eignungspruefung? Dort lagen
    log_kriminalitaetsindex und anteil_risikogewerbe_pct vorn.
  - Wird eine Faktorgruppe als praktisch bedeutungslos ausgewiesen? Das waere
    eine der wenigen wirklich inhaltlichen Aussagen der Arbeit.
  - Liegt der maximale VIF noch bei rund 11,5? Ein deutlich anderer Wert hiesse,
    dass sich die Merkmalsbasis geaendert hat.
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

# Die drei Faktorgruppen des Exposes, plus zwei getrennt gefuehrte Groessen.
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

    Grundlage ist der Primaertest auf den Wiederholungsmitteln. Verlangt werden
    BEIDE Bedingungen: die mittlere Differenz muss zugunsten des Verfahrens
    ausfallen UND der Test muss signifikant sein. Ein positiver Mittelwert
    allein waere zu wenig - genau davor warnt R-6.
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
    """Der Fold mit dem geringsten Extrapolationsanteil in Wiederholung 0."""
    w0 = folds[folds["wiederholung"] == 0]
    je_fold = w0.groupby("fold")["extrapolationsanteil"].first()
    return int(je_fold.idxmin())


def _beitraege(modell, X: pd.DataFrame, name: str) -> np.ndarray:
    """Mittlerer absoluter Beitrag je Merkmal - SHAP oder Koeffizient.

    Bei Ridge stehen standardisierte Koeffizienten; sie sind der direkte
    Gegenwert zu SHAP-Beitraegen und brauchen keinen Explainer. Bei den
    Baumverfahren rechnet der TreeExplainer exakt statt zu approximieren.

    Mehrklassige Ausgaben werden ueber die Klassen gemittelt - die Frage lautet
    "welche Faktorgruppe traegt", nicht "fuer welche Klasse".

    WARUM XGBOOST EINEN EIGENEN WEG GEHT: `shap.TreeExplainer` kann den
    mehrklassigen `base_score` von XGBoost 3.x nicht lesen und bricht mit
    `could not convert string to float` ab (geprueft mit shap 0.52.0 und
    xgboost 3.2.0, docs/07_BEFUNDE.md, B-17). XGBoost bringt TreeSHAP aber
    selbst mit - `pred_contribs=True` liefert exakt dieselben Werte, gerechnet
    vom selben Algorithmus. Kein Naeherungsverfahren, nur ein anderer Aufrufweg.
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
    """Woher kommen die 33,7 % Extrapolation - und was folgt daraus?

    WARUM DAS HIER STEHT: `03_STAND.md` behauptet, die Spanne des
    Extrapolationsanteils von 3,6 % bis 57,4 % erklaere „einen erheblichen Teil
    der Fold-Streuung". Das war eine Plausibilitaetsaussage ohne Messung. Diese
    Funktion macht eine Zahl daraus. Sie erklaert damit den zentralen Befund
    des Mengenstrangs (R-3, `07_BEFUNDE.md` B-26) und gehoert deshalb zur
    Interpretation, nicht zum Verfahrensvergleich.

    ABGRENZUNG ZU #34 - wichtig, das ist keine Haarspalterei:
    Verboten ist, die TESTMENGE nach Extrapolationsgrad aufzuteilen und dort
    nach Verfahrensunterschieden zu suchen; das waere ein nachtraeglicher
    Zuschnitt der Auswertung. Hier wird nichts aufgeteilt und nichts neu
    verglichen. Die Einheit ist der FOLD, und die Frage lautet, warum Folds
    unterschiedlich schwer sind. Die Primaeraussage bleibt unberuehrt.

    Drei Auswertungen:
      1  je Merkmal    wie oft liegt es allein ausserhalb des Trainingsbereichs
      2  je Stadtteil  wie stark bricht er aus, wenn er im Test steht
      3  je Verfahren  Zusammenhang zwischen Extrapolationsanteil eines Laufs
                       und dem dort gemessenen Fehler (Spearman, ueber alle
                       50 Laeufe)
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
    """ABLATION: Was leistet die Expositionsbehandlung?

    Der Hauptlauf modelliert die Rate und multipliziert mit der Einwohnerzahl
    zurueck (#43) - fuer alle vier Modelle gleich. Diese Ablation entfernt
    genau diesen einen Baustein bei den Baumverfahren und laesst sie direkt auf
    `anzahl_einsaetze` anpassen. Alles andere bleibt identisch: dieselben
    Folds, dieselben Merkmale, dieselben Hyperparameter.

    Es wird also EIN Bestandteil der Spezifikation isoliert. Das ist der Zweck
    einer Ablation und der Grund, warum die Hyperparameter bewusst NICHT neu
    gesucht werden - sonst aenderte man zwei Dinge gleichzeitig.

    WAS SIE BEANTWORTET. Unterfrage 4 fragt nach Implikationen fuer die
    Modellauswahl. Die Ablation zeigt, ob die Wahl des Verfahrens oder die
    Spezifikation den groesseren Hebel hat - und liefert damit eine
    uebertragbare Aussage statt eines knappen Rankings.

    Frueher gemessen (`07_BEFUNDE.md`, B-33): Ohne Expositionsbehandlung lagen
    Random Forest bei 67,7 und XGBoost bei 61,7 RMSE, mit ihr bei 36,4 und
    35,7. Der Unterschied zwischen den Spezifikationen eines Verfahrens ist
    damit ein Vielfaches des Unterschieds zwischen den Verfahren.

    DIE FRAGE. Bei `anzahl_einsaetze` liegen die Baumverfahren rund 20 RMSE
    hinter Ridge, bei `einsaetze_je_1000_ew` leicht davor. Der einzige
    Unterschied zwischen beiden Zielgroessen ist die Einwohnerzahl. Die
    Vermutung lautet: Baeume koennen „Einsaetze = Bevoelkerung x Risiko" nicht
    nachbauen, weil sie je Blatt einen festen Wert ausgeben und Extremwerte zur
    Blattmitte ziehen — und weil RMSE auf der Originalskala von den grossen
    Stadtteilen dominiert wird (Tenderloin 280, Seacliff 6,4).

    Spiegelbild zu R-9: Dort wurde der Offset der Baseline WEGGENOMMEN, Ergebnis
    null. Hier fehlt er den Baeumen. Kein Widerspruch — fuer ein Modell mit
    Log-Verknuepfung und freiem Koeffizienten auf `log_bevoelkerung` ist der
    Offset redundant, fuer einen Baum ohne beides nicht.
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
                # OHNE Exposition: direkt auf der absoluten Zahl anpassen.
                # Der Hauptlauf tut das Gegenteil (Rate schaetzen, mit der
                # Bevoelkerung zurueckrechnen); die Differenz ist der Effekt.
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


def faktorgruppen_baseline(panel: pd.DataFrame, selten: pd.Series,
                         fold: int) -> pd.DataFrame:
    """Beitrag der drei Faktorgruppen im MENGENSTRANG - aus der Baseline.

    WARUM AUS DER BASELINE. Unterfrage 1 fragt nach dem Erklaerungsbeitrag der
    drei Faktorgruppen. Fuer die Struktur liefert ihn SHAP. Fuer die Menge
    nicht: `m04` ueberspringt dort alle Modelle, weil keines seine Baseline
    schlaegt — und Beitraege eines unterlegenen Modells auszuweisen hiesse,
    Rauschen zu erklaeren.

    Die Loesung liegt im Ergebnis selbst: Das **beste Modell des Mengenstrangs
    ist das Poisson-GLM**. Seine Koeffizienten beantworten UF1 direkt und
    ehrlich. Dass die Antwort aus der Baseline statt aus einem
    Vergleichsverfahren kommt, ist kein Notbehelf, sondern die Konsequenz des
    Befunds.

    VERGLEICHBAR GEMACHT ueber standardisierte Beitraege |Koeffizient| x
    Standardabweichung des Merkmals. Ohne diesen Schritt haengt die Groesse
    eines Koeffizienten an der Einheit des Merkmals — Einkommen in Dollar
    bekaeme automatisch einen winzigen Koeffizienten.

    Gerechnet auf demselben Fold wie die SHAP-Werte, damit beide Straenge
    dieselbe Datengrundlage haben.
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
    """VIF auf zwei Bezugsmengen - und der Grund, warum es zwei sein muessen.

    Die Absicht der Spezifikation war, jede Merkmalskombination nur EINMAL zu
    zaehlen: Die Strukturmerkmale sind innerhalb eines Jahres konstant, ueber
    alle Zeilen zaehlte jede Kombination bis zu zwoelfmal, und der VIF waere
    kuenstlich stabilisiert.

    Ein `drop_duplicates()` auf allen Praediktoren leistet das aber NICHT: Seit
    Decision Log #17 ist `log_kriminalitaetsindex` ein MONATLICH rollierender
    Index. Damit ist fast jede Zeile eindeutig - gemessen 3.757 von 3.828 - und
    die Entdopplung laeuft ins Leere (docs/07_BEFUNDE.md, B-18).

    Deshalb zwei ausgewiesene Bezugsmengen:

      stadtteil_jahr   eine Zeile je Stadtteil und Jahr. Das ist die Ebene, auf
                       der die ACS- und Land-Use-Merkmale tatsaechlich variieren,
                       und die Zahl, die in den Text gehoert.
      alle_zeilen      zum Vergleich, damit der Unterschied sichtbar ist.
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
                # EXPOSITION (#43): Fuer `anzahl_einsaetze` wurde das bewertete
                # Modell auf der RATE angepasst. Wird hier direkt auf der Anzahl
                # gefittet, erklaert SHAP ein anderes Modell als das, dessen
                # Guetemasse berichtet werden - und niemand saehe es den Zahlen
                # an. Die Beitraege beziehen sich also auf das Ratenmodell; das
                # ist im Text zu benennen.
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
              "zu erklaeren.\n  Das ist ein Ergebnis, kein Fehler "
              "(docs/06_RISIKEN.md, R-2).")

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

    # Deskriptive Aufschluesselung der Extrapolation - erklaert R-3 und die
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

    # --- Faktorgruppen des Mengenstrangs aus der Baseline (UF1) ---
    k_fold = ruhigster_fold(menge_folds)
    negbin = faktorgruppen_baseline(reg, selten, k_fold)
    negbin.round(4).to_csv(OUT / "faktorgruppen_menge.csv", index=False)
    gruppiert = negbin.groupby("gruppe")["anteil"].sum().sort_values(ascending=False)
    print(f"\n  Faktorgruppen im Mengenstrang (Poisson-GLM, Fold {k_fold}):")
    for gruppe, anteil in gruppiert.items():
        print(f"    {gruppe:<24}{anteil:>7.1%}")

    vif = _vif(reg)
    vif.to_csv(OUT / "vif.csv", index=False)
    print("\n  Multikollinearitaet (hoechster VIF je Bezugsmenge):")
    for basis, g in vif.groupby("basis"):
        oben = g.iloc[0]
        print(f"    {basis:<15} {int(oben['n_zeilen']):>5} Zeilen  "
              f"{oben['merkmal']} {oben['vif']}")
    print(f"\n  => {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
