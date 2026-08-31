"""
Verfahrensvergleich fuer die MENGE der Einsatzlast.

    python modelle/m02_menge.py            Tuning, Bewertung, Aggregation, Vergleich
    python modelle/m02_menge.py holdout    zusaetzlich die einmalige Schlussbewertung
    python modelle/m02_menge.py holdout --weiter   Phase 1+2 aus results/ uebernehmen

Eingang: data/processed/regression.parquet
Ausgang: results/regression/menge_folds.csv, menge_mittel.csv, tuning.csv,
         vergleich.csv, holdout.csv

  - Zwei Zielgroessen (anzahl_einsaetze, einsaetze_je_1000_ew) x drei
    Verfahren (Ridge, Random Forest, XGBoost) x 10 Wiederholungen x 5 Folds
    = 300 Laeufe
  - Vier Phasen: tunen (nur auf Wiederholung 0, #34), bewerten, zweistufig
    aggregieren, gepaart vergleichen
  - Gegner ist die STUFE-2-BASELINE aus v1_baselines.py, nicht die triviale
    Referenz (#33). Schlaegt ein Verfahren sie nicht, ist das ein Befund
  - Die in docs/04_MODELLIERUNG.md genannten Fallstricke sind im Code
    markiert - wer eine dieser Stellen aendert, sollte den Abschnitt lesen

PRUEFAUFTRAEGE nach JEDEM Lauf (CLAUDE.md, B-9)
  1  Schlaegt jedes Verfahren die Stufe-2-Baseline, je Zielgroesse einzeln?
  2  Ueberlappen sich zwei Streuungsbereiche? Dann "nicht unterscheidbar"
     berichten, keine Rangfolge (R-1, R-6)
  3  Wie oft sind Vorhersagen negativ (n_negativ)? Nicht kappen, ausweisen.
     Erwartet: keine, seit Tweedie und Poisson log-verknuepft sind (#42)
  4  Zeilenzahl: 30 in tuning.csv (15 Suchen, zwischen den Zielgroessen
     geteilt, #43), 300 in menge_folds.csv
  5  Hold-out unberuehrt? Ohne Argument filtert main() es unwiderruflich
     heraus, bevor irgendetwas rechnet
  6  std_wiederholungen deutlich kleiner als std_folds? Waere es null,
     waeren die Wiederholungen Dubletten (B-3)
  7  Extrapolationsanteil um 34,6 %? Starke Abweichung heisst, die
     Aufteilung ist nicht die dokumentierte
  8  Laufzeiten einkernig gemessen, Parallelisierungsgewinn getrennt
     (#39/#40); Kernzahl der Maschine protokollieren
  9  parallel_abweichung_max: bei XGBoost erwartet (B-24), bei Ridge und RF
     nicht
 10  ueberanpassung_RMSE je Verfahren (#51) - bei Ridge klein, bei Baeumen
     gross. ZWISCHEN Konfigurationen vergleichen, nicht zwischen Verfahren
 11  Ist ueberanpassung_RMSE gegenueber archiv/2026-08-14_budget50/
     gesunken? Nur fuer 07_BEFUNDE.md - berichtet wird nach #52 allein der
     neue Lauf, kein Vorher-Nachher

Ausfuehrliche Fassung: docs/08_FUNKTIONSDOKUMENTATION.md
"""
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "prep"))
sys.path.insert(0, str(_ROOT / "vorpruefung"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (EXPOSURE_ROH, N_FOLDS, PFAD_KLASSIFIKATION,  # noqa: E402
                    PFAD_REGRESSION, PRAEDIKTOREN, RESULTS_DIR, ROOT, SAISON)
from config_modelle import (RANDOM_STATE, SUCHRAEUME,  # noqa: E402
                            TUNING_BUDGET, WIEDERHOLUNGEN)
from s2_datensaetze import RATE, ZIELGROESSE, fold_masken  # noqa: E402
from v0_aufteilung import (entwicklung_und_holdout,  # noqa: E402
                           selten_je_stadtteil, wiederholte_aufteilung)

OUT = RESULTS_DIR / "regression"
MERKMALE = PRAEDIKTOREN + SAISON
ZIELE = (ZIELGROESSE, RATE)
VERFAHREN = ("ridge", "random_forest", "xgboost")

# Die Stufe-2-Baseline, gegen die die Primaeraussage laeuft (#34). Der Name
# muss zu vorpruefung/v1_baselines.POISSON passen - er wird zum Filtern der
# Spalte `modell` in baselines_folds.csv benutzt, ein Tippfehler liefert also
# stillschweigend eine leere Vergleichsmenge.
BASELINE_STUFE2 = "Poisson-GLM"

# Der gepaarte Test laeuft auf RMSE. Begruendung: Bei der Rate ist R2 kein
# tragfaehiges Mass (docs/03_STAND.md, Abschnitt 4) - der Mittelwert wird
# negativ, obwohl die Baseline in jedem Fold besser ist als die Nullmarke. Zwei
# verschiedene Testmetriken fuer zwei Zielgroessen waeren schwerer zu
# verteidigen als eine. MAE und R2 wandern als Spalten mit und werden
# nachrichtlich berichtet.
TESTMASS = "RMSE"
ALPHA = 0.05

# ==========================================================================
# PARALLELISIERUNG - zwei Gruende fuer eine Entscheidung
# ==========================================================================
# Die Modelle laufen EINKERNIG, parallelisiert wird nur die Suche.
# Praktisch: RandomizedSearchCV(n_jobs=-1) um einen Schaetzer mit n_jobs=-1
# startet Prozesse ueber alle Kerne, die sich gegenseitig blockieren (B-16).
# Inhaltlich und wichtiger: Unterfrage 3 fragt nach dem Aufwand. Ridge hat als
# geschlossene Loesung nichts zu parallelisieren, RF und XGBoost skalieren -
# in unterschiedlichen Betriebsarten gemessen haengt die Zahl an der Kernzahl
# der Maschine statt am Verfahren. Der Parallelisierungsgewinn ist eine eigene
# Groesse und wird in jedem Lauf getrennt miterhoben (siehe `ein_lauf`).
N_JOBS_MODELL = 1
N_JOBS_SUCHE = -1

# ==========================================================================
# EXPOSITION - jedes Verfahren modelliert die RATE (#43)
# ==========================================================================
# Ein Satz fuer alle vier Modelle: geschaetzt wird `einsaetze_je_1000_ew`, fuer
# `anzahl_einsaetze` wird mit der Einwohnerzahl zurueckmultipliziert. Genau
# diese Konstruktion verwendet das Poisson-GLM ueber seinen Offset seit jeher.
#
# WARUM GEAENDERT (06.08.2026): Gemessen (B-33) lag der Random Forest bei
# `anzahl_einsaetze` mit 67,7 gegen 37,4 RMSE hinter der Baseline - ueber die
# Rate gerechnet sind es 36,4. Der gesamte Rueckstand stammte aus der
# Spezifikation, nicht aus dem Verfahren. Der Grund fuer die Korrektur ist
# aber nicht dieses Ergebnis: Die Frage lautet, welches VERFAHREN die hoechste
# Guete erzielt. Verlieren zwei davon, weil ihnen die Expositionsstruktur
# vorenthalten wurde, misst der Vergleich die Modellierungsentscheidung. Bei
# Zaehldaten mit Expositionsgroesse ist deren explizite Behandlung Standard.
#
# Die Gegenprobe - Baumverfahren OHNE Expositionsbehandlung - ist kein zweiter
# Betriebsmodus, sondern die Ablation in m04_shap.ablation_exposition(). Hier
# gibt es keinen Schalter: Der Lauf hat genau eine Spezifikation.


# ---------------------------------------------------------------------------
# BAUSTEIN 1  Die Pipeline
# ---------------------------------------------------------------------------
def verfahren(name: str, n_jobs: int = N_JOBS_MODELL):
    """Baut die ungetunte Pipeline fuer ein Verfahren.

    Ein:  Verfahrensname, optional n_jobs
    Aus:  scikit-learn-Pipeline ohne Hyperparameter

    - n_jobs steuert nur die Parallelisierung, nicht das Ergebnis; voreingestellt
      einkernig, damit die Laufzeiten vergleichbar bleiben
    - Ridge bekommt StandardScaler und log-Zieltransformation IN der Pipeline:
      der L2-Strafterm behandelt alle Koeffizienten gleich, und
      TransformedTargetRegressor rechnet nach der Vorhersage mit expm1 zurueck
    - die Baumverfahren bekommen keine Zieltransformation: sie sind
      skalenunempfindlich, und eine transformierte Zielgroesse machte die
      Guetemasse unvergleichbar
    - Verlustfunktion (#42): XGBoost reg:tweedie mit getuntem Exponenten, Random
      Forest criterion="poisson", Ridge unveraendert auf log(1+y)
    - Grund: Der quadratische Fehler auf rohen Zaehldaten gewichtet bei
      Einsatzzahlen von 6 bis 280 und Dispersionsindex 62,8 einen Fehler von 20
      in Tenderloin wie in Seacliff. Das war eine Ungleichbehandlung in der
      Spezifikation, kein Ergebnis ueber die Verfahren
    - scikit-learn kennt kein Tweedie fuer Waelder; diese Einschraenkung ist
      selbst ein berichtbarer Befund
    """
    from sklearn.compose import TransformedTargetRegressor
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    if name == "ridge":
        return make_pipeline(
            StandardScaler(),
            TransformedTargetRegressor(regressor=Ridge(),
                                       func=np.log1p, inverse_func=np.expm1))
    if name == "random_forest":
        return RandomForestRegressor(random_state=RANDOM_STATE, n_jobs=n_jobs,
                                     criterion="poisson")
    if name == "xgboost":
        from xgboost import XGBRegressor
        return XGBRegressor(random_state=RANDOM_STATE, n_jobs=n_jobs,
                            objective="reg:tweedie")
    raise ValueError(f"Unbekanntes Verfahren: {name}")


def suchraum(name: str) -> dict:
    """Uebersetzt SUCHRAEUME aus der Config in scipy-Verteilungen.

    Ein:  Verfahrensname
    Aus:  dict Parametername -> Verteilung, mit Pipeline-Praefix

    - die Config haelt die Raeume als Tupel ("loguniform", a, b), damit sie ohne
      scipy lesbar bleiben
    - der Praefix haengt am Pipeline-Aufbau: bei Ridge liegt der Schaetzer zwei
      Ebenen tief (Pipeline -> TransformedTargetRegressor -> Ridge), bei den
      Baumverfahren direkt
    """
    from scipy.stats import loguniform, randint, uniform

    praefix = ("transformedtargetregressor__regressor__"
               if name == "ridge" else "")
    raum = {}
    for parameter, spez in SUCHRAEUME[name].items():
        art, *werte = spez
        if art == "loguniform":
            verteilung = loguniform(werte[0], werte[1])
        elif art == "int":
            verteilung = randint(werte[0], werte[1] + 1)
        elif art == "uniform":
            verteilung = uniform(werte[0], werte[1] - werte[0])
        elif art == "choice":
            verteilung = werte[0]
        else:
            raise ValueError(f"Unbekannte Suchraum-Art: {art}")
        raum[praefix + parameter] = verteilung
    return raum


# ---------------------------------------------------------------------------
# BAUSTEIN 2  Das Tuning
# ---------------------------------------------------------------------------
def tune(name: str, train: pd.DataFrame, ziel: str) -> dict:
    """Sucht die Hyperparameter auf den Trainingsstadtteilen eines Folds.

    Ein:  Trainingsrahmen des Folds, Verfahren, Zielgroesse
    Aus:  die Parameter als dict, nicht das Modell

    - FALLSTRICK: Der innere CV muss nach Stadtteil gruppieren. RandomizedSearchCV
      nimmt voreingestellt KFold und schneidet nach Zeilen; ein Stadtteil hat aber
      132 Zeilen, und da die Strukturmerkmale innerhalb eines Jahres konstant
      sind, laegen faktisch dieselben Zeilen in innerem Training und innerer
      Validierung. Den Zahlen sieht man das nicht an - sie waeren nur zu gut
    - Rueckgabe sind die Parameter, nicht best_estimator_: der ist auf dem inneren
      Trainingsanteil gefittet und verschenkte ein Viertel der Daten
    - der Schaetzer laeuft einkernig, parallelisiert wird allein die Suche; zuvor
      blockierten sich die Prozesse gegenseitig (B-16)
    """
    from sklearn.model_selection import GroupKFold, RandomizedSearchCV

    suche = RandomizedSearchCV(
        estimator=verfahren(name, n_jobs=N_JOBS_MODELL),
        param_distributions=suchraum(name),
        n_iter=TUNING_BUDGET,
        cv=GroupKFold(n_splits=4),
        scoring="neg_root_mean_squared_error",
        random_state=RANDOM_STATE,
        n_jobs=N_JOBS_SUCHE,
    )
    suche.fit(train[MERKMALE].astype(float), train[ziel].astype(float),
              groups=train["stadtteil"])
    return suche.best_params_


# ---------------------------------------------------------------------------
# BAUSTEIN 3  Ein einzelner Lauf
# ---------------------------------------------------------------------------
def ein_lauf(name: str, parameter: dict, train: pd.DataFrame,
             test: pd.DataFrame, ziel: str,
             auch_parallel: bool = False,
             mit_vorhersagen: bool = False) -> dict:
    """Ein Fit, eine Vorhersage, mit Zeitmessung - eine Zeile fuer die CSV.

    Ein:  Trainings- und Testrahmen, Verfahren, Parameter, Zielgroesse,
          auch_parallel
    Aus:  dict mit Guetemassen, Laufzeiten, n_negativ, y_hat_min,
          Extrapolationsanteil

    - die Zeit wird um fit und predict herum gemessen, nicht um die ganze
      Funktion; sonst steckt die Metrikberechnung mit in der Zahl
    - gemessen wird einkernig, fuer alle drei Verfahren gleich
    - auch_parallel=True misst denselben Fit zusaetzlich ueber alle Kerne; die
      Differenz ist der Parallelisierungsgewinn fuer Unterfrage 4
    - im Lauf steht das Argument in jedem Aufruf auf True; ein Mass aus nur einem
      Teil der Laeufe waere eine Ausnahme im Lauf
    - n_negativ und y_hat_min erfassen, dass Ridge auf log(1+y) nach expm1 Werte
      unter null liefern kann. Nicht gekappt, nur ausgewiesen
    """
    from sklearn.metrics import (mean_absolute_error, mean_squared_error,
                                 r2_score)

    X_tr, X_te = train[MERKMALE].astype(float), test[MERKMALE].astype(float)
    y_te = test[ziel].astype(float)

    # EXPOSITION (#43): Geschaetzt wird immer die Rate; fuer die absolute Zahl
    # wird mit der Einwohnerzahl zurueckmultipliziert. Dieselbe Konstruktion
    # wie beim Poisson-GLM. Die Zeitmessung bleibt unberuehrt - die
    # Ruecktransformation ist eine Multiplikation und steht ausserhalb.
    auf_rate = ziel == ZIELGROESSE
    y_tr = train[RATE if auf_rate else ziel].astype(float)
    zurueck = (test[EXPOSURE_ROH].astype(float).to_numpy() / 1000.0
               if auf_rate else 1.0)

    modell = verfahren(name).set_params(**parameter)

    t = time.perf_counter()
    modell.fit(X_tr, y_tr)
    train_sek = time.perf_counter() - t

    t = time.perf_counter()
    y_hat = modell.predict(X_te) * zurueck
    inferenz_sek = time.perf_counter() - t

    train_par, inferenz_par = np.nan, np.nan
    abweichung, abweichung_rmse = np.nan, np.nan
    if auch_parallel:
        parallel = verfahren(name, n_jobs=-1).set_params(**parameter)
        t = time.perf_counter()
        parallel.fit(X_tr, y_tr)
        train_par = time.perf_counter() - t
        t = time.perf_counter()
        y_par = parallel.predict(X_te) * zurueck
        inferenz_par = time.perf_counter() - t
        # Aendert die Kernzahl das ERGEBNIS? Gemessen statt behauptet - und
        # gemessen statt abgebrochen, ein Diagnosewert darf keinen
        # mehrstuendigen Lauf beenden. Die berichteten Guetemasse stammen aus
        # dem einkernigen Fit.
        # BEFUND (B-24): Bei XGBoost erhebliche Abweichung (bis 34,7 bei
        # Mittelwert 76), bei Ridge und RF null. Ursache ist die parallele
        # Reduktion der Histogramme - eine andere Summierungsreihenfolge kippt
        # knapp benachbarte Split-Kandidaten und schaukelt sich ueber hunderte
        # Baeume auf. Eine Aussage ueber Reproduzierbarkeit, gehoert in Kap. 6.
        abweichung = float(np.max(np.abs(y_hat - y_par)))
        # Das Maximum sagt, wie weit EINE Zeile auseinanderlaeuft. Ob die
        # berichteten Guetemasse davon beruehrt waeren, entscheidet der
        # Abstand ueber ALLE Zeilen auf der Skala des Guetemasses.
        abweichung_rmse = float(np.sqrt(np.mean((y_hat - y_par) ** 2)))

    # UEBERANPASSUNGSNACHWEIS (#51): dieselbe Guete auf den TRAININGS-
    # stadtteilen. Der Abstand ist der Standardnachweis fuer Ueberanpassung -
    # ohne ihn bleibt die Diagnose eine Auslegung der Hold-out-Abweichung.
    # KEIN zweiter Fit, nur eine zusaetzliche Vorhersage, und NACH der
    # Zeitmessung, damit Unterfrage 3 unberuehrt bleibt. Verglichen wird auf
    # der BERICHTETEN Skala, nicht auf der Rate, auf der angepasst wurde.
    #
    # WIE DIE ZAHL ZU LESEN IST: Ein Random Forest mit min_samples_leaf = 1
    # interpoliert seine Trainingsdaten KONSTRUKTIONSBEDINGT - ein
    # Trainings-R2 von 0,98 ist dort erwartbar und kein Beweis. Der Abstand ist
    # also NICHT als "A ueberanpasst 16-mal staerker als B" zu lesen, sondern
    # zwischen KONFIGURATIONEN desselben Verfahrens (#49) und als
    # Groessenordnung gegen die linearen Modelle. Der saubere Wert fuer Baeume
    # waere die Out-of-Bag-Schaetzung; sie gibt es nur beim RF und waere
    # gegenueber Ridge und XGBoost asymmetrisch. Bewusst nicht erhoben.
    y_hat_tr = modell.predict(X_tr) * (
        train[EXPOSURE_ROH].astype(float).to_numpy() / 1000.0 if auf_rate else 1.0)
    y_tr_echt = train[ziel].astype(float)

    ergebnis = {
        "train_sekunden_parallel": train_par,
        "inferenz_sekunden_parallel": inferenz_par,
        "parallel_abweichung": abweichung,
        "parallel_abweichung_rmse": abweichung_rmse,
        "verfahren": name, "zielgroesse": ziel,
        "RMSE": float(np.sqrt(mean_squared_error(y_te, y_hat))),
        "MAE": float(mean_absolute_error(y_te, y_hat)),
        "R2": float(r2_score(y_te, y_hat)),
        "RMSE_train": float(np.sqrt(mean_squared_error(y_tr_echt, y_hat_tr))),
        "R2_train": float(r2_score(y_tr_echt, y_hat_tr)),
        "train_sekunden": train_sek, "inferenz_sekunden": inferenz_sek,
        "n_train": len(train), "n_test": len(test),
        "extrapolationsanteil": extrapolationsanteil(train, test),
        "n_negativ": int((y_hat < 0).sum()),
        "y_hat_min": float(np.min(y_hat)),
    }

    # VORHERSAGEN JE ZEILE (nur von phase_bewertung angefordert): Ohne sie
    # braucht jede spaetere Frage - Fehleranalyse je Stadtteil, Beitrag eines
    # einzelnen Teststadtteils zum Gesamtfehler - einen neuen Modelllauf.
    if mit_vorhersagen:
        ergebnis["_vorhersagen"] = pd.DataFrame({
            "stadtteil": test["stadtteil"].to_numpy(),
            "jahr_monat": test["jahr_monat"].to_numpy(),
            "zielgroesse": ziel, "verfahren": name,
            "y": y_te.to_numpy(), "y_hat": y_hat,
        })
    return ergebnis


def extrapolationsanteil(train: pd.DataFrame, test: pd.DataFrame) -> float:
    """Anteil der Testzeilen ausserhalb des Trainings-Wertebereichs.

    Ein:  Trainings- und Testmatrix
    Aus:  Anteil zwischen 0 und 1

    - erklaert spaeter, warum ein Fold aus der Reihe faellt
    - erfasst nur die Spanne je Merkmal, nicht unbekannte Kombinationen; das echte
      Extrapolationsproblem ist eher groesser (R-3)
    """
    lo, hi = train[MERKMALE].min(), train[MERKMALE].max()
    aussen = ((test[MERKMALE] < lo) | (test[MERKMALE] > hi)).any(axis=1)
    return float(aussen.mean())


# ---------------------------------------------------------------------------
# ORCHESTRIERUNG
# ---------------------------------------------------------------------------
def phase_tuning(panel: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """Phase 1: je Zielgroesse, Verfahren und Fold einmal tune().

    Ein:  Panel der Entwicklungsstadtteile
    Aus:  tuning.csv mit 30 Zeilen, Parameter als Spalten und als JSON

    - keine STILLE Wiederverwendung: tuning.csv ist ein Ergebnis dieses Laufs,
      kein Eingang. Sonst wuerden nach einer Aenderung der Spezifikation still
      Parameter aus einer anderen Welt weiterverwendet
    - die einzige Ausnahme ist der ausdrueckliche Schalter --weiter, siehe
      uebernehmen(). Er muss auf der Kommandozeile stehen und bricht ab, wenn
      Daten oder Konfiguration neuer sind als tuning.csv. Damit bleibt das
      Verbot dort bestehen, wo es gemeint war: beim unbemerkten Weiterrechnen
    - getunt wird nur auf Wiederholung 0; die Parameter gelten fuer alle zehn
      (#34). Bewusste Vereinfachung, im Text zu benennen
    - gesucht wird ueber (Verfahren x Fold) = 15 Durchgaenge; beide Zielgroessen
      erhalten denselben Satz (#43)
    - `tuning_sekunden` steht deshalb bei beiden Zielgroessen gleich; eine Summe
      ueber alle 30 Zeilen zaehlt doppelt
    """
    d = wiederholte_aufteilung(panel, wiederholung=0, selten=selten)

    # EXPOSITION (#43): Alle Modelle werden auf der RATE angepasst, es gibt
    # also nur EIN Modell je Verfahren und Fold und damit nur eine Suche.
    # Beide Zielgroessen erhalten denselben Parametersatz, wie bei der
    # Baseline. Die Suche laeuft ueber (Verfahren x Fold) = 15 Durchgaenge; die
    # 30 Zeilen der tuning.csv entstehen erst danach durch Zuordnung. Frueher
    # lief die Schleife ueber die Zielgroessen und die zweite "uebernahm" - das
    # Protokoll wies die Suche dann unter `anzahl_einsaetze` aus, obwohl auf
    # der Rate gesucht wurde (B-37).
    gefunden = {}
    for name in VERFAHREN:
        for k in range(1, N_FOLDS + 1):
            tr, _ = fold_masken(d, k)
            t = time.perf_counter()
            p = _rein_python(tune(name, d[tr], RATE))
            dauer = round(time.perf_counter() - t, 2)
            gefunden[(name, k)] = (p, dauer)
            kurz = {s.split("__")[-1]: w for s, w in p.items()}
            print(f"    tune  {name:<14} Fold {k}  auf {RATE}  "
                  f"{dauer:6.1f}s  {kurz}")

    zeilen = []
    for ziel in ZIELE:
        for name in VERFAHREN:
            for k in range(1, N_FOLDS + 1):
                p, dauer = gefunden[(name, k)]
                zeilen.append({"zielgroesse": ziel, "verfahren": name, "fold": k,
                               "getunt_auf": RATE, "tuning_sekunden": dauer,
                               **{s.split("__")[-1]: w for s, w in p.items()},
                               "parameter_json": json.dumps(p)})
    df = pd.DataFrame(zeilen)
    df.to_csv(OUT / "tuning.csv", index=False)
    return df


def _rein_python(p: dict) -> dict:
    """Wandelt NumPy-Skalare in native Typen, bevor sie nach JSON gehen.

    Ein:  Parameter-dict aus best_params_
    Aus:  dasselbe dict mit int/float

    - np.float64 erbt von float und ueberlebt json.dumps zufaellig, np.int64 erbt
      nicht von int
    - mit default=str wuerde aus 287 die Zeichenkette "287", und
      set_params(n_estimators="287") braeche nach dem Tuning ab
    - ob es auftritt, haengt an der Paketversion; deshalb explizit wandeln und
      ohne default=, damit ein unbekannter Typ auffaellt (B-23)
    """
    return {schluessel: (wert.item() if isinstance(wert, np.generic) else wert)
            for schluessel, wert in p.items()}


def _parameter_je_fold(parameter: pd.DataFrame) -> dict:
    """Liest tuning.csv als Nachschlagetabelle.

    Ein:  tuning.csv als Datenrahmen
    Aus:  {(zielgroesse, verfahren, fold): Parameter-dict}
    """
    return {(z["zielgroesse"], z["verfahren"], int(z["fold"])):
            json.loads(z["parameter_json"])
            for _, z in parameter.iterrows()}


def phase_bewertung(panel: pd.DataFrame, parameter: pd.DataFrame,
                    selten: pd.Series) -> pd.DataFrame:
    """Phase 2: 10 Wiederholungen x 5 Folds x 3 Verfahren x 2 Zielgroessen.

    Ein:  Panel, Parametertabelle aus Phase 1
    Aus:  menge_folds.csv mit 300 Zeilen und menge_vorhersagen.parquet
          mit einer Zeile je Vorhersage

    - trainiert wird je Fold auf allen Trainingsstadtteilen
    - mit den Parametern aus Phase 1, aber einem frischen Modell: best_estimator_
      aus dem Tuning waere auf nur drei Vierteln der Trainingsstadtteile gefittet
    """
    param = _parameter_je_fold(parameter)
    zeilen, vorhersagen = [], []
    for w in range(WIEDERHOLUNGEN):
        d = wiederholte_aufteilung(panel, wiederholung=w, selten=selten)
        for k in range(1, N_FOLDS + 1):
            tr, te = fold_masken(d, k)
            train, test = d[tr], d[te]
            for ziel in ZIELE:
                for name in VERFAHREN:
                    # Parallelmessung in jedem Lauf - keine Ausnahmen.
                    z = ein_lauf(name, param[(ziel, name, k)], train, test,
                                 ziel, auch_parallel=True,
                                 mit_vorhersagen=True)
                    vorhersagen.append(z.pop("_vorhersagen")
                                        .assign(wiederholung=w, fold=k))
                    zeilen.append({"wiederholung": w, "fold": k, **z})
        print(f"    Wiederholung {w}: {len(zeilen):>3} Laeufe")
    df = pd.DataFrame(zeilen)
    spalten = (["zielgroesse", "verfahren", "wiederholung", "fold",
                "RMSE", "MAE", "R2", "RMSE_train", "R2_train",
                "train_sekunden", "inferenz_sekunden",
                "train_sekunden_parallel", "inferenz_sekunden_parallel",
                "parallel_abweichung", "parallel_abweichung_rmse",
                "n_train", "n_test", "extrapolationsanteil",
                "n_negativ", "y_hat_min"])
    df = df[spalten]
    df.to_csv(OUT / "menge_folds.csv", index=False)
    pd.concat(vorhersagen, ignore_index=True).to_parquet(
        OUT / "menge_vorhersagen.parquet", index=False)
    return df


MASSE = ["RMSE", "MAE", "R2", "train_sekunden", "inferenz_sekunden"]
# Aus denselben 50 Laeufen wie die einkernigen Zeiten.
MASSE_PARALLEL = ["train_sekunden_parallel", "inferenz_sekunden_parallel"]


def aggregiere(folds: pd.DataFrame) -> pd.DataFrame:
    """Phase 3: zweistufig mitteln, erst je Wiederholung, dann darueber.

    Ein:  menge_folds.csv als Datenrahmen
    Aus:  menge_mittel.csv mit std_folds und std_wiederholungen

    - die 50 Fold-Ergebnisse sind nicht unabhaengig: dieselben 30 Stadtteile in
      zehn Gruppierungen. Ein Intervall aus std_folds/sqrt(50) waere zu eng (R-5)
    - massgeblich ist std_wiederholungen
    - beide Spalten wandern mit, damit der Unterschied sichtbar bleibt
    """
    schluessel = ["zielgroesse", "verfahren"]
    g = folds.groupby(schluessel, sort=False)
    z = g[MASSE].mean().add_suffix("_mean")
    z = z.join(g[MASSE].std().add_suffix("_std_folds"))
    je_wdh = folds.groupby(schluessel + ["wiederholung"], sort=False)[MASSE].mean()
    z = z.join(je_wdh.groupby(schluessel, sort=False).std()
                     .add_suffix("_std_wiederholungen"))
    z = z.join(g[MASSE_PARALLEL].mean().add_suffix("_mean"))
    # Parallelisierungsgewinn: Faktor, um den der Fit ueber alle Kerne
    # schneller ist. Bei Ridge zu erwarten: rund 1 - eine geschlossene Loesung
    # hat nichts zu verteilen. Das ist selbst eine Aussage fuer UF4.
    # Beide Zeiten stammen aus denselben 50 Laeufen.
    z["parallel_gewinn"] = (z["train_sekunden_mean"]
                            / z["train_sekunden_parallel_mean"])
    # Groesste Abweichung zwischen einkernigem und parallelem Modell. Null
    # heisst threadunabhaengig; alles darueber ist ein Reproduzierbarkeits-
    # befund und gehoert berichtet (B-24).
    z["parallel_abweichung_max"] = g["parallel_abweichung"].max()
    z = z.join(g[["extrapolationsanteil"]].mean())
    z = z.join(g[["n_negativ"]].sum().rename(columns={"n_negativ": "n_negativ_gesamt"}))

    # UEBERANPASSUNG: Trainingsguete und der Abstand zur Testguete. Ein grosser
    # positiver Wert heisst, das Modell erklaert die Trainingsstadtteile viel
    # besser als unbekannte - genau das ist Ueberanpassung. Beim Poisson-GLM
    # und bei Ridge ist ein kleiner Abstand zu erwarten, bei den Baumverfahren
    # ein grosser (docs/06_RISIKEN.md, R-2).
    z = z.join(g[["RMSE_train", "R2_train"]].mean())
    z["ueberanpassung_RMSE"] = z["RMSE_mean"] - z["RMSE_train"]
    z["ueberanpassung_R2"] = z["R2_train"] - z["R2_mean"]

    spalten = [f"{m}{s}" for m in MASSE for s in
               ("_mean", "_std_folds", "_std_wiederholungen")]
    spalten += ([f"{m}_mean" for m in MASSE_PARALLEL]
                + ["parallel_gewinn", "parallel_abweichung_max"])
    z = z[spalten + ["RMSE_train", "R2_train", "ueberanpassung_RMSE",
                     "ueberanpassung_R2",
                     "extrapolationsanteil", "n_negativ_gesamt"]].reset_index()
    z.round(4).to_csv(OUT / "menge_mittel.csv", index=False)
    return z


# ---------------------------------------------------------------------------
# FALLSTRICK 2  Mehrfachvergleiche (R-10)
# ---------------------------------------------------------------------------
def _holm(p: np.ndarray) -> np.ndarray:
    """Holm-Bonferroni ueber eine Testfamilie.

    Ein:  Liste von p-Werten
    Aus:  angepasste p-Werte, direkt gegen alpha pruefbar

    - p-Werte aufsteigend, kleinster gegen alpha/m, dann alpha/(m-1), bis zur
      ersten Nichtablehnung
    - uniform staerker als Bonferroni bei gleicher Fehlerkontrolle
    """
    m = len(p)
    ordnung = np.argsort(p)
    angepasst = np.empty(m, float)
    laufend = 0.0
    for rang, i in enumerate(ordnung):
        laufend = max(laufend, (m - rang) * p[i])
        angepasst[i] = min(laufend, 1.0)
    return angepasst


def _gepaart(a: np.ndarray, b: np.ndarray) -> dict:
    """Gepaarter Wilcoxon samt der Kennzahlen, die ohne p-Wert tragen.

    Ein:  zwei gepaarte Wertereihen (a = Verfahren, b = Gegner)
    Aus:  dict mit p-Wert, mittlerer Differenz, Konfidenzintervall, Siegen

    - bei RMSE ist klein besser; die Differenz b - a ist der Vorteil von a
    """
    from scipy.stats import t, wilcoxon

    diff = np.asarray(b, float) - np.asarray(a, float)
    n = len(diff)
    mittel = float(diff.mean())
    if n > 1 and diff.std(ddof=1) > 0:
        halb = float(t.ppf(1 - ALPHA / 2, n - 1) * diff.std(ddof=1) / np.sqrt(n))
    else:
        halb = 0.0
    try:
        p = float(wilcoxon(diff, zero_method="wilcox").pvalue)
    except ValueError:            # alle Differenzen null
        p = 1.0
    return {"n_paare": n, "differenz_mittel": mittel,
            "ci_unten": mittel - halb, "ci_oben": mittel + halb,
            "gewonnene": int((diff > 0).sum()), "wilcoxon_p": p}


def vergleiche(folds: pd.DataFrame, baselines: pd.DataFrame) -> pd.DataFrame:
    """Phase 4: gepaarter Wilcoxon auf RMSE, zwei Rollen und zwei Teststufen.

    Ein:  menge_folds.csv, Baseline-Laeufe aus v1_baselines.py
    Aus:  vergleich.csv

    - Rolle `primaer`: jedes Verfahren gegen die Stufe-2-Baseline (6 Tests). Keine
      Familie, weil jede Frage nach #34 vorab einzeln formuliert ist; keine
      Korrektur
    - Rolle `sekundaer`: jedes Verfahrenspaar (6 Tests). Eine Familie, darauf
      Holm-Bonferroni
    - Teststufe `wiederholung` (n = 10) ist der Primaertest; die 50 Einzellaeufe
      sind Pseudoreplikation und liefern zu kleine p-Werte (B-5)
    - Teststufe `lauf` (n = 50) laeuft als gekennzeichnete Sensitivitaet mit
    - auch die zehn Wiederholungsmittel sind nicht unabhaengig; das Intervall ist
      enger als die wahre Unsicherheit (Nadeau & Bengio 2003)
    - deshalb stehen mittlere Differenz, Intervall und gewonnene Laeufe immer
      daneben, unabhaengig vom p-Wert
    """
    basis = baselines[baselines["modell"] == BASELINE_STUFE2]
    zeilen = []

    for stufe, schluessel in (("wiederholung", ["wiederholung"]),
                              ("lauf", ["wiederholung", "fold"])):
        # Auf der Stufe "wiederholung" wird je Wiederholung ueber die 5 Folds
        # gemittelt - dieselbe zweistufige Logik wie in aggregiere().
        v = (folds.groupby(["zielgroesse", "verfahren"] + schluessel,
                           sort=False)[TESTMASS].mean().rename("wert").reset_index())
        b = (basis.groupby(["zielgroesse"] + schluessel,
                           sort=False)[TESTMASS].mean().rename("wert").reset_index())

        for ziel in ZIELE:
            # GEPAART heisst: auf denselben Laeufen. Deshalb wird ueber die
            # Schluessel VERBUNDEN und nicht auf gleiche Reihenfolge vertraut -
            # sonst subtrahiert man stillschweigend verschiedene Testmengen
            # voneinander. Fehlt ein Gegenstueck, bricht der Lauf ab.
            reihen = {n: (v[(v["zielgroesse"] == ziel) & (v["verfahren"] == n)]
                          .set_index(schluessel)["wert"]) for n in VERFAHREN}
            gegner = b[b["zielgroesse"] == ziel].set_index(schluessel)["wert"]

            def paar(links: pd.Series, rechts: pd.Series) -> dict:
                """Legt eine Vergleichszeile fuer vergleich.csv an.

                Ein:  Rolle, Teststufe, Zielgroesse, Verfahren, Gegner, Wertereihen
                Aus:  dict mit Testergebnis und Kennzahlen
                """
                zusammen = pd.concat([links.rename("a"), rechts.rename("b")],
                                     axis=1, join="inner")
                fehlend = max(len(links), len(rechts)) - len(zusammen)
                assert not fehlend, (
                    f"{fehlend} Laeufe ohne Gegenstueck bei {ziel}, Stufe "
                    f"{stufe} - Verfahren und Baseline liefen auf "
                    f"unterschiedlichen Aufteilungen.")
                return _gepaart(zusammen["a"].to_numpy(), zusammen["b"].to_numpy())

            for name in VERFAHREN:                       # primaer
                zeilen.append({"teststufe": stufe, "zielgroesse": ziel,
                               "paarung": f"{name} vs {BASELINE_STUFE2}",
                               "rolle": "primaer", "mass": TESTMASS,
                               **paar(reihen[name], gegner),
                               "n_tests_familie": 1})

            for i, a in enumerate(VERFAHREN):            # sekundaer
                for c in VERFAHREN[i + 1:]:
                    zeilen.append({"teststufe": stufe, "zielgroesse": ziel,
                                   "paarung": f"{a} vs {c}",
                                   "rolle": "sekundaer", "mass": TESTMASS,
                                   **paar(reihen[a], reihen[c]),
                                   "n_tests_familie": 3 * len(ZIELE)})

    df = pd.DataFrame(zeilen)

    # Holm je Teststufe getrennt, nur innerhalb der sekundaeren Familie.
    # ZWEI FAMILIEN, nicht sieben Tests: Regression und Klassifikation
    # beantworten verschiedene Teilfragen (Entscheidung 05.08.2026, B-6).
    # m03_struktur.py hat genau einen Test und wird nicht korrigiert.
    df["p_holm"] = np.nan
    for stufe in df["teststufe"].unique():
        maske = (df["rolle"] == "sekundaer") & (df["teststufe"] == stufe)
        df.loc[maske, "p_holm"] = _holm(df.loc[maske, "wilcoxon_p"].to_numpy())
    df["signifikant"] = np.where(
        df["rolle"] == "primaer", df["wilcoxon_p"] < ALPHA, df["p_holm"] < ALPHA)

    df.round(6).to_csv(OUT / "vergleich.csv", index=False)
    return df


def leakage_diagnose(folds: pd.DataFrame, baselines: pd.DataFrame) -> pd.DataFrame:
    """Beziffert, was das Tuning auf Wiederholung 0 kostet (B-21).

    Ein:  menge_folds.csv, Baseline-Laeufe
    Aus:  Datenrahmen mit dem Vorsprung in W0 gegen W1-9

    - in W0 stammen die Parameter aus dem Trainingssatz genau dieses Folds
    - in W1-9 werden dieselben Parameter auf andere Aufteilungen angewandt; im
      Mittel waren dort 78 % der Teststadtteile in der Suchmenge
    - waere der Effekt bedeutsam, muesste der Vorsprung in W1-9 systematisch
      groesser ausfallen
    - bewusst schwache Diagnose: W0 ist auch eine andere Aufteilung, der
      Unterschied ist konfundiert. Ein deutlicher Effekt waere sichtbar, ein
      kleiner nicht von Fold-Schwankung zu trennen
    - kostet keine zusaetzliche Rechenzeit
    """
    basis = (baselines[baselines["modell"] == BASELINE_STUFE2]
             .set_index(["zielgroesse", "wiederholung", "fold"])[TESTMASS])
    zeilen = []
    for (ziel, name), g in folds.groupby(["zielgroesse", "verfahren"], sort=False):
        g = g.set_index(["zielgroesse", "wiederholung", "fold"])
        # Positiver Vorsprung = das Verfahren ist besser als die Baseline.
        vorsprung = basis.reindex(g.index) - g[TESTMASS]
        w0 = vorsprung.xs(0, level="wiederholung")
        rest = vorsprung[vorsprung.index.get_level_values("wiederholung") > 0]
        zeilen.append({
            "zielgroesse": ziel, "verfahren": name, "mass": TESTMASS,
            "vorsprung_w0": float(w0.mean()), "n_w0": int(len(w0)),
            "vorsprung_w1_9": float(rest.mean()), "n_w1_9": int(len(rest)),
            "differenz": float(rest.mean() - w0.mean()),
            "differenz_in_std_folds": float((rest.mean() - w0.mean())
                                            / g[TESTMASS].std())})
    df = pd.DataFrame(zeilen)
    df.round(4).to_csv(OUT / "leakage_diagnose.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# FALLSTRICK 4  Das Hold-out
# ---------------------------------------------------------------------------
def hold_out(panel: pd.DataFrame, parameter: pd.DataFrame,
             folds: pd.DataFrame, selten: pd.Series) -> pd.DataFrame:
    """Einmalige Schlussbewertung: 30 Stadtteile trainieren, 6 bewerten.

    Ein:  vollstaendiges Panel, Parametertabelle aus Phase 1
    Aus:  holdout.csv mit Spalte fold_der_parameter

    - das Tuning liefert fuenf Parametersaetze je Zielgroesse und Verfahren; die
      Spezifikation legt nicht fest, welcher gilt (B-14)
    - gewaehlt ist der Satz des Folds mit dem niedrigsten RMSE in Wiederholung 0:
      deterministisch und ausschliesslich aus Entwicklungsdaten
    - beide Baselines laufen mit; ohne Bezugspunkt ist ein RMSE von 23,7 keine
      Aussage (B-38)
    - zu berichten ist, dass dies EINE Messung an SECHS Einheiten ist: kein
      Mittelwert, keine Streuung, deutlich unsicherer als die
      Kreuzvalidierungswerte (R-4)
    """
    param = _parameter_je_fold(parameter)
    dev, ho = entwicklung_und_holdout(panel)
    train, test = panel[dev], panel[ho]
    print(f"    Training auf {train['stadtteil'].nunique()} Stadtteilen "
          f"({len(train):,} Zeilen), Bewertung auf "
          f"{test['stadtteil'].nunique()} ({len(test):,} Zeilen)")

    # DIE BASELINES GEHOEREN DAZU: Ein RMSE von 23,7 ist ohne Referenz keine
    # Aussage (B-38), und die Primaeraussage nach #34 lautet "Verfahren gegen
    # Stufe-2-Baseline". Sie haben keine Hyperparameter - es gibt nichts zu
    # waehlen und damit nichts, was der Hold-out beeinflussen koennte.
    from v1_baselines import (NULLMARKE, POISSON, bewerte_regression,
                              poisson_glm)

    t = time.perf_counter()
    anzahl = poisson_glm(train, test)
    baseline_sek = time.perf_counter() - t
    bev = test[EXPOSURE_ROH].astype(float).to_numpy()

    zeilen = []
    for ziel, referenz in ((ZIELGROESSE, anzahl), (RATE, anzahl / bev * 1000)):
        y = test[ziel].astype(float)
        for stufe, modell, y_hat in (
                (1, NULLMARKE, np.full(len(test), train[ziel].astype(float).mean())),
                (2, POISSON, referenz)):
            zeilen.append({
                "verfahren": modell, "zielgroesse": ziel, "stufe": stufe,
                **bewerte_regression(y, y_hat),
                "train_sekunden": round(baseline_sek, 4) if stufe == 2 else 0.0,
                "n_train": len(train), "n_test": len(test),
                "n_negativ": int((np.asarray(y_hat) < 0).sum()),
                "n_stadtteile_test": int(test["stadtteil"].nunique())})

    w0 = folds[folds["wiederholung"] == 0]
    for ziel in ZIELE:
        for name in VERFAHREN:
            g = w0[(w0["zielgroesse"] == ziel) & (w0["verfahren"] == name)]
            bester = int(g.loc[g["RMSE"].idxmin(), "fold"])
            z = ein_lauf(name, param[(ziel, name, bester)], train, test, ziel)
            zeilen.append({**z, "stufe": 3, "fold_der_parameter": bester,
                           "n_stadtteile_test": int(test["stadtteil"].nunique())})
    df = pd.DataFrame(zeilen)
    df.round(6).to_csv(OUT / "holdout.csv", index=False)
    return df


# ---------------------------------------------------------------------------
# Anschlusslauf: Phase 1 und 2 uebernehmen statt neu rechnen
# ---------------------------------------------------------------------------
def uebernehmen(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Liest Tuning und Bewertung aus results/, statt sie neu zu rechnen.

    Ein:  Entwicklungspanel; liest tuning.csv und menge_folds.csv
    Aus:  dieselben zwei Datenrahmen, die Phase 1 und Phase 2 zurueckgeben

    - NUR ueber den Schalter --weiter erreichbar. Ohne ihn rechnet das Skript
      unveraendert alles neu; ein stiller Cache waere genau der Fehler vom
      30.08.2026 (crime_index_monatlich.csv aus einem Lauf mit falscher
      Wohnbevoelkerung)
    - WOZU: "holdout" rechnet die Phasen 1 bis 4 mit, obwohl nur Phase 5 fehlt.
      Bei unveraenderten Daten und unveraenderter Konfiguration sind das 149
      von 150 Minuten fuer ein Ergebnis, das fertig auf der Platte liegt
    - die Uebernahme ist an fuenf Bedingungen geknuepft; jede bricht ab, statt
      mit fremden Zahlen weiterzurechnen. Die wichtigste ist der Zeitstempel:
      Ist regression.parquet oder config_modelle.py neuer als tuning.csv,
      koennen Daten, Suchraum oder Budget andere sein
    - was die Pruefungen NICHT leisten: Sie sehen die Form, nicht den Inhalt
      der Parquet-Datei. Eine zurueckkopierte aeltere Datei mit derselben
      Zeilenzahl faellt nicht auf. Der Schalter ist fuer den Anschlusslauf
      desselben Tages gedacht, nicht als Dauereinrichtung
    """
    konfig = Path(__file__).resolve().parent / "config_modelle.py"
    fehlend = [d for d in ("tuning.csv", "menge_folds.csv")
               if not (OUT / d).exists()]
    if fehlend:
        raise SystemExit(f"--weiter: {', '.join(fehlend)} fehlt in "
                         f"{OUT.relative_to(ROOT)}. Erst ohne --weiter laufen.")
    parameter = pd.read_csv(OUT / "tuning.csv")
    folds = pd.read_csv(OUT / "menge_folds.csv")
    erwartet = len(ZIELE) * len(VERFAHREN) * WIEDERHOLUNGEN * N_FOLDS
    n = int(folds["n_train"].iloc[0] + folds["n_test"].iloc[0])
    juenger = max(konfig.stat().st_mtime, PFAD_REGRESSION.stat().st_mtime)
    for erfuellt, meldung in (
            (juenger <= (OUT / "tuning.csv").stat().st_mtime,
             "regression.parquet oder config_modelle.py ist neuer als "
             "tuning.csv - Daten, Suchraum oder Budget koennen andere sein"),
            (len(folds) == erwartet,
             f"menge_folds.csv hat {len(folds)} statt {erwartet} Zeilen"),
            (n == len(panel),
             f"die uebernommenen Laeufe stammen aus einem Panel mit {n} "
             f"Zeilen, das aktuelle hat {len(panel)}"),
            (set(parameter["verfahren"]) == set(VERFAHREN),
             f"tuning.csv fuehrt {sorted(set(parameter['verfahren']))} "
             f"statt {sorted(VERFAHREN)}"),
            (sorted(int(f) for f in parameter["fold"].unique())
             == list(range(1, N_FOLDS + 1)),
             f"tuning.csv fuehrt die Folds "
             f"{sorted(int(f) for f in parameter['fold'].unique())}")):
        if not erfuellt:
            raise SystemExit(f"--weiter abgebrochen: {meldung}. "
                             f"Ohne --weiter neu rechnen.")
    print(f"  Phase 1+2 uebernommen aus {OUT.relative_to(ROOT)}: "
          f"{len(parameter)} Parametersaetze, {len(folds)} Laeufe, "
          f"Panel {n} Zeilen")
    return parameter, folds


def main(argv: list[str]) -> int:
    """Faehrt die vier Phasen und schreibt alle Ergebnisdateien.

    Ein:  regression.parquet; Argument "holdout" haengt die Schlussbewertung
          an, "--weiter" uebernimmt Phase 1 und 2 aus results/regression/
    Aus:  tuning.csv, menge_folds.csv, menge_mittel.csv, vergleich.csv,
          leakage_diagnose.csv, optional holdout.csv; Exitcode

    - ohne das Argument werden die Hold-out-Zeilen zu Beginn und unwiderruflich
      herausgefiltert, bevor irgendetwas rechnet
    """
    if not PFAD_REGRESSION.exists():
        raise SystemExit(f"{PFAD_REGRESSION.relative_to(ROOT)} fehlt - "
                         f"erst 'python prep/build.py' ausfuehren.")
    if not (OUT / "baselines_folds.csv").exists():
        raise SystemExit("results/regression/baselines_folds.csv fehlt - "
                         "erst 'python vorpruefung/v1_baselines.py' ausfuehren.")
    OUT.mkdir(parents=True, exist_ok=True)

    voll = pd.read_parquet(PFAD_REGRESSION)
    selten = selten_je_stadtteil(pd.read_parquet(PFAD_KLASSIFIKATION))

    # FALLSTRICK 4, konstruktiv: Ohne das Argument "holdout" wird der Datensatz
    # HIER auf die Entwicklungsstadtteile eingeschraenkt. Alles Folgende kann
    # die Hold-out-Zeilen nicht mehr sehen, auch nicht versehentlich.
    panel = voll[voll["ist_holdout"] == 0].reset_index(drop=True)
    print(f"  Entwicklung: {len(panel):,} Zeilen | "
          f"{panel['stadtteil'].nunique()} Stadtteile\n")

    if "--weiter" in argv:
        parameter, folds = uebernehmen(panel)
    else:
        print("  Phase 1  Tuning")
        parameter = phase_tuning(panel, selten)
        print("\n  Phase 2  Bewertung")
        folds = phase_bewertung(panel, parameter, selten)
    print("\n  Phase 3  Aggregation")
    mittel = aggregiere(folds)
    print(mittel.to_string(index=False))
    auffaellig = mittel[mittel["parallel_abweichung_max"] > 0]
    if len(auffaellig):
        print("\n  HINWEIS zur Reproduzierbarkeit: Bei folgenden Verfahren "
              "haengt die Vorhersage von der Kernzahl ab.")
        print("  Die berichteten Werte stammen aus dem einkernigen Fit und "
              "sind davon unberuehrt (docs/07_BEFUNDE.md, B-24).")
        for _, z in auffaellig.iterrows():
            print(f"    {z['verfahren']:<14} {z['zielgroesse']:<21} "
                  f"groesste Abweichung {z['parallel_abweichung_max']:.3g}")
    print("\n  Phase 4  Vergleich")
    basislinien = pd.read_csv(OUT / "baselines_folds.csv")
    v = vergleiche(folds, basislinien)
    print(v[v["teststufe"] == "wiederholung"]
          [["zielgroesse", "paarung", "rolle", "differenz_mittel",
            "gewonnene", "wilcoxon_p", "p_holm", "signifikant"]]
          .to_string(index=False))

    print("\n  Diagnose zum Tuning auf Wiederholung 0 (B-21):")
    print(leakage_diagnose(folds, basislinien).to_string(index=False))

    if "holdout" in argv:
        print("\n  Phase 5  Hold-out - EINMALIGE Schlussbewertung")
        print(hold_out(voll, parameter, folds, selten).to_string(index=False))
    else:
        print("\n  Hold-out unberuehrt. Fuer die Schlussbewertung:"
              "\n  python modelle/m02_menge.py holdout")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
