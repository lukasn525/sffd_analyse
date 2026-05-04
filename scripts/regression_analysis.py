"""
Regressionsanalyse v2: Soziooekonomische Faktoren vs. Einsatzdauer
Ansatz: OLS mit Kontrollvariablen + getrennte Analyse nach Einsatzart
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np
import statsmodels.api as sm
import warnings
warnings.filterwarnings("ignore")

DATA_PATH = r"c:\Users\lukas\Git\sffd_analyse\data\processed\sffd_acs_joined.parquet"

SOCIO_VARS = [
    "median_household_income",
    "poverty_rate",
    "median_gross_rent",
    "bachelor_rate",
    "vacancy_rate",
]

CONTROL_VARS = ["hour", "is_night", "is_weekend"]

SEP = "-" * 70


# ---------------------------------------------------------------------------
# Laden & aufbereiten
# ---------------------------------------------------------------------------

def load_and_clean():
    df = pd.read_parquet(DATA_PATH)
    print(f"Rohdatensatz: {len(df):,} Zeilen, {df.shape[1]} Spalten")

    before = len(df)
    df = df[(df["response_time_min"] > 0) & (df["response_time_min"] < 60)]
    print(f"Nach Filter response_time (0–60 min): {len(df):,} ({before - len(df):,} entfernt)")

    df["incident_code"] = df["primary_situation"].str.extract(r"^(\d{3})").astype(float)
    df["incident_category"] = pd.cut(
        df["incident_code"],
        bins=[100, 200, 300, 400, 500, 600, 700, 800],
        labels=["Feuer", "Ueberhitzung", "Rettung_EMS", "Gefahrstoffe",
                "Service", "Suche_Sonstiges", "Fehlalarm"],
        right=False,
    )

    required = SOCIO_VARS + CONTROL_VARS + ["response_time_min", "incident_category"]
    df = df.dropna(subset=required)
    print(f"Nach Dropna: {len(df):,} Zeilen\n")
    return df


# ---------------------------------------------------------------------------
# Hilfsfunktion: OLS ausgeben
# ---------------------------------------------------------------------------

def print_ols(model, label):
    print(f"\n  [{label}]")
    print(f"  N = {int(model.nobs):,}   R2 = {model.rsquared:.4f}   "
          f"adj. R2 = {model.rsquared_adj:.4f}   "
          f"F-p = {model.f_pvalue:.4f}")
    print(f"\n  {'Variable':<32} {'beta':>8}  {'SE':>7}  {'p':>9}  Sig.")
    print("  " + "-" * 65)
    for var, row in pd.DataFrame(
        {"coef": model.params, "se": model.bse, "p": model.pvalues}
    ).iterrows():
        stars = ("***" if row["p"] < 0.001 else
                 "**"  if row["p"] < 0.01  else
                 "*"   if row["p"] < 0.05  else
                 "."   if row["p"] < 0.1   else " ")
        print(f"  {var:<32} {row['coef']:>+8.4f}  {row['se']:>7.4f}  {row['p']:>9.5f}  {stars}")


def build_X(df, add_cat_dummies=True):
    """Standardisierte Soziovariablen + Kontrollvariablen (optional Kategorie-Dummies)."""
    X_socio = (df[SOCIO_VARS] - df[SOCIO_VARS].mean()) / df[SOCIO_VARS].std()
    X_ctrl  = df[CONTROL_VARS].copy()

    if add_cat_dummies:
        cat_dummies = pd.get_dummies(
            df["incident_category"], prefix="cat", drop_first=True, dtype=float
        )
        X = pd.concat([X_socio, X_ctrl, cat_dummies], axis=1)
    else:
        X = pd.concat([X_socio, X_ctrl], axis=1)

    return sm.add_constant(X)


# ---------------------------------------------------------------------------
# Modell A: Voller Datensatz mit Kontrollvariablen
# ---------------------------------------------------------------------------

def model_full(df):
    print(f"\n{'='*70}")
    print("MODELL A  –  Vollständiger Datensatz MIT Kontrollvariablen")
    print(f"{'='*70}")
    print("  AV: response_time_min")
    print("  UVs: Sozioökon. Vars (std.) + Einsatzkategorie-Dummies + hour + is_night + is_weekend")

    X = build_X(df, add_cat_dummies=True)
    y = df["response_time_min"]
    model = sm.OLS(y, X).fit(cov_type="HC3")
    print_ols(model, "Alle Einsätze")

    # Vergleich: gleiches Modell OHNE Kontrollvariablen (Basismodell)
    X_base = sm.add_constant(
        (df[SOCIO_VARS] - df[SOCIO_VARS].mean()) / df[SOCIO_VARS].std()
    )
    model_base = sm.OLS(y, X_base).fit(cov_type="HC3")
    print(f"\n  Vergleich Basismodell (ohne Controls):")
    print(f"  R2 = {model_base.rsquared:.4f}  -->  Mit Controls: R2 = {model.rsquared:.4f}  "
          f"(Delta R2 = +{model.rsquared - model_base.rsquared:.4f})")
    return model, model_base


# ---------------------------------------------------------------------------
# Modell B: Nur Feuereinsätze (Code 100–199)
# ---------------------------------------------------------------------------

def model_fire(df):
    print(f"\n{'='*70}")
    print("MODELL B  –  Nur Feuereinsätze (Codes 100–199)")
    print(f"{'='*70}")

    fire = df[df["incident_code"].between(100, 199)].copy()
    print(f"  Stichprobe: {len(fire):,} Feuereinsätze")
    print(f"  response_time_min: Median={fire['response_time_min'].median():.2f}  "
          f"Mittelw={fire['response_time_min'].mean():.2f}  Std={fire['response_time_min'].std():.2f}")
    print("  AV: response_time_min | UVs: Sozioökon. (std.) + hour + is_night + is_weekend")

    # Feinere Feuer-Subkategorien als zusätzliche Controls
    fire_sub = pd.get_dummies(
        fire["primary_situation"].str.extract(r"^(\d{3})")[0],
        prefix="fcode", drop_first=True, dtype=float
    )
    X_socio = (fire[SOCIO_VARS] - fire[SOCIO_VARS].mean()) / fire[SOCIO_VARS].std()
    X_ctrl  = fire[CONTROL_VARS].copy()
    X = sm.add_constant(pd.concat([X_socio, X_ctrl, fire_sub], axis=1))
    y = fire["response_time_min"]

    model = sm.OLS(y, X).fit(cov_type="HC3")

    # Nur Sozio-Koeffizienten ausgeben (Subkategorien weglassen)
    print(f"\n  N = {int(model.nobs):,}   R2 = {model.rsquared:.4f}   "
          f"adj. R2 = {model.rsquared_adj:.4f}   F-p = {model.f_pvalue:.6f}")
    print(f"\n  Sozioökonom. Koeffizienten (netto, nach Controls):")
    print(f"  {'Variable':<32} {'beta':>8}  {'SE':>7}  {'p':>9}  Sig.")
    print("  " + "-" * 65)
    for var in ["const"] + SOCIO_VARS:
        if var not in model.params:
            continue
        b, se, p = model.params[var], model.bse[var], model.pvalues[var]
        stars = ("***" if p < 0.001 else "**" if p < 0.01 else
                 "*" if p < 0.05 else "." if p < 0.1 else " ")
        print(f"  {var:<32} {b:>+8.4f}  {se:>7.4f}  {p:>9.5f}  {stars}")

    # Poverty-Quartil-Vergleich innerhalb Feuereinsätze
    print(f"\n  Mittlere Einsatzdauer nach Poverty-Quartil (Feuereinsätze):")
    fire["pov_q"] = pd.qcut(fire["poverty_rate"], 4,
                             labels=["Q1 arm", "Q2", "Q3", "Q4 reich"])
    pov_rt = fire.groupby("pov_q", observed=True)["response_time_min"].agg(["mean","median","count"])
    pov_rt.columns = ["Mittelwert", "Median", "N"]
    print(pov_rt.round(2).to_string())

    return model, fire


# ---------------------------------------------------------------------------
# Modell C: Nur EMS-Einsätze (Code 300–399)
# ---------------------------------------------------------------------------

def model_ems(df):
    print(f"\n{'='*70}")
    print("MODELL C  –  Nur EMS-/Rettungseinsätze (Codes 300–399)")
    print(f"{'='*70}")

    ems = df[df["incident_code"].between(300, 399)].copy()
    print(f"  Stichprobe: {len(ems):,} EMS-Einsätze")
    print(f"  response_time_min: Median={ems['response_time_min'].median():.2f}  "
          f"Mittelw={ems['response_time_min'].mean():.2f}  Std={ems['response_time_min'].std():.2f}")
    print("  AV: response_time_min | UVs: Sozioökon. (std.) + hour + is_night + is_weekend")

    ems_sub = pd.get_dummies(
        ems["primary_situation"].str.extract(r"^(\d{3})")[0],
        prefix="ecode", drop_first=True, dtype=float
    )
    X_socio = (ems[SOCIO_VARS] - ems[SOCIO_VARS].mean()) / ems[SOCIO_VARS].std()
    X_ctrl  = ems[CONTROL_VARS].copy()
    X = sm.add_constant(pd.concat([X_socio, X_ctrl, ems_sub], axis=1))
    y = ems["response_time_min"]

    model = sm.OLS(y, X).fit(cov_type="HC3")

    print(f"\n  N = {int(model.nobs):,}   R2 = {model.rsquared:.4f}   "
          f"adj. R2 = {model.rsquared_adj:.4f}   F-p = {model.f_pvalue:.6f}")
    print(f"\n  Sozioökonom. Koeffizienten (netto, nach Controls):")
    print(f"  {'Variable':<32} {'beta':>8}  {'SE':>7}  {'p':>9}  Sig.")
    print("  " + "-" * 65)
    for var in ["const"] + SOCIO_VARS:
        if var not in model.params:
            continue
        b, se, p = model.params[var], model.bse[var], model.pvalues[var]
        stars = ("***" if p < 0.001 else "**" if p < 0.01 else
                 "*" if p < 0.05 else "." if p < 0.1 else " ")
        print(f"  {var:<32} {b:>+8.4f}  {se:>7.4f}  {p:>9.5f}  {stars}")

    print(f"\n  Mittlere Einsatzdauer nach Poverty-Quartil (EMS):")
    ems["pov_q"] = pd.qcut(ems["poverty_rate"], 4,
                            labels=["Q1 arm", "Q2", "Q3", "Q4 reich"])
    pov_rt = ems.groupby("pov_q", observed=True)["response_time_min"].agg(["mean","median","count"])
    pov_rt.columns = ["Mittelwert", "Median", "N"]
    print(pov_rt.round(2).to_string())

    return model, ems


# ---------------------------------------------------------------------------
# Modell D: Feuerrate nach Sozioökonomie (Neighborhood-Ebene)
# ---------------------------------------------------------------------------

def model_firerate(df):
    print(f"\n{'='*70}")
    print("MODELL D  –  Feuerrate auf Neighborhood-Ebene")
    print(f"{'='*70}")
    print("  AV: Anteil echter Feuereinsätze (%) je Neighborhood")
    print("  UVs: Sozioökon. Vars (std.)")

    fire_flag = df["incident_code"].between(100, 199)
    nb = df.groupby("neighborhood").agg(
        total_incidents=("incident_number", "count"),
        fire_incidents=("incident_number", lambda x: fire_flag.loc[x.index].sum()),
        **{v: (v, "first") for v in SOCIO_VARS},
        total_population=("total_population", "first"),
    ).reset_index()

    nb["fire_rate_pct"] = (nb["fire_incidents"] / nb["total_incidents"] * 100).round(3)
    nb = nb[nb["total_population"] > 100]

    print(f"\n  Neighborhoods: {len(nb)}")
    print(f"  Feuerrate: Median={nb['fire_rate_pct'].median():.2f}%  "
          f"Min={nb['fire_rate_pct'].min():.2f}%  Max={nb['fire_rate_pct'].max():.2f}%")
    print(f"\n  Top-5 Neighborhoods nach Feuerrate:")
    top5 = nb.nlargest(5, "fire_rate_pct")[
        ["neighborhood", "fire_rate_pct", "poverty_rate", "median_household_income"]
    ]
    print(top5.to_string(index=False))

    X_raw = nb[SOCIO_VARS]
    X_norm = (X_raw - X_raw.mean()) / X_raw.std()
    X = sm.add_constant(X_norm)
    y = nb["fire_rate_pct"]
    model = sm.OLS(y, X).fit()

    print(f"\n  N = {int(model.nobs)}   R2 = {model.rsquared:.4f}   "
          f"adj. R2 = {model.rsquared_adj:.4f}   F-p = {model.f_pvalue:.4f}")
    print(f"\n  {'Variable':<32} {'beta':>8}  {'SE':>7}  {'p':>9}  Sig.")
    print("  " + "-" * 65)
    for var, row in pd.DataFrame(
        {"coef": model.params, "se": model.bse, "p": model.pvalues}
    ).iterrows():
        stars = ("***" if row["p"] < 0.001 else "**" if row["p"] < 0.01 else
                 "*" if row["p"] < 0.05 else "." if row["p"] < 0.1 else " ")
        print(f"  {var:<32} {row['coef']:>+8.4f}  {row['se']:>7.4f}  {row['p']:>9.5f}  {stars}")

    return model, nb


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("SFFD REGRESSIONSANALYSE v2 – Mit Kontrollvariablen + Einsatzart-Split")
    print("=" * 70 + "\n")

    df = load_and_clean()
    model_full(df)
    model_fire(df)
    model_ems(df)
    model_firerate(df)

    print(f"\n{SEP}\nFERTIG\n{SEP}")


if __name__ == "__main__":
    main()
