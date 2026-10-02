"""
Stellt den Abgabeordner ABGABE/ zusammen und berichtet, was hineinkommt.

    python abgabe_packen.py             kopiert nach ABGABE/ und berichtet
    python abgabe_packen.py --pruefen   berichtet nur, kopiert nichts

- ABGABE/ wird bei jedem Lauf vollstaendig geloescht und neu angelegt
- im Repo selbst wird nichts veraendert
- die Anlage entsteht allein aus Quellen/: ANLAGE_AUS_QUELLEN ordnet jedem
  Dateinamen der Anlage (= bib-Schluessel) seine Datei in Quellen/ zu
- jede Datei landet in genau einer Gruppe: KOMMT REIN, BLEIBT DRAUSSEN
  (mit Grund) oder NICHT ZUGEORDNET (wird nicht kopiert, bitte pruefen)
- Pruefungen: Pflichtdateien, Anlage der Internetquellen, Spalten von
  fire_incidents.parquet, Census-Schluessel in keiner kopierten Datei,
  Gesamtgroesse unter 250 MB
- Fehlendes wird gemeldet, kopiert wird trotzdem (Exitcode 1)
- steht der Census-Schluessel in einer Datei, wird nicht kopiert
"""

import os
import shutil
import stat
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ZIEL = ROOT / "ABGABE"
GRENZE_MB = 250

# Ordner, die ganz draussen bleiben. Sie werden nicht durchsucht.
ORDNER_RAUS = {
    ".git": "Git-Historie, enthaelt im Commit vom 04.05. den Census-Schluessel",
    "venv": "virtuelle Umgebung, entsteht aus requirements.txt",
    ".vscode": "Editor-Einstellungen",
    ".dist": "Hilfsordner",
    "doku": "interne Arbeitsdokumentation",
    "tools": "Hilfsskripte, keine Zahlen der Arbeit",
    "archiv": "ueberholte Staende",
    "Claude outputs": "Arbeitsdateien",
    "Quellen": "Volltexte; die Anlage-Dateien kommen umbenannt aus ANLAGE_AUS_QUELLEN",
    "Anlage_Internetquellen": "alte Sammelstelle, die Anlage entsteht aus Quellen/",
    "ABGABE": "Zielordner dieses Skripts",
    "results/abbildungen": "Abbildungen, Abbildungscode geht nicht mit (Schroeter 21.09.)",
    "results/suchdiagnose_test": "Probelauf von suchdiagnose.py --test",
}

# Einzelne Dateien, die draussen bleiben.
DATEIEN_RAUS = {
    ".env": "persoenlicher API-Schluessel (Schroeter 25.09.: keinesfalls abgeben)",
    ".gitignore": "Git-Hilfsdatei",
    ".gitattributes": "Git-Hilfsdatei",
    "CLAUDE.md": "interne Arbeitsnotizen",
    "ki_verzeichnis.tex": "Teil der Arbeit, steht im PDF",
    "ki_verzeichnis_neu.tex": "Teil der Arbeit, steht im PDF",
    "abgabe_packen.py": "dieses Skript",
    "modelle/m05_abbildungen.py": "erzeugt nur Abbildungen (Schroeter 21.09.)",
    "results/eignungspruefung/01_streudiagramme.png": "Abbildung",
    "results/eignungspruefung/02_residuen.png": "Abbildung",
    "results/eignungspruefung/qq_residuen.csv": "Daten einer Abbildung, vom heutigen Code nicht mehr erzeugt",
    "results/suchdiagnose/raender.csv": "vom heutigen Code nicht mehr erzeugt",
}

CODE_ORDNER = ("prep", "vorpruefung", "modelle", "tests")
ANALYSEDATEIEN = ("data/processed/regression.parquet",
                  "data/processed/klassifikation.parquet")

PFLICHT = [
    "README.md",
    "requirements.txt",
    "data/raw/fire_incidents.parquet",
    "data/raw/crime_raw.parquet",
    "data/raw/crime_historisch_raw.parquet",
    "data/raw/land_use_2020_raw.parquet",
    "data/raw/neighborhoods.geojson",
    "data/raw/crosswalk.csv",
    "data/raw/acs_tracts_2009.csv",
    "data/raw/acs_tracts_2014.csv",
    "data/raw/acs_tracts_2019.csv",
    "data/raw/acs_tracts_2021.csv",
    "data/raw/acs_tracts_2023.csv",
    *ANALYSEDATEIEN,
    "results/suchdiagnose/kurve.csv",
    "results/suchdiagnose/zusammenfassung.md",
]

# Dateiname = bib-Schluessel. In die Anlage kommen nur Internetseiten
# (fluechtige Quellen, Schroeter 13.07.), also die @online-Eintraege.
ANLAGE = "Anlage_Internetquellen"
# Name in der Anlage -> Datei in Quellen/.
ANLAGE_AUS_QUELLEN = {
    "SFFD2026.pdf": "Quellen/SFFD2026.pdf",
    "SFPD2026.pdf": "Quellen/SFPD2026.pdf",
    "SFPD2018.pdf": "Quellen/SFPD2018.pdf",
    "SFPlanning2020.pdf": "Quellen/SFPlanning2020.pdf",
    "SFPlanning2022.pdf": "Quellen/SFPlanning2022.pdf",
    "SFPlanning2023.pdf": "Quellen/SFPlanning2023.pdf",
    "CensusACS2023.pdf": "Quellen/CensusACS2023.pdf",
    "Statsmodels2025_het_breuschpagan.pdf": "Quellen/statsmodels2025.pdf",
    "Statsmodels2025_jarque_bera.pdf": "Quellen/statsmodels2025_jarque_bera.pdf",
    "ScikitLearn2025_RandomForestRegressor.pdf":
        "Quellen/ScikitLearn2025_RandomForestRegressor.pdf",
    "ScikitLearn2025_roc_auc_score.pdf": "Quellen/ScikitLearn2025_roc_auc_score.pdf",
    "ScikitLearn2025_r2_score.pdf": "Quellen/ScikitLearn2025_r2_score.pdf",
    "XGBoost2026.pdf": "Quellen/xgboost2026.pdf",
}
ANLAGE_ERWARTET = list(ANLAGE_AUS_QUELLEN)

FIRE_SPALTEN = 15
TEXTENDUNGEN = {".py", ".md", ".txt", ".csv", ".json", ".geojson", ".tex"}


def einordnen(rel: str) -> tuple[str, str]:
    """Ordnet eine Datei einer Gruppe zu.

    Ein:  Pfad relativ zum Repo, mit /
    Aus:  ("rein" | "raus" | "offen", Grund)
    """
    if rel in DATEIEN_RAUS:
        return "raus", DATEIEN_RAUS[rel]
    teile = rel.split("/")
    name = teile[-1]

    if len(teile) == 1:
        if rel in ("README.md", "requirements.txt"):
            return "rein", "Anleitung und Umgebung"
        return "offen", "Datei im Hauptordner"

    if teile[0] in CODE_ORDNER:
        if name.endswith(".py"):
            return "rein", "Code"
        return "offen", "keine Python-Datei im Codeordner"

    if teile[0] == "results":
        return "rein", "Ergebnisse (Schroeter 25.09.)"

    if teile[0] == "docs":
        if name.endswith(".md"):
            return "rein", "Dokumentation zur Abgabe"
        return "offen", "keine Markdown-Datei in docs/"

    if teile[:2] == ["data", "raw"]:
        return "rein", "Rohdaten, eingefrorener Stand"

    if teile[:2] == ["data", "processed"]:
        if rel in ANALYSEDATEIEN:
            return "rein", "Analysedatei"
        return "raus", ("Zwischendatei, entsteht mit prep/build.py neu "
                        "(Schroeter 25.09.: die beiden Analysedateien genuegen)")

    if teile[0] == ANLAGE:
        if name.lower().endswith(".pdf"):
            return "rein", "Internetquelle als PDF"
        return "offen", "keine PDF-Datei in der Anlage"

    return "offen", "keine Regel"


def durchsuchen() -> tuple[list, list, list]:
    """Geht das Repo durch und sortiert jede Datei ein.

    Ein:  nichts, arbeitet auf ROOT
    Aus:  Listen rein, raus und offen
    """
    rein, raus, offen = [], [], []
    for ordner, unterordner, dateien in os.walk(ROOT):
        rel_ordner = Path(ordner).relative_to(ROOT).as_posix()
        behalten = []
        for u in sorted(unterordner):
            rel_u = u if rel_ordner == "." else f"{rel_ordner}/{u}"
            if u == "__pycache__":
                raus.append((rel_u + "/", "Python-Zwischendateien", None))
                continue
            if rel_u in ORDNER_RAUS:
                raus.append((rel_u + "/", ORDNER_RAUS[rel_u], None))
            else:
                behalten.append(u)
        unterordner[:] = behalten

        for d in sorted(dateien):
            rel = d if rel_ordner == "." else f"{rel_ordner}/{d}"
            gruppe, grund = einordnen(rel)
            eintrag = (rel, grund, (Path(ordner) / d).stat().st_size)
            {"rein": rein, "raus": raus, "offen": offen}[gruppe].append(eintrag)
    return rein, raus, offen


def census_schluessel() -> str | None:
    """Liest den Census-Schluessel aus der Umgebung oder aus .env.

    Ein:  nichts
    Aus:  der Schluessel oder None; er wird nie ausgegeben
    """
    wert = os.environ.get("CENSUS_API_KEY")
    env = ROOT / ".env"
    if not wert and env.exists():
        for zeile in env.read_text(encoding="utf-8", errors="ignore").splitlines():
            zeile = zeile.strip()
            if zeile.lower().startswith("set "):
                zeile = zeile[4:]
            if zeile.startswith("CENSUS_API_KEY") and "=" in zeile:
                wert = zeile.split("=", 1)[1].strip().strip('"').strip("'")
    return wert if wert and len(wert) >= 8 else None


def loeschen(ordner: Path) -> None:
    """Loescht einen Ordner samt schreibgeschuetzter Dateien.

    Ein:  Pfad des Ordners
    Aus:  nichts
    """
    def schreibbar(funktion, pfad, _):
        os.chmod(pfad, stat.S_IWRITE)
        funktion(pfad)

    if sys.version_info >= (3, 12):
        shutil.rmtree(ordner, onexc=schreibbar)
    else:
        shutil.rmtree(ordner, onerror=schreibbar)


def mb(n: int) -> str:
    return f"{n / 1_048_576:8.2f} MB"


def main(argv: list[str]) -> int:
    nur_pruefen = "--pruefen" in argv
    rein, raus, offen = durchsuchen()
    fehler, sperre, hinweise = [], [], []

    # Anlage-Dateien aus Quellen/ uebernehmen
    herkunft = {}
    pfade = {e[0] for e in rein}
    for name, quelle in ANLAGE_AUS_QUELLEN.items():
        ziel_rel = f"{ANLAGE}/{name}"
        if ziel_rel in pfade:
            continue
        if (ROOT / quelle).exists():
            rein.append((ziel_rel, f"aus {quelle}", (ROOT / quelle).stat().st_size))
            herkunft[ziel_rel] = quelle
        else:
            fehler.append(f"{quelle} fehlt, {name} kann nicht in die Anlage")

    pfade = {e[0] for e in rein}
    for p in PFLICHT:
        if p not in pfade:
            fehler.append(f"Pflichtdatei fehlt: {p}")

    in_anlage = {Path(e[0]).name for e in rein if e[0].startswith(ANLAGE + "/")}
    for name in ANLAGE_ERWARTET:
        if name not in in_anlage:
            fehler.append(f"{ANLAGE}: {name} fehlt")
    for name in sorted(in_anlage - set(ANLAGE_ERWARTET)):
        hinweise.append(f"{ANLAGE}: {name} steht nicht in der erwarteten Liste")

    fire = ROOT / "data/raw/fire_incidents.parquet"
    if fire.exists():
        try:
            import pyarrow.parquet as pq
            n = len(pq.read_schema(fire).names)
            if n != FIRE_SPALTEN:
                fehler.append(f"fire_incidents.parquet hat {n} Spalten, "
                              f"erwartet {FIRE_SPALTEN}")
        except ImportError:
            hinweise.append("pyarrow fehlt, Spalten von fire_incidents nicht geprueft")

    schluessel = census_schluessel()
    if schluessel:
        for rel, _, _ in rein:
            if Path(rel).suffix.lower() in TEXTENDUNGEN:
                text = (ROOT / rel).read_text(encoding="utf-8", errors="ignore")
                if schluessel in text:
                    sperre.append(f"Census-Schluessel steht in {rel}")
    else:
        hinweise.append("Kein Census-Schluessel gefunden, Suche danach entfaellt")

    gesamt = sum(e[2] for e in rein)
    if gesamt > GRENZE_MB * 1_048_576:
        fehler.append(f"Gesamtgroesse {gesamt / 1_048_576:.1f} MB "
                      f"ueber {GRENZE_MB} MB")

    # ---- Bericht ----------------------------------------------------------
    print(f"\n{'=' * 78}\n  KOMMT REIN ({len(rein)} Dateien)\n{'=' * 78}")
    for rel, grund, groesse in sorted(rein, key=lambda e: e[0]):
        zusatz = f"   ({grund})" if rel in herkunft else ""
        print(f"  {mb(groesse)}  {rel}{zusatz}")
    print(f"  {'-' * 74}\n  {mb(gesamt)}  gesamt, unkomprimiert")

    print(f"\n{'=' * 78}\n  BLEIBT DRAUSSEN ({len(raus)} Eintraege)\n{'=' * 78}")
    for rel, grund, _ in sorted(raus, key=lambda e: e[0]):
        print(f"  {rel:<52} {grund}")

    if offen:
        print(f"\n{'=' * 78}\n  NICHT ZUGEORDNET, wird nicht kopiert ({len(offen)})"
              f"\n{'=' * 78}")
        for rel, grund, _ in sorted(offen, key=lambda e: e[0]):
            print(f"  {rel:<52} {grund}")

    print(f"\n{'=' * 78}\n  STAND DER ERGEBNISSE (aelteste Datei je Ordner)\n{'=' * 78}")
    stand = {}
    for rel, _, _ in rein:
        if rel.startswith("results/"):
            ordner = rel.rsplit("/", 1)[0]
            t = (ROOT / rel).stat().st_mtime
            stand[ordner] = min(t, stand.get(ordner, t))
    for ordner, t in sorted(stand.items()):
        print(f"  {ordner:<40} {datetime.fromtimestamp(t):%d.%m.%Y %H:%M}")

    for h in hinweise:
        print(f"\n  HINWEIS: {h}")
    if fehler:
        print(f"\n{'=' * 78}\n  FEHLT ODER STIMMT NICHT ({len(fehler)})\n{'=' * 78}")
        for f in fehler:
            print(f"  {f}")
    if sperre:
        print(f"\n{'=' * 78}\n  GESPERRT, es wird nicht kopiert\n{'=' * 78}")
        for f in sperre:
            print(f"  {f}")
        if ZIEL.exists() and not nur_pruefen:
            loeschen(ZIEL)
            print(f"  {ZIEL} geloescht, damit kein alter Stand liegen bleibt.")
        return 1

    if nur_pruefen:
        print("\n  Nur geprueft (--pruefen), nichts kopiert.")
        return 1 if fehler else 0

    # ---- Kopieren ---------------------------------------------------------
    if ZIEL.exists():
        loeschen(ZIEL)
    for rel, _, _ in rein:
        ziel = ZIEL / rel
        ziel.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / herkunft.get(rel, rel), ziel)

    print(f"\n  {len(rein)} Dateien nach {ZIEL} kopiert.")
    if fehler:
        print("  Die Punkte unter FEHLT ODER STIMMT NICHT sind noch offen.")
    return 1 if fehler else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
