# Archiv – veraltete Dokumente

**Nichts aus diesem Ordner darf ungeprüft in die Bachelorarbeit übernommen werden.**
Die Dateien beruhen auf einem überholten Datenstand und enthalten Zahlen, die
nach dem Preprocessing-Audit vom 2026-07-26 nicht mehr stimmen. Sie liegen hier
nur, um die Entwicklung des Projekts nachvollziehbar zu halten.

| Datei | Stand | Warum veraltet |
|---|---|---|
| `kapitel_5_empirische_analyse.tex` | 2026-07-18 | 39 Stadtteile (jetzt 35), Zeitraum ab 2014 (jetzt 2015), alte statische Kriminalitätsmerkmale, Demo-Ergebnisse Ridge (S+L) R² 0,91 (jetzt 0,96), Fold-3-Einbruch als Nichtstationarität gedeutet (war der Phantom-Monat 2026-01) |
| `kapitel_5_data_preparation.tex` | 2026-07-19 | wie oben; beschreibt zusätzlich `anteil_gewaltdelikte_pct`/`anteil_eigentumsdelikte_pct`, die es nicht mehr gibt |
| `DATA_DICTIONARY_ANALYSIS.md` | 2026-05-03 | beschreibt den Datensatz *Fire Department Calls for Service* (FIR-0002, ~7 Mio. Zeilen). Dieser Datensatz wird in der Arbeit **nicht** verwendet – die Arbeit nutzt *Fire Incidents* (FIR-0001, `wr8u-xric`) |
| `exposee_anpassung_kontext.md` | 2026-05-04 | Prompt-Kontext aus einer frühen Projektphase (Zeitraum 2018–2024, 40 Stadtteile, andere Fragestellung) |

**Aktuelle Dokumente** stehen eine Ebene höher:

- `CLAUDE.md` (Projektwurzel) – verbindlicher Rahmenplan und Decision Log
- `docs/NAECHSTE_SCHRITTE.md` – Roadmap in einfacher Sprache
- `docs/KLASSIFIKATION_DESIGN.md` – Aufbau des Klassifikationsteils
- `docs/UMSETZUNGSLEITFADEN_MODELLIERUNG.md` – Programmierplan
- `docs/PREPROCESSING_AUDIT_2026-07-26.md` – Audit-Protokoll (historisch, aber abgearbeitet)
- `DATA_DICTIONARY.md` (Projektwurzel) – Spaltenbeschreibung des Analysedatensatzes
- `results/eignungspruefung/eignungspruefung_summary.md` – aktuelle Eignungsprüfung

Kapitel 5 der Arbeit ist auf Basis der aktuellen Zahlen **neu zu schreiben**.
