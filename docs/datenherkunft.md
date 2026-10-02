# Datenherkunft

Alle Rohdaten liegen in `data/raw/` im Stand des Abrufs. Abgerufen hat sie
`prep/s1_daten.py`. Die Nutzungsbedingungen stammen aus den Ausdrucken in
`Anlage_Internetquellen/`.

| Datei | Portal, Datensatz | Abruf | Abfrage und Filter | Nutzungsbedingungen |
|---|---|---|---|---|
| `fire_incidents.parquet` | DataSF, Fire Incidents (`wr8u-xric`) | 03.05.2026 | 15 Felder, Stadtteil und Ankunft vorhanden, Antwortzeit 0 bis 60 min | ODC PDDL |
| `crime_raw.parquet` | DataSF, Summary of Incident Reports (`e3si-785i`) | 26.07.2026 | 4 Felder, Stadtteil vorhanden | ODC PDDL |
| `crime_historisch_raw.parquet` | DataSF, Police Department Incident Reports: Historical 2003 to May 2018 (`tmnf-yvry`) | 26.07.2026 | Datum, x, y, 2014 bis 2017, Koordinaten vorhanden | ODC PDDL |
| `land_use_2020_raw.parquet` | DataSF, [ARCHIVED] San Francisco Land Use - 2020 (`ygi5-84iq`) | 05.05.2026 | 6 Felder, Geometrie vorhanden | im Ausdruck nicht angegeben |
| `neighborhoods.geojson` | DataSF, Analysis Neighborhoods (`j2bu-swwd`) | 05.05.2026 | vollständig | ODC PDDL |
| `crosswalk.csv` | DataSF, Analysis Neighborhoods - 2020 census tracts assigned to neighborhoods (`sevw-6tgi`) | 04.05.2026 | 2 Felder | im Ausdruck nicht angegeben |
| `acs_tracts_<Jahr>.csv` | Census Bureau, American Community Survey 5-Year Data (API) | 04.05.2026 | 9 Variablen, alle Tracts von San Francisco (state 06, county 075), Jahrgänge 2009, 2014, 2019, 2021, 2023 | im Ausdruck nicht angegeben |

ODC PDDL steht für Open Data Commons Public Domain Dedication and License.

## Bekannte Fehler in den Rohdaten

Die Anzahlen stehen in `results/deskriptiv/rohbefunde.md`.

- doppelte Einsatznummern in den Einsatzmeldungen
- Parzellen ohne plausibles Baujahr, Lakeshore und Treasure Island ganz ohne Baujahr
- ACS-Jahrgang 2009 ohne Bildungsangabe
- Tract-Grenzen wechseln zwischen den Jahrgängen, die Zuordnungstabelle kennt nur die Grenzen von 2020
- Parkgebiete fast ohne Wohnbevölkerung
- ACS-Platzhalterwerte unter -999 statt fehlender Werte
