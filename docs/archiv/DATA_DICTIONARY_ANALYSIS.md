# 🔥 Fire Calls Analysis - Data Dictionary Übersicht

## Dataset: Fire Department Calls for Service (FIR-0002)

**Gesamt Einträge:** ~7 Millionen Zeilen  
**Datentyp:** Fire units responses to calls  
**Zeitraum:** Basierend auf Dispatch-Logs  
**Einheit:** Ein Record pro Unit pro Call (daher mehrere Records pro Call-Nummer möglich)

---

## 📊 Verfügbare Spalten & Typen

### Identifikatoren
1. **Call Number** (Text)
   - Unique 9-digit ID vom 911 Dispatch Center (DEM)
   - Wird für Police & Fire genutzt
   - Kategorie für Aggregation: ✅ **Top Calls zählen**

2. **Unit ID** (Text)
   - Unit Identifier (z.B. E01 = Engine 1, T01 = Truck 1)
   - Wichtig für Unit-Performance-Analyse
   - Kategorie für Aggregation: ✅ **Top Units zählen**

3. **Incident Number** (Text)
   - Unique 8-digit incident ID
   - Unterscheidet sich von Call Number (mehrere Units pro Incident)

4. **RowID** (Text)
   - Row-Level Identifier

---

### Zeitstempel & Temporal Data
5. **Call Date** (Date & Time)
   - Datum Call erhalten wurde
   - Reporting Zweck
   - 📈 Ideal für: **Zeitreihen-Analyse**

6. **Watch Date** (Date & Time)
   - Shift-basiertes Datum (0800 - 0800 nächster Tag)
   - Für Watch-basierte Auswertungen

7. **Received DtTm** (Date & Time)
   - Exakte Zeit wenn Call im 911 System ankam

8. **Entry DtTm** (Date & Time)
   - Zeit der manuellen Eingabe ins CAD System

9. **Dispatch DtTm** (Date & Time)
   - Zeit der Unit Dispatch

10. **Response DtTm** (Date & Time)
    - Zeit Unit acknowledges & ist unterwegs

11. **On Scene DtTm** (Date & Time)
    - Zeit Unit kam an der Location an
    - 🔴 **WICHTIG für Response Time Analyse**

12. **AVL Validated On Scene DtTm** (Date & Time)
    - Aktualisierte Zeit basierend auf Unit GPS

13. **Transport DtTm** (Date & Time)
    - Falls Patienten transportiert wurden

14. **Hospital DtTm** (Date & Time)
    - Ankunft Krankenhaus

15. **Available DtTm** (Date & Time)
    - Zeit wenn Unit wieder verfügbar für neuen Call

---

### Operational Data
16. **Call Type** (Text)
    - Type des Incidents (Medical, Structure Fire, etc.)
    - 📈 **TOP Häufigkeits-Analyse: Call Types**

17. **Call Final Disposition** (Text)
    - Finales Ergebnis/Status des Calls

18. **Address** (Text)
    - Block number, intersection, oder call box (nicht exakte Adresse)
    - 📍 Für Geo-Analyse

19. **City** (Text)
    - Stadt name
    - 📍 **Top Cities zählen**

20. **Zipcode of Incident** (Text)
    - ZIP code der Location
    - 📊 Für geografische Verteilung

21. **Battalion** (Text)
    - Battalion Assignment
    - 📊 Organisatorische Einheit

22. **Station Area** (Text)
    - Station Area
    - 📊 Für Station-Performance

23. **Supervisor District** (Text)
    - Supervisor District
    - 📊 Für administratives Reporting

24. **Neighborhood Districts** (Text)
    - Neighborhood District
    - 📍 **Top Neighborhoods analysieren**

25. **Location** (Text/Geo)
    - Geo location (latitude, longitude)
    - 🗺️ Für Karten-Visualisierung

---

## 🎯 Empfohlene Analyse-Szenarien

### 1. **Häufigkeits-Analysen** (TOP 10)
- ✅ Top 10 Call Types
- ✅ Top 10 Units (Most Active)
- ✅ Top 10 Neighborhoods
- ✅ Top 10 Cities
- ✅ Top 10 Dispositions

### 2. **Response Time Analysen**
```
Response Time = On Scene DtTm - Dispatch DtTm
```
- Mean Response Time
- Median Response Time
- 95th Percentile Response Time

### 3. **Temporal Analysen**
- Calls per Hour
- Calls per Day of Week
- Calls per Battalion
- Trend Over Time (months/years)

### 4. **Unit Performance**
- Calls per Unit
- Avg Response Time per Unit
- Most Active Units

### 5. **Geographic Distribution**
- Calls per Neighborhood
- Calls per Zipcode
- Heatmap Locations

---

## 💾 Performance-Tipps für 7M Zeilen

1. **Use Polars (nicht Pandas)** - 3-5x schneller
2. **Lazy Loading** - Daten nicht komplett in RAM
3. **Use `.group_by().agg()`** - Statt `value_counts()`
4. **Select only needed columns** - Weniger RAM
5. **Use `.limit()`** - Für Top-10 Analysen
6. **Parquet format** - Schneller als CSV

---

## 📋 Column Mapping

| Analyse-Typ | Spalte(n) | Methode |
|---|---|---|
| Top Call Types | `Call Type` | `group_by().count()` |
| Top Units | `Unit ID` | `group_by().count()` |
| Response Time | `On Scene DtTm - Dispatch DtTm` | Zeitdifferenz |
| Geographic | `Neighborhood Districts` / `Zipcode` | `group_by().count()` |
| Temporal | `Call Date` / `Watch Date` | `group_by().count()` |
| City Analysis | `City` | `group_by().count()` |

---

## 🔧 Next Steps

1. ✅ CSV Datei in `data/raw/fire_calls.csv` ablegen
2. ✅ Script ausführen: `python scripts/fire_analysis.py`
3. ✅ Top 10 für jede Kategorie analysieren
4. ✅ Response Time Statistiken berechnen
5. ✅ Visualisierungen erstellen
