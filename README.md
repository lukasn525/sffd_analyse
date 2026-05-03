# sffd_analyse

## � Fire Department Calls for Service Analysis

Datenanalyse von ~7 Millionen Fire Department Einsätzen mit Fokus auf Häufigkeits-Analysen.

---

## 🚀 Setup & Installation

### 1. Ordnerstruktur erstellen
```bash
bash setup.sh
```

Das erstellt automatisch:
- `data/raw` – Deine CSV-Datei mit Fire Calls
- `data/processed` – Verarbeitete Daten
- `notebooks` – Jupyter Notebooks
- `scripts` – Spezial-Analyse Scripts
- `results` – Ergebnisse & Reports

### 2. Virtuelle Umgebung (Python 3.10+)

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Mac/Linux (Bash):**
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Daten vorbereiten
- Lege deine Fire Calls CSV-Datei als `fire_calls.csv` in `data/raw/` ab
- Muss alle Spalten aus FIR-0002 enthalten

### 4. Analyse starten
```bash
python analyze.py
```

---

## 📊 Was wird analysiert?

Das Skript analysiert automatisch:

✅ **Top 10 Call Types** - Häufigste Einsatztypen  
✅ **Top 10 Units** - Aktivste Fahrzeuge  
✅ **Top 10 Cities** - Meiste Einsätze pro Stadt  
✅ **Top 10 Neighborhoods** - Geographische Verteilung  
✅ **Top 10 Dispositions** - Einsatzergebnisse  
✅ **Statistiken** - Unique Values, Speicherverbrauch  

---

## 📖 Dokumentation

Siehe [docs/DATA_DICTIONARY_ANALYSIS.md](docs/DATA_DICTIONARY_ANALYSIS.md) für:
- Vollständige Spalten-Beschreibung
- Empfohlene Analyse-Szenarien
- Response-Time Berechnung
- Performance-Tipps für 7M+ Zeilen

---

## 🔧 Zusätzliche Scripts

```bash
# Erweiterte Fire Calls Analyse (mehr Details)
python scripts/fire_analysis.py

# Datendictionary exportieren
python read_data_dict.py
```

---

## 💾 Libraries

- **Polars** – Ultra-schnelle Datenverarbeitung (3-5x schneller als Pandas)
- **PyArrow** – Effiziente Datenspeicherung
- **Matplotlib/Seaborn** – Visualisierung
- **Pandas** – Optional für Kompatibilität
- **Jupyter** – Interaktive Notebooks

---

## 📈 Optimiert für große Dateien

✓ Lazy Loading - Daten werden nur bei Bedarf geladen  
✓ Effiziente Aggregationen - schnelle Group By Operationen  
✓ Speicherverwaltung - minimal RAM-Verbrauch  
✓ Output von Speicherverbrauch per Operation  

---

## 🎯 Nächste Schritte

1. CSV in `data/raw/fire_calls.csv` ablegen
2. `python analyze.py` ausführen
3. Ergebnisse in Terminal anschauen
4. Weitere Analysen in Notebooks ergänzen
5. Ergebnisse in `results/` speichern