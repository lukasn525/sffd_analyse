"""
Datenanalyse der Fire Department Calls for Service (FIR-0002)
Diese Datei enthält alle 7M+ Zeilen mit Fire Department Einsätzen
"""

import polars as pl
from pathlib import Path
import json

# ============================================================================
# SCHEMA UND SPALTEN (aus FIR-0002 Data Dictionary)
# ============================================================================

FIRE_CALLS_SCHEMA = {
    "Call Number": "str",           # Unique 9-digit ID (911 Dispatch)
    "Unit ID": "str",               # Engine/Truck ID (E01, T01, etc.)
    "Incident Number": "str",       # Unique 8-digit incident ID
    "Call Date": "str",             # Call received date
    "Call Type": "str",             # Type of call (Medical, Structure Fire, etc.)
    "Watch Date": "str",            # Watch date (starts 0800, ends 0800 next day)
    "Received DtTm": "str",         # Date/time call received at 911
    "Entry DtTm": "str",            # Date/time entered into CAD system
    "Dispatch DtTm": "str",         # Date/time unit dispatched
    "Response DtTm": "str",         # Date/time unit en route
    "On Scene DtTm": "str",         # Date/time unit arrived
    "Transport DtTm": "str",        # Date/time unit transported (if applicable)
    "Hospital DtTm": "str",         # Date/time arrived at hospital (if applicable)
    "Call Final Disposition": "str", # Final outcome
    "Available DtTm": "str",        # Date/time unit available for next call
    "Address": "str",              # Call address (block/intersection/box)
    "City": "str",                 # City name
    "Zipcode of Incident": "str",  # ZIP code
    "Battalion": "str",            # Battalion assigned
    "Station Area": "str",         # Station area
    "Supervisor District": "str",  # Supervisor district
    "Neighborhood Districts": "str",# Neighborhood district
    "Location": "str",             # Geo location (lat,lng)
    "RowID": "str",               # Row identifier
}

# ============================================================================
# ANALYSE-KONFIGURATION
# ============================================================================

class FireCallsAnalyzer:
    def __init__(self, data_file: str):
        self.data_file = data_file
        self.df = None
        
    def load_data_lazy(self):
        """Lade Daten mit Lazy Loading (speichert nicht alles im RAM)"""
        if Path(self.data_file).exists():
            print(f"📂 Lade Datei: {self.data_file}")
            self.df_lazy = pl.scan_csv(self.data_file)
            return True
        else:
            print(f"❌ Datei nicht gefunden: {self.data_file}")
            return False
    
    def get_column_info(self):
        """Zeige alle verfügbaren Spalten"""
        if self.df_lazy:
            # Sammle Spalten Info
            df_sample = self.df_lazy.limit(1).collect()
            print("\n" + "="*80)
            print("VERFÜGBARE SPALTEN IM DATENSATZ")
            print("="*80)
            for i, col in enumerate(df_sample.columns, 1):
                print(f"{i:2}. {col}")
    
    def analyze_call_types(self):
        """Analysiere Top 10 Call Types"""
        print("\n" + "="*80)
        print("TOP 10 CALL TYPES (Häufigste Einsatztypen)")
        print("="*80)
        
        if 'Call Type' in self.df_lazy.collect_schema():
            top_calls = (
                self.df_lazy
                .group_by("Call Type")
                .agg(pl.col("Call Type").count().alias("Anzahl"))
                .sort("Anzahl", descending=True)
                .limit(10)
                .collect()
            )
            print(top_calls.to_string())
    
    def analyze_units(self):
        """Analysiere Top 10 Most Active Units"""
        print("\n" + "="*80)
        print("TOP 10 UNITS (Meiste Einsätze)")
        print("="*80)
        
        if 'Unit ID' in self.df_lazy.collect_schema():
            top_units = (
                self.df_lazy
                .group_by("Unit ID")
                .agg(pl.col("Unit ID").count().alias("Anzahl"))
                .sort("Anzahl", descending=True)
                .limit(10)
                .collect()
            )
            print(top_units.to_string())
    
    def get_statistics(self):
        """Basis-Statistiken"""
        print("\n" + "="*80)
        print("DATENSATZ STATISTIKEN")
        print("="*80)
        
        df_collect = self.df_lazy.collect()
        print(f"Gesamtzahl Einträge: {len(df_collect):,}")
        print(f"Anzahl Spalten: {len(df_collect.columns)}")
        print(f"Speicherverbrauch: ~{df_collect.estimated_size('mb'):.2f} MB")


if __name__ == "__main__":
    print("\n🔥 FIRE DEPARTMENT CALLS FOR SERVICE - DATA ANALYZER")
    print("=" * 80)
    
    # Lade und analysiere Daten
    # WICHTIG: Datei muss in data/raw/ liegen
    analyzer = FireCallsAnalyzer("data/raw/fire_calls.csv")
    
    if analyzer.load_data_lazy():
        analyzer.get_column_info()
        analyzer.analyze_call_types()
        analyzer.analyze_units()
        analyzer.get_statistics()
        print("\n✅ Analyse abgeschlossen!")
    else:
        print("\n❌ Datei nicht vorhanden")
        print("Bitte 'fire_calls.csv' in data/raw/ ablegen")
