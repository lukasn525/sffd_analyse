"""
Einfache und effiziente Datenanalyse mit Polars.
Optimiert für Fire Calls for Service Dataset (7M+ Zeilen)
"""

import polars as pl
from pathlib import Path
import sys


def analyze_dataset(csv_file: str) -> None:
    """
    Analysiert Fire Calls Dataset mit fokus auf Häufigkeits-Analysen.
    Nutzt Lazy Loading - ideal für große Dateien.
    
    Args:
        csv_file: Pfad zur CSV-Datei
    """
    
    if not Path(csv_file).exists():
        print(f"❌ Datei nicht gefunden: {csv_file}")
        return
    
    print(f"📂 Lade Datei: {csv_file}")
    
    # Lazy Loading - Daten werden NICHT vollständig in RAM geladen
    df_lazy = pl.scan_csv(csv_file)
    
    # Sammle Schema Info
    schema = df_lazy.collect_schema()
    print(f"\n✅ Datensatz geladen - {len(schema)} Spalten vorhanden")
    
    # Hole ein kleines Sample zum Arbeiten
    df_sample = df_lazy.limit(5).collect()
    print(f"✅ Spalten: {', '.join(df_sample.columns[:5])}...")
    
    # Starte mit Datenanalyse
    df_full = df_lazy.collect()
    n_rows = len(df_full)
    
    print(f"\n{'='*80}")
    print(f"📊 FIRE CALLS ANALYSIS - HÄUFIGKEITS-ANALYSE")
    print(f"{'='*80}")
    print(f"Gesamtzahl Einträge: {n_rows:,}")
    print(f"Speicherverbrauch: {df_full.estimated_size('mb'):.2f} MB")
    print()
    
    # ========================================================================
    # TOP 10 ANALYSEN
    # ========================================================================
    
    # 1. Top 10 Call Types
    if 'Call Type' in schema:
        print(f"\n{'─'*80}")
        print("🔴 TOP 10 CALL TYPES (Einsatztypen)")
        print(f"{'─'*80}")
        top_types = (
            df_lazy
            .select('Call Type')
            .group_by('Call Type')
            .agg(pl.col('Call Type').count().alias('Anzahl'))
            .sort('Anzahl', descending=True)
            .limit(10)
            .collect()
        )
        print(top_types.to_string())
    
    # 2. Top 10 Units
    if 'Unit ID' in schema:
        print(f"\n{'─'*80}")
        print("🚒 TOP 10 UNITS (Einsatzfahrzeuge mit meisten Calls)")
        print(f"{'─'*80}")
        top_units = (
            df_lazy
            .select('Unit ID')
            .group_by('Unit ID')
            .agg(pl.col('Unit ID').count().alias('Anzahl'))
            .sort('Anzahl', descending=True)
            .limit(10)
            .collect()
        )
        print(top_units.to_string())
    
    # 3. Top 10 Cities
    if 'City' in schema:
        print(f"\n{'─'*80}")
        print("🏙️  TOP 10 CITIES")
        print(f"{'─'*80}")
        top_cities = (
            df_lazy
            .select('City')
            .group_by('City')
            .agg(pl.col('City').count().alias('Anzahl'))
            .sort('Anzahl', descending=True)
            .limit(10)
            .collect()
        )
        print(top_cities.to_string())
    
    # 4. Top 10 Neighborhoods
    if 'Neighborhood Districts' in schema:
        print(f"\n{'─'*80}")
        print("📍 TOP 10 NEIGHBORHOODS")
        print(f"{'─'*80}")
        try:
            top_neighborhoods = (
                df_lazy
                .select('Neighborhood Districts')
                .group_by('Neighborhood Districts')
                .agg(pl.col('Neighborhood Districts').count().alias('Anzahl'))
                .sort('Anzahl', descending=True)
                .limit(10)
                .collect()
            )
            print(top_neighborhoods.to_string())
        except:
            print("(Spalte nicht im erwarteten Format)")
    
    # 5. Top 10 Dispositions
    if 'Call Final Disposition' in schema:
        print(f"\n{'─'*80}")
        print("✅ TOP 10 DISPOSITIONS (Einsatzergebnisse)")
        print(f"{'─'*80}")
        try:
            top_disp = (
                df_lazy
                .select('Call Final Disposition')
                .group_by('Call Final Disposition')
                .agg(pl.col('Call Final Disposition').count().alias('Anzahl'))
                .sort('Anzahl', descending=True)
                .limit(10)
                .collect()
            )
            print(top_disp.to_string())
        except:
            print("(Spalte nicht im erwarteten Format)")
    
    # ========================================================================
    # BASIC STATISTICS
    # ========================================================================
    print(f"\n{'='*80}")
    print("📈 STATISTIKEN")
    print(f"{'='*80}")
    
    # Zähle Unique Values
    print(f"Unique Call Types: {df_full.select('Call Type').n_unique()}")
    print(f"Unique Units: {df_full.select('Unit ID').n_unique()}")
    if 'City' in schema:
        print(f"Unique Cities: {df_full.select('City').n_unique()}")
    
    print(f"\n✅ Analyse abgeschlossen!")


if __name__ == "__main__":
    # Pfad zur Datendatei
    data_file = "data/raw/fire_calls.csv"
    
    # Starte Analyse
    analyze_dataset(data_file)

