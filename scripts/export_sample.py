"""
Exportiert die ersten 100 Zeilen aus sffd_acs_joined.parquet als CSV
"""
import pandas as pd
from pathlib import Path

# Pfade
root = Path(__file__).parent.parent
input_file = root / "data" / "processed" / "sffd_acs_joined.parquet"
output_file = root / "data" / "processed" / "sffd_acs_joined_sample100.csv"

# Lese die ersten 100 Zeilen
df = pd.read_parquet(input_file)
sample = df.head(100)

# Speichere als CSV
sample.to_csv(output_file, index=False)

# Info-Output
print(f"✓ Gespeichert: {output_file.relative_to(root)}")
print(f"  Shape: {sample.shape}")
print(f"  Spalten ({len(sample.columns)}): {', '.join(sample.columns[:5])}...")
print(f"\nGesamtdaten: {df.shape[0]:,} Zeilen, {df.shape[1]} Spalten")
