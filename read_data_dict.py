import pandas as pd

file_path = r"FIR-0002_DataDictionary_fire-calls-for-service.xlsx"

# Lese DataDictionary Sheet - header ist in Zeile 5
df = pd.read_excel(file_path, sheet_name='DataDictionary', header=4, skiprows=4)

print("Spalten im DataFrame:")
print(df.columns.tolist())
print("\nErste 10 Zeilen:")
print(df.head(10).to_string())
