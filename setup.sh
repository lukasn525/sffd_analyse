#!/bin/bash
# Erstelle Ordnerstruktur für das Datenanalyse-Projekt

mkdir -p data/raw
mkdir -p data/processed
mkdir -p notebooks
mkdir -p scripts
mkdir -p results

echo "✓ Ordnerstruktur erstellt:"
echo "  - data/raw (für Rohdaten)"
echo "  - data/processed (für verarbeitete Daten)"
echo "  - notebooks (für Jupyter Notebooks)"
echo "  - scripts (für Python-Skripte)"
echo "  - results (für Ergebnisse)"
