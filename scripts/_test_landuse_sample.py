"""Schnelltest: Spatial Join mit den 3 Beispiel-Parzellen aus dem Chat."""
import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
import io
import warnings
warnings.filterwarnings("ignore")

SAMPLE = [
    {
        "the_geom": {"type": "MultiPolygon", "coordinates": [[[
            [-122.41912043184, 37.8057848887946],
            [-122.418815265608, 37.8058238290567],
            [-122.418797588546, 37.8057356825183],
            [-122.419102547256, 37.805696769104],
            [-122.41912043184, 37.8057848887946]
        ]]]},
        "blklot": "0027001", "yrbuilt": "1950.0", "landuse": "RESIDENT", "resunits": "3.0"
    },
    {
        "the_geom": {"type": "MultiPolygon", "coordinates": [[[
            [-122.419229395657, 37.8054580528662],
            [-122.419245904508, 37.8055393943837],
            [-122.41907503194,  37.8055611986881],
            [-122.418770393042, 37.8056000716906],
            [-122.418754076019, 37.8055187053657],
            [-122.419229395657, 37.8054580528662]
        ]]]},
        "blklot": "0027001A", "yrbuilt": "1934.0", "landuse": "RESIDENT", "resunits": "2.0"
    },
    {
        "the_geom": {"type": "MultiPolygon", "coordinates": [[[
            [-122.419210377517, 37.8053909390644],
            [-122.419215638482, 37.805390267662],
            [-122.419229395657, 37.8054580528662],
            [-122.418754076019, 37.8055187053657],
            [-122.418740477778, 37.8054509004049],
            [-122.419210377517, 37.8053909390644]
        ]]]},
        "blklot": "0027001B", "yrbuilt": "1934.0", "landuse": "RESIDENT", "resunits": "2.0"
    },
]

print("=== Schnelltest: Spatial Join mit 3 Beispiel-Parzellen ===\n")

# 1. Centroids berechnen
rows = []
for r in SAMPLE:
    centroid = shape(r["the_geom"]).centroid
    rows.append({
        "blklot":   r["blklot"],
        "yrbuilt":  float(r["yrbuilt"]),
        "landuse":  r["landuse"],
        "resunits": float(r["resunits"]),
        "geometry": centroid,
    })

parcels_gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
print("Centroids:")
for _, row in parcels_gdf.iterrows():
    print(f"  {row['blklot']}: lon={row['geometry'].x:.6f}, lat={row['geometry'].y:.6f} | yrbuilt={row['yrbuilt']:.0f}")

# 2. Neighborhood Boundaries laden
print("\nLade Neighborhood Boundaries (j2bu-swwd)...")
resp = requests.get(
    "https://data.sfgov.org/resource/j2bu-swwd.geojson",
    params={"$limit": 100},
    timeout=30,
)
resp.raise_for_status()
hoods_gdf = gpd.read_file(io.StringIO(resp.text))
hoods_gdf["neighborhood"] = hoods_gdf["nhood"].str.strip().str.title()
hoods_gdf = hoods_gdf[["neighborhood", "geometry"]].to_crs("EPSG:4326")
print(f"  {len(hoods_gdf)} Neighborhoods geladen.")

# 3. Spatial Join
print("\nSpatial Join...")
joined = gpd.sjoin(parcels_gdf, hoods_gdf, how="left", predicate="within")
joined = joined.drop(columns=["index_right"], errors="ignore")

print("\nErgebnis:")
print(joined[["blklot", "yrbuilt", "landuse", "resunits", "neighborhood"]].to_string(index=False))

matched = joined["neighborhood"].notna().sum()
print(f"\nMatch-Rate: {matched}/{len(joined)} ({matched/len(joined)*100:.0f}%)")
