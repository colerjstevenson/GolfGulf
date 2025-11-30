# vancouver_controls_pipeline.py

import os
import pandas as pd
import geopandas as gpd
import chardet
import io
from shapely.geometry import Point

# -----------------------------
# CONFIGURATION
# -----------------------------
BASE = "/Users/kaelkropp/CourseCollector/data"

# Filepaths
FILES = {
    "census_csv": os.path.join(BASE, "Census Metropolitan Areas, Tracted Census Agglomerations and Census Tracts.csv"),
    "zoning_shp": os.path.join(BASE, "Vancouver_zoning-districts-and-labels.shp"),
    "buildings": os.path.join(BASE, "Vancouver-property-parcel-polygons.geojson"),
    "parks": os.path.join(BASE, "Vancouver_parks.geojson"),
    "regional_parks": os.path.join(BASE, "Vancouver_RegionalParksBoundaries_OpenData_4444849495308966192.geojson"),
    "floodplain": os.path.join(BASE, "Vancouver_designated-floodplain.geojson"),
    "noise_zones": os.path.join(BASE, "Vancouver_noise-control-areas.geojson"),
    "schools": os.path.join(BASE, "Vancouver_schools.csv"),
    "permits": os.path.join(BASE, "Vancouver_issued-building-permits.csv"),
    "bikeways": os.path.join(BASE, "Vancouver_bikeways.geojson"),
    "gtfs_stops": os.path.join(BASE, "Vancouver_stops.txt"),
}

# Target CRS (Vancouver local metric projection preferred)
TARGET_CRS = "EPSG:26910"  # UTM Zone 10N

# -----------------------------
# LOADERS
# -----------------------------
def load_csv(path):
    try:
        with open(path, 'rb') as f:
            raw = f.read(10000)
            enc = chardet.detect(raw)['encoding']
        print(f"[Info] Loading {os.path.basename(path)} with encoding: {enc}")
        return pd.read_csv(path, encoding=enc, low_memory=False)
    except UnicodeDecodeError:
        print(f"[Warning] {os.path.basename(path)} failed with {enc}, retrying with cp1252...")
        try:
            return pd.read_csv(path, encoding='cp1252', low_memory=False)
        except Exception:
            print(f"[Warning] cp1252 failed. Retrying with manual utf-8 decode ignoring errors...")
            with open(path, 'rb') as f:
                content = f.read().decode('utf-8', errors='ignore')
            return pd.read_csv(io.StringIO(content), low_memory=False)
    except pd.errors.ParserError:
        print(f"[Warning] Parsing failed for {os.path.basename(path)}. Retrying with on_bad_lines='skip'...")
        try:
            return pd.read_csv(path, encoding='cp1252', on_bad_lines='skip', low_memory=False)
        except Exception:
            print(f"[Warning] cp1252 with skip failed. Retrying with manual utf-8 decode ignoring errors and on_bad_lines='skip'...")
            with open(path, 'rb') as f:
                content = f.read().decode('utf-8', errors='ignore')
            return pd.read_csv(io.StringIO(content), on_bad_lines='skip', low_memory=False)

def load_gdf(path, crs=TARGET_CRS):
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        print(f"[Warning] {os.path.basename(path)} had no CRS. Setting manually to WGS84.")
        gdf.set_crs("EPSG:4326", inplace=True)
    return gdf.to_crs(crs)

def load_gtfs_stops(path):
    stops = pd.read_csv(path)
    gdf = gpd.GeoDataFrame(
        stops,
        geometry=gpd.points_from_xy(stops.stop_lon, stops.stop_lat),
        crs="EPSG:4326"
    )
    return gdf.to_crs(TARGET_CRS)

# -----------------------------
# LOAD AND CLEAN ALL DATASETS
# -----------------------------
def load_all_inputs():
    data = {}
    try:
        data["census"] = load_csv(FILES["census_csv"])
        data["zoning"] = load_gdf(FILES["zoning_shp"])
        data["buildings"] = load_gdf(FILES["buildings"])
        data["parks"] = load_gdf(FILES["parks"])
        data["regional_parks"] = load_gdf(FILES["regional_parks"])
        data["floodplain"] = load_gdf(FILES["floodplain"])
        data["noise"] = load_gdf(FILES["noise_zones"])
        data["schools"] = load_csv(FILES["schools"])
        data["permits"] = load_csv(FILES["permits"])
        data["bikeways"] = load_gdf(FILES["bikeways"])
        data["gtfs_stops"] = load_gtfs_stops(FILES["gtfs_stops"])
        print("All datasets loaded successfully.")
    except Exception as e:
        print(f"[Error] Problem loading datasets: {e}")
    return data

# -----------------------------
# SAMPLE USAGE
# -----------------------------
if __name__ == "__main__":
    controls = load_all_inputs()
    # Example: print CRS or row count for diagnostics
    for k, v in controls.items():
        if isinstance(v, gpd.GeoDataFrame):
            print(f"{k}: {len(v)} rows, CRS = {v.crs}")
        elif isinstance(v, pd.DataFrame):
            print(f"{k}: {len(v)} rows (tabular)")
