# -----------------------------------------------
#  SPATIAL LAG – GOLF COURSES & HOUSING VALUES
#  TORONTO + VANCOUVER, CENSUS TRACT LEVEL
# -----------------------------------------------

import os
import numpy as np
import pandas as pd
import geopandas as gpd

from shapely.geometry import Point
from math import radians, sin, cos, asin, sqrt

from libpysal.weights import Queen
from spreg import ML_Lag

# ===============================================
# 0. CONFIG
# ===============================================

# Data paths (adjust if needed)
GOLF_SUBSET_PATH = "/Users/kaelkropp/CourseCollector/data/golf_toronto_vancouver_40km.csv"
GOLF_POLYGONS_PATH = "/Users/kaelkropp/CourseCollector/data/combined.geojson"
CT_SHP_PATH = "/Users/kaelkropp/CourseCollector/data/lct_000b21a_e.shp"
CMA_HOUSING_PATH = "/Users/kaelkropp/CourseCollector/data/3310000101-eng.csv"

# Distance buffer for "golf exposure" (in km)
GOLF_BUFFER_KM = 3.0

# Radius around city centres to keep CTs (in km)
CITY_RADIUS_KM = 40.0

# Toggle: use synthetic CT-level housing values so the model runs end-to-end
USE_SYNTHETIC_DV = True

# City centres in lat/lon
TORONTO_LAT, TORONTO_LON = 43.6532, -79.3832
VAN_LAT, VAN_LON = 49.2827, -123.1207

# ===============================================
# 1. HELPER: HAVERSINE DISTANCE
# ===============================================

def haversine(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two (lat, lon) points."""
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = (sin(dlat / 2)**2
         + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2)
    c = 2 * asin(np.sqrt(a))
    return R * c

# ===============================================
# 2. LOAD GOLF DATA
# ===============================================

print("Loading golf subset...")
golf_sub = pd.read_csv(GOLF_SUBSET_PATH)
print("Golf subset columns:", golf_sub.columns)
print("Golf subset rows:", len(golf_sub))

if "gcid" not in golf_sub.columns:
    raise ValueError("Expected column 'gcid' in golf_toronto_vancouver_40km.csv")

# ===============================================
# 3. LOAD CENSUS TRACTS AND FILTER TO TORONTO/VANCOUVER REGION
# ===============================================

print("\nLoading census tract shapefile...")
ct = gpd.read_file(CT_SHP_PATH)
print("CT columns:", ct.columns)

# Keep only ON (35) and BC (59)
ct["PRUID"] = ct["PRUID"].astype(str)
ct = ct[ct["PRUID"].isin(["35", "59"])].copy()

# Work in WGS84 for distance to city centres
ct = ct.to_crs("EPSG:4326")
ct["centroid"] = ct.geometry.centroid
ct["lon"] = ct["centroid"].x
ct["lat"] = ct["centroid"].y

# Distances to downtown Toronto and Vancouver
ct["dist_tor_km"] = ct.apply(
    lambda r: haversine(TORONTO_LAT, TORONTO_LON, r["lat"], r["lon"]),
    axis=1
)
ct["dist_van_km"] = ct.apply(
    lambda r: haversine(VAN_LAT, VAN_LON, r["lat"], r["lon"]),
    axis=1
)

# Keep CTs within CITY_RADIUS_KM of either centre
mask_tv = (ct["dist_tor_km"] <= CITY_RADIUS_KM) | (ct["dist_van_km"] <= CITY_RADIUS_KM)
ct_tv = ct[mask_tv].copy().reset_index(drop=True)

print(f"\nCensus tracts within {CITY_RADIUS_KM} km of Toronto/Vancouver: {len(ct_tv)}")

# Identify CT ID col
ct_id_candidates = ["CTUID", "CTUID21", "CTUID_21"]
ct_id = next((c for c in ct_id_candidates if c in ct_tv.columns), None)
if ct_id is None:
    ct_tv["CT_ID"] = ct_tv.index.astype(str)
    ct_id = "CT_ID"

# Project to a metric CRS for buffers/distances (Canada Albers)
TARGET_CRS = "EPSG:3347"
ct_tv = ct_tv.to_crs(TARGET_CRS)

# ===============================================
# 4. LOAD GOLF POLYGONS AND FILTER TO SUBSET
# ===============================================

print("\nLoading golf polygons...")
golf_poly = gpd.read_file(GOLF_POLYGONS_PATH)

if "gcid" not in golf_poly.columns:
    raise ValueError("Expected 'gcid' column in combined.geojson to match golf subset.")

golf_poly_tv = golf_poly.merge(
    golf_sub[["gcid"]],
    on="gcid",
    how="inner"
)

print("Golf polygons in TV subset:", len(golf_poly_tv))

golf_poly_tv = golf_poly_tv.to_crs(TARGET_CRS)
golf_poly_tv["centroid"] = golf_poly_tv.geometry.centroid
golf_centroids = golf_poly_tv.set_geometry("centroid")

# ===============================================
# 5. BUILD GOLF EXPOSURE VARIABLES PER CT
# ===============================================

print("\nComputing golf exposure metrics per CT...")

ct_tv["ct_centroid"] = ct_tv.geometry.centroid
ct_pts = ct_tv.set_geometry("ct_centroid")

golf_geom_series = golf_centroids.geometry

def min_distance_km_geom(geom, golf_geoms):
    if len(golf_geoms) == 0:
        return np.nan
    dists = golf_geoms.distance(geom)  # meters in projected CRS
    return dists.min() / 1000.0

ct_tv["dist_to_gc_km"] = ct_pts["ct_centroid"].apply(
    lambda g: min_distance_km_geom(g, golf_geom_series)
)

buffer_m = GOLF_BUFFER_KM * 1000.0
ct_tv["buffer_geom"] = ct_pts["ct_centroid"].buffer(buffer_m)
ct_buf = ct_tv.set_geometry("buffer_geom")

join = gpd.sjoin(
    golf_centroids[["gcid", "centroid"]],
    ct_buf[[ct_id, "buffer_geom"]],
    how="left",
    predicate="within"
)

golf_counts = (
    join.groupby(ct_id)
        .agg(golf_count_within=("gcid", "nunique"))
        .reset_index()
)

ct_tv = ct_tv.merge(golf_counts, on=ct_id, how="left")
ct_tv["golf_count_within"] = ct_tv["golf_count_within"].fillna(0)

print("Example exposure rows:")
print(ct_tv[[ct_id, "dist_to_gc_km", "golf_count_within"]].head())

# ===============================================
# 6. BUILD/LOAD HOUSING VALUE AT CT LEVEL
# ===============================================

print("\nLoading CMA-level housing values (for synthetic DV)...")

# Robust read of StatsCan 3310000101-eng.csv
statscan_cols = [
    "Geography",
    "Residency status 5",
    "Estimates",
    "2017"
]

prop_raw = pd.read_csv(
    CMA_HOUSING_PATH,
    skiprows=11,             # skip metadata + extra header lines
    names=statscan_cols,
    encoding="utf-8-sig",
    engine="python",
    on_bad_lines="skip"
)

# Forward-fill identifier columns where StatsCan leaves them blank
prop_raw["Geography"] = prop_raw["Geography"].ffill()
prop_raw["Residency status 5"] = prop_raw["Residency status 5"].ffill()
prop_raw["Estimates"] = prop_raw["Estimates"].ffill()

# Drop fully empty rows
prop_raw = prop_raw.dropna(how="all")

# Filter to Toronto & Vancouver CMAs; average value, all residency statuses
mask_cma = prop_raw["Geography"].isin([
    "Toronto, census metropolitan area",
    "Vancouver, census metropolitan area"
])

prop = prop_raw[mask_cma].copy()

mask_value = (
    (prop["Residency status 5"] == "Total, all residency status categories") &
    (prop["Estimates"] == "Average value")
)

prop = prop[mask_value].copy()

# Parse the year column
year_cols = [c for c in prop.columns if c.isdigit()]
if not year_cols:
    raise ValueError("No numeric year column found in CMA housing file.")
year_col = year_cols[0]

prop["avg_value"] = (
    prop[year_col]
    .astype(str)
    .str.replace(",", "")
    .astype(float)
)

tor_val = float(
    prop.loc[
        prop["Geography"] == "Toronto, census metropolitan area",
        "avg_value"
    ].iloc[0]
)

van_val = float(
    prop.loc[
        prop["Geography"] == "Vancouver, census metropolitan area",
        "avg_value"
    ].iloc[0]
)

print(f"Toronto CMA average value:   {tor_val:,.0f}")
print(f"Vancouver CMA average value: {van_val:,.0f}")

# Use PRUID to decide which CMA a CT belongs to:
# ON (35) -> Toronto CMA; BC (59) -> Vancouver CMA
ct_tv["base_value"] = np.where(
    ct_tv["PRUID"].astype(str) == "35",
    tor_val,
    van_val
)

if USE_SYNTHETIC_DV:
    print("\nUsing synthetic CT-level housing values (for testing).")
    rng = np.random.default_rng(seed=42)
    noise = rng.normal(loc=1.0, scale=0.05, size=len(ct_tv))  # mean 1, sd 5%

    ct_tv["house_value"] = ct_tv["base_value"] * noise
    ct_tv["log_house_value"] = np.log(ct_tv["house_value"])
else:
    raise NotImplementedError(
        "Set USE_SYNTHETIC_DV = True or plug in a real CT-level housing dataset."
    )

print("CT housing value sample:")
print(ct_tv[[ct_id, "house_value", "log_house_value"]].head())

# ===============================================
# 7. BUILD SPATIAL WEIGHTS (QUEEN CONTIGUITY)
# ===============================================

print("\nBuilding Queen contiguity weights...")
ct_tv = ct_tv.set_geometry("geometry")

w = Queen.from_dataframe(ct_tv)
w.transform = "r"

islands = [i for i, nbrs in w.neighbors.items() if len(nbrs) == 0]
print("Number of CTs:", len(ct_tv))
print("Islands (no neighbours):", islands)

# ===============================================
# 8. RUN SPATIAL LAG MODEL
# ===============================================

print("\nRunning ML spatial lag model...")

y = ct_tv["log_house_value"].values.reshape(-1, 1)
X = ct_tv[["dist_to_gc_km", "golf_count_within"]].values

model = ML_Lag(
    y,
    X,
    w=w,
    name_y="log_house_value",
    name_x=["dist_to_gc_km", "golf_count_within"],
    name_w="Queen",
    name_ds="Toronto_Vancouver_CTs"
)

print(model.summary)

# ===============================================
# 9. EXPORT RESULTING GEOJSON FOR MAPPING
# ===============================================

OUT_GEOJSON = "/Users/kaelkropp/CourseCollector/data/ct_tv_golf_spatial_lag.geojson"

# Drop helper geometries so only one geometry column remains
for col in ["centroid", "ct_centroid", "buffer_geom"]:
    if col in ct_tv.columns:
        ct_tv = ct_tv.drop(columns=col)

ct_tv = ct_tv.set_geometry("geometry")

ct_tv.to_file(OUT_GEOJSON, driver="GeoJSON")
print(f"\nSaved CT-level data with golf exposure + DV to: {OUT_GEOJSON}")

