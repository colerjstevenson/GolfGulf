import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.ops import nearest_points
from libpysal.weights import Queen
from spreg import ML_Lag

# -----------------------------------------------------------
# 1. LOAD DATA
# -----------------------------------------------------------

PARCELS_PATH = "/Users/kaelkropp/CourseCollector/data/property-tax-report.parquet"
CT_SHP_PATH = "/Users/kaelkropp/CourseCollector/data/lct_000b21a_e/lct_000b21a_e.shp"
GOLF_POINTS_PATH = "/Users/kaelkropp/CourseCollector/data/golf_toronto_vancouver_40km.csv"
GOLF_POLYGONS_PATH = "/Users/kaelkropp/CourseCollector/data/combined.geojson"


print("Loading Toronto parcel-level assessment data...")
parcels = gpd.read_parquet(PARCELS_PATH)

print("Parcel columns:", parcels.columns[:20])
print("Rows:", len(parcels))


# -----------------------------------------------------------
# 2. CLEAN PARCELS + SELECT ASSESSMENT VALUE
# -----------------------------------------------------------

# Toronto open data usually uses MPAC "CURRENT_VALUE"
value_vars = [c for c in parcels.columns if "value" in c.lower()]

print("\nDetected value fields:", value_vars)

# choose the first available (typical name: "CURRENT_VALUE", "AV_TOTAL")
value_col = value_vars[0]
print(f"Using assessment field: {value_col}")

# drop nulls and fix geometry
parcels = parcels[parcels[value_col].notna()].copy()
parcels = parcels.set_geometry("geometry")


# -----------------------------------------------------------
# 3. LOAD CENSUS TRACTS AND FILTER TO TORONTO CMA SUBSET
# -----------------------------------------------------------

print("\nLoading Census Tract boundaries...")
ct = gpd.read_file(CT_SHP_PATH)

# Only Toronto CMA CTs — PRUID = 35 (Ontario)
ct = ct[ct["PRUID"] == "35"].copy()

print("CT rows (ON):", len(ct))

# Reproject everything to a projected CRS
ct = ct.to_crs(32617)      # UTM 17N – good for Southern Ontario
parcels = parcels.to_crs(32617)


# -----------------------------------------------------------
# 4. SPATIAL JOIN: ASSIGN PARCELS → CENSUS TRACTS
# -----------------------------------------------------------

print("\nAssigning parcels to census tracts (spatial join)...")
parcels_ct = gpd.sjoin(parcels, ct[["CTUID", "geometry"]], how="inner", predicate="intersects")

print("Parcels matched to CT:", len(parcels_ct))


# -----------------------------------------------------------
# 5. AGGREGATE TO CT-LEVEL HOUSING VALUE (mean)
# -----------------------------------------------------------

ct_values = parcels_ct.groupby("CTUID")[value_col].mean().reset_index()
ct_values.columns = ["CTUID", "ct_mean_assessed_value"]

print("\nSample CT-level values:")
print(ct_values.head())


# -----------------------------------------------------------
# 6. LOAD GOLF LOCATIONS + GOLF POLYGONS
# -----------------------------------------------------------

print("\nLoading golf point dataset...")
g = pd.read_csv(GOLF_POINTS_PATH)
g = g[g["province"].isin(["ON"])].copy()

g["geometry"] = gpd.points_from_xy(g["lon"], g["lat"])
g = gpd.GeoDataFrame(g, geometry="geometry", crs="EPSG:4326").to_crs(32617)

print("Golf points in Ontario:", len(g))


print("Loading golf polygons...")
gpoly = gpd.read_file(GOLF_POLYGONS_PATH).to_crs(32617)

# Keep only polygons in Ontario
if "province" in gpoly.columns:
    gpoly = gpoly[gpoly["province"] == "ON"].copy()

print("Golf polygons:", len(gpoly))


# -----------------------------------------------------------
# 7. COMPUTE GOLF EXPOSURE PER TRACT
# -----------------------------------------------------------

def nearest_distance(row, others):
    nearest_geom = nearest_points(row.geometry, others.unary_union)[1]
    return row.geometry.distance(nearest_geom) / 1000  # km


print("\nComputing distance to nearest golf course...")
ct["dist_to_gc_km"] = ct.geometry.apply(lambda x: x.distance(g.unary_union) / 1000)

print("Counting golf polygons within CT...")
ct["golf_count"] = ct.geometry.apply(lambda poly: gpoly.within(poly).sum())


# -----------------------------------------------------------
# 8. BUILD FINAL CT DATAFRAME FOR MODELING
# -----------------------------------------------------------

df = ct.merge(ct_values, on="CTUID", how="left").copy()
df = df[df["ct_mean_assessed_value"].notna()]

df["log_value"] = np.log(df["ct_mean_assessed_value"])

print("\nFinal modeling dataframe:")
print(df[["CTUID", "dist_to_gc_km", "golf_count", "log_value"]].head())


# -----------------------------------------------------------
# 9. SPATIAL LAG MODEL
# -----------------------------------------------------------

print("\nBuilding Queen contiguity weights...")
w = Queen.from_dataframe(df)
w.transform = "r"

y = df["log_value"].values.reshape((-1, 1))
X = df[["dist_to_gc_km", "golf_count"]].values

print("Running ML spatial lag model...\n")
lag = ML_Lag(y, X, w=w, name_y="log_value",
             name_x=["dist_to_gc_km", "golf_count"],
             name_w="queen", name_ds="Toronto_CT")

print(lag.summary)


# -----------------------------------------------------------
# 10. SAVE OUTPUT
# -----------------------------------------------------------

OUT_PATH = "/Users/kaelkropp/CourseCollector/data/toronto_ct_golf_model.geojson"
df.to_file(OUT_PATH, driver="GeoJSON")

print(f"\nSaved CT-level dataset with assessment + golf exposure to: {OUT_PATH}")
