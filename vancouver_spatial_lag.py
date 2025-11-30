import geopandas as gpd
import pandas as pd
import numpy as np
from libpysal.weights import Queen
from spreg import ML_Lag

# ---------------------------------------------
# PATHS
# ---------------------------------------------
CT_SHP_PATH = "/Users/kaelkropp/CourseCollector/data/lct_000b21a_e.shp"

PARCEL_ATTR_PATH = "/Users/kaelkropp/CourseCollector/data/property-tax-report.parquet"
PARCEL_GEOM_PATH = "/Users/kaelkropp/CourseCollector/data/Vancouver-property-parcel-polygons.geojson"

GOLF_POINTS_PATH = "/Users/kaelkropp/CourseCollector/data/golf_toronto_vancouver_40km.csv"
GOLF_POLYGONS_PATH = "/Users/kaelkropp/CourseCollector/data/combined.geojson"

OUT_GEOJSON = "/Users/kaelkropp/CourseCollector/data/van_ct_golf_spatial_lag.geojson"
TARGET_CRS = "EPSG:3347"  # Canada Albers


# ---------------------------------------------
# 1. LOAD BC CENSUS TRACTS
# ---------------------------------------------
print("Loading BC census tracts...")
ct = gpd.read_file(CT_SHP_PATH)
ct["PRUID"] = ct["PRUID"].astype(str)
ct_bc = ct[ct["PRUID"] == "59"].copy()  # BC only

ct_bc = ct_bc.to_crs(TARGET_CRS)
print("BC CTs:", len(ct_bc))


# ---------------------------------------------
# 2. LOAD PARCEL ATTRIBUTES (PARQUET, NON-SPATIAL)
# ---------------------------------------------
print("\nLoading Vancouver parcel attributes (Parquet)...")
attr = pd.read_parquet(PARCEL_ATTR_PATH)
print("Attribute columns:", list(attr.columns))

# Compute total current assessed value
attr["TOTAL_VALUE"] = (
    attr["current_land_value"].astype(float).fillna(0)
    + attr["current_improvement_value"].astype(float).fillna(0)
)
attr = attr[attr["TOTAL_VALUE"] > 0].copy()

# Clean join key
attr["land_coordinate"] = attr["land_coordinate"].astype(str).str.strip()
print("Rows with non-zero TOTAL_VALUE:", len(attr))


# ---------------------------------------------
# 3. LOAD PARCEL GEOMETRIES AND MERGE
# ---------------------------------------------
print("\nLoading Vancouver parcel geometries...")
geom = gpd.read_file(PARCEL_GEOM_PATH)
geom = geom.to_crs(TARGET_CRS)

print("Geometry columns:", list(geom.columns))

if "tax_coord" not in geom.columns:
    raise ValueError("Expected 'tax_coord' in Vancouver parcel polygons GeoJSON.")

geom["tax_coord"] = geom["tax_coord"].astype(str).str.strip()

print("Joining attributes to geometry on land_coordinate ↔ tax_coord...")
parcels = geom.merge(
    attr[["land_coordinate", "TOTAL_VALUE"]],
    left_on="tax_coord",
    right_on="land_coordinate",
    how="inner",
)

parcels = parcels.set_geometry("geometry")
print("Merged parcel rows:", len(parcels))


# ---------------------------------------------
# 4. ASSIGN PARCELS → CTs (SPATIAL JOIN)
# ---------------------------------------------
print("\nAssigning parcels to CTs (spatial join, intersects)...")
join = gpd.sjoin(
    parcels[["geometry", "TOTAL_VALUE"]],
    ct_bc[["CTUID", "geometry"]],
    how="inner",
    predicate="intersects",
)

print("Parcel–CT matches:", len(join))

# Aggregate: median assessed value per CT
ct_values = (
    join.groupby("CTUID")["TOTAL_VALUE"]
        .median()
        .reset_index()
        .rename(columns={"TOTAL_VALUE": "median_assessed_value"})
)

print("Sample CT-level values:")
print(ct_values.head())

# Merge back to CT layer, keep only CTs with any parcels
ct_bc_van = ct_bc.merge(ct_values, on="CTUID", how="inner").copy()
ct_bc_van["log_value"] = np.log(ct_bc_van["median_assessed_value"])

print("CTs with assessment values:", len(ct_bc_van))


# ---------------------------------------------
# 5. LOAD GOLF DATA (BC / VANCOUVER)
# ---------------------------------------------
def is_bc(val):
    s = str(val)
    return s in ["BC", "B.C.", "British Columbia"]

print("\nLoading golf points...")
golf_pts = pd.read_csv(GOLF_POINTS_PATH)

# Filter to BC using full name as in your combined.csv
if "province" in golf_pts.columns:
    golf_pts = golf_pts[golf_pts["province"].apply(is_bc)].copy()

# If your file is already pre-filtered to TV-only, skip extra filters;
# keep the <=40km filter only if the column exists
if "dist_van_km" in golf_pts.columns:
    golf_pts = golf_pts[golf_pts["dist_van_km"] <= 40].copy()

golf_pts = gpd.GeoDataFrame(
    golf_pts,
    geometry=gpd.points_from_xy(golf_pts["lon"], golf_pts["lat"]),
    crs="EPSG:4326",
).to_crs(TARGET_CRS)

print("Golf points (BC subset):", len(golf_pts))

print("Loading golf polygons...")
golf_poly = gpd.read_file(GOLF_POLYGONS_PATH)

# Filter polygons to BC
if "province" in golf_poly.columns:
    golf_poly = golf_poly[golf_poly["province"].apply(is_bc)].copy()

# Restrict polygons to those in the BC subset if gcid present
if "gcid" in golf_poly.columns and "gcid" in golf_pts.columns:
    golf_poly = golf_poly.merge(
        golf_pts[["gcid"]].drop_duplicates(),
        on="gcid",
        how="inner",
    )

golf_poly = golf_poly.to_crs(TARGET_CRS)
golf_poly["centroid"] = golf_poly.geometry.centroid
golf_centroids = golf_poly.set_geometry("centroid")

print("Golf polygons in study:", len(golf_poly))


# ---------------------------------------------
# 6. COMPUTE GOLF EXPOSURE PER CT
# ---------------------------------------------
print("\nComputing golf exposure per CT...")

gc_union = golf_centroids.geometry.unary_union

def nearest_gc_distance_km(geom):
    if gc_union.is_empty:
        return np.nan
    return geom.centroid.distance(gc_union) / 1000.0

ct_bc_van["dist_to_gc_km"] = ct_bc_van.geometry.apply(nearest_gc_distance_km)

def count_golf_in_ct(geom, golf_geoms):
    return golf_geoms.intersects(geom).sum()

ct_bc_van["golf_count"] = ct_bc_van.geometry.apply(
    lambda g: count_golf_in_ct(g, golf_poly.geometry)
)

print("Exposure sample:")
print(ct_bc_van[["CTUID", "dist_to_gc_km", "golf_count", "median_assessed_value"]].head())


# ---------------------------------------------
# 7. DROP NaNs + SPATIAL LAG MODEL
# ---------------------------------------------
print("\nDropping rows with NaNs in DV or exposures...")
ct_bc_van = ct_bc_van.dropna(subset=["log_value", "dist_to_gc_km"]).copy()

print("Rows after dropping NaNs:", len(ct_bc_van))

print("Building Queen contiguity weights...")
w = Queen.from_dataframe(ct_bc_van)
w.transform = "r"

islands = [i for i, nbrs in w.neighbors.items() if len(nbrs) == 0]
print("Number of CTs:", len(ct_bc_van))
print("Islands (no neighbours):", islands)

print("\nRunning ML spatial lag model...")
y = ct_bc_van["log_value"].values.reshape(-1, 1)
X = ct_bc_van[["dist_to_gc_km", "golf_count"]].values

model = ML_Lag(
    y,
    X,
    w=w,
    name_y="log_value",
    name_x=["dist_to_gc_km", "golf_count"],
    name_w="Queen",
    name_ds="Vancouver_CTs",
)

print(model.summary)


# ---------------------------------------------
# 8. SAVE OUTPUT
# ---------------------------------------------
print(f"\nSaving CT-level output to {OUT_GEOJSON}...")
if "centroid" in ct_bc_van.columns:
    ct_bc_van = ct_bc_van.drop(columns=["centroid"])

ct_bc_van.to_file(OUT_GEOJSON, driver="GeoJSON")
print("Done.")
