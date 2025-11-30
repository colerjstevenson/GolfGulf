import geopandas as gpd
import numpy as np
from libpysal.weights import Queen
from spreg import ML_Lag
from esda import Moran_Local
import folium
import mapclassify as mc

# ------------------------------------------------
# PATHS
# ------------------------------------------------
IN_GEOJSON = "/Users/kaelkropp/CourseCollector/data/van_ct_golf_spatial_lag.geojson"
OUT_GEOJSON = "/Users/kaelkropp/CourseCollector/data/van_ct_golf_spatial_lag_with_moran.geojson"
OUT_HTML = "/Users/kaelkropp/CourseCollector/data/van_ct_golf_residuals_map.html"

TARGET_CRS = "EPSG:3347"   # matches modeling CRS
MAP_CRS = "EPSG:4326"      # for web map


# ------------------------------------------------
# 1. LOAD CT DATA
# ------------------------------------------------
print("Loading CT-level GeoJSON...")
gdf = gpd.read_file(IN_GEOJSON)

# Ensure expected columns exist
for col in ["log_value", "dist_to_gc_km", "golf_count"]:
    if col not in gdf.columns:
        raise ValueError(f"Missing expected column '{col}' in input GeoJSON.")

# Make sure we're in projected CRS for spatial weights
if gdf.crs is None or gdf.crs.to_string() != TARGET_CRS:
    gdf = gdf.to_crs(TARGET_CRS)

print("Rows:", len(gdf))


# ------------------------------------------------
# 2. RE-FIT SPATIAL LAG MODEL TO GET PREDICTIONS
# ------------------------------------------------
print("Building Queen contiguity weights...")
w = Queen.from_dataframe(gdf)
w.transform = "r"

print("Running ML spatial lag model (for residuals)...")
y = gdf["log_value"].values.reshape(-1, 1)
X = gdf[["dist_to_gc_km", "golf_count"]].values

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

# Predictions and residuals
gdf["pred_log_value"] = model.predy.flatten()
gdf["residual"] = gdf["log_value"] - gdf["pred_log_value"]
gdf["resid_std"] = (gdf["residual"] - gdf["residual"].mean()) / gdf["residual"].std()

print("Residual summary:")
print(gdf["residual"].describe())


# ------------------------------------------------
# 3. LOCAL MORAN'S I ON STANDARDIZED RESIDUALS
# ------------------------------------------------
print("Computing Local Moran's I on standardized residuals...")
res = gdf["resid_std"].values
moran_loc = Moran_Local(res, w)

gdf["local_I"] = moran_loc.Is
gdf["local_p"] = moran_loc.p_sim
gdf["local_q"] = moran_loc.q   # 1..4 quadrants

# Classify cluster type
def cluster_label(p, q, alpha=0.05):
    if p >= alpha:
        return "NotSig"
    if q == 1:
        return "HH"
    if q == 2:
        return "LH"
    if q == 3:
        return "LL"
    if q == 4:
        return "HL"
    return "NotSig"

gdf["cluster"] = [
    cluster_label(p, q) for p, q in zip(gdf["local_p"], gdf["local_q"])
]

print("Cluster counts:")
print(gdf["cluster"].value_counts())


# ------------------------------------------------
# 4. SAVE ENRICHED GEOJSON
# ------------------------------------------------
print(f"Saving enriched GeoJSON to {OUT_GEOJSON}...")
gdf.to_file(OUT_GEOJSON, driver="GeoJSON")


# ------------------------------------------------
# 5. BUILD INTERACTIVE HTML MAP (RESIDUALS)
# ------------------------------------------------
print("Building residuals map...")

# Reproject to WGS84 for Folium
gdf_web = gdf.to_crs(MAP_CRS).copy()

# Center map on mean centroid
centroid = gdf_web.unary_union.centroid
m = folium.Map(location=[centroid.y, centroid.x], zoom_start=11)

# Classify standardized residuals into 5 quantiles
res_vals = gdf_web["resid_std"].values
classifier = mc.Quantiles(res_vals, k=5)
gdf_web["resid_class"] = classifier.yb  # 0..4

# Simple color palette for 5 classes
colors = ["#313695", "#74add1", "#ffffbf", "#f46d43", "#a50026"]  # blue → red

def style_function(feature):
    cls = feature["properties"]["resid_class"]
    color = colors[int(cls)] if cls is not None else "#cccccc"
    return {
        "fillColor": color,
        "color": "black",
        "weight": 0.2,
        "fillOpacity": 0.7,
    }

def highlight_function(feature):
    return {
        "weight": 1.5,
        "color": "black",
        "fillOpacity": 0.9,
    }

folium.GeoJson(
    gdf_web,
    name="Standardized residuals (log median value)",
    style_function=style_function,
    highlight_function=highlight_function,
    tooltip=folium.GeoJsonTooltip(
        fields=["CTUID", "median_assessed_value", "resid_std", "cluster"],
        aliases=["CTUID", "Median value", "Std residual", "Cluster"],
        localize=True,
    ),
).add_to(m)

folium.LayerControl().add_to(m)

m.save(OUT_HTML)
print(f"Saved HTML map to {OUT_HTML}")
print("Open it in your browser (or via VS Code: 'Open in Default Browser').")
