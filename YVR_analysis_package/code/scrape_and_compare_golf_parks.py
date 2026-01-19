#!/usr/bin/env python
"""
Golf Courses vs Parks Analysis for Vancouver

Compares golf courses to parks by:
1. Scraping golf course data from OpenStreetMap
2. Scraping park data from OpenStreetMap
3. Computing comparison metrics (counts and areas)
4. Generating publication-ready comparison charts
5. Saving combined GeoJSON with all features

Output:
- golf_vs_parks_counts.png: Facility count comparison
- golf_vs_parks_areas.png: Side-by-side area comparison
- golf_vs_parks_summary.csv: Summary statistics
- golf_and_parks_vancouver.geojson: Combined features
"""

import json
from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt
import osmnx as ox
import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

OUTPUT_DIR = Path("analysis_output/golf_census_analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COLOR_GOLF = "#2E86AB"
COLOR_PARKS = "#A23B72"
GOLF_MIN_AREA_HA = 1.0
PARK_MIN_AREA_HA = 0.5


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def classify_access(val: str | None) -> str:
    """Classify golf course access type."""
    if pd.isna(val):
        return "NOMATCH"
    s = str(val).lower()
    if "private" in s:
        return "Private"
    if "municipal" in s:
        return "Municipal"
    if "public" in s:
        return "Public"
    return "NOMATCH"


def compute_area(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Compute area in hectares."""
    gdf = gdf.copy().to_crs(epsg=3347)
    gdf["area_hectares"] = gdf.geometry.area / 10000
    return gdf.to_crs(epsg=4326)


def print_stats(label: str, gdf: gpd.GeoDataFrame) -> None:
    """Print summary statistics."""
    if len(gdf) == 0:
        print(f"\n{label}: No features found")
        return
    print(f"\n{label}:")
    print(f"  Features: {len(gdf)}")
    print(f"  Total area: {gdf['area_hectares'].sum():,.1f} ha")
    print(f"  Avg area: {gdf['area_hectares'].mean():.1f} ha")


def plot_bars(cats, vals, ylabel, title, fname, max_val=None):
    """Create bar chart."""
    fig, ax = plt.subplots(figsize=(8, 6))
    fig.patch.set_facecolor("#f8f9fa")
    ax.bar(cats, vals, color=[COLOR_GOLF, COLOR_PARKS], edgecolor="white", linewidth=2.5, alpha=0.9)
    ax.set_ylabel(ylabel, fontsize=12, fontweight="bold")
    ax.set_title(title, fontsize=13, fontweight="bold", pad=15)
    if max_val is None:
        max_val = max(vals)
    ax.set_ylim(0, max_val * 1.15)
    ax.grid(axis="y", alpha=0.2)
    ax.set_axisbelow(True)
    ax.set_facecolor("#ffffff")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for i, v in enumerate(vals):
        ax.text(i, v + max_val * 0.03, f"{v:,.0f}", ha="center", fontweight="bold", fontsize=12)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / fname, dpi=300, bbox_inches="tight", facecolor="#f8f9fa")
    plt.close()


# ============================================================================
# MAIN ANALYSIS
# ============================================================================


print("\n" + "=" * 70)
print("VANCOUVER GOLF COURSES & PARKS ANALYSIS")
print("=" * 70)

# 1. SCRAPE GOLF COURSES
print("\n[1/5] Scraping Golf Courses...")
try:
    golf_gdf = ox.features_from_place("Vancouver, Canada", tags={"leisure": "golf_course"})
    if golf_gdf.empty:
        golf_gdf = gpd.GeoDataFrame()
    else:
        golf_gdf = compute_area(golf_gdf)
        golf_gdf = golf_gdf[["name", "geometry", "area_hectares"]].copy()
        golf_gdf["access"] = golf_gdf.get("access", "NOMATCH").apply(classify_access)
        golf_gdf = golf_gdf[golf_gdf["area_hectares"] >= GOLF_MIN_AREA_HA]
        print(f"  Found {len(golf_gdf)} golf courses")
except Exception as e:
    print(f"  Error: {str(e)[:60]}")
    golf_gdf = gpd.GeoDataFrame()

# 2. SCRAPE PARKS
print("\n[2/5] Scraping Parks...")
try:
    parks_gdf = ox.features_from_place("Vancouver, Canada", tags={"leisure": "park"})
    if parks_gdf.empty:
        parks_gdf = gpd.GeoDataFrame()
    else:
        parks_gdf = compute_area(parks_gdf)
        parks_gdf = parks_gdf[["name", "geometry", "area_hectares"]].copy()
        parks_gdf = parks_gdf[parks_gdf["area_hectares"] >= PARK_MIN_AREA_HA]
        print(f"  Found {len(parks_gdf)} parks")
except Exception as e:
    print(f"  Error: {str(e)[:60]}")
    parks_gdf = gpd.GeoDataFrame()

# 3. ANALYZE
print("\n[3/5] Analyzing...")
print_stats("GOLF COURSES", golf_gdf)
print_stats("PARKS", parks_gdf)

max_park_area = parks_gdf["area_hectares"].max() if len(parks_gdf) > 0 else 0
parks_excl = parks_gdf[parks_gdf["area_hectares"] != max_park_area] if max_park_area > 0 else parks_gdf.copy()

# 4. CREATE CHARTS
print("\n[4/5] Creating Charts...")
if len(golf_gdf) > 0 and len(parks_gdf) > 0:
    data = {"Type": ["Golf", "Parks"], "Count": [len(golf_gdf), len(parks_gdf)], "Area": [golf_gdf["area_hectares"].sum(), parks_gdf["area_hectares"].sum()]}
    data_excl = {"Type": ["Golf", "Parks"], "Area": [golf_gdf["area_hectares"].sum(), parks_excl["area_hectares"].sum()]}
    
    max_area = max(max(data["Area"]), max(data_excl["Area"]))
    
    plot_bars(data["Type"], data["Count"], "Count", "Golf Courses vs Parks", "golf_vs_parks_counts.png")
    plot_bars(data["Type"], data["Area"], "Area (ha)", "With Stanley Park", "golf_vs_parks_with_stanley.png", max_area)
    plot_bars(data_excl["Type"], data_excl["Area"], "Area (ha)", "Without Stanley Park", "golf_vs_parks_without_stanley.png", max_area)
    
    # Summary CSV
    pd.DataFrame({
        "Metric": ["Golf Courses", "Parks (with)", "Parks (without)", "Golf Area (ha)", "Park Area with (ha)", "Park Area without (ha)"],
        "Value": [len(golf_gdf), len(parks_gdf), len(parks_excl), f"{data['Area'][0]:.1f}", f"{data['Area'][1]:.1f}", f"{data_excl['Area'][1]:.1f}"]
    }).to_csv(OUTPUT_DIR / "golf_vs_parks_summary.csv", index=False)
    print("  Saved charts and summary")

# 5. SAVE GEOJSON
print("\n[5/5] Saving GeoJSON...")
features = []
for idx, row in golf_gdf.iterrows():
    features.append({
        "type": "Feature",
        "properties": {"name": row.get("name", "Unknown"), "type": "Golf", "area_ha": round(row["area_hectares"], 2)},
        "geometry": row["geometry"].__geo_interface__
    })
for idx, row in parks_gdf.iterrows():
    features.append({
        "type": "Feature",
        "properties": {"name": row.get("name", "Unknown"), "type": "Park", "area_ha": round(row["area_hectares"], 2)},
        "geometry": row["geometry"].__geo_interface__
    })

with open(OUTPUT_DIR / "golf_and_parks_vancouver.geojson", "w") as f:
    json.dump({"type": "FeatureCollection", "features": features}, f, indent=2)

print(f"  Saved {len(features)} features")
print("\n" + "=" * 70)
print("COMPLETE")
print("=" * 70 + "\n")
