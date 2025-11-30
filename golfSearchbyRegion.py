#!/usr/bin/env python3
"""
Collect golf courses for a given Canadian province using OSMnx and administrative subregions.
Compatible with OSMnx >=1.3.
"""

import osmnx as ox
import geopandas as gpd
import pandas as pd
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import warnings

try:
    import contextily as ctx
    _HAS_CONTEXTILY = True
except ImportError:
    _HAS_CONTEXTILY = False

# -----------------------
# Logging
# -----------------------
logging.basicConfig(
    filename="golf_course_collection.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

MIN_AREA_M2 = 10000  # Filter tiny polygons

# -----------------------
# Functions
# -----------------------
def get_subregions(province_name: str):
    """
    Return GeoDataFrame of subregions (admin_level 6/7) for a province.
    """
    # Get province polygon
    province_gdf = ox.geocode_to_gdf(province_name)
    if province_gdf.empty:
        raise ValueError(f"Province '{province_name}' not found in OSM.")
    province_polygon = province_gdf.loc[0, "geometry"]

    # Try admin_level=6 first
    subregions = ox.features_from_polygon(
        province_polygon,
        tags={"boundary": "administrative", "admin_level": "6"}
    )

    if subregions.empty:
        # fallback to admin_level=7
        subregions = ox.features_from_polygon(
            province_polygon,
            tags={"boundary": "administrative", "admin_level": "7"}
        )

    if not subregions.empty:
        subregions = subregions[subregions.is_valid]
        subregions = subregions[["name", "geometry"]]
    else:
        print(f"Warning: No admin-level 6/7 subregions found for {province_name}")

    return subregions

def fetch_golf_courses(subregion_name: str, polygon):
    """
    Fetch golf courses within a subregion polygon.
    """
    try:
        tags = {"leisure": "golf_course"}
        gdf = ox.geometries.geometries_from_polygon(polygon, tags)
        if not gdf.empty:
            gdf = gdf[['name', 'geometry']].copy()
            gdf['subregion'] = subregion_name
        print(f"{subregion_name}: {len(gdf)} golf courses found")
        return gdf
    except Exception as e:
        print(f"{subregion_name}: Failed to fetch golf courses: {e}")
        return gpd.GeoDataFrame(columns=['name','geometry','subregion'])

# -----------------------
# Main
# -----------------------
def main():
    province_name = "Alberta, Canada"
    output_prefix = province_name.split(",")[0].lower().replace(" ", "_")

    logging.info(f"Starting golf course collection for {province_name}")

    try:
        # Get subregions
        print(f"Fetching subregions in {province_name}...")
        subregions = get_subregions(province_name)
        if subregions.empty:
            print("No subregions found. Exiting.")
            return

        # Collect golf courses in all subregions
        all_gdfs = []
        for idx, row in subregions.iterrows():
            name = row.get("name", f"subregion_{idx}")
            polygon = row["geometry"]
            gdf = fetch_golf_courses(name, polygon)
            if not gdf.empty:
                all_gdfs.append(gdf)

        if not all_gdfs:
            print("No golf courses found in any subregion.")
            return

        # Merge results
        gdf = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True))
        gdf = gdf.to_crs(epsg=3347)  # StatsCan Lambert
        gdf['area_m2'] = gdf.geometry.area

        # Centroids in lat/lon
        centroids = gdf.to_crs(epsg=4326)
        gdf['lat'] = centroids.geometry.centroid.y
        gdf['lon'] = centroids.geometry.centroid.x

        # Filter tiny polygons
        gdf = gdf[gdf['area_m2'] > MIN_AREA_M2].sort_values(by='area_m2', ascending=False)

        # Save outputs
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        geojson_file = f"{output_prefix}_{timestamp}.geojson"
        csv_file = f"{output_prefix}_{timestamp}.csv"
        gdf.to_file(geojson_file, driver="GeoJSON")
        gdf[['name','subregion','lat','lon','area_m2']].to_csv(csv_file, index=False)
        print(f"Saved data:\n  {geojson_file}\n  {csv_file}")

        # Optional map overlay
        try:
            print("Creating map overlay...")
            gdf_3857 = gdf.to_crs(epsg=3857)
            fig, ax = plt.subplots(figsize=(12,12))
            gdf_3857.plot(ax=ax, facecolor='tab:green', edgecolor='black', alpha=0.35, linewidth=0.8)

            # Annotate names
            for idx, row in gdf_3857.iterrows():
                name = row.get("name") or ""
                if name:
                    x, y = row['geometry'].centroid.x, row['geometry'].centroid.y
                    ax.text(x, y, name, fontsize=8, fontweight='semibold',
                            ha='center', va='center', color='white',
                            bbox=dict(facecolor='black', alpha=0.4, boxstyle='round'))

            # Set bounds with padding
            minx, miny, maxx, maxy = gdf_3857.total_bounds
            pad_x = (maxx - minx) * 0.08
            pad_y = (maxy - miny) * 0.08
            ax.set_xlim(minx - pad_x, maxx + pad_x)
            ax.set_ylim(miny - pad_y, maxy + pad_y)
            ax.set_axis_off()

            if _HAS_CONTEXTILY:
                try:
                    ctx.add_basemap(ax, crs=gdf_3857.crs.to_string(), source=ctx.providers.CartoDB.Positron)
                except Exception as e:
                    warnings.warn(f"contextily basemap failed: {e}")

            plt.tight_layout()
            image_file = f"{output_prefix}_{timestamp}.png"
            fig.savefig(image_file, dpi=300, bbox_inches='tight', pad_inches=0)
            plt.close(fig)
            print(f"Saved map image: {image_file}")

        except Exception as e:
            print(f"Warning: Failed to create map overlay: {e}")

        print(f"Done! Found {len(gdf)} golf courses in {province_name}.")
        logging.info(f"Completed successfully with {len(gdf)} entries.")

    except Exception as e:
        logging.exception("Error during collection")
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
