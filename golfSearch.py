#!/usr/bin/env python3
"""
Collect approximate locations and areas of all golf courses in Alberta, Canada
using OpenStreetMap data via the OSMnx library.
"""
import os
import osmnx as ox
import geopandas as gpd
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import warnings

from nameFiller import find_nearby_golf_course

try:
    import contextily as ctx
    _HAS_CONTEXTILY = True
except Exception:
    _HAS_CONTEXTILY = False

# -----------------------
# Setup
# -----------------------

# Configure logging
logging.basicConfig(
    filename="golf_course_collection.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    province = "Newfoundland and Labrador"
    region = f"{province}, Canada"
    output_prefix = f"golf_courses_{province.lower().replace(' ', '_')}"
    gdf = None
    
    # if files already exists, skip data collection and load existing data to create map
    if os.path.exists(f"{output_prefix}.geojson") and os.path.exists(f"{output_prefix}.csv"):
        logging.info("Existing data found, loading...")
        gdf = gpd.read_file(f"{output_prefix}.geojson")
    else:
        logging.info(f"Starting golf course collection for {region}")

    try:
        # -----------------------
        # Fetch data from OSM
        # -----------------------
        if gdf is None:
            print(f"Fetching golf courses in {region} from OpenStreetMap...")
            tags = {'leisure': 'golf_course'}
            gdf = ox.features_from_place(region, tags=tags)

            if gdf.empty:
                logging.warning("No golf courses found in OSM data for this region.")
                print("No golf courses found. Try a smaller subregion or check OSM coverage.")
                return

            logging.info(f"Fetched {len(gdf)} golf courses.")

            # -----------------------
            # Clean and project
            # -----------------------
            # print("Processing data...")
            # gdf = gdf[['name', 'geometry']].copy()
            gdf = gdf.to_crs(epsg=3347)  # NAD83 / StatsCan Lambert (meters)
            gdf['area_m2'] = gdf['geometry'].area

            # Compute centroids and convert back to lat/lon
            centroids = gdf.copy()
            centroids = centroids.to_crs(epsg=4326)
            gdf['lat'] = centroids['geometry'].centroid.y
            gdf['lon'] = centroids['geometry'].centroid.x
            gdf['province'] = [province for _ in range(len(gdf))]
            gdf['gcid'] = [f'{province[0].upper()}{province[-1].upper()}{i+1:05d}' for i in range(len(gdf))]
            gdf['name'] = gdf['name'].fillna('')
            # gdf['name'] = [name if name and name != '' else find_nearby_golf_course(lat, lon)['name'] if find_nearby_golf_course(lat, lon) else 'Unknown Golf Course' for name, lat, lon in zip(gdf['name'], gdf['lat'], gdf['lon'])]
            name_check = []
            for name, lat, lon in zip(gdf['name'], gdf['lat'], gdf['lon']):
                print(name)
                if not name or name == '':
                    check = find_nearby_golf_course(lat, lon)
                    if check:
                        name_check.append(check['name'])
                    else:
                        name_check.append('Unknown Golf Course')
                else:
                    name_check.append(name)
            
            gdf['name'] = name_check
            # -----------------------
            # Filter and sort
            # -----------------------
            # Remove tiny polygons (sometimes OSM includes things like mini-putt or errors)
            gdf = gdf[gdf['area_m2'] > 10000]  # >1 hectare
            gdf = gdf.sort_values(by='area_m2', ascending=False)

            # -----------------------
            # Save outputs
            # -----------------------
            print("Saving results...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")

            geojson_file = f"data/{output_prefix}.geojson"
            csv_file = f"data/{output_prefix}.csv"

            gdf.to_file(geojson_file, driver="GeoJSON")
            # gdf[['name', 'lat', 'lon', 'area_m2']].to_csv(csv_file, index=False)
            gdf.drop(columns=['leisure','geometry']).to_csv(csv_file, index=False)
        # -----------------------
        # Create map overlay and save image
        # -----------------------
        try:
            print("Creating map image with translucent overlays...")

            # Convert to Web Mercator for basemap tiles
            gdf_3857 = gdf.to_crs(epsg=3857)

            # Setup plot
            fig, ax = plt.subplots(figsize=(12, 12))

            # Plot polygons as translucent boxes
            gdf_3857.plot(ax=ax, facecolor='tab:green', edgecolor='black', alpha=0.35, linewidth=0.8)

            # Annotate each polygon near its centroid with the course name
            centroids = gdf_3857.copy()
            centroids['centroid'] = centroids.geometry.centroid
            for idx, row in centroids.iterrows():
                name = row.get('name') or ''
                if name:
                    x, y = row['centroid'].x, row['centroid'].y
                    # ax.text(x, y, name, fontsize=8, fontweight='semibold',
                    #         ha='center', va='center', color='white',
                    #         bbox=dict(facecolor='black', alpha=0.4, boxstyle='round'))

            # Set extent to the data bounds with a small padding
            minx, miny, maxx, maxy = gdf_3857.total_bounds
            pad_x = (maxx - minx) * 0.08
            pad_y = (maxy - miny) * 0.08
            ax.set_xlim(minx - pad_x, maxx + pad_x)
            ax.set_ylim(miny - pad_y, maxy + pad_y)

            ax.set_axis_off()

            # Add basemap if available; otherwise warn and save plain overlay
            image_file = f"images/{output_prefix}.png"
            if _HAS_CONTEXTILY:
                try:
                    ctx.add_basemap(ax, crs=gdf_3857.crs.to_string(), source=ctx.providers.CartoDB.Positron)
                except Exception as e:
                    warnings.warn(f"contextily basemap failed: {e}")

            plt.tight_layout()
            fig.savefig(image_file, dpi=300, bbox_inches='tight', pad_inches=0)
            plt.close(fig)

            print(f"Saved map image: {image_file}")
            logging.info(f"Saved map image: {image_file}")

        except Exception as e:
            logging.exception("Failed to create/save map image")
            print(f"Warning: creating map image failed: {e}")
        print(f"Done! Found {len(gdf)} golf courses in {province}.")
        print(f"Saved to:\n  {geojson_file}\n  {csv_file}")

        logging.info(f"Completed successfully with {len(gdf)} entries.")

    except Exception as e:
        logging.exception("Error during collection")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
