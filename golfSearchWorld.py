#!/usr/bin/env python3
"""
Collect golf courses across all US states using OpenStreetMap data via the OSMnx library.
Processes each state individually, fetches course data, cleans it, and generates visualizations.
"""
import os
import osmnx as ox
import geopandas as gpd
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import warnings
import gc

from nameFiller import find_nearby_golf_course

# Try to import contextily for basemap support (optional)
try:
    import contextily as ctx
    _HAS_CONTEXTILY = True
except Exception:
    _HAS_CONTEXTILY = False

# Configure logging to write messages to a log file with timestamps
# Specify UTF-8 encoding so log writes won't fail on non-ASCII characters
logging.basicConfig(
    filename="golf_course_collection.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding='utf-8'
)

# Memory management settings
MAX_MEMORY_PERCENT = 85  # Warn if memory usage exceeds 85%
GC_INTERVAL = 1  # Run garbage collection after each state
STATE_COUNT = 0  # Track states processed
MAKE_MAP = True  # Set to True to generate map images

def cleanup_memory():
    """
    Explicitly clean up memory by closing matplotlib figures and running garbage collection.
    This helps prevent memory buildup when processing multiple large datasets.
    """
    # Close all matplotlib figures to free memory
    plt.close('all')
    # Force garbage collection
    gc.collect()
    
def check_memory_usage():
    """
    Check current memory usage and log warnings if approaching system limits.
    Returns True if memory is within acceptable range, False if too high.
    """
    try:
        import os
        # Get memory info from /proc/meminfo on Linux, or use psutil-free alternative
        try:
            with open('/proc/meminfo', 'r') as f:
                meminfo = dict((i.split()[0].rstrip(':'), int(i.split()[1])) for i in f.readlines())
                mem_percent = (1.0 - float(meminfo['MemAvailable']) / float(meminfo['MemTotal'])) * 100
        except:
            # Fallback: use a simple check without psutil
            mem_percent = 0
        
        if mem_percent > MAX_MEMORY_PERCENT:
            logging.warning(f"Memory usage high: {mem_percent:.1f}%")
            return False
        return True
    except Exception as e:
        logging.debug(f"Could not check memory: {e}")
        return True

def state_abbreviation(state_name):
    state_to_abbrev = {
        "alabama": "AL",
        "alaska": "AK",
        "arizona": "AZ",
        "arkansas": "AR",
        "california": "CA",
        "colorado": "CO",
        "connecticut": "CT",
        "delaware": "DE",
        "florida": "FL",
        "georgia": "GA",
        "hawaii": "HI",
        "idaho": "ID",
        "illinois": "IL",
        "indiana": "IN",
        "iowa": "IA",
        "kansas": "KS",
        "kentucky": "KY",
        "louisiana": "LA",
        "maine": "ME",
        "maryland": "MD",
        "massachusetts": "MA",
        "michigan": "MI",
        "minnesota": "MN",
        "mississippi": "MS",
        "missouri": "MO",
        "montana": "MT",
        "nebraska": "NE",
        "nevada": "NV",
        "new hampshire": "NH",
        "new jersey": "NJ",
        "new mexico": "NM",
        "new york": "NY",
        "north carolina": "NC",
        "north dakota": "ND",
        "ohio": "OH",
        "oklahoma": "OK",
        "oregon": "OR",
        "pennsylvania": "PA",
        "rhode island": "RI",
        "south carolina": "SC",
        "south dakota": "SD",
        "tennessee": "TN",
        "texas": "TX",
        "utah": "UT",
        "vermont": "VT",
        "virginia": "VA",
        "washington": "WA",
        "west virginia": "WV",
        "wisconsin": "WI",
        "wyoming": "WY",
        "mexico": "MX",
        "united kingdom": "UK",
        "france métropolitaine": "FR",
        "germany": "DE",
        "spain": "ES",
        "ireland": "IE",
        "costa rica": "CR",
        "qatar": "QA",
        "south korea": "KR",
        "south africa": "ZA",
        "denmark": "DK",
        "sweden": "SE",
        "belgium": "BE",
        "dominican republic": "DO",
        "united arab emirates": "AE"
    }

    key = state_name.strip().lower()
    return state_to_abbrev.get(key, 'XX')

def run_states(state):
    """
    Process golf course data for a specified US state.
    
    Args:
        state (str): The name of the region to process
    """
    province = state
    region = f"{province}"
    abbrev = state_abbreviation(province)
    output_prefix = f"golf_courses_{province.lower().replace(' ', '_')}"
    gdf = None
    
    # Check if output files already exist; if so, skip OSM data collection and load cached data
    if os.path.exists(f"{output_prefix}.geojson") and os.path.exists(f"{output_prefix}.csv"):
        logging.info("Existing data found, loading...")
        gdf = gpd.read_file(f"{output_prefix}.geojson")
    else:
        logging.info(f"Starting golf course collection for {region}")

    try:
        # Attempt to fetch golf course data from OpenStreetMap using OSMnx library
        
        if gdf is None:
            print(f"Fetching golf courses in {region} from OpenStreetMap...")
            # Query OSM for all features tagged as golf courses
            tags = {'leisure': 'golf_course'}
            gdf = ox.features_from_place(region, tags=tags)

            # Exit early if no golf courses were found in the region
            if gdf.empty:
                logging.warning("No golf courses found in OSM data for this region.")
                print("No golf courses found. Try a smaller subregion or check OSM coverage.")
                cleanup_memory()
                return

            logging.info(f"Fetched {len(gdf)} golf courses.")

            # Project to NAD83 StatsCan Lambert (meters) for accurate area calculations
            gdf = gdf.to_crs(epsg=3347)  # NAD83 / StatsCan Lambert (meters)
            # Calculate area in square meters for each golf course polygon
            gdf['area_m2'] = gdf['geometry'].area

            # Extract latitude and longitude directly without creating intermediate copies
            # Convert centroid to WGS84 (EPSG:4326) for lat/lon coordinates
            gdf_wgs84 = gdf.to_crs(epsg=4326)
            gdf['lat'] = gdf_wgs84['geometry'].centroid.y
            gdf['lon'] = gdf_wgs84['geometry'].centroid.x
            del gdf_wgs84  # Explicitly delete to free memory
            
            # Add state abbreviation and generate unique golf course IDs
            gdf['province'] = [province for _ in range(len(gdf))]
            # get state abbreviation
            
            gdf['gcid'] = [f'{abbrev}{i+1:05d}' for i in range(len(gdf))]
            gdf['name'] = gdf['name'].fillna('')
            
            # Fill in missing course names by searching nearby features
            name_check = []
            for name, lat, lon in zip(gdf['name'], gdf['lat'], gdf['lon']):
                print(name)
                if not name or name == '':
                    # Try to find a nearby golf course name
                    check = find_nearby_golf_course(lat, lon)
                    if check:
                        name_check.append(check['name'])
                    else:
                        name_check.append('Unknown Golf Course')
                else:
                    name_check.append(name)
            
            gdf['name'] = name_check
            
            # Remove very small polygons (often errors or mini-putt courses)
            gdf = gdf[gdf['area_m2'] > 10000]  # Keep only areas > 1 hectare
            # Sort by area for better data organization
            gdf = gdf.sort_values(by='area_m2', ascending=False)

            # Save processed data to GeoJSON and CSV formats
            print("Saving results...")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")

            geojson_file = f"data/world/{output_prefix}.geojson"
            csv_file = f"data/world/{output_prefix}.csv"

            gdf.to_file(geojson_file, driver="GeoJSON")
            # Save as CSV without geometry column for easier spreadsheet viewing
            gdf.drop(columns=['leisure','geometry']).to_csv(csv_file, index=False)
        
        # Create a visual map overlay showing golf course locations
        # Note: MAKE_MAP flag is currently undefined - set this to True/False as needed
        if MAKE_MAP == True:
            try:
                print("Creating map image with translucent overlays...")

                # Convert to Web Mercator (EPSG:3857) for basemap tile compatibility
                gdf_3857 = gdf.to_crs(epsg=3857)

                # Initialize figure and axis with specified dimensions
                fig, ax = plt.subplots(figsize=(12, 12))

                # Draw golf course polygons as translucent green boxes with black outlines
                gdf_3857.plot(ax=ax, facecolor='tab:green', edgecolor='black', alpha=0.35, linewidth=0.8)

                # Compute bounds without creating unnecessary copies
                minx, miny, maxx, maxy = gdf_3857.total_bounds
                pad_x = (maxx - minx) * 0.08
                pad_y = (maxy - miny) * 0.08
                ax.set_xlim(minx - pad_x, maxx + pad_x)
                ax.set_ylim(miny - pad_y, maxy + pad_y)

                ax.set_axis_off()

                # Add basemap tiles if contextily is available
                image_file = f"images/{output_prefix}.png"
                if _HAS_CONTEXTILY:
                    try:
                        ctx.add_basemap(ax, crs=gdf_3857.crs.to_string(), source=ctx.providers.CartoDB.Positron)
                    except Exception as e:
                        warnings.warn(f"contextily basemap failed: {e}")

                # Save map image at high resolution
                plt.tight_layout()
                fig.savefig(image_file, dpi=300, bbox_inches='tight', pad_inches=0)
                plt.close(fig)
                
                # Clean up map-specific data
                del gdf_3857
                del fig, ax

                print(f"Saved map image: {image_file}")
                logging.info(f"Saved map image: {image_file}")

            except Exception as e:
                logging.exception("Failed to create/save map image")
                print(f"Warning: creating map image failed: {e}")
                # Ensure figures are closed even if error occurs
                plt.close('all')
        
        print(f"Done! Found {len(gdf)} golf courses in {province}.")
        print(f"Saved to:\n  {geojson_file}\n  {csv_file}")

        logging.info(f"Completed successfully with {len(gdf)} entries.")

    except Exception as e:
        # Log any errors that occur during the data collection process
        logging.exception("Error during collection")
        print(f"An error occurred: {e}")

def main():
    """
    Main entry point: processes golf course data for all US states listed in states_list.txt
    """
    global STATE_COUNT
    
    # Load state names from file (one state per line)
    # ignore states that start with #
    # Read state list using utf-8-sig to gracefully handle a UTF-8 BOM if present
    with open("states_list.txt", "r", encoding='utf-8-sig') as f:
        states = f.read().splitlines()
        states = [state for state in states if not state.strip().startswith('#')]
    
    total_states = len(states)
    
    # Iterate through each state and process golf course data
    for state in states:
        STATE_COUNT += 1
        print(f"\n[{STATE_COUNT}/{total_states}] Processing state: {state}")
        
        # Skip states that have already been processed
        if os.path.exists(f"data/usa/golf_courses_{state.lower().replace(' ', '_')}.geojson") and os.path.exists(f"data/usa/golf_courses_{state.lower().replace(' ', '_')}.csv"):
            print(f"Data for {state} already exists, skipping...")
            continue
        
        try:
            # Check memory before processing
            if not check_memory_usage():
                logging.warning(f"Skipping {state} due to high memory usage")
                print(f"Skipping {state} due to high memory usage. Please clear memory and restart.")
                continue
            
            run_states(state)
            
            # Clean up memory after each state
            if STATE_COUNT % GC_INTERVAL == 0:
                print("Performing memory cleanup...")
                cleanup_memory()
                logging.info("Memory cleanup performed")
                
        except Exception as e:
            # Log errors for individual states and continue processing others
            logging.exception(f"Error processing state {state}")
            print(f"An error occurred while processing state {state}: {e}")
            # Clean up even if error occurs
            cleanup_memory()
    
    # Final cleanup
    cleanup_memory()
    print("\nAll states processed!")
    logging.info("All states processed successfully")

if __name__ == "__main__":
    main()
