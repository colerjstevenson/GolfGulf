import requests

api_key = "AIzaSyBmvKPSj81MeM7kslURmg8V5GGI-PTGSco"

import requests
import json
import os
from math import isclose

CACHE_FILE = "golf_course_cache.json"

def load_cache():
    """Load cache from disk, or return empty dict if none exists."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    """Save cache to disk."""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def find_nearby_golf_course(lat, lon, radius=50, tolerance=0.0005):
    """
    Check for nearby golf courses using Google Places API.
    Results are cached locally by rounded coordinates.
    """
    cache = load_cache()
    print(f"Looking up nearby golf course for ({lat}, {lon})...")
    # Round lat/lon to reduce unnecessary API calls for nearby points
    lat_key = round(lat, 4)
    lon_key = round(lon, 4)
    key = f"{lat_key},{lon_key}"

    if key in cache:
        return cache[key]

    # Build API request
    url = (
        "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        f"?location={lat},{lon}&radius={radius}&type=golf_course&key={api_key}"
    )

    response = requests.get(url)
    data = response.json()

    if "results" in data and len(data["results"]) > 0:
        first_course = data["results"][0]
        result = {
            "name": first_course["name"],
            "address": first_course.get("vicinity", "Address not available"),
            "lat": first_course["geometry"]["location"]["lat"],
            "lon": first_course["geometry"]["location"]["lng"]
        }
    else:
        result = None

    # Save to cache
    cache[key] = result
    save_cache(cache)

    return result

# Example usage:
if __name__ == "__main__":
    lat, lon = 43.6532, -79.3832  # Toronto
    course = find_nearby_golf_course(lat, lon)

    if course:
        print(f"Found golf course: {course['name']} at {course['address']}")
    else:
        print("No golf course found nearby.")

