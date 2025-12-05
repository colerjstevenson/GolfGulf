"""
Module for counting amenities/locations by type in cities using OpenStreetMap data.
Stores results in a JSON file with city as the key.
"""

import json
from pathlib import Path
from typing import Dict, Optional, List
import osmnx as ox
import geopandas as gpd
import warnings

warnings.filterwarnings('ignore')


class CityAmenityCounter:
    """Count and track amenities (parks, pools, rinks, etc.) by city."""
    
    def __init__(self, output_json: str = "city_amenities.json"):
        """
        Initialize the counter with an output JSON file.
        
        Args:
            output_json: Path to JSON file for storing results
        """
        self.output_path = Path(output_json)
        self.data = self._load_data()
    
    def _load_data(self) -> Dict:
        """Load existing data from JSON file or create empty dict."""
        if self.output_path.exists():
            try:
                with open(self.output_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse {self.output_path}, starting fresh")
                return {}
        return {}
    
    def _save_data(self):
        """Save current data to JSON file."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
    
    def _get_city_boundary(self, city: str, province: Optional[str] = None, country: str = "Canada") -> gpd.GeoDataFrame:
        """
        Get city boundary polygon using OSMnx geocoding.
        
        Args:
            city: City name
            province: Optional province/state name
            country: Country name (default: Canada)
            
        Returns:
            GeoDataFrame with city boundary
        """
        if province:
            query = f"{city}, {province}, {country}"
        else:
            query = f"{city}, {country}"
        
        try:
            gdf = ox.geocode_to_gdf(query)
            if gdf is None or len(gdf) == 0:
                raise ValueError(f"Could not find boundary for: {query}")
            return gdf
        except Exception as e:
            raise ValueError(f"Failed to geocode '{query}': {e}")

    def _buffer_boundary(self, gdf: gpd.GeoDataFrame, buffer_km: float) -> gpd.GeoSeries:
        """Optionally buffer the boundary by a radius in kilometers (for metro-ish reach)."""
        if buffer_km <= 0:
            return gdf.geometry
        # Use a metric CRS for buffering (Web Mercator is fine for modest buffers)
        gdf_metric = gdf.to_crs(epsg=3857)
        buffered = gdf_metric.geometry.buffer(buffer_km * 1000.0)
        return buffered.to_crs(epsg=4326)
    
    def _get_osm_tags(self, location_type: str) -> Dict[str, str]:
        """
        Map location type to OpenStreetMap tags.
        
        Args:
            location_type: Type of location (e.g., 'parks', 'pools', 'hockey rinks')
            
        Returns:
            Dictionary of OSM tags to query
        """
        location_type_lower = location_type.lower()
        
        # Common location type mappings to OSM tags
        tag_mappings = {
            'parks': {'leisure': 'park'},
            'park': {'leisure': 'park'},
            'pools': {'leisure': 'swimming_pool'},
            'pool': {'leisure': 'swimming_pool'},
            'swimming pools': {'leisure': 'swimming_pool'},
            'hockey rinks': {'sport': 'ice_hockey'},
            'hockey rink': {'sport': 'ice_hockey'},
            'ice hockey': {'sport': 'ice_hockey'},
            'ice rinks': {'sport': 'ice_hockey'},
            'ice rink': {'sport': 'ice_hockey'},
            'golf courses': {'leisure': 'golf_course'},
            'golf course': {'leisure': 'golf_course'},
            'playgrounds': {'leisure': 'playground'},
            'playground': {'leisure': 'playground'},
            'sports centres': {'leisure': 'sports_centre'},
            'sports center': {'leisure': 'sports_centre'},
            'sports centres': {'leisure': 'sports_centre'},
            'gyms': {'leisure': 'fitness_centre'},
            'gym': {'leisure': 'fitness_centre'},
            'fitness centres': {'leisure': 'fitness_centre'},
            'basketball courts': {'sport': 'basketball'},
            'basketball': {'sport': 'basketball'},
            'tennis courts': {'sport': 'tennis'},
            'tennis': {'sport': 'tennis'},
            'soccer fields': {'sport': 'soccer'},
            'soccer': {'sport': 'soccer'},
            'baseball fields': {'sport': 'baseball'},
            'baseball': {'sport': 'baseball'},
            'stadiums': {'leisure': 'stadium'},
            'stadium': {'leisure': 'stadium'},
            'libraries': {'amenity': 'library'},
            'library': {'amenity': 'library'},
            'schools': {'amenity': 'school'},
            'school': {'amenity': 'school'},
            'hospitals': {'amenity': 'hospital'},
            'hospital': {'amenity': 'hospital'},
        }
        
        if location_type_lower in tag_mappings:
            return tag_mappings[location_type_lower]
        
        # If not found, try to guess based on common patterns
        print(f"Warning: Unknown location type '{location_type}', attempting generic leisure tag")
        return {'leisure': location_type_lower.replace(' ', '_')}
    
    def count_amenities(
        self,
        city: str,
        location_type: str,
        province: Optional[str] = None,
        country: str = "Canada",
        min_area_m2: float = 0.0,
        buffer_km: float = 0.0
    ) -> Dict[str, any]:
        """
        Count amenities of a given type in a city.
        
        Args:
            city: City name
            location_type: Type of location (e.g., 'parks', 'pools', 'hockey rinks')
            province: Optional province/state name
            country: Country name (default: Canada)
            min_area_m2: Minimum area threshold for polygons (default: 0)
            
        Returns:
            Dictionary with count and total_area_m2
        """
        print(f"Querying {location_type} in {city}...")
        
        # Get city boundary and optionally buffer for metro area
        boundary = self._get_city_boundary(city, province, country)
        buffered_geom = self._buffer_boundary(boundary, buffer_km)
        polygon = buffered_geom.iloc[0]
        
        # Get OSM tags for this location type
        tags = self._get_osm_tags(location_type)
        print(f"  Using OSM tags: {tags}")
        
        # Query OSM data
        try:
            # Try new API first (osmnx >= 1.0)
            try:
                gdf = ox.features.features_from_polygon(polygon, tags)
            except AttributeError:
                # Fall back to older API
                gdf = ox.geometries_from_polygon(polygon, tags)
        except Exception as e:
            print(f"  No features found or query failed: {e}")
            return {'count': 0, 'total_area_m2': 0.0}
        
        if gdf.empty:
            print(f"  Found 0 {location_type}")
            return {'count': 0, 'total_area_m2': 0.0}
        
        # Project to metric CRS for area calculation (NAD83 / Canada Albers)
        gdf_projected = gdf.to_crs(epsg=3347)
        
        # Calculate areas for polygons
        gdf_projected['area_m2'] = gdf_projected.geometry.area
        
        # Filter by minimum area if specified
        if min_area_m2 > 0:
            gdf_projected = gdf_projected[gdf_projected['area_m2'] >= min_area_m2]
        
        count = len(gdf_projected)
        total_area = float(gdf_projected['area_m2'].sum())
        
        print(f"  Found {count} {location_type} (total area: {total_area:,.0f} m²)")
        
        return {
            'count': count,
            'total_area_m2': total_area
        }
    
    def add_to_city(
        self,
        city: str,
        location_type: str,
        province: Optional[str] = None,
        country: str = "Canada",
        min_area_m2: float = 0.0,
        buffer_km: float = 0.0
    ):
        """
        Count amenities and add to the city's data in the JSON file.
        
        Args:
            city: City name
            location_type: Type of location (e.g., 'parks', 'pools')
            province: Optional province/state name
            country: Country name (default: Canada)
            min_area_m2: Minimum area threshold for polygons
        """
        # Count amenities
        result = self.count_amenities(city, location_type, province, country, min_area_m2, buffer_km)
        
        # Initialize city entry if needed
        if city not in self.data:
            self.data[city] = {}
        
        # Add location type data
        self.data[city][location_type] = result
        
        # Save to file
        self._save_data()
        print(f"  Saved to {self.output_path}")
    
    def get_city_data(self, city: str) -> Optional[Dict]:
        """Get all amenity data for a city."""
        return self.data.get(city)
    
    def get_all_data(self) -> Dict:
        """Get all data for all cities."""
        return self.data


def main():
    """Example usage and CLI interface."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Count amenities/locations by type in cities using OpenStreetMap data."
    )
    parser.add_argument('city', help='City name')
    parser.add_argument('location_type', help='Type of location (e.g., parks, pools, hockey rinks)')
    parser.add_argument('--province', help='Province/state name (optional)')
    parser.add_argument('--country', default='Canada', help='Country name (default: Canada)')
    parser.add_argument('--output', default='city_amenities.json', help='Output JSON file (default: city_amenities.json)')
    parser.add_argument('--min-area', type=float, default=0.0, help='Minimum area in m² for polygons (default: 0)')
    parser.add_argument('--buffer-km', type=float, default=0.0, help='Optional buffer distance (km) around the city boundary to approximate metro area')
    
    args = parser.parse_args()
    
    # Create counter and process
    counter = CityAmenityCounter(output_json=args.output)
    counter.add_to_city(
        city=args.city,
        location_type=args.location_type,
        province=args.province,
        country=args.country,
        min_area_m2=args.min_area,
        buffer_km=args.buffer_km
    )
    
    # Show current city data
    print("\nCurrent data for", args.city + ":")
    city_data = counter.get_city_data(args.city)
    if city_data:
        for loc_type, data in city_data.items():
            print(f"  {loc_type}: {data['count']} (area: {data['total_area_m2']:,.0f} m²)")


if __name__ == '__main__':
    main()
