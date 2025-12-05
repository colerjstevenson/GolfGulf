"""
Wrapper script to collect amenity data for major cities across Canada and the US.
"""

from city_amenity_counter import CityAmenityCounter
import time

# Define major cities with their provinces/states
CANADIAN_CITIES = [
    ("Toronto", "Ontario"),
    ("Montreal", "Quebec"),
    ("Vancouver", "British Columbia"),
    ("Calgary", "Alberta"),
    ("Edmonton", "Alberta"),
    ("Ottawa", "Ontario"),
    ("Winnipeg", "Manitoba"),
]

US_CITIES = [
    ("New York", "New York"),
    ("Los Angeles", "California"),
    ("Chicago", "Illinois"),
    ("Houston", "Texas"),
    ("Phoenix", "Arizona"),
    ("Philadelphia", "Pennsylvania"),
    ("San Antonio", "Texas"),
    ("San Diego", "California"),
    ("Palm Springs", "California"),
    ("Scottsdale", "Arizona"),
    ("Dallas", "Texas"),
    ("San Jose", "California"),
    ("Austin", "Texas"),
    ("Jacksonville", "Florida"),
    ("Fort Worth", "Texas"),
    ("Columbus", "Ohio"),
    ("Indianapolis", "Indiana"),
    ("Charlotte", "North Carolina"),
    ("San Francisco", "California"),
    ("Seattle", "Washington"),
    ("Denver", "Colorado"),
    ("Washington", "District of Columbia"),
    ("Boston", "Massachusetts"),
    ("Nashville", "Tennessee"),
    ("Detroit", "Michigan"),
    ("Portland", "Oregon"),
    ("Las Vegas", "Nevada"),
]

# Amenity types to collect
AMENITY_TYPES = [
    "parks",
    "pools",
    "hockey rinks",
    "golf courses",
    "playgrounds",
    "sports centres",
    "basketball courts",
    "tennis courts",
    "soccer fields",
    "baseball fields",
    "libraries",
    "Schools",
    "hospitals",
]


def collect_all_amenities(
    cities,
    country,
    output_file="city_amenities.json",
    delay=0.05,
    skip_existing=True,
    buffer_km=0.0
):
    """
    Collect all amenity types for a list of cities sequentially.
    
    Args:
        cities: List of (city, province/state) tuples
        country: "Canada" or "United States"
        output_file: JSON file to save results
        delay: Delay in seconds between queries to be respectful to OSM servers
        skip_existing: Skip city/amenity combinations that already exist in the JSON
    """
    counter = CityAmenityCounter(output_json=output_file)
    
    # Build list of tasks
    tasks = []
    for city, province in cities:
        for amenity in AMENITY_TYPES:
            # Check if already exists
            if skip_existing:
                city_data = counter.get_city_data(city)
                if city_data and amenity in city_data:
                    continue
            tasks.append((city, province, amenity, country))
    
    total = len(tasks)
    completed = 0
    
    print(f"\nProcessing {total} tasks sequentially...")
    
    # Execute tasks sequentially
    for city, province, amenity, country in tasks:
        completed += 1
        
        try:
            result = counter.count_amenities(
                city=city,
                location_type=amenity,
                province=province,
                country=country,
                min_area_m2=0.0,
                buffer_km=buffer_km,
            )
            
            # Update data and save
            if city not in counter.data:
                counter.data[city] = {}
            counter.data[city][amenity] = result
            counter._save_data()
            
            count = result['count']
            area = result['total_area_m2']
            print(f"[{completed}/{total}] ✓ {city} - {amenity}: {count} (area: {area:,.0f} m²)")
            
            # Be respectful to OSM servers
            if delay > 0:
                time.sleep(delay)
            
        except Exception as e:
            print(f"[{completed}/{total}] ✗ {city} - {amenity}: ERROR - {e}")
    
    print(f"\n{'='*60}")
    print(f"Collection complete! Data saved to {output_file}")
    print(f"{'='*60}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Collect amenity data for major cities across Canada and the US."
    )
    parser.add_argument(
        '--country',
        choices=['canada', 'us', 'both'],
        default='both',
        help='Which country to collect data for (default: both)'
    )
    parser.add_argument(
        '--output',
        default='city_amenities.json',
        help='Output JSON file (default: city_amenities.json)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='Delay in seconds between queries (default: 1.0)'
    )
    parser.add_argument(
        '--buffer-km',
        type=float,
        default=0.0,
        help='Optional buffer distance in km around city boundary to approximate metro area (default: 0)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Re-query existing data (default: skip existing)'
    )
    
    args = parser.parse_args()
    
    skip_existing = not args.force
    
    if args.country == 'canada' or args.country == 'both':
        print("\n" + "="*60)
        print("COLLECTING DATA FOR CANADIAN CITIES")
        print("="*60)
        collect_all_amenities(
            CANADIAN_CITIES,
            "Canada",
            output_file=args.output,
            delay=args.delay,
            skip_existing=skip_existing,
            buffer_km=args.buffer_km,
        )
    
    if args.country == 'us' or args.country == 'both':
        print("\n" + "="*60)
        print("COLLECTING DATA FOR US CITIES")
        print("="*60)
        collect_all_amenities(
            US_CITIES,
            "United States",
            output_file=args.output,
            delay=args.delay,
            skip_existing=skip_existing,
            buffer_km=args.buffer_km,
        )
    
    # Print summary
    counter = CityAmenityCounter(output_json=args.output)
    all_data = counter.get_all_data()
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total cities: {len(all_data)}")
    
    for city, amenities in sorted(all_data.items()):
        total_amenities = sum(a['count'] for a in amenities.values())
        print(f"  {city}: {len(amenities)} amenity types, {total_amenities} total locations")


if __name__ == '__main__':
    main()
