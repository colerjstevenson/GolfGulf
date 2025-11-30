import time
from pathlib import Path
from interactive_map_builder import build_assets

"""Batch builder for interactive census maps for all cached Canadian cities.
Runs build_assets for each city directory under data/censusShape that contains a
profile cache. Province is omitted; geocoder will resolve using "City, Canada".
"""

def infer_city_name(slug: str) -> str:
    return ' '.join(w.capitalize() for w in slug.split('_'))

def main():
    data_root = Path('data') / 'censusShape'
    if not data_root.exists():
        raise SystemExit(f"Data root not found: {data_root}")
    # Identify city directories
    city_dirs = [d for d in data_root.iterdir() if d.is_dir()]
    targets = []
    for d in city_dirs:
        # profile cache indicator
        if (d / 'profile_cache.json').exists() or list(d.glob('*_profile_cache.json')):
            targets.append(d.name)
    if not targets:
        raise SystemExit("No cached city directories with profile_cache.json found.")
    print(f"Found {len(targets)} cached cities: {', '.join(targets)}")
    failures = []
    for slug in targets:
        city_name = infer_city_name(slug)
        print(f"\n=== Building interactive map for {city_name} ===")
        try:
            build_assets(city_name, '', data_root=data_root, out_root=Path('maps'))
            # Respect Nominatim usage (1 second pause)
            time.sleep(1)
        except Exception as e:
            print(f"FAILED {city_name}: {e}")
            failures.append((city_name, str(e)))
    print("\nBatch build complete.")
    if failures:
        print("Failures:")
        for city, err in failures:
            print(f" - {city}: {err}")
    else:
        print("All cities built successfully.")

if __name__ == '__main__':
    main()
