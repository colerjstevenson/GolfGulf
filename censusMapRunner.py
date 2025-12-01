import argparse
import subprocess
import sys
import time
from pathlib import Path
from interactive_map_builder import build_assets

# Known Canadian cities with provinces (expandable)
KNOWN_CITIES = {
    'Calgary': 'Alberta',
    'Edmonton': 'Alberta',
    'Halifax': 'Nova Scotia',
    'Hamilton': 'Ontario',
    'Kitchener': 'Ontario',
    'London': 'Ontario',
    'Montreal': 'Quebec',
    'Ottawa': 'Ontario',
    'Quebec City': 'Quebec',
    'Regina': 'Saskatchewan',
    'Saskatoon': 'Saskatchewan',
    'Toronto': 'Ontario',
    'Vancouver': 'British Columbia',
    'Victoria': 'British Columbia',
    'Winnipeg': 'Manitoba',
}

DATA_ROOT = Path('data') / 'censusShape'
MAPS_ROOT = Path('maps')


def run(cmd, check=True):
    print('> ' + ' '.join(cmd))
    proc = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr)
    if check and proc.returncode != 0:
        raise SystemExit(proc.returncode)
    return proc.returncode


def ensure_cache_for_city(city: str, province: str):
    slug = city.replace(' ', '_').lower()
    city_dir = DATA_ROOT / slug
    if (city_dir / 'profile_cache.json').exists() or list(city_dir.glob('*_profile_cache.json')):
        print(f"Cache exists for {city} ({city_dir}).")
        return
    print(f"Building cache for {city} ({province})…")
    # Invoke census_cacher.py: assumes it supports city/province args
    run([sys.executable, 'census_cacher.py', city, province])


def ensure_all_caches():
    DATA_ROOT.mkdir(exist_ok=True, parents=True)
    for city, prov in KNOWN_CITIES.items():
        try:
            ensure_cache_for_city(city, prov)
            time.sleep(1)  # be kind to geocoders
        except Exception as e:
            print(f"Failed to build cache for {city}: {e}")


def infer_city_name(slug: str) -> str:
    return ' '.join(w.capitalize() for w in slug.split('_'))


def iter_cached_city_slugs():
    if not DATA_ROOT.exists():
        return []
    slugs = []
    for d in DATA_ROOT.iterdir():
        if d.is_dir() and ((d / 'profile_cache.json').exists() or list(d.glob('*_profile_cache.json'))):
            slugs.append(d.name)
    return slugs


def map_output_path_for_slug(slug: str) -> Path:
    return MAPS_ROOT / f"{slug}_interactive_map.html"


def ensure_maps(overwrite: bool = False):
    MAPS_ROOT.mkdir(exist_ok=True, parents=True)
    slugs = iter_cached_city_slugs()
    if not slugs:
        print("No cached cities found under data/censusShape; nothing to build.")
        return
    print(f"Evaluating {len(slugs)} city/cities for map generation…")
    built = 0
    skipped = 0
    for slug in slugs:
        out_path = map_output_path_for_slug(slug)
        if out_path.exists() and not overwrite:
            print(f"Skip existing map: {out_path}")
            skipped += 1
            continue
        city_name = infer_city_name(slug)
        province = KNOWN_CITIES.get(city_name, '')
        print(f"Building interactive map for {city_name} ({province or 'province auto'})…")
        try:
            build_assets(city_name, province, data_root=DATA_ROOT, out_root=MAPS_ROOT)
            time.sleep(1)
            built += 1
        except Exception as e:
            print(f"FAILED {city_name}: {e}")
    print(f"Map generation complete. Built: {built}, Skipped: {skipped}.")


def generate_index():
    run([sys.executable, 'generate_index.py'])


def start_server(port=8000):
    # Start http.server in background
    print(f"Starting local server on http://localhost:{port} …")
    subprocess.Popen([sys.executable, '-m', 'http.server', str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    time.sleep(1.5)


def open_index(port=8000):
    url = f"http://localhost:{port}/maps/index.html"
    print(f"Opening {url}")
    # Use PowerShell Start-Process for Windows
    try:
        run(['powershell', '-NoProfile', '-Command', f"Start-Process '{url}'"], check=False)
    except Exception:
        # Fallback: print URL
        print(url)


def main():
    parser = argparse.ArgumentParser(description='Build census caches, maps, and serve index')
    parser.add_argument('--overwrite', action='store_true', help='Regenerate maps even if output HTML already exists')
    parser.add_argument('--no-serve', action='store_true', help='Do not start local HTTP server')
    parser.add_argument('--port', type=int, default=8000, help='Port for local HTTP server')
    args = parser.parse_args()

    ensure_all_caches()
    ensure_maps(overwrite=args.overwrite)
    generate_index()

    if not args.no_serve:
        start_server(args.port)
        open_index(args.port)
    print('All done.')

if __name__ == '__main__':
    main()
