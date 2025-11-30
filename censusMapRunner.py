import subprocess
import sys
import time
from pathlib import Path

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


def ensure_maps():
    # Build maps for all cached cities (script handles skipping/non-existent automatically)
    run([sys.executable, 'build_all_maps.py'])


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
    ensure_all_caches()
    ensure_maps()
    generate_index()
    start_server(8000)
    open_index(8000)
    print('All done.')

if __name__ == '__main__':
    main()
