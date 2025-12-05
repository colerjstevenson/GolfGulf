import argparse
import re
import subprocess
import sys
import time
from pathlib import Path
from interactive_map_builder import build_assets
import importlib

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


def ensure_cache_if_needed(city_name: str, province: str, out_path: Path):
    """
    Ensure cache only when we intend to build (or rebuild) a city's map.
    If map output exists already, skip cache work.
    """
    if out_path.exists():
        print(f"Map exists for {city_name}; skipping cache check.")
        return
    ensure_cache_for_city(city_name, province)


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
            # Only ensure cache when we are actually building
            ensure_cache_if_needed(city_name, province, out_path)
            build_assets(city_name, province, data_root=DATA_ROOT, out_root=MAPS_ROOT)
            time.sleep(1)
            built += 1
        except Exception as e:
            print(f"FAILED {city_name}: {e}")
    print(f"Map generation complete. Built: {built}, Skipped: {skipped}.")


def generate_index():
    run([sys.executable, 'generate_index.py'])
    apply_index_enhancements()


def apply_index_enhancements():
    """
    Post-process maps/index.html to split into 'Maps' and 'Data' sections and
    add links to the amenities dashboard and JSON files. Idempotent.
    """
    index_path = MAPS_ROOT / 'index.html'
    if not index_path.exists():
        print(f"Index not found at {index_path}; skipping enhancements.")
        return
    try:
        html = index_path.read_text(encoding='utf-8')
    except Exception:
        html = index_path.read_text()

    # We won't early-return; we may need to restructure Data/Visuals sections.

    # Insert Maps/Data sections by simple replacements
    # Wrap the main grid in a Maps section if not already
    if "<div id='grid' class='grid'>" in html and '<section' not in html:
        html = html.replace(
            "<main><div id='grid' class='grid'>",
            "<main>\n<section style='margin-bottom:26px'>\n<h2 style='margin:0 0 10px 0;font-size:16px;'>Maps</h2>\n<div id='grid' class='grid'>"
        )
        html = html.replace(
            "</div><div id='empty'",
            "</div>\n<div id='empty'"
        )
        html = html.replace(
            "</div><div id='empty' class='empty' style='display:none'>No cities match your search.</div></main>",
            "</div><div id='empty' class='empty' style='display:none'>No cities match your search.</div></section>\n</main>"
        )

    # Remove any stray dashboard links from elsewhere to avoid duplicates
    html = re.sub(r"<a[^>]+href=\"?\'\..?/visualize_city_amenities\.html\'?\"?[^>]*>[\s\S]*?</a>", "", html, flags=re.IGNORECASE)

    # Ensure a Visuals section exists with the dashboard link
    if 'Visuals</h2>' not in html:
        visuals_section = (
            "\n<section>\n"
            "<h2 style='margin:0 0 10px 0;font-size:16px;'>Visuals</h2>\n"
            "<div class='grid'>\n"
            "  <a class='city-btn' href='../visualize_city_amenities.html' data-name='amenities'>City Amenities Dashboard\n"
            "    <span class='meta'>Interactive comparison of amenities data</span>\n"
            "  </a>\n"
            "  <a class='city-btn' href='../visualize_golf_vs_demographics.html' data-name='golf vs demographics'>Golf vs Demographics\n"
            "    <span class='meta'>Golf course counts vs ACS metrics</span>\n"
            "  </a>\n"
            "  <a class='city-btn' href='../visualize_golf_animation.html' data-name='golf animation'>Golf Courses Animation\n"
            "    <span class='meta'>Animated GIF of golf courses</span>\n"
            "  </a>\n"
            "  <a class='city-btn' href='../visualize_golf_courses_across_cities.html' data-name='golf courses across cities'>Golf Courses Across Cities\n"
            "    <span class='meta'>Grouped bars: count & area</span>\n"
            "  </a>\n"
            "</div>\n"
            "</section>\n"
        )
        # Insert Visuals before closing </main>
        if '</main>' in html:
            html = html.replace('</main>', visuals_section + '</main>')
    else:
        # Visuals exists; ensure it includes the dashboard link
        if '../visualize_city_amenities.html' not in html:
            anchor = (
                "  <a class='city-btn' href='../visualize_city_amenities.html' data-name='amenities'>City Amenities\n"
                "    <span class='meta'>Interactive comparison of amenities data</span>\n"
                "  </a>\n"
            )
            head_idx = html.find('Visuals</h2>')
            if head_idx != -1:
                grid_idx = html.find("<div class='grid'>", head_idx)
                if grid_idx != -1:
                    insert_pos = grid_idx + len("<div class='grid'>")
                    html = html[:insert_pos] + "\n" + anchor + html[insert_pos:]

        # Ensure Golf vs Demographics link exists
        if '../visualize_golf_vs_demographics.html' not in html:
            anchor = (
                "  <a class='city-btn' href='../visualize_golf_vs_demographics.html' data-name='golf vs demographics'>Golf vs Demographics\n"
                "    <span class='meta'>Golf course counts vs ACS metrics</span>\n"
                "  </a>\n"
            )
            head_idx = html.find('Visuals</h2>')
            if head_idx != -1:
                grid_idx = html.find("<div class='grid'>", head_idx)
                if grid_idx != -1:
                    insert_pos = grid_idx + len("<div class='grid'>")
                    html = html[:insert_pos] + "\n" + anchor + html[insert_pos:]

        # Ensure Golf Courses Animation link exists
        if '../visualize_golf_animation.html' not in html:
            anchor = (
                "  <a class='city-btn' href='../visualize_golf_animation.html' data-name='golf animation'>Golf Courses Animation\n"
                "    <span class='meta'>Animated GIF of golf courses</span>\n"
                "  </a>\n"
            )
            head_idx = html.find('Visuals</h2>')
            if head_idx != -1:
                grid_idx = html.find("<div class='grid'>", head_idx)
                if grid_idx != -1:
                    insert_pos = grid_idx + len("<div class='grid'>")
                    html = html[:insert_pos] + "\n" + anchor + html[insert_pos:]

        # Ensure Golf Courses Across Cities link exists
        if '../visualize_golf_courses_across_cities.html' not in html:
            anchor = (
                "  <a class='city-btn' href='../visualize_golf_courses_across_cities.html' data-name='golf courses across cities'>Golf Courses Across Cities\n"
                "    <span class='meta'>Grouped bars: count & area</span>\n"
                "  </a>\n"
            )
            head_idx = html.find('Visuals</h2>')
            if head_idx != -1:
                grid_idx = html.find("<div class='grid'>", head_idx)
                if grid_idx != -1:
                    insert_pos = grid_idx + len("<div class='grid'>")
                    html = html[:insert_pos] + "\n" + anchor + html[insert_pos:]

    # Ensure a Data section exists with only raw file links
    if 'Data</h2>' not in html:
        data_section = (
            "\n<section>\n"
            "<h2 style='margin:0 0 10px 0;font-size:16px;'>Data</h2>\n"
            "<div class='grid'>\n"
            "  <a class='city-btn' href='../data/city_amenities.json' data-name='amenities json'>Raw Amenities JSON\n"
            "    <span class='meta'>Download data/city_amenities.json</span>\n"
            "  </a>\n"
            "  <a class='city-btn' href='../data/city_demographics.json' data-name='demographics json'>City Demographics JSON\n"
            "    <span class='meta'>Requires ACS fetch</span>\n"
            "  </a>\n"
            "</div>\n"
            "</section>\n"
        )
        if '</main>' in html:
            html = html.replace('</main>', data_section + '</main>')

    # Dashboard link should now only appear in Visuals section

    # Adjust search script to only target Maps grid if needed
    if "document.querySelectorAll('#grid .city-btn')" not in html:
        html = html.replace(
            "document.querySelectorAll('.city-btn').forEach(b=>{",
            "document.querySelectorAll('#grid .city-btn').forEach(b=>{"
        )

    index_path.write_text(html, encoding='utf-8')
    print('Applied index enhancements: Maps/Data sections added or verified.')


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

    # Build maps; caches are ensured only when building is needed
    ensure_maps(overwrite=args.overwrite)
    generate_index()

    if not args.no_serve:
        start_server(args.port)
        open_index(args.port)
    print('All done.')

if __name__ == '__main__':
    main()
