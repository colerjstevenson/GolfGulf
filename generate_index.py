from pathlib import Path
import json

def count_golf_courses(city_dir: Path) -> int:
    gc_path = city_dir / 'web_assets' / 'golf_courses.geojson'
    if not gc_path.exists():
        return 0
    try:
        with open(gc_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict) and 'features' in data:
            return len(data['features'])
        return 0
    except Exception:
        return 0

def slug_to_name(slug: str) -> str:
    return ' '.join(w.capitalize() for w in slug.split('_'))

def main():
    data_root = Path('data') / 'censusShape'
    maps_root = Path('maps')
    maps_root.mkdir(exist_ok=True)
    cities = []
    for d in data_root.iterdir():
        if not d.is_dir():
            continue
        slug = d.name
        map_file = maps_root / f"{slug}_interactive_map.html"
        if not map_file.exists():
            # skip cities without built map
            continue
        courses = count_golf_courses(d)
        cities.append({
            'slug': slug,
            'name': slug_to_name(slug),
            'golf_count': courses
        })
    cities.sort(key=lambda c: c['name'].lower())
    index_path = maps_root / 'index.html'
    html = ["<!DOCTYPE html>", "<html lang='en'>", "<head>",
            "<meta charset='utf-8' />",
            "<title>Canadian City Census Explorer</title>",
            "<meta name='viewport' content='width=device-width,initial-scale=1' />",
            "<style>:\n    :root { --bg:#f7f9fb; --accent:#1565c0; }\n    body{margin:0;font-family:system-ui,Arial,sans-serif;background:var(--bg);color:#222;}\n    header{padding:16px 20px;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.08);position:sticky;top:0;}\n    h1{margin:0;font-size:20px;}\n    #search{width:100%;padding:8px 10px;margin-top:10px;font-size:14px;border:1px solid #bbb;border-radius:6px;}\n    main{max-width:1100px;margin:24px auto;padding:0 20px 40px;}\n    .grid{display:grid;gap:14px;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));}\n    .city-btn{display:flex;flex-direction:column;align-items:flex-start;padding:14px 12px;background:#fff;border:1px solid #d5d9dd;border-radius:10px;text-decoration:none;color:#222;font-weight:600;font-size:14px;position:relative;transition:.15s box-shadow,.15s transform;}\n    .city-btn:hover{box-shadow:0 2px 6px rgba(0,0,0,.12);transform:translateY(-2px);}\n    .meta{font-size:11px;font-weight:400;opacity:.75;margin-top:4px;}\n    .badge{position:absolute;top:8px;right:8px;background:#1565c0;color:#fff;font-size:11px;padding:2px 6px;border-radius:12px;}\n    .empty{text-align:center;padding:30px;font-size:14px;color:#666;}\n    @media (prefers-color-scheme: dark){body{background:#12171c;color:#e7edf2;}header{background:#1d252c;} .city-btn{background:#1d252c;border-color:#2d3740;color:#e7edf2;} .city-btn:hover{box-shadow:0 2px 6px rgba(0,0,0,.6);} #search{background:#1d252c;border-color:#2d3740;color:#e7edf2;} .badge{background:#1e88e5;}}\n</style>", "</head>", "<body>",
            "<header><h1>Canadian City Census Explorer</h1><input id='search' type='text' placeholder='Filter cities…' /></header>",
            "<main><div id='grid' class='grid'>"]
    for c in cities:
        # Removed badge per user request; only show meta line
        html.append(
            f"<a class='city-btn' href='{c['slug']}_interactive_map.html' data-name='{c['name'].lower()}'>"
            f"{c['name']}<span class='meta'>Golf courses: {c['golf_count']}</span></a>"
        )
    html.append("</div><div id='empty' class='empty' style='display:none'>No cities match your search.</div></main>")
    html.append("<footer style='text-align:center;padding:30px 20px;font-size:12px;color:#555'>Generated landing page. Serve: <code>python -m http.server 8000</code></footer>")
    html.append("<script>const s=document.getElementById('search');s.addEventListener('input',()=>{const q=s.value.toLowerCase();let vis=0;document.querySelectorAll('.city-btn').forEach(b=>{const n=b.getAttribute('data-name');const show=!q||n.includes(q);b.style.display=show?'flex':'none';if(show)vis++;});document.getElementById('empty').style.display=vis?'none':'block';});</script>")
    html.append("</body></html>")
    index_path.write_text('\n'.join(html), encoding='utf-8')
    print(f"Wrote landing page with {len(cities)} cities to {index_path}")

if __name__ == '__main__':
    main()
