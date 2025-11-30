import argparse
import json
import math
import os
from pathlib import Path
import re

import geopandas as gpd
import pandas as pd
import osmnx as ox


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip('-')
    return text or "metric"


def load_city_boundary(city: str, province: str) -> gpd.GeoDataFrame:
    city = city.strip()
    province = (province or '').strip()
    if province:
        query = f"{city}, {province}, Canada"
    else:
        query = f"{city}, Canada"
    gdf = ox.geocode_to_gdf(query)
    if gdf is None or len(gdf) == 0:
        raise RuntimeError(f"City boundary not found for '{query}'.")
    return gdf.to_crs("EPSG:3347")


def load_tracts(shapefile_dir: str) -> gpd.GeoDataFrame:
    shp_files = [f for f in os.listdir(shapefile_dir) if f.lower().endswith('.shp')]
    if not shp_files:
        raise RuntimeError(f"No shapefile found in {shapefile_dir}.")
    shp_path = os.path.join(shapefile_dir, shp_files[0])
    gdf = gpd.read_file(shp_path).to_crs("EPSG:3347")
    if 'CTUID' not in gdf.columns:
        raise RuntimeError("Shapefile missing 'CTUID' column.")
    gdf['CTUID'] = gdf['CTUID'].astype(str)
    return gdf


def clip_and_simplify(tracts: gpd.GeoDataFrame, boundary: gpd.GeoDataFrame, tolerance: float = 40.0) -> gpd.GeoDataFrame:
    city_poly = boundary.iloc[0].geometry
    clipped = gpd.clip(tracts, city_poly)
    clipped['CTUID'] = clipped['CTUID'].astype(str)
    # Simplify geometry for size reduction (tolerance in meters since EPSG:3347)
    simplified = clipped.copy()
    simplified['geometry'] = simplified['geometry'].simplify(tolerance, preserve_topology=True)
    return simplified


def load_profile_cache(city_dir: Path) -> dict:
    # Accept either profile_cache.json or <city>_profile_cache.json
    primary = city_dir / "profile_cache.json"
    alt = next((p for p in city_dir.glob("*_profile_cache.json")), None)
    target = primary if primary.exists() else alt
    if not target or not target.exists():
        raise FileNotFoundError(f"Profile cache JSON not found in {city_dir}")
    with open(target, 'r', encoding='utf-8') as f:
        return json.load(f)


def summarize_metric(values: list) -> dict:
    clean = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not clean:
        return {"count": 0}
    series = pd.Series(clean)
    return {
        "count": int(series.count()),
        "min": float(series.min()),
        "max": float(series.max()),
        "mean": float(series.mean()),
        "p10": float(series.quantile(0.10)),
        "p25": float(series.quantile(0.25)),
        "p50": float(series.quantile(0.5)),
        "p75": float(series.quantile(0.75)),
        "p90": float(series.quantile(0.90))
    }


def build_assets(city: str, province: str, data_root: Path, out_root: Path, tolerance: float = 40.0):
    city_slug = city.replace(' ', '_').lower()
    city_dir = data_root / city_slug
    if not city_dir.exists():
        raise RuntimeError(f"City cache directory not found: {city_dir}. Build it first with census_cacher.py")

    print("Loading boundary + tracts…")
    boundary = load_city_boundary(city, province)
    tracts = load_tracts(str(data_root))
    simplified = clip_and_simplify(tracts, boundary, tolerance=tolerance)

    print("Loading profile cache…")
    profile = load_profile_cache(city_dir)

    # Normalization helper must match geometry CTUID transformation so joins succeed client-side
    def _normalize_ctuid(s: str) -> str:
        s = str(s)
        if '.' in s:
            left, right = s.split('.', 1)
            right = right.rstrip('0') or '0'
            return f"{left}.{right}"
        return s

    # Build category→metric→{normalized_CTUID:value}
    print("Reorganizing metrics (normalizing CTUID keys)…")
    categories = {}
    for raw_ctuid, cat_map in profile.items():
        norm_ctuid = _normalize_ctuid(raw_ctuid)
        for category, metrics in cat_map.items():
            cat_store = categories.setdefault(category, {})
            for metric, value in metrics.items():
                metric_store = cat_store.setdefault(metric, {})
                metric_store[norm_ctuid] = value

    # Write geometry GeoJSON (with only CTUID property)
    geo_out_dir = city_dir / "web_assets"
    metrics_dir = geo_out_dir / "metrics"
    geo_out_dir.mkdir(exist_ok=True)
    metrics_dir.mkdir(exist_ok=True)

    print("Writing tracts.geojson (reproject to EPSG:4326 for Leaflet)…")
    # Proper reprojection using epsg keyword
    simplified_wgs84 = simplified.to_crs(epsg=4326)
    # (Already defined above for metric key normalization)

    g_geom = simplified_wgs84[['CTUID', 'geometry']].copy()
    g_geom['CTUID'] = g_geom['CTUID'].apply(_normalize_ctuid)
    g_geom.to_file(geo_out_dir / 'tracts.geojson', driver='GeoJSON')
    # Bounding box diagnostic
    minx, miny, maxx, maxy = g_geom.total_bounds
    print(f"GeoJSON bounds lon/lat: ({minx:.4f}, {miny:.4f}) -> ({maxx:.4f}, {maxy:.4f})")
    if not (-180 <= minx <= 180 and -90 <= miny <= 90):
        print("WARNING: Reprojection may have failed; coordinates out of expected range.")

    # Build metrics index with summary stats
    print("Writing metrics_index.json…")
    index = {
        "city": city,
        "province": province,
        "categories": []
    }
    for category, metric_map in categories.items():
        cat_entry = {"category": category, "metrics": []}
        for metric, ct_values in metric_map.items():
            stats = summarize_metric(list(ct_values.values()))
            metric_slug = slugify(f"{category}-{metric}")[:80]
            cat_entry["metrics"].append({
                "name": metric,
                "slug": metric_slug,
                "stats": stats
            })
            # Write per-metric value file (sanitize NaN/inf -> null)
            sanitized = {}
            for k, v in ct_values.items():
                if isinstance(v, float):
                    if math.isnan(v) or math.isinf(v):
                        sanitized[k] = None
                    else:
                        sanitized[k] = v
                else:
                    sanitized[k] = v
            with open(metrics_dir / f"{metric_slug}.json", 'w', encoding='utf-8') as mf:
                json.dump(sanitized, mf, ensure_ascii=False)
        index["categories"].append(cat_entry)

    idx_path = geo_out_dir / 'metrics_index.json'
    if idx_path.exists():
        idx_path.unlink()
    with open(idx_path, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    with open(idx_path, 'r', encoding='utf-8') as f:
        content = f.read()
    first = next((c for c in content if not c.isspace()), '')
    if first != '{':
        with open(idx_path, 'w', encoding='utf-8') as f:
            json.dump(index, f, ensure_ascii=False, indent=2)
        print('Rewrote metrics_index.json due to leading corruption.')

    # Build golf courses layer (optional)
    try:
        print("Building golf courses GeoJSON…")
        courses_csv = Path('data') / 'canada' / 'Fully_Matched_Golf_Courses.csv'
        if courses_csv.exists():
            df = pd.read_csv(courses_csv)
            # Drop invalid coords
            df = df.dropna(subset=['latitude','longitude'])
            # Build GeoDataFrame in WGS84
            gpoints = gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(df['longitude'].astype(float), df['latitude'].astype(float)),
                crs='EPSG:4326'
            )
            # Reproject to boundary CRS for precise clip
            gpoints_proj = gpoints.to_crs('EPSG:3347')
            city_poly = boundary.iloc[0].geometry
            inside = gpd.clip(gpoints_proj, city_poly)
            # Back to WGS84 for Leaflet
            inside_wgs = inside.to_crs('EPSG:4326').copy()
            # Keep only useful columns
            keep_cols = ['CourseName','Address','City','AccessType','NumHoles','Par','url','website']
            for c in keep_cols:
                if c not in inside_wgs.columns:
                    inside_wgs[c] = None
            inside_wgs = inside_wgs[keep_cols + ['geometry']]
            # Rename for nicer popups
            inside_wgs = inside_wgs.rename(columns={
                'CourseName':'name', 'Address':'address', 'City':'city', 'AccessType':'access',
                'NumHoles':'holes', 'Par':'par', 'url':'url', 'website':'website'
            })
            inside_wgs.to_file(geo_out_dir / 'golf_courses.geojson', driver='GeoJSON')
            print(f"Wrote {len(inside_wgs)} golf course points.")
        else:
            print(f"Golf courses CSV not found at {courses_csv}; skipping.")
    except Exception as e:
        print(f"Failed to build golf courses layer: {e}")

    # Build HTML template
    print("Writing interactive map HTML…")
    maps_dir = Path(__file__).parent / 'maps'
    maps_dir.mkdir(exist_ok=True)
    html_path = maps_dir / f"{city_slug}_interactive_map.html"

    html_template = """
<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='utf-8' />
    <title>{CITY_NAME} Census Explorer</title>
    <meta name='viewport' content='width=device-width,initial-scale=1' />
    <link rel='stylesheet' href='https://unpkg.com/leaflet@1.9.4/dist/leaflet.css' />
    <style>
        body,html { margin:0; padding:0; height:100%; font-family:system-ui,Arial,sans-serif; }
        #app { display:flex; height:100%; }
        #sidebar { width:340px; overflow:auto; border-right:1px solid #ccc; padding:12px; box-sizing:border-box; background:#fafafa; }
        #map { flex:1; }
        .category { margin-bottom:10px; }
        .category h3 { margin:4px 0; font-size:14px; cursor:pointer; }
        .metrics { display:none; margin-left:8px; }
        .metric-btn { display:block; text-align:left; width:100%; border:0; background:#fff; padding:4px 6px; margin:2px 0; cursor:pointer; font-size:12px; border-radius:4px;}
        .metric-btn:hover { background:#e3f2fd; }
        #search { width:100%; padding:6px; margin-bottom:8px; }
        #legend { padding:6px; background:white; border:1px solid #999; font-size:12px; line-height:1.2; }
        #detail { padding:6px; font-size:12px; border-top:1px solid #ddd; }
        .hist-bar { height:8px; background:#90caf9; display:inline-block; margin-right:1px; }
    </style>
</head>
<body>
<div id='app'>
    <div id='sidebar'>
        <input id='search' type='text' placeholder='Search metrics…' />
        <div id='currentMetricDisplay' style='font-size:12px;margin:4px 0 8px 0;color:#333'><em>No metric selected</em></div>
        <div id='categories'></div>
        <div id='detail'><em>Click a tract for details…</em></div>
    </div>
    <div id='map'></div>
</div>
<script src='https://unpkg.com/leaflet@1.9.4/dist/leaflet.js'></script>
<script>
const ASSET_ROOT = '../data/censusShape/{CITY_SLUG}/web_assets';
let tractLayer = null;
let currentMetric = null; // slug
let metricsCache = new Map(); // slug -> { ctuid: value }
let indexData = null; // metrics index
let metricMeta = new Map(); // slug -> {name, stats}
let coursesLayer = null;
const map = L.map('map').setView([49.25, -123.1], 11);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19 }).addTo(map);
async function fetchJSON(path) {
    try {
        const r = await fetch(path);
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const txt = await r.text();
        const first = txt.trim().charAt(0);
        if (first !== '{' && first !== '[') throw new Error('Invalid JSON start');
        return JSON.parse(txt);
    } catch (e) {
        console.error('Fetch failed', path, e);
        const warn = document.getElementById('detail');
        if (warn) warn.innerHTML = '<span style="color:#b00">Failed to load ' + path + ': ' + e.message + '. If opened with file://, run a local server (python -m http.server).</span>';
        throw e;
    }
}
function clamp01(x){ return x<0?0:(x>1?1:x); }
// Single-hue scale: blue with varying lightness (light for low, dark for high)
function colorScale(val, min, max) {
    if (val==null || isNaN(val)) return '#eee';
    const t = clamp01((val - min) / (max - min + 1e-9));
    const light = 92 - 55*t; // 92% -> 37%
    return 'hsl(210,70%,' + light.toFixed(1) + '%)';
}
function buildRamp(min, max) {
    const steps = 6;
    let html = '<div style="margin-top:4px;display:flex;gap:2px;align-items:center">';
    for (let i=0;i<steps;i++){
        const t = i/(steps-1);
        const v = min + t*(max-min);
        html += '<span style="display:inline-block;width:20px;height:10px;background:'+colorScale(v,min,max)+'"></span>';
    }
    html += '</div>';
    return html;
}
function applyMetric(metricSlug, stats) {
    currentMetric = metricSlug;
    const values = metricsCache.get(metricSlug);
    // Determine scale mode: min-max vs quantile fallback when spread is tight or flat
    let min = stats.min, max = stats.max;
    let useQuantiles = false;
    const p10 = (stats.p10 ?? min);
    const p90 = (stats.p90 ?? max);
    if (!isFinite(min) || !isFinite(max) || min === max) {
        useQuantiles = true;
    } else if (isFinite(p10) && isFinite(p90)) {
        useQuantiles = (p90 - p10) < (max - min) * 0.2; // tight middle spread -> quantiles for contrast
    }
    // Precompute quantile edges if needed
    let qEdges = null;
    if (useQuantiles) {
        const arr = Object.values(values).map(v => Number(v)).filter(v => isFinite(v)).sort((a,b)=>a-b);
        if (arr.length >= 5) {
            const idx = q => arr[Math.max(0, Math.min(arr.length-1, Math.floor(arr.length*q)))];
            qEdges = [idx(0.05), idx(0.25), idx(0.5), idx(0.75), idx(0.95)];
        } else {
            // Fallback to min-max if too few values
            useQuantiles = false;
        }
    }
    const getColor = (v) => {
        if (v==null || !isFinite(v)) return '#eee';
        if (useQuantiles) {
            const val = Number(v);
            let t = 0.1;
            if (val <= qEdges[0]) t = 0.1;
            else if (val <= qEdges[1]) t = 0.3;
            else if (val <= qEdges[2]) t = 0.5;
            else if (val <= qEdges[3]) t = 0.7;
            else t = 0.9;
            const light = 92 - 55*t;
            return 'hsl(210,70%,' + light.toFixed(1) + '%)';
        }
        return colorScale(Number(v), min, max);
    };
    tractLayer.eachLayer(l => { const ct = l.feature.properties.CTUID; const v = values[ct]; l.setStyle({ fillColor: getColor(v), fillOpacity:0.75, weight:0.2, color:'#333' }); });
    const legend = document.getElementById('legend');
    if (legend) legend.remove();
    const lg = L.control({position:'bottomright'});
    const meta = metricMeta.get(metricSlug) || {name: metricSlug};
    const disp = document.getElementById('currentMetricDisplay');
    if (disp) disp.textContent = meta.name + ' (min ' + stats.min.toFixed(2) + ', max ' + stats.max.toFixed(2) + ')';
    lg.onAdd = () => { const div = L.DomUtil.create('div',''); div.id='legend'; if (useQuantiles && qEdges) { div.innerHTML = '<strong>' + meta.name + '</strong><br>Quantile bins (approx):<br>' + qEdges.map((q,i)=> (i? '&nbsp;':'') + (i<qEdges.length-1? q.toFixed(2) : ('≥ ' + q.toFixed(2)))).join('') + buildRamp(qEdges[0], qEdges[qEdges.length-1]); } else { div.innerHTML = '<strong>' + meta.name + '</strong><br>Min: ' + stats.min.toFixed(2) + '<br>Max: ' + stats.max.toFixed(2) + buildRamp(stats.min, stats.max); } return div; };
    lg.addTo(map);
}
function loadMetric(metricSlug, stats) {
    if (!tractLayer) { console.warn('Geometry layer not ready yet.'); return; }
    if (metricsCache.has(metricSlug)) { applyMetric(metricSlug, stats); return; }
    fetchJSON(ASSET_ROOT + '/metrics/' + metricSlug + '.json').then(data => { metricsCache.set(metricSlug, data); applyMetric(metricSlug, stats); });
}
function buildUI() { const catContainer = document.getElementById('categories'); catContainer.innerHTML=''; indexData.categories.forEach(cat => { const wrap=document.createElement('div'); wrap.className='category'; const h=document.createElement('h3'); h.textContent=cat.category; h.onclick=()=>{ mDiv.style.display = mDiv.style.display==='none'?'block':'none'; }; const mDiv=document.createElement('div'); mDiv.className='metrics'; cat.metrics.forEach(m => { metricMeta.set(m.slug, {name:m.name, stats:m.stats}); const btn=document.createElement('button'); btn.className='metric-btn'; btn.textContent=m.name; btn.onclick=()=>loadMetric(m.slug, m.stats); mDiv.appendChild(btn); }); wrap.appendChild(h); wrap.appendChild(mDiv); catContainer.appendChild(wrap); }); }
function filterMetrics(q) { q=q.toLowerCase(); document.querySelectorAll('.metric-btn').forEach(btn=>{ btn.style.display = btn.textContent.toLowerCase().includes(q)?'block':'none'; }); }
document.getElementById('search').addEventListener('input', e => filterMetrics(e.target.value));
fetchJSON(ASSET_ROOT + '/metrics_index.json').then(idx => { 
    indexData = idx; 
    buildUI(); 
    // Auto-load first metric if available
    try {
        const firstCat = indexData.categories[0];
        const firstMetric = firstCat && firstCat.metrics[0];
        if (firstMetric) loadMetric(firstMetric.slug, firstMetric.stats);
    } catch(e) { console.warn('Auto-load metric failed', e); }
}).catch(()=>{});
fetchJSON(ASSET_ROOT + '/tracts.geojson').then(gj => {
    if (!gj.features || !gj.features.length) {
        const warn = document.getElementById('detail');
        if (warn) warn.innerHTML = '<span style="color:#b00">No tract features loaded. Check GeoJSON generation.</span>';
        console.error('Empty GeoJSON feature collection');
        return;
    }
    tractLayer = L.geoJSON(gj, { 
        style:()=>({fillColor:'#ccc',weight:0.4,color:'#555',fillOpacity:0.5}), 
        onEachFeature:(f,l)=>{ 
            l.on('click',()=>showDetail(f));
            l.on('mouseover', (e)=> showHover(f, l));
            l.on('mouseout', ()=> { if (l.closeTooltip) l.closeTooltip(); });
        } 
    }).addTo(map);
    try { tractLayer.bringToBack(); } catch(_) {}
    map.fitBounds(tractLayer.getBounds());
    console.log('Loaded tracts:', gj.features.length);
}).catch(e => {
    console.error('Failed to load tracts.geojson', e);
    const warn = document.getElementById('detail');
    if (warn) warn.innerHTML = '<span style="color:#b00">Failed to load tract geometry. Serve over HTTP and ensure path is correct.</span>';
});

// Load golf courses overlay (if present)
fetchJSON(ASSET_ROOT + '/golf_courses.geojson').then(gj => {
    coursesLayer = L.geoJSON(gj, {
        pointToLayer: (feature, latlng) => L.circleMarker(latlng, {radius:5, color:'#b71c1c', weight:1, fillColor:'#e53935', fillOpacity:0.95}),
        onEachFeature: (f,l) => {
            const p = f.properties || {};
            const title = p.name || 'Golf Course';
            const addr = p.address ? ('<div>'+p.address+(p.city?(', '+p.city):'')+'</div>') : '';
            const meta = [p.access, p.holes?('Holes: '+p.holes):null, p.par?('Par: '+p.par):null].filter(Boolean).join(' · ');
            const links = (p.website||p.url)?('<div style="margin-top:4px">'+(p.website?('<a href="'+p.website+'" target="_blank">Website</a>'):'') + (p.website&&p.url?' | ':'') + (p.url?('<a href="'+p.url+'" target="_blank">Link</a>'):'') + '</div>') : '';
            l.bindPopup('<b>'+title+'</b><br>'+addr+(meta?('<small>'+meta+'</small>'):'')+links);
        }
    }).addTo(map);
    try { coursesLayer.bringToFront(); } catch(_) {}
    // Optional: add a simple control to toggle
    try { L.control.layers({}, {'Golf Courses': coursesLayer}, {collapsed:true}).addTo(map); } catch(_){}
}).catch(()=>{ /* optional layer, ignore errors */ });
function formatNum(x){ if (x==null || isNaN(x)) return '—'; const n = Number(x); return Math.abs(n) >= 1000 ? n.toLocaleString() : n.toString(); }
function showDetail(feature) {
    const ct=feature.properties.CTUID; const panel=document.getElementById('detail'); if (!indexData) { panel.innerHTML='Loading categories…'; return; }
    if (!currentMetric) { panel.innerHTML = '<strong>Tract: ' + ct + '</strong><br><em>Select a metric to see its value.</em>'; return; }
    const meta = metricMeta.get(currentMetric) || {name: currentMetric};
    const vals = metricsCache.get(currentMetric) || {};
    const v = vals[ct];
    panel.innerHTML = '<strong>Tract: ' + ct + '</strong><br><div><b>'+ meta.name + ':</b> ' + formatNum(v) + '</div>';
}

function showHover(feature, layer) {
    const ct = feature.properties.CTUID;
    let content = 'Tract: ' + ct;
    if (currentMetric) {
        const meta = metricMeta.get(currentMetric) || {name: currentMetric};
        const vals = metricsCache.get(currentMetric) || {};
        const v = vals[ct];
        content = '<div><b>' + meta.name + '</b><br>' + formatNum(v) + '</div>';
    } else {
        content = '<em>Select a metric…</em>';
    }
    if (layer && layer.bindTooltip) {
        layer.bindTooltip(content, {sticky:true}).openTooltip();
    }
}
</script>
</body>
</html>
    """
    html = html_template.replace('{CITY_NAME}', city).replace('{CITY_SLUG}', city_slug)
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Done. Open {html_path} in a browser to explore.")


def main():
    parser = argparse.ArgumentParser(description="Build interactive census map assets (dynamic Leaflet)")
    parser.add_argument('city', help='City name, e.g., Vancouver')
    parser.add_argument('province', help='Province name, e.g., British Columbia')
    parser.add_argument('--tolerance', type=float, default=40.0, help='Geometry simplify tolerance (meters)')
    args = parser.parse_args()

    data_root = Path('data') / 'censusShape'
    build_assets(args.city, args.province, data_root=data_root, out_root=Path('maps'), tolerance=args.tolerance)


if __name__ == '__main__':
    main()
