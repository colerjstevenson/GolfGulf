import argparse
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, Point, box
from shapely.ops import unary_union
from PIL import Image

# PyCairo for stylized vector drawing
import cairo

# ---- Data helpers ----

def load_points(csv_paths: List[Path]) -> pd.DataFrame:
    frames = []
    for p in csv_paths:
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        cols = {c.lower(): c for c in df.columns}
        lat = cols.get('latitude') or cols.get('lat')
        lon = cols.get('longitude') or cols.get('lon') or cols.get('lng')
        if not lat or not lon:
            continue
        sub = df[[lat, lon]].rename(columns={lat: 'latitude', lon: 'longitude'}).dropna()
        sub['latitude'] = sub['latitude'].astype(float)
        sub['longitude'] = sub['longitude'].astype(float)
        frames.append(sub)
    if not frames:
        return pd.DataFrame(columns=['latitude','longitude'])
    pts = pd.concat(frames, ignore_index=True)
    # Deduplicate
    pts['lat_r'] = pts['latitude'].round(6)
    pts['lon_r'] = pts['longitude'].round(6)
    pts = pts.drop_duplicates(subset=['lat_r','lon_r']).drop(columns=['lat_r','lon_r'])
    return pts

# Build NA polygon

def north_america_polygon() -> gpd.GeoSeries:
    """
    Returns a GeoSeries with North America continent polygon.
    Uses Cartopy's Natural Earth country data.
    """
    try:
        import cartopy.io.shapereader as shpreader
        
        # Load Natural Earth countries
        countries_shp = shpreader.natural_earth(resolution='110m', category='cultural', name='admin_0_countries')
        
        # North America countries to include
        na_countries = ['United States of America', 'Canada', 'Mexico', 
                       'Guatemala', 'Belize', 'Honduras', 'El Salvador', 
                       'Nicaragua', 'Costa Rica', 'Panama']
        
        # Read country geometries
        na_geoms = []
        for record in shpreader.Reader(countries_shp).records():
            name = record.attributes.get('NAME', '') or record.attributes.get('ADMIN', '')
            if name in na_countries:
                na_geoms.append(record.geometry)
        
        if len(na_geoms) > 0:
            geom = unary_union(na_geoms)
            return gpd.GeoSeries([geom], crs="EPSG:4326")
    except Exception as e:
        print(f"Warning: Could not load Natural Earth data ({e}), using fallback rectangle")
    
    # Fallback: rectangle
    poly = Polygon([(-160,5), (-60,5), (-60,80), (-160,80)])
    return gpd.GeoSeries([poly], crs="EPSG:4326")

# ---- Drawing helpers ----

def lonlat_to_canvas(lon: float, lat: float, extent: Tuple[float,float,float,float], width: int, height: int) -> Tuple[float,float]:
    west, east, south, north = extent
    x = (lon - west) / (east - west) * width
    y = (1.0 - (lat - south) / (north - south)) * height
    return x, y

def draw_paper_background(ctx: cairo.Context, width: int, height: int):
    # Base warm paper
    ctx.rectangle(0, 0, width, height)
    ctx.set_source_rgb(0.93, 0.89, 0.78)
    ctx.fill()
    # Grain via noise-like strokes
    np.random.seed(42)
    for _ in range(400):
        x = np.random.uniform(0, width)
        y = np.random.uniform(0, height)
        l = np.random.uniform(8, 18)
        ctx.set_source_rgba(0.90, 0.86, 0.76, 0.05)
        ctx.set_line_width(np.random.uniform(0.4, 0.9))
        ctx.move_to(x, y)
        ctx.line_to(x + l*np.cos(np.random.uniform(0, np.pi*2)), y + l*np.sin(np.random.uniform(0, np.pi*2)))
        ctx.stroke()
    # Slight vignette
    pat = cairo.RadialGradient(width/2, height/2, height*0.1, width/2, height/2, height*0.7)
    pat.add_color_stop_rgba(0.0, 1,1,1,0)
    pat.add_color_stop_rgba(1.0, 0.6,0.55,0.45, 0.15)
    ctx.set_source(pat)
    ctx.rectangle(0,0,width,height)
    ctx.fill()

def path_from_polygon(ctx: cairo.Context, geom, extent: Tuple[float,float,float,float], width: int, height: int):
    """Draw a Polygon or MultiPolygon geometry as a Cairo path."""
    from shapely.geometry import MultiPolygon, Polygon
    
    if isinstance(geom, MultiPolygon):
        # Draw each polygon in the multipolygon
        for poly in geom.geoms:
            path_from_polygon(ctx, poly, extent, width, height)
    elif isinstance(geom, Polygon):
        # Exterior ring
        ext = list(geom.exterior.coords)
        for i,(lon,lat) in enumerate(ext):
            x,y = lonlat_to_canvas(lon,lat,extent,width,height)
            if i==0: ctx.move_to(x,y)
            else: ctx.line_to(x,y)
        ctx.close_path()
        # Holes
        for interior in geom.interiors:
            ring = list(interior.coords)
            if not ring: continue
            for i,(lon,lat) in enumerate(ring):
                x,y = lonlat_to_canvas(lon,lat,extent,width,height)
                if i==0: ctx.move_to(x,y)
                else: ctx.line_to(x,y)
            ctx.close_path()

def draw_fairway_stripes(width: int, height: int, na_poly, extent: Tuple[float,float,float,float]) -> Image.Image:
    """
    Create fairway stripes as a PIL image with transparency outside the NA polygon.
    This is more reliable than Cairo clipping.
    na_poly can be a Polygon or MultiPolygon.
    """
    # Create a Cairo surface for the stripes
    stripe_surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    stripe_ctx = cairo.Context(stripe_surface)
    
    # Fill with base green
    stripe_ctx.set_source_rgb(0.55, 0.78, 0.56)
    stripe_ctx.paint()
    
    # Draw diagonal stripes
    stripe_color1 = (0.52, 0.75, 0.53)
    stripe_color2 = (0.60, 0.82, 0.60)
    stripe_w = 22
    
    stripe_ctx.save()
    stripe_ctx.rotate(-0.35)
    for i in range(-height, height*2, stripe_w):
        stripe_ctx.rectangle(i, -width, stripe_w//2, width*3)
        stripe_ctx.set_source_rgb(*stripe_color1)
        stripe_ctx.fill()
        stripe_ctx.rectangle(i+stripe_w//2, -width, stripe_w//2, width*3)
        stripe_ctx.set_source_rgb(*stripe_color2)
        stripe_ctx.fill()
    stripe_ctx.restore()
    
    # Convert Cairo surface to PIL Image
    buf = stripe_surface.get_data()
    stripe_img = Image.frombuffer("RGBA", (width, height), buf, "raw", "BGRA", 0, 1)
    
    # Create alpha mask from polygon - only mark pixels inside NA polygon as opaque
    mask = Image.new('L', (width, height), 0)
    mask_pixels = mask.load()
    
    print("Creating polygon mask (this may take a moment)...")
    # For each pixel, check if it's inside the NA polygon
    for y in range(height):
        if y % 100 == 0:
            print(f"  Processing row {y}/{height}...")
        for x in range(width):
            # Convert pixel to lon/lat
            lon = extent[0] + (x / width) * (extent[1] - extent[0])
            lat = extent[3] - (y / height) * (extent[3] - extent[2])
            pt = Point(lon, lat)
            if na_poly.contains(pt):
                mask_pixels[x, y] = 255
    
    # Apply mask to stripe image
    stripe_img.putalpha(mask)
    
    return stripe_img

def draw_coastlines_and_borders(ctx: cairo.Context, extent: Tuple[float,float,float,float], width: int, height: int):
    # Load Natural Earth coastlines and borders via Cartopy if available
    try:
        import cartopy.feature as cfeature
        coastlines = cfeature.COASTLINE.with_scale('110m')
        borders = cfeature.BORDERS.with_scale('110m')
        # Extract geometries
        coast_geoms = list(coastlines.geometries())
        border_geoms = list(borders.geometries())
        # Draw coastlines
        ctx.set_source_rgb(0.11, 0.37, 0.13)
        ctx.set_line_width(1.2)
        for geom in coast_geoms:
            if hasattr(geom, 'geoms'):
                for g in geom.geoms:
                    draw_linestring(ctx, g, extent, width, height)
            else:
                draw_linestring(ctx, geom, extent, width, height)
        # Draw borders
        ctx.set_source_rgba(0.15, 0.60, 0.16, 0.8)
        ctx.set_line_width(0.8)
        for geom in border_geoms:
            if hasattr(geom, 'geoms'):
                for g in geom.geoms:
                    draw_linestring(ctx, g, extent, width, height)
            else:
                draw_linestring(ctx, geom, extent, width, height)
    except Exception:
        # Fallback: draw NA polygon outline only
        pass

def draw_linestring(ctx: cairo.Context, geom, extent: Tuple[float,float,float,float], width: int, height: int):
    try:
        coords = list(geom.coords)
        for i, (lon, lat) in enumerate(coords):
            x, y = lonlat_to_canvas(lon, lat, extent, width, height)
            if i == 0:
                ctx.move_to(x, y)
            else:
                ctx.line_to(x, y)
        ctx.stroke()
    except Exception:
        pass

def draw_flag(ctx: cairo.Context, x: float, y: float, scale: float = 1.0):
    # Slight jitter for hand-drawn feel
    jx = np.random.uniform(-1.5, 1.5)
    jy = np.random.uniform(-1.0, 1.0)
    ctx.save()
    ctx.translate(x + jx, y + jy)
    ctx.scale(scale, scale)
    # Pole
    ctx.move_to(0, 0)
    ctx.line_to(0, -16)
    ctx.set_source_rgb(0.36, 0.25, 0.19)
    ctx.set_line_width(2.0)
    ctx.stroke()
    # Flag pennant (larger)
    ctx.move_to(0, -13)
    ctx.line_to(5, -16)
    ctx.line_to(0, -16)
    ctx.close_path()
    ctx.set_source_rgb(0.83, 0.20, 0.20)
    ctx.fill_preserve()
    ctx.set_line_width(1.2)
    ctx.stroke()
    # Cup shadow
    ctx.arc(0, 2, 2.8, 0, 2*np.pi)
    ctx.set_source_rgba(0,0,0,0.08)
    ctx.fill()
    ctx.restore()

# ---- Main ----

def main():
    parser = argparse.ArgumentParser(description="Stylized rendering of golf courses across North America (paper + fairway + flags).")
    parser.add_argument("--inputs", nargs="*", default=[], help="Input CSVs (lat/lon). Defaults to Canada+USA+Mexico if present.")
    parser.add_argument("--output", default="images/golf_courses_north_america_stylized.png", help="Output PNG path")
    parser.add_argument("--width", type=int, default=1200, help="Canvas width in pixels")
    parser.add_argument("--height", type=int, default=1600, help="Canvas height in pixels")
    parser.add_argument("--flag-scale", type=float, default=1.4, help="Scale for flag size")
    args = parser.parse_args()

    defaults = [
        Path("data") / "canada" / "Fully_Matched_Golf_Courses.csv",
        Path("data") / "usa" / "Fully_Matched_Golf_Courses.csv",
        Path("data") / "mexico" / "Fully_Matched_Golf_Courses.csv",
    ]
    csvs = [Path(p) for p in args.inputs] or [p for p in defaults if p.exists()]
    pts = load_points(csvs)

    na = north_america_polygon()
    na_geom = na.iloc[0]
    # Drawing extent
    extent = (-160.0, -60.0, 5.0, 80.0)
    
    # Use the full geometry (handles both Polygon and MultiPolygon)
    na_poly = na_geom

    # Cairo surface for paper background
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, args.width, args.height)
    ctx = cairo.Context(surface)

    # Layer 1: Paper background
    draw_paper_background(ctx, args.width, args.height)
    
    # Layer 2: Create fairway stripes with PIL-based masking
    fairway_img = draw_fairway_stripes(args.width, args.height, na_poly, extent)
    
    # Convert Cairo surface to PIL to composite the fairway
    buf = surface.get_data()
    paper_img = Image.frombuffer("RGBA", (args.width, args.height), buf, "raw", "BGRA", 0, 1)
    
    # Composite fairway onto paper
    paper_img = Image.alpha_composite(paper_img, fairway_img)
    
    # Convert back to Cairo surface for remaining layers
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, args.width, args.height)
    ctx = cairo.Context(surface)
    
    # Copy PIL image data to Cairo surface
    paper_data = paper_img.tobytes("raw", "BGRA")
    surface_data = surface.get_data()
    surface_data[:] = paper_data

    # Layer 3: Draw coastlines and borders on top of fairway
    draw_coastlines_and_borders(ctx, extent, args.width, args.height)
    
    # Layer 4: Draw NA polygon outline on top
    ctx.save()
    path_from_polygon(ctx, na_poly, extent, args.width, args.height)
    ctx.set_source_rgb(0.11, 0.37, 0.13)
    ctx.set_line_width(2.5)
    ctx.stroke()
    ctx.restore()

    # Layer 5: Draw flags
    np.random.seed(7)
    for _, r in pts.iterrows():
        lon = float(r['longitude']); lat = float(r['latitude'])
        x,y = lonlat_to_canvas(lon,lat,extent,args.width,args.height)
        draw_flag(ctx, x, y, scale=args.flag_scale)

    # Save PNG
    surface.write_to_png(str(out_path))
    print(f"Saved stylized PNG to {out_path}")

if __name__ == "__main__":
    main()
