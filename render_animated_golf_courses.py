import argparse
from pathlib import Path
from typing import List, Tuple
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, Point, box, MultiPolygon
from shapely.ops import unary_union
from PIL import Image, ImageDraw, ImageFont
import cairo

# Import shared functions from the main script
from render_golf_courses_stylized import (
    load_points, north_america_polygon, lonlat_to_canvas,
    draw_paper_background, draw_fairway_stripes, path_from_polygon,
    draw_coastlines_and_borders, create_base_map, draw_flag
)

def load_points_with_years(csv_paths: List[Path]) -> pd.DataFrame:
    """Load golf courses with establishment year, filtering out those without years."""
    frames = []
    for p in csv_paths:
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        cols = {c.lower(): c for c in df.columns}
        lat = cols.get('latitude') or cols.get('lat')
        lon = cols.get('longitude') or cols.get('lon') or cols.get('lng')
        year = cols.get('established') or cols.get('year')
        
        if not lat or not lon or not year:
            continue
        
        sub = df[[lat, lon, year]].rename(columns={
            lat: 'latitude',
            lon: 'longitude',
            year: 'established'
        }).dropna()
        
        sub['latitude'] = sub['latitude'].astype(float)
        sub['longitude'] = sub['longitude'].astype(float)
        
        # Try to convert to year (int)
        try:
            sub['established'] = pd.to_numeric(sub['established'], errors='coerce').astype('Int64')
            sub = sub.dropna(subset=['established'])
        except Exception:
            continue
        
        frames.append(sub)
    
    if not frames:
        return pd.DataFrame(columns=['latitude', 'longitude', 'established'])
    
    pts = pd.concat(frames, ignore_index=True)
    
    # Deduplicate
    pts['lat_r'] = pts['latitude'].round(6)
    pts['lon_r'] = pts['longitude'].round(6)
    pts = pts.drop_duplicates(subset=['lat_r', 'lon_r']).drop(columns=['lat_r', 'lon_r'])
    
    # Filter to reasonable year range
    pts = pts[(pts['established'] >= 1800) & (pts['established'] <= 2024)]
    
    return pts

def render_frame(surface, ctx, base_surface, pts_for_year, extent, width, height, year, flag_scale=1.4):
    """
    Render a single frame with flags for courses established up to year.
    """
    # Copy base map to surface
    buf = base_surface.get_data()
    base_img = Image.frombuffer("RGBA", (width, height), buf, "raw", "BGRA", 0, 1)
    
    # Copy base to surface
    surface_data = surface.get_data()
    surface_data[:] = base_img.tobytes("raw", "BGRA")
    
    # Draw flags for this year
    np.random.seed(7)
    pts_sorted = pts_for_year.sort_values('latitude', ascending=False).reset_index(drop=True)
    for _, r in pts_sorted.iterrows():
        lon = float(r['longitude'])
        lat = float(r['latitude'])
        x, y = lonlat_to_canvas(lon, lat, extent, width, height)
        draw_flag(ctx, x, y, scale=flag_scale)
    
    return surface

def draw_year_label(pil_img, year):
    """Add a year label at the top of the image using PIL."""
    draw = ImageDraw.Draw(pil_img)
    
    # Try to use a larger font, fall back to default
    try:
        font = ImageFont.truetype("arial.ttf", 60)
    except:
        font = ImageFont.load_default()
    
    # Draw year text at top-right
    year_text = str(int(year))
    text_bbox = draw.textbbox((0, 0), year_text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]
    
    # Position at top-right with padding
    x = 30
    y = 30
    
    # Draw semi-transparent background
    padding = 10
    bg_bbox = [x - padding, y - padding, x + text_width + padding, y + text_height + padding]
    draw.rectangle(bg_bbox, fill=(255, 255, 255, 200))
    
    # Draw text
    draw.text((x, y), year_text, fill=(0, 0, 0), font=font)
    
    return pil_img

def main():
    parser = argparse.ArgumentParser(description="Animated rendering of golf courses across North America over time.")
    parser.add_argument("--inputs", nargs="*", default=[], help="Input CSVs. Defaults to Canada+USA+Mexico if present.")
    parser.add_argument("--output", default="images/golf_courses_animated.gif", help="Output GIF path")
    parser.add_argument("--width", type=int, default=1200, help="Canvas width in pixels")
    parser.add_argument("--height", type=int, default=1600, help="Canvas height in pixels")
    parser.add_argument("--flag-scale", type=float, default=1.4, help="Scale for flag size")
    parser.add_argument("--fps", type=int, default=2, help="Frames per second for animation")
    parser.add_argument("--year-step", type=int, default=5, help="Years between frames")
    parser.add_argument("--no-cache", action="store_true", help="Regenerate base map")
    args = parser.parse_args()
    
    defaults = [
        Path("data") / "canada" / "Fully_Matched_Golf_Courses.csv",
        Path("data") / "usa" / "Fully_Matched_Golf_Courses.csv",
        Path("data") / "mexico" / "Fully_Matched_Golf_Courses.csv",
    ]
    csvs = [Path(p) for p in args.inputs] or [p for p in defaults if p.exists()]
    
    print("Loading golf courses with establishment years...")
    pts = load_points_with_years(csvs)
    
    if len(pts) == 0:
        print("ERROR: No golf courses with establishment years found!")
        return
    
    print(f"Loaded {len(pts)} golf courses with years")
    print(f"Year range: {pts['established'].min():.0f} to {pts['established'].max():.0f}")
    
    # Load base map data
    na = north_america_polygon()
    na_geom = na.iloc[0]
    extent = (-160.0, -60.0, 5.0, 80.0)
    na_poly = na_geom
    
    # Output paths
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Base map cache
    cache_dir = out_path.parent / ".cache"
    cache_path = cache_dir / f"base_map_{args.width}x{args.height}.png"
    
    # Load or create base map
    if cache_path.exists() and not args.no_cache:
        print(f"Loading cached base map from {cache_path}")
        base_img = Image.open(cache_path).convert("RGBA")
        base_surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, args.width, args.height)
        base_data = base_img.tobytes("raw", "BGRA")
        surface_data = base_surface.get_data()
        surface_data[:] = base_data
    else:
        print("Generating base map...")
        base_surface = create_base_map(args.width, args.height, na_poly, extent)
        cache_dir.mkdir(parents=True, exist_ok=True)
        base_surface.write_to_png(str(cache_path))
        print(f"Cached base map to {cache_path}")
    
    # Generate frames
    min_year = int(pts['established'].min())
    max_year = int(pts['established'].max())
    years = list(range(min_year, max_year + 1, args.year_step))
    
    print(f"Generating {len(years)} frames from {min_year} to {max_year}...")
    frames = []
    
    for i, year in enumerate(years):
        print(f"  Frame {i+1}/{len(years)}: Year {year}")
        
        # Filter courses established by this year
        pts_for_year = pts[pts['established'] <= year]
        
        # Create surface for this frame
        surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, args.width, args.height)
        ctx = cairo.Context(surface)
        
        # Render frame with flags
        render_frame(surface, ctx, base_surface, pts_for_year, extent, args.width, args.height, year, args.flag_scale)
        
        # Convert to PIL image
        buf = surface.get_data()
        frame_img = Image.frombuffer("RGBA", (args.width, args.height), buf, "raw", "BGRA", 0, 1)
        
        # Add year label
        frame_img = draw_year_label(frame_img, year)
        
        # Convert RGBA to RGB for GIF
        frame_img_rgb = Image.new("RGB", frame_img.size, (255, 255, 255))
        frame_img_rgb.paste(frame_img, mask=frame_img.split()[3])
        
        frames.append(frame_img_rgb)
    
    # Save as animated GIF
    duration = int(1000 / args.fps)  # Duration in milliseconds
    frames[0].save(
        str(out_path),
        save_all=True,
        append_images=frames[1:],
        duration=duration,
        loop=0
    )
    
    print(f"Saved animated GIF to {out_path}")

if __name__ == "__main__":
    main()
