import sys
from pathlib import Path
import pandas as pd
import numpy as np
from interactive_map_builder import load_city_boundary, load_tracts, clip_and_simplify
from spatial_lag_assets import load_golf_points, compute_exposure_features

def main():
    if len(sys.argv) < 3:
        print("Usage: python debug_exposure.py <City> <Province>")
        sys.exit(1)
    city = sys.argv[1]
    province = sys.argv[2]
    data_root = Path('data') / 'censusShape'

    print("Loading boundary + tracts…")
    boundary = load_city_boundary(city, province)
    tracts = load_tracts(str(data_root))
    clipped, simplified = clip_and_simplify(tracts, boundary, tolerance=40.0)
    tracts_3347 = clipped.copy()
    print(f"Tracts after clip: {len(tracts_3347)}")

    courses_csv = Path('data') / 'canada' / 'Fully_Matched_Golf_Courses.csv'
    golf_pts = load_golf_points(str(courses_csv))
    if golf_pts is None or golf_pts.empty:
        print("No golf points loaded.")
        sys.exit(0)
    print(f"Loaded golf points: {len(golf_pts)}")

    exposure_df = compute_exposure_features(tracts_3347, golf_pts)
    total = len(exposure_df)
    nan_count = int(exposure_df['dist_to_gc_km'].isna().sum())
    finite = int(exposure_df['dist_to_gc_km'].replace([np.inf, -np.inf], np.nan).dropna().shape[0])
    print(f"Exposure finite: {finite} / {total}")
    if nan_count:
        print(f"NaN exposure count: {nan_count}")
        merged = tracts_3347[['CTUID','geometry']].merge(exposure_df, on='CTUID', how='left')
        nan_rows = merged[merged['dist_to_gc_km'].isna()]
        for _, r in nan_rows.head(10).iterrows():
            geom = r['geometry']
            gt = getattr(geom, 'geom_type', type(geom).__name__)
            ie = getattr(geom, 'is_empty', True)
            print(f"  CTUID {r['CTUID']}: geom_type={gt}, is_empty={ie}")

if __name__ == '__main__':
    main()
