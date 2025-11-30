import os
from pathlib import Path
import json
import pandas as pd
import geopandas as gpd
import osmnx as ox


def load_city_boundary(city: str, province: str) -> gpd.GeoDataFrame:
    query = f"{city}, {province}, Canada"
    gdf = ox.geocode_to_gdf(query)
    if gdf is None or len(gdf) == 0:
        raise RuntimeError(f"City boundary not found for '{query}'.")
    return gdf.to_crs("EPSG:3347")


def load_tracts(shapefile_dir: str) -> gpd.GeoDataFrame:
    shp_files = [f for f in os.listdir(shapefile_dir) if f.lower().endswith('.shp')]
    if not shp_files:
        raise RuntimeError(f"No shapefile found in {shapefile_dir}.")
    shp_path = os.path.join(shapefile_dir, shp_files[0])
    gdf = gpd.read_file(shp_path)
    gdf = gdf.to_crs("EPSG:3347")
    if 'CTUID' not in gdf.columns:
        raise RuntimeError("Shapefile missing 'CTUID' column.")
    gdf['CTUID'] = gdf['CTUID'].astype(str)
    return gdf


def _build_filtered_csv_cache(data_dir: str, clipped: gpd.GeoDataFrame, city_slug: str) -> str:
    """Filter nationwide English CSV to the city's tracts; derive CTUID via Geo mapping when needed."""
    target_dir = str(Path(data_dir) / city_slug)
    os.makedirs(target_dir, exist_ok=True)

    ct_set = set(clipped['CTUID'].astype(str).tolist())

    # Build DGUID->CTUID mapping from Geo file
    dguid_to_ctuid = {}
    geo_map_path = None
    for f in os.listdir(data_dir):
        if f.lower().endswith('.csv') and 'geo' in f.lower():
            geo_map_path = str(Path(data_dir) / f)
            break
    if geo_map_path:
        try:
            geo_df = None
            for enc in ['utf-8', 'cp1252', 'latin1']:
                try:
                    geo_df = pd.read_csv(geo_map_path, low_memory=False, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            if geo_df is None:
                raise Exception("Could not decode Geo file")
            if 'Geo Code' in geo_df.columns and 'Geo Name' in geo_df.columns:
                geo_df['Geo Code'] = geo_df['Geo Code'].astype(str)
                geo_df['Geo Name'] = geo_df['Geo Name'].astype(str)
                subset = geo_df[geo_df['Geo Name'].isin(ct_set)]
                dguid_to_ctuid = dict(zip(subset['Geo Code'], subset['Geo Name']))
                print(f"Built DGUID→CTUID mapping for {len(dguid_to_ctuid)} census tracts")
        except Exception as e:
            print(f"Warning: Could not read Geo mapping file: {e}")

    csv_files = [str(Path(data_dir) / f) for f in os.listdir(data_dir) if f.lower().endswith('.csv')]
    english_csv = next((p for p in csv_files if 'english_csv_data' in Path(p).name.lower()), None)
    if not english_csv:
        raise FileNotFoundError("Could not find 98-401-X2021007_English_CSV_data.csv in data directory")
    out_path = str(Path(target_dir) / f"{city_slug}_data.csv")
    if not os.path.exists(out_path):
        print(f"Pre-caching {Path(english_csv).name} for {city_slug}…")
        first_chunk = True
        rows_written = 0
        for enc in ['utf-8', 'cp1252', 'latin1']:
            try:
                chunk_num = 0
                for chunk in pd.read_csv(english_csv, encoding=enc, low_memory=False, chunksize=100000):
                    chunk_num += 1
                    if chunk_num % 50 == 0:
                        print(f"  Processed {chunk_num} chunks, {rows_written} rows written so far...")
                    id_col = None
                    for cand in ['CTUID', 'GEO_CODE (POR)', 'DGUID']:
                        if cand in chunk.columns:
                            id_col = cand
                            break
                    if id_col is None:
                        continue
                    chunk[id_col] = chunk[id_col].astype(str)
                    if id_col == 'DGUID' and dguid_to_ctuid:
                        # Map DGUIDs to CTUID and filter
                        chunk['CTUID'] = chunk[id_col].map(dguid_to_ctuid)
                        chunk = chunk[chunk['CTUID'].notna()]
                        if chunk.empty:
                            continue
                        chunk['CTUID'] = chunk['CTUID'].astype(str)
                    elif id_col != 'CTUID':
                        chunk = chunk.rename(columns={id_col: 'CTUID'})
                        chunk['CTUID'] = chunk['CTUID'].astype(str)
                        if ct_set:
                            chunk = chunk[chunk['CTUID'].isin(ct_set)]
                    else:
                        chunk['CTUID'] = chunk['CTUID'].astype(str)
                        if ct_set:
                            chunk = chunk[chunk['CTUID'].isin(ct_set)]
                    if chunk.empty:
                        continue
                    mode = 'w' if first_chunk else 'a'
                    header = first_chunk
                    chunk.to_csv(out_path, index=False, mode=mode, header=header)
                    rows_written += len(chunk)
                    first_chunk = False
                break
            except UnicodeDecodeError:
                continue
        if os.path.exists(out_path) and rows_written > 0:
            print(f"  Cached {rows_written} rows for {Path(out_path).name}")
    return target_dir


def build_city_json_cache(cache_dir: str, city_slug: str) -> str:
    """Build CTUID-keyed JSON grouped by category→metric using broader summary grouping."""
    city_dir = os.path.join(cache_dir, city_slug)
    data_csv = os.path.join(city_dir, f"{city_slug}_data.csv")
    out_json = os.path.join(city_dir, f"{city_slug}_profile_cache.json")
    if not os.path.exists(data_csv):
        raise FileNotFoundError(f"Cached data CSV not found: {data_csv}")
    try:
        df = pd.read_csv(data_csv, encoding="cp1252", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(data_csv, encoding="latin1", low_memory=False)
    if "CTUID" not in df.columns:
        raise ValueError("Filtered city CSV missing CTUID after processing; expected CTUID column.")

    value_col = None
    for cand in ["VALUE", "C1_COUNT_TOTAL", "C10_RATE_TOTAL", "C11_RATE_MEN+", "C12_RATE_WOMEN+", "C2_COUNT_MEN+", "C3_COUNT_WOMEN+"]:
        if cand in df.columns:
            value_col = cand
            break
    if value_col is None:
        for col in df.columns:
            if col in ("CTUID", "DGUID", "CHARACTERISTIC_NAME", "DIMENSION", "MEMBER_ID"):
                continue
            try:
                pd.to_numeric(df[col])
                value_col = col
                break
            except Exception:
                continue
    if value_col is None:
        raise ValueError("No usable numeric value column found in cached CSV")

    work = df[df["CTUID"].notna()].copy()
    member_label_col = None
    for cand in ["MEMBER", "MEMBER_LABEL", "MEMBER_NAME", "Member", "Member Label"]:
        if cand in work.columns:
            member_label_col = cand
            break

    dim_col = "DIMENSION" if "DIMENSION" in work.columns else None
    cache = {}
    work.sort_values(["CTUID", "CHARACTERISTIC_ID"], inplace=True)
    current_category_by_ct = {}
    for _, row in work.iterrows():
        ctuid = str(row["CTUID"]).strip()
        raw_char = str(row.get("CHARACTERISTIC_NAME", ""))
        leading_spaces = len(raw_char) - len(raw_char.lstrip(' '))
        char_name = raw_char.strip()
        if dim_col:
            category = str(row[dim_col]).strip() or char_name
        else:
            if char_name.startswith("Total - ") and leading_spaces == 0:
                current_category_by_ct[ctuid] = char_name
                category = char_name
            else:
                category = current_category_by_ct.get(ctuid)
                if not category:
                    parts = char_name.split(" - ")
                    category = parts[0] if parts and parts[0] else char_name or "Other"
        if member_label_col:
            metric = str(row.get(member_label_col, "")).strip() or char_name
        else:
            if char_name.startswith("Total - "):
                metric = "Total"
            else:
                if current_category_by_ct.get(ctuid) and leading_spaces > 2:
                    continue
                metric = char_name
        val_raw = row[value_col]
        try:
            val = float(val_raw)
        except Exception:
            try:
                val = float(str(val_raw).replace(",", ""))
            except Exception:
                continue
        ct_entry = cache.setdefault(ctuid, {})
        cat_entry = ct_entry.setdefault(category, {})
        cat_entry[metric] = val
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    return out_json


def build_city_cache_json(data_root: str, city: str, province: str, overwrite: bool = False) -> str:
    """End-to-end: clip tracts, filter CSVs, and write grouped JSON for a city."""
    city_slug = city.replace(' ', '_').lower()
    cache_dir = Path(data_root) / city_slug
    if cache_dir.exists() and (cache_dir / "profile_cache.json").exists() and not overwrite:
        return str(cache_dir)
    # Load tracts and boundary
    tracts = load_tracts(data_root)
    boundary = load_city_boundary(city, province)
    city_poly = boundary.iloc[0].geometry
    clipped = gpd.clip(tracts, city_poly)
    clipped['CTUID'] = clipped['CTUID'].astype(str)
    # Build filtered CSV cache
    _build_filtered_csv_cache(data_root, clipped, city_slug)
    # Build JSON
    build_city_json_cache(str(Path(data_root)), city_slug)
    return str(cache_dir)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Build Census Profile JSON caches for cities")
    parser.add_argument("city", nargs="?", help="City name, e.g., Vancouver")
    parser.add_argument("province", nargs="?", help="Province, e.g., British Columbia")
    # shape_dir hardcoded to data/censusShape
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing caches")
    parser.add_argument("--list", nargs="*", help="Optional list of cities as 'City,Province' pairs")
    args = parser.parse_args()

    data_root = str(Path('data') / 'censusShape')
    targets = []
    if args.list:
        for item in args.list:
            if ',' in item:
                c, p = item.split(',', 1)
                targets.append((c.strip(), p.strip()))
    elif args.city and args.province:
        targets.append((args.city, args.province))
    else:
        targets = [
            ("Vancouver", "British Columbia"), ("Toronto", "Ontario"), ("Montreal", "Quebec"),
            ("Calgary", "Alberta"), ("Edmonton", "Alberta"), ("Ottawa", "Ontario"), ("Winnipeg", "Manitoba"),
            ("Quebec City", "Quebec"), ("Hamilton", "Ontario"), ("Kitchener", "Ontario"), ("London", "Ontario"),
            ("Victoria", "British Columbia"), ("Halifax", "Nova Scotia"), ("Saskatoon", "Saskatchewan"), ("Regina", "Saskatchewan")
        ]

    for city, province in targets:
        print(f"Building cache for {city}, {province}...")
        try:
            out = build_city_cache_json(data_root, city, province, overwrite=args.overwrite)
            print(f"  Cache ready: {out}")
        except Exception as e:
            print(f"  Failed for {city}: {e}")


if __name__ == "__main__":
    main()
