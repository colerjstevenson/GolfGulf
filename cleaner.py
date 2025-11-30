from pathlib import Path
import sys
import json
import pandas as pd
import geopandas as gpd
from postal_lookup import PostalCodeLookup
import re
try:
    from deep_translator import GoogleTranslator
    has_translator = True
except ImportError:
    try:
        from googletrans import Translator
        has_translator = True
        use_deep_translator = False
    except ImportError:
        has_translator = False
        print("Warning: Neither deep_translator nor googletrans available. Translation will be skipped.")
else:
    use_deep_translator = True

#!/usr/bin/env python3
"""
cleaner.py

Combine all CSV and GeoJSON files from data/ into a single CSV and a single GeoJSON.
- Ensures first column is "gcid" and second is "name" (attempts to map common variants).
- Translates non-English text to English (requires: pip install deep-translator or googletrans==4.0.0rc1).
- Drops columns that are mostly empty after combining (default threshold: 50% empty).
- Outputs: data/combined.csv and data/combined.geojson
"""



# geopandas is optional but preferred for GeoJSON; fall back to manual merge if absent
try:
    has_gpd = True
except Exception:
    has_gpd = False

DATA_DIR = Path(__file__).resolve().parent / "data/world"
CSV_OUTPUT = DATA_DIR / "combined.csv"
GEOJSON_OUTPUT = DATA_DIR / "combined.geojson"

# Column name candidates for mapping to gcid and name
GCID_CANDIDATES = ["gcid", "id", "gid", "uuid", "global_id", "objectid"]
NAME_CANDIDATES = ["name", "title", "label", "placename", "site_name", "display_name"]


def has_non_english_chars(text):
    """Check if text contains non-English characters (excluding basic punctuation and numbers)."""
    if not isinstance(text, str) or not text.strip():
        return False
    
    # Allow basic English letters, numbers, spaces, and common punctuation
    english_pattern = re.compile(r'^[a-zA-Z0-9\s\.,\-\'"&()\[\]/\\:;!?@#$%^*+=~`|<>{}]+$')
    return not english_pattern.match(text.strip())


def translate_text(text, translator):
    """Translate text to English if it contains non-English characters."""
    if not has_translator or not translator or not isinstance(text, str) or not text.strip():
        return text
    
    if not has_non_english_chars(text):
        return text
    
    try:
        if use_deep_translator:
            # Using deep_translator (GoogleTranslator)
            result = translator.translate(text)
            if result:
                print(f"Translated: '{text}' -> '{result}'")
                return result
        else:
            # Using googletrans (legacy)
            result = translator.translate(text, dest='en')
            if result and hasattr(result, 'text') and result.text:
                print(f"Translated: '{text}' -> '{result.text}'")
                return result.text
    except Exception as e:
        print(f"Translation failed for '{text}': {e}")
    
    return text


def translate_dataframe_columns(df, columns_to_translate=None):
    """Translate specified columns in dataframe to English."""
    if not has_translator:
        print("Translation skipped: translation library not available")
        return df
    
    # Initialize the appropriate translator
    try:
        if use_deep_translator:
            translator = GoogleTranslator(source='auto', target='en')
        else:
            translator = Translator()
    except Exception as e:
        print(f"Failed to initialize translator: {e}")
        return df
    
    # Default to translating name-related columns
    if columns_to_translate is None:
        columns_to_translate = ['name', 'title', 'label', 'placename', 'site_name', 'display_name']
    
    # Filter to only existing columns
    existing_cols = [col for col in columns_to_translate if col in df.columns]
    
    if not existing_cols:
        return df
    
    print(f"Translating columns: {existing_cols}")
    
    for col in existing_cols:
        if col in df.columns:
            # Apply translation to non-null values
            mask = df[col].notna() & (df[col] != '')
            if mask.any():
                df.loc[mask, col] = df.loc[mask, col].apply(lambda x: translate_text(x, translator))
    
    return df


def normalize_cols(columns):
    """Return normalized column names (strip, lower, replace spaces with underscores)."""
    def norm(c):
        if c is None:
            return ""
        c = str(c).strip().lower()
        c = c.replace(" ", "_").replace("-", "_")
        return c
    return [norm(c) for c in columns]


def find_best_col(cols, candidates):
    """Return the first matching column name from candidates in cols, or None."""
    lc = {c: c for c in cols}
    for cand in candidates:
        if cand in cols:
            return cand
    # try fuzzy: remove underscores
    simplified = {c.replace("_", ""): c for c in cols}
    for cand in candidates:
        key = cand.replace("_", "")
        if key in simplified:
            return simplified[key]
    return None


def drop_sparse_columns(df, thresh=0.2):
    """
    Instead of dropping mostly-empty columns, reorder columns so that
    "sparse" columns (those with < thresh fraction of non-empty values)
    appear first, followed by the remaining columns sorted
    alphabetically. The geometry column (if present) is preserved at the end.
    thresh: fraction of values that must be non-empty to be considered "dense"
    For string columns treat empty string and whitespace as missing.
    """
    n = len(df)
    if n == 0:
        return df

    geom_name = df.geometry.name if hasattr(df, "geometry") else None
    sparse_cols = []
    dense_cols = []

    for col in df.columns:
        # skip geometry for now; append it at the end unchanged
        if geom_name and col == geom_name:
            continue
        s = df[col]
        if pd.api.types.is_string_dtype(s.dtype):
            non_empty = s.dropna().map(lambda v: str(v).strip() != "").sum()
        else:
            non_empty = s.count()  # non-null
        if non_empty / n >= thresh:
            dense_cols.append(col)
        else:
            sparse_cols.append(col)

    # keep sparse columns in their original order, sort dense columns alphabetically
    dense_cols_sorted = sorted(dense_cols, key=lambda x: x.lower())
    new_order = sparse_cols + dense_cols_sorted
    if geom_name and geom_name in df.columns:
        new_order.append(geom_name)

    # Reindex to the new column order
    return df.loc[:, new_order]


def load_existing_provinces(file_path):
    """Load existing provinces/countries from combined.csv if it exists."""
    existing_provinces = set()
    if file_path.exists():
        try:
            existing_df = pd.read_csv(file_path, dtype=str, low_memory=False)
            if 'province' in existing_df.columns:
                # Filter out null/nan values and convert to set
                existing_provinces = set(existing_df['province'].dropna().astype(str))
                existing_provinces.discard('nan')  # Remove 'nan' strings
                print(f"Found existing provinces/countries: {sorted(existing_provinces)}")
            else:
                print(f"No 'province' column found in existing {file_path}")
        except Exception as e:
            print(f"Failed to read existing {file_path}: {e}")
    return existing_provinces


def load_existing_provinces_from_geojson(file_path):
    """Load existing provinces/countries from combined.geojson if it exists."""
    existing_provinces = set()
    if file_path.exists():
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                geojson_data = json.load(f)
            
            if 'features' in geojson_data:
                for feature in geojson_data['features']:
                    if 'properties' in feature and 'province' in feature['properties']:
                        province = feature['properties']['province']
                        if province and str(province).strip() not in ['', 'nan', 'None']:
                            existing_provinces.add(str(province).strip())
            
            print(f"Found existing provinces/countries in GeoJSON: {sorted(existing_provinces)}")
        except Exception as e:
            print(f"Failed to read existing GeoJSON {file_path}: {e}")
    return existing_provinces


def extract_province_from_filename(filename):
    """Extract province/country name from golf_courses_*.csv filename."""
    # Remove golf_courses_ prefix and .csv suffix
    name = filename.replace('golf_courses_', '').replace('.csv', '').replace('.geojson', '').replace('.json', '')
    # Convert underscores to spaces and title case
    return name.replace('_', ' ').title()


def combine_csvs(data_dir, out_path, sparsity_threshold=0.2):
    # Load existing provinces/countries to avoid reprocessing entire regions
    existing_provinces = load_existing_provinces(out_path)
    
    csv_files = list(Path(data_dir).glob("**/golf_courses_*.csv"))
    if not csv_files:
        print("No CSV files found in", data_dir)
        return

    dfs = []
    existing_data_df = None
    files_to_process = []
    skipped_files = []
    
    # Load existing data once if it exists
    if existing_provinces and out_path.exists():
        try:
            existing_data_df = pd.read_csv(out_path, dtype=str, low_memory=False)
        except Exception as e:
            print(f"Failed to read existing {out_path}: {e}")
    
    # Filter files based on whether their province/country already exists
    for p in csv_files:
        province_from_file = extract_province_from_filename(p.name)
        if province_from_file in existing_provinces:
            print(f"Skipping {p.name} - {province_from_file} already exists in combined file")
            skipped_files.append(p)
        else:
            files_to_process.append(p)
    
    print(f"Processing {len(files_to_process)} new files, skipping {len(skipped_files)} existing regions")
    
    for p in files_to_process:
        try:
            df = pd.read_csv(p, dtype=str, low_memory=False)
        except Exception as e:
            print(f"Failed to read {p}: {e}", file=sys.stderr)
            continue
        # normalize columns
        orig_cols = list(df.columns)
        norm = normalize_cols(orig_cols)
        col_map = {o: n for o, n in zip(orig_cols, norm)}
        df = df.rename(columns=col_map)
        cols = list(df.columns)

        # Ensure gcid and name columns exist; try to map common variants
        gcid_col = find_best_col(cols, GCID_CANDIDATES)
        name_col = find_best_col(cols, NAME_CANDIDATES)

        if gcid_col and gcid_col != "gcid":
            df = df.rename(columns={gcid_col: "gcid"})
        elif not gcid_col:
            # create empty gcid to keep shape; will be NaN
            df["gcid"] = pd.NA

        if name_col and name_col != "name":
            df = df.rename(columns={name_col: "name"})
        elif not name_col:
            df["name"] = pd.NA
        
        # Set province from filename if not present
        province_from_file = extract_province_from_filename(p.name)
        if 'province' not in df.columns or df['province'].isna().all():
            df['province'] = province_from_file
        
        print(f"Processing {len(df)} rows from {p.name} ({province_from_file})")
        
        # Move gcid and name to front
        cols_now = [c for c in df.columns if c in ("gcid", "name", "province", 'lat', 'lon', 'area_m2')]
        new_order = ["gcid", "name", "province", 'lat', 'lon', 'area_m2']
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.reindex(columns=new_order)
        dfs.append(df)

    if not dfs and existing_data_df is None:
        print("No CSV content to combine.", file=sys.stderr)
        return
    
    # Combine new data if any
    if dfs:
        new_combined = pd.concat(dfs, ignore_index=True, sort=False)
        # Normalize gcid and name to string
        new_combined["gcid"] = new_combined["gcid"].astype(str).replace({"nan": pd.NA})
        new_combined["name"] = new_combined["name"].astype(str).replace({"nan": pd.NA})
        
        # Translate non-English text to English
        new_combined = translate_dataframe_columns(new_combined)
        print(f"Processed {len(new_combined)} new rows")
    else:
        new_combined = pd.DataFrame()
    
    # Combine with existing data
    if existing_data_df is not None and not existing_data_df.empty:
        if not new_combined.empty:
            combined = pd.concat([existing_data_df, new_combined], ignore_index=True, sort=False)
            print(f"Combined {len(new_combined)} new rows with {len(existing_data_df)} existing rows")
        else:
            combined = existing_data_df
            print(f"No new data to process, keeping {len(existing_data_df)} existing rows")
    else:
        combined = new_combined
    
    # Ensure output dir exists
    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out_path, index=False)
    print(f"Wrote combined CSV to {out_path} ({combined.shape[0]} rows, {combined.shape[1]} cols)")


def read_geojson_features(path):
    """Read a geojson file and return list of features (as dicts)."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if "features" in data:
        return data["features"]
    # If it's a single Feature, wrap it
    if data.get("type", "").lower() == "feature":
        return [data]
    raise ValueError("Unsupported GeoJSON structure in " + str(path))


def combine_geojsons(data_dir, out_path, sparsity_threshold=0.2):
    # Load existing provinces/countries to avoid reprocessing  
    # For GeoJSON, check the GeoJSON file itself, not the CSV
    existing_provinces = load_existing_provinces_from_geojson(out_path)
    
    geo_files = list(Path(data_dir).glob("**/golf_courses_*.geojson")) + list(Path(data_dir).glob("**/golf_courses_*.json"))
    geo_files = [p for p in geo_files if p.is_file()]
    if not geo_files:
        print("No GeoJSON files found in", data_dir)
        return

    # Filter files based on whether their province/country already exists
    files_to_process = []
    skipped_files = []
    
    for p in geo_files:
        province_from_file = extract_province_from_filename(p.name)
        if province_from_file in existing_provinces:
            print(f"Skipping {p.name} - {province_from_file} already exists in combined file")
            skipped_files.append(p)
        else:
            files_to_process.append(p)
    
    print(f"Processing {len(files_to_process)} new GeoJSON files, skipping {len(skipped_files)} existing regions")

    # Prefer geopandas if available
    if has_gpd:
        gdfs = []
        for p in files_to_process:
            try:
                gdf = gpd.read_file(p)
            except Exception as e:
                print(f"geopandas failed to read {p}: {e}", file=sys.stderr)
                # fallback to manual read
                try:
                    feats = read_geojson_features(p)
                    tmp = gpd.GeoDataFrame.from_features(feats)
                    gdfs.append(tmp)
                    continue
                except Exception as e2:
                    print(f"Failed to parse {p}: {e2}", file=sys.stderr)
                    continue
            # normalize columns
            orig_cols = list(gdf.columns)
            # keep geometry name separate
            geom_name = gdf.geometry.name if hasattr(gdf, "geometry") else None
            cols_non_geom = [c for c in orig_cols if c != geom_name]
            norm = normalize_cols(cols_non_geom)
            col_map = {o: n for o, n in zip(cols_non_geom, norm)}
            gdf = gdf.rename(columns=col_map)
            # map gcid/name
            cols = list(gdf.columns)
            gcid_col = find_best_col(cols, GCID_CANDIDATES)
            name_col = find_best_col(cols, NAME_CANDIDATES)
            if gcid_col and gcid_col != "gcid":
                gdf = gdf.rename(columns={gcid_col: "gcid"})
            elif not gcid_col:
                gdf["gcid"] = pd.NA
            if name_col and name_col != "name":
                gdf = gdf.rename(columns={name_col: "name"})
            elif not name_col:
                gdf["name"] = pd.NA
           # Force gcid and name to exist
            if "gcid" not in gdf.columns:
                gdf["gcid"] = pd.NA
            if "name" not in gdf.columns:
                gdf["name"] = pd.NA
            
            # Set province from filename if not present
            province_from_file = extract_province_from_filename(p.name)
            if 'province' not in gdf.columns or gdf['province'].isna().all():
                gdf['province'] = province_from_file

            # geometry name
            geom_name = gdf.geometry.name if hasattr(gdf, "geometry") else None

            # Build clean new_order
            new_order = ["gcid", "name"]

            # # Add all other columns except geometry
            # for c in gdf.columns:
            #     if c not in new_order and c != geom_name:
            #         new_order.append(c)

            # Add geometry last
            if geom_name:
                new_order.append(geom_name)

            # Remove any duplicates in case upstream added some
            new_order = list(dict.fromkeys(new_order))

            # reindex safely
            gdf = gdf.loc[:, ~gdf.columns.duplicated()]
            gdf = gdf.reindex(columns=new_order)
            gdfs.append(gdf)

        if not gdfs:
            print("No GeoDataFrames to combine.", file=sys.stderr)
            return

        combined_gdf = pd.concat(gdfs, ignore_index=True, sort=False)
        
        # Translate non-English text to English
        combined_gdf = translate_dataframe_columns(combined_gdf)
        
        # ensure geometry column
        if hasattr(combined_gdf, "geometry"):
            # Drop sparse columns excluding geometry
            combined_gdf = drop_sparse_columns(combined_gdf, thresh=sparsity_threshold)
            # Ensure geometry column exists and is named "geometry"
            if "geometry" not in combined_gdf.columns:
                # try to find a geometry-like column
                geoms = [c for c in combined_gdf.columns if combined_gdf[c].dtype.name == "geometry"]
                if geoms:
                    combined_gdf = combined_gdf.set_geometry(geoms[0])
            # write
            out_path.parent.mkdir(parents=True, exist_ok=True)
            combined_gdf.to_file(out_path, driver="GeoJSON")
            print(f"Wrote combined GeoJSON to {out_path} ({len(combined_gdf)} features, {len(combined_gdf.columns)} props)")
            return
        else:
            # fallback to manual feature merge below
            pass

    # Manual GeoJSON merge (no geopandas)
    all_features = []
    prop_keys = set()
    
    for p in files_to_process:
        try:
            feats = read_geojson_features(p)
        except Exception as e:
            print(f"Failed to read {p}: {e}", file=sys.stderr)
            continue
        
        province_from_file = extract_province_from_filename(p.name)
        
        for f in feats:
            props = f.get("properties", {}) or {}
            # normalize property keys
            new_props = {}
            for k, v in props.items():
                nk = k.strip().lower().replace(" ", "_").replace("-", "_")
                new_props[nk] = v
                prop_keys.add(nk)
            # attempt to map gcid/name
            if "gcid" not in new_props:
                gcid_k = find_best_col(list(new_props.keys()), GCID_CANDIDATES)
                if gcid_k:
                    new_props["gcid"] = new_props.pop(gcid_k)
                else:
                    new_props.setdefault("gcid", None)
            if "name" not in new_props:
                name_k = find_best_col(list(new_props.keys()), NAME_CANDIDATES)
                if name_k:
                    new_props["name"] = new_props.pop(name_k)
                else:
                    new_props.setdefault("name", None)
            
            # Set province from filename
            new_props["province"] = province_from_file
            
            f["properties"] = new_props
            all_features.append(f)

    if not all_features:
        print("No new features collected.", file=sys.stderr)
        return
    
    print(f"Collected {len(all_features)} features from new regions")

    # Build properties dataframe to compute sparsity
    props_list = [f.get("properties", {}) for f in all_features]
    props_df = pd.DataFrame(props_list)
    props_df = props_df.astype(object)
    
    # Translate non-English text to English
    props_df = translate_dataframe_columns(props_df)
    
    # Use drop_sparse_columns to compute desired ordering (sparse-first, dense alpha)
    props_df = drop_sparse_columns(props_df, thresh=sparsity_threshold)

    # Keep ordered list of keys to preserve column ordering in output
    keep_keys = list(props_df.columns)
    
    # Update feature properties with translated values
    for i, f in enumerate(all_features):
        props = f.get("properties", {})
        for key in keep_keys:
            if key in props and i < len(props_df):
                props[key] = props_df.iloc[i][key] if key in props_df.columns else props[key]
        f["properties"] = props
    
    # Handle merging with existing GeoJSON data
    existing_features = []
    if existing_provinces and out_path.exists():
        try:
            with open(out_path, 'r', encoding='utf-8') as f:
                existing_geojson = json.load(f)
            if 'features' in existing_geojson:
                existing_features = existing_geojson['features']
                print(f"Loaded {len(existing_features)} existing features from {out_path}")
        except Exception as e:
            print(f"Failed to read existing GeoJSON for merging: {e}")
    
    # Combine existing and new features
    all_combined_features = existing_features + all_features
    
    combined = {"type": "FeatureCollection", "features": []}
    for i, f in enumerate(all_combined_features):
        geom = f.get("geometry")
        props = f.get("properties", {})
        # Preserve ordering: iterate keep_keys and pick from props if present
        # For existing features, keep all their properties even if not in keep_keys
        if i < len(existing_features):
            # Existing feature - preserve all properties
            filtered_props = props
        else:
            # New feature - filter to keep_keys
            filtered_props = {k: props.get(k) for k in keep_keys if k in props}
        combined["features"].append({"type": "Feature", "geometry": geom, "properties": filtered_props})

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    
    if existing_features and all_features:
        print(f"Wrote combined GeoJSON to {out_path} ({len(existing_features)} existing + {len(all_features)} new = {len(combined['features'])} total features)")
    else:
        print(f"Wrote combined GeoJSON to {out_path} ({len(combined['features'])} features, {len(keep_keys)} props)")


def main():
    if not DATA_DIR.exists() or not DATA_DIR.is_dir():
        print(f"Data directory not found: {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    combine_csvs(DATA_DIR, CSV_OUTPUT)
    combine_geojsons(DATA_DIR, GEOJSON_OUTPUT)


if __name__ == "__main__":
    main()