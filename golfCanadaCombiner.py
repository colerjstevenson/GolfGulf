#!/usr/bin/env python3
"""Combine all files in data/ whose filename contains 'golf_canada' into
one CSV and one JSON called golf_canada_full.csv/.json.

Usage: python combiner.py
"""
from pathlib import Path
import json
import re
import pandas as pd


DATA_DIR = Path(__file__).resolve().parent / "data/canada"
OUT_CSV = DATA_DIR / "golf_canada_full.csv"
OUT_JSON = DATA_DIR / "golf_canada_full.json"


def load_json_records(p: Path):
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Common patterns: list of dicts (scraper output), or GeoJSON FeatureCollection
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if data.get("type") == "FeatureCollection" and isinstance(data.get("features"), list):
            recs = []
            for feat in data.get("features", []):
                props = feat.get("properties", {}) or {}
                # optionally preserve geometry
                if "geometry" in feat:
                    props["geometry"] = feat.get("geometry")
                recs.append(props)
            return recs
        # fallback: wrap dict
        return [data]
    # unknown type, return empty
    return []


def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    # Trim and collapse whitespace for string columns
    def _clean(v):
        if isinstance(v, str):
            s = v.replace("\xa0", " ")
            s = s.strip()
            # collapse internal whitespace
            s = " ".join(s.split())
            return s
        return v
    
    # split address column into google_link, address, and postal_code if present
    if 'Address' in df.columns:
        def split_address(addr):
            if not isinstance(addr, str):
                return pd.Series([pd.NA, pd.NA, pd.NA])
            parts = [part.strip() for part in addr.split(' ')]
            google_link = parts[0] if len(parts) > 0 else pd.NA
            address = ' '.join(parts[1:]) if len(parts) > 2 else pd.NA
            
            postal_code_match = re.search(r'[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d', addr)
            postal_code = postal_code_match.group(0) if postal_code_match else pd.NA
            return pd.Series([google_link, address, postal_code])
        
        addr_split = df['Address'].apply(split_address)
        addr_split.columns = ['google_link', 'address', 'postal_code']
        df = pd.concat([df.drop(columns=['Address']), addr_split], axis=1)

    return df.applymap(_clean)


def drop_sparse_columns(df: pd.DataFrame, thresh: float = 0.2) -> pd.DataFrame:
    """Drop columns that are mostly empty.

    thresh: fraction of values that must be non-empty to keep the column.
    For string/object columns, empty string after normalization is considered missing.
    """
    n = len(df)
    if n == 0:
        return df
    keep = []
    for col in df.columns:
        s = df[col]
        # treat object/string dtype specially to consider empty strings missing
        if s.dtype == object:
            non_empty = s.dropna().map(lambda v: str(v).strip() != "").sum()
        else:
            non_empty = s.count()
        if non_empty / n >= thresh:
            keep.append(col)
    return df.loc[:, keep]


def main():
    files = sorted(DATA_DIR.glob("**/golf_canada_data*"))
    # csvs = [p for p in files if p.suffix.lower() == ".csv"]
    jsons = [p for p in files if p.suffix.lower() in (".json", ".ndjson")]  # include .json

    print(f"Found {len(jsons)} JSON file(s) matching 'golf_canada'.")

    dfs = []
    # for p in csvs:
    #     try:
    #         df = pd.read_csv(p, dtype=str, low_memory=False)
    #         dfs.append(df)
    #         print(f"Loaded CSV: {p} ({len(df)} rows)")
    #     except Exception as e:
    #         print(f"Failed to read CSV {p}: {e}")

    for p in jsons:
        try:
            recs = load_json_records(p)
            if not recs:
                print(f"No records found in JSON {p}")
                continue
            df = pd.json_normalize(recs)
            dfs.append(df)
            print(f"Loaded JSON: {p} ({len(df)} rows)")
        except Exception as e:
            print(f"Failed to read JSON {p}: {e}")

    if not dfs:
        print("No data loaded; nothing to combine.")
        return

    combined = pd.concat(dfs, ignore_index=True, sort=False)
    combined = normalize_strings(combined)

    # Exclude any records where the URL contains '-fr' (French pages)
    if 'url' in combined.columns:
        before = len(combined)
        # match "-fr" optionally followed by a dash and digits (e.g. "-fr", "-fr-2", "-fr-10")
        mask_fr = combined['url'].fillna('').str.contains(r'-fr(?:-\d+)?', regex=True)
        removed = int(mask_fr.sum())
        if removed:
            print(f"Excluding {removed} record(s) with '-fr/' in the url")
            combined = combined.loc[~mask_fr].reset_index(drop=True)
        else:
            print("No records with '-fr' found in url column.")
    else:
        print("No 'url' column present; skipping '-fr' exclusion.")

    # Drop columns that are mostly empty after filtering
    before_cols = list(combined.columns)
    combined = drop_sparse_columns(combined, thresh=0.2)
    dropped_cols = [c for c in before_cols if c not in combined.columns]
    if dropped_cols:
        print(f"Dropped {len(dropped_cols)} mostly-empty column(s): {dropped_cols}")
    else:
        print("No mostly-empty columns to drop.")

    # ensure output dir
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUT_CSV, index=False)

    # Save JSON as list of dicts
    records = combined.where(pd.notnull(combined), None).to_dict(orient="records")
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Wrote combined CSV: {OUT_CSV} ({len(combined)} rows, {len(combined.columns)} cols)")
    print(f"Wrote combined JSON: {OUT_JSON} ({len(records)} records)")


if __name__ == "__main__":
    main()

