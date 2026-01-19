#!/usr/bin/env python3
"""
Compute paper-grade tract-level golf exposure metrics for Vancouver and write them
to the existing Leaflet web_assets/metrics/ directory (one JSON per metric).

Inputs (as per your repo structure):
- data/censusShape/vancouver/web_assets/tracts.geojson
- data/censusShape/vancouver/web_assets/golf_courses.geojson
- data/censusShape/vancouver/vancouver_data.csv   (used only for join audit here)

Outputs:
- data/censusShape/vancouver/web_assets/metrics/dist_to_golf_m.json
- data/censusShape/vancouver/web_assets/metrics/golf_area_within_800m_m2.json
- data/censusShape/vancouver/web_assets/metrics/golf_area_within_1000m_m2.json
- data/censusShape/vancouver/web_assets/metrics/_paper_audit.json
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import unary_union

# ---- Paths (match your existing structure) ----
ROOT = Path(__file__).resolve().parent.parent.parent
TRACTS_GEOJSON = ROOT / "data/censusShape/vancouver/web_assets/tracts.geojson"
GOLF_GEOJSON   = ROOT / "data/censusShape/vancouver/web_assets/golf_courses.geojson"
CENSUS_CSV     = ROOT / "data/censusShape/vancouver/vancouver_data.csv"

METRICS_DIR    = ROOT / "data/censusShape/vancouver/web_assets/metrics"
AUDIT_JSON     = METRICS_DIR / "_paper_audit.json"

# ---- Parameters ----
# Use a metric CRS for Vancouver distances/areas
# EPSG:32610 = WGS84 / UTM zone 10N (common for Vancouver)
METRIC_CRS = "EPSG:32610"

BUFFERS_M = [800, 1000]


def normalize_ctuid(x: object) -> str | None:
    """
    Normalize CTUID strings to avoid float-ish formatting mismatches.
    Rules:
    - cast to string, strip
    - if endswith '.0' exactly, drop it
    - keep meaningful decimals (e.g., .01, .04)
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def write_metric_json(path: Path, series: pd.Series) -> None:
    """
    Write {CTUID: value} JSON with CTUID keys as strings and values as floats (or null).
    """
    out = {}
    for k, v in series.items():
        if k is None:
            continue
        if v is None or (isinstance(v, float) and np.isnan(v)):
            out[str(k)] = None
        else:
            out[str(k)] = float(v)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    # --- Load tracts ---
    tracts = gpd.read_file(TRACTS_GEOJSON)
    if "CTUID" not in tracts.columns:
        raise ValueError("tracts.geojson is missing properties.CTUID")

    tracts["CTUID_raw"] = tracts["CTUID"]
    tracts["CTUID"] = tracts["CTUID"].map(normalize_ctuid)

    # --- Load golf courses ---
    golf = gpd.read_file(GOLF_GEOJSON)

    # --- Basic audits: CTUID uniqueness and missingness ---
    if tracts["CTUID"].isna().any():
        missing = tracts.loc[tracts["CTUID"].isna(), "CTUID_raw"].head(10).tolist()
        raise ValueError(f"Some tract CTUIDs could not be normalized. Examples: {missing}")

    if tracts["CTUID"].duplicated().any():
        dups = tracts.loc[tracts["CTUID"].duplicated(), "CTUID"].head(10).tolist()
        raise ValueError(f"Duplicate CTUIDs in tracts after normalization. Examples: {dups}")

    # --- Load census CSV only to audit join coverage (long format) ---
    census = pd.read_csv(CENSUS_CSV, dtype={"CTUID": "string"}, low_memory=False)
    if "CTUID" not in census.columns:
        raise ValueError("vancouver_data.csv is missing CTUID column")
    census["CTUID"] = census["CTUID"].map(normalize_ctuid)
    census_ctuids = set(census["CTUID"].dropna().unique().tolist())
    tract_ctuids  = set(tracts["CTUID"].unique().tolist())

    in_tract_not_census = sorted(list(tract_ctuids - census_ctuids))[:25]
    in_census_not_tract = sorted(list(census_ctuids - tract_ctuids))[:25]

    # --- Reproject to metric CRS for distance/area ---
    tracts_m = tracts.to_crs(METRIC_CRS)
    golf_m   = golf.to_crs(METRIC_CRS)

    # --- Representative points inside each polygon (safer than centroid) ---
    rep_points = tracts_m.geometry.representative_point()
    # Ensure we have points (some geometries could be invalid; fix lightly if needed)
    rep_points = rep_points.apply(lambda g: g if isinstance(g, Point) else g.centroid)

    # --- Prepare golf union geometry for fast distance calculations ---
    # (unary_union can be heavy but should be fine for a handful of polygons)
    golf_union = unary_union(golf_m.geometry.values)

    # --- Metric 1: distance to nearest golf boundary ---
    dist_to_golf_m = rep_points.apply(lambda p: float(p.distance(golf_union)))
    dist_to_golf_m.index = tracts_m["CTUID"]

    # --- Metrics 2/3: golf area within buffers of each tract point ---
    # Compute intersection area of (buffer around point) with golf polygons
    area_by_buffer = {}
    for buf in BUFFERS_M:
        buf_geoms = rep_points.buffer(buf)
        # For each tract buffer, intersect with golf polygons; sum areas
        # Optimization: use spatial index + overlay-like logic without full overlay
        # Approach:
        #  - build GeoSeries for buffers with CTUID index
        buffers_gdf = gpd.GeoDataFrame({"CTUID": tracts_m["CTUID"].values}, geometry=buf_geoms, crs=METRIC_CRS)
        # Use sjoin to find golf polygons that intersect each buffer
        joined = gpd.sjoin(buffers_gdf, golf_m[["geometry"]].copy(), how="left", predicate="intersects")
        # For each match, compute intersection area
        # If a buffer has multiple golf polygons, sum them
        # If no match, area=0
        inter_areas = []
        for idx, row in joined.iterrows():
            if pd.isna(row.get("index_right")):
                inter_areas.append((row["CTUID"], 0.0))
            else:
                bgeom = buffers_gdf.loc[idx, "geometry"]
                ggeom = golf_m.loc[int(row["index_right"]), "geometry"]
                inter = bgeom.intersection(ggeom)
                inter_areas.append((row["CTUID"], float(inter.area) if not inter.is_empty else 0.0))

        inter_df = pd.DataFrame(inter_areas, columns=["CTUID", "area_m2"])
        area_series = inter_df.groupby("CTUID", as_index=True)["area_m2"].sum()
        # Ensure every CTUID has a value
        area_series = area_series.reindex(tracts_m["CTUID"]).fillna(0.0)
        area_by_buffer[buf] = area_series

    # --- Write outputs in your existing metrics directory ---
    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    write_metric_json(METRICS_DIR / "dist_to_golf_m.json", dist_to_golf_m)
    write_metric_json(METRICS_DIR / "golf_area_within_800m_m2.json", area_by_buffer[800])
    write_metric_json(METRICS_DIR / "golf_area_within_1000m_m2.json", area_by_buffer[1000])

    # --- Audit trail for verifiability ---
    audit = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "tracts_geojson": str(TRACTS_GEOJSON),
            "golf_geojson": str(GOLF_GEOJSON),
            "census_csv": str(CENSUS_CSV),
        },
        "crs": {
            "tracts_source": str(tracts.crs),
            "golf_source": str(golf.crs),
            "analysis_metric_crs": METRIC_CRS,
        },
        "counts": {
            "tracts_features": int(len(tracts)),
            "golf_features": int(len(golf)),
            "census_rows": int(len(census)),
            "unique_ctuid_tracts": int(len(tract_ctuids)),
            "unique_ctuid_census": int(len(census_ctuids)),
        },
        "ctuid_join_audit_examples": {
            "in_tract_not_census_first25": in_tract_not_census,
            "in_census_not_tract_first25": in_census_not_tract,
        },
        "metric_summaries": {
            "dist_to_golf_m": {
                "min": float(dist_to_golf_m.min()),
                "p50": float(dist_to_golf_m.median()),
                "p90": float(dist_to_golf_m.quantile(0.9)),
                "max": float(dist_to_golf_m.max()),
            },
            "golf_area_within_800m_m2": {
                "min": float(area_by_buffer[800].min()),
                "p50": float(area_by_buffer[800].median()),
                "p90": float(area_by_buffer[800].quantile(0.9)),
                "max": float(area_by_buffer[800].max()),
            },
            "golf_area_within_1000m_m2": {
                "min": float(area_by_buffer[1000].min()),
                "p50": float(area_by_buffer[1000].median()),
                "p90": float(area_by_buffer[1000].quantile(0.9)),
                "max": float(area_by_buffer[1000].max()),
            },
        },
        "notes": [
            "CTUID normalized by stripping trailing '.0' only when present.",
            "Distances/areas computed in EPSG:32610 (UTM 10N).",
            "Representative points used (inside polygons) rather than centroids.",
            "golf_area_within_* computed as sum of intersections between buffers and golf polygons."
        ]
    }
    AUDIT_JSON.write_text(json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8")

    print("Wrote metrics to:", METRICS_DIR)
    print("Wrote audit to:", AUDIT_JSON)
    print("Join audit (tract not in census) first few:", in_tract_not_census[:5])
    print("Join audit (census not in tract) first few:", in_census_not_tract[:5])


if __name__ == "__main__":
    main()



