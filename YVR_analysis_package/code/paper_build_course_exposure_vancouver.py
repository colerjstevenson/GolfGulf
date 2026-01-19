from __future__ import annotations

import re
import json
from pathlib import Path
from typing import Dict, Any, Tuple, List

import numpy as np
import pandas as pd
from shapely.geometry import shape
from shapely.ops import transform
from pyproj import Transformer

ROOT = Path(__file__).resolve().parent.parent.parent

TRACTS_GEOJSON = ROOT / "data/censusShape/vancouver/web_assets/tracts.geojson"
GOLF_GEOJSON = ROOT / "data/censusShape/vancouver/web_assets/golf_courses.geojson"

OUT_CSV = ROOT / "data/censusShape/vancouver/web_assets/metrics/_paper_tract_course_exposure.csv"
OUT_AUDIT = ROOT / "data/censusShape/vancouver/web_assets/metrics/_paper_tract_course_exposure_audit.json"

# Project lon/lat -> meters (Vancouver region)
TRANSFORMER = Transformer.from_crs("EPSG:4326", "EPSG:26910", always_xy=True)

def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:50] if len(s) > 50 else s

def load_features(path: Path) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        gj = json.load(f)
    return gj.get("features", []) or []

def geom_to_utm(geom) -> Any:
    return transform(TRANSFORMER.transform, geom)

def main() -> None:
    # --- Load tracts
    tract_feats = load_features(TRACTS_GEOJSON)
    tract_rows = []
    tract_geoms = {}

    for ft in tract_feats:
        props = ft.get("properties", {}) or {}
        ctuid = props.get("CTUID")
        if ctuid is None:
            continue
        g = ft.get("geometry")
        if not g:
            continue
        shp = shape(g)
        shp_utm = geom_to_utm(shp)
        tract_geoms[str(ctuid)] = shp_utm
        tract_rows.append({"CTUID": str(ctuid)})

    tracts = pd.DataFrame(tract_rows).drop_duplicates("CTUID")
    if len(tracts) == 0:
        raise RuntimeError("No tracts loaded; check tracts.geojson properties for CTUID.")

    # --- Load golf courses
    golf_feats = load_features(GOLF_GEOJSON)
    courses = []
    for ft in golf_feats:
        props = ft.get("properties", {}) or {}
        name = str(props.get("name", "")).strip()
        if not name:
            continue
        g = ft.get("geometry")
        if not g:
            continue
        shp = shape(g)
        shp_utm = geom_to_utm(shp)
        courses.append({
            "name": name,
            "slug": slugify(name),
            "access": str(props.get("access", "")).strip(),
            "geom": shp_utm
        })

    if len(courses) == 0:
        raise RuntimeError("No golf courses loaded; check golf_courses.geojson.")

    # --- For each tract: nearest-course and per-course 800m exposure
    out = tracts.copy()
    out["nearest_course_name"] = None
    out["nearest_course_access"] = None
    out["nearest_course_dist_m"] = np.nan

    # precreate per-course columns
    for c in courses:
        out[f"area800_{c['slug']}_m2"] = np.nan

    out["area800_all_courses_m2"] = np.nan

    # compute
    for i, ctuid in enumerate(out["CTUID"].tolist()):
        tgeom = tract_geoms.get(ctuid)
        if tgeom is None:
            continue

        # nearest course distance (polygon-to-polygon)
        best = (None, None, float("inf"))  # (name, access, dist)
        area_sum = 0.0

        # buffer the tract boundary by 800m (definition: within 800m of tract)
        tbuf = tgeom.buffer(800.0)

        for c in courses:
            cg = c["geom"]

            # distance in meters
            d = float(tgeom.distance(cg))
            if d < best[2]:
                best = (c["name"], c["access"], d)

            # area of this course that lies within 800m of tract
            inter = cg.intersection(tbuf)
            a = float(inter.area) if not inter.is_empty else 0.0
            out.loc[i, f"area800_{c['slug']}_m2"] = a
            area_sum += a

        out.loc[i, "nearest_course_name"] = best[0]
        out.loc[i, "nearest_course_access"] = best[1]
        out.loc[i, "nearest_course_dist_m"] = best[2] if np.isfinite(best[2]) else np.nan
        out.loc[i, "area800_all_courses_m2"] = area_sum

    # --- Audit
    audit = {
        "tract_rows": int(len(out)),
        "course_n": int(len(courses)),
        "nearest_dist_missing_n": int(out["nearest_course_dist_m"].isna().sum()),
        "area800_all_missing_n": int(out["area800_all_courses_m2"].isna().sum()),
        "nearest_course_counts": out["nearest_course_name"].value_counts(dropna=False).to_dict()
    }

    out.to_csv(OUT_CSV, index=False)
    with open(OUT_AUDIT, "w", encoding="utf-8") as f:
        json.dump(audit, f, indent=2)

    print("Wrote:", OUT_CSV)
    print("Wrote:", OUT_AUDIT)
    print("Preview:\n", out[["CTUID","nearest_course_name","nearest_course_access","nearest_course_dist_m","area800_all_courses_m2"]].head(8).to_string(index=False))

if __name__ == "__main__":
    main()
