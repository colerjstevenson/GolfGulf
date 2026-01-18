from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

# geopandas/shapely required
import geopandas as gpd

ROOT = Path(__file__).resolve().parent

IN_CSV = ROOT / "data/censusShape/vancouver/web_assets/metrics/_paper_analytic_tracts.csv"
GOLF_GJ = ROOT / "data/censusShape/vancouver/web_assets/golf_courses.geojson"




# MUST be the tract geometry file that corresponds to the same CTUID universe as IN_CSV (133 tracts).
# If this file does not include all CTUIDs in IN_CSV, the script will stop with a clear error and
# write an audit CSV listing the missing CTUIDs.
TRACTS_GEO = ROOT / "data/censusShape/vancouver/web_assets/tracts.geojson"

OUT_DIR = ROOT / "outputs/paper_tables"
OUT_DIR.mkdir(parents=True, exist_ok=True)

AUDIT_DIR = ROOT / "outputs/paper_audits"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------
# Helpers
# -----------------------
def normalize_ctuid(obj) -> str | None:
    """
    Normalize CTUIDs so joins work across formats:
    - whitespace -> stripped
    - '9330005.0' / '9330005.00' -> '9330005'
    - keep digits as-is (do NOT remove internal dots unless they are strictly a trailing decimal)
    """
    if pd.isna(obj):
        return None
    s = str(obj).strip()
    # Convert float-like CTUIDs coming from GeoJSON (e.g., "9330005.0")
    if s.endswith(".0"):
        s = s[:-2]
    # Handle rare ".00" cases
    if s.endswith(".00"):
        s = s[:-3]
    return s

def qcut_safe(s: pd.Series, q: int = 5) -> pd.Series:
    """
    Quintiles with safe fallback when many ties exist.
    Returns labels 1..q (or fewer if duplicates must be dropped).
    """
    s2 = pd.to_numeric(s, errors="coerce").copy()
    nunq = s2.dropna().nunique()
    if nunq == 0:
        return pd.Series([pd.NA] * len(s2), index=s2.index, dtype="Int64")
    if nunq < q:
        r = s2.rank(method="average")
        out = pd.qcut(r, q=q, labels=False, duplicates="drop")
    else:
        out = pd.qcut(s2, q=q, labels=False, duplicates="drop")
    return (out + 1).astype("Int64")

def weighted_mean(x: pd.Series, w: pd.Series) -> float:
    x = pd.to_numeric(x, errors="coerce")
    w = pd.to_numeric(w, errors="coerce")
    m = x.notna() & w.notna() & np.isfinite(x) & np.isfinite(w) & (w > 0)
    if int(m.sum()) == 0:
        return float("nan")
    return float(np.average(x[m].astype(float), weights=w[m].astype(float)))

# -----------------------
# Load analytic CSV
# -----------------------
df = pd.read_csv(IN_CSV, dtype={"CTUID": "string"})
if "CTUID" not in df.columns:
    raise RuntimeError(f"IN_CSV missing CTUID column: {IN_CSV}")

# Create canonical join key
df["CTUID_key"] = df["CTUID"].map(normalize_ctuid).astype("string")

for c in ["median_hh_income_2020", "pct_renters", "pct_65plus", "pop_2021"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

expected_n = int(df["CTUID_key"].nunique())
if expected_n == 0:
    raise RuntimeError(f"No CTUIDs found in IN_CSV: {IN_CSV}")

# Guard: duplicates in analytic universe
if df["CTUID_key"].isna().any():
    bad = df.loc[df["CTUID_key"].isna(), ["CTUID"]].head(20)
    raise RuntimeError(f"Some CTUIDs in IN_CSV could not be normalized. Examples:\n{bad.to_string(index=False)}")
if df["CTUID_key"].duplicated().any():
    dups = df.loc[df["CTUID_key"].duplicated(), "CTUID_key"].unique().tolist()
    raise RuntimeError(f"Duplicate CTUIDs in IN_CSV after normalization (showing up to 20): {dups[:20]}")

# -----------------------
# Load geometries
# -----------------------
tracts = gpd.read_file(TRACTS_GEO)

from paper_helpers import load_golf_courses, EXCLUDE_COURSES_DEFAULT

golf = load_golf_courses(GOLF_GJ, exclude_courses=EXCLUDE_COURSES_DEFAULT)

# Harmonize course-name column and filter exclusions
if "course_name" not in golf.columns and "name" in golf.columns:
    golf = golf.rename(columns={"name": "course_name"})
if "course_name" not in golf.columns:
    raise RuntimeError(f"Golf GeoJSON missing name field. Columns: {list(golf.columns)}")

golf["course_name"] = golf["course_name"].astype("string").str.strip()

EXCLUDE_COURSES = {"Stanley Park Pitch & Putt"}
golf = golf[~golf["course_name"].isin(EXCLUDE_COURSES)].copy()


if "CTUID" not in tracts.columns:
    raise RuntimeError(f"TRACTS_GEO must contain CTUID column: {TRACTS_GEO}")

# Canonical join key for tracts
tracts["CTUID_key"] = tracts["CTUID"].map(normalize_ctuid).astype("string")

# Guard: duplicates in geometry universe
if tracts["CTUID_key"].isna().any():
    bad = tracts.loc[tracts["CTUID_key"].isna(), ["CTUID"]].head(20)
    raise RuntimeError(f"Some CTUIDs in TRACTS_GEO could not be normalized. Examples:\n{bad.to_string(index=False)}")
if tracts["CTUID_key"].duplicated().any():
    dups = tracts.loc[tracts["CTUID_key"].duplicated(), "CTUID_key"].unique().tolist()
    raise RuntimeError(f"Duplicate CTUIDs in TRACTS_GEO after normalization (showing up to 20): {dups[:20]}")

if "course_name" not in golf.columns:
    raise RuntimeError(f"Golf GeoJSON must contain course_name (or name). Columns: {list(golf.columns)}")

# -----------------------
# Harmonize to analytic universe (133 tracts)
# -----------------------
# Keep ALL analytic tracts by doing a RIGHT join from tracts onto df.
g = tracts.merge(df, on="CTUID_key", how="right", suffixes=("_geo", ""))

# Identify missing geometries (CTUIDs present in df but absent from TRACTS_GEO)
missing_geom = g["geometry"].isna()
n_missing_geom = int(missing_geom.sum())

# Also identify extra geometries (CTUIDs present in TRACTS_GEO but not in df)
extra_in_tracts = tracts.loc[~tracts["CTUID_key"].isin(df["CTUID_key"]), "CTUID_key"].dropna().unique()

print("\n=== HARMONIZATION CHECK ===")
print("Analytic CTUIDs (expected):", expected_n)
print("Rows after right-merge (should equal expected):", len(g))
print("Missing geometries after merge:", n_missing_geom)
print("Extra CTUIDs in TRACTS_GEO not in analytic CSV:", len(extra_in_tracts))

if len(g) != expected_n:
    raise RuntimeError(
        "Row count after merge does not match analytic CTUID count. "
        f"expected={expected_n}, got={len(g)}."
    )

if n_missing_geom > 0:
    missing_ctuids = g.loc[missing_geom, "CTUID_key"].astype("string").tolist()
    audit_path = AUDIT_DIR / "audit_missing_tract_geometries_for_analytic_ctuids.csv"
    pd.DataFrame({"CTUID_missing_in_TRACTS_GEO": missing_ctuids}).to_csv(audit_path, index=False)
    raise RuntimeError(
        "TRACTS_GEO does not contain geometries for all analytic CTUIDs, so the by-course analysis "
        "cannot be harmonized to the full analytic universe.\n"
        f"- Analytic tracts (CTUID unique): {expected_n}\n"
        f"- Missing geometries in TRACTS_GEO: {n_missing_geom}\n"
        f"- Wrote missing CTUIDs to: {audit_path}\n"
        "Fix: point TRACTS_GEO to the tract-geometry file that corresponds to the same CTUID universe "
        "as _paper_analytic_tracts.csv (same census product, same coverage)."
    )

# At this point, g has geometries for ALL analytic tracts (133).
# Compute quintiles on the harmonized universe (not on a subset).
nonmiss_income = pd.to_numeric(g["median_hh_income_2020"], errors="coerce").dropna()
if len(nonmiss_income) == 0:
    raise RuntimeError("median_hh_income_2020 is entirely missing; cannot build income quintiles.")
g["income_q"] = qcut_safe(g["median_hh_income_2020"], q=5)

# -----------------------
# Project to metric CRS and clean geometries
# -----------------------
g = gpd.GeoDataFrame(g, geometry="geometry", crs=tracts.crs)

# EPSG:32610 = WGS84 / UTM zone 10N (Vancouver)
g = g.to_crs(epsg=32610)
golf = golf.to_crs(epsg=32610)

# Ensure valid geometries (important for intersections)
g["geometry"] = g["geometry"].buffer(0)
golf["geometry"] = golf["geometry"].buffer(0)

# -----------------------
# Course-by-course exposure (tract-level, then quintile summaries)
# -----------------------
rows = []
roll_rows = []
buffer_m = 800.0

for _, course in golf.iterrows():
    course_name = str(course["course_name"])
    course_geom = course.geometry
    course_buf = course_geom.buffer(buffer_m)

    # Distance from tract polygon to course polygon
    dist = g.geometry.distance(course_geom)

    # Area of tract that lies within 800m buffer around the course
    inter_area = g.geometry.intersection(course_buf).area

    # Use CTUID_key as stable ID
    tmp = g[["CTUID_key", "income_q", "pop_2021", "median_hh_income_2020"]].copy()
    tmp["course_name"] = course_name
    tmp["dist_to_course_m"] = pd.to_numeric(dist, errors="coerce")
    tmp["area_within_800m_m2"] = pd.to_numeric(inter_area, errors="coerce")

    # Course-level rollup from tract-level values (all 133 tracts)
    roll_rows.append({
        "course_name": course_name,
        "n_tracts": int(tmp["CTUID_key"].nunique()),
        "pop_sum": float(np.nansum(tmp["pop_2021"])),

        "dist_mean_m_tract_weighted": float(np.nanmean(tmp["dist_to_course_m"])),
        "area800_mean_m2_tract_weighted": float(np.nanmean(tmp["area_within_800m_m2"])),

        "dist_mean_m_pop_weighted": weighted_mean(tmp["dist_to_course_m"], tmp["pop_2021"]),
        "area800_mean_m2_pop_weighted": weighted_mean(tmp["area_within_800m_m2"], tmp["pop_2021"]),

        "share_tracts_within_800m": float(np.mean(tmp["area_within_800m_m2"] > 0)),
        "share_pop_within_800m": float(
            np.nansum(tmp.loc[tmp["area_within_800m_m2"] > 0, "pop_2021"]) / np.nansum(tmp["pop_2021"])
            if np.nansum(tmp["pop_2021"]) > 0 else np.nan
        ),
    })

    # Summarize by income quintile
    for q in sorted(tmp["income_q"].dropna().unique()):
        sub = tmp[tmp["income_q"] == q]
        rows.append({
            "course_name": course_name,
            "income_quintile": int(q),
            "n_tracts": int(len(sub)),
            "pop_sum": float(np.nansum(sub["pop_2021"])),
            "income_median": float(np.nanmedian(sub["median_hh_income_2020"])),

            "dist_mean_m": float(np.nanmean(sub["dist_to_course_m"])),
            "dist_median_m": float(np.nanmedian(sub["dist_to_course_m"])),

            "area800_mean_m2": float(np.nanmean(sub["area_within_800m_m2"])),
            "area800_median_m2": float(np.nanmedian(sub["area_within_800m_m2"])),

            "dist_pop_weighted_mean_m": weighted_mean(sub["dist_to_course_m"], sub["pop_2021"]),
            "area800_pop_weighted_mean_m2": weighted_mean(sub["area_within_800m_m2"], sub["pop_2021"]),

            "share_tracts_within_800m": float(np.mean(sub["area_within_800m_m2"] > 0)),
            "share_pop_within_800m": float(
                np.nansum(sub.loc[sub["area_within_800m_m2"] > 0, "pop_2021"]) / np.nansum(sub["pop_2021"])
                if np.nansum(sub["pop_2021"]) > 0 else np.nan
            ),
        })

tab = pd.DataFrame(rows).sort_values(["course_name", "income_quintile"])
out_path = OUT_DIR / "table_exposure_by_income_quintile_by_course.csv"
tab.to_csv(out_path, index=False)
print("Wrote:", out_path)

roll = pd.DataFrame(roll_rows).sort_values("course_name")
roll_path = OUT_DIR / "table_course_rollup.csv"
roll.to_csv(roll_path, index=False)
print("Wrote:", roll_path)

print("\n=== DONE ===")
print("Harmonized tract universe (CTUID unique):", expected_n)
print("Courses:", len(golf))
