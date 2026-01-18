# paper_course_inequality_contrasts.py
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd

ROOT = Path(__file__).resolve().parent

IN_CSV = ROOT / "data/censusShape/vancouver/web_assets/metrics/_paper_analytic_tracts.csv"
TRACTS_GEO = ROOT / "data/censusShape/vancouver/web_assets/tracts.geojson"
GOLF_GJ = ROOT / "data/censusShape/vancouver/web_assets/golf_courses.geojson"
OUT_DIR = ROOT / "outputs/paper_tables"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------
# Helpers
# -----------------------
def normalize_ctuid(s: pd.Series) -> pd.Series:
    """
    Normalize CTUIDs so joins work across formats:
    - '9330005.0' -> '9330005'
    - '  9330005' -> '9330005'
    - '9330.005' (unlikely here) -> '9330005' (only if dots exist)
    """
    s = s.astype("string").str.strip()
    # Remove trailing .0 / .00 if present (common GeoJSON export artifact)
    s = s.str.replace(r"\.0+$", "", regex=True)
    # If any dots remain (e.g., 9330.005), remove them
    s = s.str.replace(".", "", regex=False)
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

def weighted_mean(x: np.ndarray, w: np.ndarray) -> float:
    x = np.asarray(x, dtype=float)
    w = np.asarray(w, dtype=float)
    m = np.isfinite(x) & np.isfinite(w) & (w > 0)
    if m.sum() == 0:
        return float("nan")
    return float(np.average(x[m], weights=w[m]))

def share_pop_within_800m(area800: np.ndarray, pop: np.ndarray) -> float:
    area800 = np.asarray(area800, dtype=float)
    pop = np.asarray(pop, dtype=float)
    m = np.isfinite(area800) & np.isfinite(pop) & (pop >= 0)
    if m.sum() == 0:
        return float("nan")
    pop_tot = pop[m].sum()
    if pop_tot <= 0:
        return float("nan")
    return float(pop[m & (area800 > 0)].sum() / pop_tot)

def bootstrap_ci(diffs: np.ndarray, alpha: float = 0.05) -> tuple[float, float]:
    diffs = np.asarray(diffs, dtype=float)
    diffs = diffs[np.isfinite(diffs)]
    if diffs.size == 0:
        return (float("nan"), float("nan"))
    lo = float(np.quantile(diffs, alpha / 2))
    hi = float(np.quantile(diffs, 1 - alpha / 2))
    return lo, hi

# -----------------------
# Load and harmonize universe (133 tracts)
# -----------------------
df = pd.read_csv(IN_CSV, dtype={"CTUID": "string"})
if "CTUID" not in df.columns:
    raise RuntimeError(f"IN_CSV missing CTUID column: {IN_CSV}")

df["CTUID"] = normalize_ctuid(df["CTUID"])
for c in ["median_hh_income_2020", "pct_renters", "pct_65plus", "pop_2021"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

expected_n = int(df["CTUID"].nunique())
if expected_n != 133:
    print(f"WARNING: expected 133 tracts but analytic CSV has {expected_n} unique CTUIDs.")

tracts = gpd.read_file(TRACTS_GEO)
if "CTUID" not in tracts.columns:
    raise RuntimeError(f"TRACTS_GEO missing CTUID column: {TRACTS_GEO}")
tracts["CTUID"] = normalize_ctuid(tracts["CTUID"])

# -----------------------
# Load + standardize golf courses (single source of truth)
# -----------------------
EXCLUDE_COURSES = {"Stanley Park Pitch & Putt"}

from paper_helpers import load_golf_courses, EXCLUDE_COURSES_DEFAULT

golf = load_golf_courses(GOLF_GJ, exclude_courses=EXCLUDE_COURSES_DEFAULT)

# Harmonize name column
if "course_name" not in golf.columns and "name" in golf.columns:
    golf = golf.rename(columns={"name": "course_name"})

if "course_name" not in golf.columns:
    raise RuntimeError(f"Golf GeoJSON missing course_name/name. Columns: {list(golf.columns)}")

# Clean course names
golf["course_name"] = (
    golf["course_name"]
    .astype("string")
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)

# Apply exclusions
pre_n = len(golf)
golf = golf[~golf["course_name"].isin(EXCLUDE_COURSES)].copy().reset_index(drop=True)
dropped = pre_n - len(golf)
if dropped != len(EXCLUDE_COURSES):
    missing = sorted(EXCLUDE_COURSES - set(golf["course_name"].tolist()))
    print(f"[WARN] Exclusion names not matched exactly: {missing}")

# Right-join to keep full analytic universe
g = tracts.merge(df, on="CTUID", how="right")

print("\n=== HARMONIZATION CHECK ===")
print("Analytic CTUIDs (expected):", expected_n)
print("Rows after right-merge:", len(g))
print("Missing geometries after merge:", int(g["geometry"].isna().sum()))
if int(g["geometry"].isna().sum()) > 0:
    miss = g.loc[g["geometry"].isna(), "CTUID"].astype("string").tolist()[:20]
    raise RuntimeError(
        "TRACTS_GEO is missing geometries for analytic CTUIDs. "
        f"First few missing: {miss}"
    )

# Quintiles computed on the harmonized 133-tract universe
nonmiss_income = pd.to_numeric(g["median_hh_income_2020"], errors="coerce").dropna()
if len(nonmiss_income) == 0:
    raise RuntimeError("median_hh_income_2020 is entirely missing; cannot build income quintiles.")
g["income_q"] = qcut_safe(g["median_hh_income_2020"], q=5)

# Ensure GeoDataFrame + project to meters
g = gpd.GeoDataFrame(g, geometry="geometry", crs=tracts.crs)
g = g.to_crs(epsg=32610)      # Vancouver UTM 10N
golf = golf.to_crs(epsg=32610)

# Clean geometries
g["geometry"] = g["geometry"].buffer(0)
golf["geometry"] = golf["geometry"].buffer(0)

# -----------------------
# Build tract-level exposure table for each course
# -----------------------
buffer_m = 800.0

tract_rows = []
for _, course in golf.iterrows():
    course_name = str(course["course_name"])
    course_geom = course.geometry
    course_buf = course_geom.buffer(buffer_m)

    dist = g.geometry.distance(course_geom)
    area800 = g.geometry.intersection(course_buf).area

    tmp = pd.DataFrame({
        "CTUID": g["CTUID"].astype("string"),
        "course_name": course_name,
        "income_q": g["income_q"].astype("Int64"),
        "pop_2021": pd.to_numeric(g["pop_2021"], errors="coerce"),
        "median_hh_income_2020": pd.to_numeric(g["median_hh_income_2020"], errors="coerce"),
        "dist_to_course_m": pd.to_numeric(dist, errors="coerce"),
        "area_within_800m_m2": pd.to_numeric(area800, errors="coerce"),
    })
    tract_rows.append(tmp)

tract_exp = pd.concat(tract_rows, ignore_index=True)

# -----------------------
# Course inequality contrasts (Q5 - Q1) + bootstrap 95% CIs
# -----------------------
N_BOOT = 5000
SEED = 12345
rng = np.random.default_rng(SEED)

contrast_rows = []

for course_name, d0 in tract_exp.groupby("course_name", sort=True):
    d0 = d0.copy()

    # Keep only tracts with defined quintile (should be almost all)
    d0 = d0[d0["income_q"].notna()]
    d0["income_q"] = d0["income_q"].astype(int)

    q1 = d0[d0["income_q"] == 1]
    q5 = d0[d0["income_q"] == 5]

    if len(q1) == 0 or len(q5) == 0:
        # This should not happen with 5 quintiles, but keep it safe.
        contrast_rows.append({
            "course_name": course_name,
            "n_tracts_total": int(d0["CTUID"].nunique()),
            "n_tracts_q1": int(len(q1)),
            "n_tracts_q5": int(len(q5)),
            "diff_dist_mean_m_q5_minus_q1": np.nan,
            "diff_dist_mean_m_ci95_lo": np.nan,
            "diff_dist_mean_m_ci95_hi": np.nan,
            "diff_dist_pop_weighted_mean_m_q5_minus_q1": np.nan,
            "diff_dist_pop_weighted_mean_m_ci95_lo": np.nan,
            "diff_dist_pop_weighted_mean_m_ci95_hi": np.nan,
            "diff_share_pop_within_800m_q5_minus_q1": np.nan,
            "diff_share_pop_within_800m_ci95_lo": np.nan,
            "diff_share_pop_within_800m_ci95_hi": np.nan,
        })
        continue

    # Point estimates
    dist_mean_q1 = float(np.nanmean(q1["dist_to_course_m"]))
    dist_mean_q5 = float(np.nanmean(q5["dist_to_course_m"]))
    diff_dist_mean = dist_mean_q5 - dist_mean_q1

    dist_w_q1 = weighted_mean(q1["dist_to_course_m"].to_numpy(), q1["pop_2021"].to_numpy())
    dist_w_q5 = weighted_mean(q5["dist_to_course_m"].to_numpy(), q5["pop_2021"].to_numpy())
    diff_dist_w = dist_w_q5 - dist_w_q1

    share_pop_q1 = share_pop_within_800m(q1["area_within_800m_m2"].to_numpy(), q1["pop_2021"].to_numpy())
    share_pop_q5 = share_pop_within_800m(q5["area_within_800m_m2"].to_numpy(), q5["pop_2021"].to_numpy())
    diff_share_pop = share_pop_q5 - share_pop_q1

    # Bootstrap: stratified within quintile (Q1 and Q5 independently)
    b_diff_dist_mean = np.empty(N_BOOT, dtype=float)
    b_diff_dist_w = np.empty(N_BOOT, dtype=float)
    b_diff_share_pop = np.empty(N_BOOT, dtype=float)

    q1_idx = q1.index.to_numpy()
    q5_idx = q5.index.to_numpy()

    for b in range(N_BOOT):
        s1 = rng.choice(q1_idx, size=len(q1_idx), replace=True)
        s5 = rng.choice(q5_idx, size=len(q5_idx), replace=True)

        q1b = d0.loc[s1]
        q5b = d0.loc[s5]

        # Mean distance
        b_diff_dist_mean[b] = float(np.nanmean(q5b["dist_to_course_m"])) - float(np.nanmean(q1b["dist_to_course_m"]))

        # Pop-weighted mean distance
        b_diff_dist_w[b] = (
            weighted_mean(q5b["dist_to_course_m"].to_numpy(), q5b["pop_2021"].to_numpy())
            - weighted_mean(q1b["dist_to_course_m"].to_numpy(), q1b["pop_2021"].to_numpy())
        )

        # Share of population within 800m
        b_diff_share_pop[b] = (
            share_pop_within_800m(q5b["area_within_800m_m2"].to_numpy(), q5b["pop_2021"].to_numpy())
            - share_pop_within_800m(q1b["area_within_800m_m2"].to_numpy(), q1b["pop_2021"].to_numpy())
        )

    ci_dist_lo, ci_dist_hi = bootstrap_ci(b_diff_dist_mean)
    ci_dist_w_lo, ci_dist_w_hi = bootstrap_ci(b_diff_dist_w)
    ci_share_lo, ci_share_hi = bootstrap_ci(b_diff_share_pop)

    contrast_rows.append({
        "course_name": course_name,
        "n_tracts_total": int(d0["CTUID"].nunique()),
        "n_tracts_q1": int(len(q1)),
        "n_tracts_q5": int(len(q5)),

        "diff_dist_mean_m_q5_minus_q1": float(diff_dist_mean),
        "diff_dist_mean_m_ci95_lo": float(ci_dist_lo),
        "diff_dist_mean_m_ci95_hi": float(ci_dist_hi),

        "diff_dist_pop_weighted_mean_m_q5_minus_q1": float(diff_dist_w),
        "diff_dist_pop_weighted_mean_m_ci95_lo": float(ci_dist_w_lo),
        "diff_dist_pop_weighted_mean_m_ci95_hi": float(ci_dist_w_hi),

        "diff_share_pop_within_800m_q5_minus_q1": float(diff_share_pop),
        "diff_share_pop_within_800m_ci95_lo": float(ci_share_lo),
        "diff_share_pop_within_800m_ci95_hi": float(ci_share_hi),
    })

out = pd.DataFrame(contrast_rows).sort_values("course_name").reset_index(drop=True)

OUT_PATH = OUT_DIR / "table_course_inequality_contrasts.csv"
out.to_csv(OUT_PATH, index=False)

print("\n=== WROTE ===")
print(OUT_PATH)
print("\n=== QUICK CHECK (first 10 rows) ===")
print(out.head(10).to_string(index=False))
